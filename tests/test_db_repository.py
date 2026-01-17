"""Tests for database repository module.

Tests the TradeRepository class including initialization, connection pooling,
wallet/market/trade CRUD operations, and error handling.
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from src.db import OrderSide, TradeSide, TradeStatus
from src.db.models import Market, Trade, Wallet
from src.db.repository import (
    DatabaseConnectionError,
    DatabaseSchemaError,
    TradeRepository,
)


class TestTradeRepositoryInit:
    """Test TradeRepository initialization."""

    def test_init_disabled_when_empty_database_url(self):
        """Verify repository is disabled when database URL is empty."""
        repo = TradeRepository("")
        assert repo.is_enabled is False
        assert repo.is_configured is False

    def test_init_disabled_when_none_database_url(self):
        """Verify repository handles None-like empty string gracefully."""
        repo = TradeRepository("")
        assert repo.is_enabled is False
        assert repo._pool is None
        assert repo.is_configured is False

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_init_enabled_with_valid_database_url(self, mock_pool_class, mock_verify):
        """Verify repository is enabled with valid database URL."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        assert repo.is_enabled is True
        assert repo._pool is not None
        assert repo.is_configured is True

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_init_creates_connection_pool(self, mock_pool_class, mock_verify):
        """Verify connection pool is created with correct parameters."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        TradeRepository("postgresql://user:pass@localhost/db")

        mock_pool_class.assert_called_once()
        call_args = mock_pool_class.call_args
        assert call_args[0][0] == "postgresql://user:pass@localhost/db"
        assert call_args[1]["min_size"] == 1
        assert call_args[1]["max_size"] == 5
        assert call_args[1]["timeout"] == 10.0

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_init_tests_connection(self, mock_pool_class, mock_verify):
        """Verify pool tests connection on initialization."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        TradeRepository("postgresql://user:pass@localhost/db")

        # Verify SELECT 1 was executed to test connection
        mock_conn.execute.assert_called_once_with("SELECT 1")

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_init_raises_on_connection_error(self, mock_pool_class, mock_verify):
        """Verify repository raises when connection fails (database configured)."""
        mock_pool_class.side_effect = Exception("Connection refused")

        with pytest.raises(DatabaseConnectionError):
            TradeRepository("postgresql://invalid:url@localhost/db")

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_init_raises_on_pool_test_error(self, mock_pool_class, mock_verify):
        """Verify repository raises when connection test fails (database configured)."""
        mock_pool = MagicMock()
        mock_pool.connection.side_effect = Exception("Test query failed")
        mock_pool_class.return_value = mock_pool

        with pytest.raises(DatabaseConnectionError):
            TradeRepository("postgresql://user:pass@localhost/db")

    @patch.object(TradeRepository, "_run_migrations")
    @patch.object(TradeRepository, "_verify_schema", side_effect=[False, False])
    @patch("src.db.repository.ConnectionPool")
    def test_init_raises_on_schema_missing_after_migration(
        self, mock_pool_class, mock_verify, mock_migrate
    ):
        """Verify repository raises when schema is still missing after migration."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        with pytest.raises(DatabaseSchemaError):
            TradeRepository("postgresql://user:pass@localhost/db")

        # Verify migration was attempted
        mock_migrate.assert_called_once()

    @patch.object(TradeRepository, "_run_migrations")
    @patch.object(TradeRepository, "_verify_schema", side_effect=[False, True])
    @patch("src.db.repository.ConnectionPool")
    def test_init_runs_migrations_when_schema_missing(
        self, mock_pool_class, mock_verify, mock_migrate
    ):
        """Verify repository runs migrations when schema is missing."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")

        # Verify migration was run
        mock_migrate.assert_called_once()
        assert repo.is_enabled is True


class TestTradeRepositoryClose:
    """Test TradeRepository close method."""

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_close_closes_pool(self, mock_pool_class, mock_verify):
        """Verify close() closes the connection pool."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        repo.close()

        mock_pool.close.assert_called_once()

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_close_sets_disabled(self, mock_pool_class, mock_verify):
        """Verify close() sets is_enabled to False."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        assert repo.is_enabled is True

        repo.close()
        assert repo.is_enabled is False
        assert repo._pool is None

    def test_close_when_disabled_no_error(self):
        """Verify close() handles disabled state gracefully."""
        repo = TradeRepository("")
        repo.close()  # Should not raise
        assert repo.is_enabled is False


class TestTradeRepositoryWalletOperations:
    """Test wallet CRUD operations."""

    def test_get_or_create_wallet_when_disabled(self):
        """Verify get_or_create_wallet returns None when disabled."""
        repo = TradeRepository("")
        result = repo.get_or_create_wallet("0x1234567890123456789012345678901234567890")
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_or_create_wallet_success(self, mock_pool_class, mock_verify):
        """Verify get_or_create_wallet creates and returns wallet."""
        wallet_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": wallet_id,
            "address": "0x1234567890123456789012345678901234567890",
            "name": "Test Wallet",
            "signature_type": 1,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_or_create_wallet(
            "0x1234567890123456789012345678901234567890",
            name="Test Wallet",
            signature_type=1,
        )

        assert result is not None
        assert isinstance(result, Wallet)
        assert result.id == wallet_id
        assert result.address == "0x1234567890123456789012345678901234567890"
        assert result.name == "Test Wallet"
        assert result.signature_type == 1

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_or_create_wallet_commits_transaction(self, mock_pool_class, mock_verify):
        """Verify get_or_create_wallet commits the transaction."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "id": uuid4(),
            "address": "0x1234567890123456789012345678901234567890",
            "name": None,
            "signature_type": 0,
            "is_active": True,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        repo.get_or_create_wallet("0x1234567890123456789012345678901234567890")

        mock_conn.commit.assert_called_once()

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_or_create_wallet_returns_none_on_error(self, mock_pool_class, mock_verify):
        """Verify get_or_create_wallet returns None on database error."""
        import psycopg

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_or_create_wallet("0x1234567890123456789012345678901234567890")

        assert result is None

    def test_get_wallet_by_address_when_disabled(self):
        """Verify get_wallet_by_address returns None when disabled."""
        repo = TradeRepository("")
        result = repo.get_wallet_by_address("0x1234567890123456789012345678901234567890")
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_wallet_by_address_found(self, mock_pool_class, mock_verify):
        """Verify get_wallet_by_address returns wallet when found."""
        wallet_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": wallet_id,
            "address": "0xabcdef1234567890abcdef1234567890abcdef12",
            "name": "Found Wallet",
            "signature_type": 0,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_wallet_by_address("0xabcdef1234567890abcdef1234567890abcdef12")

        assert result is not None
        assert isinstance(result, Wallet)
        assert result.id == wallet_id
        assert result.address == "0xabcdef1234567890abcdef1234567890abcdef12"

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_wallet_by_address_not_found(self, mock_pool_class, mock_verify):
        """Verify get_wallet_by_address returns None when not found."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_wallet_by_address("0x0000000000000000000000000000000000000000")

        assert result is None


class TestTradeRepositoryMarketOperations:
    """Test market CRUD operations."""

    def test_get_or_create_market_when_disabled(self):
        """Verify get_or_create_market returns None when disabled."""
        repo = TradeRepository("")
        result = repo.get_or_create_market("condition-abc-123")
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_or_create_market_success(self, mock_pool_class, mock_verify):
        """Verify get_or_create_market creates and returns market."""
        market_id = uuid4()
        now = datetime.now()
        end_date = datetime(2024, 12, 31, 23, 59, 59)
        mock_row = {
            "id": market_id,
            "condition_id": "polymarket-condition-xyz",
            "question": "Will BTC reach $100k?",
            "end_date": end_date,
            "resolved": False,
            "winning_side": None,
            "resolution_price": None,
            "created_at": now,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_or_create_market(
            "polymarket-condition-xyz",
            question="Will BTC reach $100k?",
            end_date=end_date,
        )

        assert result is not None
        assert isinstance(result, Market)
        assert result.id == market_id
        assert result.condition_id == "polymarket-condition-xyz"
        assert result.question == "Will BTC reach $100k?"
        assert result.end_date == end_date
        assert result.resolved is False

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_or_create_market_commits_transaction(self, mock_pool_class, mock_verify):
        """Verify get_or_create_market commits the transaction."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "id": uuid4(),
            "condition_id": "test-condition",
            "question": None,
            "end_date": None,
            "resolved": False,
            "winning_side": None,
            "resolution_price": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        repo.get_or_create_market("test-condition")

        mock_conn.commit.assert_called_once()

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_or_create_market_returns_none_on_error(self, mock_pool_class, mock_verify):
        """Verify get_or_create_market returns None on database error."""
        import psycopg

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_or_create_market("test-condition")

        assert result is None

    def test_get_market_by_condition_id_when_disabled(self):
        """Verify get_market_by_condition_id returns None when disabled."""
        repo = TradeRepository("")
        result = repo.get_market_by_condition_id("condition-abc-123")
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_market_by_condition_id_found(self, mock_pool_class, mock_verify):
        """Verify get_market_by_condition_id returns market when found."""
        market_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": market_id,
            "condition_id": "found-condition-id",
            "question": "Test Question?",
            "end_date": None,
            "resolved": True,
            "winning_side": "YES",
            "resolution_price": Decimal("1.0"),
            "created_at": now,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_market_by_condition_id("found-condition-id")

        assert result is not None
        assert isinstance(result, Market)
        assert result.condition_id == "found-condition-id"
        assert result.resolved is True
        assert result.winning_side == "YES"

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_market_by_condition_id_not_found(self, mock_pool_class, mock_verify):
        """Verify get_market_by_condition_id returns None when not found."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_market_by_condition_id("nonexistent-condition")

        assert result is None


class TestTradeRepositoryTradeOperations:
    """Test trade CRUD operations."""

    def test_create_trade_when_disabled(self):
        """Verify create_trade returns None when disabled."""
        repo = TradeRepository("")
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        result = repo.create_trade(trade)
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_create_trade_success(self, mock_pool_class, mock_verify):
        """Verify create_trade creates and returns trade with database ID."""
        trade_id = uuid4()
        wallet_id = uuid4()
        market_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": trade_id,
            "wallet_id": wallet_id,
            "market_id": market_id,
            "order_id": "clob-order-123",
            "token_id": "test-token-id",
            "side": "YES",
            "order_type": "BUY",
            "quantity": Decimal("100"),
            "filled_quantity": Decimal("0"),
            "limit_price": Decimal("0.65"),
            "avg_fill_price": None,
            "exit_price": None,
            "cost_basis_usd": None,
            "proceeds_usd": None,
            "realized_pnl": None,
            "neg_risk": False,
            "status": "open",
            "created_at": now,
            "filled_at": None,
            "closed_at": None,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        trade = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            order_id="clob-order-123",
            token_id="test-token-id",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        result = repo.create_trade(trade)

        assert result is not None
        assert isinstance(result, Trade)
        assert result.id == trade_id
        assert result.wallet_id == wallet_id
        assert result.market_id == market_id
        assert result.status == TradeStatus.OPEN

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_create_trade_commits_transaction(self, mock_pool_class, mock_verify):
        """Verify create_trade commits the transaction."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "id": uuid4(),
            "wallet_id": uuid4(),
            "market_id": uuid4(),
            "order_id": None,
            "token_id": "token",
            "side": "YES",
            "order_type": "BUY",
            "quantity": Decimal("100"),
            "filled_quantity": Decimal("0"),
            "limit_price": Decimal("0.65"),
            "avg_fill_price": None,
            "exit_price": None,
            "cost_basis_usd": None,
            "proceeds_usd": None,
            "realized_pnl": None,
            "neg_risk": False,
            "status": "open",
            "created_at": datetime.now(),
            "filled_at": None,
            "closed_at": None,
            "updated_at": datetime.now(),
        }
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        repo.create_trade(trade)

        mock_conn.commit.assert_called_once()

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_create_trade_returns_none_on_error(self, mock_pool_class, mock_verify):
        """Verify create_trade returns None on database error."""
        import psycopg

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        result = repo.create_trade(trade)

        assert result is None


class TestTradeRepositoryUpdateTrade:
    """Test update_trade operation."""

    def test_update_trade_when_disabled(self):
        """Verify update_trade returns None when disabled."""
        repo = TradeRepository("")
        result = repo.update_trade(uuid4(), status=TradeStatus.FILLED)
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_update_trade_status_success(self, mock_pool_class, mock_verify):
        """Verify update_trade updates status successfully."""
        trade_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": trade_id,
            "wallet_id": uuid4(),
            "market_id": uuid4(),
            "order_id": "clob-123",
            "token_id": "token",
            "side": "YES",
            "order_type": "BUY",
            "quantity": Decimal("100"),
            "filled_quantity": Decimal("100"),
            "limit_price": Decimal("0.65"),
            "avg_fill_price": Decimal("0.64"),
            "exit_price": None,
            "cost_basis_usd": None,
            "proceeds_usd": None,
            "realized_pnl": None,
            "neg_risk": False,
            "status": "filled",
            "created_at": now,
            "filled_at": now,
            "closed_at": None,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.update_trade(
            trade_id,
            status=TradeStatus.FILLED,
            filled_quantity=Decimal("100"),
            avg_fill_price=Decimal("0.64"),
            filled_at=now,
        )

        assert result is not None
        assert result.status == TradeStatus.FILLED
        assert result.filled_quantity == Decimal("100")

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_update_trade_partial_fill(self, mock_pool_class, mock_verify):
        """Verify update_trade can update for partial fill."""
        trade_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": trade_id,
            "wallet_id": uuid4(),
            "market_id": uuid4(),
            "order_id": "clob-123",
            "token_id": "token",
            "side": "NO",
            "order_type": "SELL",
            "quantity": Decimal("100"),
            "filled_quantity": Decimal("50"),
            "limit_price": Decimal("0.45"),
            "avg_fill_price": Decimal("0.46"),
            "exit_price": None,
            "cost_basis_usd": None,
            "proceeds_usd": None,
            "realized_pnl": None,
            "neg_risk": True,
            "status": "partially_filled",
            "created_at": now,
            "filled_at": None,
            "closed_at": None,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.update_trade(
            trade_id,
            status=TradeStatus.PARTIALLY_FILLED,
            filled_quantity=Decimal("50"),
            avg_fill_price=Decimal("0.46"),
        )

        assert result is not None
        assert result.status == TradeStatus.PARTIALLY_FILLED
        assert result.filled_quantity == Decimal("50")
        assert result.neg_risk is True

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_update_trade_commits_transaction(self, mock_pool_class, mock_verify):
        """Verify update_trade commits the transaction."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {
            "id": uuid4(),
            "wallet_id": uuid4(),
            "market_id": uuid4(),
            "order_id": None,
            "token_id": "token",
            "side": "YES",
            "order_type": "BUY",
            "quantity": Decimal("100"),
            "filled_quantity": Decimal("0"),
            "limit_price": Decimal("0.65"),
            "avg_fill_price": None,
            "exit_price": None,
            "cost_basis_usd": None,
            "proceeds_usd": None,
            "realized_pnl": None,
            "neg_risk": False,
            "status": "cancelled",
            "created_at": datetime.now(),
            "filled_at": None,
            "closed_at": None,
            "updated_at": datetime.now(),
        }
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        repo.update_trade(uuid4(), status=TradeStatus.CANCELLED)

        mock_conn.commit.assert_called_once()

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_update_trade_not_found(self, mock_pool_class, mock_verify):
        """Verify update_trade returns None when trade not found."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.update_trade(uuid4(), status=TradeStatus.FILLED)

        assert result is None


class TestTradeRepositoryGetTradeByOrderId:
    """Test get_trade_by_order_id operation."""

    def test_get_trade_by_order_id_when_disabled(self):
        """Verify get_trade_by_order_id returns None when disabled."""
        repo = TradeRepository("")
        result = repo.get_trade_by_order_id("clob-order-123")
        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_trade_by_order_id_found(self, mock_pool_class, mock_verify):
        """Verify get_trade_by_order_id returns trade when found."""
        trade_id = uuid4()
        wallet_id = uuid4()
        market_id = uuid4()
        now = datetime.now()
        mock_row = {
            "id": trade_id,
            "wallet_id": wallet_id,
            "market_id": market_id,
            "order_id": "clob-order-xyz",
            "token_id": "token-abc",
            "side": "YES",
            "order_type": "BUY",
            "quantity": Decimal("200"),
            "filled_quantity": Decimal("200"),
            "limit_price": Decimal("0.70"),
            "avg_fill_price": Decimal("0.69"),
            "exit_price": None,
            "cost_basis_usd": Decimal("138.00"),
            "proceeds_usd": None,
            "realized_pnl": None,
            "neg_risk": False,
            "status": "filled",
            "created_at": now,
            "filled_at": now,
            "closed_at": None,
            "updated_at": now,
        }

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_trade_by_order_id("clob-order-xyz")

        assert result is not None
        assert isinstance(result, Trade)
        assert result.id == trade_id
        assert result.order_id == "clob-order-xyz"
        assert result.status == TradeStatus.FILLED

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_trade_by_order_id_not_found(self, mock_pool_class, mock_verify):
        """Verify get_trade_by_order_id returns None when not found."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_trade_by_order_id("nonexistent-order")

        assert result is None

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_trade_by_order_id_returns_none_on_error(self, mock_pool_class, mock_verify):
        """Verify get_trade_by_order_id returns None on database error."""
        import psycopg

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_trade_by_order_id("some-order")

        assert result is None


class TestTradeRepositoryGetOpenTrades:
    """Test get_open_trades operation."""

    def test_get_open_trades_when_disabled(self):
        """Verify get_open_trades returns empty list when disabled."""
        repo = TradeRepository("")
        result = repo.get_open_trades()
        assert result == []

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_open_trades_returns_open_and_partial(self, mock_pool_class, mock_verify):
        """Verify get_open_trades returns trades with open or partially_filled status."""
        now = datetime.now()
        mock_rows = [
            {
                "id": uuid4(),
                "wallet_id": uuid4(),
                "market_id": uuid4(),
                "order_id": "order-1",
                "token_id": "token-1",
                "side": "YES",
                "order_type": "BUY",
                "quantity": Decimal("100"),
                "filled_quantity": Decimal("0"),
                "limit_price": Decimal("0.65"),
                "avg_fill_price": None,
                "exit_price": None,
                "cost_basis_usd": None,
                "proceeds_usd": None,
                "realized_pnl": None,
                "neg_risk": False,
                "status": "open",
                "created_at": now,
                "filled_at": None,
                "closed_at": None,
                "updated_at": now,
            },
            {
                "id": uuid4(),
                "wallet_id": uuid4(),
                "market_id": uuid4(),
                "order_id": "order-2",
                "token_id": "token-2",
                "side": "NO",
                "order_type": "SELL",
                "quantity": Decimal("200"),
                "filled_quantity": Decimal("100"),
                "limit_price": Decimal("0.40"),
                "avg_fill_price": Decimal("0.41"),
                "exit_price": None,
                "cost_basis_usd": None,
                "proceeds_usd": None,
                "realized_pnl": None,
                "neg_risk": True,
                "status": "partially_filled",
                "created_at": now,
                "filled_at": None,
                "closed_at": None,
                "updated_at": now,
            },
        ]

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = mock_rows
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_open_trades()

        assert len(result) == 2
        assert all(isinstance(t, Trade) for t in result)
        assert result[0].status == TradeStatus.OPEN
        assert result[1].status == TradeStatus.PARTIALLY_FILLED

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_open_trades_filtered_by_wallet(self, mock_pool_class, mock_verify):
        """Verify get_open_trades can filter by wallet_id."""
        wallet_id = uuid4()
        now = datetime.now()
        mock_rows = [
            {
                "id": uuid4(),
                "wallet_id": wallet_id,
                "market_id": uuid4(),
                "order_id": "order-1",
                "token_id": "token-1",
                "side": "YES",
                "order_type": "BUY",
                "quantity": Decimal("100"),
                "filled_quantity": Decimal("0"),
                "limit_price": Decimal("0.65"),
                "avg_fill_price": None,
                "exit_price": None,
                "cost_basis_usd": None,
                "proceeds_usd": None,
                "realized_pnl": None,
                "neg_risk": False,
                "status": "open",
                "created_at": now,
                "filled_at": None,
                "closed_at": None,
                "updated_at": now,
            },
        ]

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = mock_rows
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_open_trades(wallet_id=wallet_id)

        assert len(result) == 1
        assert result[0].wallet_id == wallet_id

        # Verify the query included wallet_id parameter
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "wallet_id = %s" in call_args[0][0]

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_open_trades_empty_result(self, mock_pool_class, mock_verify):
        """Verify get_open_trades returns empty list when no open trades."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_open_trades()

        assert result == []

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_open_trades_returns_empty_on_error(self, mock_pool_class, mock_verify):
        """Verify get_open_trades returns empty list on database error."""
        import psycopg

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")
        result = repo.get_open_trades()

        assert result == []


class TestDatabaseConnectionError:
    """Test DatabaseConnectionError exception."""

    def test_database_connection_error_is_exception(self):
        """Verify DatabaseConnectionError is an Exception."""
        assert issubclass(DatabaseConnectionError, Exception)

    def test_database_connection_error_message(self):
        """Verify DatabaseConnectionError stores message."""
        error = DatabaseConnectionError("Connection failed")
        assert str(error) == "Connection failed"

    def test_database_connection_error_can_be_raised(self):
        """Verify DatabaseConnectionError can be raised and caught."""
        with pytest.raises(DatabaseConnectionError) as exc_info:
            raise DatabaseConnectionError("Test error")
        assert "Test error" in str(exc_info.value)


class TestTradeRepositoryConnectionContext:
    """Test _get_connection context manager."""

    def test_get_connection_raises_when_pool_not_initialized(self):
        """Verify _get_connection raises when pool is None."""
        repo = TradeRepository("")
        with pytest.raises(DatabaseConnectionError) as exc_info:
            with repo._get_connection():
                pass
        assert "not initialized" in str(exc_info.value)

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_get_connection_yields_connection(self, mock_pool_class, mock_verify):
        """Verify _get_connection yields connection from pool."""
        mock_conn = MagicMock()
        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")

        with repo._get_connection() as conn:
            assert conn is mock_conn


class TestTradeRepositoryRowConversion:
    """Test row-to-dataclass conversion methods."""

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_row_to_wallet_converts_all_fields(self, mock_pool_class, mock_verify):
        """Verify _row_to_wallet converts all fields correctly."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")

        wallet_id = uuid4()
        now = datetime.now()
        row = {
            "id": wallet_id,
            "address": "0x1234567890123456789012345678901234567890",
            "name": "Test",
            "signature_type": 2,
            "is_active": False,
            "created_at": now,
            "updated_at": now,
        }

        wallet = repo._row_to_wallet(row)

        assert wallet.id == wallet_id
        assert wallet.address == "0x1234567890123456789012345678901234567890"
        assert wallet.name == "Test"
        assert wallet.signature_type == 2
        assert wallet.is_active is False
        assert wallet.created_at == now
        assert wallet.updated_at == now

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_row_to_market_converts_all_fields(self, mock_pool_class, mock_verify):
        """Verify _row_to_market converts all fields correctly."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")

        market_id = uuid4()
        now = datetime.now()
        end_date = datetime(2024, 12, 31)
        row = {
            "id": market_id,
            "condition_id": "cond-123",
            "question": "Test?",
            "end_date": end_date,
            "resolved": True,
            "winning_side": "NO",
            "resolution_price": Decimal("0"),
            "created_at": now,
            "updated_at": now,
        }

        market = repo._row_to_market(row)

        assert market.id == market_id
        assert market.condition_id == "cond-123"
        assert market.question == "Test?"
        assert market.end_date == end_date
        assert market.resolved is True
        assert market.winning_side == "NO"
        assert market.resolution_price == Decimal("0")

    @patch.object(TradeRepository, "_verify_schema", return_value=True)
    @patch("src.db.repository.ConnectionPool")
    def test_row_to_trade_converts_all_fields(self, mock_pool_class, mock_verify):
        """Verify _row_to_trade converts all fields correctly."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool_class.return_value = mock_pool

        repo = TradeRepository("postgresql://user:pass@localhost/db")

        trade_id = uuid4()
        wallet_id = uuid4()
        market_id = uuid4()
        now = datetime.now()
        row = {
            "id": trade_id,
            "wallet_id": wallet_id,
            "market_id": market_id,
            "order_id": "order-xyz",
            "token_id": "token-abc",
            "side": "NO",
            "order_type": "SELL",
            "quantity": Decimal("500"),
            "filled_quantity": Decimal("250"),
            "limit_price": Decimal("0.45"),
            "avg_fill_price": Decimal("0.46"),
            "exit_price": Decimal("0.50"),
            "cost_basis_usd": Decimal("115"),
            "proceeds_usd": Decimal("125"),
            "realized_pnl": Decimal("10"),
            "neg_risk": True,
            "status": "closed",
            "created_at": now,
            "filled_at": now,
            "closed_at": now,
            "updated_at": now,
        }

        trade = repo._row_to_trade(row)

        assert trade.id == trade_id
        assert trade.wallet_id == wallet_id
        assert trade.market_id == market_id
        assert trade.order_id == "order-xyz"
        assert trade.token_id == "token-abc"
        assert trade.side == TradeSide.NO
        assert trade.order_type == OrderSide.SELL
        assert trade.quantity == Decimal("500")
        assert trade.filled_quantity == Decimal("250")
        assert trade.limit_price == Decimal("0.45")
        assert trade.avg_fill_price == Decimal("0.46")
        assert trade.exit_price == Decimal("0.50")
        assert trade.cost_basis_usd == Decimal("115")
        assert trade.proceeds_usd == Decimal("125")
        assert trade.realized_pnl == Decimal("10")
        assert trade.neg_risk is True
        assert trade.status == TradeStatus.CLOSED
        assert trade.created_at == now
        assert trade.filled_at == now
        assert trade.closed_at == now
        assert trade.updated_at == now
