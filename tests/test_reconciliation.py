"""Tests for trade reconciliation module.

Tests the TradeReconciler class including initialization, reconciliation logic,
CLOB API status mapping, and error handling.
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from src.config import Config
from src.db import OrderSide, TradeSide, TradeStatus
from src.db.models import Trade
from src.db.reconciliation import (
    ReconciliationError,
    TradeReconciler,
    map_clob_status_to_trade_status,
)


class TestMapClobStatusToTradeStatus:
    """Test map_clob_status_to_trade_status function."""

    def test_live_status_returns_open(self):
        """Verify LIVE status maps to OPEN."""
        result = map_clob_status_to_trade_status("LIVE")
        assert result == TradeStatus.OPEN

    def test_live_status_case_insensitive(self):
        """Verify LIVE status mapping is case insensitive."""
        assert map_clob_status_to_trade_status("live") == TradeStatus.OPEN
        assert map_clob_status_to_trade_status("Live") == TradeStatus.OPEN
        assert map_clob_status_to_trade_status("LIVE") == TradeStatus.OPEN

    def test_cancelled_status_returns_cancelled(self):
        """Verify CANCELLED status maps to CANCELLED."""
        result = map_clob_status_to_trade_status("CANCELLED")
        assert result == TradeStatus.CANCELLED

    def test_cancelled_status_case_insensitive(self):
        """Verify CANCELLED status mapping is case insensitive."""
        assert map_clob_status_to_trade_status("cancelled") == TradeStatus.CANCELLED
        assert map_clob_status_to_trade_status("Cancelled") == TradeStatus.CANCELLED

    def test_matched_status_returns_filled_without_sizes(self):
        """Verify MATCHED status defaults to FILLED without size info."""
        result = map_clob_status_to_trade_status("MATCHED")
        assert result == TradeStatus.FILLED

    def test_matched_status_returns_filled_when_fully_matched(self):
        """Verify MATCHED with full fill returns FILLED."""
        result = map_clob_status_to_trade_status(
            "MATCHED",
            size_matched=Decimal("100"),
            original_size=Decimal("100"),
        )
        assert result == TradeStatus.FILLED

    def test_matched_status_returns_filled_when_overfilled(self):
        """Verify MATCHED with overfill returns FILLED."""
        result = map_clob_status_to_trade_status(
            "MATCHED",
            size_matched=Decimal("110"),
            original_size=Decimal("100"),
        )
        assert result == TradeStatus.FILLED

    def test_matched_status_returns_partially_filled_when_partial(self):
        """Verify MATCHED with partial fill returns PARTIALLY_FILLED."""
        result = map_clob_status_to_trade_status(
            "MATCHED",
            size_matched=Decimal("50"),
            original_size=Decimal("100"),
        )
        assert result == TradeStatus.PARTIALLY_FILLED

    def test_matched_status_returns_filled_when_zero_matched(self):
        """Verify MATCHED with zero matched defaults to FILLED (edge case)."""
        result = map_clob_status_to_trade_status(
            "MATCHED",
            size_matched=Decimal("0"),
            original_size=Decimal("100"),
        )
        # Zero matched is not > 0, so defaults to FILLED
        assert result == TradeStatus.FILLED

    def test_unknown_status_returns_open(self):
        """Verify unknown status defaults to OPEN."""
        result = map_clob_status_to_trade_status("UNKNOWN_STATUS")
        assert result == TradeStatus.OPEN

    def test_empty_status_returns_open(self):
        """Verify empty status defaults to OPEN."""
        result = map_clob_status_to_trade_status("")
        assert result == TradeStatus.OPEN

    def test_matched_with_only_size_matched_returns_filled(self):
        """Verify MATCHED with only size_matched (no original_size) returns FILLED."""
        result = map_clob_status_to_trade_status(
            "MATCHED",
            size_matched=Decimal("50"),
            original_size=None,
        )
        assert result == TradeStatus.FILLED

    def test_matched_with_only_original_size_returns_filled(self):
        """Verify MATCHED with only original_size (no size_matched) returns FILLED."""
        result = map_clob_status_to_trade_status(
            "MATCHED",
            size_matched=None,
            original_size=Decimal("100"),
        )
        assert result == TradeStatus.FILLED


class TestReconciliationError:
    """Test ReconciliationError exception."""

    def test_reconciliation_error_is_exception(self):
        """Verify ReconciliationError is an Exception."""
        assert issubclass(ReconciliationError, Exception)

    def test_reconciliation_error_message(self):
        """Verify ReconciliationError stores message."""
        error = ReconciliationError("Reconciliation failed")
        assert str(error) == "Reconciliation failed"

    def test_reconciliation_error_can_be_raised(self):
        """Verify ReconciliationError can be raised and caught."""
        with pytest.raises(ReconciliationError) as exc_info:
            raise ReconciliationError("Test error")
        assert "Test error" in str(exc_info.value)


class TestTradeReconcilerInit:
    """Test TradeReconciler initialization."""

    def test_init_disabled_when_repository_disabled(self):
        """Verify reconciler is disabled when repository is disabled."""
        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = False

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is False

    def test_init_disabled_when_no_private_key(self):
        """Verify reconciler is disabled when private_key is empty."""
        config = Config(private_key="")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is False

    def test_init_disabled_when_signature_type_1_without_funder(self):
        """Verify reconciler is disabled when signature_type=1 but funder_address is empty."""
        config = Config(
            private_key="test_key",
            signature_type=1,
            funder_address="",
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is False

    @patch("src.db.reconciliation.ClobClient")
    def test_init_enabled_with_valid_config(self, mock_clob_client):
        """Verify reconciler is enabled with valid config."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            private_key="test_private_key",
            signature_type=0,
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is True

    @patch("src.db.reconciliation.ClobClient")
    def test_init_enabled_with_signature_type_1_and_funder(self, mock_clob_client):
        """Verify reconciler is enabled with signature_type=1 and funder_address."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            private_key="test_private_key",
            signature_type=1,
            funder_address="0xfunder1234567890abcdef1234567890abcdef1234",
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is True

    @patch("src.db.reconciliation.ClobClient")
    def test_init_calls_create_or_derive_api_creds(self, mock_clob_client):
        """Verify CLOB client initializes with API credential derivation."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_private_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        TradeReconciler(config, mock_repository)

        mock_client_instance.create_or_derive_api_creds.assert_called_once()
        mock_client_instance.set_api_creds.assert_called_once()

    @patch("src.db.reconciliation.ClobClient")
    def test_init_disabled_on_client_error(self, mock_clob_client):
        """Verify reconciler is disabled when CLOB client initialization fails."""
        mock_clob_client.side_effect = Exception("Connection failed")

        config = Config(private_key="test_private_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is False

    @patch("src.db.reconciliation.ClobClient")
    def test_init_passes_funder_to_clob_client_for_signature_type_1(self, mock_clob_client):
        """Verify funder parameter is passed to ClobClient when signature_type=1."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        funder_address = "0xfunder1234567890abcdef1234567890abcdef1234"
        config = Config(
            private_key="test_private_key",
            signature_type=1,
            funder_address=funder_address,
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        TradeReconciler(config, mock_repository)

        mock_clob_client.assert_called_once()
        call_kwargs = mock_clob_client.call_args[1]
        assert "funder" in call_kwargs
        assert call_kwargs["funder"] == funder_address

    @patch("src.db.reconciliation.ClobClient")
    def test_init_no_funder_for_signature_type_0(self, mock_clob_client):
        """Verify funder parameter is NOT passed for signature_type=0 (EOA)."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            private_key="test_private_key",
            signature_type=0,
            funder_address="0xsome_address",  # Should be ignored
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        TradeReconciler(config, mock_repository)

        mock_clob_client.assert_called_once()
        call_kwargs = mock_clob_client.call_args[1]
        assert "funder" not in call_kwargs


class TestTradeReconcilerIsEnabled:
    """Test is_enabled property."""

    def test_is_enabled_property_returns_false_when_disabled(self):
        """Verify is_enabled returns False when reconciler is disabled."""
        config = Config(private_key="")
        mock_repository = MagicMock()
        mock_repository.is_enabled = False

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is False

    @patch("src.db.reconciliation.ClobClient")
    def test_is_enabled_property_returns_true_when_enabled(self, mock_clob_client):
        """Verify is_enabled returns True when reconciler is enabled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is True


class TestTradeReconcilerReconcile:
    """Test reconcile() method."""

    def test_reconcile_returns_zero_when_disabled(self):
        """Verify reconcile returns 0 when reconciler is disabled."""
        config = Config(private_key="")
        mock_repository = MagicMock()
        mock_repository.is_enabled = False

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 0
        mock_repository.get_open_trades.assert_not_called()

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_returns_zero_when_no_open_trades(self, mock_clob_client):
        """Verify reconcile returns 0 when there are no open trades."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.get_open_trades.return_value = []

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 0
        mock_repository.get_open_trades.assert_called_once()

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_skips_trades_without_order_id(self, mock_clob_client):
        """Verify reconcile skips trades without order_id."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        # Create a trade without order_id
        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id=None,  # No order_id
            status=TradeStatus.OPEN,
        )
        mock_repository.get_open_trades.return_value = [trade]

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 0
        mock_client_instance.get_order.assert_not_called()

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_updates_filled_trade(self, mock_clob_client):
        """Verify reconcile updates a trade that has been filled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "MATCHED",
            "size_matched": "100",
            "original_size": "100",
            "average_price": "0.64",
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="clob-order-123",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )
        mock_repository.get_open_trades.return_value = [trade]

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 1
        mock_repository.update_trade.assert_called_once()
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["status"] == TradeStatus.FILLED
        assert call_kwargs["filled_quantity"] == Decimal("100")
        assert call_kwargs["avg_fill_price"] == Decimal("0.64")

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_updates_partially_filled_trade(self, mock_clob_client):
        """Verify reconcile updates a trade that has been partially filled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "MATCHED",
            "size_matched": "50",
            "original_size": "100",
            "average_price": "0.65",
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="clob-order-123",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )
        mock_repository.get_open_trades.return_value = [trade]

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 1
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["status"] == TradeStatus.PARTIALLY_FILLED
        assert call_kwargs["filled_quantity"] == Decimal("50")

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_updates_cancelled_trade(self, mock_clob_client):
        """Verify reconcile updates a trade that has been cancelled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "CANCELLED",
            "size_matched": "0",
            "original_size": "100",
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="clob-order-123",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )
        mock_repository.get_open_trades.return_value = [trade]

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 1
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["status"] == TradeStatus.CANCELLED

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_skips_unchanged_trade(self, mock_clob_client):
        """Verify reconcile skips trades that haven't changed."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "LIVE",
            "size_matched": "0",
            "original_size": "100",
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="clob-order-123",
            status=TradeStatus.OPEN,  # Already OPEN
            filled_quantity=Decimal("0"),  # Already 0
        )
        mock_repository.get_open_trades.return_value = [trade]

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        assert result == 0
        mock_repository.update_trade.assert_not_called()

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_multiple_trades(self, mock_clob_client):
        """Verify reconcile processes multiple trades."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        # First trade filled, second unchanged, third cancelled
        mock_client_instance.get_order.side_effect = [
            {"status": "MATCHED", "size_matched": "100", "original_size": "100"},
            {"status": "LIVE", "size_matched": "0", "original_size": "100"},
            {"status": "CANCELLED", "size_matched": "0", "original_size": "100"},
        ]
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        trades = [
            Trade(
                id=uuid4(),
                wallet_id=uuid4(),
                market_id=uuid4(),
                token_id="token-1",
                side=TradeSide.YES,
                order_type=OrderSide.BUY,
                quantity=Decimal("100"),
                limit_price=Decimal("0.65"),
                order_id="order-1",
                status=TradeStatus.OPEN,
                filled_quantity=Decimal("0"),
            ),
            Trade(
                id=uuid4(),
                wallet_id=uuid4(),
                market_id=uuid4(),
                token_id="token-2",
                side=TradeSide.NO,
                order_type=OrderSide.SELL,
                quantity=Decimal("100"),
                limit_price=Decimal("0.45"),
                order_id="order-2",
                status=TradeStatus.OPEN,
                filled_quantity=Decimal("0"),
            ),
            Trade(
                id=uuid4(),
                wallet_id=uuid4(),
                market_id=uuid4(),
                token_id="token-3",
                side=TradeSide.YES,
                order_type=OrderSide.BUY,
                quantity=Decimal("100"),
                limit_price=Decimal("0.70"),
                order_id="order-3",
                status=TradeStatus.OPEN,
                filled_quantity=Decimal("0"),
            ),
        ]
        mock_repository.get_open_trades.return_value = trades

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        # Two trades updated (filled and cancelled), one unchanged
        assert result == 2
        assert mock_repository.update_trade.call_count == 2

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_continues_on_single_trade_error(self, mock_clob_client):
        """Verify reconcile continues processing after single trade error."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        # First trade errors, second succeeds
        mock_client_instance.get_order.side_effect = [
            Exception("API Error"),
            {"status": "MATCHED", "size_matched": "100", "original_size": "100"},
        ]
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        trades = [
            Trade(
                id=uuid4(),
                wallet_id=uuid4(),
                market_id=uuid4(),
                token_id="token-1",
                side=TradeSide.YES,
                order_type=OrderSide.BUY,
                quantity=Decimal("100"),
                limit_price=Decimal("0.65"),
                order_id="order-1",
                status=TradeStatus.OPEN,
                filled_quantity=Decimal("0"),
            ),
            Trade(
                id=uuid4(),
                wallet_id=uuid4(),
                market_id=uuid4(),
                token_id="token-2",
                side=TradeSide.YES,
                order_type=OrderSide.BUY,
                quantity=Decimal("100"),
                limit_price=Decimal("0.70"),
                order_id="order-2",
                status=TradeStatus.OPEN,
                filled_quantity=Decimal("0"),
            ),
        ]
        mock_repository.get_open_trades.return_value = trades

        reconciler = TradeReconciler(config, mock_repository)
        result = reconciler.reconcile()

        # Only second trade updated (first errored)
        assert result == 1
        assert mock_repository.update_trade.call_count == 1


class TestTradeReconcilerReconcileTrade:
    """Test _reconcile_trade() method."""

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_returns_false_without_order_id(self, mock_clob_client):
        """Verify _reconcile_trade returns False without order_id."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id=None,
        )

        result = reconciler._reconcile_trade(trade)
        assert result is False

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_handles_order_not_found(self, mock_clob_client):
        """Verify _reconcile_trade handles order not found by marking cancelled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.side_effect = Exception("Order not found")
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)

        trade_id = uuid4()
        trade = Trade(
            id=trade_id,
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="nonexistent-order",
        )

        result = reconciler._reconcile_trade(trade)
        assert result is True
        mock_repository.update_trade.assert_called_once_with(
            trade_id=trade_id,
            status=TradeStatus.CANCELLED,
        )

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_handles_404_error(self, mock_clob_client):
        """Verify _reconcile_trade handles 404 error by marking cancelled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.side_effect = Exception("404 Not Found")
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)

        trade_id = uuid4()
        trade = Trade(
            id=trade_id,
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="missing-order",
        )

        result = reconciler._reconcile_trade(trade)
        assert result is True
        mock_repository.update_trade.assert_called_once_with(
            trade_id=trade_id,
            status=TradeStatus.CANCELLED,
        )

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_raises_on_other_errors(self, mock_clob_client):
        """Verify _reconcile_trade raises on non-404 errors."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.side_effect = Exception("Server error 500")
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="some-order",
        )

        with pytest.raises(Exception) as exc_info:
            reconciler._reconcile_trade(trade)
        assert "500" in str(exc_info.value)

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_returns_false_on_empty_order_response(self, mock_clob_client):
        """Verify _reconcile_trade returns False when order response is empty."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = None
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="some-order",
        )

        result = reconciler._reconcile_trade(trade)
        assert result is False

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_uses_size_field_as_fallback(self, mock_clob_client):
        """Verify _reconcile_trade uses 'size' field when 'original_size' is missing."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "MATCHED",
            "size_matched": "50",
            "size": "100",  # No original_size, use size instead
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="some-order",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )

        result = reconciler._reconcile_trade(trade)
        assert result is True
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["status"] == TradeStatus.PARTIALLY_FILLED

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_uses_price_field_as_fallback(self, mock_clob_client):
        """Verify _reconcile_trade uses 'price' field when 'average_price' is missing."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "MATCHED",
            "size_matched": "100",
            "original_size": "100",
            "price": "0.65",  # No average_price, use price instead
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="some-order",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )

        result = reconciler._reconcile_trade(trade)
        assert result is True
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["avg_fill_price"] == Decimal("0.65")

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_handles_invalid_avg_price(self, mock_clob_client):
        """Verify _reconcile_trade handles invalid average_price gracefully."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "MATCHED",
            "size_matched": "100",
            "original_size": "100",
            "average_price": "invalid",  # Invalid decimal
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="some-order",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )

        result = reconciler._reconcile_trade(trade)
        assert result is True
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["avg_fill_price"] is None

    @patch("src.db.reconciliation.ClobClient")
    def test_reconcile_trade_detects_fill_change_without_status_change(self, mock_clob_client):
        """Verify _reconcile_trade detects fill quantity change even if status unchanged."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "LIVE",  # Still LIVE/OPEN
            "size_matched": "50",  # But partially filled
            "original_size": "100",
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(private_key="test_key")
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        reconciler = TradeReconciler(config, mock_repository)

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="some-order",
            status=TradeStatus.OPEN,  # Still OPEN
            filled_quantity=Decimal("0"),  # But fill quantity changed
        )

        result = reconciler._reconcile_trade(trade)
        assert result is True
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["filled_quantity"] == Decimal("50")


class TestTradeReconcilerIntegration:
    """Integration tests for TradeReconciler."""

    @patch("src.db.reconciliation.ClobClient")
    def test_full_reconciliation_workflow(self, mock_clob_client):
        """Test complete reconciliation workflow from init to reconcile."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "MATCHED",
            "size_matched": "100",
            "original_size": "100",
            "average_price": "0.64",
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            private_key="test_private_key",
            signature_type=0,
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_repository.update_trade.return_value = MagicMock()

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="clob-order-abc123",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )
        mock_repository.get_open_trades.return_value = [trade]

        # Full workflow
        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is True

        result = reconciler.reconcile()
        assert result == 1

        # Verify the trade was updated
        mock_repository.update_trade.assert_called_once()
        call_kwargs = mock_repository.update_trade.call_args[1]
        assert call_kwargs["status"] == TradeStatus.FILLED
        assert call_kwargs["filled_quantity"] == Decimal("100")
        assert call_kwargs["avg_fill_price"] == Decimal("0.64")

    @patch("src.db.reconciliation.ClobClient")
    def test_reconciliation_with_magic_wallet(self, mock_clob_client):
        """Test reconciliation with signature_type=1 (Magic wallet)."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.get_order.return_value = {
            "status": "LIVE",
            "size_matched": "0",
            "original_size": "100",
        }
        mock_clob_client.return_value = mock_client_instance

        funder_address = "0xfunder1234567890abcdef1234567890abcdef1234"
        config = Config(
            private_key="test_private_key",
            signature_type=1,
            funder_address=funder_address,
        )
        mock_repository = MagicMock()
        mock_repository.is_enabled = True

        trade = Trade(
            id=uuid4(),
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="test-token",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            order_id="magic-order-123",
            status=TradeStatus.OPEN,
            filled_quantity=Decimal("0"),
        )
        mock_repository.get_open_trades.return_value = [trade]

        reconciler = TradeReconciler(config, mock_repository)
        assert reconciler.is_enabled is True

        # Verify ClobClient was initialized with funder
        call_kwargs = mock_clob_client.call_args[1]
        assert call_kwargs["funder"] == funder_address

        result = reconciler.reconcile()
        # Trade unchanged (still LIVE/OPEN with 0 filled)
        assert result == 0
