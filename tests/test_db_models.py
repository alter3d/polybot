"""Tests for database models and enums.

Tests the database model dataclasses (Wallet, Market, Trade) and enum types
(TradeStatus, TradeSide, OrderSide) including instantiation, field types,
default values, and string representations.
"""

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from src.db import OrderSide, TradeSide, TradeStatus
from src.db.models import Market, Trade, Wallet


class TestTradeStatusEnum:
    """Test TradeStatus enum."""

    def test_trade_status_open(self):
        """Verify OPEN status value."""
        assert TradeStatus.OPEN.value == "open"

    def test_trade_status_filled(self):
        """Verify FILLED status value."""
        assert TradeStatus.FILLED.value == "filled"

    def test_trade_status_partially_filled(self):
        """Verify PARTIALLY_FILLED status value."""
        assert TradeStatus.PARTIALLY_FILLED.value == "partially_filled"

    def test_trade_status_cancelled(self):
        """Verify CANCELLED status value."""
        assert TradeStatus.CANCELLED.value == "cancelled"

    def test_trade_status_closed(self):
        """Verify CLOSED status value."""
        assert TradeStatus.CLOSED.value == "closed"

    def test_trade_status_is_string_enum(self):
        """Verify TradeStatus inherits from str."""
        assert isinstance(TradeStatus.OPEN, str)
        assert TradeStatus.OPEN == "open"

    def test_trade_status_all_values(self):
        """Verify all expected status values exist."""
        expected = {"open", "filled", "partially_filled", "cancelled", "closed"}
        actual = {status.value for status in TradeStatus}
        assert actual == expected


class TestTradeSideEnum:
    """Test TradeSide enum."""

    def test_trade_side_yes(self):
        """Verify YES side value."""
        assert TradeSide.YES.value == "YES"

    def test_trade_side_no(self):
        """Verify NO side value."""
        assert TradeSide.NO.value == "NO"

    def test_trade_side_is_string_enum(self):
        """Verify TradeSide inherits from str."""
        assert isinstance(TradeSide.YES, str)
        assert TradeSide.YES == "YES"

    def test_trade_side_all_values(self):
        """Verify all expected side values exist."""
        expected = {"YES", "NO"}
        actual = {side.value for side in TradeSide}
        assert actual == expected


class TestOrderSideEnum:
    """Test OrderSide enum."""

    def test_order_side_buy(self):
        """Verify BUY side value."""
        assert OrderSide.BUY.value == "BUY"

    def test_order_side_sell(self):
        """Verify SELL side value."""
        assert OrderSide.SELL.value == "SELL"

    def test_order_side_is_string_enum(self):
        """Verify OrderSide inherits from str."""
        assert isinstance(OrderSide.BUY, str)
        assert OrderSide.BUY == "BUY"

    def test_order_side_all_values(self):
        """Verify all expected order side values exist."""
        expected = {"BUY", "SELL"}
        actual = {side.value for side in OrderSide}
        assert actual == expected


class TestWalletDataclass:
    """Test Wallet dataclass."""

    def test_wallet_creation_minimal(self):
        """Verify Wallet can be created with only required fields."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        assert wallet.address == "0x1234567890123456789012345678901234567890"

    def test_wallet_creation_all_fields(self):
        """Verify Wallet can be created with all fields."""
        wallet_id = uuid4()
        now = datetime(2024, 1, 15, 10, 0, 0)
        wallet = Wallet(
            id=wallet_id,
            address="0xabcdef1234567890abcdef1234567890abcdef12",
            name="Trading Wallet",
            signature_type=1,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        assert wallet.id == wallet_id
        assert wallet.address == "0xabcdef1234567890abcdef1234567890abcdef12"
        assert wallet.name == "Trading Wallet"
        assert wallet.signature_type == 1
        assert wallet.is_active is True
        assert wallet.created_at == now
        assert wallet.updated_at == now

    def test_wallet_default_id_none(self):
        """Verify Wallet id defaults to None."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        assert wallet.id is None

    def test_wallet_default_name_none(self):
        """Verify Wallet name defaults to None."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        assert wallet.name is None

    def test_wallet_default_signature_type_zero(self):
        """Verify Wallet signature_type defaults to 0 (EOA)."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        assert wallet.signature_type == 0

    def test_wallet_default_is_active_true(self):
        """Verify Wallet is_active defaults to True."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        assert wallet.is_active is True

    def test_wallet_default_timestamps_none(self):
        """Verify Wallet timestamps default to None."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        assert wallet.created_at is None
        assert wallet.updated_at is None

    def test_wallet_str_representation(self):
        """Verify Wallet __str__ produces readable output."""
        wallet = Wallet(address="0x1234567890123456789012345678901234567890")
        str_repr = str(wallet)
        assert "Wallet" in str_repr
        assert "0x1234567890123456789012345678901234567890" in str_repr

    def test_wallet_str_with_name(self):
        """Verify Wallet __str__ includes name when present."""
        wallet = Wallet(
            address="0x1234567890123456789012345678901234567890",
            name="Main"
        )
        str_repr = str(wallet)
        assert "Main" in str_repr
        assert "0x1234567890123456789012345678901234567890" in str_repr

    def test_wallet_equality(self):
        """Verify two Wallet instances with same values are equal."""
        wallet1 = Wallet(
            address="0x1234567890123456789012345678901234567890",
            name="Test"
        )
        wallet2 = Wallet(
            address="0x1234567890123456789012345678901234567890",
            name="Test"
        )
        assert wallet1 == wallet2

    def test_wallet_inequality(self):
        """Verify Wallet instances with different addresses are not equal."""
        wallet1 = Wallet(address="0x1234567890123456789012345678901234567890")
        wallet2 = Wallet(address="0xabcdef1234567890abcdef1234567890abcdef12")
        assert wallet1 != wallet2


class TestMarketDataclass:
    """Test Market dataclass."""

    def test_market_creation_minimal(self):
        """Verify Market can be created with only required fields."""
        market = Market(condition_id="abc123")
        assert market.condition_id == "abc123"

    def test_market_creation_all_fields(self):
        """Verify Market can be created with all fields."""
        market_id = uuid4()
        now = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 16, 10, 0, 0)
        market = Market(
            id=market_id,
            condition_id="polymarket-condition-xyz",
            question="Will BTC reach $100k by end of 2024?",
            end_date=end,
            resolved=True,
            winning_side="YES",
            resolution_price=Decimal("1.0"),
            created_at=now,
            updated_at=now,
        )
        assert market.id == market_id
        assert market.condition_id == "polymarket-condition-xyz"
        assert market.question == "Will BTC reach $100k by end of 2024?"
        assert market.end_date == end
        assert market.resolved is True
        assert market.winning_side == "YES"
        assert market.resolution_price == Decimal("1.0")
        assert market.created_at == now
        assert market.updated_at == now

    def test_market_default_id_none(self):
        """Verify Market id defaults to None."""
        market = Market(condition_id="abc123")
        assert market.id is None

    def test_market_default_question_none(self):
        """Verify Market question defaults to None."""
        market = Market(condition_id="abc123")
        assert market.question is None

    def test_market_default_resolved_false(self):
        """Verify Market resolved defaults to False."""
        market = Market(condition_id="abc123")
        assert market.resolved is False

    def test_market_default_winning_side_none(self):
        """Verify Market winning_side defaults to None."""
        market = Market(condition_id="abc123")
        assert market.winning_side is None

    def test_market_default_resolution_price_none(self):
        """Verify Market resolution_price defaults to None."""
        market = Market(condition_id="abc123")
        assert market.resolution_price is None

    def test_market_str_representation(self):
        """Verify Market __str__ produces readable output."""
        market = Market(
            condition_id="abc123",
            question="Test question?"
        )
        str_repr = str(market)
        assert "Market" in str_repr
        assert "Test question?" in str_repr
        assert "open" in str_repr

    def test_market_str_resolved(self):
        """Verify Market __str__ shows resolved status."""
        market = Market(
            condition_id="abc123",
            question="Test",
            resolved=True
        )
        str_repr = str(market)
        assert "resolved" in str_repr

    def test_market_str_long_question_truncated(self):
        """Verify Market __str__ truncates long questions."""
        long_question = "This is a very long question that should be truncated in the string representation"
        market = Market(
            condition_id="abc123",
            question=long_question
        )
        str_repr = str(market)
        assert "..." in str_repr
        # Should contain the first 30 characters
        assert "This is a very long question t" in str_repr

    def test_market_str_no_question(self):
        """Verify Market __str__ handles missing question."""
        market = Market(condition_id="abc123")
        str_repr = str(market)
        assert "No question" in str_repr

    def test_market_equality(self):
        """Verify two Market instances with same values are equal."""
        market1 = Market(condition_id="abc123", question="Test?")
        market2 = Market(condition_id="abc123", question="Test?")
        assert market1 == market2

    def test_market_inequality(self):
        """Verify Market instances with different condition_ids are not equal."""
        market1 = Market(condition_id="abc123")
        market2 = Market(condition_id="xyz789")
        assert market1 != market2


class TestTradeDataclass:
    """Test Trade dataclass."""

    def test_trade_creation_minimal(self):
        """Verify Trade can be created with only required fields."""
        wallet_id = uuid4()
        market_id = uuid4()
        trade = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="12345678901234567890",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.wallet_id == wallet_id
        assert trade.market_id == market_id
        assert trade.token_id == "12345678901234567890"
        assert trade.side == TradeSide.YES
        assert trade.order_type == OrderSide.BUY
        assert trade.quantity == Decimal("100")
        assert trade.limit_price == Decimal("0.65")

    def test_trade_creation_all_fields(self):
        """Verify Trade can be created with all fields."""
        trade_id = uuid4()
        wallet_id = uuid4()
        market_id = uuid4()
        now = datetime(2024, 1, 15, 10, 0, 0)
        trade = Trade(
            id=trade_id,
            wallet_id=wallet_id,
            market_id=market_id,
            order_id="clob-order-123",
            token_id="token-abc-123",
            side=TradeSide.NO,
            order_type=OrderSide.SELL,
            quantity=Decimal("500.00"),
            filled_quantity=Decimal("250.00"),
            limit_price=Decimal("0.45"),
            avg_fill_price=Decimal("0.46"),
            exit_price=Decimal("0.50"),
            cost_basis_usd=Decimal("115.00"),
            proceeds_usd=Decimal("125.00"),
            realized_pnl=Decimal("10.00"),
            neg_risk=True,
            status=TradeStatus.CLOSED,
            created_at=now,
            filled_at=now,
            closed_at=now,
            updated_at=now,
        )
        assert trade.id == trade_id
        assert trade.wallet_id == wallet_id
        assert trade.market_id == market_id
        assert trade.order_id == "clob-order-123"
        assert trade.token_id == "token-abc-123"
        assert trade.side == TradeSide.NO
        assert trade.order_type == OrderSide.SELL
        assert trade.quantity == Decimal("500.00")
        assert trade.filled_quantity == Decimal("250.00")
        assert trade.limit_price == Decimal("0.45")
        assert trade.avg_fill_price == Decimal("0.46")
        assert trade.exit_price == Decimal("0.50")
        assert trade.cost_basis_usd == Decimal("115.00")
        assert trade.proceeds_usd == Decimal("125.00")
        assert trade.realized_pnl == Decimal("10.00")
        assert trade.neg_risk is True
        assert trade.status == TradeStatus.CLOSED
        assert trade.created_at == now
        assert trade.filled_at == now
        assert trade.closed_at == now
        assert trade.updated_at == now

    def test_trade_default_id_none(self):
        """Verify Trade id defaults to None."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.id is None

    def test_trade_default_order_id_none(self):
        """Verify Trade order_id defaults to None."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.order_id is None

    def test_trade_default_filled_quantity_zero(self):
        """Verify Trade filled_quantity defaults to 0."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.filled_quantity == Decimal("0")

    def test_trade_default_neg_risk_false(self):
        """Verify Trade neg_risk defaults to False."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.neg_risk is False

    def test_trade_default_status_open(self):
        """Verify Trade status defaults to OPEN."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.status == TradeStatus.OPEN

    def test_trade_default_timestamps_none(self):
        """Verify Trade timestamps default to None."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade.created_at is None
        assert trade.filled_at is None
        assert trade.closed_at is None
        assert trade.updated_at is None

    def test_trade_str_representation(self):
        """Verify Trade __str__ produces readable output."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.6543"),
        )
        str_repr = str(trade)
        assert "Trade" in str_repr
        assert "BUY" in str_repr
        assert "YES" in str_repr
        assert "0.65" in str_repr  # Formatted to 2 decimal places
        assert "open" in str_repr

    def test_trade_str_sell_no(self):
        """Verify Trade __str__ shows SELL NO correctly."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.NO,
            order_type=OrderSide.SELL,
            quantity=Decimal("100"),
            limit_price=Decimal("0.35"),
            status=TradeStatus.FILLED,
        )
        str_repr = str(trade)
        assert "SELL" in str_repr
        assert "NO" in str_repr
        assert "filled" in str_repr


class TestTradeProperties:
    """Test Trade property methods."""

    def test_is_open_with_open_status(self):
        """Verify is_open returns True for OPEN status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.OPEN,
        )
        assert trade.is_open is True

    def test_is_open_with_partially_filled_status(self):
        """Verify is_open returns True for PARTIALLY_FILLED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.PARTIALLY_FILLED,
        )
        assert trade.is_open is True

    def test_is_open_with_filled_status(self):
        """Verify is_open returns False for FILLED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.FILLED,
        )
        assert trade.is_open is False

    def test_is_open_with_closed_status(self):
        """Verify is_open returns False for CLOSED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.CLOSED,
        )
        assert trade.is_open is False

    def test_is_open_with_cancelled_status(self):
        """Verify is_open returns False for CANCELLED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.CANCELLED,
        )
        assert trade.is_open is False

    def test_is_complete_with_filled_status(self):
        """Verify is_complete returns True for FILLED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.FILLED,
        )
        assert trade.is_complete is True

    def test_is_complete_with_closed_status(self):
        """Verify is_complete returns True for CLOSED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.CLOSED,
        )
        assert trade.is_complete is True

    def test_is_complete_with_open_status(self):
        """Verify is_complete returns False for OPEN status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.OPEN,
        )
        assert trade.is_complete is False

    def test_is_complete_with_partially_filled_status(self):
        """Verify is_complete returns False for PARTIALLY_FILLED status."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            status=TradeStatus.PARTIALLY_FILLED,
        )
        assert trade.is_complete is False

    def test_fill_percentage_zero(self):
        """Verify fill_percentage returns 0 when nothing filled."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            filled_quantity=Decimal("0"),
        )
        assert trade.fill_percentage == Decimal("0")

    def test_fill_percentage_partial(self):
        """Verify fill_percentage calculates partial fill correctly."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            filled_quantity=Decimal("25"),
        )
        assert trade.fill_percentage == Decimal("25")

    def test_fill_percentage_full(self):
        """Verify fill_percentage returns 100 for fully filled orders."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
            filled_quantity=Decimal("100"),
        )
        assert trade.fill_percentage == Decimal("100")

    def test_fill_percentage_zero_quantity(self):
        """Verify fill_percentage handles zero quantity gracefully."""
        trade = Trade(
            wallet_id=uuid4(),
            market_id=uuid4(),
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("0"),
            limit_price=Decimal("0.65"),
        )
        assert trade.fill_percentage == Decimal("0")


class TestTradeEquality:
    """Test Trade equality comparisons."""

    def test_trade_equality(self):
        """Verify two Trade instances with same values are equal."""
        wallet_id = uuid4()
        market_id = uuid4()
        trade1 = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        trade2 = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade1 == trade2

    def test_trade_inequality_side(self):
        """Verify Trade instances with different sides are not equal."""
        wallet_id = uuid4()
        market_id = uuid4()
        trade1 = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        trade2 = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="token123",
            side=TradeSide.NO,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        assert trade1 != trade2

    def test_trade_inequality_price(self):
        """Verify Trade instances with different prices are not equal."""
        wallet_id = uuid4()
        market_id = uuid4()
        trade1 = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.65"),
        )
        trade2 = Trade(
            wallet_id=wallet_id,
            market_id=market_id,
            token_id="token123",
            side=TradeSide.YES,
            order_type=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("0.75"),
        )
        assert trade1 != trade2
