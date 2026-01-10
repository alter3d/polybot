"""Tests for trade executor module.

Tests the TradeExecutor class including initialization, trade execution,
error handling, and BaseNotifier interface implementation.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.config import Config
from src.market.opportunity_detector import Opportunity
from src.notifications.console import BaseNotifier
from src.trading.executor import (
    LIMIT_PRICE,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    APIError,
    AllowanceError,
    InsufficientBalanceError,
    InvalidOrderError,
    NetworkError,
    RateLimitError,
    TradeExecutionError,
    TradeExecutor,
)


class TestTradeExecutorInit:
    """Test TradeExecutor initialization."""

    def test_executor_is_base_notifier(self):
        """Verify TradeExecutor is a subclass of BaseNotifier."""
        assert issubclass(TradeExecutor, BaseNotifier)

    def test_init_disabled_when_auto_trade_false(self):
        """Verify executor is disabled when auto_trade_enabled is false."""
        config = Config(auto_trade_enabled=False, private_key="test_key")
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_disabled_when_no_private_key(self):
        """Verify executor is disabled when private_key is empty."""
        config = Config(auto_trade_enabled=True, private_key="")
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_disabled_when_private_key_none(self):
        """Verify executor is disabled when private_key is None-like."""
        config = Config(auto_trade_enabled=True, private_key="")
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_disabled_when_zero_trade_amount(self):
        """Verify executor is disabled when trade_amount_usd is zero."""
        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=0.0,
        )
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_disabled_when_negative_trade_amount(self):
        """Verify executor is disabled when trade_amount_usd is negative."""
        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=-10.0,
        )
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    @patch("src.trading.executor.ClobClient")
    def test_init_enabled_with_valid_config(self, mock_clob_client):
        """Verify executor is enabled with valid trading config."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_private_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)
        assert executor.is_enabled

    @patch("src.trading.executor.ClobClient")
    def test_init_calls_create_or_derive_api_creds(self, mock_clob_client):
        """Verify CLOB client initializes with API credential derivation."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_private_key",
            trade_amount_usd=20.0,
        )
        TradeExecutor(config)

        mock_client_instance.create_or_derive_api_creds.assert_called_once()
        mock_client_instance.set_api_creds.assert_called_once()

    @patch("src.trading.executor.ClobClient")
    def test_init_handles_client_initialization_error(self, mock_clob_client):
        """Verify executor handles CLOB client initialization errors gracefully."""
        mock_clob_client.side_effect = Exception("Connection failed")

        config = Config(
            auto_trade_enabled=True,
            private_key="test_private_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_stores_config(self):
        """Verify executor stores the configuration."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        assert executor._config is config


class TestTradeExecutorShareCalculation:
    """Test share quantity calculation."""

    def test_calculate_shares_twenty_dollars(self):
        """Verify $20 at $0.99 = 20.20 shares."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares(20.0)
        assert abs(shares - 20.20) < 0.01

    def test_calculate_shares_one_dollar(self):
        """Verify $1 at $0.99 = 1.01 shares."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares(1.0)
        assert abs(shares - 1.01) < 0.01

    def test_calculate_shares_hundred_dollars(self):
        """Verify $100 at $0.99 = 101.01 shares."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares(100.0)
        assert abs(shares - 101.01) < 0.01

    def test_calculate_shares_uses_limit_price(self):
        """Verify share calculation uses the LIMIT_PRICE constant."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        amount = 50.0
        shares = executor._calculate_shares(amount)
        expected = amount / LIMIT_PRICE
        assert shares == expected

    def test_limit_price_is_ninety_nine_cents(self):
        """Verify LIMIT_PRICE constant is $0.99."""
        assert LIMIT_PRICE == 0.99


class TestTradeExecutorNotify:
    """Test notify() method implementation."""

    def test_notify_returns_true_when_disabled(self):
        """Verify notify returns True when trading is disabled."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="test-market",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
        )
        result = executor.notify(opportunity)
        assert result is True

    @patch("src.trading.executor.ClobClient")
    def test_notify_executes_trade_when_enabled(self, mock_clob_client):
        """Verify notify executes trade when trading is enabled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        result = executor.notify(opportunity)
        assert result is True
        mock_client_instance.create_order.assert_called_once()
        mock_client_instance.post_order.assert_called_once()

    @patch("src.trading.executor.ClobClient")
    def test_notify_returns_false_on_trade_error(self, mock_clob_client):
        """Verify notify returns False when trade fails."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.side_effect = Exception("Order failed")
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        result = executor.notify(opportunity)
        assert result is False

    def test_notify_skips_empty_token_id_and_market_id(self):
        """Verify notify skips opportunities with no token_id and empty market_id."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        # Enable manually to test this path
        executor._enabled = True
        executor._client = MagicMock()

        opportunity = Opportunity(
            market_id="",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id=None,
        )
        result = executor.notify(opportunity)
        # No valid token_id or market_id, trade should be skipped
        assert result is False


class TestTradeExecutorNotifyBatch:
    """Test notify_batch() method implementation."""

    def test_notify_batch_empty_list(self):
        """Verify notify_batch returns 0 for empty list."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        result = executor.notify_batch([])
        assert result == 0

    def test_notify_batch_disabled_returns_count(self):
        """Verify notify_batch returns count when disabled (no action needed = success)."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunities = [
            Opportunity("m1", "YES", 0.75, datetime.now(), "last_trade"),
            Opportunity("m2", "NO", 0.80, datetime.now(), "last_trade"),
            Opportunity("m3", "YES", 0.85, datetime.now(), "last_trade"),
        ]
        result = executor.notify_batch(opportunities)
        assert result == 3

    @patch("src.trading.executor.ClobClient")
    def test_notify_batch_processes_all_opportunities(self, mock_clob_client):
        """Verify notify_batch processes each opportunity."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunities = [
            Opportunity("m1", "YES", 0.75, datetime.now(), "last_trade"),
            Opportunity("m2", "YES", 0.80, datetime.now(), "last_trade"),
        ]
        result = executor.notify_batch(opportunities)
        assert result == 2
        assert mock_client_instance.create_order.call_count == 2

    @patch("src.trading.executor.ClobClient")
    def test_notify_batch_partial_success(self, mock_clob_client):
        """Verify notify_batch returns count of successful trades."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        # First call succeeds, second fails
        mock_client_instance.create_order.side_effect = [
            MagicMock(),
            Exception("Order failed"),
        ]
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunities = [
            Opportunity("m1", "YES", 0.75, datetime.now(), "last_trade"),
            Opportunity("m2", "YES", 0.80, datetime.now(), "last_trade"),
        ]
        result = executor.notify_batch(opportunities)
        assert result == 1


class TestTradeExecutorErrorCategorization:
    """Test error categorization logic."""

    def test_categorize_insufficient_balance_error(self):
        """Verify insufficient balance errors are categorized correctly."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Insufficient balance in wallet")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, InsufficientBalanceError)

    def test_categorize_allowance_error(self):
        """Verify allowance errors are categorized correctly."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Token not approved for trading")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, AllowanceError)

    def test_categorize_rate_limit_error(self):
        """Verify rate limit errors are categorized correctly."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Rate limit exceeded - too many requests")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, RateLimitError)

    def test_categorize_network_error_timeout(self):
        """Verify timeout errors are categorized as NetworkError."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Connection timeout")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, NetworkError)

    def test_categorize_network_error_connection(self):
        """Verify connection errors are categorized as NetworkError."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Connection refused")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, NetworkError)

    def test_categorize_invalid_order_error(self):
        """Verify invalid parameter errors are categorized correctly."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Invalid order parameters")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, InvalidOrderError)

    def test_categorize_api_error_with_status_code(self):
        """Verify API errors with status codes are categorized correctly."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Server error 500")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, APIError)
        assert categorized.status_code == 500

    def test_categorize_generic_error(self):
        """Verify unknown errors are categorized as APIError."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = Exception("Some unknown error occurred")
        categorized = executor._categorize_error(error)
        assert isinstance(categorized, APIError)


class TestTradeExecutorRetryLogic:
    """Test retry logic for transient errors."""

    def test_is_retryable_network_error(self):
        """Verify NetworkError is retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = NetworkError("Connection timeout")
        assert executor._is_retryable_error(error) is True

    def test_is_retryable_rate_limit_error(self):
        """Verify RateLimitError is retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = RateLimitError("Too many requests")
        assert executor._is_retryable_error(error) is True

    def test_is_not_retryable_insufficient_balance(self):
        """Verify InsufficientBalanceError is not retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = InsufficientBalanceError("No funds")
        assert executor._is_retryable_error(error) is False

    def test_is_not_retryable_allowance_error(self):
        """Verify AllowanceError is not retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = AllowanceError("Not approved")
        assert executor._is_retryable_error(error) is False

    def test_is_not_retryable_invalid_order(self):
        """Verify InvalidOrderError is not retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = InvalidOrderError("Bad parameters")
        assert executor._is_retryable_error(error) is False

    def test_is_retryable_api_error_5xx(self):
        """Verify 5xx APIError is retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = APIError("Server error", status_code=500)
        assert executor._is_retryable_error(error) is True

        error = APIError("Bad gateway", status_code=502)
        assert executor._is_retryable_error(error) is True

    def test_is_not_retryable_api_error_4xx(self):
        """Verify 4xx APIError is not retryable."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        error = APIError("Not found", status_code=404)
        assert executor._is_retryable_error(error) is False

        error = APIError("Unauthorized", status_code=401)
        assert executor._is_retryable_error(error) is False

    def test_max_retries_constant(self):
        """Verify MAX_RETRIES is set to 1."""
        assert MAX_RETRIES == 1

    def test_retry_delay_constant(self):
        """Verify RETRY_DELAY_SECONDS is set to 1.0."""
        assert RETRY_DELAY_SECONDS == 1.0


class TestTradeExecutorExceptionHierarchy:
    """Test exception class hierarchy."""

    def test_trade_execution_error_is_exception(self):
        """Verify TradeExecutionError is an Exception."""
        assert issubclass(TradeExecutionError, Exception)

    def test_insufficient_balance_error_hierarchy(self):
        """Verify InsufficientBalanceError extends TradeExecutionError."""
        assert issubclass(InsufficientBalanceError, TradeExecutionError)

    def test_allowance_error_hierarchy(self):
        """Verify AllowanceError extends TradeExecutionError."""
        assert issubclass(AllowanceError, TradeExecutionError)

    def test_network_error_hierarchy(self):
        """Verify NetworkError extends TradeExecutionError."""
        assert issubclass(NetworkError, TradeExecutionError)

    def test_rate_limit_error_hierarchy(self):
        """Verify RateLimitError extends TradeExecutionError."""
        assert issubclass(RateLimitError, TradeExecutionError)

    def test_invalid_order_error_hierarchy(self):
        """Verify InvalidOrderError extends TradeExecutionError."""
        assert issubclass(InvalidOrderError, TradeExecutionError)

    def test_api_error_hierarchy(self):
        """Verify APIError extends TradeExecutionError."""
        assert issubclass(APIError, TradeExecutionError)

    def test_api_error_stores_status_code(self):
        """Verify APIError stores optional status code."""
        error = APIError("Server error", status_code=500)
        assert error.status_code == 500

    def test_api_error_status_code_optional(self):
        """Verify APIError status_code defaults to None."""
        error = APIError("Some error")
        assert error.status_code is None


class TestTradeExecutorTokenId:
    """Test token ID extraction from opportunities."""

    def test_get_token_id_prefers_token_id_field(self):
        """Verify token_id field is preferred over market_id."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="26649923323844112890821751864994084620998105018839072358340634246989649300706",
        )
        token_id = executor._get_token_id_for_opportunity(opportunity)
        assert token_id == "26649923323844112890821751864994084620998105018839072358340634246989649300706"

    def test_get_token_id_falls_back_to_market_id(self):
        """Verify market_id is used as fallback when token_id is None."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="0x123abc456def",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id=None,
        )
        token_id = executor._get_token_id_for_opportunity(opportunity)
        assert token_id == "0x123abc456def"

    def test_get_token_id_empty_market_id_no_token_id(self):
        """Verify None returned when both market_id and token_id are empty/None."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id=None,
        )
        token_id = executor._get_token_id_for_opportunity(opportunity)
        assert token_id is None

    def test_get_token_id_long_token_id(self):
        """Verify long token IDs are returned correctly."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        long_id = "a" * 100
        opportunity = Opportunity(
            market_id="short-market-id",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id=long_id,
        )
        token_id = executor._get_token_id_for_opportunity(opportunity)
        assert token_id == long_id

    def test_get_token_id_empty_token_id_uses_market_id(self):
        """Verify empty string token_id falls back to market_id."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="fallback-market-id",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="",
        )
        token_id = executor._get_token_id_for_opportunity(opportunity)
        # Empty string is falsy, so should fall back to market_id
        assert token_id == "fallback-market-id"


class TestTradeExecutorIsEnabledProperty:
    """Test is_enabled property."""

    def test_is_enabled_property_true(self):
        """Verify is_enabled property returns True when enabled."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        executor._enabled = True
        assert executor.is_enabled is True

    def test_is_enabled_property_false(self):
        """Verify is_enabled property returns False when disabled."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        assert executor.is_enabled is False


class TestTradeExecutorIntegration:
    """Integration tests for TradeExecutor with mocked CLOB client."""

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.time.sleep")
    def test_trade_with_retry_on_network_error(self, mock_sleep, mock_clob_client):
        """Verify trade retries on network error then succeeds."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        # First call fails with network error, second succeeds
        mock_client_instance.create_order.side_effect = [
            Exception("Connection timeout"),
            MagicMock(),
        ]
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        result = executor.notify(opportunity)
        assert result is True
        assert mock_client_instance.create_order.call_count == 2
        mock_sleep.assert_called_once_with(RETRY_DELAY_SECONDS)

    @patch("src.trading.executor.ClobClient")
    def test_trade_fails_after_max_retries(self, mock_clob_client):
        """Verify trade fails after exhausting retries."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        # All calls fail with network error
        mock_client_instance.create_order.side_effect = Exception("Connection timeout")
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        with patch("src.trading.executor.time.sleep"):
            opportunity = Opportunity(
                market_id="condition-12345",
                side="YES",
                price=0.80,
                detected_at=datetime.now(),
                source="last_trade",
                token_id="test-clob-token-id",
            )
            result = executor.notify(opportunity)
            assert result is False
            # Should try initial + MAX_RETRIES attempts
            assert mock_client_instance.create_order.call_count == MAX_RETRIES + 1

    @patch("src.trading.executor.ClobClient")
    def test_trade_no_retry_on_insufficient_balance(self, mock_clob_client):
        """Verify no retry on insufficient balance error."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.side_effect = Exception("Insufficient balance")
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        result = executor.notify(opportunity)
        assert result is False
        # Should only try once (no retries for non-transient errors)
        assert mock_client_instance.create_order.call_count == 1

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_order_created_with_correct_parameters(self, mock_order_args, mock_clob_client):
        """Verify order is created with correct parameters using token_id."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        # Use token_id (CLOB token) separate from market_id (condition ID)
        clob_token_id = "26649923323844112890821751864994084620998105018839072358340634246989649300706"
        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id=clob_token_id,
        )
        executor.notify(opportunity)

        # Verify OrderArgs was called with the token_id (not market_id)
        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        assert call_kwargs["token_id"] == clob_token_id
        assert call_kwargs["price"] == LIMIT_PRICE
        assert abs(call_kwargs["size"] - 20.20) < 0.01
        assert call_kwargs["side"] == "BUY"

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderType")
    def test_order_posted_as_gtc(self, mock_order_type, mock_clob_client):
        """Verify order is posted as Good-Til-Cancelled."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        executor.notify(opportunity)

        # Verify post_order was called with GTC order type
        mock_client_instance.post_order.assert_called_once()
        call_args = mock_client_instance.post_order.call_args
        assert call_args[0][1] == mock_order_type.GTC

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.PartialCreateOrderOptions")
    def test_order_created_with_neg_risk_option(self, mock_options, mock_clob_client):
        """Verify order is created with neg_risk option for negative risk markets."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        # Create opportunity for a negative risk market
        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
            neg_risk=True,
        )
        executor.notify(opportunity)

        # Verify PartialCreateOrderOptions was called with neg_risk=True
        mock_options.assert_called_once_with(neg_risk=True)

        # Verify create_order was called with options
        mock_client_instance.create_order.assert_called_once()
        call_args = mock_client_instance.create_order.call_args
        assert len(call_args[0]) == 2  # order_args and options

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.PartialCreateOrderOptions")
    def test_order_created_with_neg_risk_false(self, mock_options, mock_clob_client):
        """Verify order is created with neg_risk=False for non-negative risk markets."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_amount_usd=20.0,
        )
        executor = TradeExecutor(config)

        # Create opportunity for a non-negative risk market (default)
        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
            neg_risk=False,
        )
        executor.notify(opportunity)

        # Verify PartialCreateOrderOptions was called with neg_risk=False
        mock_options.assert_called_once_with(neg_risk=False)
