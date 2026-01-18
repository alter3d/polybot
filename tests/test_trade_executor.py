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

    def test_init_disabled_when_zero_base_shares(self):
        """Verify executor is disabled when trade_base_shares is zero."""
        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=0.0,
        )
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_disabled_when_negative_base_shares(self):
        """Verify executor is disabled when trade_base_shares is negative."""
        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=-10.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
        )
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    def test_init_disabled_when_signature_type_1_without_funder(self):
        """Verify executor is disabled when signature_type=1 but funder_address is empty."""
        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
            signature_type=1,
            funder_address="",  # Missing funder address
        )
        executor = TradeExecutor(config)
        assert not executor.is_enabled

    @patch("src.trading.executor.ClobClient")
    def test_init_enabled_with_signature_type_1_and_funder(self, mock_clob_client):
        """Verify executor is enabled when signature_type=1 with funder_address."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_private_key",
            trade_base_shares=3.0,
            signature_type=1,
            funder_address="0xfunder1234567890abcdef1234567890abcdef1234",
        )
        executor = TradeExecutor(config)
        assert executor.is_enabled

    @patch("src.trading.executor.ClobClient")
    def test_init_passes_funder_to_clob_client_for_signature_type_1(self, mock_clob_client):
        """Verify funder parameter is passed to ClobClient when signature_type=1."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        funder_address = "0xfunder1234567890abcdef1234567890abcdef1234"
        config = Config(
            auto_trade_enabled=True,
            private_key="test_private_key",
            trade_base_shares=3.0,
            signature_type=1,
            funder_address=funder_address,
        )
        TradeExecutor(config)

        # Verify ClobClient was called with funder parameter
        mock_clob_client.assert_called_once()
        call_kwargs = mock_clob_client.call_args[1]
        assert "funder" in call_kwargs
        assert call_kwargs["funder"] == funder_address

    @patch("src.trading.executor.ClobClient")
    def test_init_no_funder_for_signature_type_0(self, mock_clob_client):
        """Verify funder parameter is NOT passed for signature_type=0 (EOA)."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_private_key",
            trade_base_shares=3.0,
            signature_type=0,  # EOA wallet type
            funder_address="0xsome_address",  # Should be ignored
        )
        TradeExecutor(config)

        # Verify ClobClient was called WITHOUT funder parameter
        mock_clob_client.assert_called_once()
        call_kwargs = mock_clob_client.call_args[1]
        assert "funder" not in call_kwargs

    def test_init_stores_config(self):
        """Verify executor stores the configuration."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)
        assert executor._config is config


class TestTradeExecutorShareCalculation:
    """Test share quantity calculation with base shares and multiplier."""

    def test_calculate_shares_default_multiplier(self):
        """Verify default multiplier of 1.0 returns base shares unchanged."""
        config = Config(auto_trade_enabled=False, trade_base_shares=3.0)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares()
        assert shares == 3.0

    def test_calculate_shares_with_multiplier_1(self):
        """Verify multiplier=1.0 returns base shares."""
        config = Config(auto_trade_enabled=False, trade_base_shares=5.0)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares(multiplier=1.0)
        assert shares == 5.0

    def test_calculate_shares_with_multiplier_2(self):
        """Verify multiplier=2.0 doubles the base shares."""
        config = Config(auto_trade_enabled=False, trade_base_shares=3.0)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares(multiplier=2.0)
        assert shares == 6.0

    def test_calculate_shares_with_fractional_multiplier(self):
        """Verify fractional multiplier (1.5x) scales correctly."""
        config = Config(auto_trade_enabled=False, trade_base_shares=4.0)
        executor = TradeExecutor(config)
        shares = executor._calculate_shares(multiplier=1.5)
        assert shares == 6.0

    def test_default_limit_buy_price_is_ninety_cents(self):
        """Verify default config.limit_buy_price is $0.90."""
        config = Config(auto_trade_enabled=False)
        assert config.limit_buy_price == 0.90

    def test_calculate_shares_rounds_to_two_decimal_places(self):
        """Verify shares are rounded to 2 decimals to match exchange precision.

        This is critical for correct fill detection. The exchange rounds share
        quantities, so if we store unrounded values, fills will appear partial
        when they're actually complete.

        Example: 3.333 base shares * 1.0 = 3.333... raw, but exchange fills 3.33.
        Without rounding, filled_quantity(3.33) < quantity(3.333) = partial.
        """
        config = Config(auto_trade_enabled=False, trade_base_shares=3.333)
        executor = TradeExecutor(config)

        # This tests the rounding behavior: 3.333 * 1.0 = 3.333... rounds to 3.33
        shares = executor._calculate_shares(multiplier=1.0)
        assert shares == 3.33  # Must be exactly 3.33, not 3.333...

        # Verify no extra precision by checking string representation
        assert str(shares) == "3.33"

    def test_calculate_shares_with_multiplier_rounding(self):
        """Verify multiplier calculations are rounded correctly."""
        config = Config(auto_trade_enabled=False, trade_base_shares=3.0)
        executor = TradeExecutor(config)

        # 3.0 * 1.5 = 4.5 - no rounding needed
        shares = executor._calculate_shares(multiplier=1.5)
        assert shares == 4.5

        # Test case that requires rounding: 3.0 * 1.111 = 3.333
        config2 = Config(auto_trade_enabled=False, trade_base_shares=3.0)
        executor2 = TradeExecutor(config2)
        shares2 = executor2._calculate_shares(multiplier=1.111)
        assert shares2 == 3.33


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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
        assert call_kwargs["price"] == config.limit_buy_price
        # 3.0 base shares * 1.0 multiplier = 3.0 shares
        assert call_kwargs["size"] == 3.0
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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
            trade_base_shares=3.0,
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


class TestTradeExecutorMultiplierAppliedSizing:
    """Test multiplier-applied trade sizing using base shares."""

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_notify_with_default_multiplier_uses_base_shares(
        self, mock_order_args, mock_clob_client
    ):
        """Verify notify with default multiplier uses base shares unchanged."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
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
        # Call notify without explicit multiplier (defaults to 1.0)
        result = executor.notify(opportunity)
        assert result is True

        # Verify OrderArgs was called with base shares (3.0 * 1.0 = 3.0)
        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        expected_shares = config.trade_base_shares * 1.0
        assert call_kwargs["size"] == expected_shares

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_notify_with_multiplier_1_uses_base_shares(
        self, mock_order_args, mock_clob_client
    ):
        """Verify notify with explicit multiplier=1.0 uses base shares."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
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
        result = executor.notify(opportunity, multiplier=1.0)
        assert result is True

        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        # 3.0 base shares * 1.0 multiplier = 3.0 shares
        expected_shares = config.trade_base_shares * 1.0
        assert call_kwargs["size"] == expected_shares

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_notify_with_multiplier_2_doubles_shares(
        self, mock_order_args, mock_clob_client
    ):
        """Verify notify with multiplier=2.0 doubles the base shares."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
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
        result = executor.notify(opportunity, multiplier=2.0)
        assert result is True

        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        # 3.0 base shares * 2.0 multiplier = 6.0 shares
        expected_shares = config.trade_base_shares * 2.0
        assert call_kwargs["size"] == expected_shares

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_notify_with_multiplier_3_triples_shares(
        self, mock_order_args, mock_clob_client
    ):
        """Verify notify with multiplier=3.0 triples the base shares."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=5.0,
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
        result = executor.notify(opportunity, multiplier=3.0)
        assert result is True

        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        # 5.0 base shares * 3.0 multiplier = 15.0 shares
        expected_shares = config.trade_base_shares * 3.0
        assert call_kwargs["size"] == expected_shares

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_notify_with_fractional_multiplier(self, mock_order_args, mock_clob_client):
        """Verify notify with fractional multiplier (e.g., 1.5x) scales correctly."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=4.0,
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
        result = executor.notify(opportunity, multiplier=1.5)
        assert result is True

        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        # 4.0 base shares * 1.5 multiplier = 6.0 shares
        expected_shares = config.trade_base_shares * 1.5
        assert call_kwargs["size"] == expected_shares

    @patch("src.trading.executor.ClobClient")
    def test_notify_multiplier_with_disabled_trading_returns_true(self, mock_clob_client):
        """Verify notify with multiplier returns True when trading is disabled."""
        config = Config(auto_trade_enabled=False)
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        # Multiplier should be ignored when trading is disabled
        result = executor.notify(opportunity, multiplier=5.0)
        assert result is True

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_multiplier_applied_to_different_base_shares(
        self, mock_order_args, mock_clob_client
    ):
        """Verify multiplier works correctly with various base share amounts."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        # Test with 5 base shares and 2x multiplier
        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=5.0,
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
        result = executor.notify(opportunity, multiplier=2.0)
        assert result is True

        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        # 5.0 base shares * 2.0 multiplier = 10.0 shares
        expected_shares = config.trade_base_shares * 2.0
        assert call_kwargs["size"] == expected_shares

    @patch("src.trading.executor.ClobClient")
    @patch("src.trading.executor.OrderArgs")
    def test_multiplier_combined_with_neg_risk_market(
        self, mock_order_args, mock_clob_client
    ):
        """Verify multiplier works correctly with negative risk markets."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {"orderID": "12345"}
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=6.0,
        )
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
            neg_risk=True,
        )
        result = executor.notify(opportunity, multiplier=2.0)
        assert result is True

        mock_order_args.assert_called_once()
        call_kwargs = mock_order_args.call_args[1]
        # 6.0 base shares * 2.0 multiplier = 12.0 shares
        expected_shares = config.trade_base_shares * 2.0
        assert call_kwargs["size"] == expected_shares


class TestTradeExecutorImmediateFillHandling:
    """Test immediate fill handling when CLOB response contains match data."""

    @patch("src.trading.executor.ClobClient")
    def test_immediate_match_sets_filled_status(self, mock_clob_client):
        """Verify trade record created with FILLED status when order matches immediately."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.get_address.return_value = "0x1234567890abcdef"
        # Response indicates immediate match with full fill
        # Order size = 3.0 base shares * 1.0 multiplier = 3.0 shares
        # takingAmount must be >= order size for FILLED status
        mock_client_instance.post_order.return_value = {
            "orderID": "0x1696f07adc0bc4342ea26b8ce0b3bb552fab2be255d5cc66c31f6b2a1463d186",
            "status": "matched",
            "takingAmount": "3.0",  # Matches order size for FILLED status
            "makingAmount": "2.70",  # 3.0 shares * $0.90 limit = $2.70
            "success": True,
        }
        mock_clob_client.return_value = mock_client_instance

        # Create a mock repository
        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_wallet = MagicMock()
        mock_wallet.id = "wallet-uuid"
        mock_repository.get_or_create_wallet.return_value = mock_wallet
        mock_market = MagicMock()
        mock_market.id = "market-uuid"
        mock_repository.get_or_create_market.return_value = mock_market
        mock_repository.create_trade.return_value = MagicMock(id="trade-uuid")

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
        )
        executor = TradeExecutor(config, repository=mock_repository)

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

        # Verify create_trade was called with correct values
        mock_repository.create_trade.assert_called_once()
        trade_arg = mock_repository.create_trade.call_args[0][0]

        # Import TradeStatus to check the status
        from src.db import TradeStatus
        from decimal import Decimal

        assert trade_arg.status == TradeStatus.FILLED
        assert trade_arg.filled_quantity == Decimal("3.0")
        # avg_fill_price = makingAmount / takingAmount = 2.70 / 3.0 = 0.9
        assert trade_arg.avg_fill_price is not None
        assert abs(trade_arg.avg_fill_price - Decimal("0.9")) < Decimal("0.01")
        assert trade_arg.filled_at is not None

    @patch("src.trading.executor.ClobClient")
    def test_immediate_partial_match_sets_partially_filled_status(self, mock_clob_client):
        """Verify trade record created with PARTIALLY_FILLED when partial match."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.get_address.return_value = "0x1234567890abcdef"
        # Response indicates partial match (1.5 filled out of 3.0 ordered)
        mock_client_instance.post_order.return_value = {
            "orderID": "0xabc123",
            "status": "matched",
            "takingAmount": "1.5",
            "makingAmount": "1.35",
            "success": True,
        }
        mock_clob_client.return_value = mock_client_instance

        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_wallet = MagicMock()
        mock_wallet.id = "wallet-uuid"
        mock_repository.get_or_create_wallet.return_value = mock_wallet
        mock_market = MagicMock()
        mock_market.id = "market-uuid"
        mock_repository.get_or_create_market.return_value = mock_market
        mock_repository.create_trade.return_value = MagicMock(id="trade-uuid")

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
        )
        executor = TradeExecutor(config, repository=mock_repository)

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

        mock_repository.create_trade.assert_called_once()
        trade_arg = mock_repository.create_trade.call_args[0][0]

        from src.db import TradeStatus
        from decimal import Decimal

        assert trade_arg.status == TradeStatus.PARTIALLY_FILLED
        assert trade_arg.filled_quantity == Decimal("1.5")

    @patch("src.trading.executor.ClobClient")
    def test_no_match_creates_open_trade(self, mock_clob_client):
        """Verify trade record created with OPEN status when not matched."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.get_address.return_value = "0x1234567890abcdef"
        # Response indicates order is live (not matched)
        mock_client_instance.post_order.return_value = {
            "orderID": "0xabc123",
            "status": "live",
            "success": True,
        }
        mock_clob_client.return_value = mock_client_instance

        mock_repository = MagicMock()
        mock_repository.is_enabled = True
        mock_wallet = MagicMock()
        mock_wallet.id = "wallet-uuid"
        mock_repository.get_or_create_wallet.return_value = mock_wallet
        mock_market = MagicMock()
        mock_market.id = "market-uuid"
        mock_repository.get_or_create_market.return_value = mock_market
        mock_repository.create_trade.return_value = MagicMock(id="trade-uuid")

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
        )
        executor = TradeExecutor(config, repository=mock_repository)

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

        mock_repository.create_trade.assert_called_once()
        trade_arg = mock_repository.create_trade.call_args[0][0]

        from src.db import TradeStatus
        from decimal import Decimal

        assert trade_arg.status == TradeStatus.OPEN
        assert trade_arg.filled_quantity == Decimal("0")
        assert trade_arg.avg_fill_price is None
        assert trade_arg.filled_at is None

    @patch("src.trading.executor.ClobClient")
    def test_no_repository_skips_trade_record_creation(self, mock_clob_client):
        """Verify no errors when repository is not provided."""
        mock_client_instance = MagicMock()
        mock_client_instance.create_or_derive_api_creds.return_value = {"key": "value"}
        mock_client_instance.create_order.return_value = MagicMock()
        mock_client_instance.post_order.return_value = {
            "orderID": "0xabc123",
            "status": "matched",
            "takingAmount": "3.0",
            "makingAmount": "2.70",
            "success": True,
        }
        mock_clob_client.return_value = mock_client_instance

        config = Config(
            auto_trade_enabled=True,
            private_key="test_key",
            trade_base_shares=3.0,
        )
        # No repository provided
        executor = TradeExecutor(config)

        opportunity = Opportunity(
            market_id="condition-12345",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
            token_id="test-clob-token-id",
        )
        # Should not raise any errors
        result = executor.notify(opportunity)
        assert result is True
