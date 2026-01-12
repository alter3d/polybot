"""Trade execution for Polymarket opportunities.

This module provides the TradeExecutor class for automatically executing
trades on detected opportunities via the Polymarket CLOB API.
"""

import logging
import time
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.constants import POLYGON

from src.config import Config
from src.market.opportunity_detector import Opportunity
from src.notifications.console import BaseNotifier

logger = logging.getLogger(__name__)


class TradeExecutionError(Exception):
    """Base exception for trade execution errors."""

    pass


class InsufficientBalanceError(TradeExecutionError):
    """Raised when wallet balance is insufficient for trade."""

    pass


class AllowanceError(TradeExecutionError):
    """Raised when token allowance is not set or insufficient."""

    pass


class NetworkError(TradeExecutionError):
    """Raised on network-related failures (timeout, connection issues)."""

    pass


class RateLimitError(TradeExecutionError):
    """Raised when API rate limit is exceeded."""

    pass


class InvalidOrderError(TradeExecutionError):
    """Raised when order parameters are invalid."""

    pass


class APIError(TradeExecutionError):
    """Raised for general API errors with status codes."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        """Initialize API error with optional status code.

        Args:
            message: Error description.
            status_code: HTTP status code if available.
        """
        super().__init__(message)
        self.status_code = status_code


# Retry configuration for transient errors
MAX_RETRIES = 1
RETRY_DELAY_SECONDS = 1.0


class TradeExecutor(BaseNotifier):
    """Trade executor that implements the BaseNotifier interface.

    Executes trades on Polymarket when opportunities are detected.
    Follows the same notification pattern as ConsoleNotifier for
    seamless integration with the monitoring pipeline.

    When enabled, automatically places limit orders at $0.99 for
    the configured dollar amount when opportunities are detected.

    Attributes:
        _config: Application configuration with trading parameters.
        _enabled: Whether trading is enabled (requires auto_trade_enabled and private_key).
        _client: Authenticated CLOB client for order submission.
    """

    def __init__(self, config: Config) -> None:
        """Initialize the trade executor.

        Sets up authenticated CLOB client if trading is enabled and
        private key is provided. Gracefully disables trading if
        configuration is missing or invalid.

        Args:
            config: Application configuration with trading parameters.
        """
        self._config = config
        self._enabled = False
        self._client: Optional[ClobClient] = None

        # Check if trading should be enabled
        if not config.auto_trade_enabled:
            logger.info("Auto-trade is disabled via configuration")
            return

        if not config.private_key:
            logger.warning(
                "Auto-trade enabled but PRIVATE_KEY not set - trading disabled"
            )
            return

        # For signature_type=1 (Magic wallet), funder_address is required
        if config.signature_type == 1 and not config.funder_address:
            logger.warning(
                "SIGNATURE_TYPE=1 requires FUNDER_ADDRESS - trading disabled"
            )
            return

        if config.trade_amount_usd <= 0:
            logger.error(
                "Invalid trade amount: $%.2f - trading disabled",
                config.trade_amount_usd,
            )
            return

        # Initialize authenticated CLOB client
        try:
            self._initialize_client()
            self._enabled = True
            shares = self._calculate_shares(config.trade_amount_usd)
            logger.info(
                "TradeExecutor initialized: $%.2f per trade (%.2f shares @ $%.2f)",
                config.trade_amount_usd,
                shares,
                self._config.limit_price,
            )
        except Exception as e:
            logger.error("Failed to initialize trading client: %s", e)
            self._enabled = False

    def _initialize_client(self) -> None:
        """Initialize and authenticate the CLOB client.

        Creates the client with wallet credentials and derives
        API credentials for order submission.

        When signature_type=1 (Magic wallet), the funder parameter is required
        for proper signature validation.

        Raises:
            Exception: If client initialization or credential derivation fails.
        """
        logger.debug(
            "Initializing authenticated CLOB client (signature_type=%d, funder_address=%s)",
            self._config.signature_type,
            "set" if self._config.funder_address else "not set",
        )

        # Build client kwargs - funder is required for signature_type=1 (Magic wallet)
        client_kwargs = {
            "host": self._config.clob_host,
            "key": self._config.private_key,
            "chain_id": POLYGON,
            "signature_type": self._config.signature_type,
        }

        # Add funder parameter when using Magic wallet (signature_type=1)
        if self._config.signature_type == 1 and self._config.funder_address:
            client_kwargs["funder"] = self._config.funder_address
            logger.debug(
                "Using funder address for Magic wallet: %s",
                self._config.funder_address[:10] + "..." if len(self._config.funder_address) > 10 else self._config.funder_address
            )
        elif self._config.signature_type == 1:
            # signature_type=1 but no funder_address - this should have been caught earlier
            # but log a warning just in case
            logger.warning(
                "SIGNATURE_TYPE=1 (Magic wallet) but FUNDER_ADDRESS is empty - this may cause signature errors"
            )
        elif self._config.funder_address:
            # funder_address is set but signature_type is not 1 - user may have misconfigured
            logger.warning(
                "FUNDER_ADDRESS is configured but SIGNATURE_TYPE=%d (not 1/Magic). "
                "If you're using a Magic wallet, set SIGNATURE_TYPE=1",
                self._config.signature_type,
            )

        self._client = ClobClient(**client_kwargs)

        # CRITICAL: Must derive and set API credentials before trading
        logger.debug("Deriving API credentials")
        api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(api_creds)

        logger.debug("CLOB client authenticated successfully")

    def _calculate_shares(self, amount_usd: float) -> float:
        """Calculate the number of shares for a given dollar amount.

        Shares are calculated based on the configured limit price.

        Args:
            amount_usd: Dollar amount to invest.

        Returns:
            Number of shares to purchase (e.g., $20 / $0.90 = 22.22 shares).
        """
        return amount_usd / self._config.limit_price

    def _categorize_error(self, error: Exception) -> TradeExecutionError:
        """Categorize an exception into a specific TradeExecutionError type.

        Analyzes the error message and type to determine the appropriate
        error category for proper handling and logging.

        Args:
            error: The original exception to categorize.

        Returns:
            A specific TradeExecutionError subclass instance.
        """
        error_msg = str(error).lower()
        error_type = type(error).__name__

        # Check for insufficient balance
        if any(
            keyword in error_msg
            for keyword in ["insufficient", "balance", "not enough", "low balance"]
        ):
            return InsufficientBalanceError(
                "Insufficient balance. Please deposit funds to your Polymarket wallet."
            )

        # Check for allowance issues
        if any(
            keyword in error_msg
            for keyword in ["allowance", "approve", "approval", "not approved"]
        ):
            return AllowanceError(
                "Token allowance required. Please approve token spending on Polymarket first."
            )

        # Check for rate limiting
        if any(
            keyword in error_msg
            for keyword in ["rate limit", "too many requests", "throttl"]
        ) or "429" in str(error):
            return RateLimitError("API rate limit exceeded. Please wait before retrying.")

        # Check for network issues
        if any(
            keyword in error_msg
            for keyword in [
                "timeout",
                "timed out",
                "network",
                "connection",
                "connect",
                "unreachable",
                "dns",
                "socket",
            ]
        ) or error_type in ["TimeoutError", "ConnectionError", "OSError"]:
            return NetworkError(f"Network error: {error}")

        # Check for invalid order parameters
        if any(
            keyword in error_msg
            for keyword in [
                "invalid",
                "bad request",
                "validation",
                "malformed",
                "parameter",
            ]
        ):
            return InvalidOrderError(f"Invalid order parameters: {error}")

        # Check for HTTP status codes in error message
        for status_code in [400, 401, 403, 404, 500, 502, 503, 504]:
            if str(status_code) in str(error):
                return APIError(str(error), status_code=status_code)

        # Default to generic API error
        return APIError(str(error))

    def _is_retryable_error(self, error: TradeExecutionError) -> bool:
        """Determine if an error is transient and should be retried.

        Args:
            error: The categorized error to check.

        Returns:
            True if the error is transient and may succeed on retry.
        """
        # Network errors and rate limits are typically transient
        if isinstance(error, (NetworkError, RateLimitError)):
            return True

        # Some API errors (5xx) are transient
        if isinstance(error, APIError) and error.status_code:
            return error.status_code >= 500

        return False

    def _log_trade_error(
        self, error: TradeExecutionError, token_id: str, attempt: int = 1
    ) -> None:
        """Log a trade error with appropriate severity and context.

        Args:
            error: The categorized trade execution error.
            token_id: The token ID being traded.
            attempt: Current retry attempt number.
        """
        token_display = token_id[:40] + "..." if len(token_id) > 40 else token_id

        if isinstance(error, InsufficientBalanceError):
            logger.error(
                "Trade failed for %s - %s",
                token_display,
                str(error),
            )
        elif isinstance(error, AllowanceError):
            logger.error(
                "Trade failed for %s - %s",
                token_display,
                str(error),
            )
        elif isinstance(error, RateLimitError):
            logger.warning(
                "Trade for %s rate limited (attempt %d/%d): %s",
                token_display,
                attempt,
                MAX_RETRIES + 1,
                str(error),
            )
        elif isinstance(error, NetworkError):
            logger.warning(
                "Trade for %s network error (attempt %d/%d): %s",
                token_display,
                attempt,
                MAX_RETRIES + 1,
                str(error),
            )
        elif isinstance(error, InvalidOrderError):
            logger.error(
                "Trade failed for %s - %s",
                token_display,
                str(error),
            )
        elif isinstance(error, APIError):
            status_info = (
                f" (HTTP {error.status_code})" if error.status_code else ""
            )
            logger.error(
                "Trade failed for %s - API error%s: %s",
                token_display,
                status_info,
                str(error),
            )
        else:
            logger.error(
                "Trade failed for %s: %s",
                token_display,
                str(error),
            )

    def _get_token_id_for_opportunity(self, opportunity: Opportunity) -> Optional[str]:
        """Extract the token ID from an opportunity.

        The token_id field contains the CLOB token ID needed for order
        placement. Falls back to market_id for backward compatibility.

        Args:
            opportunity: The opportunity containing market information.

        Returns:
            Token ID for order placement, or None if invalid.
        """
        # Prefer token_id (CLOB token) over market_id (condition ID)
        token_id = opportunity.token_id
        if token_id:
            return token_id

        # Fallback to market_id for backward compatibility
        if opportunity.market_id:
            logger.warning(
                "Opportunity has no token_id, falling back to market_id: %s",
                opportunity.market_id,
            )
            return opportunity.market_id

        logger.warning("Opportunity has no token_id or market_id")
        return None

    def _execute_trade(self, opportunity: Opportunity, multiplier: float = 1.0) -> bool:
        """Execute a trade for the given opportunity.

        Creates and submits a limit order at $0.99 for the configured
        dollar amount. Implements retry logic for transient errors.

        Args:
            opportunity: The opportunity to trade on.
            multiplier: Position size multiplier for reversal trading (default 1.0).

        Returns:
            True if order was submitted successfully, False otherwise.
        """
        if not self._client:
            logger.error("Cannot execute trade: client not initialized")
            return False

        token_id = self._get_token_id_for_opportunity(opportunity)
        if not token_id:
            logger.warning(
                "Skipping trade: invalid opportunity data - %s", opportunity
            )
            return False

        # Apply multiplier to base trade amount
        effective_amount = self._config.trade_amount_usd * multiplier
        shares = self._calculate_shares(effective_amount)

        # Validate order parameters
        if shares <= 0:
            logger.error(
                "Invalid share quantity: %.4f - trade aborted",
                shares,
            )
            return False

        token_display = token_id[:40] + "..." if len(token_id) > 40 else token_id

        # Log with multiplier info when scaling is applied
        if multiplier > 1.0:
            logger.info(
                "Executing trade: %s %s @ $%.2f (%.2f shares, $%.2f = $%.2f × %.1fx) for %s (neg_risk=%s)",
                "BUY",
                opportunity.side,
                self._config.limit_price,
                shares,
                effective_amount,
                self._config.trade_amount_usd,
                multiplier,
                token_display,
                opportunity.neg_risk,
            )
        else:
            logger.info(
                "Executing trade: %s %s @ $%.2f (%.2f shares) for %s (neg_risk=%s)",
                "BUY",
                opportunity.side,
                self._config.limit_price,
                shares,
                token_display,
                opportunity.neg_risk,
            )

        # Attempt trade with retry logic for transient errors
        last_error: Optional[TradeExecutionError] = None

        for attempt in range(1, MAX_RETRIES + 2):  # +2 because range is exclusive
            try:
                return self._submit_order(token_id, shares, opportunity.neg_risk)

            except Exception as e:
                # Categorize the error for proper handling
                categorized_error = self._categorize_error(e)
                last_error = categorized_error

                # Log the error with context
                self._log_trade_error(categorized_error, token_id, attempt)

                # Check if we should retry
                if self._is_retryable_error(categorized_error) and attempt <= MAX_RETRIES:
                    logger.info(
                        "Retrying trade for %s in %.1f seconds...",
                        token_display,
                        RETRY_DELAY_SECONDS,
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                # Non-retryable error or max retries exceeded
                break

        # All retries exhausted
        if last_error and isinstance(last_error, (NetworkError, RateLimitError)):
            logger.error(
                "Trade for %s failed after %d attempts: %s",
                token_display,
                MAX_RETRIES + 1,
                str(last_error),
            )

        return False

    def _submit_order(self, token_id: str, shares: float, neg_risk: bool = False) -> bool:
        """Submit a limit order to the CLOB API.

        This is the core order submission logic, separated to allow
        for retry handling in _execute_trade.

        Args:
            token_id: The token to trade.
            shares: Number of shares to purchase.
            neg_risk: Whether this is a negative risk market.

        Returns:
            True if order was submitted successfully.

        Raises:
            Exception: Any error from the CLOB client.
        """
        if not self._client:
            raise RuntimeError("CLOB client not initialized")

        # Create order arguments
        order_args = OrderArgs(
            token_id=token_id,
            price=self._config.limit_price,
            size=shares,
            side="BUY",
        )

        # Create order options with neg_risk flag
        # This is CRITICAL for negative risk markets - without it, signature validation fails
        order_options = PartialCreateOrderOptions(neg_risk=neg_risk)

        # Create signed order with options
        signed_order = self._client.create_order(order_args, order_options)

        # Submit order as Good-Til-Cancelled
        response = self._client.post_order(signed_order, OrderType.GTC)

        token_display = token_id[:40] + "..." if len(token_id) > 40 else token_id
        logger.info(
            "Order submitted successfully for %s (neg_risk=%s): %s",
            token_display,
            neg_risk,
            response if response else "no response data",
        )
        return True

    def notify(self, opportunity: Opportunity, multiplier: float = 1.0) -> bool:
        """Execute a trade for a detected opportunity.

        Implements the BaseNotifier interface. When trading is enabled,
        submits a limit order for the opportunity. Always returns gracefully
        to allow monitoring to continue.

        Args:
            opportunity: The opportunity to trade on.
            multiplier: Position size multiplier for reversal trading (default 1.0).

        Returns:
            True if trade was executed successfully, False otherwise.
            Returns True when trading is disabled (no action needed = success).
        """
        if not self._enabled:
            logger.debug(
                "Trading disabled - skipping trade for %s", opportunity.market_id
            )
            # Return True because "no action needed" is not a failure
            return True

        try:
            return self._execute_trade(opportunity, multiplier=multiplier)
        except Exception as e:
            # Catch-all to ensure we never crash the monitoring loop
            logger.error("Unexpected error during trade execution: %s", e)
            return False

    def notify_batch(self, opportunities: list[Opportunity]) -> int:
        """Execute trades for multiple opportunities.

        Implements the BaseNotifier interface for batch processing.
        Executes trades sequentially to avoid overwhelming the API.

        Args:
            opportunities: List of opportunities to trade on.

        Returns:
            Number of trades executed successfully.
        """
        if not opportunities:
            return 0

        if not self._enabled:
            logger.debug(
                "Trading disabled - skipping %d opportunities", len(opportunities)
            )
            # Return count because all "no action needed" = all successful
            return len(opportunities)

        logger.info("Processing batch of %d trading opportunities", len(opportunities))

        successful = 0
        for opportunity in opportunities:
            if self.notify(opportunity):
                successful += 1

        logger.info(
            "Batch trading complete: %d/%d successful", successful, len(opportunities)
        )
        return successful

    @property
    def is_enabled(self) -> bool:
        """Check if trading is enabled.

        Returns:
            True if trading is enabled and client is ready.
        """
        return self._enabled
