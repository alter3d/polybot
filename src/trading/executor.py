"""Trade execution for Polymarket opportunities.

This module provides the TradeExecutor class for automatically executing
trades on detected opportunities via the Polymarket CLOB API.
"""

import logging
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.constants import POLYGON

from src.config import Config
from src.market.opportunity_detector import Opportunity
from src.notifications.console import BaseNotifier

logger = logging.getLogger(__name__)

# Fixed limit price for all orders
LIMIT_PRICE = 0.99


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
                LIMIT_PRICE,
            )
        except Exception as e:
            logger.error("Failed to initialize trading client: %s", e)
            self._enabled = False

    def _initialize_client(self) -> None:
        """Initialize and authenticate the CLOB client.

        Creates the client with wallet credentials and derives
        API credentials for order submission.

        Raises:
            Exception: If client initialization or credential derivation fails.
        """
        logger.debug("Initializing authenticated CLOB client")

        self._client = ClobClient(
            host=self._config.clob_host,
            key=self._config.private_key,
            chain_id=POLYGON,
            signature_type=self._config.signature_type,
        )

        # CRITICAL: Must derive and set API credentials before trading
        logger.debug("Deriving API credentials")
        api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(api_creds)

        logger.debug("CLOB client authenticated successfully")

    def _calculate_shares(self, amount_usd: float) -> float:
        """Calculate the number of shares for a given dollar amount.

        Shares are calculated based on the fixed limit price of $0.99.

        Args:
            amount_usd: Dollar amount to invest.

        Returns:
            Number of shares to purchase (e.g., $20 / $0.99 = 20.20 shares).
        """
        return amount_usd / LIMIT_PRICE

    def _get_token_id_for_opportunity(self, opportunity: Opportunity) -> Optional[str]:
        """Extract the token ID from an opportunity.

        The market_id in the opportunity corresponds to the token ID
        needed for order placement.

        Args:
            opportunity: The opportunity containing market information.

        Returns:
            Token ID for order placement, or None if invalid.
        """
        if not opportunity.market_id:
            logger.warning("Opportunity has no market_id")
            return None
        return opportunity.market_id

    def _execute_trade(self, opportunity: Opportunity) -> bool:
        """Execute a trade for the given opportunity.

        Creates and submits a limit order at $0.99 for the configured
        dollar amount.

        Args:
            opportunity: The opportunity to trade on.

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

        shares = self._calculate_shares(self._config.trade_amount_usd)

        logger.info(
            "Executing trade: %s %s @ $%.2f (%.2f shares) for %s",
            "BUY",
            opportunity.side,
            LIMIT_PRICE,
            shares,
            token_id[:40] + "..." if len(token_id) > 40 else token_id,
        )

        try:
            # Create order arguments
            order_args = OrderArgs(
                token_id=token_id,
                price=LIMIT_PRICE,
                size=shares,
                side="BUY",
            )

            # Create signed order
            signed_order = self._client.create_order(order_args)

            # Submit order as Good-Til-Cancelled
            response = self._client.post_order(signed_order, OrderType.GTC)

            logger.info(
                "Order submitted successfully: %s",
                response if response else "no response data",
            )
            return True

        except Exception as e:
            error_msg = str(e).lower()

            # Handle common error cases with user-friendly messages
            if "insufficient" in error_msg or "balance" in error_msg:
                logger.error(
                    "Trade failed - insufficient balance. "
                    "Please deposit funds to your Polymarket wallet."
                )
            elif "allowance" in error_msg:
                logger.error(
                    "Trade failed - token allowance required. "
                    "Please approve token spending on Polymarket first."
                )
            elif "timeout" in error_msg or "network" in error_msg:
                logger.error("Trade failed - network error: %s", e)
                # Could implement retry logic here
            else:
                logger.error("Trade execution failed: %s", e)

            return False

    def notify(self, opportunity: Opportunity) -> bool:
        """Execute a trade for a detected opportunity.

        Implements the BaseNotifier interface. When trading is enabled,
        submits a limit order for the opportunity. Always returns gracefully
        to allow monitoring to continue.

        Args:
            opportunity: The opportunity to trade on.

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
            return self._execute_trade(opportunity)
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
