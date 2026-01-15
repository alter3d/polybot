"""Startup reconciliation for trade tracking.

This module provides the TradeReconciler class for syncing local database
trade state with the Polymarket CLOB API on application startup.
"""

import logging
from decimal import Decimal, InvalidOperation
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

from src.config import Config
from src.db import TradeStatus
from src.db.repository import TradeRepository

logger = logging.getLogger(__name__)


class ReconciliationError(Exception):
    """Raised when reconciliation fails."""

    pass


def map_clob_status_to_trade_status(
    clob_status: str,
    size_matched: Optional[Decimal] = None,
    original_size: Optional[Decimal] = None,
) -> TradeStatus:
    """Map a CLOB API order status to a TradeStatus enum.

    Args:
        clob_status: Status string from CLOB API (LIVE, MATCHED, CANCELLED).
        size_matched: Amount filled so far (optional, for partial fill detection).
        original_size: Original order size (optional, for partial fill detection).

    Returns:
        Corresponding TradeStatus enum value.
    """
    status_upper = clob_status.upper()

    if status_upper == "LIVE":
        return TradeStatus.OPEN

    if status_upper == "CANCELLED":
        return TradeStatus.CANCELLED

    if status_upper == "MATCHED":
        # Determine if fully or partially filled
        if size_matched is not None and original_size is not None:
            if size_matched >= original_size:
                return TradeStatus.FILLED
            elif size_matched > 0:
                return TradeStatus.PARTIALLY_FILLED
        # Default to FILLED if we can't determine partial fill
        return TradeStatus.FILLED

    # Unknown status - log warning and return OPEN as safe default
    logger.warning("Unknown CLOB status '%s', defaulting to OPEN", clob_status)
    return TradeStatus.OPEN


class TradeReconciler:
    """Reconciler for syncing database state with CLOB API.

    Queries open trades from the database and checks their current status
    via the CLOB API. Updates the database if status has changed, ensuring
    consistency before websocket connections are established.

    This should be run on application startup BEFORE connecting to websockets
    to catch any updates that occurred while the application was offline.

    Attributes:
        _config: Application configuration with API credentials.
        _repository: Database repository for trade operations.
        _enabled: Whether reconciliation is enabled.
        _client: Authenticated CLOB client for API queries.
    """

    def __init__(self, config: Config, repository: TradeRepository) -> None:
        """Initialize the trade reconciler.

        Sets up authenticated CLOB client for API queries if configuration
        allows. Gracefully disables reconciliation if required configuration
        is missing.

        Args:
            config: Application configuration with API credentials.
            repository: Database repository for trade operations.
        """
        self._config = config
        self._repository = repository
        self._enabled = False
        self._client: Optional[ClobClient] = None

        # Reconciliation requires both database and CLOB client
        if not repository.is_enabled:
            logger.info("Database not configured - reconciliation disabled")
            return

        if not config.private_key:
            logger.info(
                "PRIVATE_KEY not configured - reconciliation disabled"
            )
            return

        # For signature_type=1 (Magic wallet), funder_address is required
        if config.signature_type == 1 and not config.funder_address:
            logger.warning(
                "SIGNATURE_TYPE=1 requires FUNDER_ADDRESS - reconciliation disabled"
            )
            return

        # Initialize authenticated CLOB client
        try:
            self._initialize_client()
            self._enabled = True
            logger.info("TradeReconciler initialized")
        except Exception as e:
            logger.error("Failed to initialize reconciler: %s", e)
            self._enabled = False

    def _initialize_client(self) -> None:
        """Initialize and authenticate the CLOB client.

        Creates the client with wallet credentials and derives
        API credentials for order status queries.

        Raises:
            Exception: If client initialization or credential derivation fails.
        """
        logger.debug(
            "Initializing CLOB client for reconciliation (signature_type=%d)",
            self._config.signature_type,
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
                self._config.funder_address[:10] + "..."
                if len(self._config.funder_address) > 10
                else self._config.funder_address,
            )

        self._client = ClobClient(**client_kwargs)

        # CRITICAL: Must derive and set API credentials before API operations
        logger.debug("Deriving API credentials for reconciliation")
        api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(api_creds)

        logger.debug("CLOB client authenticated for reconciliation")

    @property
    def is_enabled(self) -> bool:
        """Check if reconciliation is enabled."""
        return self._enabled

    def reconcile(self) -> int:
        """Reconcile open trades in the database with CLOB API state.

        Queries all open trades from the database, fetches their current
        status from the CLOB API, and updates any trades whose status
        has changed.

        This should be called on application startup BEFORE connecting
        to websockets.

        Returns:
            Number of trades that were updated during reconciliation.
        """
        if not self._enabled:
            logger.debug("Reconciliation disabled, skipping")
            return 0

        if not self._client:
            logger.error("Cannot reconcile: CLOB client not initialized")
            return 0

        # Get all open trades from database
        open_trades = self._repository.get_open_trades()

        if not open_trades:
            logger.info("No open trades to reconcile")
            return 0

        logger.info("Reconciling %d open trades", len(open_trades))

        updated_count = 0
        for trade in open_trades:
            if not trade.order_id:
                logger.warning(
                    "Trade %s has no order_id, skipping reconciliation",
                    trade.id,
                )
                continue

            try:
                updated = self._reconcile_trade(trade)
                if updated:
                    updated_count += 1
            except Exception as e:
                logger.error(
                    "Failed to reconcile trade %s (order_id=%s): %s",
                    trade.id,
                    trade.order_id[:8] + "..." if trade.order_id else "None",
                    e,
                )

        logger.info(
            "Reconciliation complete: %d/%d trades updated",
            updated_count,
            len(open_trades),
        )
        return updated_count

    def _reconcile_trade(self, trade) -> bool:
        """Reconcile a single trade with CLOB API state.

        Args:
            trade: Trade dataclass with order_id to query.

        Returns:
            True if the trade was updated, False otherwise.

        Raises:
            Exception: If CLOB API call fails.
        """
        if not self._client or not trade.order_id:
            return False

        order_display = (
            trade.order_id[:8] + "..."
            if len(trade.order_id) > 8
            else trade.order_id
        )
        logger.debug("Reconciling trade %s (order_id=%s)", trade.id, order_display)

        # Fetch order status from CLOB API
        try:
            order = self._client.get_order(trade.order_id)
        except Exception as e:
            error_msg = str(e).lower()
            # Order not found is expected if cancelled or very old
            if "not found" in error_msg or "404" in str(e):
                logger.warning(
                    "Order %s not found in CLOB API - may have been cancelled",
                    order_display,
                )
                # Update to CANCELLED if order not found
                self._repository.update_trade(
                    trade_id=trade.id,
                    status=TradeStatus.CANCELLED,
                )
                return True
            raise

        if not order:
            logger.warning(
                "No order data returned for %s",
                order_display,
            )
            return False

        # Extract status and fill information from response
        clob_status = order.get("status", "")
        size_matched_str = order.get("size_matched", "0")
        original_size_str = order.get("original_size") or order.get("size", "0")
        avg_price_str = order.get("average_price") or order.get("price")

        # Parse numeric values
        size_matched = Decimal(str(size_matched_str)) if size_matched_str else Decimal("0")
        original_size = Decimal(str(original_size_str)) if original_size_str else trade.quantity

        # Map CLOB status to TradeStatus
        new_status = map_clob_status_to_trade_status(
            clob_status,
            size_matched=size_matched,
            original_size=original_size,
        )

        # Check if anything changed
        status_changed = new_status != trade.status
        fill_changed = size_matched != trade.filled_quantity

        if not status_changed and not fill_changed:
            logger.debug(
                "Trade %s unchanged (status=%s, filled=%s)",
                order_display,
                trade.status.value,
                trade.filled_quantity,
            )
            return False

        # Parse average price if available
        avg_fill_price = None
        if avg_price_str:
            try:
                avg_fill_price = Decimal(str(avg_price_str))
            except (ValueError, TypeError, InvalidOperation):
                pass

        # Update the trade in database
        logger.info(
            "Updating trade %s: status %s -> %s, filled %s -> %s",
            order_display,
            trade.status.value,
            new_status.value,
            trade.filled_quantity,
            size_matched,
        )

        self._repository.update_trade(
            trade_id=trade.id,
            status=new_status,
            filled_quantity=size_matched,
            avg_fill_price=avg_fill_price,
        )

        return True


__all__ = ["TradeReconciler", "ReconciliationError", "map_clob_status_to_trade_status"]
