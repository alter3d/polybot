"""Trade tracking callbacks for WebSocket messages.

This module provides the TradeTrackingCallback class for handling
OrderMessage and TradeMessage updates from the user channel WebSocket.
It updates trade records in the database based on real-time order
and trade execution events.
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from src.api.websocket_handler import OrderMessage, TradeMessage
from src.db import TradeStatus
from src.db.reconciliation import map_clob_status_to_trade_status
from src.db.repository import TradeRepository

logger = logging.getLogger(__name__)


class TradeTrackingCallback:
    """Callback handler for trade tracking via WebSocket messages.

    Processes OrderMessage and TradeMessage events from the user channel
    WebSocket and updates corresponding trade records in the database.

    Attributes:
        _repository: Database repository for trade operations.
        _enabled: Whether trade tracking is enabled.
    """

    def __init__(self, repository: TradeRepository) -> None:
        """Initialize the trade tracking callback handler.

        Args:
            repository: Database repository for trade operations.
        """
        self._repository = repository
        self._enabled = repository.is_enabled

        if self._enabled:
            logger.info("TradeTrackingCallback initialized")
        else:
            logger.info("TradeTrackingCallback disabled (database not configured)")

    @property
    def is_enabled(self) -> bool:
        """Check if trade tracking is enabled."""
        return self._enabled

    def on_message(self, msg_type: str, data: Any) -> None:
        """Handle incoming WebSocket messages.

        Dispatches to the appropriate handler based on message type.
        This method can be passed directly as the callback to
        UserChannelWebSocket.

        Args:
            msg_type: Type of the message (trade, order, etc.).
            data: Parsed message data.
        """
        if not self._enabled:
            return

        if isinstance(data, TradeMessage):
            self.handle_trade_message(data)
        elif isinstance(data, OrderMessage):
            self.handle_order_message(data)

    def handle_order_message(self, order_msg: OrderMessage) -> None:
        """Handle an order status update message.

        Updates the trade status in the database based on the order status
        received from the WebSocket. Maps CLOB statuses (LIVE, MATCHED,
        CANCELLED) to TradeStatus enum values.

        Args:
            order_msg: Parsed OrderMessage from the user channel.
        """
        if not self._enabled or not order_msg.order_id:
            return

        order_display = (
            order_msg.order_id[:8] + "..."
            if len(order_msg.order_id) > 8
            else order_msg.order_id
        )

        logger.debug(
            "Processing order message: id=%s status=%s matched=%.4f/%.4f",
            order_display,
            order_msg.status,
            order_msg.size_matched,
            order_msg.original_size,
        )

        # Look up the trade by order_id
        trade = self._repository.get_trade_by_order_id(order_msg.order_id)
        if not trade:
            logger.debug(
                "No trade found for order_id %s (may be external order)",
                order_display,
            )
            return

        # Parse fill quantities
        size_matched = Decimal(str(order_msg.size_matched)) if order_msg.size_matched else Decimal("0")
        original_size = Decimal(str(order_msg.original_size)) if order_msg.original_size else trade.quantity

        # Map CLOB status to TradeStatus
        new_status = map_clob_status_to_trade_status(
            order_msg.status,
            size_matched=size_matched,
            original_size=original_size,
        )

        # Check if anything changed
        status_changed = new_status != trade.status
        fill_changed = size_matched != trade.filled_quantity

        if not status_changed and not fill_changed:
            logger.debug(
                "Order %s unchanged (status=%s, filled=%s)",
                order_display,
                trade.status.value,
                trade.filled_quantity,
            )
            return

        # Determine filled_at timestamp for completed orders
        filled_at: Optional[datetime] = None
        if new_status in (TradeStatus.FILLED, TradeStatus.PARTIALLY_FILLED):
            if order_msg.timestamp:
                # Convert milliseconds to datetime
                filled_at = datetime.fromtimestamp(
                    order_msg.timestamp / 1000.0,
                    tz=timezone.utc,
                )
            else:
                filled_at = datetime.now(timezone.utc)

        # Calculate average fill price from order message if available
        avg_fill_price: Optional[Decimal] = None
        cost_basis_usd: Optional[Decimal] = None
        if order_msg.price and size_matched > 0:
            avg_fill_price = Decimal(str(order_msg.price))
            # Calculate cost basis in USD: filled_quantity * avg_fill_price
            cost_basis_usd = size_matched * avg_fill_price

        # Update the trade in database
        logger.info(
            "Updating trade from order message: id=%s status %s -> %s, filled %s -> %s",
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
            filled_at=filled_at,
            cost_basis_usd=cost_basis_usd,
        )

    def handle_trade_message(self, trade_msg: TradeMessage) -> None:
        """Handle a trade execution message.

        Updates the filled_quantity in the database when a trade execution
        is received. Trade messages indicate actual fills that have occurred.

        Args:
            trade_msg: Parsed TradeMessage from the user channel.
        """
        if not self._enabled or not trade_msg.order_id:
            return

        order_display = (
            trade_msg.order_id[:8] + "..."
            if len(trade_msg.order_id) > 8
            else trade_msg.order_id
        )

        logger.debug(
            "Processing trade message: order_id=%s side=%s price=%.4f size=%.4f",
            order_display,
            trade_msg.side,
            trade_msg.price,
            trade_msg.size,
        )

        # Look up the trade by order_id
        trade = self._repository.get_trade_by_order_id(trade_msg.order_id)
        if not trade:
            logger.debug(
                "No trade found for order_id %s (may be external order)",
                order_display,
            )
            return

        # Parse fill size from this trade execution
        fill_size = Decimal(str(trade_msg.size)) if trade_msg.size else Decimal("0")

        if fill_size <= 0:
            logger.debug("Trade message has zero size, skipping")
            return

        # Calculate new filled quantity (cumulative)
        new_filled_quantity = trade.filled_quantity + fill_size

        # Determine new status based on fill progress
        if new_filled_quantity >= trade.quantity:
            new_status = TradeStatus.FILLED
        elif new_filled_quantity > 0:
            new_status = TradeStatus.PARTIALLY_FILLED
        else:
            new_status = trade.status

        # Calculate weighted average fill price
        # If we have an existing avg_fill_price, compute weighted average
        avg_fill_price: Optional[Decimal] = None
        cost_basis_usd: Optional[Decimal] = None
        if trade_msg.price:
            trade_price = Decimal(str(trade_msg.price))
            if trade.avg_fill_price and trade.filled_quantity > 0:
                # Weighted average: (old_price * old_qty + new_price * new_qty) / total_qty
                total_value = (
                    trade.avg_fill_price * trade.filled_quantity
                    + trade_price * fill_size
                )
                avg_fill_price = total_value / new_filled_quantity
            else:
                avg_fill_price = trade_price
            # Calculate cost basis in USD: filled_quantity * avg_fill_price
            if avg_fill_price:
                cost_basis_usd = new_filled_quantity * avg_fill_price

        # Determine filled_at timestamp
        filled_at: Optional[datetime] = None
        if new_status == TradeStatus.FILLED:
            if trade_msg.timestamp:
                filled_at = datetime.fromtimestamp(
                    trade_msg.timestamp / 1000.0,
                    tz=timezone.utc,
                )
            else:
                filled_at = datetime.now(timezone.utc)

        # Update the trade in database
        logger.info(
            "Updating trade from trade execution: order_id=%s filled %s -> %s (fill=%.4f)",
            order_display,
            trade.filled_quantity,
            new_filled_quantity,
            fill_size,
        )

        self._repository.update_trade(
            trade_id=trade.id,
            status=new_status,
            filled_quantity=new_filled_quantity,
            avg_fill_price=avg_fill_price,
            filled_at=filled_at,
            cost_basis_usd=cost_basis_usd,
        )


__all__ = ["TradeTrackingCallback"]
