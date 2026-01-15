"""Database enums and constants for trade tracking.

This module provides Python Enum classes that mirror the PostgreSQL
enum types used in the trade tracking database schema.
"""

from enum import Enum


class TradeStatus(str, Enum):
    """Trade status enum matching PostgreSQL trade_status type.

    Tracks the lifecycle of a trade from creation to completion.

    Values:
        OPEN: Order has been placed but not yet filled
        FILLED: Order has been completely filled
        PARTIALLY_FILLED: Order has been partially filled
        CANCELLED: Order was cancelled before completion
        CLOSED: Position has been closed (sold or redeemed)
    """

    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    CLOSED = "closed"


class TradeSide(str, Enum):
    """Trade side enum matching PostgreSQL trade_side type.

    Represents which outcome token is being traded.

    Values:
        YES: Trading YES outcome tokens
        NO: Trading NO outcome tokens
    """

    YES = "YES"
    NO = "NO"


class OrderSide(str, Enum):
    """Order side enum matching PostgreSQL order_side type.

    Represents the direction of the order.

    Values:
        BUY: Buying tokens (going long)
        SELL: Selling tokens (closing position or going short)
    """

    BUY = "BUY"
    SELL = "SELL"


__all__ = ["TradeStatus", "TradeSide", "OrderSide"]
