"""WebSocket handler for real-time Polymarket order book updates.

This module provides a WebSocket client for subscribing to real-time market data
from the Polymarket CLOB WebSocket feed. It handles connection management,
automatic reconnection with exponential backoff, and message parsing.
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import websocket

from src.config import Config

logger = logging.getLogger(__name__)

# Default reconnection settings
DEFAULT_RECONNECT_DELAY = 1.0  # Initial delay in seconds
MAX_RECONNECT_DELAY = 60.0  # Maximum delay in seconds
RECONNECT_BACKOFF_FACTOR = 2.0  # Exponential backoff multiplier

# Heartbeat interval in seconds (per Polymarket docs)
HEARTBEAT_INTERVAL = 10


@dataclass
class OrderBookUpdate:
    """Represents an order book snapshot from WebSocket.

    Attributes:
        asset_id: Token ID for this order book.
        market_id: Condition ID (market) for this order book.
        timestamp: Unix timestamp in milliseconds.
        hash: Order book hash for verification.
        bids: List of bid orders with price and size.
        asks: List of ask orders with price and size.
        best_bid: Highest bid price, or None if no bids.
        best_ask: Lowest ask price, or None if no asks.
    """

    asset_id: str
    market_id: str
    timestamp: int
    hash: str
    bids: list[dict[str, str]] = field(default_factory=list)
    asks: list[dict[str, str]] = field(default_factory=list)
    best_bid: float | None = None
    best_ask: float | None = None


@dataclass
class PriceChange:
    """Represents a price change event from WebSocket.

    Attributes:
        asset_id: Token ID for this update.
        price: New price level.
        side: Order side ("buy" or "sell").
        size: New size at this price level.
        timestamp: When this change occurred.
    """

    asset_id: str
    price: float
    side: str
    size: float
    timestamp: int


@dataclass
class LastTradePrice:
    """Represents a last trade price update from WebSocket.

    Attributes:
        asset_id: Token ID for this update.
        price: Last trade price.
        timestamp: When the trade occurred.
    """

    asset_id: str
    price: float
    timestamp: int


# Type alias for message callback
MessageCallback = Callable[[str, Any], None]


class MarketWebSocket:
    """WebSocket client for Polymarket market data.

    This class manages a WebSocket connection to the Polymarket CLOB feed,
    handling subscriptions, message parsing, and automatic reconnection.

    Example:
        >>> config = Config.from_env()
        >>> def on_message(msg_type: str, data: Any):
        ...     print(f"Received {msg_type}: {data}")
        >>> ws = MarketWebSocket(config, on_message=on_message)
        >>> ws.connect(["token_id_1", "token_id_2"])
        >>> ws.run()  # Blocks until disconnected
    """

    def __init__(
        self,
        config: Config,
        on_message: MessageCallback | None = None,
        auto_reconnect: bool = True,
    ) -> None:
        """Initialize the WebSocket client.

        Args:
            config: Application configuration containing WebSocket URL.
            on_message: Callback function called with (message_type, parsed_data).
            auto_reconnect: Whether to automatically reconnect on disconnection.
        """
        self._config = config
        self._url = config.ws_host
        self._on_message_callback = on_message
        self._auto_reconnect = auto_reconnect

        # Connection state
        self._ws: websocket.WebSocketApp | None = None
        self._subscribed_assets: list[str] = []
        self._is_running = False
        self._should_stop = False
        self._reconnect_delay = DEFAULT_RECONNECT_DELAY
        self._last_sequence: dict[str, int] = {}

        # Thread for running the WebSocket
        self._ws_thread: threading.Thread | None = None

        # Heartbeat tracking
        self._last_ping_time: float = 0.0
        self._last_pong_time: float = 0.0

        logger.info("Initialized MarketWebSocket for %s", self._url)

    @property
    def is_connected(self) -> bool:
        """Check if the WebSocket is currently connected."""
        return self._ws is not None and self._is_running

    @property
    def subscribed_assets(self) -> list[str]:
        """Get the list of currently subscribed asset IDs."""
        return self._subscribed_assets.copy()

    def connect(self, asset_ids: list[str] | None = None) -> None:
        """Establish WebSocket connection and optionally subscribe to assets.

        Args:
            asset_ids: Optional list of token IDs to subscribe to on connection.
        """
        if asset_ids:
            self._subscribed_assets = list(asset_ids)

        self._should_stop = False
        self._reconnect_delay = DEFAULT_RECONNECT_DELAY

        self._ws = websocket.WebSocketApp(
            self._url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_ping=self._on_ping,
            on_pong=self._on_pong,
        )

        logger.info(
            "WebSocket connection configured for %d assets",
            len(self._subscribed_assets),
        )

    def run(self, blocking: bool = True) -> None:
        """Run the WebSocket connection.

        Args:
            blocking: If True, blocks until disconnected. If False, runs in a thread.
        """
        if self._ws is None:
            logger.error("WebSocket not connected. Call connect() first.")
            return

        if blocking:
            self._run_forever()
        else:
            self._ws_thread = threading.Thread(target=self._run_forever, daemon=True)
            self._ws_thread.start()
            logger.debug("WebSocket running in background thread")

    def _run_forever(self) -> None:
        """Internal method to run the WebSocket event loop with reconnection."""
        while not self._should_stop:
            try:
                self._is_running = True
                logger.info("Starting WebSocket connection...")

                # Run the WebSocket (blocks until disconnection)
                # ping_interval must be > ping_timeout per websocket-client library
                # Send ping every 30s, wait 10s for pong response
                self._ws.run_forever(
                    ping_interval=HEARTBEAT_INTERVAL * 3,  # 30 seconds
                    ping_timeout=HEARTBEAT_INTERVAL,       # 10 seconds
                )

            except Exception as e:
                logger.error("WebSocket error: %s", e)

            finally:
                self._is_running = False

            # Handle reconnection
            if not self._should_stop and self._auto_reconnect:
                logger.info(
                    "Reconnecting in %.1f seconds...",
                    self._reconnect_delay,
                )
                time.sleep(self._reconnect_delay)

                # Exponential backoff
                self._reconnect_delay = min(
                    self._reconnect_delay * RECONNECT_BACKOFF_FACTOR,
                    MAX_RECONNECT_DELAY,
                )

                # Recreate the WebSocket for reconnection
                self.connect(self._subscribed_assets)
            else:
                break

        logger.info("WebSocket connection stopped")

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the WebSocket connection and prevent reconnection.

        Args:
            timeout: Maximum time in seconds to wait for the WebSocket thread
                to terminate. Default is 5 seconds.
        """
        self._should_stop = True
        if self._ws:
            self._ws.close()
        logger.info("WebSocket stop requested")

        # Wait for the WebSocket thread to terminate
        if self._ws_thread and self._ws_thread.is_alive():
            logger.debug("Waiting for WebSocket thread to terminate...")
            self._ws_thread.join(timeout=timeout)
            if self._ws_thread.is_alive():
                logger.warning(
                    "WebSocket thread did not terminate within %.1f seconds",
                    timeout,
                )
            else:
                logger.debug("WebSocket thread terminated successfully")
        self._ws_thread = None

    def subscribe(self, asset_ids: list[str]) -> None:
        """Subscribe to market data for specified assets.

        Args:
            asset_ids: List of token IDs to subscribe to.
        """
        if not asset_ids:
            logger.warning("No asset IDs provided for subscription")
            return

        # Track subscribed assets
        for asset_id in asset_ids:
            if asset_id not in self._subscribed_assets:
                self._subscribed_assets.append(asset_id)

        if not self.is_connected:
            logger.debug("Subscription queued (not connected yet)")
            return

        subscribe_msg = {
            "type": "market",
            "assets_ids": asset_ids,  # Note: API uses "assets_ids" (plural)
        }

        try:
            self._ws.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to %d assets: %s", len(asset_ids), asset_ids[:3])
        except Exception as e:
            logger.error("Failed to send subscribe message: %s", e)

    def unsubscribe(self, asset_ids: list[str]) -> None:
        """Unsubscribe from market data for specified assets.

        Args:
            asset_ids: List of token IDs to unsubscribe from.
        """
        if not asset_ids:
            return

        # Remove from tracked subscriptions
        self._subscribed_assets = [
            a for a in self._subscribed_assets if a not in asset_ids
        ]

        if not self.is_connected:
            return

        unsubscribe_msg = {
            "type": "unsubscribe",
            "assets_ids": asset_ids,
        }

        try:
            self._ws.send(json.dumps(unsubscribe_msg))
            logger.info("Unsubscribed from %d assets", len(asset_ids))
        except Exception as e:
            logger.error("Failed to send unsubscribe message: %s", e)

    def _on_open(self, ws: websocket.WebSocket) -> None:
        """Handle WebSocket connection open event."""
        logger.info("WebSocket connected to %s", self._url)
        self._reconnect_delay = DEFAULT_RECONNECT_DELAY  # Reset on successful connect

        # Subscribe to any queued assets
        if self._subscribed_assets:
            self.subscribe(self._subscribed_assets)

    def _on_message(self, ws: websocket.WebSocket, message: str) -> None:
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            msg_type = self._get_message_type(data)

            # Log at debug level to avoid spam
            logger.debug("Received %s message", msg_type)

            # Parse and dispatch based on message type
            parsed_data = self._parse_message(msg_type, data)

            # Check for sequence gaps
            self._check_sequence(data)

            # Call user callback if provided
            if self._on_message_callback and parsed_data is not None:
                try:
                    self._on_message_callback(msg_type, parsed_data)
                except Exception as e:
                    logger.error("Error in message callback: %s", e)

        except json.JSONDecodeError as e:
            logger.warning("Failed to decode WebSocket message: %s", e)
        except Exception as e:
            logger.error("Error processing WebSocket message: %s", e)

    def _on_error(self, ws: websocket.WebSocket, error: Exception) -> None:
        """Handle WebSocket error event."""
        logger.error("WebSocket error: %s", error)

    def _on_close(
        self,
        ws: websocket.WebSocket,
        close_status_code: int | None,
        close_msg: str | None,
    ) -> None:
        """Handle WebSocket close event."""
        logger.info(
            "WebSocket closed (code=%s, reason=%s)",
            close_status_code,
            close_msg,
        )
        self._is_running = False

    def _on_ping(self, ws: websocket.WebSocket, message: bytes) -> None:
        """Handle ping message from server."""
        self._last_ping_time = time.time()
        logger.debug("Received ping")

    def _on_pong(self, ws: websocket.WebSocket, message: bytes) -> None:
        """Handle pong response from server."""
        self._last_pong_time = time.time()
        logger.debug("Received pong")

    def _get_message_type(self, data: dict[str, Any]) -> str:
        """Extract the message type from a WebSocket message.

        Args:
            data: Parsed JSON message data.

        Returns:
            Message type string (e.g., "book", "price_change", "last_trade_price").
        """
        # Different message formats use different fields
        if "event_type" in data:
            return str(data["event_type"])
        if "type" in data:
            return str(data["type"])
        return "unknown"

    def _parse_message(
        self,
        msg_type: str,
        data: dict[str, Any],
    ) -> OrderBookUpdate | PriceChange | LastTradePrice | dict[str, Any] | None:
        """Parse a WebSocket message into a structured object.

        Args:
            msg_type: The message type identifier.
            data: Raw message data.

        Returns:
            Parsed message object or None if parsing fails.
        """
        try:
            if msg_type == "book":
                return self._parse_book_message(data)
            elif msg_type == "price_change":
                return self._parse_price_change(data)
            elif msg_type == "last_trade_price":
                return self._parse_last_trade_price(data)
            else:
                # Return raw data for other message types
                return data

        except Exception as e:
            logger.warning("Failed to parse %s message: %s", msg_type, e)
            return None

    def _parse_book_message(self, data: dict[str, Any]) -> OrderBookUpdate:
        """Parse a book (order book snapshot) message.

        Args:
            data: Raw book message data.

        Returns:
            Parsed OrderBookUpdate object.
        """
        bids = data.get("bids", []) or []
        asks = data.get("asks", []) or []

        # Calculate best bid and ask
        best_bid = None
        best_ask = None

        if bids:
            try:
                bid_prices = [
                    float(b.get("price", 0)) if isinstance(b, dict) else float(b[0])
                    for b in bids
                ]
                best_bid = max(bid_prices) if bid_prices else None
            except (ValueError, TypeError, IndexError):
                pass

        if asks:
            try:
                ask_prices = [
                    float(a.get("price", 0)) if isinstance(a, dict) else float(a[0])
                    for a in asks
                ]
                best_ask = min(ask_prices) if ask_prices else None
            except (ValueError, TypeError, IndexError):
                pass

        return OrderBookUpdate(
            asset_id=str(data.get("asset_id", "")),
            market_id=str(data.get("market", "")),
            timestamp=int(data.get("timestamp", 0)),
            hash=str(data.get("hash", "")),
            bids=self._normalize_orders(bids),
            asks=self._normalize_orders(asks),
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def _parse_price_change(self, data: dict[str, Any]) -> PriceChange:
        """Parse a price change message.

        Args:
            data: Raw price change message data.

        Returns:
            Parsed PriceChange object.
        """
        return PriceChange(
            asset_id=str(data.get("asset_id", "")),
            price=float(data.get("price", 0)),
            side=str(data.get("side", "")),
            size=float(data.get("size", 0)),
            timestamp=int(data.get("timestamp", 0)),
        )

    def _parse_last_trade_price(self, data: dict[str, Any]) -> LastTradePrice:
        """Parse a last trade price message.

        Args:
            data: Raw last trade price message data.

        Returns:
            Parsed LastTradePrice object.
        """
        return LastTradePrice(
            asset_id=str(data.get("asset_id", "")),
            price=float(data.get("price", 0)),
            timestamp=int(data.get("timestamp", 0)),
        )

    def _normalize_orders(self, orders: list[Any]) -> list[dict[str, str]]:
        """Normalize order list to consistent dict format.

        Args:
            orders: List of orders in various formats.

        Returns:
            List of dicts with 'price' and 'size' string keys.
        """
        normalized = []
        for order in orders:
            try:
                if isinstance(order, dict):
                    normalized.append({
                        "price": str(order.get("price", "")),
                        "size": str(order.get("size", "")),
                    })
                elif isinstance(order, (list, tuple)) and len(order) >= 2:
                    normalized.append({
                        "price": str(order[0]),
                        "size": str(order[1]),
                    })
            except Exception:
                continue
        return normalized

    def _check_sequence(self, data: dict[str, Any]) -> None:
        """Check for gaps in message sequence numbers.

        Args:
            data: Message data that may contain a sequence number.
        """
        if "sequence" not in data:
            return

        asset_id = str(data.get("asset_id", "unknown"))
        sequence = int(data.get("sequence", 0))

        if asset_id in self._last_sequence:
            expected = self._last_sequence[asset_id] + 1
            if sequence != expected:
                logger.warning(
                    "Sequence gap detected for %s: expected %d, got %d",
                    asset_id,
                    expected,
                    sequence,
                )

        self._last_sequence[asset_id] = sequence
