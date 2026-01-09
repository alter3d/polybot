"""CLOB API client wrapper for Polymarket.

This module provides a wrapper around the py-clob-client library for
interacting with the Polymarket Central Limit Order Book (CLOB) API.
Supports read-only market data access for monitoring purposes.
"""

import logging
from dataclasses import dataclass
from typing import Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams

from src.config import Config

logger = logging.getLogger(__name__)


@dataclass
class OrderBookData:
    """Structured representation of order book data.

    Attributes:
        token_id: The token identifier for this order book.
        bids: List of bid orders, each with 'price' and 'size'.
        asks: List of ask orders, each with 'price' and 'size'.
        best_bid: The highest bid price, or None if no bids.
        best_ask: The lowest ask price, or None if no asks.
    """

    token_id: str
    bids: list[dict[str, str]]
    asks: list[dict[str, str]]
    best_bid: float | None
    best_ask: float | None


class PolymarketClobClient:
    """Wrapper for Polymarket CLOB API client.

    This client provides read-only access to market data including
    order books and last trade prices. No authentication is required
    for read operations.

    Example:
        >>> config = Config.from_env()
        >>> client = PolymarketClobClient(config)
        >>> if client.health_check():
        ...     order_book = client.get_order_book("token_id_here")
        ...     print(f"Best bid: {order_book.best_bid}")
    """

    def __init__(self, config: Config) -> None:
        """Initialize the CLOB client.

        Args:
            config: Application configuration containing API endpoints.
        """
        self._config = config
        self._client = ClobClient(host=config.clob_host)
        logger.info("Initialized CLOB client for %s", config.clob_host)

    def health_check(self) -> bool:
        """Check if the CLOB API is healthy and reachable.

        Returns:
            True if the API responds successfully, False otherwise.
        """
        try:
            result = self._client.get_ok()
            is_healthy = result == "OK"
            if is_healthy:
                logger.debug("CLOB API health check passed")
            else:
                logger.warning("CLOB API health check returned unexpected: %s", result)
            return is_healthy
        except Exception as e:
            logger.error("CLOB API health check failed: %s", e)
            return False

    def get_server_time(self) -> int | None:
        """Get the current server timestamp.

        Returns:
            Unix timestamp in milliseconds, or None on error.
        """
        try:
            return self._client.get_server_time()
        except Exception as e:
            logger.error("Failed to get server time: %s", e)
            return None

    def get_order_book(self, token_id: str) -> OrderBookData | None:
        """Fetch the order book for a specific token.

        Args:
            token_id: The token identifier to fetch order book for.

        Returns:
            OrderBookData with bids, asks, and best prices, or None on error.
        """
        try:
            raw_book = self._client.get_order_book(token_id)
            return self._parse_order_book(token_id, raw_book)
        except Exception as e:
            logger.error("Failed to get order book for %s: %s", token_id, e)
            return None

    def get_order_books(self, token_ids: list[str]) -> list[OrderBookData]:
        """Fetch order books for multiple tokens in a single request.

        Args:
            token_ids: List of token identifiers.

        Returns:
            List of OrderBookData objects for successfully fetched books.
        """
        if not token_ids:
            return []

        try:
            params = [BookParams(token_id=tid) for tid in token_ids]
            raw_books = self._client.get_order_books(params)

            results = []
            for i, raw_book in enumerate(raw_books):
                parsed = self._parse_order_book(token_ids[i], raw_book)
                if parsed:
                    results.append(parsed)

            logger.debug("Fetched %d order books", len(results))
            return results

        except Exception as e:
            logger.error("Failed to get order books: %s", e)
            return []

    def get_last_trade_price(self, token_id: str) -> float | None:
        """Get the last trade price for a specific token.

        Args:
            token_id: The token identifier.

        Returns:
            Last trade price as a float, or None if unavailable or on error.
        """
        try:
            result = self._client.get_last_trade_price(token_id)
            if result is not None:
                price = float(result)
                logger.debug("Last trade price for %s: %.4f", token_id, price)
                return price
            return None
        except Exception as e:
            logger.error("Failed to get last trade price for %s: %s", token_id, e)
            return None

    def get_midpoint(self, token_id: str) -> float | None:
        """Get the midpoint price for a specific token.

        Args:
            token_id: The token identifier.

        Returns:
            Midpoint price as a float, or None if unavailable or on error.
        """
        try:
            result = self._client.get_midpoint(token_id)
            if result is not None:
                midpoint = float(result)
                logger.debug("Midpoint for %s: %.4f", token_id, midpoint)
                return midpoint
            return None
        except Exception as e:
            logger.error("Failed to get midpoint for %s: %s", token_id, e)
            return None

    def get_price(self, token_id: str, side: str = "BUY") -> float | None:
        """Get the price for a specific token and side.

        Args:
            token_id: The token identifier.
            side: The side to get price for ("BUY" or "SELL").

        Returns:
            Price as a float, or None if unavailable or on error.
        """
        try:
            result = self._client.get_price(token_id, side)
            if result is not None:
                price = float(result)
                logger.debug("%s price for %s: %.4f", side, token_id, price)
                return price
            return None
        except Exception as e:
            logger.error("Failed to get %s price for %s: %s", side, token_id, e)
            return None

    def _parse_order_book(self, token_id: str, raw_book: Any) -> OrderBookData | None:
        """Parse raw order book response into OrderBookData.

        Args:
            token_id: The token identifier.
            raw_book: Raw response from the API.

        Returns:
            Parsed OrderBookData or None if parsing fails.
        """
        try:
            # Handle different response formats from the API
            if isinstance(raw_book, dict):
                bids = raw_book.get("bids", [])
                asks = raw_book.get("asks", [])
            else:
                # Handle object with attributes
                bids = getattr(raw_book, "bids", []) or []
                asks = getattr(raw_book, "asks", []) or []

            # Calculate best bid and ask
            best_bid = None
            best_ask = None

            if bids:
                try:
                    bid_prices = [float(b.get("price", 0) if isinstance(b, dict) else b.price) for b in bids]
                    best_bid = max(bid_prices) if bid_prices else None
                except (ValueError, TypeError, AttributeError):
                    pass

            if asks:
                try:
                    ask_prices = [float(a.get("price", 0) if isinstance(a, dict) else a.price) for a in asks]
                    best_ask = min(ask_prices) if ask_prices else None
                except (ValueError, TypeError, AttributeError):
                    pass

            # Normalize to list of dicts
            normalized_bids = self._normalize_orders(bids)
            normalized_asks = self._normalize_orders(asks)

            return OrderBookData(
                token_id=token_id,
                bids=normalized_bids,
                asks=normalized_asks,
                best_bid=best_bid,
                best_ask=best_ask,
            )

        except Exception as e:
            logger.error("Failed to parse order book for %s: %s", token_id, e)
            return None

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
                else:
                    normalized.append({
                        "price": str(getattr(order, "price", "")),
                        "size": str(getattr(order, "size", "")),
                    })
            except Exception:
                continue
        return normalized
