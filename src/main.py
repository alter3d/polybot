#!/usr/bin/env python3

"""Main entry point for Polymarket position monitoring.

This module provides the main monitoring loop that integrates all components:
- Configuration management
- Market discovery via Gamma API
- Real-time price monitoring via WebSocket
- Opportunity detection based on price thresholds
- Console notifications

The monitoring workflow operates on 15-minute windows:
1. Wait until 3 minutes before window end
2. Discover active crypto markets
3. Subscribe to WebSocket for real-time prices
4. Detect opportunities when prices exceed threshold
5. Output notifications to console
6. Repeat for next window

Usage:
    python -m src.main           # Run as module (recommended)
    python src/main.py           # Run as script (also supported)
    python src/main.py --dry-run # Test configuration
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# Ensure the parent directory is in sys.path for both execution methods:
# - `python -m src.main` (module execution)
# - `python src/main.py` (direct script execution)
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv

from src.api.clob_client import PolymarketClobClient
from src.api.gamma_client import GammaClient, Market
from src.api.websocket_handler import (
    LastTradePrice,
    MarketWebSocket,
    OrderBookUpdate,
    PriceChange,
)
from src.config import Config
from src.market.opportunity_detector import Opportunity, detect_opportunity
from src.market.timing import (
    format_window_info,
    get_current_market_window,
    should_start_monitoring,
    time_until_monitoring_starts,
    time_until_window_ends,
)
from src.notifications.console import ConsoleNotifier

# Load environment variables from .env file
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


class PolymarketMonitor:
    """Main monitoring class that coordinates all components.

    Handles the lifecycle of market monitoring including:
    - Initialization of API clients
    - Market discovery and filtering
    - WebSocket subscription management
    - Price monitoring and opportunity detection
    - Graceful shutdown handling

    Example:
        >>> config = Config.from_env()
        >>> monitor = PolymarketMonitor(config)
        >>> monitor.run()  # Blocks until shutdown
    """

    def __init__(self, config: Config) -> None:
        """Initialize the monitor with configuration.

        Args:
            config: Application configuration instance.
        """
        self._config = config
        self._running = False
        self._shutdown_requested = False

        # Initialize components
        self._clob_client: PolymarketClobClient | None = None
        self._gamma_client: GammaClient | None = None
        self._websocket: MarketWebSocket | None = None
        self._notifier = ConsoleNotifier()

        # Market tracking
        self._active_markets: list[Market] = []
        self._token_to_market: dict[str, Market] = {}

        # Price tracking for opportunity detection
        self._last_prices: dict[str, float] = {}
        self._best_bids: dict[str, float] = {}

        # Detected opportunities in current window
        self._window_opportunities: list[Opportunity] = []

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("PolymarketMonitor initialized with threshold $%.2f", config.opportunity_threshold)

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals gracefully.

        Args:
            signum: Signal number received.
            frame: Current stack frame.
        """
        logger.info("Shutdown signal received (signal %d)", signum)
        self._shutdown_requested = True
        self.stop()

    def _setup_logging(self) -> None:
        """Configure application logging based on config."""
        log_level = getattr(logging, self._config.log_level.upper(), logging.INFO)

        # Configure root logger
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Reduce noise from third-party libraries
        logging.getLogger("websocket").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)

        logger.info("Logging configured at %s level", self._config.log_level)

    def _initialize_clients(self) -> bool:
        """Initialize API clients.

        Returns:
            True if all clients initialized successfully, False otherwise.
        """
        try:
            # Initialize CLOB client
            self._clob_client = PolymarketClobClient(self._config)

            # Verify CLOB API connectivity
            if not self._clob_client.health_check():
                logger.error("CLOB API health check failed")
                return False

            logger.info("CLOB API connection verified")

            # Initialize Gamma client
            self._gamma_client = GammaClient(self._config)
            logger.info("Gamma API client initialized")

            return True

        except Exception as e:
            logger.error("Failed to initialize API clients: %s", e)
            return False

    def _discover_markets(self) -> list[Market]:
        """Discover markets via series-based event discovery.

        Uses the configured series_ids to:
        1. Query each series for its events
        2. Find the event whose time period covers the current time
        3. Extract the markets from that event

        Returns:
            List of markets from current events across all configured series.
        """
        if not self._gamma_client:
            logger.error("Gamma client not initialized")
            return []

        if not self._config.series_ids:
            logger.error(
                "No series IDs configured. Set SERIES_IDS environment variable "
                "with comma-separated series IDs to monitor."
            )
            return []

        try:
            # Use series-based discovery to find current markets
            markets = self._gamma_client.get_current_markets_for_series(
                self._config.series_ids
            )

            if not markets:
                logger.warning(
                    "No markets found for current time in configured series: %s",
                    self._config.series_ids,
                )
                return []

            logger.info(
                "Discovered %d markets from %d series",
                len(markets),
                len(self._config.series_ids),
            )

            # Log markets for visibility
            for market in markets[:5]:
                token_count = len(market.tokens)
                logger.debug(
                    "Market: %s (slug: %s, %d tokens)",
                    market.question[:50] if market.question else "(no question)",
                    market.slug,
                    token_count,
                )

            return markets

        except Exception as e:
            logger.error("Failed to discover markets: %s", e)
            return []

    def _build_token_mapping(self, markets: list[Market]) -> None:
        """Build mapping from token IDs to markets.

        Args:
            markets: List of markets to process.
        """
        self._token_to_market.clear()
        for market in markets:
            for token in market.tokens:
                self._token_to_market[token.token_id] = market

        logger.debug(
            "Built token mapping: %d tokens across %d markets",
            len(self._token_to_market),
            len(markets),
        )

    def _get_token_ids(self, markets: list[Market]) -> list[str]:
        """Extract all token IDs from a list of markets.

        Args:
            markets: List of markets to extract tokens from.

        Returns:
            List of token IDs.
        """
        token_ids = []
        for market in markets:
            for token in market.tokens:
                token_ids.append(token.token_id)
        return token_ids

    def _on_websocket_message(self, msg_type: str, data: Any) -> None:
        """Handle incoming WebSocket messages.

        Processes price updates and detects opportunities.

        Args:
            msg_type: Type of the message (book, price_change, last_trade_price).
            data: Parsed message data.
        """
        try:
            if isinstance(data, OrderBookUpdate):
                self._handle_order_book_update(data)
            elif isinstance(data, LastTradePrice):
                self._handle_last_trade_price(data)
            elif isinstance(data, PriceChange):
                self._handle_price_change(data)
            # Other message types are logged but not processed
            else:
                logger.debug("Received %s message (not processed)", msg_type)

        except Exception as e:
            logger.error("Error handling WebSocket message: %s", e)

    def _handle_order_book_update(self, update: OrderBookUpdate) -> None:
        """Process order book update and check for opportunities.

        Args:
            update: Order book update from WebSocket.
        """
        if update.best_bid is not None:
            self._best_bids[update.asset_id] = update.best_bid
            self._check_opportunity(update.asset_id)

    def _handle_last_trade_price(self, update: LastTradePrice) -> None:
        """Process last trade price update and check for opportunities.

        Args:
            update: Last trade price update from WebSocket.
        """
        self._last_prices[update.asset_id] = update.price
        self._check_opportunity(update.asset_id)

    def _handle_price_change(self, update: PriceChange) -> None:
        """Process price change and update best bid if applicable.

        Args:
            update: Price change update from WebSocket.
        """
        if update.side.lower() == "buy":
            # Update best bid if this is a higher bid
            current_bid = self._best_bids.get(update.asset_id, 0)
            if update.price > current_bid:
                self._best_bids[update.asset_id] = update.price
                self._check_opportunity(update.asset_id)

    def _check_opportunity(self, token_id: str) -> None:
        """Check if current prices indicate an opportunity.

        Args:
            token_id: Token ID to check for opportunities.
        """
        market = self._token_to_market.get(token_id)
        if not market:
            return

        bid_price = self._best_bids.get(token_id)
        last_trade_price = self._last_prices.get(token_id)

        opportunities = detect_opportunity(
            bid_price=bid_price,
            last_trade_price=last_trade_price,
            threshold=self._config.opportunity_threshold,
            market_id=market.id,
        )

        for opp in opportunities:
            # Avoid duplicate notifications for same opportunity
            if not self._is_duplicate_opportunity(opp):
                self._window_opportunities.append(opp)
                self._notifier.notify(opp)

    def _is_duplicate_opportunity(self, new_opp: Opportunity) -> bool:
        """Check if an opportunity has already been notified for this market.

        Limits alerts to a single notification per market when the threshold
        is first breached. This prevents multiple alerts for the same market
        regardless of whether subsequent trades/bids also exceed the threshold.

        Args:
            new_opp: The opportunity to check.

        Returns:
            True if this market has already triggered an alert, False otherwise.
        """
        for existing in self._window_opportunities:
            if existing.market_id == new_opp.market_id:
                return True
        return False

    def _start_websocket(self, token_ids: list[str]) -> bool:
        """Start WebSocket connection and subscribe to tokens.

        Args:
            token_ids: List of token IDs to subscribe to.

        Returns:
            True if WebSocket started successfully, False otherwise.
        """
        if not token_ids:
            logger.warning("No token IDs to subscribe to")
            return False

        try:
            self._websocket = MarketWebSocket(
                config=self._config,
                on_message=self._on_websocket_message,
                auto_reconnect=True,
            )
            self._websocket.connect(token_ids)
            self._websocket.run(blocking=False)

            logger.info("WebSocket started, subscribed to %d tokens", len(token_ids))
            return True

        except Exception as e:
            logger.error("Failed to start WebSocket: %s", e)
            return False

    def _stop_websocket(self) -> None:
        """Stop the WebSocket connection and wait for cleanup.

        This ensures the WebSocket thread is fully terminated before
        we proceed to discover markets for the next window.
        """
        if self._websocket:
            logger.info("Stopping WebSocket connection (market window ended)...")
            self._websocket.stop(timeout=5.0)
            self._websocket = None
            logger.info("WebSocket connection closed, ready for next window")

    def _wait_for_monitoring_window(self) -> bool:
        """Wait until the monitoring window starts.

        Returns:
            True if we should continue monitoring, False if shutdown requested.
        """
        while not self._shutdown_requested:
            if should_start_monitoring(self._config.monitor_start_minutes_before_end):
                return True

            wait_time = time_until_monitoring_starts(
                self._config.monitor_start_minutes_before_end
            )

            if wait_time.total_seconds() > 0:
                # Show status while waiting
                print(f"\r{format_window_info(self._config.monitor_start_minutes_before_end)}", end="")

                # Sleep in small intervals to allow for quick shutdown response
                sleep_duration = min(wait_time.total_seconds(), 5.0)
                time.sleep(sleep_duration)
            else:
                # Already in monitoring window
                print()  # Clear the status line
                return True

        return False

    def _monitor_window(self) -> None:
        """Monitor for opportunities during the current window.

        Handles the full monitoring cycle for one 15-minute window:
        1. Discover markets
        2. Subscribe to WebSocket
        3. Monitor until window ends
        4. Clean up
        """
        # Get window timing
        _, window_end = get_current_market_window()

        # Reset window state
        self._window_opportunities.clear()
        self._last_prices.clear()
        self._best_bids.clear()

        # Notify window start
        self._notifier.notify_window_start(window_end)

        # Discover markets
        self._active_markets = self._discover_markets()
        if not self._active_markets:
            logger.warning("No markets to monitor, waiting for next window")
            return

        # Build token mapping and get token IDs
        self._build_token_mapping(self._active_markets)
        token_ids = self._get_token_ids(self._active_markets)

        if not token_ids:
            logger.warning("No tokens found in markets")
            return

        # Start WebSocket monitoring
        if not self._start_websocket(token_ids):
            logger.error("Failed to start WebSocket monitoring")
            return

        try:
            # Monitor until window ends
            while not self._shutdown_requested:
                remaining = time_until_window_ends()

                if remaining.total_seconds() <= 0:
                    logger.info(
                        "Market window ended - stopping monitoring for this window"
                    )
                    break

                # Update status periodically
                logger.debug(
                    "Monitoring... %d opportunities detected, %.0fs remaining",
                    len(self._window_opportunities),
                    remaining.total_seconds(),
                )

                # Sleep briefly to avoid busy-waiting
                time.sleep(1.0)

        finally:
            # Clean up WebSocket - ensures complete shutdown before next window
            self._stop_websocket()

        # Notify window end
        self._notifier.notify_window_end()
        logger.info(
            "Window monitoring complete. Will discover new markets for next window."
        )

        # Summary for this window
        if self._window_opportunities:
            logger.info(
                "Window complete: %d opportunities detected",
                len(self._window_opportunities),
            )
        else:
            self._notifier.notify_no_opportunities()

    def run(self) -> None:
        """Run the main monitoring loop.

        This is the main entry point for the monitoring system.
        It runs continuously until shutdown is requested.
        """
        self._running = True

        print("=" * 60)
        print("Starting Polymarket Monitor...")
        print(f"  Threshold: ${self._config.opportunity_threshold:.2f}")
        print(f"  Monitor window: {self._config.monitor_start_minutes_before_end} minutes before end")
        if self._config.series_ids:
            print(f"  Series IDs: {', '.join(self._config.series_ids)}")
        else:
            print("  Series IDs: (none configured)")
        print(f"  Log level: {self._config.log_level}")
        print("=" * 60)

        # Setup logging
        self._setup_logging()

        # Initialize API clients
        if not self._initialize_clients():
            logger.error("Failed to initialize clients, exiting")
            return

        logger.info("Polymarket Monitor started successfully")

        try:
            while not self._shutdown_requested:
                # Wait for monitoring window
                if not self._wait_for_monitoring_window():
                    break  # Shutdown requested

                # Monitor current window
                self._monitor_window()

                # Small delay before checking for next window
                if not self._shutdown_requested:
                    time.sleep(1.0)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")

        finally:
            self.stop()

        logger.info("Polymarket Monitor stopped")

    def stop(self) -> None:
        """Stop the monitor and clean up resources."""
        if not self._running:
            return

        logger.info("Stopping Polymarket Monitor...")
        self._running = False
        self._shutdown_requested = True

        # Stop WebSocket
        self._stop_websocket()

        # Close API clients
        if self._gamma_client:
            self._gamma_client.close()
            self._gamma_client = None

        logger.info("Cleanup complete")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(
        description="Polymarket Position Entry Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main                    # Run with default settings
  python -m src.main --threshold 0.80   # Use custom price threshold
  python -m src.main --log-level DEBUG  # Enable debug logging

Environment variables:
  OPPORTUNITY_THRESHOLD   Price threshold (default: 0.70)
  SHARES_TO_TRADE         Shares per trade (default: 20)
  MONITOR_START_MINUTES   Minutes before window end (default: 3)
  SERIES_IDS              Comma-separated list of Polymarket series IDs to monitor
  LOG_LEVEL               Logging verbosity (default: INFO)
        """,
    )

    parser.add_argument(
        "--threshold",
        type=float,
        help="Price threshold for opportunity detection (overrides env var)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level (overrides env var)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test configuration and exit without monitoring",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point for the application.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    # Parse command line arguments
    args = parse_args()

    # Load configuration from environment
    config = Config.from_env()

    # Apply command line overrides
    if args.threshold is not None:
        config.opportunity_threshold = args.threshold

    if args.log_level is not None:
        config.log_level = args.log_level

    # Dry run mode - test configuration and exit
    if args.dry_run:
        print("Configuration valid:")
        print(f"  Threshold: ${config.opportunity_threshold:.2f}")
        print(f"  Shares to trade: {config.shares_to_trade}")
        print(f"  Monitor start: {config.monitor_start_minutes_before_end} min before end")
        print(f"  Series IDs: {', '.join(config.series_ids) if config.series_ids else '(none)'}")
        print(f"  CLOB host: {config.clob_host}")
        print(f"  Gamma host: {config.gamma_host}")
        print(f"  WebSocket host: {config.ws_host}")
        print(f"  Log level: {config.log_level}")
        return 0

    # Create and run monitor
    monitor = PolymarketMonitor(config)

    try:
        monitor.run()
        return 0
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
