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
from datetime import datetime, timedelta, timezone
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
from src.trading.executor import TradeExecutor

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
        # TradeExecutor is initialized later in run() after logging is configured
        # This ensures all initialization logs are captured
        self._trade_executor: TradeExecutor | None = None

        # Market tracking
        self._active_markets: list[Market] = []
        self._token_to_market: dict[str, Market] = {}

        # Price tracking for opportunity detection
        self._last_prices: dict[str, float] = {}
        self._best_bids: dict[str, float] = {}

        # Detected opportunities in current window
        self._window_opportunities: list[Opportunity] = []

        # Market lifecycle tracking
        self._current_market_closing_time: datetime | None = None

        # Per-market reversal state tracking
        self._last_alerted_side: dict[str, str] = {}
        self._market_multipliers: dict[str, float] = {}

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
                self._current_market_closing_time = None
                return []

            logger.info(
                "Discovered %d markets from %d series",
                len(markets),
                len(self._config.series_ids),
            )

            # Extract closing time from current events
            # Find the soonest closing time across all monitored series
            soonest_closing_time: datetime | None = None
            for series_id in self._config.series_ids:
                event = self._gamma_client.get_current_event_for_series(series_id)
                if event:
                    closing_time = self._gamma_client.get_closing_time_for_event(
                        event.title
                    )
                    if closing_time:
                        if soonest_closing_time is None or closing_time < soonest_closing_time:
                            soonest_closing_time = closing_time
                            logger.debug(
                                "Series %s event '%s' closes at %s",
                                series_id,
                                event.title[:50],
                                closing_time.strftime("%H:%M:%S UTC"),
                            )

            self._current_market_closing_time = soonest_closing_time
            if soonest_closing_time:
                logger.info(
                    "Market closing time set to %s",
                    soonest_closing_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                )
            else:
                logger.warning("Could not determine market closing time from events")

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

        Handles bidirectional alerts by tracking the last-alerted side per market
        and applying a reversal multiplier when the opposite side triggers.

        Args:
            token_id: Token ID to check for opportunities.
        """
        market = self._token_to_market.get(token_id)
        if not market:
            return

        # Find the token's outcome (YES or NO) from the market's tokens
        token_outcome = "YES"  # Default fallback
        for token in market.tokens:
            if token.token_id == token_id:
                token_outcome = token.outcome
                break

        last_trade_price = self._last_prices.get(token_id)

        opportunities = detect_opportunity(
            last_trade_price=last_trade_price,
            threshold=self._config.opportunity_threshold,
            market_id=market.id,
            token_id=token_id,
            neg_risk=market.neg_risk,
            outcome=token_outcome,
        )

        for opp in opportunities:
            # Avoid duplicate notifications for same opportunity
            if not self._is_duplicate_opportunity(opp):
                # Check if this is a reversal (opposite side from last alert)
                last_side = self._last_alerted_side.get(opp.market_id)
                is_reversal = last_side is not None and last_side != opp.side

                # Initialize or update the multiplier
                if opp.market_id not in self._market_multipliers:
                    # First alert for this market - start at 1.0
                    self._market_multipliers[opp.market_id] = 1.0
                elif is_reversal:
                    # Reversal detected - apply the reversal multiplier
                    self._market_multipliers[opp.market_id] *= self._config.reversal_multiplier
                    logger.info(
                        "Reversal detected for market %s: %s -> %s, multiplier now %.2fx",
                        opp.market_id,
                        last_side,
                        opp.side,
                        self._market_multipliers[opp.market_id],
                    )

                # Update last alerted side for this market
                self._last_alerted_side[opp.market_id] = opp.side

                # Get the current multiplier for this market
                multiplier = self._market_multipliers[opp.market_id]

                self._window_opportunities.append(opp)
                self._notifier.notify(opp)
                if self._trade_executor:
                    self._trade_executor.notify(opp, multiplier=multiplier)

    def _is_duplicate_opportunity(self, new_opp: Opportunity) -> bool:
        """Check if an opportunity is a duplicate based on last-alerted side.

        Allows bidirectional alerts within a single market: if the last alert
        was for one side (YES or NO), an alert for the opposite side is allowed.
        Only blocks alerts when the same side is triggered consecutively.

        This enables a ping-pong pattern where reversals trigger new alerts,
        while preventing duplicate notifications for the same direction.

        Args:
            new_opp: The opportunity to check.

        Returns:
            True if this is a same-side duplicate (should be blocked),
            False if this is a new alert or opposite-side (should be allowed).
        """
        last_side = self._last_alerted_side.get(new_opp.market_id)

        # First alert for this market - not a duplicate
        if last_side is None:
            return False

        # Same side as last alert - this is a duplicate
        if last_side == new_opp.side:
            return True

        # Opposite side - this is a reversal, not a duplicate
        return False

    def _time_until_market_closes(self) -> timedelta:
        """Calculate time remaining until the current market closes.

        Returns:
            Time until the current market's closing time. Returns timedelta(0)
            if no closing time is set or the time has already passed.
            Falls back to time_until_window_ends() if closing time is None.
        """
        if self._current_market_closing_time is None:
            return time_until_window_ends()

        now = datetime.now(timezone.utc)
        remaining = self._current_market_closing_time - now
        return remaining if remaining > timedelta(0) else timedelta(0)

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

    def _clear_market_state(self) -> None:
        """Reset all per-market tracking state for market transition.

        Clears all tracking data when transitioning between markets to ensure
        no stale data from the previous market affects the next market's monitoring.
        This includes price tracking, opportunity detection state, market mappings,
        and per-market reversal tracking state.
        """
        logger.debug(
            "Clearing market state: %d prices, %d bids, %d opportunities, "
            "%d tokens, %d markets, %d alerted_sides, %d multipliers",
            len(self._last_prices),
            len(self._best_bids),
            len(self._window_opportunities),
            len(self._token_to_market),
            len(self._active_markets),
            len(self._last_alerted_side),
            len(self._market_multipliers),
        )

        self._last_prices.clear()
        self._best_bids.clear()
        self._window_opportunities.clear()
        self._token_to_market.clear()
        self._active_markets.clear()
        self._current_market_closing_time = None
        self._last_alerted_side.clear()
        self._market_multipliers.clear()

        logger.info("Market state cleared for new market transition")

    def _transition_to_next_market(self) -> bool:
        """Execute the full market transition sequence.

        Handles the complete transition from one market to the next:
        1. Stop current WebSocket connection
        2. Clear all market tracking state
        3. Discover next available market
        4. Start new WebSocket connection if market found

        Returns:
            True if successfully transitioned to a new market with active
            WebSocket monitoring, False if no next market available.

        Manual Verification Steps:
            To verify this method works correctly during market transitions:

            1. **Run with debug logging**::

                LOG_LEVEL=DEBUG python -m src.main

            2. **Expected log output during transition**::

                # When transition begins:
                INFO  | src.main | Transitioning to next market...
                INFO  | src.main | Stopping WebSocket connection (market window ended)...
                INFO  | src.main | WebSocket connection closed, ready for next window

                # State cleanup (DEBUG level):
                DEBUG | src.main | Clearing market state: N prices, N bids, N opportunities, N tokens, N markets
                INFO  | src.main | Market state cleared for new market transition

                # New market discovery:
                INFO  | src.main | Discovered N markets from N series
                DEBUG | src.main | Series XXX event 'Event Title...' closes at HH:MM:SS UTC
                INFO  | src.main | Market closing time set to YYYY-MM-DD HH:MM:SS UTC

                # On success:
                INFO  | src.main | WebSocket started, subscribed to N tokens
                INFO  | src.main | Successfully transitioned to next market - monitoring N markets with N tokens

                # On failure (no next market):
                WARNING | src.main | No next market available - waiting for next discovery opportunity

            3. **What to observe**:
                - WebSocket connection cleanly stops before new discovery
                - All state counters go to 0 during clearing
                - New market has different closing time than previous
                - Token subscription count matches new market
        """
        logger.info("Transitioning to next market...")

        # Step 1: Stop current WebSocket
        self._stop_websocket()

        # Step 2: Clear all market state
        self._clear_market_state()

        # Step 3: Discover next market
        self._active_markets = self._discover_markets()
        if not self._active_markets:
            logger.warning(
                "No next market available - waiting for next discovery opportunity"
            )
            return False

        # Step 4: Build token mapping and start new WebSocket
        self._build_token_mapping(self._active_markets)
        token_ids = self._get_token_ids(self._active_markets)

        if not token_ids:
            logger.warning("No tokens found in discovered markets")
            return False

        if not self._start_websocket(token_ids):
            logger.error("Failed to start WebSocket for next market")
            return False

        logger.info(
            "Successfully transitioned to next market - monitoring %d markets "
            "with %d tokens",
            len(self._active_markets),
            len(token_ids),
        )
        return True

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
        """Monitor for opportunities with continuous market lifecycle management.

        Implements a continuous monitoring loop that automatically transitions
        between markets as they close. The loop continues indefinitely until:
        - Shutdown is requested (via signal or stop() call)
        - No next market is available after retry timeout (30 seconds)

        Market Lifecycle Flow:
        1. Discover initial markets for current time
        2. Subscribe to WebSocket for price updates
        3. Monitor until market closing time is reached
        4. When market closes:
           a. Log "Market closed at [time]"
           b. Call _transition_to_next_market()
           c. If successful, continue monitoring the new market
           d. If no next market, retry with backoff up to 30s timeout
        5. Exit only on shutdown or after exhausting retries

        Manual Verification Steps:
            To verify market lifecycle behavior end-to-end:

            1. **Run with debug logging**::

                LOG_LEVEL=DEBUG python -m src.main

               Or with explicit series::

                SERIES_IDS=abc123,def456 LOG_LEVEL=DEBUG python -m src.main

            2. **Expected log output during normal operation**::

                # Initial startup:
                INFO  | src.main | Starting Polymarket Monitor...
                INFO  | src.main | Discovered N markets from N series
                DEBUG | src.main | Series XXX event 'Bitcoin Up or Down - Jan 10, 8:15PM-8:30PM ET' closes at 01:30:00 UTC
                INFO  | src.main | Market closing time set to 2026-01-10 01:30:00 UTC
                INFO  | src.main | WebSocket started, subscribed to N tokens
                INFO  | src.main | Now monitoring market closing at 2026-01-10 01:30:00 UTC

                # During monitoring (every ~1 second at DEBUG level):
                DEBUG | src.main | Monitoring... N opportunities detected, Ns until market closes

                # When market closes:
                INFO  | src.main | Market closed at 2026-01-10 01:30:00 UTC
                INFO  | src.main | Transitioning to next market...

                # After successful transition:
                INFO  | src.main | Successfully transitioned to next market - monitoring N markets with N tokens
                INFO  | src.main | Now monitoring market closing at 2026-01-10 01:45:00 UTC

            3. **Expected log output when no next market available**::

                INFO  | src.main | Market closed at 2026-01-10 01:30:00 UTC
                INFO  | src.main | Transitioning to next market...
                WARNING | src.main | No next market available - will retry for up to 30s
                DEBUG | src.main | Waiting for next market... (5s/30s elapsed)
                DEBUG | src.main | Waiting for next market... (10s/30s elapsed)
                ...
                WARNING | src.main | No next market available after 30s timeout - exiting monitor loop

            4. **What to observe during market transition**:
                - Closing time in logs matches expected market end time (from event title)
                - WebSocket cleanly disconnects before new market discovery
                - New market's closing time is later than previous market
                - No errors or warnings during normal transitions
                - Opportunity count resets to 0 for each new market

            5. **Key indicators of correct behavior**:
                - Market closing times parsed from event titles (e.g., "8:30PM ET" -> 01:30 UTC)
                - Continuous monitoring across multiple market periods
                - State isolation: opportunities from previous market don't affect next
                - Clean shutdown on Ctrl+C (SIGINT) with "Shutdown signal received"

            6. **Troubleshooting**:
                - If "Could not determine market closing time" appears, event title
                  may not match expected format (check series configuration)
                - If "No markets to monitor" appears, verify SERIES_IDS contains valid series
                - For WebSocket issues, check network connectivity and API status
        """
        # Retry configuration for when no next market is immediately available
        MAX_RETRY_WAIT_SECONDS = 30
        RETRY_INTERVAL_SECONDS = 5

        # Get window timing for initial notification
        _, window_end = get_current_market_window()

        # Reset window state
        self._window_opportunities.clear()
        self._last_prices.clear()
        self._best_bids.clear()

        # Notify window start
        self._notifier.notify_window_start(window_end)

        # Discover initial markets
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

        # Log initial market closing time
        if self._current_market_closing_time:
            logger.info(
                "Now monitoring market closing at %s",
                self._current_market_closing_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            )

        # Continuous market lifecycle loop
        while not self._shutdown_requested:
            remaining = self._time_until_market_closes()

            if remaining.total_seconds() <= 0:
                # Market has closed - log and transition
                closed_at = self._current_market_closing_time
                if closed_at:
                    logger.info(
                        "Market closed at %s",
                        closed_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    )
                else:
                    logger.info("Market closing time reached")

                # Notify window end and log summary for this market
                self._notifier.notify_window_end()
                if self._window_opportunities:
                    logger.info(
                        "Market window complete: %d opportunities detected",
                        len(self._window_opportunities),
                    )
                else:
                    self._notifier.notify_no_opportunities()

                # Attempt transition to next market with retry logic
                retry_elapsed = 0
                transition_success = False

                while retry_elapsed < MAX_RETRY_WAIT_SECONDS and not self._shutdown_requested:
                    if self._transition_to_next_market():
                        transition_success = True
                        # Log new market closing time
                        if self._current_market_closing_time:
                            logger.info(
                                "Now monitoring market closing at %s",
                                self._current_market_closing_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                            )
                        # Update window timing for notification
                        _, window_end = get_current_market_window()
                        self._notifier.notify_window_start(window_end)
                        break

                    # No next market available - wait and retry
                    if retry_elapsed == 0:
                        logger.warning(
                            "No next market available - will retry for up to %ds",
                            MAX_RETRY_WAIT_SECONDS,
                        )

                    logger.debug(
                        "Waiting for next market... (%ds/%ds elapsed)",
                        retry_elapsed,
                        MAX_RETRY_WAIT_SECONDS,
                    )
                    time.sleep(RETRY_INTERVAL_SECONDS)
                    retry_elapsed += RETRY_INTERVAL_SECONDS

                if not transition_success and not self._shutdown_requested:
                    logger.warning(
                        "No next market available after %ds timeout - exiting monitor loop",
                        MAX_RETRY_WAIT_SECONDS,
                    )
                    break

                continue  # Start monitoring the new market

            # Update status periodically while monitoring
            logger.debug(
                "Monitoring... %d opportunities detected, %.0fs until market closes",
                len(self._window_opportunities),
                remaining.total_seconds(),
            )

            # Sleep briefly to avoid busy-waiting
            time.sleep(1.0)

        # Clean up WebSocket on exit
        self._stop_websocket()
        logger.info("Market monitoring loop exited")

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

        # Initialize TradeExecutor after logging is configured
        # This ensures all initialization logs (including any errors) are captured
        self._trade_executor = TradeExecutor(self._config)

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
        print(f"  Trade amount USD: ${config.trade_amount_usd:.2f}")
        print(f"  Reversal multiplier: {config.reversal_multiplier:.1f}x")
        print(f"  Auto trade enabled: {config.auto_trade_enabled}")
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
