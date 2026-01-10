"""Tests for main.py PolymarketMonitor methods.

Tests the market lifecycle management methods including:
- _time_until_market_closes()
- _clear_market_state()
- _transition_to_next_market()
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.config import Config
from src.main import PolymarketMonitor


class TestTimeUntilMarketCloses:
    """Test _time_until_market_closes helper method."""

    @pytest.fixture
    def monitor(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor instance for testing."""
        config = Config()
        # Create monitor but don't initialize signal handlers in test
        with patch.object(PolymarketMonitor, "__init__", lambda self, cfg: None):
            monitor = PolymarketMonitor.__new__(PolymarketMonitor)
        # Manually set required attributes
        monitor._config = config
        monitor._running = False
        monitor._shutdown_requested = False
        monitor._clob_client = None
        monitor._gamma_client = None
        monitor._websocket = None
        monitor._notifier = MagicMock()
        monitor._active_markets = []
        monitor._token_to_market = {}
        monitor._last_prices = {}
        monitor._best_bids = {}
        monitor._window_opportunities = []
        monitor._current_market_closing_time = None
        return monitor

    def test_returns_positive_timedelta_when_closing_time_in_future(
        self, monitor: PolymarketMonitor
    ):
        """Verify positive timedelta returned when market closes in the future."""
        # Set closing time to 5 minutes from now
        future_close = datetime.now(timezone.utc) + timedelta(minutes=5)
        monitor._current_market_closing_time = future_close

        result = monitor._time_until_market_closes()

        # Should be approximately 5 minutes (within 1 second tolerance)
        assert result.total_seconds() > 299  # At least 4:59
        assert result.total_seconds() <= 300  # At most 5:00

    def test_returns_zero_timedelta_when_closing_time_in_past(
        self, monitor: PolymarketMonitor
    ):
        """Verify zero timedelta returned when market closing time has passed."""
        # Set closing time to 5 minutes ago
        past_close = datetime.now(timezone.utc) - timedelta(minutes=5)
        monitor._current_market_closing_time = past_close

        result = monitor._time_until_market_closes()

        assert result == timedelta(0)

    def test_falls_back_to_window_end_when_closing_time_is_none(
        self, monitor: PolymarketMonitor
    ):
        """Verify fallback to time_until_window_ends when no closing time set."""
        monitor._current_market_closing_time = None

        # Mock time_until_window_ends to return a known value
        mock_window_remaining = timedelta(minutes=7)
        with patch(
            "src.main.time_until_window_ends", return_value=mock_window_remaining
        ):
            result = monitor._time_until_market_closes()

        assert result == mock_window_remaining

    def test_returns_zero_when_closing_time_is_exactly_now(
        self, monitor: PolymarketMonitor
    ):
        """Verify zero timedelta when closing time equals current time."""
        # Set closing time to exactly now
        monitor._current_market_closing_time = datetime.now(timezone.utc)

        result = monitor._time_until_market_closes()

        # Should be zero or very close to zero
        assert result.total_seconds() <= 0.1


class TestClearMarketState:
    """Test _clear_market_state helper method."""

    @pytest.fixture
    def monitor_with_state(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor with pre-populated state."""
        config = Config()
        with patch.object(PolymarketMonitor, "__init__", lambda self, cfg: None):
            monitor = PolymarketMonitor.__new__(PolymarketMonitor)
        # Manually set required attributes with populated state
        monitor._config = config
        monitor._running = False
        monitor._shutdown_requested = False
        monitor._clob_client = None
        monitor._gamma_client = None
        monitor._websocket = None
        monitor._notifier = MagicMock()

        # Populate with sample state data
        monitor._active_markets = [MagicMock(), MagicMock()]
        monitor._token_to_market = {"token1": MagicMock(), "token2": MagicMock()}
        monitor._last_prices = {"token1": 0.75, "token2": 0.82}
        monitor._best_bids = {"token1": 0.74, "token2": 0.81}
        monitor._window_opportunities = [MagicMock()]
        monitor._current_market_closing_time = datetime.now(timezone.utc) + timedelta(minutes=10)
        return monitor

    def test_clears_all_price_tracking_data(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify all price tracking dictionaries are cleared."""
        assert len(monitor_with_state._last_prices) > 0
        assert len(monitor_with_state._best_bids) > 0

        monitor_with_state._clear_market_state()

        assert len(monitor_with_state._last_prices) == 0
        assert len(monitor_with_state._best_bids) == 0

    def test_clears_opportunity_detection_state(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify window opportunities list is cleared."""
        assert len(monitor_with_state._window_opportunities) > 0

        monitor_with_state._clear_market_state()

        assert len(monitor_with_state._window_opportunities) == 0

    def test_clears_market_mappings(self, monitor_with_state: PolymarketMonitor):
        """Verify token to market mapping and active markets are cleared."""
        assert len(monitor_with_state._token_to_market) > 0
        assert len(monitor_with_state._active_markets) > 0

        monitor_with_state._clear_market_state()

        assert len(monitor_with_state._token_to_market) == 0
        assert len(monitor_with_state._active_markets) == 0

    def test_resets_closing_time_to_none(self, monitor_with_state: PolymarketMonitor):
        """Verify market closing time is reset to None."""
        assert monitor_with_state._current_market_closing_time is not None

        monitor_with_state._clear_market_state()

        assert monitor_with_state._current_market_closing_time is None


class TestTransitionToNextMarket:
    """Test _transition_to_next_market helper method."""

    @pytest.fixture
    def monitor(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor instance for testing."""
        config = Config()
        with patch.object(PolymarketMonitor, "__init__", lambda self, cfg: None):
            monitor = PolymarketMonitor.__new__(PolymarketMonitor)
        # Manually set required attributes
        monitor._config = config
        monitor._running = False
        monitor._shutdown_requested = False
        monitor._clob_client = None
        monitor._gamma_client = MagicMock()
        monitor._websocket = MagicMock()
        monitor._notifier = MagicMock()
        monitor._active_markets = []
        monitor._token_to_market = {}
        monitor._last_prices = {"old_token": 0.5}
        monitor._best_bids = {"old_token": 0.49}
        monitor._window_opportunities = [MagicMock()]
        monitor._current_market_closing_time = datetime.now(timezone.utc)
        return monitor

    def test_successful_transition_stops_websocket_first(
        self, monitor: PolymarketMonitor
    ):
        """Verify websocket is stopped before discovering new markets."""
        # Create mock market with tokens
        mock_market = MagicMock()
        mock_token = MagicMock()
        mock_token.token_id = "new_token_123"
        mock_market.tokens = [mock_token]

        call_order = []

        def mock_stop_websocket():
            call_order.append("stop_websocket")

        def mock_discover_markets():
            call_order.append("discover_markets")
            return [mock_market]

        with patch.object(
            monitor, "_stop_websocket", side_effect=mock_stop_websocket
        ), patch.object(
            monitor, "_discover_markets", side_effect=mock_discover_markets
        ), patch.object(
            monitor, "_start_websocket", return_value=True
        ):
            monitor._transition_to_next_market()

        assert call_order == ["stop_websocket", "discover_markets"]

    def test_successful_transition_clears_market_state(
        self, monitor: PolymarketMonitor
    ):
        """Verify market state is cleared during transition."""
        mock_market = MagicMock()
        mock_token = MagicMock()
        mock_token.token_id = "new_token_123"
        mock_market.tokens = [mock_token]

        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", return_value=[mock_market]
        ), patch.object(monitor, "_start_websocket", return_value=True):
            monitor._transition_to_next_market()

        # Old state should be cleared (but new state populated)
        assert "old_token" not in monitor._last_prices
        assert "old_token" not in monitor._best_bids

    def test_returns_true_on_successful_transition(self, monitor: PolymarketMonitor):
        """Verify True returned when transition completes successfully."""
        mock_market = MagicMock()
        mock_token = MagicMock()
        mock_token.token_id = "new_token_123"
        mock_market.tokens = [mock_token]

        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", return_value=[mock_market]
        ), patch.object(monitor, "_start_websocket", return_value=True):
            result = monitor._transition_to_next_market()

        assert result is True

    def test_returns_false_when_no_markets_discovered(
        self, monitor: PolymarketMonitor
    ):
        """Verify False returned when no next market is available."""
        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", return_value=[]
        ):
            result = monitor._transition_to_next_market()

        assert result is False

    def test_returns_false_when_no_tokens_found(self, monitor: PolymarketMonitor):
        """Verify False returned when discovered markets have no tokens."""
        # Market with empty tokens list
        mock_market = MagicMock()
        mock_market.tokens = []

        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", return_value=[mock_market]
        ):
            result = monitor._transition_to_next_market()

        assert result is False

    def test_returns_false_when_websocket_fails_to_start(
        self, monitor: PolymarketMonitor
    ):
        """Verify False returned when websocket fails to start for new market."""
        mock_market = MagicMock()
        mock_token = MagicMock()
        mock_token.token_id = "new_token_123"
        mock_market.tokens = [mock_token]

        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", return_value=[mock_market]
        ), patch.object(monitor, "_start_websocket", return_value=False):
            result = monitor._transition_to_next_market()

        assert result is False

    def test_builds_token_mapping_for_new_markets(self, monitor: PolymarketMonitor):
        """Verify token mapping is built for newly discovered markets."""
        mock_market = MagicMock()
        mock_token = MagicMock()
        mock_token.token_id = "new_token_123"
        mock_market.tokens = [mock_token]

        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", return_value=[mock_market]
        ), patch.object(monitor, "_start_websocket", return_value=True):
            monitor._transition_to_next_market()

        assert "new_token_123" in monitor._token_to_market
        assert monitor._token_to_market["new_token_123"] == mock_market


class TestMarketLifecycleIntegration:
    """Integration test for complete market lifecycle transitions.

    Tests the full transition sequence: websocket stops → state clears →
    new market discovered when a market closes.
    """

    @pytest.fixture
    def monitor(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor with mocked dependencies for integration testing."""
        config = Config()
        with patch.object(PolymarketMonitor, "__init__", lambda self, cfg: None):
            monitor = PolymarketMonitor.__new__(PolymarketMonitor)

        # Set up required attributes
        monitor._config = config
        monitor._running = False
        monitor._shutdown_requested = False
        monitor._clob_client = None
        monitor._gamma_client = MagicMock()
        monitor._websocket = MagicMock()
        monitor._notifier = MagicMock()

        # Pre-populate state to simulate an active market
        monitor._active_markets = [MagicMock()]
        monitor._token_to_market = {"old_token_abc": MagicMock()}
        monitor._last_prices = {"old_token_abc": 0.65}
        monitor._best_bids = {"old_token_abc": 0.64}
        monitor._window_opportunities = [MagicMock()]
        monitor._current_market_closing_time = datetime.now(timezone.utc) - timedelta(seconds=1)

        return monitor

    def test_lifecycle_integration_transition_sequence(
        self, monitor: PolymarketMonitor
    ):
        """Verify complete lifecycle: websocket stops, state clears, new market discovered.

        This integration test simulates a market closing by:
        1. Setting up initial market state with old tokens/prices
        2. Mocking _discover_markets to return a new market
        3. Calling _transition_to_next_market()
        4. Verifying the correct sequence of operations
        """
        # Create mock for new market that will be discovered
        new_market = MagicMock()
        new_token = MagicMock()
        new_token.token_id = "new_token_xyz"
        new_market.tokens = [new_token]

        # Track operation sequence
        operation_sequence = []

        def mock_stop_websocket():
            operation_sequence.append("websocket_stopped")

        def mock_discover_markets():
            operation_sequence.append("market_discovered")
            return [new_market]

        def mock_start_websocket(token_ids):
            operation_sequence.append("websocket_started")
            return True

        # Store original state for verification
        original_prices = dict(monitor._last_prices)
        original_bids = dict(monitor._best_bids)
        original_opportunities = list(monitor._window_opportunities)

        # Verify initial state exists
        assert len(original_prices) > 0
        assert len(original_bids) > 0
        assert len(original_opportunities) > 0
        assert "old_token_abc" in monitor._token_to_market

        # Apply mocks and execute transition
        with patch.object(
            monitor, "_stop_websocket", side_effect=mock_stop_websocket
        ), patch.object(
            monitor, "_discover_markets", side_effect=mock_discover_markets
        ), patch.object(
            monitor, "_start_websocket", side_effect=mock_start_websocket
        ):
            result = monitor._transition_to_next_market()

        # Verify transition succeeded
        assert result is True

        # Verify sequence: websocket stopped BEFORE market discovery
        assert operation_sequence == [
            "websocket_stopped",
            "market_discovered",
            "websocket_started",
        ]

        # Verify old state was cleared
        assert "old_token_abc" not in monitor._last_prices
        assert "old_token_abc" not in monitor._best_bids
        assert len(monitor._window_opportunities) == 0

        # Verify new market state is populated
        assert "new_token_xyz" in monitor._token_to_market
        assert monitor._token_to_market["new_token_xyz"] == new_market
        assert new_market in monitor._active_markets

    def test_lifecycle_integration_no_market_available(
        self, monitor: PolymarketMonitor
    ):
        """Verify transition handles case when no next market is available.

        Simulates Gamma API returning no markets (e.g., outside trading hours).
        """
        operation_sequence = []

        def mock_stop_websocket():
            operation_sequence.append("websocket_stopped")

        def mock_discover_markets():
            operation_sequence.append("market_discovery_attempted")
            return []  # No markets available

        with patch.object(
            monitor, "_stop_websocket", side_effect=mock_stop_websocket
        ), patch.object(
            monitor, "_discover_markets", side_effect=mock_discover_markets
        ):
            result = monitor._transition_to_next_market()

        # Verify transition failed due to no markets
        assert result is False

        # Verify websocket was still stopped (cleanup happened)
        assert "websocket_stopped" in operation_sequence
        assert "market_discovery_attempted" in operation_sequence

        # Verify state was still cleared (no stale data)
        assert len(monitor._last_prices) == 0
        assert len(monitor._best_bids) == 0
        assert len(monitor._window_opportunities) == 0

    def test_lifecycle_integration_state_isolation_between_markets(
        self, monitor: PolymarketMonitor
    ):
        """Verify complete state isolation between old and new market data.

        Ensures no data leakage from previous market to next market.
        """
        # Create distinctly different new market
        new_market = MagicMock()
        new_token = MagicMock()
        new_token.token_id = "completely_different_token"
        new_market.tokens = [new_token]
        new_market.id = "new_market_id"

        # Track what state is visible during discovery
        state_during_discovery = {}

        def capture_state_during_discovery():
            # Capture state at the moment of discovery
            state_during_discovery["prices"] = dict(monitor._last_prices)
            state_during_discovery["bids"] = dict(monitor._best_bids)
            state_during_discovery["opportunities"] = list(
                monitor._window_opportunities
            )
            state_during_discovery["token_map"] = dict(monitor._token_to_market)
            return [new_market]

        with patch.object(monitor, "_stop_websocket"), patch.object(
            monitor, "_discover_markets", side_effect=capture_state_during_discovery
        ), patch.object(monitor, "_start_websocket", return_value=True):
            monitor._transition_to_next_market()

        # Verify state was clear at discovery time (no stale data visible)
        assert len(state_during_discovery["prices"]) == 0
        assert len(state_during_discovery["bids"]) == 0
        assert len(state_during_discovery["opportunities"]) == 0
        assert len(state_during_discovery["token_map"]) == 0
