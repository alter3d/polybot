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
from src.market.opportunity_detector import Opportunity


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
        monitor._last_alerted_side = {}
        monitor._market_multipliers = {}
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
        monitor._last_alerted_side = {"market1": "YES"}
        monitor._market_multipliers = {"market1": 1.5}
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
        monitor._last_alerted_side = {}
        monitor._market_multipliers = {}
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
        monitor._last_alerted_side = {}
        monitor._market_multipliers = {}

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


class TestIsDuplicateOpportunity:
    """Test _is_duplicate_opportunity for bidirectional alert detection."""

    @pytest.fixture
    def monitor(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor instance for testing duplicate detection."""
        config = Config()
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
        # Add new state tracking for bidirectional alerts
        monitor._last_alerted_side = {}
        monitor._market_multipliers = {}
        return monitor

    @pytest.fixture
    def sample_opportunity_yes(self) -> Opportunity:
        """Create a sample YES-side opportunity."""
        return Opportunity(
            market_id="market_123",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_abc",
            neg_risk=False,
        )

    @pytest.fixture
    def sample_opportunity_no(self) -> Opportunity:
        """Create a sample NO-side opportunity for the same market."""
        return Opportunity(
            market_id="market_123",
            side="NO",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_def",
            neg_risk=False,
        )

    def test_first_alert_always_allowed(
        self, monitor: PolymarketMonitor, sample_opportunity_yes: Opportunity
    ):
        """Verify first alert for a market is never considered a duplicate."""
        # No prior alerts for this market
        assert "market_123" not in monitor._last_alerted_side

        result = monitor._is_duplicate_opportunity(sample_opportunity_yes)

        assert result is False

    def test_same_side_is_duplicate(
        self, monitor: PolymarketMonitor, sample_opportunity_yes: Opportunity
    ):
        """Verify same-side consecutive alert is blocked as duplicate."""
        # Simulate prior YES alert for this market
        monitor._last_alerted_side["market_123"] = "YES"

        result = monitor._is_duplicate_opportunity(sample_opportunity_yes)

        assert result is True

    def test_opposite_side_not_duplicate(
        self,
        monitor: PolymarketMonitor,
        sample_opportunity_yes: Opportunity,
        sample_opportunity_no: Opportunity,
    ):
        """Verify opposite-side alert is allowed (not a duplicate)."""
        # Simulate prior YES alert for this market
        monitor._last_alerted_side["market_123"] = "YES"

        # NO-side alert should be allowed (reversal)
        result = monitor._is_duplicate_opportunity(sample_opportunity_no)

        assert result is False

    def test_reversal_in_opposite_direction(
        self,
        monitor: PolymarketMonitor,
        sample_opportunity_yes: Opportunity,
        sample_opportunity_no: Opportunity,
    ):
        """Verify reversal works in both directions (NO -> YES)."""
        # Simulate prior NO alert for this market
        monitor._last_alerted_side["market_123"] = "NO"

        # YES-side alert should be allowed (reversal)
        result = monitor._is_duplicate_opportunity(sample_opportunity_yes)

        assert result is False

    def test_different_markets_independent(self, monitor: PolymarketMonitor):
        """Verify different markets have independent duplicate tracking."""
        # Alert on market_123
        monitor._last_alerted_side["market_123"] = "YES"

        # Alert on different market should be allowed
        opp_different_market = Opportunity(
            market_id="market_456",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_xyz",
            neg_risk=False,
        )

        result = monitor._is_duplicate_opportunity(opp_different_market)

        assert result is False


class TestMultiplierAccumulation:
    """Test multiplier accumulation on reversals and reset on cycle clear."""

    @pytest.fixture
    def monitor(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor instance for testing multiplier behavior."""
        config = Config()
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
        monitor._trade_executor = None
        monitor._active_markets = []
        monitor._token_to_market = {}
        monitor._last_prices = {}
        monitor._best_bids = {}
        monitor._window_opportunities = []
        monitor._current_market_closing_time = None
        # Add new state tracking for bidirectional alerts
        monitor._last_alerted_side = {}
        monitor._market_multipliers = {}
        return monitor

    def test_first_alert_initializes_multiplier_to_one(
        self, monitor: PolymarketMonitor
    ):
        """Verify first alert for a market initializes multiplier to 1.0."""
        market_id = "market_123"
        assert market_id not in monitor._market_multipliers

        # Simulate the multiplier initialization logic from _check_opportunity
        if market_id not in monitor._market_multipliers:
            monitor._market_multipliers[market_id] = 1.0

        assert monitor._market_multipliers[market_id] == 1.0

    def test_multiplier_accumulates_on_reversal(self, monitor: PolymarketMonitor):
        """Verify multiplier compounds by reversal_multiplier on each reversal."""
        market_id = "market_123"
        reversal_multiplier = monitor._config.reversal_multiplier  # Default 1.5

        # First alert - initialize multiplier
        monitor._market_multipliers[market_id] = 1.0
        monitor._last_alerted_side[market_id] = "YES"

        # Simulate first reversal (YES -> NO)
        is_reversal = True
        if is_reversal:
            monitor._market_multipliers[market_id] *= reversal_multiplier

        assert monitor._market_multipliers[market_id] == 1.5  # 1.0 * 1.5

        # Simulate second reversal (NO -> YES)
        monitor._market_multipliers[market_id] *= reversal_multiplier

        assert monitor._market_multipliers[market_id] == 2.25  # 1.5 * 1.5

    def test_multiplier_accumulates_with_custom_reversal_value(
        self, monitor: PolymarketMonitor
    ):
        """Verify multiplier accumulation with custom REVERSAL_MULTIPLIER value."""
        market_id = "market_123"
        # Override reversal_multiplier to 2.0 for testing
        monitor._config = Config(reversal_multiplier=2.0)

        # First alert - initialize multiplier
        monitor._market_multipliers[market_id] = 1.0
        monitor._last_alerted_side[market_id] = "YES"

        # First reversal
        monitor._market_multipliers[market_id] *= monitor._config.reversal_multiplier

        assert monitor._market_multipliers[market_id] == 2.0  # 1.0 * 2.0

        # Second reversal
        monitor._market_multipliers[market_id] *= monitor._config.reversal_multiplier

        assert monitor._market_multipliers[market_id] == 4.0  # 2.0 * 2.0

    def test_multiplier_resets_on_clear_state(self, monitor: PolymarketMonitor):
        """Verify multiplier returns to empty dict on _clear_market_state()."""
        # Pre-populate state
        monitor._market_multipliers = {"market_123": 3.375, "market_456": 2.25}
        monitor._last_alerted_side = {"market_123": "NO", "market_456": "YES"}
        monitor._last_prices = {"token_abc": 0.75}
        monitor._best_bids = {"token_abc": 0.74}
        monitor._window_opportunities = [MagicMock()]
        monitor._token_to_market = {"token_abc": MagicMock()}
        monitor._active_markets = [MagicMock()]
        monitor._current_market_closing_time = datetime.now(timezone.utc)

        monitor._clear_market_state()

        # Verify multipliers and alerted sides are cleared
        assert len(monitor._market_multipliers) == 0
        assert len(monitor._last_alerted_side) == 0

    def test_same_side_does_not_increase_multiplier(self, monitor: PolymarketMonitor):
        """Verify multiplier does not increase on same-side consecutive alerts."""
        market_id = "market_123"

        # First alert
        monitor._market_multipliers[market_id] = 1.0
        monitor._last_alerted_side[market_id] = "YES"

        # Same-side alert would be blocked, but even if processed,
        # is_reversal would be False
        last_side = monitor._last_alerted_side.get(market_id)
        is_reversal = last_side is not None and last_side != "YES"

        # Should not be a reversal
        assert is_reversal is False

        # Multiplier should not change
        if is_reversal:
            monitor._market_multipliers[market_id] *= monitor._config.reversal_multiplier

        assert monitor._market_multipliers[market_id] == 1.0

    def test_independent_multipliers_per_market(self, monitor: PolymarketMonitor):
        """Verify each market maintains independent multiplier tracking."""
        reversal_multiplier = monitor._config.reversal_multiplier

        # Initialize two markets
        monitor._market_multipliers["market_1"] = 1.0
        monitor._market_multipliers["market_2"] = 1.0
        monitor._last_alerted_side["market_1"] = "YES"
        monitor._last_alerted_side["market_2"] = "NO"

        # Reversal on market_1 only
        monitor._market_multipliers["market_1"] *= reversal_multiplier

        # market_1 should have increased multiplier
        assert monitor._market_multipliers["market_1"] == 1.5
        # market_2 should remain at 1.0
        assert monitor._market_multipliers["market_2"] == 1.0


class TestReversalFlowIntegration:
    """Integration tests for complete reversal alert flow.

    Tests the full flow from price change through opportunity detection
    to trade execution with progressive multiplier application.
    """

    @pytest.fixture
    def monitor_with_executor(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor with mocked trade executor for integration testing."""
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
        monitor._trade_executor = MagicMock()
        monitor._active_markets = []
        monitor._token_to_market = {}
        monitor._last_prices = {}
        monitor._best_bids = {}
        monitor._window_opportunities = []
        monitor._current_market_closing_time = None
        monitor._last_alerted_side = {}
        monitor._market_multipliers = {}
        return monitor

    def test_reversal_flow_first_alert_uses_multiplier_one(
        self, monitor_with_executor: PolymarketMonitor
    ):
        """Verify first alert for a market uses multiplier 1.0.

        Integration test: price change -> opportunity detection -> trade at 1x.
        """
        # Set up market mapping with token including outcome
        mock_token = MagicMock()
        mock_token.token_id = "token_123"
        mock_token.outcome = "Yes"
        mock_market = MagicMock()
        mock_market.id = "market_abc"
        mock_market.neg_risk = False
        mock_market.tokens = [mock_token]
        monitor_with_executor._token_to_market = {"token_123": mock_market}
        monitor_with_executor._last_prices = {"token_123": 0.85}  # Above threshold

        # Create a YES opportunity that will be detected
        yes_opp = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        # Mock detect_opportunity to return our YES opportunity
        with patch("src.main.detect_opportunity", return_value=[yes_opp]):
            monitor_with_executor._check_opportunity("token_123")

        # Verify trade executor was called with multiplier 1.0 (first alert)
        monitor_with_executor._trade_executor.notify.assert_called_once()
        call_args = monitor_with_executor._trade_executor.notify.call_args
        assert call_args[1]["multiplier"] == 1.0

        # Verify state was updated
        assert monitor_with_executor._last_alerted_side["market_abc"] == "YES"
        assert monitor_with_executor._market_multipliers["market_abc"] == 1.0

    def test_reversal_flow_applies_multiplier_on_reversal(
        self, monitor_with_executor: PolymarketMonitor
    ):
        """Verify multiplier is applied when opposite side alert triggers.

        Integration test: YES alert -> NO alert -> trade at 1.5x (reversal multiplier).
        """
        # Set up market mapping with NO token (for the reversal)
        mock_token = MagicMock()
        mock_token.token_id = "token_123"
        mock_token.outcome = "No"
        mock_market = MagicMock()
        mock_market.id = "market_abc"
        mock_market.neg_risk = False
        mock_market.tokens = [mock_token]
        monitor_with_executor._token_to_market = {"token_123": mock_market}
        monitor_with_executor._last_prices = {"token_123": 0.85}

        # Simulate first YES alert has already happened
        monitor_with_executor._last_alerted_side["market_abc"] = "YES"
        monitor_with_executor._market_multipliers["market_abc"] = 1.0

        # Create a NO opportunity (reversal from YES)
        no_opp = Opportunity(
            market_id="market_abc",
            side="NO",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        # Mock detect_opportunity to return our NO opportunity
        with patch("src.main.detect_opportunity", return_value=[no_opp]):
            monitor_with_executor._check_opportunity("token_123")

        # Verify trade executor was called with multiplier 1.5 (default reversal multiplier)
        monitor_with_executor._trade_executor.notify.assert_called_once()
        call_args = monitor_with_executor._trade_executor.notify.call_args
        assert call_args[1]["multiplier"] == 1.5

        # Verify state was updated
        assert monitor_with_executor._last_alerted_side["market_abc"] == "NO"
        assert monitor_with_executor._market_multipliers["market_abc"] == 1.5

    def test_reversal_flow_multiple_reversals_compound_multiplier(
        self, monitor_with_executor: PolymarketMonitor
    ):
        """Verify multiplier compounds across multiple reversals.

        Integration test: YES (1x) -> NO (1.5x) -> YES (2.25x).
        """
        # Set up market mapping with both YES and NO tokens
        mock_token_yes = MagicMock()
        mock_token_yes.token_id = "token_123"
        mock_token_yes.outcome = "Yes"
        mock_token_no = MagicMock()
        mock_token_no.token_id = "token_456"
        mock_token_no.outcome = "No"
        mock_market = MagicMock()
        mock_market.id = "market_abc"
        mock_market.neg_risk = False
        mock_market.tokens = [mock_token_yes, mock_token_no]
        monitor_with_executor._token_to_market = {"token_123": mock_market}
        monitor_with_executor._last_prices = {"token_123": 0.85}

        # First alert: YES at 1.0x
        yes_opp = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[yes_opp]):
            monitor_with_executor._check_opportunity("token_123")

        assert monitor_with_executor._market_multipliers["market_abc"] == 1.0

        # Second alert: NO at 1.5x (first reversal)
        monitor_with_executor._trade_executor.reset_mock()
        no_opp = Opportunity(
            market_id="market_abc",
            side="NO",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[no_opp]):
            monitor_with_executor._check_opportunity("token_123")

        assert monitor_with_executor._market_multipliers["market_abc"] == 1.5
        call_args = monitor_with_executor._trade_executor.notify.call_args
        assert call_args[1]["multiplier"] == 1.5

        # Third alert: YES at 2.25x (second reversal)
        monitor_with_executor._trade_executor.reset_mock()
        yes_opp_2 = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[yes_opp_2]):
            monitor_with_executor._check_opportunity("token_123")

        assert monitor_with_executor._market_multipliers["market_abc"] == 2.25
        call_args = monitor_with_executor._trade_executor.notify.call_args
        assert call_args[1]["multiplier"] == 2.25

    def test_reversal_flow_same_side_blocked(
        self, monitor_with_executor: PolymarketMonitor
    ):
        """Verify same-side consecutive alerts are blocked.

        Integration test: YES alert -> YES alert (blocked, no trade).
        """
        # Set up market mapping with YES token
        mock_token = MagicMock()
        mock_token.token_id = "token_123"
        mock_token.outcome = "Yes"
        mock_market = MagicMock()
        mock_market.id = "market_abc"
        mock_market.neg_risk = False
        mock_market.tokens = [mock_token]
        monitor_with_executor._token_to_market = {"token_123": mock_market}
        monitor_with_executor._last_prices = {"token_123": 0.85}

        # First YES alert
        yes_opp_1 = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[yes_opp_1]):
            monitor_with_executor._check_opportunity("token_123")

        # Verify first alert went through
        assert monitor_with_executor._trade_executor.notify.call_count == 1

        # Second YES alert (same side - should be blocked)
        monitor_with_executor._trade_executor.reset_mock()
        yes_opp_2 = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.86,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[yes_opp_2]):
            monitor_with_executor._check_opportunity("token_123")

        # Verify second alert was blocked
        monitor_with_executor._trade_executor.notify.assert_not_called()

        # Multiplier should not have changed
        assert monitor_with_executor._market_multipliers["market_abc"] == 1.0

    def test_reversal_flow_with_custom_multiplier(
        self, monitor_with_executor: PolymarketMonitor
    ):
        """Verify custom REVERSAL_MULTIPLIER value is applied correctly.

        Integration test: YES (1x) -> NO (2x) with REVERSAL_MULTIPLIER=2.0.
        """
        # Override config with custom multiplier
        monitor_with_executor._config = Config(reversal_multiplier=2.0)

        # Set up market mapping with both YES and NO tokens
        mock_token_yes = MagicMock()
        mock_token_yes.token_id = "token_123"
        mock_token_yes.outcome = "Yes"
        mock_token_no = MagicMock()
        mock_token_no.token_id = "token_456"
        mock_token_no.outcome = "No"
        mock_market = MagicMock()
        mock_market.id = "market_abc"
        mock_market.neg_risk = False
        mock_market.tokens = [mock_token_yes, mock_token_no]
        monitor_with_executor._token_to_market = {"token_123": mock_market}
        monitor_with_executor._last_prices = {"token_123": 0.85}

        # First alert: YES at 1.0x
        yes_opp = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[yes_opp]):
            monitor_with_executor._check_opportunity("token_123")

        # Second alert: NO at 2.0x (custom reversal multiplier)
        monitor_with_executor._trade_executor.reset_mock()
        no_opp = Opportunity(
            market_id="market_abc",
            side="NO",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_123",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[no_opp]):
            monitor_with_executor._check_opportunity("token_123")

        # Verify custom multiplier was applied
        assert monitor_with_executor._market_multipliers["market_abc"] == 2.0
        call_args = monitor_with_executor._trade_executor.notify.call_args
        assert call_args[1]["multiplier"] == 2.0


class TestCycleBoundaryReset:
    """Integration tests for multiplier reset at monitoring cycle boundaries.

    Tests that all reversal state is properly cleared when transitioning
    to a new monitoring cycle.
    """

    @pytest.fixture
    def monitor_with_state(self) -> PolymarketMonitor:
        """Create a PolymarketMonitor with populated reversal state."""
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
        monitor._trade_executor = MagicMock()

        # Populate with market state
        monitor._active_markets = [MagicMock()]
        monitor._token_to_market = {"token_abc": MagicMock(), "token_def": MagicMock()}
        monitor._last_prices = {"token_abc": 0.75, "token_def": 0.82}
        monitor._best_bids = {"token_abc": 0.74, "token_def": 0.81}
        monitor._window_opportunities = [MagicMock(), MagicMock()]
        monitor._current_market_closing_time = datetime.now(timezone.utc) + timedelta(minutes=10)

        # Populate with reversal state (simulating previous reversals)
        monitor._last_alerted_side = {
            "market_abc": "NO",   # Had reversal from YES to NO
            "market_def": "YES",  # First alert only
        }
        monitor._market_multipliers = {
            "market_abc": 3.375,  # After 3 reversals (1.5^3)
            "market_def": 1.5,    # After 1 reversal
        }

        return monitor

    def test_cycle_boundary_clears_multipliers(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify multiplier state is cleared on cycle transition.

        Integration test: Populated state -> _clear_market_state() -> Empty multipliers.
        """
        # Verify pre-populated state
        assert len(monitor_with_state._market_multipliers) == 2
        assert monitor_with_state._market_multipliers["market_abc"] == 3.375
        assert monitor_with_state._market_multipliers["market_def"] == 1.5

        # Simulate cycle transition
        monitor_with_state._clear_market_state()

        # Verify multipliers are reset
        assert len(monitor_with_state._market_multipliers) == 0

    def test_cycle_boundary_clears_last_alerted_side(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify last-alerted-side tracking is cleared on cycle transition.

        Integration test: Populated state -> _clear_market_state() -> Empty tracking.
        """
        # Verify pre-populated state
        assert len(monitor_with_state._last_alerted_side) == 2
        assert monitor_with_state._last_alerted_side["market_abc"] == "NO"
        assert monitor_with_state._last_alerted_side["market_def"] == "YES"

        # Simulate cycle transition
        monitor_with_state._clear_market_state()

        # Verify tracking is reset
        assert len(monitor_with_state._last_alerted_side) == 0

    def test_cycle_boundary_new_alerts_start_fresh(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify new alerts after cycle transition start with multiplier 1.0.

        Integration test: Clear state -> New alert -> Multiplier is 1.0 (not previous value).
        """
        # Simulate cycle transition
        monitor_with_state._clear_market_state()

        # Set up for new market discovery with token including outcome
        mock_token = MagicMock()
        mock_token.token_id = "token_new"
        mock_token.outcome = "Yes"
        mock_market = MagicMock()
        mock_market.id = "market_abc"  # Same market ID as before
        mock_market.neg_risk = False
        mock_market.tokens = [mock_token]
        monitor_with_state._token_to_market = {"token_new": mock_market}
        monitor_with_state._last_prices = {"token_new": 0.85}

        # New YES alert on same market that previously had multiplier 3.375
        yes_opp = Opportunity(
            market_id="market_abc",
            side="YES",
            price=0.85,
            detected_at=datetime.now(timezone.utc),
            source="last_trade",
            token_id="token_new",
            neg_risk=False,
        )

        with patch("src.main.detect_opportunity", return_value=[yes_opp]):
            monitor_with_state._check_opportunity("token_new")

        # Verify multiplier is 1.0 (fresh start, not 3.375 from previous cycle)
        assert monitor_with_state._market_multipliers["market_abc"] == 1.0
        call_args = monitor_with_state._trade_executor.notify.call_args
        assert call_args[1]["multiplier"] == 1.0

    def test_cycle_boundary_transition_clears_all_state(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify _transition_to_next_market clears reversal state.

        Integration test: Full transition -> All state cleared including reversals.
        """
        # Create mock for new market with token including outcome
        new_market = MagicMock()
        new_token = MagicMock()
        new_token.token_id = "new_token_xyz"
        new_token.outcome = "Yes"
        new_market.tokens = [new_token]

        # Verify pre-populated reversal state
        assert len(monitor_with_state._market_multipliers) == 2
        assert len(monitor_with_state._last_alerted_side) == 2

        with patch.object(monitor_with_state, "_stop_websocket"), patch.object(
            monitor_with_state, "_discover_markets", return_value=[new_market]
        ), patch.object(monitor_with_state, "_start_websocket", return_value=True):
            result = monitor_with_state._transition_to_next_market()

        # Verify transition succeeded
        assert result is True

        # Verify reversal state was cleared
        assert len(monitor_with_state._market_multipliers) == 0
        assert len(monitor_with_state._last_alerted_side) == 0

    def test_cycle_boundary_failed_transition_still_clears_state(
        self, monitor_with_state: PolymarketMonitor
    ):
        """Verify state is cleared even when transition fails (no markets available).

        Integration test: Failed transition -> State still cleared (no stale data).
        """
        # Verify pre-populated reversal state
        assert len(monitor_with_state._market_multipliers) == 2
        assert len(monitor_with_state._last_alerted_side) == 2

        with patch.object(monitor_with_state, "_stop_websocket"), patch.object(
            monitor_with_state, "_discover_markets", return_value=[]  # No markets
        ):
            result = monitor_with_state._transition_to_next_market()

        # Verify transition failed
        assert result is False

        # Verify reversal state was still cleared (no stale data from previous cycle)
        assert len(monitor_with_state._market_multipliers) == 0
        assert len(monitor_with_state._last_alerted_side) == 0
