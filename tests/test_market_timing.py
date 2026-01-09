"""Tests for market timing calculations.

Tests the 15-minute market window timing logic including boundary
calculations, monitoring window detection, and time calculations.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from src.market.timing import (
    MarketWindow,
    format_window_info,
    get_current_market_window,
    get_monitoring_window_times,
    get_next_window,
    get_window_for_time,
    should_start_monitoring,
    time_until_monitoring_starts,
    time_until_window_ends,
)


class TestMarketWindow:
    """Test MarketWindow dataclass."""

    def test_market_window_creation(self):
        """Verify MarketWindow can be created with start and end times."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)
        assert window.start == start
        assert window.end == end

    def test_market_window_duration(self):
        """Verify duration property returns correct timedelta."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)
        assert window.duration == timedelta(minutes=15)

    def test_market_window_contains_within(self):
        """Verify contains returns True for time within window."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)

        # Middle of window
        assert window.contains(datetime(2024, 1, 15, 10, 7, 30))

    def test_market_window_contains_start_boundary(self):
        """Verify contains returns True for exact start time (inclusive)."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)
        assert window.contains(start)

    def test_market_window_contains_end_boundary(self):
        """Verify contains returns False for exact end time (exclusive)."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)
        assert not window.contains(end)

    def test_market_window_contains_before(self):
        """Verify contains returns False for time before window."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)
        assert not window.contains(datetime(2024, 1, 15, 9, 59, 59))

    def test_market_window_contains_after(self):
        """Verify contains returns False for time after window."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)
        assert not window.contains(datetime(2024, 1, 15, 10, 15, 1))

    def test_market_window_time_until_end(self):
        """Verify time_until_end calculates remaining time correctly."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)

        from_time = datetime(2024, 1, 15, 10, 10, 0)
        remaining = window.time_until_end(from_time)
        assert remaining == timedelta(minutes=5)

    def test_market_window_time_until_end_at_start(self):
        """Verify time_until_end returns full duration at window start."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)

        remaining = window.time_until_end(start)
        assert remaining == timedelta(minutes=15)

    def test_market_window_time_until_end_after_window(self):
        """Verify time_until_end returns zero after window ends."""
        start = datetime(2024, 1, 15, 10, 0, 0)
        end = datetime(2024, 1, 15, 10, 15, 0)
        window = MarketWindow(start=start, end=end)

        from_time = datetime(2024, 1, 15, 10, 20, 0)
        remaining = window.time_until_end(from_time)
        assert remaining == timedelta(0)


class TestGetWindowForTime:
    """Test get_window_for_time function."""

    def test_window_for_minute_00(self):
        """Verify window calculation for :00-:15 boundary."""
        dt = datetime(2024, 1, 15, 10, 0, 0)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 0, 0)
        assert window.end == datetime(2024, 1, 15, 10, 15, 0)

    def test_window_for_minute_05(self):
        """Verify window for time in first quarter."""
        dt = datetime(2024, 1, 15, 10, 5, 30)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 0, 0)
        assert window.end == datetime(2024, 1, 15, 10, 15, 0)

    def test_window_for_minute_14(self):
        """Verify window for time just before :15 boundary."""
        dt = datetime(2024, 1, 15, 10, 14, 59)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 0, 0)
        assert window.end == datetime(2024, 1, 15, 10, 15, 0)

    def test_window_for_minute_15(self):
        """Verify window calculation for :15-:30 boundary."""
        dt = datetime(2024, 1, 15, 10, 15, 0)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 15, 0)
        assert window.end == datetime(2024, 1, 15, 10, 30, 0)

    def test_window_for_minute_22(self):
        """Verify window for time in second quarter."""
        dt = datetime(2024, 1, 15, 10, 22, 45)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 15, 0)
        assert window.end == datetime(2024, 1, 15, 10, 30, 0)

    def test_window_for_minute_30(self):
        """Verify window calculation for :30-:45 boundary."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 30, 0)
        assert window.end == datetime(2024, 1, 15, 10, 45, 0)

    def test_window_for_minute_37(self):
        """Verify window for time in third quarter."""
        dt = datetime(2024, 1, 15, 10, 37, 15)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 30, 0)
        assert window.end == datetime(2024, 1, 15, 10, 45, 0)

    def test_window_for_minute_45(self):
        """Verify window calculation for :45-:00 boundary."""
        dt = datetime(2024, 1, 15, 10, 45, 0)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 45, 0)
        assert window.end == datetime(2024, 1, 15, 11, 0, 0)

    def test_window_for_minute_55(self):
        """Verify window for time in fourth quarter."""
        dt = datetime(2024, 1, 15, 10, 55, 30)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 45, 0)
        assert window.end == datetime(2024, 1, 15, 11, 0, 0)

    def test_window_for_minute_59(self):
        """Verify window for time just before hour boundary."""
        dt = datetime(2024, 1, 15, 10, 59, 59)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 10, 45, 0)
        assert window.end == datetime(2024, 1, 15, 11, 0, 0)

    def test_window_clears_seconds_microseconds(self):
        """Verify window start has seconds and microseconds cleared."""
        dt = datetime(2024, 1, 15, 10, 23, 45, 123456)
        window = get_window_for_time(dt)
        assert window.start.second == 0
        assert window.start.microsecond == 0

    def test_window_duration_is_15_minutes(self):
        """Verify all windows are exactly 15 minutes."""
        for minute in [0, 15, 30, 45]:
            dt = datetime(2024, 1, 15, 10, minute, 0)
            window = get_window_for_time(dt)
            assert window.duration == timedelta(minutes=15)


class TestGetCurrentMarketWindow:
    """Test get_current_market_window function."""

    def test_returns_tuple_of_datetimes(self):
        """Verify function returns tuple of two datetimes."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 22, 30)
            start, end = get_current_market_window()
            assert isinstance(start, datetime)
            assert isinstance(end, datetime)

    def test_returns_correct_window_at_minute_5(self):
        """Verify correct window returned for minute 5."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            start, end = get_current_market_window()
            assert start == datetime(2024, 1, 15, 10, 0, 0)
            assert end == datetime(2024, 1, 15, 10, 15, 0)

    def test_returns_correct_window_at_minute_20(self):
        """Verify correct window returned for minute 20."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 20, 0)
            start, end = get_current_market_window()
            assert start == datetime(2024, 1, 15, 10, 15, 0)
            assert end == datetime(2024, 1, 15, 10, 30, 0)

    def test_returns_correct_window_at_minute_35(self):
        """Verify correct window returned for minute 35."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 35, 0)
            start, end = get_current_market_window()
            assert start == datetime(2024, 1, 15, 10, 30, 0)
            assert end == datetime(2024, 1, 15, 10, 45, 0)

    def test_returns_correct_window_at_minute_50(self):
        """Verify correct window returned for minute 50."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 50, 0)
            start, end = get_current_market_window()
            assert start == datetime(2024, 1, 15, 10, 45, 0)
            assert end == datetime(2024, 1, 15, 11, 0, 0)


class TestGetNextWindow:
    """Test get_next_window function."""

    def test_next_window_from_first_quarter(self):
        """Verify next window from :00-:15 is :15-:30."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            window = get_next_window()
            assert window.start == datetime(2024, 1, 15, 10, 15, 0)
            assert window.end == datetime(2024, 1, 15, 10, 30, 0)

    def test_next_window_from_last_quarter(self):
        """Verify next window from :45-:00 crosses hour boundary."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 50, 0)
            window = get_next_window()
            assert window.start == datetime(2024, 1, 15, 11, 0, 0)
            assert window.end == datetime(2024, 1, 15, 11, 15, 0)

    def test_next_window_duration_is_15_minutes(self):
        """Verify next window is always 15 minutes."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 22, 0)
            window = get_next_window()
            assert window.duration == timedelta(minutes=15)


class TestShouldStartMonitoring:
    """Test should_start_monitoring function."""

    def test_not_monitoring_at_window_start(self):
        """Verify monitoring is False at start of window (default 3 min)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 0, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert not should_start_monitoring()

    def test_not_monitoring_at_minute_11(self):
        """Verify monitoring is False at minute 11 (before last 3 min)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 11, 59)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert not should_start_monitoring()

    def test_monitoring_at_minute_12(self):
        """Verify monitoring is True at minute 12 (within last 3 min)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 12, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring()

    def test_monitoring_at_minute_14(self):
        """Verify monitoring is True at minute 14 (within last 3 min)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 14, 30)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring()

    def test_not_monitoring_at_exact_boundary(self):
        """Verify monitoring is False at exact window end (next window)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 15, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            # At :15, we're in the new window, not monitoring yet
            assert not should_start_monitoring()

    def test_monitoring_with_custom_minutes(self):
        """Verify monitoring respects custom minutes_before_end."""
        with patch("src.market.timing.datetime") as mock_dt:
            # At minute 10, with 5 minutes before end, should be monitoring
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 10, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring(minutes_before_end=5)

    def test_not_monitoring_with_custom_minutes(self):
        """Verify not monitoring outside custom minutes_before_end."""
        with patch("src.market.timing.datetime") as mock_dt:
            # At minute 12, with 2 minutes before end, should not be monitoring yet
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 12, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert not should_start_monitoring(minutes_before_end=2)

    def test_monitoring_in_second_quarter(self):
        """Verify monitoring works in :15-:30 window."""
        with patch("src.market.timing.datetime") as mock_dt:
            # Minute 27 is within last 3 minutes of :15-:30 window
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 27, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring()

    def test_monitoring_in_third_quarter(self):
        """Verify monitoring works in :30-:45 window."""
        with patch("src.market.timing.datetime") as mock_dt:
            # Minute 42 is within last 3 minutes of :30-:45 window
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 42, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring()

    def test_monitoring_in_fourth_quarter(self):
        """Verify monitoring works in :45-:00 window."""
        with patch("src.market.timing.datetime") as mock_dt:
            # Minute 57 is within last 3 minutes of :45-:00 window
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 57, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring()


class TestTimeUntilMonitoringStarts:
    """Test time_until_monitoring_starts function."""

    def test_time_until_monitoring_at_window_start(self):
        """Verify time until monitoring at start of window."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 0, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            time_remaining = time_until_monitoring_starts()
            # 12 minutes until monitoring starts (at minute 12)
            assert time_remaining == timedelta(minutes=12)

    def test_time_until_monitoring_at_minute_5(self):
        """Verify time until monitoring at minute 5."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            time_remaining = time_until_monitoring_starts()
            # 7 minutes until monitoring starts (at minute 12)
            assert time_remaining == timedelta(minutes=7)

    def test_time_until_monitoring_during_monitoring(self):
        """Verify zero returned when already in monitoring window."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 13, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            time_remaining = time_until_monitoring_starts()
            assert time_remaining == timedelta(0)

    def test_time_until_monitoring_with_custom_minutes(self):
        """Verify custom minutes_before_end affects calculation."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            # With 5 minutes before end, monitoring starts at minute 10
            time_remaining = time_until_monitoring_starts(minutes_before_end=5)
            assert time_remaining == timedelta(minutes=5)

    def test_time_until_monitoring_at_exact_start(self):
        """Verify zero returned at exact monitoring start time."""
        with patch("src.market.timing.datetime") as mock_dt:
            # Exactly at minute 12 when monitoring starts
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 12, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            time_remaining = time_until_monitoring_starts()
            assert time_remaining == timedelta(0)


class TestTimeUntilWindowEnds:
    """Test time_until_window_ends function."""

    def test_time_until_end_at_window_start(self):
        """Verify full 15 minutes at window start."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 0, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            remaining = time_until_window_ends()
            assert remaining == timedelta(minutes=15)

    def test_time_until_end_at_minute_10(self):
        """Verify 5 minutes remaining at minute 10."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 10, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            remaining = time_until_window_ends()
            assert remaining == timedelta(minutes=5)

    def test_time_until_end_with_seconds(self):
        """Verify correct calculation with seconds."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 14, 30)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            remaining = time_until_window_ends()
            assert remaining == timedelta(seconds=30)

    def test_time_until_end_returns_zero_after_window(self):
        """Verify zero returned after window theoretically ends."""
        with patch("src.market.timing.datetime") as mock_dt:
            # At exact window end, we're in new window, so 15 minutes remaining
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 15, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            remaining = time_until_window_ends()
            # At :15, we're in new window (:15-:30), 15 minutes remain
            assert remaining == timedelta(minutes=15)


class TestGetMonitoringWindowTimes:
    """Test get_monitoring_window_times function."""

    def test_monitoring_times_default(self):
        """Verify default monitoring window times (last 3 minutes)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            start, end = get_monitoring_window_times()
            assert start == datetime(2024, 1, 15, 10, 12, 0)
            assert end == datetime(2024, 1, 15, 10, 15, 0)

    def test_monitoring_times_custom_minutes(self):
        """Verify custom minutes_before_end affects monitoring start."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            start, end = get_monitoring_window_times(minutes_before_end=5)
            assert start == datetime(2024, 1, 15, 10, 10, 0)
            assert end == datetime(2024, 1, 15, 10, 15, 0)

    def test_monitoring_times_in_second_quarter(self):
        """Verify monitoring times in :15-:30 window."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 20, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            start, end = get_monitoring_window_times()
            assert start == datetime(2024, 1, 15, 10, 27, 0)
            assert end == datetime(2024, 1, 15, 10, 30, 0)

    def test_monitoring_times_in_fourth_quarter(self):
        """Verify monitoring times in :45-:00 window (crosses hour)."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 50, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            start, end = get_monitoring_window_times()
            assert start == datetime(2024, 1, 15, 10, 57, 0)
            assert end == datetime(2024, 1, 15, 11, 0, 0)


class TestFormatWindowInfo:
    """Test format_window_info function."""

    def test_format_window_info_waiting(self):
        """Verify format when not in monitoring window."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            info = format_window_info()
            assert "10:00" in info
            assert "10:15" in info
            assert "WAITING" in info

    def test_format_window_info_monitoring(self):
        """Verify format when in monitoring window."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 13, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            info = format_window_info()
            assert "10:00" in info
            assert "10:15" in info
            assert "MONITORING" in info

    def test_format_window_info_contains_monitor_time(self):
        """Verify monitor start time is included in output."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 5, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            info = format_window_info()
            assert "10:12" in info  # Default 3 minutes before end


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_midnight_boundary(self):
        """Verify window calculation at midnight."""
        dt = datetime(2024, 1, 15, 0, 0, 0)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 0, 0, 0)
        assert window.end == datetime(2024, 1, 15, 0, 15, 0)

    def test_day_boundary_crossing(self):
        """Verify window that would cross day boundary."""
        dt = datetime(2024, 1, 15, 23, 50, 0)
        window = get_window_for_time(dt)
        assert window.start == datetime(2024, 1, 15, 23, 45, 0)
        assert window.end == datetime(2024, 1, 16, 0, 0, 0)

    def test_window_at_exact_15_minute_boundary(self):
        """Verify behavior at exact 15-minute boundaries."""
        for minute in [0, 15, 30, 45]:
            dt = datetime(2024, 1, 15, 10, minute, 0)
            window = get_window_for_time(dt)
            assert window.start.minute == minute

    def test_monitor_minutes_one(self):
        """Verify monitoring with 1 minute before end."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 14, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring(minutes_before_end=1)

    def test_monitor_minutes_full_window(self):
        """Verify monitoring for full 15-minute window."""
        with patch("src.market.timing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2024, 1, 15, 10, 0, 0)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs) if args else mock_dt.now.return_value
            assert should_start_monitoring(minutes_before_end=15)

    def test_microsecond_precision_cleared(self):
        """Verify microseconds don't affect window calculation."""
        dt = datetime(2024, 1, 15, 10, 7, 30, 999999)
        window = get_window_for_time(dt)
        assert window.start.microsecond == 0
        assert window.end.microsecond == 0
