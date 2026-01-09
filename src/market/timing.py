"""15-minute market window timing calculations.

This module provides timing utilities for tracking 15-minute crypto market
windows on Polymarket. Markets operate in 15-minute intervals starting at
:00, :15, :30, and :45 past each hour.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MarketWindow:
    """Represents a 15-minute market window.

    Attributes:
        start: Window start time (aligned to 0, 15, 30, or 45 minutes).
        end: Window end time (15 minutes after start).
    """

    start: datetime
    end: datetime

    @property
    def duration(self) -> timedelta:
        """Total duration of the window."""
        return self.end - self.start

    def contains(self, dt: datetime) -> bool:
        """Check if a datetime falls within this window.

        Args:
            dt: The datetime to check.

        Returns:
            True if dt is within [start, end), False otherwise.
        """
        return self.start <= dt < self.end

    def time_until_end(self, from_time: Optional[datetime] = None) -> timedelta:
        """Calculate time remaining until window ends.

        Args:
            from_time: Reference time (defaults to now).

        Returns:
            Time remaining until window end. Returns zero if window has ended.
        """
        if from_time is None:
            from_time = datetime.now()
        remaining = self.end - from_time
        return remaining if remaining > timedelta(0) else timedelta(0)


def get_window_for_time(dt: datetime) -> MarketWindow:
    """Get the market window containing a specific datetime.

    Market windows are 15-minute intervals:
    - :00 to :15 (minutes 0-14)
    - :15 to :30 (minutes 15-29)
    - :30 to :45 (minutes 30-44)
    - :45 to :00 (minutes 45-59)

    Args:
        dt: The datetime to find the window for.

    Returns:
        MarketWindow containing the given datetime.

    Example:
        >>> dt = datetime(2024, 1, 15, 10, 37, 45)
        >>> window = get_window_for_time(dt)
        >>> print(window.start.minute)  # 30
        >>> print(window.end.minute)    # 45
    """
    # Calculate window start by flooring to nearest 15-minute boundary
    window_start_minute = (dt.minute // 15) * 15
    window_start = dt.replace(minute=window_start_minute, second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=15)

    return MarketWindow(start=window_start, end=window_end)


def get_current_market_window() -> tuple[datetime, datetime]:
    """Get the start and end times of the current 15-minute market window.

    This is the primary function for determining the active trading window.
    Windows are aligned to 0, 15, 30, and 45 minutes past each hour.

    Returns:
        Tuple of (window_start, window_end) datetimes.

    Example:
        >>> start, end = get_current_market_window()
        >>> print(f"Current window: {start.strftime('%H:%M')} - {end.strftime('%H:%M')}")
    """
    now = datetime.now()
    window = get_window_for_time(now)
    logger.debug(
        "Current market window: %s - %s",
        window.start.strftime("%H:%M:%S"),
        window.end.strftime("%H:%M:%S"),
    )
    return window.start, window.end


def get_next_window() -> MarketWindow:
    """Get the next upcoming market window.

    Returns:
        MarketWindow for the next 15-minute period after the current window.
    """
    now = datetime.now()
    current_window = get_window_for_time(now)
    next_start = current_window.end
    next_end = next_start + timedelta(minutes=15)
    return MarketWindow(start=next_start, end=next_end)


def should_start_monitoring(minutes_before_end: int = 3) -> bool:
    """Check if we should be monitoring for opportunities.

    Monitoring occurs during the final minutes of each 15-minute window.
    By default, this is the last 3 minutes (from minute 12-15, 27-30, etc.).

    Args:
        minutes_before_end: Number of minutes before window end to start
            monitoring. Default is 3 minutes.

    Returns:
        True if current time is within the monitoring window, False otherwise.

    Example:
        >>> if should_start_monitoring():
        ...     print("Start monitoring for opportunities!")
    """
    window_start, window_end = get_current_market_window()
    now = datetime.now()
    monitor_start = window_end - timedelta(minutes=minutes_before_end)

    is_monitoring = monitor_start <= now < window_end

    if is_monitoring:
        logger.debug(
            "Within monitoring window (started at %s, ends at %s)",
            monitor_start.strftime("%H:%M:%S"),
            window_end.strftime("%H:%M:%S"),
        )
    else:
        logger.debug(
            "Not in monitoring window. Monitoring starts at %s",
            monitor_start.strftime("%H:%M:%S"),
        )

    return is_monitoring


def time_until_monitoring_starts(minutes_before_end: int = 3) -> timedelta:
    """Calculate time until the monitoring period begins.

    The monitoring period starts a configurable number of minutes before
    the end of each 15-minute window.

    Args:
        minutes_before_end: Number of minutes before window end that
            monitoring starts. Default is 3 minutes.

    Returns:
        Time until monitoring should start. Returns zero if already in
        monitoring window or if monitoring period has passed.

    Example:
        >>> wait_time = time_until_monitoring_starts()
        >>> if wait_time > timedelta(0):
        ...     print(f"Wait {wait_time.total_seconds():.0f}s before monitoring")
    """
    window_start, window_end = get_current_market_window()
    now = datetime.now()
    monitor_start = window_end - timedelta(minutes=minutes_before_end)

    # If we're already in or past the monitoring window
    if now >= monitor_start:
        return timedelta(0)

    remaining = monitor_start - now
    logger.debug(
        "Time until monitoring: %s (monitoring starts at %s)",
        remaining,
        monitor_start.strftime("%H:%M:%S"),
    )
    return remaining


def time_until_window_ends() -> timedelta:
    """Calculate time remaining in the current market window.

    Returns:
        Time until the current 15-minute window ends.
    """
    _, window_end = get_current_market_window()
    now = datetime.now()
    remaining = window_end - now
    return remaining if remaining > timedelta(0) else timedelta(0)


def get_monitoring_window_times(minutes_before_end: int = 3) -> tuple[datetime, datetime]:
    """Get the start and end times of the current monitoring period.

    Args:
        minutes_before_end: Number of minutes before window end that
            monitoring starts. Default is 3 minutes.

    Returns:
        Tuple of (monitor_start, monitor_end) datetimes for the current
        window's monitoring period.
    """
    _, window_end = get_current_market_window()
    monitor_start = window_end - timedelta(minutes=minutes_before_end)
    return monitor_start, window_end


def format_window_info(minutes_before_end: int = 3) -> str:
    """Get a formatted string describing the current window and monitoring status.

    Args:
        minutes_before_end: Number of minutes before window end for monitoring.

    Returns:
        Human-readable string with window and monitoring timing information.
    """
    window_start, window_end = get_current_market_window()
    monitor_start, _ = get_monitoring_window_times(minutes_before_end)
    now = datetime.now()

    is_monitoring = should_start_monitoring(minutes_before_end)
    time_to_monitor = time_until_monitoring_starts(minutes_before_end)
    time_to_end = time_until_window_ends()

    if is_monitoring:
        status = f"MONITORING (ends in {time_to_end.total_seconds():.0f}s)"
    else:
        status = f"WAITING (monitoring in {time_to_monitor.total_seconds():.0f}s)"

    return (
        f"Window: {window_start.strftime('%H:%M')} - {window_end.strftime('%H:%M')} | "
        f"Monitor: {monitor_start.strftime('%H:%M')} | "
        f"Status: {status}"
    )
