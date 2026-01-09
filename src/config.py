"""Configuration management for Polymarket monitoring.

This module provides a Config dataclass for managing application settings
with support for environment variable overrides.
"""

from dataclasses import dataclass, field
from os import environ


@dataclass
class Config:
    """Application configuration with sensible defaults.

    Attributes:
        opportunity_threshold: Price threshold for opportunity detection (default: 0.70)
        shares_to_trade: Number of shares per trade for future use (default: 20)
        monitor_start_minutes_before_end: Minutes before window end to start monitoring (default: 3)
        clob_host: CLOB API endpoint
        gamma_host: Gamma API endpoint for market discovery
        ws_host: WebSocket endpoint for real-time market data
        log_level: Logging verbosity level (default: INFO)
        series_ids: List of Polymarket series IDs to monitor. A series is a higher-level
                   abstraction that encapsulates all recurring instances of an event/market
                   that are identical other than the time period they cover.
    """

    # Trading parameters
    opportunity_threshold: float = 0.70  # Price threshold for opportunities
    shares_to_trade: int = 20  # Default shares per trade (future use)

    # Timing parameters
    monitor_start_minutes_before_end: int = 3  # Start monitoring 3 min before window end

    # API endpoints
    clob_host: str = "https://clob.polymarket.com"
    gamma_host: str = "https://gamma-api.polymarket.com"
    ws_host: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Logging
    log_level: str = "INFO"

    # Series-based market selection
    # User configures series IDs they want to trade on
    series_ids: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "Config":
        """Create a Config instance from environment variables.

        Environment variables:
            OPPORTUNITY_THRESHOLD: Price threshold (default: 0.70)
            SHARES_TO_TRADE: Number of shares per trade (default: 20)
            MONITOR_START_MINUTES: Minutes before window end to start monitoring (default: 3)
            LOG_LEVEL: Logging verbosity (default: INFO)
            SERIES_IDS: Comma-separated list of Polymarket series IDs to monitor

        Returns:
            Config instance with values from environment or defaults.
        """
        # Parse comma-separated list for series IDs
        series_str = environ.get("SERIES_IDS", "")
        series_ids = [s.strip() for s in series_str.split(",") if s.strip()]

        return cls(
            opportunity_threshold=float(environ.get("OPPORTUNITY_THRESHOLD", "0.70")),
            shares_to_trade=int(environ.get("SHARES_TO_TRADE", "20")),
            monitor_start_minutes_before_end=int(environ.get("MONITOR_START_MINUTES", "3")),
            log_level=environ.get("LOG_LEVEL", "INFO"),
            series_ids=series_ids,
        )
