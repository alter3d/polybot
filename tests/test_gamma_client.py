"""Tests for Gamma API client.

Tests the GammaClient methods including time parsing and event selection logic.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from src.api.gamma_client import GammaClient
from src.config import Config


class TestParseMarketClosingTime:
    """Test _parse_market_closing_time helper method."""

    @pytest.fixture
    def client(self) -> GammaClient:
        """Create a GammaClient instance for testing."""
        config = Config()
        return GammaClient(config)

    def test_parse_standard_pm_format(self, client: GammaClient):
        """Verify parsing standard PM format extracts correct closing time."""
        # Reference date: January 9, 2026 at 7:54PM ET
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 19, 54, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 8:15PM-8:30PM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        # Expected: January 9, 2026 8:30PM ET = 01:30 UTC (next day)
        expected_et = datetime(2026, 1, 9, 20, 30, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_am_format(self, client: GammaClient):
        """Verify parsing AM format extracts correct closing time."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 9, 30, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 9:45AM-10:00AM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        # Expected: January 9, 2026 10:00AM ET
        expected_et = datetime(2026, 1, 9, 10, 0, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_noon_boundary(self, client: GammaClient):
        """Verify parsing handles 12:00PM (noon) correctly."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 11, 50, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 11:45AM-12:00PM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        # Expected: January 9, 2026 12:00PM ET (noon)
        expected_et = datetime(2026, 1, 9, 12, 0, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_midnight_boundary(self, client: GammaClient):
        """Verify parsing handles midnight boundary (11:45PM-12:00AM) correctly."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 23, 50, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 11:45PM-12:00AM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        # Expected: January 10, 2026 12:00AM ET (next day)
        expected_et = datetime(2026, 1, 10, 0, 0, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_12am_format(self, client: GammaClient):
        """Verify parsing 12:00AM start time (midnight) is handled correctly."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 10, 0, 5, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 10, 12:00AM-12:15AM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        # Expected: January 10, 2026 12:15AM ET
        expected_et = datetime(2026, 1, 10, 0, 15, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_handles_timezone_conversion(self, client: GammaClient):
        """Verify ET to UTC timezone conversion is correct."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 15, 0, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 3:00PM-3:15PM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        # January 9, 2026 3:15PM ET should be 8:15PM UTC (EST is UTC-5 in January)
        assert result.tzinfo == timezone.utc
        # Verify the time components
        expected_et = datetime(2026, 1, 9, 15, 15, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_invalid_format_returns_none(self, client: GammaClient):
        """Verify invalid title format returns None."""
        reference = datetime(2026, 1, 9, 20, 0, 0, tzinfo=timezone.utc)

        # Title without proper time range format
        title = "Bitcoin Up or Down - January 9"
        result = client._parse_market_closing_time(title, reference)

        assert result is None

    def test_parse_missing_time_range_returns_none(self, client: GammaClient):
        """Verify title without time range returns None."""
        reference = datetime(2026, 1, 9, 20, 0, 0, tzinfo=timezone.utc)

        # Title with date but no time range
        title = "Bitcoin Up or Down - January 9, 2026"
        result = client._parse_market_closing_time(title, reference)

        assert result is None

    def test_parse_empty_title_returns_none(self, client: GammaClient):
        """Verify empty title returns None."""
        reference = datetime(2026, 1, 9, 20, 0, 0, tzinfo=timezone.utc)

        result = client._parse_market_closing_time("", reference)
        assert result is None

    def test_parse_none_title_returns_none(self, client: GammaClient):
        """Verify None title returns None."""
        reference = datetime(2026, 1, 9, 20, 0, 0, tzinfo=timezone.utc)

        # The method signature expects str, but we test None handling
        result = client._parse_market_closing_time(None, reference)  # type: ignore
        assert result is None

    def test_parse_malformed_time_returns_none(self, client: GammaClient):
        """Verify malformed time in title returns None."""
        reference = datetime(2026, 1, 9, 20, 0, 0, tzinfo=timezone.utc)

        # Malformed time (missing minutes)
        title = "Bitcoin Up or Down - January 9, 8PM-8:30PM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is None

    def test_parse_invalid_month_returns_none(self, client: GammaClient):
        """Verify invalid month name returns None."""
        reference = datetime(2026, 1, 9, 20, 0, 0, tzinfo=timezone.utc)

        title = "Bitcoin Up or Down - Janury 9, 8:15PM-8:30PM ET"  # Misspelled month
        result = client._parse_market_closing_time(title, reference)

        assert result is None

    def test_parse_different_months(self, client: GammaClient):
        """Verify parsing works for different months."""
        et_tz = ZoneInfo("America/New_York")

        test_cases = [
            ("February 14", 2),
            ("March 15", 3),
            ("April 1", 4),
            ("May 5", 5),
            ("June 21", 6),
            ("July 4", 7),
            ("August 15", 8),
            ("September 1", 9),
            ("October 31", 10),
            ("November 25", 11),
            ("December 25", 12),
        ]

        for month_day, expected_month in test_cases:
            reference = datetime(2026, expected_month, 1, 12, 0, 0, tzinfo=et_tz).astimezone(timezone.utc)
            title = f"Bitcoin Up or Down - {month_day}, 1:00PM-1:15PM ET"
            result = client._parse_market_closing_time(title, reference)

            assert result is not None, f"Failed to parse month: {month_day}"
            # Convert back to ET to verify the month
            result_et = result.astimezone(et_tz)
            assert result_et.month == expected_month, f"Wrong month for {month_day}"

    def test_parse_without_reference_date_uses_current_time(self, client: GammaClient):
        """Verify parsing works without explicit reference date."""
        title = "Bitcoin Up or Down - January 9, 8:15PM-8:30PM ET"

        # Call without reference_date parameter
        result = client._parse_market_closing_time(title)

        # Should return a valid datetime (not None)
        # We can't assert exact value since it depends on current year
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_parse_case_insensitive_am_pm(self, client: GammaClient):
        """Verify AM/PM parsing is case insensitive."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 15, 0, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Test lowercase am/pm
        title = "Bitcoin Up or Down - January 9, 3:00pm-3:15pm ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        expected_et = datetime(2026, 1, 9, 15, 15, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_single_digit_hour(self, client: GammaClient):
        """Verify parsing handles single-digit hours correctly."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 8, 0, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 8:00AM-8:15AM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        expected_et = datetime(2026, 1, 9, 8, 15, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc

    def test_parse_double_digit_hour(self, client: GammaClient):
        """Verify parsing handles double-digit hours correctly."""
        et_tz = ZoneInfo("America/New_York")
        reference = datetime(2026, 1, 9, 10, 0, 0, tzinfo=et_tz).astimezone(timezone.utc)

        title = "Bitcoin Up or Down - January 9, 10:00AM-10:15AM ET"
        result = client._parse_market_closing_time(title, reference)

        assert result is not None
        expected_et = datetime(2026, 1, 9, 10, 15, 0, tzinfo=et_tz)
        expected_utc = expected_et.astimezone(timezone.utc)
        assert result == expected_utc
