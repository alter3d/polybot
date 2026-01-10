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


class TestGetCurrentEventForSeries:
    """Test get_current_event_for_series() event selection logic."""

    @pytest.fixture
    def client(self) -> GammaClient:
        """Create a GammaClient instance for testing."""
        config = Config()
        return GammaClient(config)

    def _make_event_data(self, event_id: str, title: str, closed: bool = False) -> dict:
        """Create a mock event data dictionary."""
        return {
            "id": event_id,
            "title": title,
            "slug": f"event-{event_id}",
            "description": "Test event",
            "start_date_iso": "2026-01-09T00:00:00Z",
            "end_date_iso": "2026-01-10T00:00:00Z",
            "closed": closed,
            "markets": [
                {
                    "condition_id": f"market-{event_id}",
                    "question": "Test market question?",
                    "active": True,
                    "closed": False,
                    "clobTokenIds": '["token1", "token2"]',
                    "outcomes": '["Yes", "No"]',
                }
            ],
        }

    def test_selects_event_with_nearest_closing_time(self, client: GammaClient):
        """Verify at 7:54PM, selects 8:00PM closing event not 8:30PM."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 7:54PM ET = close to 8:00PM closing
        current_time = datetime(2026, 1, 9, 19, 54, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event 1: 7:45PM-8:00PM - closes in 6 minutes (should be selected)
        event1_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 7:45PM-8:00PM ET",
        )
        # Event 2: 8:15PM-8:30PM - closes in 36 minutes (too far)
        event2_data = self._make_event_data(
            "event2",
            "Bitcoin Up or Down - January 9, 8:15PM-8:30PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event1_data, event2_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                # Need to pass through the datetime constructor for time parsing
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        assert result is not None
        assert result.id == "event1"
        assert "7:45PM-8:00PM" in result.title

    def test_selects_event_closing_soonest_in_future(self, client: GammaClient):
        """Verify selection prefers event closing soonest when multiple are valid."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 8:05PM ET
        current_time = datetime(2026, 1, 9, 20, 5, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event 1: 8:00PM-8:15PM - closes in 10 minutes (should be selected)
        event1_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 8:00PM-8:15PM ET",
        )
        # Event 2: 8:15PM-8:30PM - closes in 25 minutes (outside 15 min window)
        event2_data = self._make_event_data(
            "event2",
            "Bitcoin Up or Down - January 9, 8:15PM-8:30PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event1_data, event2_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        assert result is not None
        assert result.id == "event1"
        assert "8:00PM-8:15PM" in result.title

    def test_skips_events_beyond_15_minute_window(self, client: GammaClient):
        """Verify events closing more than 15 minutes away are not selected."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 7:30PM ET
        current_time = datetime(2026, 1, 9, 19, 30, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event 1: 8:00PM-8:15PM - closes in 45 minutes (too far)
        event1_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 8:00PM-8:15PM ET",
        )
        # Event 2: 8:30PM-8:45PM - closes in 75 minutes (too far)
        event2_data = self._make_event_data(
            "event2",
            "Bitcoin Up or Down - January 9, 8:30PM-8:45PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event1_data, event2_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        assert result is None

    def test_returns_none_when_no_events_in_window(self, client: GammaClient):
        """Verify returns None when no events have closing time within 15 minutes."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 7:00PM ET - no events close within 15 minutes
        current_time = datetime(2026, 1, 9, 19, 0, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Only event closes at 8:00PM (60 min away)
        event_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 7:45PM-8:00PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        assert result is None

    def test_handles_events_with_unparseable_titles(self, client: GammaClient):
        """Verify events with invalid titles are skipped gracefully."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 7:54PM ET
        current_time = datetime(2026, 1, 9, 19, 54, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event 1: Invalid title (no time range)
        event1_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9",  # Missing time range
        )
        # Event 2: Valid title, closes in 6 minutes (should be selected)
        event2_data = self._make_event_data(
            "event2",
            "Bitcoin Up or Down - January 9, 7:45PM-8:00PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event1_data, event2_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        # Should select the valid event, skipping the invalid one
        assert result is not None
        assert result.id == "event2"

    def test_returns_none_when_no_events_found(self, client: GammaClient):
        """Verify returns None when series has no events."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            result = client.get_current_event_for_series("empty-series")

        assert result is None

    def test_accepts_recently_closed_events(self, client: GammaClient):
        """Verify events that just closed (within 2 min grace period) are accepted."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 8:01PM ET - 1 minute after 8:00PM close
        current_time = datetime(2026, 1, 9, 20, 1, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event closed 1 minute ago (within 2-min grace period)
        event_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 7:45PM-8:00PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        assert result is not None
        assert result.id == "event1"

    def test_rejects_events_closed_beyond_grace_period(self, client: GammaClient):
        """Verify events closed more than 2 minutes ago are rejected."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 8:05PM ET - 5 minutes after 8:00PM close
        current_time = datetime(2026, 1, 9, 20, 5, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event closed 5 minutes ago (outside 2-min grace period)
        event_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 7:45PM-8:00PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        assert result is None

    def test_prefers_future_closing_over_past(self, client: GammaClient):
        """Verify events closing in future are preferred over recently closed."""
        et_tz = ZoneInfo("America/New_York")
        # Current time: 8:14PM ET
        current_time = datetime(2026, 1, 9, 20, 14, 0, tzinfo=et_tz).astimezone(timezone.utc)

        # Event 1: Closed 14 minutes ago (within 2-min limit? no, -14 min < -2 min limit)
        # Actually -14 min is beyond grace period, let's adjust
        # Let's use 8:14PM and have one event close at 8:13PM (1 min ago) and another at 8:15PM (1 min future)
        # Event 1: 7:58PM-8:13PM - closed 1 minute ago (within grace)
        event1_data = self._make_event_data(
            "event1",
            "Bitcoin Up or Down - January 9, 7:58PM-8:13PM ET",
        )
        # Event 2: 8:00PM-8:15PM - closes in 1 minute (should be preferred)
        event2_data = self._make_event_data(
            "event2",
            "Bitcoin Up or Down - January 9, 8:00PM-8:15PM ET",
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [event1_data, event2_data]
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._client, "get", return_value=mock_response):
            with patch("src.api.gamma_client.datetime") as mock_datetime:
                mock_datetime.now.return_value = current_time
                mock_datetime.fromisoformat = datetime.fromisoformat
                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                result = client.get_current_event_for_series("test-series")

        # Should prefer the one closing in the future
        assert result is not None
        assert result.id == "event2"
