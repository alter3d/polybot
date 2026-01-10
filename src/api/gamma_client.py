"""Gamma API client for Polymarket market discovery.

This module provides a client for interacting with the Polymarket Gamma API
to discover and filter markets and events. The Gamma API is a read-only REST API that
provides market metadata, categorization, and volume metrics.

The client supports series-based event discovery, where a "series" is a higher-level
abstraction that encapsulates all recurring instances of an event/market that are
identical other than the time period they cover.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from src.config import Config

logger = logging.getLogger(__name__)

# Default timeout for HTTP requests (seconds)
DEFAULT_TIMEOUT = 30.0

# Default pagination settings
DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 500


@dataclass
class MarketToken:
    """Represents a token within a market.

    Attributes:
        token_id: Unique identifier for the token (used for CLOB/WebSocket).
        outcome: The outcome this token represents (e.g., "Yes", "No").
        winner: Whether this token won (True/False/None for unresolved).
    """

    token_id: str
    outcome: str
    winner: bool | None = None


@dataclass
class Market:
    """Structured representation of a Polymarket market.

    Attributes:
        id: Unique market identifier (condition_id).
        question: The market question/title.
        description: Detailed market description.
        active: Whether the market is currently active for trading.
        closed: Whether the market is closed.
        archived: Whether the market is archived.
        tokens: List of tokens (outcomes) for this market.
        slug: URL-friendly market identifier.
        start_date_iso: Market start date in ISO format.
        end_date_iso: Market end date in ISO format.
        volume: Total trading volume.
        liquidity: Current liquidity in the market.
        tags: List of category tags associated with the market.
        enable_order_book: Whether order book trading is enabled.
    """

    id: str
    question: str
    description: str = ""
    active: bool = False
    closed: bool = False
    archived: bool = False
    tokens: list[MarketToken] = field(default_factory=list)
    slug: str = ""
    start_date_iso: str | None = None
    end_date_iso: str | None = None
    volume: float = 0.0
    liquidity: float = 0.0
    tags: list[str] = field(default_factory=list)
    enable_order_book: bool = False


@dataclass
class Event:
    """Structured representation of a Polymarket event.

    An event groups related markets together. Events are returned from the
    series API and contain the markets for a specific time period.

    Attributes:
        id: Unique event identifier.
        title: Event title/question.
        slug: URL-friendly event identifier.
        description: Detailed event description.
        start_date_iso: Event start date in ISO format.
        end_date_iso: Event end date in ISO format.
        closed: Whether the event is closed.
        markets: List of markets associated with this event.
        series_id: The series this event belongs to.
    """

    id: str
    title: str
    slug: str = ""
    description: str = ""
    start_date_iso: str | None = None
    end_date_iso: str | None = None
    closed: bool = False
    markets: list[Market] = field(default_factory=list)
    series_id: str = ""


class GammaClient:
    """Client for Polymarket Gamma API.

    The Gamma API provides market discovery and metadata. This client
    supports filtering markets by various criteria including activity status,
    tags, and order book availability.

    Example:
        >>> config = Config.from_env()
        >>> client = GammaClient(config)
        >>> markets = client.get_active_markets(limit=10)
        >>> for market in markets:
        ...     print(f"{market.question}: {len(market.tokens)} tokens")
    """

    def __init__(self, config: Config) -> None:
        """Initialize the Gamma API client.

        Args:
            config: Application configuration containing the Gamma API host.
        """
        self._config = config
        self._base_url = config.gamma_host
        self._client = httpx.Client(timeout=DEFAULT_TIMEOUT)
        logger.info("Initialized Gamma client for %s", self._base_url)

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        self._client.close()
        logger.debug("Gamma client closed")

    def __enter__(self) -> "GammaClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def get_markets(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        enable_order_book: bool | None = None,
        tag_id: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> list[Market]:
        """Fetch markets from the Gamma API with optional filters.

        Args:
            active: Filter for active markets (True/False/None for no filter).
            closed: Filter for closed markets.
            archived: Filter for archived markets.
            enable_order_book: Filter for markets with order book enabled.
            tag_id: Filter by tag ID.
            limit: Maximum number of results per page.
            offset: Pagination offset.

        Returns:
            List of Market objects matching the filters.
        """
        params: dict[str, Any] = {
            "limit": min(limit, MAX_PAGE_LIMIT),
            "offset": offset,
        }

        # Add optional filters (only include if explicitly set)
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        if archived is not None:
            params["archived"] = str(archived).lower()
        if enable_order_book is not None:
            params["enableOrderBook"] = str(enable_order_book).lower()
        if tag_id is not None:
            params["tag_id"] = tag_id

        try:
            response = self._client.get(
                f"{self._base_url}/markets",
                params=params,
            )
            response.raise_for_status()
            raw_markets = response.json()

            markets = [self._parse_market(m) for m in raw_markets]
            logger.debug(
                "Fetched %d markets (offset=%d, limit=%d)",
                len(markets),
                offset,
                limit,
            )
            return markets

        except httpx.HTTPStatusError as e:
            logger.error(
                "Gamma API HTTP error %d: %s",
                e.response.status_code,
                e.response.text[:200],
            )
            return []
        except httpx.RequestError as e:
            logger.error("Gamma API request failed: %s", e)
            return []
        except Exception as e:
            logger.error("Failed to fetch markets: %s", e)
            return []

    def get_active_markets(
        self,
        *,
        enable_order_book: bool = True,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> list[Market]:
        """Fetch active markets with order book enabled.

        This is a convenience method for fetching tradeable markets.

        Args:
            enable_order_book: Whether to filter for order book enabled (default: True).
            limit: Maximum number of results per page.
            offset: Pagination offset.

        Returns:
            List of active Market objects.
        """
        return self.get_markets(
            active=True,
            closed=False,
            archived=False,
            enable_order_book=enable_order_book,
            limit=limit,
            offset=offset,
        )

    def get_all_active_markets(
        self,
        *,
        enable_order_book: bool = True,
        max_pages: int = 10,
    ) -> list[Market]:
        """Fetch all active markets with pagination.

        Iterates through pages until all active markets are retrieved
        or max_pages is reached.

        Args:
            enable_order_book: Whether to filter for order book enabled.
            max_pages: Maximum number of pages to fetch (safety limit).

        Returns:
            List of all active Market objects.
        """
        all_markets: list[Market] = []
        offset = 0

        for page in range(max_pages):
            markets = self.get_active_markets(
                enable_order_book=enable_order_book,
                limit=MAX_PAGE_LIMIT,
                offset=offset,
            )

            if not markets:
                logger.debug("No more markets found at page %d", page)
                break

            all_markets.extend(markets)
            logger.debug(
                "Fetched page %d: %d markets (total: %d)",
                page,
                len(markets),
                len(all_markets),
            )

            # If we got fewer than the limit, we've reached the end
            if len(markets) < MAX_PAGE_LIMIT:
                break

            offset += len(markets)

        logger.info("Retrieved %d total active markets", len(all_markets))
        return all_markets

    def get_market_by_id(self, market_id: str) -> Market | None:
        """Fetch a specific market by its ID.

        Args:
            market_id: The market condition ID.

        Returns:
            Market object if found, None otherwise.
        """
        try:
            response = self._client.get(
                f"{self._base_url}/markets/{market_id}",
            )
            response.raise_for_status()
            raw_market = response.json()
            market = self._parse_market(raw_market)
            logger.debug("Fetched market: %s", market.question[:50])
            return market

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning("Market not found: %s", market_id)
            else:
                logger.error(
                    "Gamma API HTTP error %d fetching market %s",
                    e.response.status_code,
                    market_id,
                )
            return None
        except Exception as e:
            logger.error("Failed to fetch market %s: %s", market_id, e)
            return None

    def get_markets_by_tag(self, tag: str, *, active_only: bool = True) -> list[Market]:
        """Fetch markets filtered by a specific tag.

        Args:
            tag: Tag name to filter by (e.g., "crypto", "politics").
            active_only: Whether to only return active markets.

        Returns:
            List of Market objects with the specified tag.
        """
        return self.get_markets(
            active=active_only if active_only else None,
            tag_id=tag,
            enable_order_book=True,
        )

    def search_markets(self, query: str, *, active_only: bool = True) -> list[Market]:
        """Search markets by question text.

        Note: This performs client-side filtering as the Gamma API
        doesn't provide a search endpoint.

        Args:
            query: Search string to match against market questions.
            active_only: Whether to only search active markets.

        Returns:
            List of Market objects matching the search query.
        """
        query_lower = query.lower()

        if active_only:
            markets = self.get_all_active_markets()
        else:
            markets = self.get_markets(limit=MAX_PAGE_LIMIT)

        matching = [m for m in markets if query_lower in m.question.lower()]
        logger.debug(
            "Search for '%s' found %d matches out of %d markets",
            query,
            len(matching),
            len(markets),
        )
        return matching

    def get_crypto_markets(self, *, active_only: bool = True) -> list[Market]:
        """Fetch markets related to cryptocurrency.

        This method filters for markets tagged with crypto-related tags
        or containing crypto keywords in the question.

        Args:
            active_only: Whether to only return active markets.

        Returns:
            List of crypto-related Market objects.
        """
        # Crypto-related keywords to search for
        crypto_keywords = [
            "bitcoin", "btc", "ethereum", "eth", "crypto",
            "solana", "sol", "cardano", "ada", "xrp",
            "dogecoin", "doge", "bnb", "polygon", "matic",
        ]

        if active_only:
            markets = self.get_all_active_markets()
        else:
            markets = self.get_markets(limit=MAX_PAGE_LIMIT)

        crypto_markets = []
        for market in markets:
            question_lower = market.question.lower()
            tags_lower = [t.lower() for t in market.tags]

            # Check if any crypto keyword is in the question or tags
            is_crypto = any(
                kw in question_lower or kw in tags_lower
                for kw in crypto_keywords
            )

            if is_crypto:
                crypto_markets.append(market)

        logger.info(
            "Found %d crypto markets out of %d total",
            len(crypto_markets),
            len(markets),
        )
        return crypto_markets

    def get_15m_crypto_markets(self, *, active_only: bool = True) -> list[Market]:
        """Fetch 15-minute interval crypto markets.

        This method filters for crypto markets that operate on 15-minute
        intervals by looking for the "15m" pattern in the market slug.
        These are the quick-resolution crypto markets (e.g., btc-updown-15m-*).

        Note: This method uses default patterns. For configurable filtering,
        use get_filtered_crypto_markets() instead.

        Args:
            active_only: Whether to only return active markets.

        Returns:
            List of 15-minute crypto Market objects.
        """
        return self.get_filtered_crypto_markets(
            slug_patterns=["-14m-", "-15m-"],
            active_only=active_only,
        )

    def get_filtered_crypto_markets(
        self,
        slug_patterns: list[str] | None = None,
        allowed_assets: list[str] | None = None,
        excluded_assets: list[str] | None = None,
        *,
        active_only: bool = True,
    ) -> list[Market]:
        """Fetch crypto markets with configurable filtering.

        This method provides flexible filtering for crypto markets based on:
        - Slug patterns (e.g., "-14m-", "-15m-" for quick-resolution markets)
        - Allowed assets (e.g., ["BTC", "ETH"] to only monitor specific assets)
        - Excluded assets (e.g., ["XRP"] to skip specific assets)

        Args:
            slug_patterns: List of patterns to match in slugs. If None or empty,
                           all crypto markets are included (no slug filtering).
            allowed_assets: List of asset symbols to include. If None or empty,
                           all assets are included.
            excluded_assets: List of asset symbols to exclude.
            active_only: Whether to only return active markets.

        Returns:
            List of filtered crypto Market objects.
        """
        # First get all crypto markets
        crypto_markets = self.get_crypto_markets(active_only=active_only)

        if not crypto_markets:
            logger.warning("No crypto markets found to filter")
            return []

        filtered_markets = crypto_markets

        # Filter by slug patterns (if specified)
        if slug_patterns:
            slug_patterns_lower = [p.lower() for p in slug_patterns]
            filtered_markets = [
                market for market in filtered_markets
                if any(pattern in market.slug.lower() for pattern in slug_patterns_lower)
            ]
            logger.info(
                "Slug pattern filter: %d markets match patterns %s (from %d crypto markets)",
                len(filtered_markets),
                slug_patterns,
                len(crypto_markets),
            )

        # Filter by allowed assets (if specified)
        if allowed_assets:
            allowed_lower = [a.lower() for a in allowed_assets]
            filtered_markets = [
                market for market in filtered_markets
                if any(asset in market.slug.lower() or asset in market.question.lower()
                       for asset in allowed_lower)
            ]
            logger.info(
                "Allowed assets filter: %d markets contain %s",
                len(filtered_markets),
                allowed_assets,
            )

        # Filter out excluded assets (if specified)
        if excluded_assets:
            excluded_lower = [a.lower() for a in excluded_assets]
            pre_filter_count = len(filtered_markets)
            filtered_markets = [
                market for market in filtered_markets
                if not any(asset in market.slug.lower() or asset in market.question.lower()
                           for asset in excluded_lower)
            ]
            logger.info(
                "Excluded assets filter: removed %d markets containing %s",
                pre_filter_count - len(filtered_markets),
                excluded_assets,
            )

        # Log detailed info about found markets for debugging
        if filtered_markets:
            logger.info(
                "Found %d filtered crypto markets",
                len(filtered_markets),
            )
            for market in filtered_markets[:5]:  # Log first 5 for visibility
                logger.debug(
                    "  Market: %s (slug: %s)",
                    market.question[:50],
                    market.slug,
                )
        else:
            logger.warning(
                "No markets found matching filters. "
                "Patterns: %s, Allowed: %s, Excluded: %s",
                slug_patterns or "(none)",
                allowed_assets or "(all)",
                excluded_assets or "(none)",
            )

        return filtered_markets

    def get_events_by_series(self, series_id: str) -> list[Event]:
        """Fetch events for a specific series ID.

        A series is a higher-level abstraction that encapsulates all recurring
        instances of an event/market that are identical other than the time
        period they cover.

        Args:
            series_id: The series identifier to query.

        Returns:
            List of Event objects for the series (excludes closed events).
        """
        try:
            response = self._client.get(
                f"{self._base_url}/events",
                params={
                    "series_id": series_id,
                    "closed": "false",
                },
            )
            response.raise_for_status()
            raw_events = response.json()

            events = [self._parse_event(e, series_id) for e in raw_events]
            logger.info(
                "Fetched %d open events for series %s",
                len(events),
                series_id,
            )
            return events

        except httpx.HTTPStatusError as e:
            logger.error(
                "Gamma API HTTP error %d fetching series %s: %s",
                e.response.status_code,
                series_id,
                e.response.text[:200],
            )
            return []
        except httpx.RequestError as e:
            logger.error("Gamma API request failed for series %s: %s", series_id, e)
            return []
        except Exception as e:
            logger.error("Failed to fetch events for series %s: %s", series_id, e)
            return []

    def get_current_event_for_series(self, series_id: str) -> Event | None:
        """Find the event whose market closing time is closest to now.

        For a given series, this method finds the event that:
        1. Has a market closing time (parsed from title) within 15 minutes of now
        2. Is not closed
        3. Has the closest closing time to current time

        This ensures we select the market that is about to close soon rather than
        a future market whose event validity window happens to overlap with now.

        Args:
            series_id: The series identifier to query.

        Returns:
            The Event with closing time closest to now (within 15 min), or None if not found.
        """
        events = self.get_events_by_series(series_id)
        if not events:
            logger.warning("No events found for series %s", series_id)
            return None

        now = datetime.now(timezone.utc)
        max_closing_window = timedelta(minutes=15)

        # Collect events with valid closing times within the 15-minute window
        candidates: list[tuple[Event, datetime, timedelta]] = []

        for event in events:
            # Parse closing time from the event title
            closing_time = self._parse_market_closing_time(event.title, now)

            if closing_time is None:
                logger.debug(
                    "Event %s: could not parse closing time from title '%s'",
                    event.id,
                    event.title[:50],
                )
                continue

            # Calculate time until closing (can be negative if just closed)
            time_to_close = closing_time - now

            # Check if closing time is within 15 minutes of now
            # Accept events closing soon (positive) or just closed (small negative)
            if timedelta(minutes=-2) <= time_to_close <= max_closing_window:
                candidates.append((event, closing_time, time_to_close))
                logger.debug(
                    "Event %s is a candidate: closes at %s (in %s)",
                    event.id,
                    closing_time.strftime("%H:%M:%S UTC"),
                    time_to_close,
                )
            else:
                logger.debug(
                    "Event %s skipped: closes at %s (delta %s outside window)",
                    event.id,
                    closing_time.strftime("%H:%M:%S UTC"),
                    time_to_close,
                )

        if not candidates:
            logger.warning(
                "No event found with closing time within 15 minutes for series %s",
                series_id,
            )
            return None

        # Select the event with closing time closest to (but preferably after) now
        # Sort by time_to_close: prefer positive values (closing soon) over negative (just closed)
        # Then by absolute distance to now
        def sort_key(item: tuple[Event, datetime, timedelta]) -> tuple[int, float]:
            _, _, delta = item
            # Prefer events closing in the future (priority 0) over past (priority 1)
            priority = 0 if delta >= timedelta(0) else 1
            # Then sort by absolute distance to now
            return (priority, abs(delta.total_seconds()))

        candidates.sort(key=sort_key)
        selected_event, closing_time, time_to_close = candidates[0]

        logger.info(
            "Selected event for series %s: %s (closes at %s, in %s)",
            series_id,
            selected_event.title[:50],
            closing_time.strftime("%H:%M:%S UTC"),
            time_to_close,
        )
        return selected_event

    def get_current_markets_for_series(self, series_ids: list[str]) -> list[Market]:
        """Find all markets from events that cover the current time.

        For each series ID, find the current event and extract its markets.

        Args:
            series_ids: List of series identifiers to query.

        Returns:
            List of Market objects from current events across all series.
        """
        all_markets: list[Market] = []

        for series_id in series_ids:
            event = self.get_current_event_for_series(series_id)
            if event and event.markets:
                logger.info(
                    "Adding %d markets from series %s event: %s",
                    len(event.markets),
                    series_id,
                    event.title[:50],
                )
                all_markets.extend(event.markets)

        logger.info(
            "Found %d total markets from %d series",
            len(all_markets),
            len(series_ids),
        )
        return all_markets

    def _parse_iso_datetime(self, iso_str: str | None) -> datetime | None:
        """Parse an ISO datetime string to a datetime object.

        Args:
            iso_str: ISO format datetime string (may include timezone).

        Returns:
            Parsed datetime in UTC, or None if parsing fails.
        """
        if not iso_str:
            return None

        try:
            # Handle various ISO formats
            # First try with timezone info
            if iso_str.endswith("Z"):
                iso_str = iso_str[:-1] + "+00:00"

            # Try parsing with fromisoformat
            dt = datetime.fromisoformat(iso_str)

            # Ensure it's in UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

            return dt

        except (ValueError, TypeError) as e:
            logger.debug("Failed to parse datetime '%s': %s", iso_str, e)
            return None

    def _parse_market_closing_time(
        self, event_title: str, reference_date: datetime | None = None
    ) -> datetime | None:
        """Parse the market closing time from an event title.

        Event titles typically follow the format:
        "[Asset] Up or Down - [Month Day], [StartTime]-[EndTime] ET"
        Example: "Bitcoin Up or Down - January 9, 8:15PM-8:30PM ET"

        This method extracts the end time (closing time) from the time range.

        Args:
            event_title: The event title containing the time range.
            reference_date: Optional reference datetime for inferring year.
                           Defaults to current time if not provided.

        Returns:
            Parsed closing time in UTC, or None if parsing fails.
        """
        if not event_title:
            return None

        # Use current time as reference if not provided
        if reference_date is None:
            reference_date = datetime.now(timezone.utc)

        try:
            # Regex to extract: "Month Day, StartTime-EndTime ET"
            # Examples:
            #   "January 9, 8:15PM-8:30PM ET"
            #   "December 31, 11:45PM-12:00AM ET"
            pattern = r"(\w+)\s+(\d{1,2}),\s*(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)\s*ET"
            match = re.search(pattern, event_title, re.IGNORECASE)

            if not match:
                logger.debug(
                    "Could not extract time range from event title: '%s'",
                    event_title[:100],
                )
                return None

            # Extract matched groups
            month_name = match.group(1)
            day = int(match.group(2))
            # Start time groups: 3, 4, 5 (hour, minute, am/pm)
            # End time groups: 6, 7, 8 (hour, minute, am/pm)
            end_hour = int(match.group(6))
            end_minute = int(match.group(7))
            end_ampm = match.group(8).upper()

            # Convert 12-hour to 24-hour format
            if end_ampm == "PM" and end_hour != 12:
                end_hour += 12
            elif end_ampm == "AM" and end_hour == 12:
                end_hour = 0

            # Parse month name to number
            month_map = {
                "january": 1, "february": 2, "march": 3, "april": 4,
                "may": 5, "june": 6, "july": 7, "august": 8,
                "september": 9, "october": 10, "november": 11, "december": 12,
            }
            month = month_map.get(month_name.lower())
            if month is None:
                logger.debug("Invalid month name '%s' in event title", month_name)
                return None

            # Determine year from reference date
            # If the parsed month/day would be more than 6 months in the past,
            # assume it's for the next year
            year = reference_date.year
            et_tz = ZoneInfo("America/New_York")

            # Create datetime in ET timezone
            closing_time_et = datetime(
                year=year,
                month=month,
                day=day,
                hour=end_hour,
                minute=end_minute,
                second=0,
                tzinfo=et_tz,
            )

            # Handle midnight boundary: if end time is 12:00AM, it's the next day
            # Check if start time > end time (e.g., 11:45PM-12:00AM)
            start_hour = int(match.group(3))
            start_ampm = match.group(5).upper()
            if start_ampm == "PM" and start_hour != 12:
                start_hour += 12
            elif start_ampm == "AM" and start_hour == 12:
                start_hour = 0

            if start_hour > end_hour or (start_hour == 23 and end_hour == 0):
                # End time is on the next day
                closing_time_et = closing_time_et + timedelta(days=1)

            # Convert to UTC
            closing_time_utc = closing_time_et.astimezone(timezone.utc)

            logger.debug(
                "Parsed closing time from '%s': %s ET -> %s UTC",
                event_title[:50],
                closing_time_et.strftime("%Y-%m-%d %H:%M %Z"),
                closing_time_utc.strftime("%Y-%m-%d %H:%M %Z"),
            )

            return closing_time_utc

        except (ValueError, TypeError, AttributeError) as e:
            logger.debug(
                "Failed to parse market closing time from '%s': %s",
                event_title[:100],
                e,
            )
            return None

    def _parse_event(self, raw: dict[str, Any], series_id: str = "") -> Event:
        """Parse raw API response into an Event object.

        Args:
            raw: Raw event data from the API.
            series_id: The series ID this event belongs to.

        Returns:
            Parsed Event object.
        """
        # Parse markets within the event
        markets = []
        raw_markets = raw.get("markets", []) or []
        for raw_market in raw_markets:
            if isinstance(raw_market, dict):
                markets.append(self._parse_market(raw_market))

        return Event(
            id=str(raw.get("id", "")),
            title=str(raw.get("title", "")),
            slug=str(raw.get("slug", "") or ""),
            description=str(raw.get("description", "") or ""),
            start_date_iso=raw.get("start_date_iso") or raw.get("startDate"),
            end_date_iso=raw.get("end_date_iso") or raw.get("endDate"),
            closed=bool(raw.get("closed", False)),
            markets=markets,
            series_id=series_id,
        )

    def _parse_market(self, raw: dict[str, Any]) -> Market:
        """Parse raw API response into a Market object.

        Args:
            raw: Raw market data from the API.

        Returns:
            Parsed Market object.
        """
        # Parse tokens - handle two different API formats:
        # 1. Markets endpoint: "tokens" array with {token_id, outcome, winner} objects
        # 2. Events endpoint: "clobTokenIds" JSON string + "outcomes" JSON string
        tokens = []
        raw_tokens = raw.get("tokens", []) or []

        if raw_tokens:
            # Format 1: tokens array (from /markets endpoint)
            for token in raw_tokens:
                if isinstance(token, dict):
                    tokens.append(MarketToken(
                        token_id=str(token.get("token_id", "")),
                        outcome=str(token.get("outcome", "")),
                        winner=token.get("winner"),
                    ))
        else:
            # Format 2: clobTokenIds + outcomes (from /events endpoint)
            # These fields are JSON strings that need to be parsed
            clob_token_ids = raw.get("clobTokenIds")
            outcomes = raw.get("outcomes")

            if clob_token_ids and outcomes:
                try:
                    # Parse JSON strings
                    token_ids = json.loads(clob_token_ids) if isinstance(clob_token_ids, str) else clob_token_ids
                    outcome_list = json.loads(outcomes) if isinstance(outcomes, str) else outcomes

                    # Create MarketToken objects pairing token IDs with outcomes
                    for i, token_id in enumerate(token_ids):
                        outcome = outcome_list[i] if i < len(outcome_list) else f"Outcome {i}"
                        tokens.append(MarketToken(
                            token_id=str(token_id),
                            outcome=str(outcome),
                            winner=None,  # Not available in this format
                        ))

                    logger.debug(
                        "Parsed %d tokens from clobTokenIds for market %s",
                        len(tokens),
                        raw.get("slug", raw.get("id", "unknown")),
                    )

                except (json.JSONDecodeError, TypeError, IndexError) as e:
                    logger.warning(
                        "Failed to parse clobTokenIds/outcomes for market %s: %s",
                        raw.get("slug", raw.get("id", "unknown")),
                        e,
                    )

        # Parse tags
        tags = []
        raw_tags = raw.get("tags", []) or []
        for tag in raw_tags:
            if isinstance(tag, dict):
                tag_label = tag.get("label") or tag.get("slug") or ""
                if tag_label:
                    tags.append(str(tag_label))
            elif isinstance(tag, str):
                tags.append(tag)

        # Parse volume and liquidity safely
        volume = 0.0
        try:
            volume = float(raw.get("volume", 0) or 0)
        except (ValueError, TypeError):
            pass

        liquidity = 0.0
        try:
            liquidity = float(raw.get("liquidity", 0) or 0)
        except (ValueError, TypeError):
            pass

        return Market(
            id=str(raw.get("condition_id", "") or raw.get("id", "")),
            question=str(raw.get("question", "")),
            description=str(raw.get("description", "") or ""),
            active=bool(raw.get("active", False)),
            closed=bool(raw.get("closed", False)),
            archived=bool(raw.get("archived", False)),
            tokens=tokens,
            slug=str(raw.get("slug", "") or ""),
            start_date_iso=raw.get("start_date_iso") or raw.get("startDate"),
            end_date_iso=raw.get("end_date_iso") or raw.get("endDate"),
            volume=volume,
            liquidity=liquidity,
            tags=tags,
            enable_order_book=bool(raw.get("enable_order_book", False)),
        )
