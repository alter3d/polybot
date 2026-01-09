"""Price threshold detection for trading opportunities.

This module provides opportunity detection logic based on price thresholds.
When bid prices or last trade prices exceed the configured threshold,
opportunities are flagged for potential trading action.
"""

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Opportunity:
    """Represents a detected trading opportunity.

    An opportunity is created when price data exceeds the configured
    threshold, indicating potential trading action.

    Attributes:
        market_id: Unique identifier for the market.
        side: Trading side, either "YES" or "NO".
        price: The price that triggered the opportunity.
        detected_at: Timestamp when the opportunity was detected.
        source: Source of the price data, either "bid" or "last_trade".
    """

    market_id: str
    side: str  # "YES" or "NO"
    price: float
    detected_at: datetime
    source: str  # "bid" or "last_trade"

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"Opportunity({self.side} @ ${self.price:.2f} "
            f"from {self.source} on {self.market_id})"
        )


def _is_valid_price(price: Optional[float]) -> bool:
    """Check if a price value is valid for comparison.

    A valid price is a non-None, finite number that is non-negative.

    Args:
        price: The price value to validate.

    Returns:
        True if the price is valid, False otherwise.
    """
    if price is None:
        return False
    if isinstance(price, float) and (math.isnan(price) or math.isinf(price)):
        return False
    if price < 0:
        return False
    return True


def detect_opportunity(
    bid_price: Optional[float],
    last_trade_price: Optional[float],
    threshold: float,
    market_id: str,
) -> list[Opportunity]:
    """Detect trading opportunities based on price thresholds.

    Examines both bid price and last trade price to identify when either
    exceeds the configured threshold. Each qualifying price creates a
    separate opportunity record.

    Args:
        bid_price: Current best bid price (can be None if unavailable).
        last_trade_price: Most recent trade price (can be None if unavailable).
        threshold: Price threshold for opportunity detection (e.g., 0.70).
        market_id: Unique identifier for the market being monitored.

    Returns:
        List of Opportunity objects for each price exceeding threshold.
        Empty list if no opportunities detected.

    Example:
        >>> opps = detect_opportunity(0.75, 0.65, 0.70, 'btc-15min-market')
        >>> print(f"Detected {len(opps)} opportunities")
        Detected 1 opportunities
        >>> print(opps[0].source)
        bid

    Notes:
        - Invalid prices (None, NaN, negative) are safely skipped
        - Both prices exceeding threshold creates two separate opportunities
        - Side is set to "YES" when prices are above threshold
    """
    opportunities: list[Opportunity] = []
    now = datetime.now()

    # Check bid price against threshold
    if _is_valid_price(bid_price) and bid_price >= threshold:
        opp = Opportunity(
            market_id=market_id,
            side="YES",  # Bid above threshold suggests YES confidence
            price=bid_price,
            detected_at=now,
            source="bid",
        )
        opportunities.append(opp)
        logger.info(
            "Opportunity detected: bid price $%.2f >= threshold $%.2f for %s",
            bid_price,
            threshold,
            market_id,
        )

    # Check last trade price against threshold
    if _is_valid_price(last_trade_price) and last_trade_price >= threshold:
        opp = Opportunity(
            market_id=market_id,
            side="YES",  # Last trade above threshold suggests YES confidence
            price=last_trade_price,
            detected_at=now,
            source="last_trade",
        )
        opportunities.append(opp)
        logger.info(
            "Opportunity detected: last trade price $%.2f >= threshold $%.2f for %s",
            last_trade_price,
            threshold,
            market_id,
        )

    if not opportunities:
        logger.debug(
            "No opportunities: bid=$%s, last_trade=$%s, threshold=$%.2f for %s",
            bid_price if _is_valid_price(bid_price) else "N/A",
            last_trade_price if _is_valid_price(last_trade_price) else "N/A",
            threshold,
            market_id,
        )

    return opportunities


def detect_opportunities_batch(
    price_data: list[dict],
    threshold: float,
) -> list[Opportunity]:
    """Detect opportunities across multiple markets.

    Convenience function for processing multiple markets at once.

    Args:
        price_data: List of dicts with keys 'market_id', 'bid_price', 'last_trade_price'.
        threshold: Price threshold for opportunity detection.

    Returns:
        Combined list of all opportunities detected across markets.

    Example:
        >>> data = [
        ...     {'market_id': 'btc-15min', 'bid_price': 0.80, 'last_trade_price': 0.75},
        ...     {'market_id': 'eth-15min', 'bid_price': 0.60, 'last_trade_price': 0.55},
        ... ]
        >>> opps = detect_opportunities_batch(data, 0.70)
        >>> print(f"Found {len(opps)} opportunities")
        Found 2 opportunities
    """
    all_opportunities: list[Opportunity] = []

    for data in price_data:
        market_id = data.get("market_id", "unknown")
        bid_price = data.get("bid_price")
        last_trade_price = data.get("last_trade_price")

        opportunities = detect_opportunity(
            bid_price=bid_price,
            last_trade_price=last_trade_price,
            threshold=threshold,
            market_id=market_id,
        )
        all_opportunities.extend(opportunities)

    logger.info(
        "Batch detection complete: %d opportunities across %d markets",
        len(all_opportunities),
        len(price_data),
    )

    return all_opportunities
