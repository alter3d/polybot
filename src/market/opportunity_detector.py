"""Price threshold detection for trading opportunities.

This module provides opportunity detection logic based on price thresholds.
When last trade prices exceed the configured threshold, opportunities are
flagged for potential trading action.
"""

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Track (market_id, source) pairs that have already triggered an opportunity alert.
# First detection logs at INFO level, subsequent detections log at DEBUG to reduce spam.
_alerted_opportunities: set[tuple[str, str]] = set()


@dataclass
class Opportunity:
    """Represents a detected trading opportunity.

    An opportunity is created when price data exceeds the configured
    threshold, indicating potential trading action.

    Attributes:
        market_id: Unique identifier for the market (condition ID).
        side: Trading side, either "YES" or "NO".
        price: The price that triggered the opportunity.
        detected_at: Timestamp when the opportunity was detected.
        source: Source of the price data ("last_trade").
        token_id: CLOB token ID for trading (long string required by API).
        neg_risk: Whether this is a negative risk market (requires special order handling).
    """

    market_id: str
    side: str  # "YES" or "NO"
    price: float
    detected_at: datetime
    source: str  # "last_trade"
    token_id: Optional[str] = None  # CLOB token ID for trading
    neg_risk: bool = False  # Whether market is negative risk

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
    last_trade_price: Optional[float],
    threshold: float,
    market_id: str,
    token_id: Optional[str] = None,
    neg_risk: bool = False,
    outcome: str = "YES",
) -> list[Opportunity]:
    """Detect trading opportunities based on price thresholds.

    Examines the last trade price to identify when it exceeds the configured
    threshold. A qualifying price creates an opportunity record.

    Args:
        last_trade_price: Most recent trade price (can be None if unavailable).
        threshold: Price threshold for opportunity detection (e.g., 0.70).
        market_id: Unique identifier for the market being monitored.
        token_id: CLOB token ID for trading (required for order submission).
        neg_risk: Whether this is a negative risk market (affects order creation).
        outcome: Token outcome ("YES" or "NO") to set as the opportunity side.

    Returns:
        List of Opportunity objects for each price exceeding threshold.
        Empty list if no opportunities detected.

    Example:
        >>> opps = detect_opportunity(0.75, 0.70, 'btc-15min-market')
        >>> print(f"Detected {len(opps)} opportunities")
        Detected 1 opportunities
        >>> print(opps[0].source)
        last_trade

    Notes:
        - Invalid prices (None, NaN, negative) are safely skipped
        - At most one opportunity is returned per call
        - Side is set based on the token's outcome (YES or NO)
    """
    opportunities: list[Opportunity] = []
    now = datetime.now()

    # Normalize outcome to uppercase for consistency
    side = outcome.upper() if outcome else "YES"

    # Check last trade price against threshold
    if _is_valid_price(last_trade_price) and last_trade_price >= threshold:
        opp = Opportunity(
            market_id=market_id,
            side=side,  # Use the token's outcome (YES or NO)
            price=last_trade_price,
            detected_at=now,
            source="last_trade",
            token_id=token_id,
            neg_risk=neg_risk,
        )
        opportunities.append(opp)
        alert_key = (market_id, "last_trade")
        if alert_key in _alerted_opportunities:
            logger.debug(
                "Opportunity detected: last trade price $%.2f >= threshold $%.2f for %s",
                last_trade_price,
                threshold,
                market_id,
            )
        else:
            logger.info(
                "Opportunity detected: last trade price $%.2f >= threshold $%.2f for %s",
                last_trade_price,
                threshold,
                market_id,
            )
            _alerted_opportunities.add(alert_key)

    if not opportunities:
        logger.debug(
            "No opportunities: last_trade=$%s, threshold=$%.2f for %s",
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
        price_data: List of dicts with keys 'market_id', 'last_trade_price', 'token_id', 'neg_risk'.
        threshold: Price threshold for opportunity detection.

    Returns:
        Combined list of all opportunities detected across markets.

    Example:
        >>> data = [
        ...     {'market_id': 'btc-15min', 'last_trade_price': 0.75},
        ...     {'market_id': 'eth-15min', 'last_trade_price': 0.55},
        ... ]
        >>> opps = detect_opportunities_batch(data, 0.70)
        >>> print(f"Found {len(opps)} opportunities")
        Found 1 opportunities
    """
    all_opportunities: list[Opportunity] = []

    for data in price_data:
        market_id = data.get("market_id", "unknown")
        last_trade_price = data.get("last_trade_price")
        token_id = data.get("token_id")
        neg_risk = data.get("neg_risk", False)

        opportunities = detect_opportunity(
            last_trade_price=last_trade_price,
            threshold=threshold,
            market_id=market_id,
            token_id=token_id,
            neg_risk=neg_risk,
        )
        all_opportunities.extend(opportunities)

    logger.info(
        "Batch detection complete: %d opportunities across %d markets",
        len(all_opportunities),
        len(price_data),
    )

    return all_opportunities
