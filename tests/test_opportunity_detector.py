"""Tests for opportunity detection module.

Tests the price threshold detection logic including the Opportunity dataclass,
_is_valid_price helper function, detect_opportunity function, and batch processing.
"""

import math
from datetime import datetime
from unittest.mock import patch

import pytest

from src.market.opportunity_detector import (
    Opportunity,
    _is_valid_price,
    detect_opportunity,
    detect_opportunities_batch,
)


class TestOpportunityDataclass:
    """Test Opportunity dataclass."""

    def test_opportunity_creation(self):
        """Verify Opportunity can be created with all required fields."""
        now = datetime(2024, 1, 15, 10, 13, 0)
        opp = Opportunity(
            market_id="btc-15min-market",
            side="YES",
            price=0.75,
            detected_at=now,
            source="bid",
        )
        assert opp.market_id == "btc-15min-market"
        assert opp.side == "YES"
        assert opp.price == 0.75
        assert opp.detected_at == now
        assert opp.source == "bid"

    def test_opportunity_side_yes(self):
        """Verify Opportunity accepts YES side."""
        opp = Opportunity(
            market_id="test",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="bid",
        )
        assert opp.side == "YES"

    def test_opportunity_side_no(self):
        """Verify Opportunity accepts NO side."""
        opp = Opportunity(
            market_id="test",
            side="NO",
            price=0.80,
            detected_at=datetime.now(),
            source="bid",
        )
        assert opp.side == "NO"

    def test_opportunity_source_bid(self):
        """Verify Opportunity accepts bid source."""
        opp = Opportunity(
            market_id="test",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="bid",
        )
        assert opp.source == "bid"

    def test_opportunity_source_last_trade(self):
        """Verify Opportunity accepts last_trade source."""
        opp = Opportunity(
            market_id="test",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
        )
        assert opp.source == "last_trade"

    def test_opportunity_str_representation(self):
        """Verify __str__ produces human-readable output."""
        opp = Opportunity(
            market_id="btc-15min",
            side="YES",
            price=0.7543,
            detected_at=datetime.now(),
            source="bid",
        )
        str_repr = str(opp)
        assert "YES" in str_repr
        assert "0.75" in str_repr  # Formatted to 2 decimal places
        assert "bid" in str_repr
        assert "btc-15min" in str_repr

    def test_opportunity_str_last_trade_source(self):
        """Verify __str__ correctly shows last_trade source."""
        opp = Opportunity(
            market_id="eth-15min",
            side="YES",
            price=0.80,
            detected_at=datetime.now(),
            source="last_trade",
        )
        str_repr = str(opp)
        assert "last_trade" in str_repr

    def test_opportunity_equality(self):
        """Verify two Opportunity instances with same values are equal."""
        now = datetime(2024, 1, 15, 10, 13, 0)
        opp1 = Opportunity("test", "YES", 0.75, now, "bid")
        opp2 = Opportunity("test", "YES", 0.75, now, "bid")
        assert opp1 == opp2

    def test_opportunity_inequality_price(self):
        """Verify Opportunity instances with different prices are not equal."""
        now = datetime(2024, 1, 15, 10, 13, 0)
        opp1 = Opportunity("test", "YES", 0.75, now, "bid")
        opp2 = Opportunity("test", "YES", 0.80, now, "bid")
        assert opp1 != opp2

    def test_opportunity_inequality_source(self):
        """Verify Opportunity instances with different sources are not equal."""
        now = datetime(2024, 1, 15, 10, 13, 0)
        opp1 = Opportunity("test", "YES", 0.75, now, "bid")
        opp2 = Opportunity("test", "YES", 0.75, now, "last_trade")
        assert opp1 != opp2


class TestIsValidPrice:
    """Test _is_valid_price helper function."""

    def test_valid_price_positive(self):
        """Verify positive prices are valid."""
        assert _is_valid_price(0.75)
        assert _is_valid_price(0.50)
        assert _is_valid_price(1.0)
        assert _is_valid_price(0.01)

    def test_valid_price_zero(self):
        """Verify zero price is valid."""
        assert _is_valid_price(0.0)
        assert _is_valid_price(0)

    def test_invalid_price_none(self):
        """Verify None price is invalid."""
        assert not _is_valid_price(None)

    def test_invalid_price_nan(self):
        """Verify NaN price is invalid."""
        assert not _is_valid_price(float("nan"))
        assert not _is_valid_price(math.nan)

    def test_invalid_price_positive_infinity(self):
        """Verify positive infinity is invalid."""
        assert not _is_valid_price(float("inf"))
        assert not _is_valid_price(math.inf)

    def test_invalid_price_negative_infinity(self):
        """Verify negative infinity is invalid."""
        assert not _is_valid_price(float("-inf"))
        assert not _is_valid_price(-math.inf)

    def test_invalid_price_negative(self):
        """Verify negative prices are invalid."""
        assert not _is_valid_price(-0.50)
        assert not _is_valid_price(-1.0)
        assert not _is_valid_price(-0.01)

    def test_valid_price_integer(self):
        """Verify integer prices are valid."""
        assert _is_valid_price(1)
        assert _is_valid_price(0)

    def test_valid_price_small_positive(self):
        """Verify very small positive prices are valid."""
        assert _is_valid_price(0.0001)
        assert _is_valid_price(1e-10)


class TestDetectOpportunity:
    """Test detect_opportunity function."""

    def test_bid_above_threshold(self):
        """Verify opportunity detected when bid exceeds threshold."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].source == "bid"
        assert opportunities[0].price == 0.75
        assert opportunities[0].side == "YES"

    def test_last_trade_above_threshold(self):
        """Verify opportunity detected when last trade exceeds threshold."""
        opportunities = detect_opportunity(
            bid_price=0.65,
            last_trade_price=0.75,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"
        assert opportunities[0].price == 0.75
        assert opportunities[0].side == "YES"

    def test_both_prices_above_threshold(self):
        """Verify two opportunities detected when both prices exceed threshold."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.80,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 2

        sources = {opp.source for opp in opportunities}
        assert sources == {"bid", "last_trade"}

        bid_opp = next(o for o in opportunities if o.source == "bid")
        last_trade_opp = next(o for o in opportunities if o.source == "last_trade")
        assert bid_opp.price == 0.75
        assert last_trade_opp.price == 0.80

    def test_no_opportunity_both_below_threshold(self):
        """Verify no opportunities when both prices below threshold."""
        opportunities = detect_opportunity(
            bid_price=0.65,
            last_trade_price=0.60,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 0

    def test_opportunity_at_exact_threshold(self):
        """Verify opportunity detected at exact threshold (>=)."""
        opportunities = detect_opportunity(
            bid_price=0.70,
            last_trade_price=0.60,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].price == 0.70

    def test_no_opportunity_just_below_threshold(self):
        """Verify no opportunity just below threshold."""
        opportunities = detect_opportunity(
            bid_price=0.699999,
            last_trade_price=0.60,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 0

    def test_opportunity_with_none_bid_price(self):
        """Verify opportunity detection handles None bid price."""
        opportunities = detect_opportunity(
            bid_price=None,
            last_trade_price=0.75,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"

    def test_opportunity_with_none_last_trade_price(self):
        """Verify opportunity detection handles None last trade price."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=None,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].source == "bid"

    def test_no_opportunity_both_prices_none(self):
        """Verify no opportunities when both prices are None."""
        opportunities = detect_opportunity(
            bid_price=None,
            last_trade_price=None,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 0

    def test_opportunity_with_nan_bid_price(self):
        """Verify opportunity detection handles NaN bid price."""
        opportunities = detect_opportunity(
            bid_price=float("nan"),
            last_trade_price=0.75,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"

    def test_opportunity_with_negative_bid_price(self):
        """Verify opportunity detection handles negative bid price."""
        opportunities = detect_opportunity(
            bid_price=-0.50,
            last_trade_price=0.75,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"

    def test_opportunity_market_id_preserved(self):
        """Verify market ID is correctly assigned to opportunity."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="custom-market-123",
        )
        assert opportunities[0].market_id == "custom-market-123"

    def test_opportunity_side_is_yes(self):
        """Verify all opportunities have side set to YES."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.80,
            threshold=0.70,
            market_id="btc-15min",
        )
        for opp in opportunities:
            assert opp.side == "YES"

    def test_opportunity_detected_at_is_datetime(self):
        """Verify detected_at is a datetime instance."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="btc-15min",
        )
        assert isinstance(opportunities[0].detected_at, datetime)

    def test_opportunity_with_zero_threshold(self):
        """Verify opportunity detection with zero threshold."""
        opportunities = detect_opportunity(
            bid_price=0.01,
            last_trade_price=0.02,
            threshold=0.0,
            market_id="btc-15min",
        )
        # Both prices should be >= 0, so both detected
        assert len(opportunities) == 2

    def test_opportunity_with_high_threshold(self):
        """Verify opportunity detection with threshold of 1.0."""
        opportunities = detect_opportunity(
            bid_price=0.99,
            last_trade_price=0.95,
            threshold=1.0,
            market_id="btc-15min",
        )
        assert len(opportunities) == 0

    def test_opportunity_price_at_one(self):
        """Verify opportunity detection when price equals 1.0."""
        opportunities = detect_opportunity(
            bid_price=1.0,
            last_trade_price=0.95,
            threshold=1.0,
            market_id="btc-15min",
        )
        assert len(opportunities) == 1
        assert opportunities[0].price == 1.0


class TestDetectOpportunityThresholdVariations:
    """Test detect_opportunity with various threshold values."""

    @pytest.mark.parametrize(
        "threshold,bid_price,expected_detected",
        [
            (0.70, 0.75, True),
            (0.70, 0.70, True),
            (0.70, 0.69, False),
            (0.50, 0.55, True),
            (0.50, 0.50, True),
            (0.50, 0.49, False),
            (0.80, 0.85, True),
            (0.80, 0.80, True),
            (0.80, 0.79, False),
        ],
    )
    def test_threshold_comparison_bid(self, threshold, bid_price, expected_detected):
        """Verify threshold comparison works correctly for bid prices."""
        opportunities = detect_opportunity(
            bid_price=bid_price,
            last_trade_price=0.0,  # Below any reasonable threshold
            threshold=threshold,
            market_id="test",
        )
        if expected_detected:
            assert len(opportunities) == 1
            assert opportunities[0].source == "bid"
        else:
            assert len(opportunities) == 0

    @pytest.mark.parametrize(
        "threshold,last_trade_price,expected_detected",
        [
            (0.70, 0.75, True),
            (0.70, 0.70, True),
            (0.70, 0.69, False),
            (0.50, 0.55, True),
            (0.50, 0.50, True),
            (0.50, 0.49, False),
            (0.80, 0.85, True),
            (0.80, 0.80, True),
            (0.80, 0.79, False),
        ],
    )
    def test_threshold_comparison_last_trade(
        self, threshold, last_trade_price, expected_detected
    ):
        """Verify threshold comparison works correctly for last trade prices."""
        opportunities = detect_opportunity(
            bid_price=0.0,  # Below any reasonable threshold
            last_trade_price=last_trade_price,
            threshold=threshold,
            market_id="test",
        )
        if expected_detected:
            assert len(opportunities) == 1
            assert opportunities[0].source == "last_trade"
        else:
            assert len(opportunities) == 0


class TestDetectOpportunitiesBatch:
    """Test detect_opportunities_batch function."""

    def test_batch_single_market_with_opportunity(self):
        """Verify batch detection with single market having opportunity."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": 0.75, "last_trade_price": 0.65},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 1
        assert opportunities[0].market_id == "btc-15min"

    def test_batch_single_market_no_opportunity(self):
        """Verify batch detection with single market without opportunity."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": 0.65, "last_trade_price": 0.60},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 0

    def test_batch_multiple_markets_mixed(self):
        """Verify batch detection with multiple markets having mixed results."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": 0.80, "last_trade_price": 0.75},
            {"market_id": "eth-15min", "bid_price": 0.60, "last_trade_price": 0.55},
            {"market_id": "sol-15min", "bid_price": 0.72, "last_trade_price": 0.68},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)

        # btc: 2 opportunities (both above), eth: 0, sol: 1 (bid only)
        assert len(opportunities) == 3

        market_ids = {opp.market_id for opp in opportunities}
        assert "btc-15min" in market_ids
        assert "sol-15min" in market_ids
        assert "eth-15min" not in market_ids

    def test_batch_empty_list(self):
        """Verify batch detection with empty price data list."""
        opportunities = detect_opportunities_batch([], threshold=0.70)
        assert len(opportunities) == 0

    def test_batch_all_markets_with_opportunities(self):
        """Verify batch detection when all markets have opportunities."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": 0.75, "last_trade_price": 0.65},
            {"market_id": "eth-15min", "bid_price": 0.80, "last_trade_price": 0.82},
            {"market_id": "sol-15min", "bid_price": 0.90, "last_trade_price": 0.88},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)

        # btc: 1, eth: 2, sol: 2
        assert len(opportunities) == 5

    def test_batch_no_markets_with_opportunities(self):
        """Verify batch detection when no markets have opportunities."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": 0.55, "last_trade_price": 0.50},
            {"market_id": "eth-15min", "bid_price": 0.60, "last_trade_price": 0.58},
            {"market_id": "sol-15min", "bid_price": 0.65, "last_trade_price": 0.62},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 0

    def test_batch_handles_missing_market_id(self):
        """Verify batch detection handles missing market_id gracefully."""
        price_data = [
            {"bid_price": 0.75, "last_trade_price": 0.65},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 1
        assert opportunities[0].market_id == "unknown"

    def test_batch_handles_missing_bid_price(self):
        """Verify batch detection handles missing bid_price gracefully."""
        price_data = [
            {"market_id": "btc-15min", "last_trade_price": 0.75},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"

    def test_batch_handles_missing_last_trade_price(self):
        """Verify batch detection handles missing last_trade_price gracefully."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": 0.75},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 1
        assert opportunities[0].source == "bid"

    def test_batch_handles_none_values(self):
        """Verify batch detection handles None values in price data."""
        price_data = [
            {"market_id": "btc-15min", "bid_price": None, "last_trade_price": 0.75},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"

    def test_batch_order_preserved(self):
        """Verify batch opportunities are returned in order of input markets."""
        price_data = [
            {"market_id": "aaa-market", "bid_price": 0.75, "last_trade_price": 0.65},
            {"market_id": "bbb-market", "bid_price": 0.80, "last_trade_price": 0.65},
            {"market_id": "ccc-market", "bid_price": 0.85, "last_trade_price": 0.65},
        ]
        opportunities = detect_opportunities_batch(price_data, threshold=0.70)

        # Opportunities should be in order of markets processed
        market_order = [opp.market_id for opp in opportunities]
        assert market_order == ["aaa-market", "bbb-market", "ccc-market"]


class TestDetectOpportunityEdgeCases:
    """Test edge cases in opportunity detection."""

    def test_infinity_bid_price(self):
        """Verify infinity bid price is handled as invalid."""
        opportunities = detect_opportunity(
            bid_price=float("inf"),
            last_trade_price=0.75,
            threshold=0.70,
            market_id="test",
        )
        # Only last_trade should be detected since inf is invalid
        assert len(opportunities) == 1
        assert opportunities[0].source == "last_trade"

    def test_negative_infinity_last_trade(self):
        """Verify negative infinity last trade is handled as invalid."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=float("-inf"),
            threshold=0.70,
            market_id="test",
        )
        # Only bid should be detected since -inf is invalid
        assert len(opportunities) == 1
        assert opportunities[0].source == "bid"

    def test_very_small_positive_price(self):
        """Verify very small positive prices work correctly."""
        opportunities = detect_opportunity(
            bid_price=1e-10,
            last_trade_price=0.0,
            threshold=0.0,
            market_id="test",
        )
        # Both should be >= 0.0
        assert len(opportunities) == 2

    def test_floating_point_precision_at_threshold(self):
        """Verify floating point comparison at threshold boundary."""
        # 0.1 + 0.1 + 0.1 + 0.1 + 0.1 + 0.1 + 0.1 might not equal 0.7 exactly
        price = 0.1 + 0.1 + 0.1 + 0.1 + 0.1 + 0.1 + 0.1
        opportunities = detect_opportunity(
            bid_price=price,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="test",
        )
        # Due to floating point, this should still work at threshold
        # The sum is approximately 0.7000000000000001
        assert len(opportunities) == 1

    def test_empty_market_id(self):
        """Verify empty string market ID is accepted."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="",
        )
        assert len(opportunities) == 1
        assert opportunities[0].market_id == ""

    def test_unicode_market_id(self):
        """Verify unicode market ID is accepted."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="btc-15min-\u4e2d\u6587",
        )
        assert len(opportunities) == 1
        assert opportunities[0].market_id == "btc-15min-\u4e2d\u6587"

    def test_large_price_values(self):
        """Verify large price values work correctly."""
        opportunities = detect_opportunity(
            bid_price=999999.99,
            last_trade_price=888888.88,
            threshold=100000.0,
            market_id="test",
        )
        assert len(opportunities) == 2

    def test_negative_threshold(self):
        """Verify behavior with negative threshold (edge case)."""
        opportunities = detect_opportunity(
            bid_price=0.01,
            last_trade_price=0.02,
            threshold=-0.50,
            market_id="test",
        )
        # Both positive prices should be >= -0.50
        assert len(opportunities) == 2

    def test_both_prices_exactly_zero(self):
        """Verify both prices at exactly zero with zero threshold."""
        opportunities = detect_opportunity(
            bid_price=0.0,
            last_trade_price=0.0,
            threshold=0.0,
            market_id="test",
        )
        # Both should be >= 0.0
        assert len(opportunities) == 2


class TestOpportunityTimestamp:
    """Test that opportunities have correct timestamps."""

    def test_opportunity_timestamp_is_recent(self):
        """Verify opportunity timestamp is approximately current time."""
        before = datetime.now()
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.65,
            threshold=0.70,
            market_id="test",
        )
        after = datetime.now()

        assert len(opportunities) == 1
        opp_time = opportunities[0].detected_at
        assert before <= opp_time <= after

    def test_multiple_opportunities_same_timestamp(self):
        """Verify multiple opportunities from same call have same timestamp."""
        opportunities = detect_opportunity(
            bid_price=0.75,
            last_trade_price=0.80,
            threshold=0.70,
            market_id="test",
        )
        assert len(opportunities) == 2
        # Both should have the same timestamp (created in same call)
        assert opportunities[0].detected_at == opportunities[1].detected_at
