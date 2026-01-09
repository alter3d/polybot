"""Tests for configuration module.

Tests the Config dataclass and environment variable handling.
"""

import os
from unittest.mock import patch

import pytest

from src.config import Config


class TestConfigDefaults:
    """Test default configuration values."""

    def test_default_opportunity_threshold(self):
        """Verify default opportunity threshold is 0.70."""
        config = Config()
        assert config.opportunity_threshold == 0.70

    def test_default_shares_to_trade(self):
        """Verify default shares to trade is 20."""
        config = Config()
        assert config.shares_to_trade == 20

    def test_default_monitor_start_minutes(self):
        """Verify default monitoring starts 3 minutes before window end."""
        config = Config()
        assert config.monitor_start_minutes_before_end == 3

    def test_default_clob_host(self):
        """Verify default CLOB API endpoint."""
        config = Config()
        assert config.clob_host == "https://clob.polymarket.com"

    def test_default_gamma_host(self):
        """Verify default Gamma API endpoint."""
        config = Config()
        assert config.gamma_host == "https://gamma-api.polymarket.com"

    def test_default_ws_host(self):
        """Verify default WebSocket endpoint."""
        config = Config()
        assert config.ws_host == "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def test_default_log_level(self):
        """Verify default log level is INFO."""
        config = Config()
        assert config.log_level == "INFO"

    def test_default_series_ids_empty(self):
        """Verify default series_ids is empty list."""
        config = Config()
        assert config.series_ids == []


class TestConfigFromEnv:
    """Test environment variable configuration loading."""

    def test_from_env_with_no_env_vars(self):
        """Verify from_env uses defaults when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.70
            assert config.shares_to_trade == 20
            assert config.monitor_start_minutes_before_end == 3
            assert config.log_level == "INFO"
            assert config.series_ids == []

    def test_from_env_opportunity_threshold_override(self):
        """Verify OPPORTUNITY_THRESHOLD env var overrides default."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.50"}, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.50

    def test_from_env_shares_to_trade_override(self):
        """Verify SHARES_TO_TRADE env var overrides default."""
        with patch.dict(os.environ, {"SHARES_TO_TRADE": "100"}, clear=True):
            config = Config.from_env()
            assert config.shares_to_trade == 100

    def test_from_env_monitor_start_minutes_override(self):
        """Verify MONITOR_START_MINUTES env var overrides default."""
        with patch.dict(os.environ, {"MONITOR_START_MINUTES": "5"}, clear=True):
            config = Config.from_env()
            assert config.monitor_start_minutes_before_end == 5

    def test_from_env_log_level_override(self):
        """Verify LOG_LEVEL env var overrides default."""
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}, clear=True):
            config = Config.from_env()
            assert config.log_level == "DEBUG"

    def test_from_env_multiple_overrides(self):
        """Verify multiple env vars can be set simultaneously."""
        env_vars = {
            "OPPORTUNITY_THRESHOLD": "0.65",
            "SHARES_TO_TRADE": "50",
            "MONITOR_START_MINUTES": "2",
            "LOG_LEVEL": "WARNING",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.65
            assert config.shares_to_trade == 50
            assert config.monitor_start_minutes_before_end == 2
            assert config.log_level == "WARNING"

    def test_from_env_threshold_float_conversion(self):
        """Verify opportunity threshold is correctly converted to float."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.85"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.opportunity_threshold, float)
            assert config.opportunity_threshold == 0.85

    def test_from_env_shares_int_conversion(self):
        """Verify shares_to_trade is correctly converted to int."""
        with patch.dict(os.environ, {"SHARES_TO_TRADE": "42"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.shares_to_trade, int)
            assert config.shares_to_trade == 42

    def test_from_env_series_ids_single(self):
        """Verify SERIES_IDS env var with single series ID."""
        with patch.dict(os.environ, {"SERIES_IDS": "abc123"}, clear=True):
            config = Config.from_env()
            assert config.series_ids == ["abc123"]

    def test_from_env_series_ids_multiple(self):
        """Verify SERIES_IDS env var with multiple series IDs."""
        with patch.dict(os.environ, {"SERIES_IDS": "abc123,def456,ghi789"}, clear=True):
            config = Config.from_env()
            assert config.series_ids == ["abc123", "def456", "ghi789"]

    def test_from_env_series_ids_with_spaces(self):
        """Verify SERIES_IDS env var handles whitespace properly."""
        with patch.dict(os.environ, {"SERIES_IDS": " abc123 , def456 "}, clear=True):
            config = Config.from_env()
            assert config.series_ids == ["abc123", "def456"]

    def test_from_env_series_ids_empty_string(self):
        """Verify empty SERIES_IDS results in empty list."""
        with patch.dict(os.environ, {"SERIES_IDS": ""}, clear=True):
            config = Config.from_env()
            assert config.series_ids == []


class TestConfigDataclass:
    """Test Config dataclass behavior."""

    def test_config_is_dataclass(self):
        """Verify Config is a proper dataclass."""
        config = Config()
        # Dataclasses have __dataclass_fields__
        assert hasattr(config, "__dataclass_fields__")

    def test_config_custom_values(self):
        """Verify Config can be instantiated with custom values."""
        config = Config(
            opportunity_threshold=0.80,
            shares_to_trade=30,
            monitor_start_minutes_before_end=5,
            log_level="DEBUG",
        )
        assert config.opportunity_threshold == 0.80
        assert config.shares_to_trade == 30
        assert config.monitor_start_minutes_before_end == 5
        assert config.log_level == "DEBUG"

    def test_config_custom_endpoints(self):
        """Verify Config can use custom API endpoints."""
        config = Config(
            clob_host="https://custom-clob.example.com",
            gamma_host="https://custom-gamma.example.com",
            ws_host="wss://custom-ws.example.com",
        )
        assert config.clob_host == "https://custom-clob.example.com"
        assert config.gamma_host == "https://custom-gamma.example.com"
        assert config.ws_host == "wss://custom-ws.example.com"

    def test_config_custom_series_ids(self):
        """Verify Config can be instantiated with custom series_ids."""
        config = Config(series_ids=["series1", "series2"])
        assert config.series_ids == ["series1", "series2"]

    def test_config_equality(self):
        """Verify two Config instances with same values are equal."""
        config1 = Config(opportunity_threshold=0.75)
        config2 = Config(opportunity_threshold=0.75)
        assert config1 == config2

    def test_config_inequality(self):
        """Verify two Config instances with different values are not equal."""
        config1 = Config(opportunity_threshold=0.75)
        config2 = Config(opportunity_threshold=0.80)
        assert config1 != config2


class TestConfigEdgeCases:
    """Test edge cases in configuration handling."""

    def test_from_env_threshold_zero(self):
        """Verify zero threshold is valid."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.0"}, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.0

    def test_from_env_threshold_one(self):
        """Verify threshold of 1.0 (100%) is valid."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "1.0"}, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 1.0

    def test_from_env_shares_zero(self):
        """Verify zero shares is valid."""
        with patch.dict(os.environ, {"SHARES_TO_TRADE": "0"}, clear=True):
            config = Config.from_env()
            assert config.shares_to_trade == 0

    def test_from_env_monitor_minutes_one(self):
        """Verify single minute monitoring window is valid."""
        with patch.dict(os.environ, {"MONITOR_START_MINUTES": "1"}, clear=True):
            config = Config.from_env()
            assert config.monitor_start_minutes_before_end == 1

    def test_from_env_preserves_default_endpoints(self):
        """Verify API endpoints keep defaults when not overridden."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.60"}, clear=True):
            config = Config.from_env()
            # Should still have default endpoints
            assert config.clob_host == "https://clob.polymarket.com"
            assert config.gamma_host == "https://gamma-api.polymarket.com"
            assert config.ws_host == "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def test_from_env_invalid_float_raises_error(self):
        """Verify invalid float value raises ValueError."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "not-a-number"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()

    def test_from_env_invalid_int_raises_error(self):
        """Verify invalid int value raises ValueError."""
        with patch.dict(os.environ, {"SHARES_TO_TRADE": "not-an-int"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()
