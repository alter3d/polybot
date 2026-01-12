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

    def test_default_trade_amount_usd(self):
        """Verify default trade amount is $20.00."""
        config = Config()
        assert config.trade_amount_usd == 20.0

    def test_default_auto_trade_enabled(self):
        """Verify auto trade is disabled by default."""
        config = Config()
        assert config.auto_trade_enabled is False

    def test_default_private_key_empty(self):
        """Verify private key is empty string by default."""
        config = Config()
        assert config.private_key == ""

    def test_default_signature_type(self):
        """Verify default signature type is 0 (EOA)."""
        config = Config()
        assert config.signature_type == 0

    def test_default_funder_address_empty(self):
        """Verify funder_address is empty string by default."""
        config = Config()
        assert config.funder_address == ""

    def test_default_reversal_multiplier(self):
        """Verify default reversal multiplier is 1.5."""
        config = Config()
        assert config.reversal_multiplier == 1.5

    def test_default_limit_price(self):
        """Verify default limit price is 0.90."""
        config = Config()
        assert config.limit_price == 0.90


class TestConfigFromEnv:
    """Test environment variable configuration loading."""

    def test_from_env_with_no_env_vars(self):
        """Verify from_env uses defaults when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.70
            assert config.monitor_start_minutes_before_end == 3
            assert config.log_level == "INFO"
            assert config.series_ids == []
            assert config.reversal_multiplier == 1.5
            assert config.limit_price == 0.90

    def test_from_env_opportunity_threshold_override(self):
        """Verify OPPORTUNITY_THRESHOLD env var overrides default."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.50"}, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.50

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
            "MONITOR_START_MINUTES": "2",
            "LOG_LEVEL": "WARNING",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            config = Config.from_env()
            assert config.opportunity_threshold == 0.65
            assert config.monitor_start_minutes_before_end == 2
            assert config.log_level == "WARNING"

    def test_from_env_threshold_float_conversion(self):
        """Verify opportunity threshold is correctly converted to float."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.85"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.opportunity_threshold, float)
            assert config.opportunity_threshold == 0.85

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

    def test_from_env_trade_amount_usd_override(self):
        """Verify TRADE_AMOUNT_USD env var overrides default."""
        with patch.dict(os.environ, {"TRADE_AMOUNT_USD": "50.0"}, clear=True):
            config = Config.from_env()
            assert config.trade_amount_usd == 50.0

    def test_from_env_trade_amount_usd_float_conversion(self):
        """Verify trade_amount_usd is correctly converted to float."""
        with patch.dict(os.environ, {"TRADE_AMOUNT_USD": "25.50"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.trade_amount_usd, float)
            assert config.trade_amount_usd == 25.50

    def test_from_env_auto_trade_enabled_true(self):
        """Verify AUTO_TRADE_ENABLED=true enables trading."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": "true"}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is True

    def test_from_env_auto_trade_enabled_false(self):
        """Verify AUTO_TRADE_ENABLED=false disables trading."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": "false"}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is False

    def test_from_env_auto_trade_enabled_one(self):
        """Verify AUTO_TRADE_ENABLED=1 enables trading."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": "1"}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is True

    def test_from_env_auto_trade_enabled_yes(self):
        """Verify AUTO_TRADE_ENABLED=yes enables trading."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": "yes"}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is True

    def test_from_env_auto_trade_enabled_case_insensitive(self):
        """Verify AUTO_TRADE_ENABLED handles mixed case."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": "TRUE"}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is True

    def test_from_env_private_key_override(self):
        """Verify PRIVATE_KEY env var overrides default."""
        with patch.dict(os.environ, {"PRIVATE_KEY": "0x1234567890abcdef"}, clear=True):
            config = Config.from_env()
            assert config.private_key == "0x1234567890abcdef"

    def test_from_env_signature_type_override(self):
        """Verify SIGNATURE_TYPE env var overrides default."""
        with patch.dict(os.environ, {"SIGNATURE_TYPE": "1"}, clear=True):
            config = Config.from_env()
            assert config.signature_type == 1

    def test_from_env_signature_type_int_conversion(self):
        """Verify signature_type is correctly converted to int."""
        with patch.dict(os.environ, {"SIGNATURE_TYPE": "2"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.signature_type, int)
            assert config.signature_type == 2

    def test_from_env_trading_multiple_overrides(self):
        """Verify multiple trading env vars can be set simultaneously."""
        env_vars = {
            "TRADE_AMOUNT_USD": "100.0",
            "AUTO_TRADE_ENABLED": "true",
            "PRIVATE_KEY": "0xdeadbeef",
            "SIGNATURE_TYPE": "1",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            config = Config.from_env()
            assert config.trade_amount_usd == 100.0
            assert config.auto_trade_enabled is True
            assert config.private_key == "0xdeadbeef"
            assert config.signature_type == 1

    def test_from_env_funder_address_override(self):
        """Verify FUNDER_ADDRESS env var overrides default."""
        with patch.dict(os.environ, {"FUNDER_ADDRESS": "0x1234567890abcdef1234567890abcdef12345678"}, clear=True):
            config = Config.from_env()
            assert config.funder_address == "0x1234567890abcdef1234567890abcdef12345678"

    def test_from_env_funder_address_with_signature_type_1(self):
        """Verify FUNDER_ADDRESS with SIGNATURE_TYPE=1 for Magic wallet."""
        env_vars = {
            "SIGNATURE_TYPE": "1",
            "FUNDER_ADDRESS": "0xfunder1234567890abcdef1234567890abcdef1234",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            config = Config.from_env()
            assert config.signature_type == 1
            assert config.funder_address == "0xfunder1234567890abcdef1234567890abcdef1234"

    def test_from_env_reversal_multiplier_override(self):
        """Verify REVERSAL_MULTIPLIER env var overrides default."""
        with patch.dict(os.environ, {"REVERSAL_MULTIPLIER": "2.0"}, clear=True):
            config = Config.from_env()
            assert config.reversal_multiplier == 2.0

    def test_from_env_reversal_multiplier_float_conversion(self):
        """Verify reversal_multiplier is correctly converted to float."""
        with patch.dict(os.environ, {"REVERSAL_MULTIPLIER": "2.5"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.reversal_multiplier, float)
            assert config.reversal_multiplier == 2.5

    def test_from_env_limit_price_override(self):
        """Verify LIMIT_PRICE env var overrides default."""
        with patch.dict(os.environ, {"LIMIT_PRICE": "0.85"}, clear=True):
            config = Config.from_env()
            assert config.limit_price == 0.85

    def test_from_env_limit_price_float_conversion(self):
        """Verify limit_price is correctly converted to float."""
        with patch.dict(os.environ, {"LIMIT_PRICE": "0.95"}, clear=True):
            config = Config.from_env()
            assert isinstance(config.limit_price, float)
            assert config.limit_price == 0.95


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
            monitor_start_minutes_before_end=5,
            log_level="DEBUG",
        )
        assert config.opportunity_threshold == 0.80
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

    def test_config_custom_trading_values(self):
        """Verify Config can be instantiated with custom trading values."""
        config = Config(
            trade_amount_usd=50.0,
            auto_trade_enabled=True,
            private_key="0xabc123",
            signature_type=2,
        )
        assert config.trade_amount_usd == 50.0
        assert config.auto_trade_enabled is True
        assert config.private_key == "0xabc123"
        assert config.signature_type == 2

    def test_config_custom_funder_address(self):
        """Verify Config can be instantiated with custom funder_address."""
        config = Config(
            signature_type=1,
            funder_address="0xfunder1234567890abcdef",
        )
        assert config.signature_type == 1
        assert config.funder_address == "0xfunder1234567890abcdef"

    def test_config_custom_reversal_multiplier(self):
        """Verify Config can be instantiated with custom reversal_multiplier."""
        config = Config(reversal_multiplier=2.5)
        assert config.reversal_multiplier == 2.5

    def test_config_custom_limit_price(self):
        """Verify Config can be instantiated with custom limit_price."""
        config = Config(limit_price=0.85)
        assert config.limit_price == 0.85

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

    def test_from_env_trade_amount_zero(self):
        """Verify zero trade amount is valid."""
        with patch.dict(os.environ, {"TRADE_AMOUNT_USD": "0.0"}, clear=True):
            config = Config.from_env()
            assert config.trade_amount_usd == 0.0

    def test_from_env_trade_amount_large(self):
        """Verify large trade amounts are valid."""
        with patch.dict(os.environ, {"TRADE_AMOUNT_USD": "10000.0"}, clear=True):
            config = Config.from_env()
            assert config.trade_amount_usd == 10000.0

    def test_from_env_trade_amount_invalid_raises_error(self):
        """Verify invalid trade amount value raises ValueError."""
        with patch.dict(os.environ, {"TRADE_AMOUNT_USD": "not-a-number"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()

    def test_from_env_signature_type_invalid_raises_error(self):
        """Verify invalid signature type value raises ValueError."""
        with patch.dict(os.environ, {"SIGNATURE_TYPE": "not-an-int"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()

    def test_from_env_auto_trade_enabled_invalid_string(self):
        """Verify invalid AUTO_TRADE_ENABLED string defaults to false."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": "invalid"}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is False

    def test_from_env_auto_trade_enabled_empty_string(self):
        """Verify empty AUTO_TRADE_ENABLED string defaults to false."""
        with patch.dict(os.environ, {"AUTO_TRADE_ENABLED": ""}, clear=True):
            config = Config.from_env()
            assert config.auto_trade_enabled is False

    def test_from_env_preserves_default_trading_params(self):
        """Verify trading params keep defaults when not overridden."""
        with patch.dict(os.environ, {"OPPORTUNITY_THRESHOLD": "0.60"}, clear=True):
            config = Config.from_env()
            # Should still have default trading params
            assert config.trade_amount_usd == 20.0
            assert config.auto_trade_enabled is False
            assert config.private_key == ""
            assert config.signature_type == 0
            assert config.funder_address == ""

    def test_from_env_funder_address_empty_string(self):
        """Verify empty FUNDER_ADDRESS results in empty string."""
        with patch.dict(os.environ, {"FUNDER_ADDRESS": ""}, clear=True):
            config = Config.from_env()
            assert config.funder_address == ""

    def test_from_env_reversal_multiplier_one(self):
        """Verify reversal multiplier of 1.0 (no multiplier) is valid."""
        with patch.dict(os.environ, {"REVERSAL_MULTIPLIER": "1.0"}, clear=True):
            config = Config.from_env()
            assert config.reversal_multiplier == 1.0

    def test_from_env_reversal_multiplier_large(self):
        """Verify large reversal multiplier values are valid."""
        with patch.dict(os.environ, {"REVERSAL_MULTIPLIER": "5.0"}, clear=True):
            config = Config.from_env()
            assert config.reversal_multiplier == 5.0

    def test_from_env_reversal_multiplier_invalid_raises_error(self):
        """Verify invalid reversal multiplier value raises ValueError."""
        with patch.dict(os.environ, {"REVERSAL_MULTIPLIER": "not-a-number"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()

    def test_from_env_limit_price_zero(self):
        """Verify zero limit price is valid."""
        with patch.dict(os.environ, {"LIMIT_PRICE": "0.0"}, clear=True):
            config = Config.from_env()
            assert config.limit_price == 0.0

    def test_from_env_limit_price_one(self):
        """Verify limit price of 1.0 (100%) is valid."""
        with patch.dict(os.environ, {"LIMIT_PRICE": "1.0"}, clear=True):
            config = Config.from_env()
            assert config.limit_price == 1.0

    def test_from_env_limit_price_invalid_raises_error(self):
        """Verify invalid limit price value raises ValueError."""
        with patch.dict(os.environ, {"LIMIT_PRICE": "not-a-number"}, clear=True):
            with pytest.raises(ValueError):
                Config.from_env()
