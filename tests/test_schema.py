"""Tests for database schema validation.

Tests the SQL schema files for structure, required elements, and syntax patterns
without requiring an actual PostgreSQL database connection.
"""

import os
import re
from pathlib import Path

import pytest


class TestSchemaFilesExist:
    """Test that required schema files exist."""

    def test_schema_sql_exists(self):
        """Verify db/schema.sql file exists."""
        schema_path = Path("db/schema.sql")
        assert schema_path.exists(), "db/schema.sql should exist"

    def test_migration_file_exists(self):
        """Verify db/migrations/001_initial_schema.sql file exists."""
        migration_path = Path("db/migrations/001_initial_schema.sql")
        assert migration_path.exists(), "db/migrations/001_initial_schema.sql should exist"

    def test_migrations_directory_exists(self):
        """Verify db/migrations directory exists."""
        migrations_dir = Path("db/migrations")
        assert migrations_dir.is_dir(), "db/migrations should be a directory"


class TestSchemaContent:
    """Test db/schema.sql content and structure."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_schema_contains_wallets_table(self, schema_content):
        """Verify schema contains CREATE TABLE wallets statement."""
        assert "CREATE TABLE wallets" in schema_content

    def test_schema_contains_markets_table(self, schema_content):
        """Verify schema contains CREATE TABLE markets statement."""
        assert "CREATE TABLE markets" in schema_content

    def test_schema_contains_trades_table(self, schema_content):
        """Verify schema contains CREATE TABLE trades statement."""
        assert "CREATE TABLE trades" in schema_content

    def test_schema_contains_trade_status_enum(self, schema_content):
        """Verify schema contains trade_status enum definition."""
        assert "CREATE TYPE trade_status AS ENUM" in schema_content

    def test_schema_contains_trade_side_enum(self, schema_content):
        """Verify schema contains trade_side enum definition."""
        assert "CREATE TYPE trade_side AS ENUM" in schema_content

    def test_schema_contains_order_side_enum(self, schema_content):
        """Verify schema contains order_side enum definition."""
        assert "CREATE TYPE order_side AS ENUM" in schema_content


class TestSchemaEnumValues:
    """Test that schema enums contain expected values."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_trade_status_contains_open(self, schema_content):
        """Verify trade_status enum contains 'open' value."""
        # Find the trade_status enum definition
        match = re.search(r"CREATE TYPE trade_status AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None, "trade_status enum should be defined"
        assert "'open'" in match.group()

    def test_trade_status_contains_filled(self, schema_content):
        """Verify trade_status enum contains 'filled' value."""
        match = re.search(r"CREATE TYPE trade_status AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None
        assert "'filled'" in match.group()

    def test_trade_status_contains_partially_filled(self, schema_content):
        """Verify trade_status enum contains 'partially_filled' value."""
        match = re.search(r"CREATE TYPE trade_status AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None
        assert "'partially_filled'" in match.group()

    def test_trade_status_contains_cancelled(self, schema_content):
        """Verify trade_status enum contains 'cancelled' value."""
        match = re.search(r"CREATE TYPE trade_status AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None
        assert "'cancelled'" in match.group()

    def test_trade_status_contains_closed(self, schema_content):
        """Verify trade_status enum contains 'closed' value."""
        match = re.search(r"CREATE TYPE trade_status AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None
        assert "'closed'" in match.group()

    def test_trade_side_contains_yes(self, schema_content):
        """Verify trade_side enum contains 'YES' value."""
        match = re.search(r"CREATE TYPE trade_side AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None, "trade_side enum should be defined"
        assert "'YES'" in match.group()

    def test_trade_side_contains_no(self, schema_content):
        """Verify trade_side enum contains 'NO' value."""
        match = re.search(r"CREATE TYPE trade_side AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None
        assert "'NO'" in match.group()

    def test_order_side_contains_buy(self, schema_content):
        """Verify order_side enum contains 'BUY' value."""
        match = re.search(r"CREATE TYPE order_side AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None, "order_side enum should be defined"
        assert "'BUY'" in match.group()

    def test_order_side_contains_sell(self, schema_content):
        """Verify order_side enum contains 'SELL' value."""
        match = re.search(r"CREATE TYPE order_side AS ENUM\s*\([^)]+\)", schema_content)
        assert match is not None
        assert "'SELL'" in match.group()


class TestSchemaTableColumns:
    """Test that tables contain required columns."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_wallets_has_id_column(self, schema_content):
        """Verify wallets table has id column."""
        # Find wallets table definition
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None, "wallets table should be defined"
        assert "id UUID" in match.group()

    def test_wallets_has_address_column(self, schema_content):
        """Verify wallets table has address column."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "address VARCHAR" in match.group()

    def test_wallets_has_signature_type_column(self, schema_content):
        """Verify wallets table has signature_type column."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "signature_type" in match.group()

    def test_wallets_has_is_active_column(self, schema_content):
        """Verify wallets table has is_active column."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "is_active BOOLEAN" in match.group()

    def test_markets_has_id_column(self, schema_content):
        """Verify markets table has id column."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None, "markets table should be defined"
        assert "id UUID" in match.group()

    def test_markets_has_condition_id_column(self, schema_content):
        """Verify markets table has condition_id column."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "condition_id VARCHAR" in match.group()

    def test_markets_has_resolved_column(self, schema_content):
        """Verify markets table has resolved column."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "resolved BOOLEAN" in match.group()

    def test_markets_has_winning_side_column(self, schema_content):
        """Verify markets table has winning_side column."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "winning_side" in match.group()

    def test_trades_has_id_column(self, schema_content):
        """Verify trades table has id column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None, "trades table should be defined"
        assert "id UUID" in match.group()

    def test_trades_has_wallet_id_foreign_key(self, schema_content):
        """Verify trades table has wallet_id foreign key."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "wallet_id UUID" in match.group()
        assert "REFERENCES wallets(id)" in match.group()

    def test_trades_has_market_id_foreign_key(self, schema_content):
        """Verify trades table has market_id foreign key."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "market_id UUID" in match.group()
        assert "REFERENCES markets(id)" in match.group()

    def test_trades_has_token_id_column(self, schema_content):
        """Verify trades table has token_id column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "token_id TEXT" in match.group()

    def test_trades_has_side_column(self, schema_content):
        """Verify trades table has side column with trade_side type."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "side trade_side" in match.group()

    def test_trades_has_order_type_column(self, schema_content):
        """Verify trades table has order_type column with order_side type."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "order_type order_side" in match.group()

    def test_trades_has_quantity_column(self, schema_content):
        """Verify trades table has quantity column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "quantity NUMERIC" in match.group()

    def test_trades_has_filled_quantity_column(self, schema_content):
        """Verify trades table has filled_quantity column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "filled_quantity NUMERIC" in match.group()

    def test_trades_has_limit_price_column(self, schema_content):
        """Verify trades table has limit_price column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "limit_price NUMERIC" in match.group()

    def test_trades_has_cost_basis_usd_column(self, schema_content):
        """Verify trades table has cost_basis_usd column for P&L tracking."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "cost_basis_usd NUMERIC" in match.group()

    def test_trades_has_proceeds_usd_column(self, schema_content):
        """Verify trades table has proceeds_usd column for P&L tracking."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "proceeds_usd NUMERIC" in match.group()

    def test_trades_has_realized_pnl_column(self, schema_content):
        """Verify trades table has realized_pnl column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "realized_pnl NUMERIC" in match.group()

    def test_trades_has_neg_risk_column(self, schema_content):
        """Verify trades table has neg_risk column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "neg_risk BOOLEAN" in match.group()

    def test_trades_has_status_column(self, schema_content):
        """Verify trades table has status column with trade_status type."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "status trade_status" in match.group()


class TestSchemaIndexes:
    """Test that schema contains required indexes."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_wallets_address_index(self, schema_content):
        """Verify index on wallets.address exists."""
        assert "CREATE INDEX idx_wallets_address ON wallets(address)" in schema_content

    def test_markets_condition_id_index(self, schema_content):
        """Verify index on markets.condition_id exists."""
        assert "CREATE INDEX idx_markets_condition_id ON markets(condition_id)" in schema_content

    def test_markets_resolved_index(self, schema_content):
        """Verify partial index on markets.resolved exists."""
        assert "idx_markets_resolved" in schema_content
        assert "resolved" in schema_content

    def test_trades_wallet_id_index(self, schema_content):
        """Verify index on trades.wallet_id exists."""
        assert "CREATE INDEX idx_trades_wallet_id ON trades(wallet_id)" in schema_content

    def test_trades_market_id_index(self, schema_content):
        """Verify index on trades.market_id exists."""
        assert "CREATE INDEX idx_trades_market_id ON trades(market_id)" in schema_content

    def test_trades_status_index(self, schema_content):
        """Verify index on trades.status exists."""
        assert "CREATE INDEX idx_trades_status ON trades(status)" in schema_content

    def test_trades_wallet_status_composite_index(self, schema_content):
        """Verify composite index on trades(wallet_id, status) exists."""
        assert "idx_trades_wallet_status" in schema_content

    def test_trades_created_at_index(self, schema_content):
        """Verify index on trades.created_at exists."""
        assert "CREATE INDEX idx_trades_created_at ON trades(created_at)" in schema_content

    def test_trades_wallet_market_composite_index(self, schema_content):
        """Verify composite index on trades(wallet_id, market_id) exists."""
        assert "idx_trades_wallet_market" in schema_content


class TestSchemaTimestamps:
    """Test that tables have required timestamp columns."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_wallets_has_created_at(self, schema_content):
        """Verify wallets table has created_at column."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "created_at TIMESTAMPTZ" in match.group()

    def test_wallets_has_updated_at(self, schema_content):
        """Verify wallets table has updated_at column."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "updated_at TIMESTAMPTZ" in match.group()

    def test_markets_has_created_at(self, schema_content):
        """Verify markets table has created_at column."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "created_at TIMESTAMPTZ" in match.group()

    def test_markets_has_updated_at(self, schema_content):
        """Verify markets table has updated_at column."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "updated_at TIMESTAMPTZ" in match.group()

    def test_trades_has_created_at(self, schema_content):
        """Verify trades table has created_at column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "created_at TIMESTAMPTZ" in match.group()

    def test_trades_has_updated_at(self, schema_content):
        """Verify trades table has updated_at column."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "updated_at TIMESTAMPTZ" in match.group()

    def test_trades_has_filled_at(self, schema_content):
        """Verify trades table has filled_at column for tax reporting."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "filled_at TIMESTAMPTZ" in match.group()

    def test_trades_has_closed_at(self, schema_content):
        """Verify trades table has closed_at column for tax reporting."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "closed_at TIMESTAMPTZ" in match.group()


class TestMigrationContent:
    """Test db/migrations/001_initial_schema.sql content."""

    @pytest.fixture
    def migration_content(self):
        """Load migration file content."""
        migration_path = Path("db/migrations/001_initial_schema.sql")
        return migration_path.read_text()

    def test_migration_has_transaction_begin(self, migration_content):
        """Verify migration starts with BEGIN transaction."""
        assert "BEGIN" in migration_content

    def test_migration_has_transaction_commit(self, migration_content):
        """Verify migration ends with COMMIT transaction."""
        assert "COMMIT" in migration_content

    def test_migration_is_idempotent_tables(self, migration_content):
        """Verify migration uses IF NOT EXISTS for tables."""
        assert "CREATE TABLE IF NOT EXISTS wallets" in migration_content
        assert "CREATE TABLE IF NOT EXISTS markets" in migration_content
        assert "CREATE TABLE IF NOT EXISTS trades" in migration_content

    def test_migration_is_idempotent_indexes(self, migration_content):
        """Verify migration uses IF NOT EXISTS for indexes."""
        assert "CREATE INDEX IF NOT EXISTS" in migration_content

    def test_migration_is_idempotent_enums(self, migration_content):
        """Verify migration checks for existing enum types."""
        # Enums use DO $$ block with pg_type check
        assert "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'trade_status')" in migration_content
        assert "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'trade_side')" in migration_content
        assert "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_side')" in migration_content

    def test_migration_contains_all_tables(self, migration_content):
        """Verify migration creates all required tables."""
        assert "CREATE TABLE" in migration_content
        assert "wallets" in migration_content
        assert "markets" in migration_content
        assert "trades" in migration_content

    def test_migration_contains_all_enums(self, migration_content):
        """Verify migration creates all required enum types."""
        assert "trade_status" in migration_content
        assert "trade_side" in migration_content
        assert "order_side" in migration_content

    def test_migration_contains_all_indexes(self, migration_content):
        """Verify migration creates indexes."""
        assert "idx_wallets_address" in migration_content
        assert "idx_markets_condition_id" in migration_content
        assert "idx_trades_wallet_id" in migration_content
        assert "idx_trades_market_id" in migration_content
        assert "idx_trades_status" in migration_content


class TestSchemaDataTypes:
    """Test that schema uses correct PostgreSQL data types."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_uses_uuid_for_primary_keys(self, schema_content):
        """Verify UUID is used for primary keys."""
        assert "id UUID PRIMARY KEY" in schema_content

    def test_uses_timestamptz_for_timestamps(self, schema_content):
        """Verify TIMESTAMPTZ is used for timestamp columns."""
        assert "TIMESTAMPTZ" in schema_content
        # Should not use plain TIMESTAMP
        lines_with_timestamp = [
            line for line in schema_content.split("\n")
            if "created_at" in line or "updated_at" in line
        ]
        for line in lines_with_timestamp:
            if "TIMESTAMP" in line.upper():
                assert "TIMESTAMPTZ" in line

    def test_uses_numeric_for_monetary_values(self, schema_content):
        """Verify NUMERIC is used for monetary/price columns."""
        # Check that price columns use NUMERIC, not FLOAT/DOUBLE
        assert "limit_price NUMERIC" in schema_content
        assert "cost_basis_usd NUMERIC" in schema_content
        assert "proceeds_usd NUMERIC" in schema_content
        assert "realized_pnl NUMERIC" in schema_content

    def test_does_not_use_float_for_money(self, schema_content):
        """Verify FLOAT/DOUBLE is not used for monetary values."""
        # Check common money-related columns
        money_columns = [
            "limit_price",
            "avg_fill_price",
            "exit_price",
            "cost_basis_usd",
            "proceeds_usd",
            "realized_pnl",
        ]
        for col in money_columns:
            # Find the line defining this column
            pattern = rf"{col}\s+(FLOAT|DOUBLE|REAL)"
            match = re.search(pattern, schema_content, re.IGNORECASE)
            assert match is None, f"{col} should not use FLOAT/DOUBLE type"


class TestSchemaConstraints:
    """Test that schema has appropriate constraints."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_wallets_address_not_null(self, schema_content):
        """Verify wallets.address has NOT NULL constraint."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        # Check that address line has NOT NULL
        assert re.search(r"address\s+VARCHAR\(\d+\)\s+NOT NULL", match.group())

    def test_wallets_address_unique(self, schema_content):
        """Verify wallets.address has UNIQUE constraint."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "UNIQUE" in match.group()
        assert "address" in match.group()

    def test_markets_condition_id_not_null(self, schema_content):
        """Verify markets.condition_id has NOT NULL constraint."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"condition_id\s+VARCHAR\(\d+\)\s+NOT NULL", match.group())

    def test_markets_condition_id_unique(self, schema_content):
        """Verify markets.condition_id has UNIQUE constraint."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert "UNIQUE" in match.group()

    def test_trades_token_id_not_null(self, schema_content):
        """Verify trades.token_id has NOT NULL constraint."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"token_id\s+TEXT\s+NOT NULL", match.group())

    def test_trades_quantity_not_null(self, schema_content):
        """Verify trades.quantity has NOT NULL constraint."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        # Check quantity line has NOT NULL
        quantity_line = re.search(r"quantity\s+NUMERIC\([^)]+\)\s+NOT NULL", match.group())
        assert quantity_line is not None


class TestSchemaDefaultValues:
    """Test that schema has appropriate default values."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_wallets_is_active_defaults_true(self, schema_content):
        """Verify wallets.is_active defaults to true."""
        match = re.search(
            r"CREATE TABLE wallets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"is_active\s+BOOLEAN.*DEFAULT\s+true", match.group(), re.IGNORECASE)

    def test_markets_resolved_defaults_false(self, schema_content):
        """Verify markets.resolved defaults to false."""
        match = re.search(
            r"CREATE TABLE markets\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"resolved\s+BOOLEAN.*DEFAULT\s+false", match.group(), re.IGNORECASE)

    def test_trades_status_defaults_open(self, schema_content):
        """Verify trades.status defaults to 'open'."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"status\s+trade_status.*DEFAULT\s+'open'", match.group())

    def test_trades_neg_risk_defaults_false(self, schema_content):
        """Verify trades.neg_risk defaults to false."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"neg_risk\s+BOOLEAN.*DEFAULT\s+false", match.group(), re.IGNORECASE)

    def test_trades_filled_quantity_defaults_zero(self, schema_content):
        """Verify trades.filled_quantity defaults to 0."""
        match = re.search(
            r"CREATE TABLE trades\s*\([^;]+\);",
            schema_content,
            re.DOTALL,
        )
        assert match is not None
        assert re.search(r"filled_quantity\s+NUMERIC\([^)]+\).*DEFAULT\s+0", match.group())

    def test_timestamps_default_now(self, schema_content):
        """Verify timestamp columns default to NOW()."""
        assert "DEFAULT NOW()" in schema_content


class TestSchemaComments:
    """Test that schema includes documentation comments."""

    @pytest.fixture
    def schema_content(self):
        """Load schema.sql content."""
        schema_path = Path("db/schema.sql")
        return schema_path.read_text()

    def test_has_table_comments(self, schema_content):
        """Verify schema includes COMMENT ON TABLE statements."""
        assert "COMMENT ON TABLE wallets" in schema_content
        assert "COMMENT ON TABLE markets" in schema_content
        assert "COMMENT ON TABLE trades" in schema_content

    def test_has_column_comments(self, schema_content):
        """Verify schema includes COMMENT ON COLUMN statements."""
        assert "COMMENT ON COLUMN" in schema_content
