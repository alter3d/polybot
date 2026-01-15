#!/usr/bin/env python3
"""Static validation script for PostgreSQL migration file."""

import re
import sys
import os

def validate_migration():
    """Validate the migration file structure and content."""
    migration_path = 'db/migrations/001_initial_schema.sql'

    if not os.path.exists(migration_path):
        print(f"ERROR: Migration file not found: {migration_path}")
        return False

    with open(migration_path, 'r') as f:
        content = f.read()

    errors = []

    # Check for BEGIN/COMMIT transaction wrapper
    if 'BEGIN;' not in content:
        errors.append("Missing BEGIN; statement for transaction")
    else:
        print("OK: BEGIN; transaction statement found")

    if 'COMMIT;' not in content:
        errors.append("Missing COMMIT; statement for transaction")
    else:
        print("OK: COMMIT; transaction statement found")

    # Check for required ENUMs
    required_enums = ['trade_status', 'trade_side', 'order_side']
    for enum in required_enums:
        if f"CREATE TYPE {enum}" not in content:
            errors.append(f"Missing ENUM: {enum}")
        else:
            print(f"OK: ENUM {enum} found")

    # Check for required tables
    required_tables = ['wallets', 'markets', 'trades']
    for table in required_tables:
        if f"CREATE TABLE IF NOT EXISTS {table}" not in content:
            errors.append(f"Missing TABLE: {table}")
        else:
            print(f"OK: TABLE {table} found")

    # Check for idempotent ENUM creation
    if content.count("IF NOT EXISTS (SELECT 1 FROM pg_type") == 3:
        print("OK: All ENUMs have idempotent checks")
    else:
        errors.append("Not all ENUMs have idempotent checks")

    # Check for required indexes
    required_indexes = [
        'idx_wallets_address',
        'idx_markets_condition_id',
        'idx_markets_resolved',
        'idx_trades_wallet_id',
        'idx_trades_market_id',
        'idx_trades_status',
        'idx_trades_wallet_status',
        'idx_trades_created_at',
        'idx_trades_wallet_market'
    ]
    for idx in required_indexes:
        if f"CREATE INDEX IF NOT EXISTS {idx}" not in content:
            errors.append(f"Missing INDEX: {idx}")
        else:
            print(f"OK: INDEX {idx} found")

    # Check for foreign key relationships
    if "REFERENCES wallets(id)" in content:
        print("OK: Foreign key trades.wallet_id -> wallets.id found")
    else:
        errors.append("Missing foreign key: trades.wallet_id -> wallets.id")

    if "REFERENCES markets(id)" in content:
        print("OK: Foreign key trades.market_id -> markets.id found")
    else:
        errors.append("Missing foreign key: trades.market_id -> markets.id")

    # Check for proper data types
    if "NUMERIC(20, 8)" in content:
        print("OK: NUMERIC(20, 8) data type found for monetary values")
    else:
        errors.append("Missing NUMERIC(20, 8) for monetary values")

    if "TIMESTAMPTZ" in content:
        print("OK: TIMESTAMPTZ data type found")
    else:
        errors.append("Missing TIMESTAMPTZ for timestamps")

    if "UUID PRIMARY KEY DEFAULT gen_random_uuid()" in content:
        print("OK: UUID primary keys with gen_random_uuid() found")
    else:
        errors.append("Missing UUID primary keys with gen_random_uuid()")

    # Check trade_status enum values
    trade_status_match = re.search(r"CREATE TYPE trade_status AS ENUM\s*\(([^)]+)\)", content)
    if trade_status_match:
        values = trade_status_match.group(1)
        required_statuses = ['open', 'filled', 'partially_filled', 'cancelled', 'closed']
        missing = [s for s in required_statuses if f"'{s}'" not in values]
        if missing:
            errors.append(f"Missing trade_status values: {missing}")
        else:
            print("OK: trade_status enum has all required values")

    print("\n" + "="*60)
    if errors:
        print("VALIDATION FAILED:")
        for e in errors:
            print(f"  ERROR: {e}")
        return False
    else:
        print("All static validations PASSED!")
        return True


if __name__ == '__main__':
    success = validate_migration()
    sys.exit(0 if success else 1)
