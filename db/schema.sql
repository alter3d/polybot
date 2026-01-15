-- PostgreSQL Schema for Trade Tracking
-- Polybot Trading Bot Database Schema
-- Version: 1.0.0
--
-- This schema supports:
-- - Multi-wallet trading with wallet identification
-- - Market condition tracking for redemption
-- - Complete trade lifecycle (open -> filled -> closed)
-- - Profit/Loss calculation with cost basis tracking
-- - Tax reporting with full timestamp history

-- ============================================================================
-- ENUMS
-- ============================================================================

-- Trade status enum for tracking order lifecycle
CREATE TYPE trade_status AS ENUM ('open', 'filled', 'partially_filled', 'cancelled', 'closed');

-- Trade side enum for YES/NO outcome tokens
CREATE TYPE trade_side AS ENUM ('YES', 'NO');

-- Order side enum for BUY/SELL operations
CREATE TYPE order_side AS ENUM ('BUY', 'SELL');

-- ============================================================================
-- TABLES
-- ============================================================================

-- Wallets table: Stores wallet information for multi-wallet support
CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address VARCHAR(42) NOT NULL UNIQUE,  -- Ethereum address (0x + 40 hex chars)
    name VARCHAR(100),                     -- Human-readable identifier
    signature_type SMALLINT NOT NULL DEFAULT 0,  -- 0=EOA, 1=Magic, 2=Browser
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Markets table: Stores market/condition information for trade association and redemption
CREATE TABLE markets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    condition_id VARCHAR(100) NOT NULL UNIQUE,  -- Polymarket condition ID
    question TEXT,                              -- Market question/title
    end_date TIMESTAMPTZ,                       -- Market end date
    resolved BOOLEAN NOT NULL DEFAULT false,
    winning_side VARCHAR(3),                    -- 'YES' or 'NO' or NULL
    resolution_price NUMERIC(10, 8),            -- Final resolution price (0-1)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trades table: Stores all trade information for P&L tracking and tax reporting
CREATE TABLE trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    market_id UUID NOT NULL REFERENCES markets(id),

    -- Order identification
    order_id VARCHAR(100),                      -- External order ID from CLOB
    token_id TEXT NOT NULL,                     -- CLOB token ID for the outcome

    -- Order details
    side trade_side NOT NULL,                   -- YES or NO token
    order_type order_side NOT NULL,             -- BUY or SELL

    -- Quantities
    quantity NUMERIC(20, 8) NOT NULL,           -- Original order quantity
    filled_quantity NUMERIC(20, 8) NOT NULL DEFAULT 0,

    -- Prices
    limit_price NUMERIC(10, 8) NOT NULL,        -- Order limit price (0-1)
    avg_fill_price NUMERIC(10, 8),              -- Average fill price
    exit_price NUMERIC(10, 8),                  -- Price when position closed

    -- P&L tracking
    cost_basis_usd NUMERIC(20, 8),              -- Total cost in USD
    proceeds_usd NUMERIC(20, 8),                -- Total proceeds in USD
    realized_pnl NUMERIC(20, 8),                -- Realized profit/loss

    -- Flags
    neg_risk BOOLEAN NOT NULL DEFAULT false,    -- Negative risk market flag
    status trade_status NOT NULL DEFAULT 'open',

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    filled_at TIMESTAMPTZ,                      -- When order was filled
    closed_at TIMESTAMPTZ,                      -- When position was closed
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Wallet lookups
CREATE INDEX idx_wallets_address ON wallets(address);

-- Market lookups
CREATE INDEX idx_markets_condition_id ON markets(condition_id);
CREATE INDEX idx_markets_resolved ON markets(resolved) WHERE resolved = false;

-- Trade queries
CREATE INDEX idx_trades_wallet_id ON trades(wallet_id);
CREATE INDEX idx_trades_market_id ON trades(market_id);
CREATE INDEX idx_trades_status ON trades(status);
CREATE INDEX idx_trades_wallet_status ON trades(wallet_id, status);
CREATE INDEX idx_trades_created_at ON trades(created_at);

-- Composite index for common queries
CREATE INDEX idx_trades_wallet_market ON trades(wallet_id, market_id);

-- ============================================================================
-- COMMENTS
-- ============================================================================

-- Table comments
COMMENT ON TABLE wallets IS 'Trading wallets with Ethereum addresses and configuration';
COMMENT ON TABLE markets IS 'Prediction markets with condition IDs and resolution status';
COMMENT ON TABLE trades IS 'Trade records for P&L tracking, tax reporting, and position management';

-- Column comments for wallets
COMMENT ON COLUMN wallets.address IS 'Ethereum address in 0x format (42 characters)';
COMMENT ON COLUMN wallets.signature_type IS 'Wallet signature type: 0=EOA, 1=Magic, 2=Browser';
COMMENT ON COLUMN wallets.is_active IS 'Whether this wallet is currently active for trading';

-- Column comments for markets
COMMENT ON COLUMN markets.condition_id IS 'Unique Polymarket condition ID for the market';
COMMENT ON COLUMN markets.winning_side IS 'Resolved outcome: YES, NO, or NULL if unresolved';
COMMENT ON COLUMN markets.resolution_price IS 'Final price at resolution (0.0 to 1.0)';

-- Column comments for trades
COMMENT ON COLUMN trades.token_id IS 'CLOB token ID (long hash) required for order placement';
COMMENT ON COLUMN trades.side IS 'Outcome token being traded: YES or NO';
COMMENT ON COLUMN trades.order_type IS 'Order direction: BUY or SELL';
COMMENT ON COLUMN trades.quantity IS 'Original order quantity in token units';
COMMENT ON COLUMN trades.filled_quantity IS 'Quantity filled so far (for partial fills)';
COMMENT ON COLUMN trades.limit_price IS 'Limit price for the order (0.01 to 0.99)';
COMMENT ON COLUMN trades.avg_fill_price IS 'Volume-weighted average fill price';
COMMENT ON COLUMN trades.exit_price IS 'Price when position was closed';
COMMENT ON COLUMN trades.cost_basis_usd IS 'Total cost of position in USD';
COMMENT ON COLUMN trades.proceeds_usd IS 'Total proceeds from closing position in USD';
COMMENT ON COLUMN trades.realized_pnl IS 'Realized profit or loss in USD';
COMMENT ON COLUMN trades.neg_risk IS 'Whether this is a negative risk market';
COMMENT ON COLUMN trades.status IS 'Current status: open, filled, partially_filled, cancelled, closed';
