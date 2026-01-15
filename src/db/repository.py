"""Database repository for trade tracking.

This module provides database connection pooling and CRUD operations
for trades, wallets, and markets using psycopg3.
"""

import logging
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from src.db import OrderSide, TradeSide, TradeStatus
from src.db.models import Market, Trade, Wallet

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""

    pass


class TradeRepository:
    """Repository for database operations on trades, wallets, and markets.

    Provides CRUD operations with connection pooling for efficient
    database access. Uses psycopg3 with parameterized queries for
    security and performance.

    Attributes:
        _pool: Connection pool for managing database connections.
        _enabled: Whether database operations are enabled.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize the trade repository with database connection.

        Creates a connection pool for efficient database access.
        Gracefully disables database operations if connection fails.

        Args:
            database_url: PostgreSQL connection string.
        """
        self._enabled = False
        self._pool: Optional[ConnectionPool] = None

        if not database_url:
            logger.info("Database URL not configured - trade tracking disabled")
            return

        try:
            self._initialize_pool(database_url)
            self._enabled = True
            logger.info("TradeRepository initialized with connection pool")
        except Exception as e:
            logger.error("Failed to initialize database connection: %s", e)
            self._enabled = False

    def _initialize_pool(self, database_url: str) -> None:
        """Initialize the connection pool.

        Creates a connection pool with reasonable defaults for
        a trading application.

        Args:
            database_url: PostgreSQL connection string.

        Raises:
            DatabaseConnectionError: If pool creation fails.
        """
        logger.debug("Creating database connection pool")
        try:
            self._pool = ConnectionPool(
                database_url,
                min_size=1,
                max_size=5,
                timeout=10.0,
                kwargs={"row_factory": dict_row},
            )
            # Test the connection
            with self._pool.connection() as conn:
                conn.execute("SELECT 1")
            logger.debug("Database connection pool created successfully")
        except psycopg.Error as e:
            raise DatabaseConnectionError(f"Failed to create connection pool: {e}")

    @property
    def is_enabled(self) -> bool:
        """Check if database operations are enabled."""
        return self._enabled

    @contextmanager
    def _get_connection(self):
        """Get a connection from the pool.

        Yields:
            Database connection from the pool.

        Raises:
            DatabaseConnectionError: If pool is not initialized.
        """
        if not self._pool:
            raise DatabaseConnectionError("Connection pool not initialized")
        with self._pool.connection() as conn:
            yield conn

    def close(self) -> None:
        """Close the connection pool.

        Should be called during application shutdown to release
        database resources.
        """
        if self._pool:
            logger.debug("Closing database connection pool")
            self._pool.close()
            self._pool = None
            self._enabled = False

    # =========================================================================
    # Wallet Operations
    # =========================================================================

    def get_or_create_wallet(
        self,
        address: str,
        name: Optional[str] = None,
        signature_type: int = 0,
    ) -> Optional[Wallet]:
        """Get an existing wallet or create a new one.

        Uses upsert pattern to handle concurrent wallet creation.

        Args:
            address: Ethereum address (0x + 40 hex chars).
            name: Human-readable identifier for the wallet.
            signature_type: Wallet signature type (0=EOA, 1=Magic, 2=Browser).

        Returns:
            Wallet dataclass if successful, None if database disabled.
        """
        if not self._enabled:
            logger.debug("Database disabled, skipping wallet get_or_create")
            return None

        logger.debug("Getting or creating wallet: %s", address[:10] + "...")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Try to insert, on conflict return existing
                    cur.execute(
                        """
                        INSERT INTO wallets (address, name, signature_type)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (address) DO UPDATE
                        SET updated_at = NOW()
                        RETURNING id, address, name, signature_type, is_active,
                                  created_at, updated_at
                        """,
                        (address, name, signature_type),
                    )
                    row = cur.fetchone()
                    conn.commit()

                    if row:
                        return self._row_to_wallet(row)
                    return None
        except psycopg.Error as e:
            logger.error("Failed to get_or_create wallet: %s", e)
            return None

    def get_wallet_by_address(self, address: str) -> Optional[Wallet]:
        """Get a wallet by its Ethereum address.

        Args:
            address: Ethereum address to look up.

        Returns:
            Wallet dataclass if found, None otherwise.
        """
        if not self._enabled:
            return None

        logger.debug("Looking up wallet: %s", address[:10] + "...")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, address, name, signature_type, is_active,
                               created_at, updated_at
                        FROM wallets
                        WHERE address = $1
                        """,
                        (address,),
                    )
                    row = cur.fetchone()
                    if row:
                        return self._row_to_wallet(row)
                    return None
        except psycopg.Error as e:
            logger.error("Failed to get wallet by address: %s", e)
            return None

    def _row_to_wallet(self, row: dict) -> Wallet:
        """Convert a database row to a Wallet dataclass."""
        return Wallet(
            id=row["id"],
            address=row["address"],
            name=row["name"],
            signature_type=row["signature_type"],
            is_active=row["is_active"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # =========================================================================
    # Market Operations
    # =========================================================================

    def get_or_create_market(
        self,
        condition_id: str,
        question: Optional[str] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[Market]:
        """Get an existing market or create a new one.

        Uses upsert pattern to handle concurrent market creation.

        Args:
            condition_id: Polymarket condition ID.
            question: Market question or title.
            end_date: Market end date/time.

        Returns:
            Market dataclass if successful, None if database disabled.
        """
        if not self._enabled:
            logger.debug("Database disabled, skipping market get_or_create")
            return None

        logger.debug("Getting or creating market: %s", condition_id[:16] + "...")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO markets (condition_id, question, end_date)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (condition_id) DO UPDATE
                        SET updated_at = NOW()
                        RETURNING id, condition_id, question, end_date, resolved,
                                  winning_side, resolution_price, created_at, updated_at
                        """,
                        (condition_id, question, end_date),
                    )
                    row = cur.fetchone()
                    conn.commit()

                    if row:
                        return self._row_to_market(row)
                    return None
        except psycopg.Error as e:
            logger.error("Failed to get_or_create market: %s", e)
            return None

    def get_market_by_condition_id(self, condition_id: str) -> Optional[Market]:
        """Get a market by its condition ID.

        Args:
            condition_id: Polymarket condition ID to look up.

        Returns:
            Market dataclass if found, None otherwise.
        """
        if not self._enabled:
            return None

        logger.debug("Looking up market: %s", condition_id[:16] + "...")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, condition_id, question, end_date, resolved,
                               winning_side, resolution_price, created_at, updated_at
                        FROM markets
                        WHERE condition_id = $1
                        """,
                        (condition_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return self._row_to_market(row)
                    return None
        except psycopg.Error as e:
            logger.error("Failed to get market by condition_id: %s", e)
            return None

    def _row_to_market(self, row: dict) -> Market:
        """Convert a database row to a Market dataclass."""
        return Market(
            id=row["id"],
            condition_id=row["condition_id"],
            question=row["question"],
            end_date=row["end_date"],
            resolved=row["resolved"],
            winning_side=row["winning_side"],
            resolution_price=row["resolution_price"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # =========================================================================
    # Trade Operations
    # =========================================================================

    def create_trade(self, trade: Trade) -> Optional[Trade]:
        """Create a new trade record.

        Args:
            trade: Trade dataclass with order details.

        Returns:
            Trade with database-assigned ID and timestamps, None on failure.
        """
        if not self._enabled:
            logger.debug("Database disabled, skipping trade creation")
            return None

        logger.debug(
            "Creating trade: %s %s @ %s",
            trade.order_type.value,
            trade.side.value,
            trade.limit_price,
        )

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO trades (
                            wallet_id, market_id, order_id, token_id,
                            side, order_type, quantity, filled_quantity,
                            limit_price, avg_fill_price, neg_risk, status
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        RETURNING id, wallet_id, market_id, order_id, token_id,
                                  side, order_type, quantity, filled_quantity,
                                  limit_price, avg_fill_price, exit_price,
                                  cost_basis_usd, proceeds_usd, realized_pnl,
                                  neg_risk, status, created_at, filled_at,
                                  closed_at, updated_at
                        """,
                        (
                            trade.wallet_id,
                            trade.market_id,
                            trade.order_id,
                            trade.token_id,
                            trade.side.value,
                            trade.order_type.value,
                            trade.quantity,
                            trade.filled_quantity,
                            trade.limit_price,
                            trade.avg_fill_price,
                            trade.neg_risk,
                            trade.status.value,
                        ),
                    )
                    row = cur.fetchone()
                    conn.commit()

                    if row:
                        created_trade = self._row_to_trade(row)
                        logger.debug("Trade created with ID: %s", created_trade.id)
                        return created_trade
                    return None
        except psycopg.Error as e:
            logger.error("Failed to create trade: %s", e)
            return None

    def update_trade(
        self,
        trade_id: UUID,
        status: Optional[TradeStatus] = None,
        filled_quantity: Optional[Decimal] = None,
        avg_fill_price: Optional[Decimal] = None,
        filled_at: Optional[datetime] = None,
    ) -> Optional[Trade]:
        """Update an existing trade record.

        Only updates fields that are provided (not None).

        Args:
            trade_id: UUID of the trade to update.
            status: New trade status.
            filled_quantity: New filled quantity.
            avg_fill_price: New average fill price.
            filled_at: Timestamp when order was filled.

        Returns:
            Updated Trade dataclass, None on failure.
        """
        if not self._enabled:
            logger.debug("Database disabled, skipping trade update")
            return None

        logger.debug("Updating trade: %s", trade_id)

        # Build dynamic update query
        updates = ["updated_at = NOW()"]
        params = []
        param_idx = 1

        if status is not None:
            updates.append(f"status = ${param_idx}")
            params.append(status.value)
            param_idx += 1

        if filled_quantity is not None:
            updates.append(f"filled_quantity = ${param_idx}")
            params.append(filled_quantity)
            param_idx += 1

        if avg_fill_price is not None:
            updates.append(f"avg_fill_price = ${param_idx}")
            params.append(avg_fill_price)
            param_idx += 1

        if filled_at is not None:
            updates.append(f"filled_at = ${param_idx}")
            params.append(filled_at)
            param_idx += 1

        # Add trade_id as the last parameter
        params.append(trade_id)

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE trades
                        SET {", ".join(updates)}
                        WHERE id = ${param_idx}
                        RETURNING id, wallet_id, market_id, order_id, token_id,
                                  side, order_type, quantity, filled_quantity,
                                  limit_price, avg_fill_price, exit_price,
                                  cost_basis_usd, proceeds_usd, realized_pnl,
                                  neg_risk, status, created_at, filled_at,
                                  closed_at, updated_at
                        """,
                        tuple(params),
                    )
                    row = cur.fetchone()
                    conn.commit()

                    if row:
                        updated_trade = self._row_to_trade(row)
                        logger.debug(
                            "Trade updated: %s, status=%s",
                            trade_id,
                            updated_trade.status.value,
                        )
                        return updated_trade
                    return None
        except psycopg.Error as e:
            logger.error("Failed to update trade: %s", e)
            return None

    def get_trade_by_order_id(self, order_id: str) -> Optional[Trade]:
        """Get a trade by its external order ID.

        Args:
            order_id: External order ID from CLOB API.

        Returns:
            Trade dataclass if found, None otherwise.
        """
        if not self._enabled:
            return None

        logger.debug("Looking up trade by order_id: %s", order_id[:8] + "...")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, wallet_id, market_id, order_id, token_id,
                               side, order_type, quantity, filled_quantity,
                               limit_price, avg_fill_price, exit_price,
                               cost_basis_usd, proceeds_usd, realized_pnl,
                               neg_risk, status, created_at, filled_at,
                               closed_at, updated_at
                        FROM trades
                        WHERE order_id = $1
                        """,
                        (order_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return self._row_to_trade(row)
                    return None
        except psycopg.Error as e:
            logger.error("Failed to get trade by order_id: %s", e)
            return None

    def get_open_trades(self, wallet_id: Optional[UUID] = None) -> list[Trade]:
        """Get all open trades, optionally filtered by wallet.

        Args:
            wallet_id: Optional wallet ID to filter by.

        Returns:
            List of Trade dataclasses with open or partially_filled status.
        """
        if not self._enabled:
            return []

        logger.debug("Getting open trades")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    if wallet_id:
                        cur.execute(
                            """
                            SELECT id, wallet_id, market_id, order_id, token_id,
                                   side, order_type, quantity, filled_quantity,
                                   limit_price, avg_fill_price, exit_price,
                                   cost_basis_usd, proceeds_usd, realized_pnl,
                                   neg_risk, status, created_at, filled_at,
                                   closed_at, updated_at
                            FROM trades
                            WHERE wallet_id = $1
                              AND status IN ('open', 'partially_filled')
                            ORDER BY created_at DESC
                            """,
                            (wallet_id,),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, wallet_id, market_id, order_id, token_id,
                                   side, order_type, quantity, filled_quantity,
                                   limit_price, avg_fill_price, exit_price,
                                   cost_basis_usd, proceeds_usd, realized_pnl,
                                   neg_risk, status, created_at, filled_at,
                                   closed_at, updated_at
                            FROM trades
                            WHERE status IN ('open', 'partially_filled')
                            ORDER BY created_at DESC
                            """
                        )
                    rows = cur.fetchall()
                    trades = [self._row_to_trade(row) for row in rows]
                    logger.debug("Found %d open trades", len(trades))
                    return trades
        except psycopg.Error as e:
            logger.error("Failed to get open trades: %s", e)
            return []

    def _row_to_trade(self, row: dict) -> Trade:
        """Convert a database row to a Trade dataclass."""
        return Trade(
            id=row["id"],
            wallet_id=row["wallet_id"],
            market_id=row["market_id"],
            order_id=row["order_id"],
            token_id=row["token_id"],
            side=TradeSide(row["side"]),
            order_type=OrderSide(row["order_type"]),
            quantity=row["quantity"],
            filled_quantity=row["filled_quantity"],
            limit_price=row["limit_price"],
            avg_fill_price=row["avg_fill_price"],
            exit_price=row["exit_price"],
            cost_basis_usd=row["cost_basis_usd"],
            proceeds_usd=row["proceeds_usd"],
            realized_pnl=row["realized_pnl"],
            neg_risk=row["neg_risk"],
            status=TradeStatus(row["status"]),
            created_at=row["created_at"],
            filled_at=row["filled_at"],
            closed_at=row["closed_at"],
            updated_at=row["updated_at"],
        )


__all__ = ["TradeRepository", "DatabaseConnectionError"]
