"""
core/database.py — SQLite persistence layer.

Rules:
- Only db_consumer writes to the DB.  Other agents use the read helpers here.
- A threading.Lock protects every write so concurrent threads are safe.
- check_same_thread=False lets the connection be shared across threads while
  the Lock serialises all writes manually.
- All tables are created with CREATE TABLE IF NOT EXISTS (idempotent startup).
"""

import logging
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import config

logger = logging.getLogger(__name__)


class Database:
    """
    Thin wrapper around a SQLite connection providing:
    - Schema initialisation
    - Write helpers (called ONLY by db_consumer)
    - Read helpers used by agents for risk checks, zone lookups, etc.
    """

    def __init__(self, db_path: str = config.DB_PATH) -> None:
        """
        Open (or create) the SQLite database and initialise all tables.

        Args:
            db_path: File-system path to the .db file, or ":memory:" for tests.
        """
        self._db_path = db_path
        # check_same_thread=False: we control thread safety via the write lock.
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row      # rows accessible by column name
        self._write_lock = threading.Lock()
        self._create_tables()
        logger.info("Database initialised at %s", db_path)

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        """Create all tables if they do not already exist (idempotent)."""
        ddl_statements = [
            # S/R zones detected by sr_mapper
            """
            CREATE TABLE IF NOT EXISTS zones (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                timeframe   TEXT    NOT NULL,
                zone_type   TEXT    NOT NULL,         -- 'support' or 'resistance'
                price_center REAL   NOT NULL,
                price_upper  REAL   NOT NULL,
                price_lower  REAL   NOT NULL,
                strength    INTEGER NOT NULL DEFAULT 1,
                is_active   INTEGER NOT NULL DEFAULT 1,  -- 1=active, 0=invalidated
                created_at  TEXT    NOT NULL,
                updated_at  TEXT    NOT NULL
            )
            """,

            # Generic audit log — one row per published event
            """
            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type  TEXT    NOT NULL,
                symbol      TEXT,
                payload     TEXT,                      -- JSON dump of event
                created_at  TEXT    NOT NULL
            )
            """,

            # Signals produced by analysis_agent
            """
            CREATE TABLE IF NOT EXISTS signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                entry       REAL    NOT NULL,
                stop_loss   REAL    NOT NULL,
                take_profit REAL    NOT NULL,
                confidence  REAL    NOT NULL,
                reasoning   TEXT,
                zone_id     INTEGER,
                ema21       REAL,
                ema50       REAL,
                rsi14       REAL,
                macd_line   REAL,
                macd_signal REAL,
                macd_hist   REAL,
                created_at  TEXT    NOT NULL
            )
            """,

            # Risk decisions from risk_agent
            """
            CREATE TABLE IF NOT EXISTS risk_decisions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id   INTEGER,
                symbol      TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                approved    INTEGER NOT NULL,         -- 1=approved, 0=rejected
                reason      TEXT,
                volume      REAL,
                rr_ok       INTEGER,
                max_trades_ok INTEGER,
                correlation_ok INTEGER,
                daily_loss_ok  INTEGER,
                weekly_loss_ok INTEGER,
                created_at  TEXT    NOT NULL
            )
            """,

            # Executed (or attempted) trades from executor
            """
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                direction   TEXT    NOT NULL,
                volume      REAL    NOT NULL,
                entry       REAL    NOT NULL,
                stop_loss   REAL    NOT NULL,
                take_profit REAL    NOT NULL,
                order_id    INTEGER,
                fill_price  REAL,
                success     INTEGER NOT NULL,         -- 1=success, 0=failure
                sl_tp_ok    INTEGER,
                error_msg   TEXT,
                dry_run     INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT    NOT NULL
            )
            """,
        ]

        with self._write_lock:
            cur = self._conn.cursor()
            for stmt in ddl_statements:
                cur.execute(stmt)
            self._conn.commit()
        self._migrate_tables()

    def _migrate_tables(self) -> None:
        """Add columns introduced after the initial schema (idempotent)."""
        new_columns = [
            ("trades", "close_price", "REAL"),
            ("trades", "close_time", "TEXT"),
            ("trades", "realized_pnl", "REAL"),
        ]
        with self._write_lock:
            cur = self._conn.cursor()
            for table, col, col_type in new_columns:
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                    self._conn.commit()
                    logger.info("Database: added column %s.%s", table, col)
                except sqlite3.OperationalError:
                    pass  # column already exists

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _now_iso() -> str:
        """Return the current UTC time as an ISO-8601 string."""
        return datetime.now(tz=timezone.utc).isoformat()

    def _execute_write(self, sql: str, params: Tuple = ()) -> int:
        """
        Execute a single INSERT/UPDATE/DELETE under the write lock.

        Returns:
            int: lastrowid of the executed statement.
        """
        with self._write_lock:
            cur = self._conn.cursor()
            cur.execute(sql, params)
            self._conn.commit()
            return cur.lastrowid

    # ------------------------------------------------------------------
    # Write helpers (called ONLY by db_consumer)
    # ------------------------------------------------------------------

    def insert_zone(
        self,
        symbol: str,
        timeframe: str,
        zone_type: str,
        price_center: float,
        price_upper: float,
        price_lower: float,
        strength: int,
        is_active: bool = True,
    ) -> int:
        """
        Insert a new S/R zone row.

        Returns:
            int: The new row's id.
        """
        now = self._now_iso()
        return self._execute_write(
            """
            INSERT INTO zones
                (symbol, timeframe, zone_type, price_center, price_upper, price_lower,
                 strength, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (symbol, timeframe, zone_type, price_center, price_upper, price_lower,
             strength, int(is_active), now, now),
        )

    def deactivate_zones_for_symbol(self, symbol: str, timeframe: str) -> None:
        """Mark all existing zones for *symbol* + *timeframe* as inactive."""
        self._execute_write(
            "UPDATE zones SET is_active=0, updated_at=? WHERE symbol=? AND timeframe=?",
            (self._now_iso(), symbol, timeframe),
        )

    def deactivate_zones_before(
        self, symbol: str, timeframe: str, cutoff_iso: str
    ) -> None:
        """
        Deactivate only zones for *symbol* + *timeframe* whose created_at is
        strictly before *cutoff_iso*.  Used by sr_mapper so that the new zones
        (inserted during the current scan) are never deactivated — eliminating
        the gap window where no active zones exist.
        """
        self._execute_write(
            """
            UPDATE zones SET is_active=0, updated_at=?
            WHERE symbol=? AND timeframe=? AND created_at < ?
            """,
            (self._now_iso(), symbol, timeframe, cutoff_iso),
        )

    def insert_event_log(self, event_type: str, symbol: Optional[str], payload: str) -> int:
        """Append a row to the generic events audit table."""
        return self._execute_write(
            "INSERT INTO events (event_type, symbol, payload, created_at) VALUES (?, ?, ?, ?)",
            (event_type, symbol, payload, self._now_iso()),
        )

    def insert_signal(
        self,
        symbol: str,
        direction: str,
        entry: float,
        stop_loss: float,
        take_profit: float,
        confidence: float,
        reasoning: str,
        zone_id: Optional[int],
        ema21: Optional[float],
        ema50: Optional[float],
        rsi14: Optional[float],
        macd_line: Optional[float],
        macd_signal: Optional[float],
        macd_hist: Optional[float],
    ) -> int:
        """Insert a signal row. Returns the new row id."""
        return self._execute_write(
            """
            INSERT INTO signals
                (symbol, direction, entry, stop_loss, take_profit, confidence,
                 reasoning, zone_id, ema21, ema50, rsi14, macd_line, macd_signal,
                 macd_hist, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (symbol, direction, entry, stop_loss, take_profit, confidence,
             reasoning, zone_id, ema21, ema50, rsi14, macd_line, macd_signal,
             macd_hist, self._now_iso()),
        )

    def insert_risk_decision(
        self,
        signal_id: Optional[int],
        symbol: str,
        direction: str,
        approved: bool,
        reason: str,
        volume: float,
        rr_ok: bool,
        max_trades_ok: bool,
        correlation_ok: bool,
        daily_loss_ok: bool,
        weekly_loss_ok: bool,
    ) -> int:
        """Insert a risk_decisions row. Returns the new row id."""
        return self._execute_write(
            """
            INSERT INTO risk_decisions
                (signal_id, symbol, direction, approved, reason, volume,
                 rr_ok, max_trades_ok, correlation_ok, daily_loss_ok, weekly_loss_ok,
                 created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (signal_id, symbol, direction, int(approved), reason, volume,
             int(rr_ok), int(max_trades_ok), int(correlation_ok),
             int(daily_loss_ok), int(weekly_loss_ok), self._now_iso()),
        )

    def update_trade_close(
        self,
        order_id: int,
        close_price: Optional[float],
        realized_pnl: Optional[float],
    ) -> None:
        """
        Record the close price and realized P&L for a position that was closed
        in MT5 (by TP, SL, or manually).  Called by db_consumer on TradeClosedEvent.
        """
        self._execute_write(
            "UPDATE trades SET close_price=?, close_time=?, realized_pnl=? WHERE order_id=?",
            (close_price, self._now_iso(), realized_pnl, order_id),
        )

    def insert_trade(
        self,
        symbol: str,
        direction: str,
        volume: float,
        entry: float,
        stop_loss: float,
        take_profit: float,
        order_id: Optional[int],
        fill_price: Optional[float],
        success: bool,
        sl_tp_ok: Optional[bool],
        error_msg: Optional[str],
        dry_run: bool,
    ) -> int:
        """Insert a trades row. Returns the new row id."""
        return self._execute_write(
            """
            INSERT INTO trades
                (symbol, direction, volume, entry, stop_loss, take_profit,
                 order_id, fill_price, success, sl_tp_ok, error_msg, dry_run, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (symbol, direction, volume, entry, stop_loss, take_profit,
             order_id, fill_price, int(success),
             int(sl_tp_ok) if sl_tp_ok is not None else None,
             error_msg, int(dry_run), self._now_iso()),
        )

    # ------------------------------------------------------------------
    # Read helpers (used by agents for risk checks, zone lookups, etc.)
    # ------------------------------------------------------------------

    def get_active_zones(self, symbol: str) -> List[sqlite3.Row]:
        """
        Return all active S/R zones for *symbol* across all timeframes,
        sorted by strength descending (strongest zones first).
        """
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT * FROM zones
            WHERE symbol=? AND is_active=1
            ORDER BY strength DESC
            """,
            (symbol,),
        )
        return cur.fetchall()

    def get_last_zone_touch(self, symbol: str, zone_id: int) -> Optional[str]:
        """
        Return the ISO timestamp of the most recent ZoneTouchEvent logged for
        the given symbol+zone_id, or None if never touched.
        Uses json_extract instead of LIKE so the query is robust to whitespace
        differences in JSON serialisation.
        """
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT created_at FROM events
            WHERE event_type='zone_touch_event'
              AND symbol=?
              AND json_extract(payload, '$.zone_id') = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (symbol, zone_id),
        )
        row = cur.fetchone()
        return row["created_at"] if row else None

    def get_today_realized_pnl(self) -> float:
        """
        Return the sum of realised P&L (in account currency, from MT5) for
        positions closed today (UTC).  Uses the realized_pnl column populated
        by trade_monitor / db_consumer when a position closes.
        """
        today = datetime.now(tz=timezone.utc).date().isoformat()
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0.0)
            FROM trades
            WHERE dry_run=0
              AND close_time IS NOT NULL
              AND realized_pnl IS NOT NULL
              AND DATE(close_time) = ?
            """,
            (today,),
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    def get_week_realized_pnl(self) -> float:
        """
        Return the sum of realised P&L (in account currency, from MT5) for
        positions closed this ISO week (Monday–UTC).  Uses the realized_pnl
        column populated by trade_monitor / db_consumer when a position closes.
        """
        now = datetime.now(tz=timezone.utc)
        week_start = (now - timedelta(days=now.weekday())).date().isoformat()
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0.0)
            FROM trades
            WHERE dry_run=0
              AND close_time IS NOT NULL
              AND realized_pnl IS NOT NULL
              AND DATE(close_time) >= ?
            """,
            (week_start,),
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    def close(self) -> None:
        """Close the underlying SQLite connection cleanly."""
        try:
            self._conn.close()
            logger.info("Database connection closed.")
        except Exception:
            logger.exception("Error closing database connection.")
