"""
agents/trade_monitor.py — Post-trade lifecycle monitor.

Responsibilities:
1. Subscribe to TradeExecutedEvent to learn which positions the bot opened.
2. Poll open positions every 30s.
3. When a tracked position disappears from MT5 (closed by TP, SL, or manually),
   look up the exit deal in MT5 deal history and publish TradeClosedEvent.
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

from metatrader_client import MT5Client

from core.event_bus import EventBus
from core.events import TradeClosedEvent, TradeExecutedEvent

logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 30.0


class TradeMonitor:
    """
    Per-account watcher that fires TradeClosedEvent when bot-placed positions close.
    """

    def __init__(self, client: MT5Client, bus: EventBus, account_id: int = 0) -> None:
        self._client     = client
        self._bus        = bus
        self._account_id = account_id

        self._tracked: Dict[int, Dict] = {}
        self._lock = threading.Lock()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"TradeMonitor-{account_id}",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

        self._bus.subscribe(TradeExecutedEvent, self._on_trade_executed)

    def start(self) -> None:
        logger.info("TradeMonitor starting …")
        self._recover_open_positions()
        self._thread.start()

    def stop(self) -> None:
        logger.info("TradeMonitor stopping …")
        self._stop_event.set()
        self._thread.join(timeout=15)

    def restart(self) -> None:
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"TradeMonitor-{self._account_id}",
            daemon=True,
        )
        self._thread.start()
        logger.warning("TradeMonitor[acct=%d] thread restarted by watchdog.", self._account_id)

    # ------------------------------------------------------------------
    # Track newly placed positions
    # ------------------------------------------------------------------

    def _on_trade_executed(self, event: TradeExecutedEvent) -> None:
        if event.account_id != self._account_id:
            return
        if not event.success or not event.order_id or event.dry_run:
            return
        with self._lock:
            self._tracked[event.order_id] = {
                "symbol":      event.symbol,
                "direction":   event.direction,
                "volume":      event.volume,
                "entry_price": event.fill_price or event.entry,
            }
        logger.info(
            "TradeMonitor[acct=%d]: tracking position %d (%s %s)",
            self._account_id, event.order_id, event.symbol, event.direction,
        )

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            try:
                self._check_closed_positions()
            except Exception:
                logger.exception("TradeMonitor._check_closed_positions raised")
            self._stop_event.wait(timeout=POLL_INTERVAL_SEC)

    def _check_closed_positions(self) -> None:
        with self._lock:
            tracked_ids = set(self._tracked.keys())

        try:
            open_df = self._client.order.get_all_positions()
        except Exception:
            logger.exception("TradeMonitor: failed to fetch open positions")
            return

        open_ids: Set[int] = set()
        if open_df is not None and len(open_df) > 0:
            for id_col in ("id", "ticket"):
                if id_col in open_df.columns:
                    open_ids = {int(x) for x in open_df[id_col].tolist()}
                    break

        # Orphan detection: positions live in MT5 but not in internal tracking
        for orphan_id in open_ids - tracked_ids:
            logger.warning(
                "TradeMonitor[acct=%d]: orphaned position %d detected "
                "— not in internal tracking",
                self._account_id, orphan_id,
            )

        if not tracked_ids:
            return

        # Closed detection: tracked positions no longer open in MT5
        closed_ids = tracked_ids - open_ids
        for position_id in closed_ids:
            with self._lock:
                trade_info = self._tracked.pop(position_id, None)
            if trade_info:
                self._handle_closed(position_id, trade_info)

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    def _recover_open_positions(self) -> None:
        """
        On startup, query MT5 for currently open positions and add them to
        internal tracking so the bot can detect when they close.
        """
        try:
            open_df = self._client.order.get_all_positions()
        except Exception:
            logger.exception(
                "TradeMonitor[acct=%d]: failed to recover open positions on startup",
                self._account_id,
            )
            return

        if open_df is None or len(open_df) == 0:
            logger.info(
                "TradeMonitor[acct=%d]: no open positions to recover on startup",
                self._account_id,
            )
            return

        count = 0
        with self._lock:
            for _, row in open_df.iterrows():
                pos_id = None
                for id_col in ("id", "ticket"):
                    if id_col in row.index:
                        try:
                            pos_id = int(row[id_col])
                        except (TypeError, ValueError):
                            pass
                        break
                if pos_id is None or pos_id in self._tracked:
                    continue

                pos_type = row.get("type", 0)
                direction = "BUY" if pos_type in (0, "BUY", "buy") else "SELL"
                self._tracked[pos_id] = {
                    "symbol":      str(row.get("symbol", "UNKNOWN")),
                    "direction":   direction,
                    "volume":      float(row.get("volume", 0.0)),
                    "entry_price": float(row.get("price_open", 0.0)),
                }
                count += 1

        logger.info(
            "TradeMonitor[acct=%d]: recovered %d open positions from MT5 on startup",
            self._account_id, count,
        )

    # ------------------------------------------------------------------
    # Handle a closed position
    # ------------------------------------------------------------------

    def _handle_closed(self, position_id: int, trade_info: Dict) -> None:
        close_price, realized_pnl = self._get_close_deal(position_id)

        logger.info(
            "TradeMonitor: position %d closed — %s %s entry=%.5f close=%s pnl=%.2f",
            position_id,
            trade_info["symbol"],
            trade_info["direction"],
            trade_info["entry_price"],
            f"{close_price:.5f}" if close_price else "unknown",
            realized_pnl,
        )

        self._bus.publish(
            TradeClosedEvent(
                symbol=trade_info["symbol"],
                direction=trade_info["direction"],
                volume=trade_info["volume"],
                entry_price=trade_info["entry_price"],
                order_id=position_id,
                account_id=self._account_id,
                close_price=close_price,
                realized_pnl=realized_pnl,
            )
        )

    def _get_close_deal(self, position_id: int) -> tuple:
        """Look up the closing deal for position_id in MT5 deal history."""
        try:
            from_date = datetime.now() - timedelta(days=2)
            df = self._client.history.get_deals_as_dataframe(from_date=from_date)
            if df is None or len(df) == 0:
                return None, 0.0
            if "position_id" not in df.columns or "entry" not in df.columns:
                return None, 0.0
            exit_df = df[
                (df["position_id"] == position_id) & (df["entry"].isin([1, 2]))
            ]
            if len(exit_df) == 0:
                return None, 0.0
            row = exit_df.iloc[-1]
            close_price  = float(row["price"])  if "price"  in row.index else None
            realized_pnl = float(row["profit"]) if "profit" in row.index else 0.0
            return close_price, realized_pnl
        except Exception:
            logger.exception("TradeMonitor: failed to get close deal for position %d", position_id)
            return None, 0.0
