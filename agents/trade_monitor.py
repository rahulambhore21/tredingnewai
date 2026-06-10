"""
agents/trade_monitor.py — Post-trade lifecycle monitor.

Responsibilities:
1. Subscribe to TradeExecutedEvent to learn which positions the bot opened.
2. Poll open positions every POLL_INTERVAL_SEC seconds.
3. When a tracked position disappears from MT5 (closed by TP, SL, or manually),
   look up the exit deal in MT5 deal history and publish TradeClosedEvent.
   db_consumer persists the close price and realized P&L to the trades table.

Thread model:
    One daemon thread runs _run_loop().
    _tracked dict is guarded by a threading.Lock since TradeExecutedEvent
    handlers run on the PriceWatcher thread while the monitor loop runs on
    its own thread.
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

from metatrader_client import MT5Client

import config
from core.event_bus import EventBus
from core.events import (
    BreakevenMovedEvent,
    TradeClosedEvent,
    TradeExecutedEvent,
    TrailingUpdatedEvent,
)
from core.signal_tracker import SignalTracker

logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 30.0


class TradeMonitor:
    """
    Watches bot-placed positions and fires TradeClosedEvent when they close.

    Injected dependencies:
        client: Shared MT5Client for position and history queries.
        bus:    Shared EventBus for subscribing and publishing.
    """

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        signal_tracker: Optional[SignalTracker] = None,
    ) -> None:
        self._client  = client
        self._bus     = bus
        self._tracker = signal_tracker

        # position_id → {symbol, direction, volume, entry_price, stop_loss, take_profit}
        self._tracked: Dict[int, Dict] = {}
        # position_id → {breakeven_done: bool, current_sl: float}
        self._sl_state: Dict[int, Dict] = {}
        self._lock = threading.Lock()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="TradeMonitor",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

        self._bus.subscribe(TradeExecutedEvent, self._on_trade_executed)

    def start(self) -> None:
        logger.info("TradeMonitor starting …")
        self._thread.start()

    def stop(self) -> None:
        logger.info("TradeMonitor stopping …")
        self._stop_event.set()
        self._thread.join(timeout=15)

    def restart(self) -> None:
        """Restart a dead thread (called by the main watchdog)."""
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="TradeMonitor", daemon=True
        )
        self._thread.start()
        logger.warning("TradeMonitor thread restarted by watchdog.")

    # ------------------------------------------------------------------
    # Track newly placed positions
    # ------------------------------------------------------------------

    def _on_trade_executed(self, event: TradeExecutedEvent) -> None:
        if not event.success or not event.order_id or event.dry_run:
            return
        with self._lock:
            self._tracked[event.order_id] = {
                "symbol":      event.symbol,
                "direction":   event.direction,
                "volume":      event.volume,
                # Use the actual broker fill price; fall back to signal entry on dry-run
                "entry_price": event.fill_price or event.entry,
                "stop_loss":   event.stop_loss,
                "take_profit": event.take_profit,
            }
            self._sl_state[event.order_id] = {
                "breakeven_done": False,
                "current_sl":     event.stop_loss,
            }
        logger.info(
            "TradeMonitor: tracking position %d (%s %s)",
            event.order_id, event.symbol, event.direction,
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
            try:
                self._manage_stops()
            except Exception:
                logger.exception("TradeMonitor._manage_stops raised")
            self._stop_event.wait(timeout=POLL_INTERVAL_SEC)

    def _check_closed_positions(self) -> None:
        with self._lock:
            if not self._tracked:
                return
            tracked_ids = set(self._tracked.keys())

        # Fetch currently open position IDs from MT5
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

        closed_ids = tracked_ids - open_ids
        for position_id in closed_ids:
            with self._lock:
                trade_info = self._tracked.pop(position_id, None)
                self._sl_state.pop(position_id, None)
            if trade_info:
                self._handle_closed(position_id, trade_info)

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

        if self._tracker is not None:
            self._tracker.record_result(position_id, won=realized_pnl > 0)

        self._bus.publish(
            TradeClosedEvent(
                symbol=trade_info["symbol"],
                direction=trade_info["direction"],
                volume=trade_info["volume"],
                entry_price=trade_info["entry_price"],
                order_id=position_id,
                close_price=close_price,
                realized_pnl=realized_pnl,
            )
        )

    # ------------------------------------------------------------------
    # Trailing / breakeven stop management
    # ------------------------------------------------------------------

    def _manage_stops(self) -> None:
        """
        For each tracked open position, move the SL to breakeven once price
        reaches BREAKEVEN_TRIGGER_PCT of the TP distance, then trail the SL
        once price reaches TRAIL_TRIGGER_PCT.  Only ever moves SL in the
        profitable direction — never backward.
        """
        with self._lock:
            snapshot = list(self._tracked.items())

        for position_id, info in snapshot:
            try:
                symbol = config.resolve_symbol(info["symbol"])
                price_data = self._client.market.get_symbol_price(symbol)
                if not price_data:
                    continue

                direction    = info["direction"]
                entry        = info["entry_price"]
                original_sl  = info["stop_loss"]
                tp           = info["take_profit"]

                with self._lock:
                    state = self._sl_state.get(position_id)
                if state is None:
                    continue

                current_sl = state["current_sl"]

                if direction == "BUY":
                    current_price = float(price_data.get("bid", 0))
                    tp_distance   = tp - entry
                    risk_distance = entry - original_sl
                    if tp_distance <= 0:
                        continue
                    progress = (current_price - entry) / tp_distance

                    new_sl    = None
                    move_type = None
                    if not state["breakeven_done"] and progress >= config.BREAKEVEN_TRIGGER_PCT:
                        new_sl    = entry
                        move_type = "breakeven"
                    elif progress >= config.TRAIL_TRIGGER_PCT:
                        trail_buffer = config.TRAIL_DISTANCE_RATIO * risk_distance
                        candidate    = current_price - trail_buffer
                        if candidate > current_sl:
                            new_sl    = candidate
                            move_type = "trailing"

                    if new_sl is not None and new_sl > current_sl:
                        old_sl = current_sl
                        if self._modify_sl(position_id, info, new_sl, current_price):
                            with self._lock:
                                if position_id in self._sl_state:
                                    self._sl_state[position_id]["current_sl"]     = new_sl
                                    self._sl_state[position_id]["breakeven_done"] = True
                            self._emit_sl_event(
                                move_type, position_id, info, old_sl, new_sl, current_price
                            )

                elif direction == "SELL":
                    current_price = float(price_data.get("ask", 0))
                    tp_distance   = entry - tp
                    risk_distance = original_sl - entry
                    if tp_distance <= 0:
                        continue
                    progress = (entry - current_price) / tp_distance

                    new_sl    = None
                    move_type = None
                    if not state["breakeven_done"] and progress >= config.BREAKEVEN_TRIGGER_PCT:
                        new_sl    = entry
                        move_type = "breakeven"
                    elif progress >= config.TRAIL_TRIGGER_PCT:
                        trail_buffer = config.TRAIL_DISTANCE_RATIO * risk_distance
                        candidate    = current_price + trail_buffer
                        if candidate < current_sl:
                            new_sl    = candidate
                            move_type = "trailing"

                    if new_sl is not None and new_sl < current_sl:
                        old_sl = current_sl
                        if self._modify_sl(position_id, info, new_sl, current_price):
                            with self._lock:
                                if position_id in self._sl_state:
                                    self._sl_state[position_id]["current_sl"]     = new_sl
                                    self._sl_state[position_id]["breakeven_done"] = True
                            self._emit_sl_event(
                                move_type, position_id, info, old_sl, new_sl, current_price
                            )

            except Exception:
                logger.exception(
                    "TradeMonitor._manage_stops: error processing position %d", position_id
                )

    def _emit_sl_event(
        self,
        move_type: Optional[str],
        position_id: int,
        info: Dict,
        old_sl: float,
        new_sl: float,
        current_price: float,
    ) -> None:
        """Publish a BreakevenMovedEvent or TrailingUpdatedEvent after a successful SL move."""
        try:
            if move_type == "breakeven":
                self._bus.publish(BreakevenMovedEvent(
                    symbol=info["symbol"],
                    direction=info["direction"],
                    position_id=position_id,
                    entry_price=info["entry_price"],
                    new_sl=new_sl,
                    current_price=current_price,
                ))
            elif move_type == "trailing":
                self._bus.publish(TrailingUpdatedEvent(
                    symbol=info["symbol"],
                    direction=info["direction"],
                    position_id=position_id,
                    old_sl=old_sl,
                    new_sl=new_sl,
                    current_price=current_price,
                ))
        except Exception:
            logger.exception(
                "TradeMonitor: failed to publish SL-move event for position %d", position_id
            )

    def _modify_sl(
        self,
        position_id: int,
        trade_info: Dict,
        new_sl: float,
        current_price: float,
    ) -> bool:
        """
        Call modify_position to update the SL; TP is left unchanged.

        Returns True on success.
        """
        try:
            result = self._client.order.modify_position(
                position_id,
                stop_loss=round(new_sl, 5),
                take_profit=trade_info["take_profit"],
            )
            if result and not result.get("error"):
                logger.info(
                    "TradeMonitor: SL moved to %.5f for position %d "
                    "(price=%.5f symbol=%s)",
                    new_sl, position_id, current_price, trade_info["symbol"],
                )
                return True
            msg = result.get("message", "unknown") if result else "None response"
            logger.warning(
                "TradeMonitor: failed to modify SL for position %d — %s",
                position_id, msg,
            )
            return False
        except Exception:
            logger.exception(
                "TradeMonitor: exception modifying SL for position %d", position_id
            )
            return False

    def _get_close_deal(self, position_id: int) -> tuple:
        """
        Look up the closing deal for *position_id* in MT5 deal history.
        Searches the past 2 days to catch end-of-session closes.

        Returns:
            (close_price, realized_pnl) — both 0.0 on failure.
        """
        try:
            from_date = datetime.now() - timedelta(days=2)
            df = self._client.history.get_deals_as_dataframe(from_date=from_date)
            if df is None or len(df) == 0:
                return None, 0.0
            if "position_id" not in df.columns or "entry" not in df.columns:
                return None, 0.0
            # entry=1 (OUT) is the closing leg; entry=2 (INOUT) for reversals
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
            logger.exception(
                "TradeMonitor: failed to get close deal for position %d", position_id
            )
            return None, 0.0
