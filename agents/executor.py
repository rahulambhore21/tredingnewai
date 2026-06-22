"""
agents/executor.py — Order execution agent.

Subscribes to RiskEvaluatedEvent.
Only acts when approved=True.

Live execution (EXECUTION_LIVE=True):
    1. place_market_order(type=direction, symbol=sym, volume=v)
       → returns {error, message, data}; position id in data.order
    2. modify_position(pos_id, stop_loss=sl, take_profit=tp)
    3. Publish TradeExecutedEvent

Dry-run (EXECUTION_LIVE=False):
    Log the would-be order and publish TradeExecutedEvent(dry_run=True).
"""

import logging
import time
from typing import Dict, Optional, Tuple

from metatrader_client import MT5Client

import config
from core.event_bus import EventBus
from core.events import RiskEvaluatedEvent, TradeExecutedEvent
from core.notifier import Notifier

logger = logging.getLogger(__name__)


class Executor:
    """
    Per-account order execution agent — the last stage of the pipeline.
    Synchronous event handler — no dedicated thread.
    """

    def __init__(self, client: MT5Client, bus: EventBus, account_config: Dict) -> None:
        self._client     = client
        self._bus        = bus
        self._account_id = int(account_config["account_id"])

        # Duplicate-signal guard: (symbol, direction, entry) → last execution timestamp
        self._last_signal: Optional[Tuple] = None
        self._last_signal_time: float = 0.0
        self._dedup_window_sec: float = 5.0

        self._bus.subscribe(RiskEvaluatedEvent, self._on_risk_evaluated)
        logger.info(
            "Executor[acct=%d] initialised — EXECUTION_LIVE=%s",
            self._account_id, config.EXECUTION_LIVE,
        )

    # ------------------------------------------------------------------
    # Event handler
    # ------------------------------------------------------------------

    def _on_risk_evaluated(self, event: RiskEvaluatedEvent) -> None:
        if event.account_id != self._account_id:
            return
        if not event.approved:
            logger.debug(
                "Executor[acct=%d]: signal rejected for %s — %s",
                self._account_id, event.symbol, event.reason,
            )
            return

        # Duplicate-signal guard: same (symbol, direction, entry) within 5 s → skip
        sig_key: Tuple = (event.symbol, event.direction, event.entry)
        now = time.time()
        if (
            self._last_signal == sig_key
            and (now - self._last_signal_time) < self._dedup_window_sec
        ):
            logger.warning(
                "Executor[acct=%d]: duplicate signal detected for %s %s entry=%.5f "
                "— skipping",
                self._account_id, event.symbol, event.direction, event.entry,
            )
            return
        self._last_signal = sig_key
        self._last_signal_time = now

        logger.info(
            "Executor[acct=%d] received approved signal: %s %s vol=%.2f "
            "entry=%.5f sl=%.5f tp=%.5f",
            self._account_id, event.symbol, event.direction, event.volume,
            event.entry, event.stop_loss, event.take_profit,
        )
        try:
            if config.EXECUTION_LIVE:
                self._execute_live(event)
            else:
                self._execute_dry_run(event)
        except Exception:
            logger.exception(
                "Executor[acct=%d] raised during order processing for %s",
                self._account_id, event.symbol,
            )
            self._publish_failure(event, "Unhandled executor exception")

    # ------------------------------------------------------------------
    # Dry-run path
    # ------------------------------------------------------------------

    def _execute_dry_run(self, event: RiskEvaluatedEvent) -> None:
        symbol = config.resolve_symbol(event.symbol)
        logger.info(
            "DRY RUN [acct=%d] — would place %s %s vol=%.2f sl=%.5f tp=%.5f on %s",
            self._account_id, event.direction, symbol, event.volume,
            event.stop_loss, event.take_profit, symbol,
        )
        self._bus.publish(
            TradeExecutedEvent(
                symbol=event.symbol,
                direction=event.direction,
                volume=event.volume,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                success=True,
                account_id=self._account_id,
                order_id=None,
                fill_price=event.entry,
                sl_tp_modified=True,
                error_message=None,
                dry_run=True,
            )
        )

    # ------------------------------------------------------------------
    # Live execution path
    # ------------------------------------------------------------------

    def _execute_live(self, event: RiskEvaluatedEvent) -> None:
        """
        Two-step execution:
        Step 1: place_market_order → get position_id from data.order
        Step 2: modify_position(position_id, stop_loss, take_profit)
        """
        symbol = config.resolve_symbol(event.symbol)

        # Step 1: Place market order
        logger.info(
            "Executor: placing %s %s vol=%.2f on %s",
            event.direction, symbol, event.volume, symbol,
        )
        order_result = self._client.order.place_market_order(
            type=event.direction,
            symbol=symbol,
            volume=event.volume,
        )

        if order_result is None:
            logger.error("Executor: place_market_order returned None for %s", symbol)
            self._publish_failure(event, "place_market_order returned None")
            return

        if order_result.get("error"):
            msg = order_result.get("message", "Unknown error")
            logger.error("Executor: order placement failed for %s — %s", symbol, msg)
            self._publish_failure(event, f"Order placement error: {msg}")
            return

        data = order_result.get("data")
        if data is None:
            logger.error("Executor: order response data is None for %s", symbol)
            self._publish_failure(event, "Order data is None after placement")
            return

        try:
            position_id = int(data.order)
            fill_price  = float(data.price)
        except (AttributeError, TypeError, ValueError) as exc:
            logger.error("Executor: could not extract position_id from data — %s", exc)
            self._publish_failure(event, f"Could not parse position id: {exc}")
            return

        if not position_id:  # 0 or None after conversion
            try:
                last_err = (
                    self._client.last_error()
                    if hasattr(self._client, "last_error")
                    else "N/A"
                )
            except Exception:
                last_err = "unavailable"
            logger.error(
                "Executor[acct=%d]: place_market_order returned position_id=%s for %s "
                "— MT5 last_error: %s",
                self._account_id, position_id, symbol, last_err,
            )
            self._publish_failure(
                event,
                f"place_market_order returned zero/None position_id; MT5 error: {last_err}",
            )
            return

        logger.info("Executor: order placed — position_id=%d fill=%.5f", position_id, fill_price)

        # Step 2: Attach SL/TP via modify_position
        sl_tp_ok = False
        sl_tp_error = None
        try:
            result = self._client.order.modify_position(
                position_id,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
            )
            if result and not result.get("error"):
                sl_tp_ok = True
                logger.info(
                    "Executor: SL/TP set — position %d sl=%.5f tp=%.5f",
                    position_id, event.stop_loss, event.take_profit,
                )
            else:
                sl_tp_error = result.get("message", "unknown") if result else "None response"
                logger.error(
                    "Executor: modify_position failed for position %d — %s",
                    position_id, sl_tp_error,
                )
        except Exception as exc:
            sl_tp_error = str(exc)
            logger.exception("Executor: modify_position raised for position %d", position_id)

        if not sl_tp_ok:
            logger.error(
                "NAKED POSITION: %s position %d has no SL/TP — manual intervention required",
                symbol, position_id,
            )
            try:
                Notifier().send(
                    f"NAKED POSITION: {event.symbol} position {position_id} opened "
                    f"but SL/TP attachment failed ({sl_tp_error}). "
                    f"Manual intervention required!"
                )
            except Exception:
                logger.exception(
                    "Executor: failed to send naked position notification for %d", position_id
                )

        self._bus.publish(
            TradeExecutedEvent(
                symbol=event.symbol,
                direction=event.direction,
                volume=event.volume,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                success=True,         # order was placed; sl_tp_modified tracks SL/TP separately
                account_id=self._account_id,
                order_id=position_id,
                fill_price=fill_price,
                sl_tp_modified=sl_tp_ok,
                error_message=sl_tp_error,
                dry_run=False,
            )
        )

    # ------------------------------------------------------------------
    # Helper: failure event
    # ------------------------------------------------------------------

    def _publish_failure(self, event: RiskEvaluatedEvent, reason: str) -> None:
        try:
            self._bus.publish(
                TradeExecutedEvent(
                    symbol=event.symbol,
                    direction=event.direction,
                    volume=event.volume,
                    entry=event.entry,
                    stop_loss=event.stop_loss,
                    take_profit=event.take_profit,
                    success=False,
                    account_id=self._account_id,
                    order_id=None,
                    fill_price=None,
                    sl_tp_modified=False,
                    error_message=reason,
                    dry_run=not config.EXECUTION_LIVE,
                )
            )
        except Exception:
            logger.exception(
                "Executor[acct=%d]: failed to publish failure event for %s",
                self._account_id, event.symbol,
            )
