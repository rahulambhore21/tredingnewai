"""
core/db_consumer.py — The sole DB writer in the system.

All events flow through db_consumer via the EventBus.  No other module may
call Database write helpers directly — this is the single-writer rule.

Subscribes to:
    ZoneEvent              → zones table
    ZonesRefreshedEvent    → deactivate stale zones
    ZoneTouchEvent         → events (audit log)
    SignalGeneratedEvent   → signals table + events
    RiskEvaluatedEvent     → risk_decisions table + events
    TradeExecutedEvent     → trades table + events
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Tuple

from core.database import Database
from core.event_bus import EventBus
from core.events import (
    Event,
    RiskEvaluatedEvent,
    SignalGeneratedEvent,
    TradeClosedEvent,
    TradeExecutedEvent,
    ZoneEvent,
    ZonesRefreshedEvent,
    ZoneTouchEvent,
)

logger = logging.getLogger(__name__)


def _to_json(event: Event) -> str:
    try:
        return json.dumps(event.model_dump(mode="json"), default=str)
    except Exception:
        return "{}"


class DBConsumer:
    """
    Subscribes to every event type and persists them to the database.
    The ONLY place that calls Database write helpers.
    """

    def __init__(self, db: Database, bus: EventBus) -> None:
        self._db  = db
        self._bus = bus
        # Keyed by (symbol, account_id) to avoid collisions across 4 accounts
        self._last_signal_id: Dict[Tuple[str, int], int] = {}
        self._last_validation_id: Dict[Tuple[str, int], int] = {}
        self._wire_subscriptions()
        logger.info("DBConsumer initialised and subscriptions wired.")

    def _wire_subscriptions(self) -> None:
        self._bus.subscribe(ZoneEvent,            self._on_zone_event)
        self._bus.subscribe(ZonesRefreshedEvent,  self._on_zones_refreshed)
        self._bus.subscribe(ZoneTouchEvent,       self._on_zone_touch)
        self._bus.subscribe(SignalGeneratedEvent, self._on_signal)
        self._bus.subscribe(RiskEvaluatedEvent,   self._on_risk)
        self._bus.subscribe(TradeExecutedEvent,   self._on_trade)
        self._bus.subscribe(TradeClosedEvent,     self._on_trade_closed)

    # ------------------------------------------------------------------
    # ZoneEvent
    # ------------------------------------------------------------------

    def _on_zone_event(self, event: ZoneEvent) -> None:
        try:
            self._db.insert_zone(
                symbol=event.symbol,
                timeframe=event.timeframe,
                zone_type=event.zone_type,
                price_center=event.price_center,
                price_upper=event.price_upper,
                price_lower=event.price_lower,
                strength=event.strength,
                is_active=event.is_active,
            )
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.debug(
                "Zone persisted: %s %s %.5f (%s)",
                event.symbol, event.zone_type, event.price_center, event.timeframe,
            )
        except Exception:
            logger.exception("DBConsumer._on_zone_event failed for %s", event)

    # ------------------------------------------------------------------
    # ZonesRefreshedEvent
    # ------------------------------------------------------------------

    def _on_zones_refreshed(self, event: ZonesRefreshedEvent) -> None:
        try:
            cutoff = event.zones_deactivated_before.isoformat()
            self._db.deactivate_zones_before(event.symbol, event.timeframe, cutoff)
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.debug(
                "Zones deactivated for %s %s before %s",
                event.symbol, event.timeframe, cutoff,
            )
        except Exception:
            logger.exception("DBConsumer._on_zones_refreshed failed for %s", event)

    # ------------------------------------------------------------------
    # ZoneTouchEvent
    # ------------------------------------------------------------------

    def _on_zone_touch(self, event: ZoneTouchEvent) -> None:
        try:
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.debug("ZoneTouchEvent logged for %s zone_id=%s", event.symbol, event.zone_id)
        except Exception:
            logger.exception("DBConsumer._on_zone_touch failed for %s", event)

    # ------------------------------------------------------------------
    # SignalGeneratedEvent
    # ------------------------------------------------------------------

    def _on_signal(self, event: SignalGeneratedEvent) -> None:
        try:
            acct_key = (event.symbol, event.account_id)
            signal_id = self._db.insert_signal(
                symbol=event.symbol,
                direction=event.direction,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                confidence=event.confidence,
                reasoning=event.reasoning,
                zone_id=event.zone_id,
                ema21=event.ema21,
                ema50=event.ema50,
                rsi14=event.rsi14,
                macd_line=event.macd_line,
                macd_signal=event.macd_signal,
                macd_hist=event.macd_hist,
            )
            self._last_signal_id[acct_key] = signal_id

            val_id = self._db.insert_validation_log(
                symbol=event.symbol,
                timeframe=event.timeframe,
                zone_id=event.zone_id,
                zone_type=event.zone_type,
                zone_strength=event.zone_strength,
                ai_decision=event.direction,
                confidence=event.confidence,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                signal_id=signal_id,
                account_id=event.account_id,
            )
            self._last_validation_id[acct_key] = val_id

            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.info(
                "Signal persisted: %s %s entry=%.5f conf=%.1f val_id=%d",
                event.symbol, event.direction, event.entry, event.confidence, val_id,
            )
        except Exception:
            logger.exception("DBConsumer._on_signal failed for %s", event)

    # ------------------------------------------------------------------
    # RiskEvaluatedEvent
    # ------------------------------------------------------------------

    def _on_risk(self, event: RiskEvaluatedEvent) -> None:
        try:
            acct_key = (event.symbol, event.account_id)
            self._db.insert_risk_decision(
                signal_id=self._last_signal_id.get(acct_key),
                symbol=event.symbol,
                direction=event.direction,
                approved=event.approved,
                reason=event.reason,
                volume=event.volume,
                rr_ok=event.rr_ok,
                max_trades_ok=event.max_trades_ok,
                correlation_ok=event.correlation_ok,
                daily_loss_ok=event.daily_loss_ok,
                weekly_loss_ok=event.weekly_loss_ok,
            )
            audit_event_type = (
                "risk_approved_event" if event.approved else "risk_rejected_event"
            )
            self._db.insert_event_log(
                event_type=audit_event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            val_id = self._last_validation_id.get(acct_key)
            if val_id is not None:
                self._db.update_validation_log_risk(val_id, event.approved, event.reason)
            logger.info(
                "Risk decision persisted: %s %s approved=%s reason='%s'",
                event.symbol, event.direction, event.approved, event.reason,
            )
        except Exception:
            logger.exception("DBConsumer._on_risk failed for %s", event)

    # ------------------------------------------------------------------
    # TradeExecutedEvent
    # ------------------------------------------------------------------

    def _on_trade(self, event: TradeExecutedEvent) -> None:
        try:
            acct_key = (event.symbol, event.account_id)
            self._db.insert_trade(
                symbol=event.symbol,
                direction=event.direction,
                volume=event.volume,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                order_id=event.order_id,
                fill_price=event.fill_price,
                success=event.success,
                sl_tp_ok=event.sl_tp_modified,
                error_msg=event.error_message,
                dry_run=event.dry_run,
                account_id=event.account_id,
            )
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            val_id = self._last_validation_id.get(acct_key)
            if val_id is not None and event.order_id:
                self._db.update_validation_log_trade(val_id, event.order_id, event.fill_price)
            logger.info(
                "Trade persisted: %s %s vol=%.2f success=%s order_id=%s",
                event.symbol, event.direction, event.volume, event.success, event.order_id,
            )
        except Exception:
            logger.exception("DBConsumer._on_trade failed for %s", event)

    # ------------------------------------------------------------------
    # TradeClosedEvent
    # ------------------------------------------------------------------

    def _on_trade_closed(self, event: TradeClosedEvent) -> None:
        try:
            self._db.update_trade_close(
                order_id=event.order_id,
                close_price=event.close_price,
                realized_pnl=event.realized_pnl,
            )
            # Look up validation_log row by order_id so that positions recovered
            # on startup (not in _last_validation_id) are also finalized correctly.
            val_id = self._db.get_validation_log_id_by_order(event.order_id)
            if val_id is None:
                # Fallback: use in-memory pointer for current-session trades whose
                # order_id was never written to validation_log (e.g. crash before update).
                val_id = self._last_validation_id.get((event.symbol, event.account_id))
            if val_id is not None:
                closed_at = datetime.now(tz=timezone.utc).isoformat()
                trade_result = "WIN" if event.realized_pnl > 0 else ("LOSS" if event.realized_pnl < 0 else "BREAKEVEN")
                self._db.update_validation_log_close(
                    val_id=val_id,
                    close_price=event.close_price,
                    realized_pnl=event.realized_pnl,
                    trade_result=trade_result,
                    closed_at=closed_at,
                )
            else:
                logger.warning(
                    "DBConsumer: no validation_log row found for order_id=%s — close not recorded",
                    event.order_id,
                )
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.info(
                "Trade close persisted: %s order_id=%s pnl=%.2f",
                event.symbol, event.order_id, event.realized_pnl,
            )
        except Exception:
            logger.exception("DBConsumer._on_trade_closed failed for %s", event)
