"""
core/db_consumer.py — The sole DB writer in the system.

All events flow through db_consumer via the EventBus.  No other module may
call Database write helpers directly — this is the single-writer rule that
eliminates cross-thread contention and makes the audit log complete.

Subscribes to:
    ZoneEvent              → zones table
    ZoneTouchEvent         → events (audit log)
    SignalGeneratedEvent   → signals table + events
    RiskEvaluatedEvent     → risk_decisions table + events
    TradeExecutedEvent     → trades table + events
"""

import json
import logging
from typing import Any, Dict

from core.database import Database
from core.event_bus import EventBus
from core.events import (
    Event,
    ZoneEvent,
    ZoneTouchEvent,
    SignalGeneratedEvent,
    RiskEvaluatedEvent,
    TradeExecutedEvent,
    TradeClosedEvent,
)

logger = logging.getLogger(__name__)


def _to_json(event: Event) -> str:
    """
    Serialise a Pydantic event model to a JSON string safe for SQLite storage.
    datetime objects are converted to ISO strings via model_dump().
    """
    try:
        return json.dumps(event.model_dump(mode="json"), default=str)
    except Exception:
        return "{}"


class DBConsumer:
    """
    Subscribes to every event type and persists them to the database.

    This class is the ONLY place that calls Database write helpers.
    Each handler is wrapped in try/except so a persistence failure never
    crashes the publishing agent.
    """

    def __init__(self, db: Database, bus: EventBus) -> None:
        """
        Initialise and wire up all subscriptions.

        Args:
            db:  The shared Database instance.
            bus: The shared EventBus instance.
        """
        self._db = db
        self._bus = bus
        # Tracks the most recent signal DB id per symbol for risk_decision linkage
        self._last_signal_id: Dict[str, int] = {}
        self._wire_subscriptions()
        logger.info("DBConsumer initialised and subscriptions wired.")

    def _wire_subscriptions(self) -> None:
        """Register all event handlers on the EventBus."""
        self._bus.subscribe(ZoneEvent, self._on_zone_event)
        self._bus.subscribe(ZoneTouchEvent, self._on_zone_touch)
        self._bus.subscribe(SignalGeneratedEvent, self._on_signal)
        self._bus.subscribe(RiskEvaluatedEvent, self._on_risk)
        self._bus.subscribe(TradeExecutedEvent, self._on_trade)
        self._bus.subscribe(TradeClosedEvent, self._on_trade_closed)

    # ------------------------------------------------------------------
    # ZoneEvent — new S/R zone discovered by sr_mapper
    # ------------------------------------------------------------------

    def _on_zone_event(self, event: ZoneEvent) -> None:
        """
        Persist a newly computed S/R zone to the zones table and log to events.
        The sr_mapper calls deactivate_zones_for_symbol() before publishing the
        fresh batch, so we just INSERT here.
        """
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
    # ZoneTouchEvent — price entered a zone
    # ------------------------------------------------------------------

    def _on_zone_touch(self, event: ZoneTouchEvent) -> None:
        """Log the zone touch to the events audit table."""
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
    # SignalGeneratedEvent — GPT-4o produced a signal
    # ------------------------------------------------------------------

    def _on_signal(self, event: SignalGeneratedEvent) -> None:
        """Persist signal data to the signals table and the audit log."""
        try:
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
            self._last_signal_id[event.symbol] = signal_id
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.info(
                "Signal persisted: %s %s entry=%.5f conf=%.1f",
                event.symbol, event.direction, event.entry, event.confidence,
            )
        except Exception:
            logger.exception("DBConsumer._on_signal failed for %s", event)

    # ------------------------------------------------------------------
    # RiskEvaluatedEvent — risk_agent verdict
    # ------------------------------------------------------------------

    def _on_risk(self, event: RiskEvaluatedEvent) -> None:
        """Persist the risk decision and audit log entry."""
        try:
            self._db.insert_risk_decision(
                signal_id=self._last_signal_id.get(event.symbol),
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
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.info(
                "Risk decision persisted: %s %s approved=%s reason='%s'",
                event.symbol, event.direction, event.approved, event.reason,
            )
        except Exception:
            logger.exception("DBConsumer._on_risk failed for %s", event)

    # ------------------------------------------------------------------
    # TradeExecutedEvent — executor outcome
    # ------------------------------------------------------------------

    def _on_trade(self, event: TradeExecutedEvent) -> None:
        """Persist the trade outcome and audit log entry."""
        try:
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
            )
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.info(
                "Trade persisted: %s %s vol=%.2f success=%s order_id=%s",
                event.symbol, event.direction, event.volume, event.success, event.order_id,
            )
        except Exception:
            logger.exception("DBConsumer._on_trade failed for %s", event)

    # ------------------------------------------------------------------
    # TradeClosedEvent — position closed in MT5 (TP/SL/manual)
    # ------------------------------------------------------------------

    def _on_trade_closed(self, event: TradeClosedEvent) -> None:
        """Update the trades row with close price and realized P&L."""
        try:
            self._db.update_trade_close(
                order_id=event.order_id,
                close_price=event.close_price,
                realized_pnl=event.realized_pnl,
            )
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.info(
                "Trade close persisted: order_id=%d %s %s pnl=%.2f",
                event.order_id, event.symbol, event.direction, event.realized_pnl or 0.0,
            )
        except Exception:
            logger.exception("DBConsumer._on_trade_closed failed for %s", event)
