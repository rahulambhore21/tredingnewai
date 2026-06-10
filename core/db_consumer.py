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
from core.notifier import Notifier
from core.events import (
    AnalysisStartedEvent,
    BreakevenMovedEvent,
    Event,
    RiskEvaluatedEvent,
    SignalGeneratedEvent,
    TradeClosedEvent,
    TradeExecutedEvent,
    TrailingUpdatedEvent,
    ZoneEvent,
    ZonesRefreshedEvent,
    ZoneTouchEvent,
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
        self._notifier = Notifier()
        # Tracks the most recent signal DB id per symbol for risk_decision linkage
        self._last_signal_id: Dict[str, int] = {}
        # Tracks the most recent validation_log id per symbol for lifecycle updates
        self._last_validation_id: Dict[str, int] = {}
        # Maps MT5 order_id → validation_log id for close-time updates
        self._validation_id_by_order: Dict[int, int] = {}
        self._wire_subscriptions()
        logger.info("DBConsumer initialised and subscriptions wired.")

    def _wire_subscriptions(self) -> None:
        """Register all event handlers on the EventBus."""
        self._bus.subscribe(ZoneEvent, self._on_zone_event)
        self._bus.subscribe(ZonesRefreshedEvent, self._on_zones_refreshed)
        self._bus.subscribe(ZoneTouchEvent, self._on_zone_touch)
        self._bus.subscribe(AnalysisStartedEvent, self._on_analysis_started)
        self._bus.subscribe(SignalGeneratedEvent, self._on_signal)
        self._bus.subscribe(RiskEvaluatedEvent, self._on_risk)
        self._bus.subscribe(TradeExecutedEvent, self._on_trade)
        self._bus.subscribe(TradeClosedEvent, self._on_trade_closed)
        self._bus.subscribe(BreakevenMovedEvent, self._on_sl_move)
        self._bus.subscribe(TrailingUpdatedEvent, self._on_sl_move)

    # ------------------------------------------------------------------
    # ZoneEvent — new S/R zone discovered by sr_mapper
    # ------------------------------------------------------------------

    def _on_zone_event(self, event: ZoneEvent) -> None:
        """Persist a newly computed S/R zone to the zones table and log to events."""
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
    # ZonesRefreshedEvent — sr_mapper finished publishing a fresh batch
    # ------------------------------------------------------------------

    def _on_zones_refreshed(self, event: ZonesRefreshedEvent) -> None:
        """Deactivate stale zones now that the fresh batch has been inserted."""
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
    # AnalysisStartedEvent — GPT call about to be made
    # ------------------------------------------------------------------

    def _on_analysis_started(self, event: AnalysisStartedEvent) -> None:
        """Log that analysis was started so zone-to-signal conversion can be measured."""
        try:
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.debug(
                "AnalysisStartedEvent logged for %s zone_id=%s tf=%s",
                event.symbol, event.zone_id, event.timeframe,
            )
        except Exception:
            logger.exception("DBConsumer._on_analysis_started failed for %s", event)

    # ------------------------------------------------------------------
    # SignalGeneratedEvent — GPT-4o produced a signal
    # ------------------------------------------------------------------

    def _on_signal(self, event: SignalGeneratedEvent) -> None:
        """Persist signal data to the signals table, validation_log, and the audit log."""
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

            # Open a validation_log row — risk/trade/close handlers update it
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
            )
            self._last_validation_id[event.symbol] = val_id

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
            # Split into granular event types for easier audit querying
            audit_event_type = (
                "risk_approved_event" if event.approved else "risk_rejected_event"
            )
            self._db.insert_event_log(
                event_type=audit_event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            # Update validation_log with risk verdict
            val_id = self._last_validation_id.get(event.symbol)
            if val_id is not None:
                self._db.update_validation_log_risk(
                    val_id, event.approved, event.reason
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
            # Link validation_log to MT5 order so close handler can update it
            val_id = self._last_validation_id.get(event.symbol)
            if val_id is not None and event.order_id:
                self._db.update_validation_log_trade(val_id, event.order_id, event.fill_price)
                self._validation_id_by_order[event.order_id] = val_id
            logger.info(
                "Trade persisted: %s %s vol=%.2f success=%s order_id=%s",
                event.symbol, event.direction, event.volume, event.success, event.order_id,
            )
            self._notifier.send(
                f"✅ Trade Executed: {event.symbol} {event.direction} | "
                f"Entry: {event.entry} | SL: {event.stop_loss} | "
                f"TP: {event.take_profit} | Lot: {event.volume}"
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
            # Finalize validation_log: determine WIN/LOSS/BREAKEVEN
            val_id = self._validation_id_by_order.pop(event.order_id, None)
            if val_id is not None:
                pnl = event.realized_pnl or 0.0
                if pnl > 0:
                    result = "WIN"
                elif pnl < 0:
                    result = "LOSS"
                else:
                    result = "BREAKEVEN"
                self._db.update_validation_log_close(
                    val_id,
                    close_price=event.close_price,
                    realized_pnl=event.realized_pnl,
                    trade_result=result,
                    closed_at=event.timestamp.isoformat(),
                )
            logger.info(
                "Trade close persisted: order_id=%d %s %s pnl=%.2f",
                event.order_id, event.symbol, event.direction, event.realized_pnl or 0.0,
            )
            pnl = event.realized_pnl or 0.0
            pnl_emoji = "✅" if pnl >= 0 else "❌"
            close_price_str = f"{event.close_price:.5f}" if event.close_price else "unknown"
            self._notifier.send(
                f"🔴 Trade Closed: {event.symbol} {event.direction}\n"
                f"📦 Lot: {event.volume} | Order: {event.order_id}\n"
                f"📥 Entry: {event.entry_price:.5f} → 📤 Close: {close_price_str}\n"
                f"{pnl_emoji} P&L: {pnl:+.2f} USD"
            )
        except Exception:
            logger.exception("DBConsumer._on_trade_closed failed for %s", event)

    # ------------------------------------------------------------------
    # SL move events (breakeven + trailing)
    # ------------------------------------------------------------------

    def _on_sl_move(self, event: Any) -> None:
        """Log breakeven and trailing stop moves to the audit table."""
        try:
            self._db.insert_event_log(
                event_type=event.event_type,
                symbol=event.symbol,
                payload=_to_json(event),
            )
            logger.debug("SL move logged: %s for %s pos=%s",
                         event.event_type, event.symbol,
                         getattr(event, "position_id", "?"))
        except Exception:
            logger.exception("DBConsumer._on_sl_move failed for %s", event)
