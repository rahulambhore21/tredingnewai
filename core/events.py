"""
core/events.py — Pydantic event schemas for the inter-agent EventBus.

Every event carries a UTC timestamp and all data that downstream consumers
(db_consumer, analysis_agent, risk_agent, executor) require so they never
need to re-query the source.

All fields are Optional where a downstream stage might not fill them in
(e.g. TradeExecutedEvent carries an error_message when the order failed).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Base event
# ---------------------------------------------------------------------------

class Event(BaseModel):
    """
    Base class for all events on the bus.

    Every concrete event inherits event_type (set as a literal on the class)
    and timestamp (auto-filled to UTC now).
    """

    # Human-readable event name; subclasses override this.
    event_type: str = "base_event"

    # UTC wall-clock time the event was created (auto-set, not injected by caller).
    timestamp: datetime = Field(default_factory=_utcnow)

    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# Zone / S&R events
# ---------------------------------------------------------------------------

class ZoneEvent(Event):
    """
    Emitted by sr_mapper when a new S/R zone is detected.
    db_consumer persists this to the zones table.
    """

    event_type: str = "zone_event"

    symbol: str                      # e.g. "XAUUSD"
    timeframe: str                   # e.g. "H1"
    zone_type: str                   # "support" or "resistance"
    price_center: float              # midpoint of the zone
    price_upper: float               # upper boundary
    price_lower: float               # lower boundary
    strength: int                    # number of pivots that formed this zone
    is_active: bool = True           # False once price has closed through it


class ZoneTouchEvent(Event):
    """
    Emitted by price_watcher when the live price enters a stored S/R zone.
    Triggers the analysis agent to generate a signal.
    """

    event_type: str = "zone_touch_event"

    symbol: str                      # e.g. "EURUSD"
    zone_type: str                   # "support" or "resistance"
    price_center: float              # zone midpoint
    price_upper: float
    price_lower: float
    zone_strength: int               # how many pivots formed this zone

    # Live tick at the moment of the touch
    bid: float
    ask: float
    mid_price: float                 # (bid + ask) / 2

    # DB row id for cross-referencing
    zone_id: Optional[int] = None

    # Timeframe the touched zone was mapped on (e.g. "M5" or "M15") — lets the
    # analysis agent run its indicator/candle analysis on the matching timeframe
    timeframe: Optional[str] = None


# ---------------------------------------------------------------------------
# Signal event (analysis_agent → risk_agent)
# ---------------------------------------------------------------------------

class SignalGeneratedEvent(Event):
    """
    Emitted by analysis_agent after GPT-4o produces a tradeable signal.
    Carries the full indicator snapshot and AI reasoning for audit logging.
    """

    event_type: str = "signal_generated_event"

    symbol: str
    direction: str                   # "BUY" or "SELL"
    entry: float
    stop_loss: float
    take_profit: float
    confidence: float                # 0–100 as returned by GPT-4o
    reasoning: str                   # GPT-4o reasoning text (for audit log)

    # Zone that triggered this signal (for traceability)
    zone_type: str
    zone_center: float
    zone_id: Optional[int] = None

    # Indicator snapshot at signal time
    ema21: Optional[float] = None
    ema50: Optional[float] = None
    rsi14: Optional[float] = None
    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None

    # Live mid-price when the signal was generated
    mid_price: Optional[float] = None


# ---------------------------------------------------------------------------
# Risk event (risk_agent → executor)
# ---------------------------------------------------------------------------

class RiskEvaluatedEvent(Event):
    """
    Emitted by risk_agent after running all 6 risk checks.
    executor acts only when approved=True.
    """

    event_type: str = "risk_evaluated_event"

    # Pass-through fields from the originating SignalGeneratedEvent
    symbol: str
    direction: str
    entry: float
    stop_loss: float
    take_profit: float
    confidence: float

    # Risk decision
    approved: bool
    reason: str                      # human-readable pass/fail summary
    volume: float                    # lot size (0 if rejected)

    # Individual check results for audit logging
    rr_ok: bool = False
    max_trades_ok: bool = False
    correlation_ok: bool = False
    daily_loss_ok: bool = False
    weekly_loss_ok: bool = False


# ---------------------------------------------------------------------------
# Trade execution event (executor → db_consumer)
# ---------------------------------------------------------------------------

class TradeExecutedEvent(Event):
    """
    Emitted by executor after attempting to place an order.
    Captures the outcome whether the order succeeded or failed.
    """

    event_type: str = "trade_executed_event"

    symbol: str
    direction: str
    volume: float
    entry: float
    stop_loss: float
    take_profit: float

    # Outcome fields
    success: bool
    order_id: Optional[int] = None          # MT5 position / order ticket
    fill_price: Optional[float] = None      # actual execution price
    sl_tp_modified: Optional[bool] = None   # True if modify_position succeeded
    error_message: Optional[str] = None     # populated on failure
    dry_run: bool = False                   # True when EXECUTION_LIVE=False


# ---------------------------------------------------------------------------
# Trade closed event (trade_monitor → db_consumer)
# ---------------------------------------------------------------------------

class TradeClosedEvent(Event):
    """
    Emitted by trade_monitor when a bot-placed position disappears from MT5
    (closed by TP, SL, or manually). Carries the actual realised P&L from
    MT5 deal history.
    """

    event_type: str = "trade_closed_event"

    symbol: str
    direction: str
    volume: float
    entry_price: float
    order_id: int                            # MT5 position ticket
    close_price: Optional[float] = None     # exit price from deal history
    realized_pnl: Optional[float] = None   # actual P&L in account currency
