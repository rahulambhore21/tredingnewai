"""
core/events.py — Pydantic event schemas for the inter-agent EventBus.

Every event carries a UTC timestamp and all data that downstream consumers
require so they never need to re-query the source.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Base event
# ---------------------------------------------------------------------------

class Event(BaseModel):
    event_type: str = "base_event"
    timestamp: datetime = Field(default_factory=_utcnow)
    model_config = {"arbitrary_types_allowed": True}


# ---------------------------------------------------------------------------
# Zone / S&R events
# ---------------------------------------------------------------------------

class ZoneEvent(Event):
    event_type: str = "zone_event"
    symbol: str
    timeframe: str
    zone_type: str
    price_center: float
    price_upper: float
    price_lower: float
    strength: int
    is_active: bool = True


class ZonesRefreshedEvent(Event):
    event_type: str = "zones_refreshed_event"
    symbol: str
    timeframe: str
    refreshed_at: datetime
    zones_deactivated_before: datetime


class ZoneTouchEvent(Event):
    event_type: str = "zone_touch_event"
    symbol: str
    zone_type: str
    price_center: float
    price_upper: float
    price_lower: float
    zone_strength: int
    bid: float
    ask: float
    mid_price: float
    zone_id: Optional[int] = None
    timeframe: Optional[str] = None


# ---------------------------------------------------------------------------
# Signal event (analysis_agent → risk_agent)
# ---------------------------------------------------------------------------

class SignalGeneratedEvent(Event):
    event_type: str = "signal_generated_event"
    symbol: str
    direction: str
    entry: float
    stop_loss: float
    take_profit: float
    confidence: float
    reasoning: str
    zone_type: str
    zone_center: float
    account_id: int = 0
    zone_id: Optional[int] = None
    zone_strength: Optional[int] = None
    timeframe: Optional[str] = None
    ema21: Optional[float] = None
    ema50: Optional[float] = None
    rsi14: Optional[float] = None
    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    mid_price: Optional[float] = None


# ---------------------------------------------------------------------------
# Risk event (risk_agent → executor)
# ---------------------------------------------------------------------------

class RiskEvaluatedEvent(Event):
    event_type: str = "risk_evaluated_event"
    symbol: str
    direction: str
    entry: float
    stop_loss: float
    take_profit: float
    confidence: float
    approved: bool
    reason: str
    volume: float
    account_id: int = 0
    rr_ok: bool = False
    max_trades_ok: bool = False
    correlation_ok: bool = False
    daily_loss_ok: bool = False
    weekly_loss_ok: bool = True
    direction_ok: bool = True
    daily_count_ok: bool = True


# ---------------------------------------------------------------------------
# Trade execution event (executor → db_consumer)
# ---------------------------------------------------------------------------

class TradeExecutedEvent(Event):
    event_type: str = "trade_executed_event"
    symbol: str
    direction: str
    volume: float
    entry: float
    stop_loss: float
    take_profit: float
    success: bool
    account_id: int = 0
    order_id: Optional[int] = None
    fill_price: Optional[float] = None
    sl_tp_modified: Optional[bool] = None
    error_message: Optional[str] = None
    dry_run: bool = False


# ---------------------------------------------------------------------------
# Trade closed event (trade_monitor → db_consumer)
# ---------------------------------------------------------------------------

class TradeClosedEvent(Event):
    event_type: str = "trade_closed_event"
    symbol: str
    direction: str
    volume: float
    entry_price: float
    order_id: int
    account_id: int = 0
    close_price: Optional[float] = None
    realized_pnl: float = 0.0

