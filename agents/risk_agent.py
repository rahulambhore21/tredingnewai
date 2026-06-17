"""
agents/risk_agent.py — Risk gating layer.

Subscribes to SignalGeneratedEvent.
Runs 5 independent risk checks; publishes RiskEvaluatedEvent with
approved=True/False and the computed lot size.

Checks (all must pass for approval):
    1. R:R >= MIN_RR (2.0)
    2. Open trades < MAX_OPEN_TRADES (1) — account-wide across all symbols
    3. Correlated pairs not both open — EURUSD and USDJPY are correlated
    4. Today's realised P&L > -DAILY_LOSS_LIMIT_USD (default -$30)
    5. Today's realised P&L < DAILY_PROFIT_TARGET_USD (default $100)

Lot sizing uses pip/point-value formula:
    sl_distance = abs(entry - sl)
    lot = RISK_PER_TRADE_USD / ((tick_value / tick_size) * sl_distance)
    lot = clamp(lot, volume_min, volume_max)
    lot = round_to_step(lot, volume_step)
"""

import logging
import math
from datetime import datetime, timezone
from typing import List, Set, Tuple

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import RiskEvaluatedEvent, SignalGeneratedEvent

logger = logging.getLogger(__name__)


class RiskAgent:
    """
    Risk management gate between analysis signals and order execution.
    Synchronous event handler — no dedicated thread.
    """

    def __init__(self, client: MT5Client, bus: EventBus, db: Database) -> None:
        self._client = client
        self._bus    = bus
        self._db     = db

        self._bus.subscribe(SignalGeneratedEvent, self._on_signal)
        logger.info("RiskAgent initialised and subscribed to SignalGeneratedEvent.")

    # ------------------------------------------------------------------
    # Event handler
    # ------------------------------------------------------------------

    def _on_signal(self, event: SignalGeneratedEvent) -> None:
        logger.info(
            "RiskAgent evaluating: %s %s entry=%.5f conf=%.1f",
            event.symbol, event.direction, event.entry, event.confidence,
        )
        try:
            self._evaluate(event)
        except Exception:
            logger.exception("RiskAgent._evaluate raised for %s — publishing rejection", event.symbol)
            self._publish_rejection(event, "Internal risk-agent error")

    # ------------------------------------------------------------------
    # Evaluation logic
    # ------------------------------------------------------------------

    def _evaluate(self, event: SignalGeneratedEvent) -> None:
        failures: List[str] = []

        # Check 1: R:R >= MIN_RR
        rr_ok = False
        try:
            rr_ok = self._check_rr(event.entry, event.stop_loss, event.take_profit)
            if not rr_ok:
                failures.append(f"R:R too low (min {config.MIN_RR})")
        except Exception:
            logger.exception("RiskAgent: R:R check failed")
            failures.append("R:R check error")

        # Check 2: Open trades < MAX_OPEN_TRADES
        max_trades_ok = False
        open_bases: Set[str] = set()
        try:
            positions_df = self._client.order.get_all_positions()
            open_count = len(positions_df) if positions_df is not None else 0

            if positions_df is not None and len(positions_df) > 0:
                for sym_col in ("symbol", "Symbol"):
                    if sym_col in positions_df.columns:
                        open_bases = {
                            s.replace(config.SYMBOL_SUFFIX, "").upper()
                            for s in positions_df[sym_col].tolist()
                        }
                        break

            max_trades_ok = open_count < config.MAX_OPEN_TRADES
            if not max_trades_ok:
                failures.append(
                    f"Too many open trades ({open_count} >= {config.MAX_OPEN_TRADES})"
                )
        except Exception:
            logger.exception("RiskAgent: open-trade count check failed")
            failures.append("Open-trade count check error")

        # Check 3: Correlated pairs not both open
        correlation_ok = True
        try:
            for pair in config.CORRELATED_PAIRS:
                if event.symbol.upper() in pair:
                    other = pair[0] if pair[1] == event.symbol.upper() else pair[1]
                    if other in open_bases:
                        correlation_ok = False
                        failures.append(
                            f"Correlated pair {event.symbol}/{other} already open"
                        )
                        break
        except Exception:
            logger.exception("RiskAgent: correlation check failed")
            failures.append("Correlation check error")

        # Check 4 & 5: Today's realised P&L within bounds
        daily_loss_ok = False
        try:
            today_pnl = self._get_realized_pnl()
            daily_loss_ok = (
                today_pnl > -config.DAILY_LOSS_LIMIT_USD
                and today_pnl < config.DAILY_PROFIT_TARGET_USD
            )
            if not daily_loss_ok:
                if today_pnl >= config.DAILY_PROFIT_TARGET_USD:
                    failures.append(
                        f"Daily profit target reached (${today_pnl:.2f} >= "
                        f"${config.DAILY_PROFIT_TARGET_USD:.2f})"
                    )
                else:
                    failures.append(
                        f"Daily loss limit hit (${today_pnl:.2f} <= "
                        f"-${config.DAILY_LOSS_LIMIT_USD:.2f})"
                    )
        except Exception:
            logger.exception("RiskAgent: daily P&L check failed")
            failures.append("Daily P&L check error")

        # Lot size computation — may itself veto the trade if the risk-correct
        # lot is below the broker minimum (clamping up to vol_min would over-risk).
        volume, lot_ok, lot_reason = self._compute_lot_size(
            event.symbol, event.entry, event.stop_loss
        )
        if not lot_ok:
            failures.append(lot_reason)

        # Verdict
        approved = (
            rr_ok and max_trades_ok and correlation_ok and daily_loss_ok and lot_ok
        )
        reason = "All checks passed" if approved else "; ".join(failures)

        self._bus.publish(
            RiskEvaluatedEvent(
                symbol=event.symbol,
                direction=event.direction,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                confidence=event.confidence,
                approved=approved,
                reason=reason,
                volume=volume if approved else 0.0,
                rr_ok=rr_ok,
                max_trades_ok=max_trades_ok,
                correlation_ok=correlation_ok,
                daily_loss_ok=daily_loss_ok,
                weekly_loss_ok=True,
            )
        )
        logger.info(
            "RiskAgent verdict for %s: approved=%s reason='%s' volume=%.2f",
            event.symbol, approved, reason, volume if approved else 0.0,
        )

    # ------------------------------------------------------------------
    # Individual check helpers
    # ------------------------------------------------------------------

    def _check_rr(self, entry: float, stop_loss: float, take_profit: float) -> bool:
        risk   = abs(entry - stop_loss)
        reward = abs(take_profit - entry)
        if risk == 0:
            return False
        rr = reward / risk
        logger.debug("RiskAgent R:R = %.3f (min %.1f)", rr, config.MIN_RR)
        return rr >= (config.MIN_RR - 1e-9)

    def _get_realized_pnl(self) -> float:
        """
        Return today's realised P&L (UTC).
        Primary source: MT5 deal history.
        Fallback: local DB trades table (bot-tracked trades only).
        Never silently returns 0 on API failure — falls back to DB so the
        daily limit check is never bypassed by a transient MT5 error.
        """
        today_start = datetime.now(tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Strip tzinfo only if the MT5 client requires naive datetimes
        today_start_naive = today_start.replace(tzinfo=None)
        try:
            now_naive = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            df = self._client.history.get_deals_as_dataframe(
                from_date=today_start_naive,
                to_date=now_naive,
            )
            if df is not None and len(df) > 0:
                if "entry" in df.columns and "profit" in df.columns:
                    closing = df[df["entry"].isin([1, 2])]
                    pnl = float(closing["profit"].sum())
                    logger.debug("RiskAgent daily P&L from MT5 history: %.2f", pnl)
                    return pnl
        except Exception:
            logger.warning(
                "RiskAgent: MT5 deal history unavailable — falling back to DB for today's P&L"
            )

        # Fallback: local DB (populated by TradeMonitor → db_consumer)
        pnl = self._db.get_today_realized_pnl()
        logger.debug("RiskAgent daily P&L from DB fallback: %.2f", pnl)
        return pnl

    def _compute_lot_size(
        self, symbol: str, entry: float, stop_loss: float
    ) -> Tuple[float, bool, str]:
        """
        Compute lot size using pip/point-value formula:
            sl_distance = abs(entry - sl)
            lot = RISK_PER_TRADE_USD / ((tick_value / tick_size) * sl_distance)
            lot = clamp(lot, volume_min, volume_max)
            lot = round_to_step(lot, volume_step)

        Returns ``(volume, lot_ok, reason)``:
            - If the risk-correct lot is >= the broker minimum, returns the
              step-rounded, clamped volume with ``lot_ok=True``.
            - If it is below the broker minimum, returns ``(0.0, False, reason)``
              so the caller can reject the trade. Clamping up to vol_min in that
              case would place a position risking MORE than RISK_PER_TRADE_USD.
        """
        sl_distance = abs(entry - stop_loss)
        if sl_distance == 0:
            logger.warning("RiskAgent: sl_distance is 0 for %s — using volume_min", symbol)
            sl_distance = 1e-5

        fallback = config.TICK_VALUE_FALLBACK.get(symbol, {"tick_value": 1.0, "tick_size": 0.0001})
        tick_value = fallback["tick_value"]
        tick_size  = fallback["tick_size"]
        vol_min    = 0.01
        vol_max    = 100.0
        vol_step   = 0.01

        try:
            info = self._client.market.get_symbol_info(config.resolve_symbol(symbol))
            if info:
                tick_value = float(info.get("trade_tick_value", tick_value) or tick_value)
                tick_size  = float(info.get("trade_tick_size",  tick_size)  or tick_size)
                vol_min    = float(info.get("volume_min",  vol_min)  or vol_min)
                vol_max    = float(info.get("volume_max",  vol_max)  or vol_max)
                vol_step   = float(info.get("volume_step", vol_step) or vol_step)
        except Exception:
            logger.warning(
                "RiskAgent: get_symbol_info failed for %s — using fallback tick values", symbol
            )

        if tick_size == 0:
            tick_size = fallback["tick_size"]

        value_per_unit = tick_value / tick_size  # account-currency loss per 1.0 price unit, per lot
        raw_lot = config.RISK_PER_TRADE_USD / (value_per_unit * sl_distance)

        # Option-1 guard: reject when the risk-correct lot is below the broker
        # minimum instead of silently clamping up to vol_min. Clamping would
        # place a position risking MORE than RISK_PER_TRADE_USD. Logged with a
        # distinct, greppable "SUB-MIN LOT REJECT" tag for per-symbol auditing.
        if raw_lot < vol_min:
            would_be_risk = value_per_unit * sl_distance * vol_min
            logger.warning(
                "SUB-MIN LOT REJECT %s: raw_lot=%.6f < vol_min=%.5f "
                "(sl_dist=%.5f tick_val=%.4f tick_sz=%.5f) — clamping to vol_min "
                "would risk $%.2f vs cap $%.2f",
                symbol, raw_lot, vol_min, sl_distance, tick_value, tick_size,
                would_be_risk, config.RISK_PER_TRADE_USD,
            )
            return 0.0, False, (
                f"Risk-correct lot {raw_lot:.6f} below broker min {vol_min:.5f} for "
                f"{symbol} (sl_dist={sl_distance:.5f}); clamping would risk "
                f"${would_be_risk:.2f} > cap ${config.RISK_PER_TRADE_USD:.2f}"
            )

        if vol_step > 0:
            volume = math.floor(raw_lot / vol_step) * vol_step
        else:
            volume = raw_lot

        volume = round(max(vol_min, min(volume, vol_max)), 5)

        logger.info(
            "Lot sizing: risk=$%.2f sl_dist=%.5f tick_val=%.4f tick_sz=%.5f → raw=%.8f lot=%.5f",
            config.RISK_PER_TRADE_USD, sl_distance, tick_value, tick_size, raw_lot, volume,
        )
        return volume, True, "ok"

    # ------------------------------------------------------------------
    # Helper: publish rejection
    # ------------------------------------------------------------------

    def _publish_rejection(self, event: SignalGeneratedEvent, reason: str) -> None:
        try:
            self._bus.publish(
                RiskEvaluatedEvent(
                    symbol=event.symbol,
                    direction=event.direction,
                    entry=event.entry,
                    stop_loss=event.stop_loss,
                    take_profit=event.take_profit,
                    confidence=event.confidence,
                    approved=False,
                    reason=reason,
                    volume=0.0,
                    weekly_loss_ok=True,
                )
            )
        except Exception:
            logger.exception("RiskAgent: failed to publish rejection event")
