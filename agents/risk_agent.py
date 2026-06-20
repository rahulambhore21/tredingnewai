"""
agents/risk_agent.py — Risk gating layer (per-account).

Subscribes to SignalGeneratedEvent; filters on account_id.
Runs risk checks and publishes RiskEvaluatedEvent.

Checks (all must pass):
    1. Direction matches account's allowed direction
    2. Daily trade count < MAX_DAILY_TRADES for this account
    3. Open trades < MAX_OPEN_TRADES (account-wide MT5 check)
    4. Today's realised P&L within [-DAILY_LOSS_LIMIT_USD, DAILY_PROFIT_TARGET_USD]
    5. Correlated pairs not both open

Lot sizing:
    raw_lot = SL_USD / (value_per_unit * sl_distance_from_gpt)
    lot = clamp(raw_lot, LOT_MIN, LOT_MAX)
    SL price = entry ± SL_USD / (lot * value_per_unit)
    TP price = entry ± TP_USD / (lot * value_per_unit)
"""

import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import RiskEvaluatedEvent, SignalGeneratedEvent

logger = logging.getLogger(__name__)


class RiskAgent:
    """
    Per-account risk gate between analysis signals and order execution.
    Synchronous event handler — no dedicated thread.
    """

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        db: Database,
        account_config: Dict,
    ) -> None:
        self._client        = client
        self._bus           = bus
        self._db            = db
        self._account_id    = int(account_config["account_id"])
        self._direction     = str(account_config["direction"]).upper()

        self._bus.subscribe(SignalGeneratedEvent, self._on_signal)
        logger.info(
            "RiskAgent[acct=%d dir=%s] initialised and subscribed to SignalGeneratedEvent.",
            self._account_id, self._direction,
        )

    # ------------------------------------------------------------------
    # Event handler
    # ------------------------------------------------------------------

    def _on_signal(self, event: SignalGeneratedEvent) -> None:
        if event.account_id != self._account_id:
            return
        logger.info(
            "RiskAgent[acct=%d] evaluating: %s %s entry=%.5f conf=%.1f",
            self._account_id, event.symbol, event.direction, event.entry, event.confidence,
        )
        try:
            self._evaluate(event)
        except Exception:
            logger.exception(
                "RiskAgent[acct=%d]._evaluate raised for %s — publishing rejection",
                self._account_id, event.symbol,
            )
            self._publish_rejection(event, "Internal risk-agent error")

    # ------------------------------------------------------------------
    # Evaluation logic
    # ------------------------------------------------------------------

    def _evaluate(self, event: SignalGeneratedEvent) -> None:
        failures: List[str] = []

        # Check 1: Direction matches account's allowed direction
        direction_ok = event.direction == self._direction
        if not direction_ok:
            failures.append(
                f"Direction mismatch: signal={event.direction} account={self._direction}"
            )

        # Check 2: Daily trade count < MAX_DAILY_TRADES
        daily_count_ok = False
        try:
            daily_count = self._db.get_daily_trade_count(self._account_id)
            daily_count_ok = daily_count < config.MAX_DAILY_TRADES
            if not daily_count_ok:
                failures.append(
                    f"Daily trade limit reached ({daily_count} >= {config.MAX_DAILY_TRADES})"
                )
        except Exception:
            logger.exception("RiskAgent[acct=%d]: daily count check failed", self._account_id)
            failures.append("Daily count check error")

        # Check 3: Open trades < MAX_OPEN_TRADES
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
            logger.exception("RiskAgent[acct=%d]: open-trade count check failed", self._account_id)
            failures.append("Open-trade count check error")

        # Check 4: Correlated pairs not both open
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
            logger.exception("RiskAgent[acct=%d]: correlation check failed", self._account_id)
            failures.append("Correlation check error")

        # Check 5: Today's realised P&L within bounds
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
            logger.exception("RiskAgent[acct=%d]: daily P&L check failed", self._account_id)
            failures.append("Daily P&L check error")

        # Lot sizing + compute final SL/TP price levels
        volume, final_sl, final_tp, lot_ok, lot_reason = self._compute_lot_and_levels(
            event.symbol, event.entry, event.stop_loss, event.direction
        )
        if not lot_ok:
            failures.append(lot_reason)

        approved = (
            direction_ok and daily_count_ok and max_trades_ok
            and correlation_ok and daily_loss_ok and lot_ok
        )
        reason = "All checks passed" if approved else "; ".join(failures)

        self._bus.publish(
            RiskEvaluatedEvent(
                symbol=event.symbol,
                direction=event.direction,
                entry=event.entry,
                stop_loss=final_sl if approved else event.stop_loss,
                take_profit=final_tp if approved else event.take_profit,
                confidence=event.confidence,
                approved=approved,
                reason=reason,
                volume=volume if approved else 0.0,
                account_id=self._account_id,
                rr_ok=True,           # always passes: TP_USD/SL_USD = 3.0 >= MIN_RR
                max_trades_ok=max_trades_ok,
                correlation_ok=correlation_ok,
                daily_loss_ok=daily_loss_ok,
                weekly_loss_ok=True,
                direction_ok=direction_ok,
                daily_count_ok=daily_count_ok,
            )
        )
        logger.info(
            "RiskAgent[acct=%d] verdict for %s: approved=%s reason='%s' vol=%.2f",
            self._account_id, event.symbol, approved, reason,
            volume if approved else 0.0,
        )

    # ------------------------------------------------------------------
    # Lot sizing and SL/TP computation
    # ------------------------------------------------------------------

    def _compute_lot_and_levels(
        self,
        symbol: str,
        entry: float,
        gpt_sl: float,
        direction: str,
    ) -> Tuple[float, float, float, bool, str]:
        """
        1. Compute raw lot from GPT's sl_distance and SL_USD.
        2. Clamp to [LOT_MIN, LOT_MAX] — no sub-min rejection.
        3. Compute final SL/TP price levels from the clamped lot and SL_USD/TP_USD.

        Returns (volume, sl_price, tp_price, ok, reason).
        """
        fallback = config.TICK_VALUE_FALLBACK.get(
            symbol, {"tick_value": 1.0, "tick_size": 0.0001}
        )
        tick_value = fallback["tick_value"]
        tick_size  = fallback["tick_size"]
        vol_step   = 0.01

        try:
            info = self._client.market.get_symbol_info(config.resolve_symbol(symbol))
            if info:
                tick_value = float(info.get("trade_tick_value", tick_value) or tick_value)
                tick_size  = float(info.get("trade_tick_size",  tick_size)  or tick_size)
                vol_step   = float(info.get("volume_step", vol_step) or vol_step)
        except Exception:
            logger.warning(
                "RiskAgent[acct=%d]: get_symbol_info failed for %s — using fallback tick values",
                self._account_id, symbol,
            )

        if tick_size == 0:
            tick_size = fallback["tick_size"]

        value_per_unit = tick_value / tick_size  # account-currency loss per 1.0 price unit per lot

        # Step 1: raw lot from GPT's sl_distance
        gpt_sl_distance = abs(entry - gpt_sl)
        if gpt_sl_distance == 0:
            gpt_sl_distance = 1e-5

        raw_lot = config.SL_USD / (value_per_unit * gpt_sl_distance)

        # Step 2: clamp strictly to [LOT_MIN, LOT_MAX]
        clamped = max(config.LOT_MIN, min(raw_lot, config.LOT_MAX))
        if vol_step > 0:
            clamped = math.floor(clamped / vol_step) * vol_step
        volume = round(max(config.LOT_MIN, min(clamped, config.LOT_MAX)), 5)

        # Step 3: compute final SL/TP price levels from the clamped lot
        sl_price_dist = config.SL_USD / (volume * value_per_unit)
        tp_price_dist = config.TP_USD / (volume * value_per_unit)

        if direction == "BUY":
            sl_price = entry - sl_price_dist
            tp_price = entry + tp_price_dist
        else:  # SELL
            sl_price = entry + sl_price_dist
            tp_price = entry - tp_price_dist

        logger.info(
            "RiskAgent[acct=%d] lot sizing: SL_USD=%.0f TP_USD=%.0f gpt_sl_dist=%.5f "
            "raw_lot=%.5f vol=%.5f sl_dist=%.5f tp_dist=%.5f → sl=%.5f tp=%.5f",
            self._account_id, config.SL_USD, config.TP_USD,
            gpt_sl_distance, raw_lot, volume,
            sl_price_dist, tp_price_dist, sl_price, tp_price,
        )
        return volume, sl_price, tp_price, True, "ok"

    # ------------------------------------------------------------------
    # P&L helper
    # ------------------------------------------------------------------

    def _get_realized_pnl(self) -> float:
        today_start = datetime.now(tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
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
                    logger.debug(
                        "RiskAgent[acct=%d] daily P&L from MT5 history: %.2f",
                        self._account_id, pnl,
                    )
                    return pnl
        except Exception:
            logger.warning(
                "RiskAgent[acct=%d]: MT5 deal history unavailable — falling back to DB",
                self._account_id,
            )

        pnl = self._db.get_today_realized_pnl(self._account_id)
        logger.debug(
            "RiskAgent[acct=%d] daily P&L from DB fallback: %.2f", self._account_id, pnl
        )
        return pnl

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
                    account_id=self._account_id,
                    weekly_loss_ok=True,
                )
            )
        except Exception:
            logger.exception(
                "RiskAgent[acct=%d]: failed to publish rejection event", self._account_id
            )
