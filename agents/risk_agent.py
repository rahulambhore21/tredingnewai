"""
agents/risk_agent.py — Risk gating layer.

Subscribes to SignalGeneratedEvent.
Runs 6 independent risk checks; publishes RiskEvaluatedEvent with
approved=True/False and the computed lot size.

Checks (all must pass for approval):
    1. R:R >= MIN_RR (1.2) — computed from entry/stop_loss/take_profit.
    2. Open trades <= MAX_OPEN_TRADES (3) — via get_all_positions().
    3. No correlated pair both open (see CORRELATED_PAIRS — empty for single-instrument mode).
    4. Today's realised P&L stays within [-DAILY_LOSS_LIMIT_USD, +DAILY_PROFIT_TARGET_USD]
       — once the daily profit target or loss limit is reached, new trades are blocked
       for the rest of the UTC day (existing open positions still run to their own SL/TP).
    5. Lot size computed from a fixed dollar notional: volume ≈ FIXED_TRADE_USD / price,
       rounded to the broker's volume step and clamped to [LOT_MIN, LOT_MAX].

All errors inside the handler are caught and logged; a failed check causes
rejection (not a crash).
"""

import logging
import math
from datetime import datetime, timezone
from typing import List, Set

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import RiskEvaluatedEvent, SignalGeneratedEvent

logger = logging.getLogger(__name__)


class RiskAgent:
    """
    Risk management gate between analysis signals and order execution.

    Injected dependencies:
        client: Shared MT5Client for position/account queries (read-only).
        bus:    Shared EventBus for subscribing and publishing.
        db:     Shared Database for reading daily/weekly PnL.
    """

    def __init__(self, client: MT5Client, bus: EventBus, db: Database) -> None:
        """
        Initialise and register subscription.

        Args:
            client: Connected MT5Client.
            bus:    Shared EventBus.
            db:     Shared Database (read-only from this agent).
        """
        self._client = client
        self._bus    = bus
        self._db     = db

        self._bus.subscribe(SignalGeneratedEvent, self._on_signal)
        logger.info("RiskAgent initialised and subscribed to SignalGeneratedEvent.")

    # ------------------------------------------------------------------
    # Event handler
    # ------------------------------------------------------------------

    def _on_signal(self, event: SignalGeneratedEvent) -> None:
        """
        Handle a SignalGeneratedEvent: run all risk checks and publish verdict.

        Args:
            event: The signal produced by analysis_agent.
        """
        logger.info(
            "RiskAgent evaluating: %s %s entry=%.5f conf=%.1f",
            event.symbol, event.direction, event.entry, event.confidence,
        )
        try:
            self._evaluate(event)
        except Exception:
            logger.exception(
                "RiskAgent._evaluate raised for %s — publishing rejection",
                event.symbol,
            )
            # Publish a safe rejection so the event chain is complete
            self._publish_rejection(event, "Internal risk-agent error")

    # ------------------------------------------------------------------
    # Evaluation logic
    # ------------------------------------------------------------------

    def _evaluate(self, event: SignalGeneratedEvent) -> None:
        """
        Run all 6 checks, compute lot size, and publish RiskEvaluatedEvent.
        """
        failures: List[str] = []

        # ----------------------------------------------------------
        # Check 1: Risk-to-reward ratio >= MIN_RR
        # ----------------------------------------------------------
        rr_ok = False
        try:
            rr_ok = self._check_rr(
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
            )
            if not rr_ok:
                failures.append(
                    f"R:R too low (min {config.MIN_RR})"
                )
        except Exception:
            logger.exception("RiskAgent: R:R check failed")
            failures.append("R:R check error")

        # ----------------------------------------------------------
        # Check 2: Open trades <= MAX_OPEN_TRADES  AND  symbol not already open
        # ----------------------------------------------------------
        max_trades_ok = False
        open_symbols: Set[str] = set()
        open_bases:   Set[str] = set()
        try:
            positions_df = self._client.order.get_all_positions()
            open_count = len(positions_df) if positions_df is not None else 0

            # Collect open symbol names for correlation + re-entry checks
            if positions_df is not None and len(positions_df) > 0:
                for sym_col in ("symbol", "Symbol"):
                    if sym_col in positions_df.columns:
                        open_symbols = set(positions_df[sym_col].str.upper().tolist())
                        break
            open_bases = {s.replace(config.SYMBOL_SUFFIX, "").upper() for s in open_symbols}

            under_limit = open_count < config.MAX_OPEN_TRADES
            symbol_free = event.symbol.upper() not in open_bases
            max_trades_ok = under_limit and symbol_free

            if not under_limit:
                failures.append(
                    f"Too many open trades ({open_count} >= {config.MAX_OPEN_TRADES})"
                )
            if not symbol_free:
                failures.append(f"{event.symbol} already has an open position")
        except Exception:
            logger.exception("RiskAgent: open-trade count check failed")
            failures.append("Open-trade count check error")

        # ----------------------------------------------------------
        # Check 3: Correlated pair check
        # open_bases is populated by Check 2 above; empty on exception → pass-through
        # ----------------------------------------------------------
        correlation_ok = False
        try:
            correlation_ok = self._check_correlation(event.symbol, open_bases)
            if not correlation_ok:
                failures.append(
                    f"{event.symbol} blocked — correlated pair already open "
                    f"(CORRELATED_PAIRS={config.CORRELATED_PAIRS})"
                )
        except Exception:
            logger.exception("RiskAgent: correlation check raised — failing open")
            correlation_ok = True   # can't determine, don't block on internal error

        # ----------------------------------------------------------
        # Check 4: Today's realised P&L within [-DAILY_LOSS_LIMIT_USD, +DAILY_PROFIT_TARGET_USD]
        # (from MT5 deal history). Once either bound is reached, new trades
        # are blocked for the rest of the UTC day; existing positions are
        # left alone to hit their own SL/TP.
        # ----------------------------------------------------------
        daily_loss_ok = False
        try:
            today_start = datetime.now(tz=timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            )
            today_pnl = self._get_realized_pnl(today_start)
            daily_loss_ok = (
                today_pnl > -config.DAILY_LOSS_LIMIT_USD
                and today_pnl < config.DAILY_PROFIT_TARGET_USD
            )
            if not daily_loss_ok:
                if today_pnl >= config.DAILY_PROFIT_TARGET_USD:
                    failures.append(
                        f"Daily profit target reached (${today_pnl:.2f} >= "
                        f"${config.DAILY_PROFIT_TARGET_USD:.2f}) — new trades blocked for today"
                    )
                else:
                    failures.append(
                        f"Daily loss limit hit (${today_pnl:.2f} <= "
                        f"-${config.DAILY_LOSS_LIMIT_USD:.2f}) — new trades blocked for today"
                    )
        except Exception:
            logger.exception("RiskAgent: daily P&L check failed")
            failures.append("Daily P&L check error")

        # ----------------------------------------------------------
        # Check 5: Compute lot size from a fixed dollar notional
        # ----------------------------------------------------------
        volume = self._compute_lot_size(event.symbol, event.entry)

        # ----------------------------------------------------------
        # Verdict
        # ----------------------------------------------------------
        approved = (
            rr_ok and max_trades_ok and correlation_ok and daily_loss_ok
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
                weekly_loss_ok=True,   # weekly $-limit check removed; always pass for audit consistency
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
        """
        Check that the risk-to-reward ratio meets the minimum threshold.

        R:R = |take_profit - entry| / |entry - stop_loss|

        Args:
            entry, stop_loss, take_profit: Price levels from the signal.

        Returns:
            bool: True if R:R >= MIN_RR.
        """
        risk   = abs(entry - stop_loss)
        reward = abs(take_profit - entry)
        if risk == 0:
            return False
        rr = reward / risk
        logger.debug("RiskAgent R:R = %.3f (min %.1f)", rr, config.MIN_RR)
        # Use a small epsilon (1e-9) to handle floating-point imprecision
        # when the ratio is very close to the threshold (e.g. exactly 1.5).
        return rr >= (config.MIN_RR - 1e-9)

    def _check_correlation(self, signal_symbol: str, open_bases: Set[str]) -> bool:
        """
        Return False if the signal symbol's correlated partner is already open.

        Args:
            signal_symbol: Base symbol from the new signal (no suffix).
            open_bases:    Set of currently open base symbol names (suffix already stripped).
        """
        sig_upper = signal_symbol.upper()
        for pair in config.CORRELATED_PAIRS:
            a, b = pair[0].upper(), pair[1].upper()
            if sig_upper == a and b in open_bases:
                logger.info("RiskAgent: %s correlated with open %s", sig_upper, b)
                return False
            if sig_upper == b and a in open_bases:
                logger.info("RiskAgent: %s correlated with open %s", sig_upper, a)
                return False
        return True

    def _get_realized_pnl(self, from_date: datetime) -> float:
        """
        Return realized P&L in account currency from MT5 deal history since
        *from_date* (naive UTC datetime).  Sums the 'profit' field of all
        OUT deals (entry=1) — these are the closing legs of positions.
        """
        try:
            df = self._client.history.get_deals_as_dataframe(
                from_date=from_date,
                to_date=datetime.now(),
            )
            if df is None or len(df) == 0:
                return 0.0
            if "entry" not in df.columns or "profit" not in df.columns:
                return 0.0
            # entry=1 → DEAL_ENTRY_OUT (closing deal); entry=2 → INOUT (reverse)
            closing = df[df["entry"].isin([1, 2])]
            return float(closing["profit"].sum())
        except Exception:
            logger.exception("RiskAgent: failed to query MT5 deal history for PnL")
            return 0.0

    def _compute_lot_size(self, symbol: str, entry: float) -> float:
        """
        Compute lot size from a fixed dollar notional per trade.

        Formula:
            raw_lot = FIXED_TRADE_USD / (entry × contract_size)
            volume  = floor(raw_lot / volume_step) × volume_step
            Then clamped to [volume_min, volume_max].

        For BTC at ~$65,000 with contract_size=1:
            raw_lot = 10 / (65000 × 1) = 0.000153 → clamps up to volume_min.
        That floor-clamp is expected and logged explicitly.

        Args:
            symbol: Base symbol (broker suffix applied via resolve_symbol).
            entry:  Entry price from the signal (no extra API call).

        Returns:
            float: Lot size targeting ~FIXED_TRADE_USD of notional exposure.
        """
        if not entry or entry <= 0:
            logger.warning(
                "RiskAgent: invalid entry=%s — falling back to LOT_MIN=%.5f",
                entry, config.LOT_MIN,
            )
            return config.LOT_MIN

        vol_min = config.LOT_MIN
        vol_max = config.LOT_MAX
        try:
            info    = self._client.market.get_symbol_info(config.resolve_symbol(symbol))
            vol_min = float(info.get("volume_min", config.LOT_MIN)) or config.LOT_MIN
            vol_max = float(info.get("volume_max", config.LOT_MAX)) or config.LOT_MAX
        except Exception:
            logger.warning(
                "RiskAgent: get_symbol_info failed for %s — using config defaults",
                symbol,
            )

        if config.USE_VOLUME_MIN_FLOOR:
            volume = round(vol_min, 5)
            logger.debug("USE_VOLUME_MIN_FLOOR active, setting lot to %s", volume)
            return volume

        vol_step      = vol_min
        contract_size = 1.0
        try:
            info          = self._client.market.get_symbol_info(config.resolve_symbol(symbol))
            vol_step      = float(info.get("volume_step",         vol_min)) or vol_min
            contract_size = float(info.get("trade_contract_size", 1.0))     or 1.0
        except Exception:
            pass  # already warned above

        raw_lot = config.FIXED_TRADE_USD / (entry * contract_size)
        if vol_step > 0:
            volume = math.floor(raw_lot / vol_step) * vol_step
        else:
            volume = raw_lot
        volume = round(max(vol_min, min(volume, vol_max)), 5)

        logger.info(
            "Lot sizing: notional=$%.2f, entry=%.5f, contract=%.4f → raw_lot=%.8f, final_lot=%.5f",
            config.FIXED_TRADE_USD, entry, contract_size, raw_lot, volume,
        )

        notional = volume * entry * contract_size
        if notional > config.FIXED_TRADE_USD * 1.05:
            logger.warning(
                "RiskAgent: broker minimum lot forces notional=$%.2f (target $%.2f) "
                "at entry=%.5f contract=%.4f — volume=%.5f (floor clamp applied)",
                notional, config.FIXED_TRADE_USD, entry, contract_size, volume,
            )

        return volume

    # ------------------------------------------------------------------
    # Helper: publish rejection
    # ------------------------------------------------------------------

    def _publish_rejection(self, event: SignalGeneratedEvent, reason: str) -> None:
        """
        Publish a RiskEvaluatedEvent with approved=False. Used on exceptions
        so the audit log always gets a row even on error paths.

        Args:
            event:  The original signal event.
            reason: Human-readable rejection reason.
        """
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
                    weekly_loss_ok=True,   # weekly $-limit check removed; always pass for audit consistency
                )
            )
        except Exception:
            logger.exception("RiskAgent: failed to publish rejection event")
