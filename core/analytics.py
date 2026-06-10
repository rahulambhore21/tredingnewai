"""
core/analytics.py — Strategy performance analytics engine.

Reads from the validation_log table (and supporting tables) to compute every
metric needed to determine whether the S/R + AI strategy actually has an edge.

Usage:
    from core.database import Database
    from core.analytics import AnalyticsEngine

    db = Database()
    engine = AnalyticsEngine(db)
    report = engine.compute_report()
    engine.print_report(report)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from core.database import Database

logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """
    Computes performance metrics from the validation_log table.

    All methods are read-only — they never write to the DB.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # Main report entry point
    # ------------------------------------------------------------------

    def compute_report(self) -> Dict[str, Any]:
        """
        Return a structured dict with every analytics section.

        Keys:
            summary         — overall P&L and trade counts
            performance     — win rate, R:R, profit factor, expectancy
            pipeline        — zone-touch → analysis → signal conversion rates
            by_zone_type    — support vs resistance breakdown
            by_zone_strength — performance split by strength bucket
            by_timeframe    — performance split by timeframe
            by_symbol       — performance split by symbol
            risk_gate_stats — how often risk rejects signals and why
        """
        trades       = self._db.get_closed_trades_for_analytics()
        all_signals  = self._db.get_all_signals_for_analytics()
        event_counts = self._db.get_event_counts()
        pipeline     = self._db.get_zone_touch_to_signal_rate()

        report: Dict[str, Any] = {
            "summary":          self._compute_summary(trades),
            "performance":      self._compute_performance(trades),
            "pipeline":         self._compute_pipeline(pipeline, all_signals),
            "by_zone_type":     self._split_by(trades, "zone_type"),
            "by_zone_strength": self._split_by_strength(trades),
            "by_timeframe":     self._split_by(trades, "timeframe"),
            "by_symbol":        self._split_by(trades, "symbol"),
            "risk_gate_stats":  self._compute_risk_stats(all_signals),
            "event_counts":     event_counts,
        }
        return report

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _compute_summary(self, trades: List[Dict]) -> Dict[str, Any]:
        if not trades:
            return {"total_closed": 0, "total_pnl": 0.0}
        total_pnl   = sum(t["realized_pnl"] for t in trades)
        wins        = [t for t in trades if t["trade_result"] == "WIN"]
        losses      = [t for t in trades if t["trade_result"] == "LOSS"]
        breakevens  = [t for t in trades if t["trade_result"] == "BREAKEVEN"]
        durations   = [t["duration_sec"] for t in trades if t.get("duration_sec")]
        return {
            "total_closed":    len(trades),
            "wins":            len(wins),
            "losses":          len(losses),
            "breakevens":      len(breakevens),
            "total_pnl":       round(total_pnl, 2),
            "avg_duration_sec": round(sum(durations) / len(durations), 0) if durations else None,
            "avg_duration_min": round(sum(durations) / len(durations) / 60, 1) if durations else None,
        }

    # ------------------------------------------------------------------
    # Core performance metrics
    # ------------------------------------------------------------------

    def _compute_performance(self, trades: List[Dict]) -> Dict[str, Any]:
        if not trades:
            return {}

        wins   = [t for t in trades if t["trade_result"] == "WIN"]
        losses = [t for t in trades if t["trade_result"] == "LOSS"]

        win_rate = len(wins) / len(trades) if trades else 0.0

        # R:R per trade — computed from signal entry/sl/tp (not fill prices)
        rr_values = []
        for t in trades:
            entry = t.get("fill_price") or t.get("entry")
            sl    = t.get("stop_loss")
            tp    = t.get("take_profit")
            if entry and sl and tp and entry != sl:
                rr = abs(tp - entry) / abs(entry - sl)
                rr_values.append(rr)

        avg_rr = round(sum(rr_values) / len(rr_values), 3) if rr_values else None

        # Profit factor = gross profit / gross loss
        gross_profit = sum(t["realized_pnl"] for t in wins)  if wins   else 0.0
        gross_loss   = abs(sum(t["realized_pnl"] for t in losses)) if losses else 0.0
        profit_factor = (
            round(gross_profit / gross_loss, 3) if gross_loss > 0 else float("inf")
        )

        # Expectancy = (win_rate × avg_win) − (loss_rate × avg_loss)
        avg_win  = gross_profit / len(wins)   if wins   else 0.0
        avg_loss = gross_loss   / len(losses) if losses else 0.0
        expectancy = round(win_rate * avg_win - (1 - win_rate) * avg_loss, 4)

        # Average confidence of winning vs losing signals
        conf_win  = [t["confidence"] for t in wins   if t.get("confidence")]
        conf_loss = [t["confidence"] for t in losses if t.get("confidence")]

        return {
            "win_rate":          round(win_rate, 4),
            "win_rate_pct":      round(win_rate * 100, 2),
            "avg_rr":            avg_rr,
            "gross_profit":      round(gross_profit, 2),
            "gross_loss":        round(gross_loss, 2),
            "profit_factor":     profit_factor,
            "expectancy_usd":    expectancy,
            "avg_win_usd":       round(avg_win, 2),
            "avg_loss_usd":      round(avg_loss, 2),
            "avg_conf_wins":     round(sum(conf_win) / len(conf_win), 1) if conf_win else None,
            "avg_conf_losses":   round(sum(conf_loss) / len(conf_loss), 1) if conf_loss else None,
        }

    # ------------------------------------------------------------------
    # Pipeline conversion metrics
    # ------------------------------------------------------------------

    def _compute_pipeline(
        self, pipeline: Dict[str, int], all_signals: List[Dict]
    ) -> Dict[str, Any]:
        touches   = pipeline.get("zone_touch_event", 0)
        analyses  = pipeline.get("analysis_started_event", 0)
        signals   = pipeline.get("signal_generated_event", 0)
        approved  = sum(1 for s in all_signals if s.get("risk_approved") == 1)
        executed  = sum(1 for s in all_signals if s.get("order_id") is not None)
        closed    = sum(1 for s in all_signals if s.get("trade_result") in ("WIN", "LOSS", "BREAKEVEN"))

        def pct(num: int, denom: int) -> Optional[float]:
            return round(num / denom * 100, 1) if denom else None

        return {
            "zone_touches":              touches,
            "analyses_started":          analyses,
            "signals_generated":         signals,
            "risk_approved":             approved,
            "orders_executed":           executed,
            "trades_closed":             closed,
            "touch_to_analysis_pct":     pct(analyses, touches),
            "analysis_to_signal_pct":    pct(signals, analyses),
            "signal_to_approval_pct":    pct(approved, signals),
            "approval_to_execution_pct": pct(executed, approved),
        }

    # ------------------------------------------------------------------
    # Split by categorical field
    # ------------------------------------------------------------------

    def _split_by(self, trades: List[Dict], field: str) -> Dict[str, Any]:
        """Compute per-category win rate and P&L for a given field."""
        categories: Dict[str, List[Dict]] = {}
        for t in trades:
            key = str(t.get(field) or "unknown")
            categories.setdefault(key, []).append(t)

        result = {}
        for cat, cat_trades in sorted(categories.items()):
            wins   = [t for t in cat_trades if t["trade_result"] == "WIN"]
            losses = [t for t in cat_trades if t["trade_result"] == "LOSS"]
            total  = len(cat_trades)
            pnl    = sum(t["realized_pnl"] for t in cat_trades)
            result[cat] = {
                "trades":    total,
                "wins":      len(wins),
                "losses":    len(losses),
                "win_rate":  round(len(wins) / total, 4) if total else 0.0,
                "total_pnl": round(pnl, 2),
            }
        return result

    def _split_by_strength(self, trades: List[Dict]) -> Dict[str, Any]:
        """Bucket zone_strength (1–2, 3–4, 5+) and compute per-bucket metrics."""
        def bucket(strength: Optional[int]) -> str:
            if strength is None:
                return "unknown"
            if strength <= 2:
                return "weak (1-2)"
            if strength <= 4:
                return "moderate (3-4)"
            return "strong (5+)"

        bucketed = []
        for t in trades:
            bucketed.append({**t, "_bucket": bucket(t.get("zone_strength"))})

        categories: Dict[str, List[Dict]] = {}
        for t in bucketed:
            categories.setdefault(t["_bucket"], []).append(t)

        result = {}
        for cat, cat_trades in sorted(categories.items()):
            wins  = [t for t in cat_trades if t["trade_result"] == "WIN"]
            total = len(cat_trades)
            pnl   = sum(t["realized_pnl"] for t in cat_trades)
            result[cat] = {
                "trades":    total,
                "wins":      len(wins),
                "win_rate":  round(len(wins) / total, 4) if total else 0.0,
                "total_pnl": round(pnl, 2),
            }
        return result

    # ------------------------------------------------------------------
    # Risk gate stats
    # ------------------------------------------------------------------

    def _compute_risk_stats(self, all_signals: List[Dict]) -> Dict[str, Any]:
        total     = len(all_signals)
        approved  = sum(1 for s in all_signals if s.get("risk_approved") == 1)
        rejected  = sum(1 for s in all_signals if s.get("risk_approved") == 0)
        pending   = total - approved - rejected

        # Count rejection reasons
        reasons: Dict[str, int] = {}
        for s in all_signals:
            if s.get("risk_approved") == 0 and s.get("risk_reason"):
                reason = s["risk_reason"]
                reasons[reason] = reasons.get(reason, 0) + 1

        return {
            "total_signals":   total,
            "approved":        approved,
            "rejected":        rejected,
            "pending":         pending,
            "approval_rate":   round(approved / total, 4) if total else 0.0,
            "rejection_reasons": dict(sorted(reasons.items(), key=lambda x: -x[1])[:10]),
        }

    # ------------------------------------------------------------------
    # Print report
    # ------------------------------------------------------------------

    def print_report(self, report: Optional[Dict[str, Any]] = None) -> None:
        """Print a human-readable performance report to stdout."""
        if report is None:
            report = self.compute_report()

        s   = report.get("summary", {})
        p   = report.get("performance", {})
        pip = report.get("pipeline", {})
        rg  = report.get("risk_gate_stats", {})

        sep = "=" * 60

        print(f"\n{sep}")
        print("  TRADING BOT PERFORMANCE REPORT")
        print(sep)

        print(f"\n{'-'*40}")
        print("  SUMMARY")
        print(f"{'-'*40}")
        print(f"  Total closed trades : {s.get('total_closed', 0)}")
        print(f"  Wins / Losses / BE  : {s.get('wins', 0)} / {s.get('losses', 0)} / {s.get('breakevens', 0)}")
        print(f"  Total P&L           : ${s.get('total_pnl', 0.0):+.2f}")
        avg_dur = s.get("avg_duration_min")
        if avg_dur is not None:
            print(f"  Avg trade duration  : {avg_dur} min")

        print(f"\n{'-'*40}")
        print("  PERFORMANCE METRICS")
        print(f"{'-'*40}")
        print(f"  Win rate            : {p.get('win_rate_pct', 0.0):.1f}%")
        print(f"  Average R:R         : {p.get('avg_rr', 'N/A')}")
        print(f"  Profit factor       : {p.get('profit_factor', 'N/A')}")
        print(f"  Expectancy          : ${p.get('expectancy_usd', 0.0):+.4f} / trade")
        print(f"  Avg win             : ${p.get('avg_win_usd', 0.0):+.2f}")
        print(f"  Avg loss            : ${p.get('avg_loss_usd', 0.0):-.2f}")
        if p.get("avg_conf_wins") is not None:
            print(f"  Avg conf (wins)     : {p.get('avg_conf_wins'):.1f}")
            print(f"  Avg conf (losses)   : {p.get('avg_conf_losses'):.1f}")

        print(f"\n{'-'*40}")
        print("  PIPELINE CONVERSION")
        print(f"{'-'*40}")
        print(f"  Zone touches        : {pip.get('zone_touches', 0)}")
        print(f"  Analysis started    : {pip.get('analyses_started', 0)}  "
              f"({pip.get('touch_to_analysis_pct', 'N/A')}%)")
        print(f"  Signals generated   : {pip.get('signals_generated', 0)}  "
              f"({pip.get('analysis_to_signal_pct', 'N/A')}%)")
        print(f"  Risk approved       : {pip.get('risk_approved', 0)}  "
              f"({pip.get('signal_to_approval_pct', 'N/A')}%)")
        print(f"  Orders executed     : {pip.get('orders_executed', 0)}  "
              f"({pip.get('approval_to_execution_pct', 'N/A')}%)")
        print(f"  Trades closed       : {pip.get('trades_closed', 0)}")

        print(f"\n{'-'*40}")
        print("  BY ZONE TYPE")
        print(f"{'-'*40}")
        for zone_type, m in report.get("by_zone_type", {}).items():
            print(f"  {zone_type:12s}  trades={m['trades']:3d}  "
                  f"win%={m['win_rate']*100:.1f}%  pnl=${m['total_pnl']:+.2f}")

        print(f"\n{'-'*40}")
        print("  BY ZONE STRENGTH")
        print(f"{'-'*40}")
        for bucket_name, m in report.get("by_zone_strength", {}).items():
            print(f"  {bucket_name:20s}  trades={m['trades']:3d}  "
                  f"win%={m['win_rate']*100:.1f}%  pnl=${m['total_pnl']:+.2f}")

        print(f"\n{'-'*40}")
        print("  BY TIMEFRAME")
        print(f"{'-'*40}")
        for tf, m in report.get("by_timeframe", {}).items():
            print(f"  {tf:6s}  trades={m['trades']:3d}  "
                  f"win%={m['win_rate']*100:.1f}%  pnl=${m['total_pnl']:+.2f}")

        print(f"\n{'-'*40}")
        print("  BY SYMBOL")
        print(f"{'-'*40}")
        for sym, m in report.get("by_symbol", {}).items():
            print(f"  {sym:10s}  trades={m['trades']:3d}  "
                  f"win%={m['win_rate']*100:.1f}%  pnl=${m['total_pnl']:+.2f}")

        print(f"\n{'-'*40}")
        print("  RISK GATE")
        print(f"{'-'*40}")
        print(f"  Total signals       : {rg.get('total_signals', 0)}")
        print(f"  Approved            : {rg.get('approved', 0)}  "
              f"({rg.get('approval_rate', 0.0)*100:.1f}%)")
        print(f"  Rejected            : {rg.get('rejected', 0)}")
        reasons = rg.get("rejection_reasons", {})
        if reasons:
            print("  Top rejection reasons:")
            for reason, cnt in list(reasons.items())[:5]:
                print(f"    [{cnt:3d}x] {reason}")

        print(f"\n{sep}\n")
