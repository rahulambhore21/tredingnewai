"""
debug_panel.py — Live terminal transparency view for the trading bot.

Shows every calculation, decision, and internal state in real-time:
  - Active S/R zones (exact price levels, strength, timeframe)
  - Recent zone touches (proximity %, which zone fired, cooldown state)
  - Latest signal (all 6 indicators, preflight filter logic, GPT response)
  - Risk decisions (every check with exact numbers, lot-size formula)
  - Open/closed trades (position progress, P&L, breakeven/trailing state)
  - Event flow funnel (touches → analysis → signals → risk → execution)
  - Agent health heartbeats

Run this in a separate terminal while the bot is running:
    python debug_panel.py

Press Ctrl+C to exit.
"""

import json
import math
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.columns import Columns
    from rich.text import Text
    from rich.live import Live
    from rich.layout import Layout
    from rich.rule import Rule
    from rich import box
    from rich.align import Align
except ImportError:
    print("rich not installed. Run: pip install rich")
    sys.exit(1)

try:
    import config as _cfg
    DB_PATH = str(ROOT / _cfg.DB_PATH)
    DAILY_LOSS_LIMIT = float(_cfg.DAILY_LOSS_LIMIT_USD)
    DAILY_PROFIT_TARGET = float(_cfg.DAILY_PROFIT_TARGET_USD)
    MIN_RR = float(_cfg.MIN_RR)
    MIN_CONFIDENCE = float(_cfg.MIN_CONFIDENCE)
    FIXED_TRADE_USD = float(_cfg.FIXED_TRADE_USD)
    MAX_OPEN_TRADES = int(_cfg.MAX_OPEN_TRADES)
    BREAKEVEN_TRIGGER = getattr(_cfg, "BREAKEVEN_TRIGGER_PCT", 0.6)
    TRAILING_TRIGGER = getattr(_cfg, "TRAILING_TRIGGER_PCT", 0.8)
except Exception as e:
    print(f"Warning: could not import config ({e}) — using defaults")
    DB_PATH = str(ROOT / "trading_bot.db")
    DAILY_LOSS_LIMIT = 30.0
    DAILY_PROFIT_TARGET = 100.0
    MIN_RR = 1.5
    MIN_CONFIDENCE = 65.0
    FIXED_TRADE_USD = 10.0
    MAX_OPEN_TRADES = 4
    BREAKEVEN_TRIGGER = 0.6
    TRAILING_TRIGGER = 0.8

LOG_PATH = str(ROOT / "trading_bot.log")

console = Console()


# ── DB helpers ──────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA query_only = ON")
    c.row_factory = sqlite3.Row
    return c


def _q(sql: str, params: tuple = ()) -> List[Dict]:
    try:
        c = _conn()
        rows = [dict(r) for r in c.execute(sql, params).fetchall()]
        c.close()
        return rows
    except Exception:
        return []


def _s(sql: str, params: tuple = (), default=None):
    try:
        c = _conn()
        row = c.execute(sql, params).fetchone()
        c.close()
        return (row[0] if row[0] is not None else default) if row else default
    except Exception:
        return default


def _today() -> str:
    return datetime.now(tz=timezone.utc).date().isoformat()


def _week_start() -> str:
    now = datetime.now(tz=timezone.utc)
    return (now - timedelta(days=now.weekday())).date().isoformat()


def _ago(iso: Optional[str]) -> str:
    if not iso:
        return "never"
    try:
        ts = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        delta = datetime.now(tz=timezone.utc) - ts
        s = int(delta.total_seconds())
        if s < 60:
            return f"{s}s ago"
        if s < 3600:
            return f"{s // 60}m {s % 60}s ago"
        return f"{s // 3600}h {(s % 3600) // 60}m ago"
    except Exception:
        return iso[:19] if iso else "?"


def _fmt(v, decimals=5, prefix=""):
    if v is None:
        return "[dim]—[/dim]"
    return f"{prefix}{float(v):.{decimals}f}"


def _pct_bar(pct: float, width: int = 12) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(pct / 100 * width)
    bar = "█" * filled + "░" * (width - filled)
    return bar


# ── Section builders ─────────────────────────────────────────────────────────

def build_header() -> Panel:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    db_exists = Path(DB_PATH).exists()
    status = "[green]DB connected[/green]" if db_exists else "[red]DB not found[/red]"
    return Panel(
        f"[bold cyan]Trading Bot — Debug Transparency Panel[/bold cyan]   "
        f"[dim]{now}[/dim]   {status}   "
        f"[dim]DB: {DB_PATH}[/dim]",
        box=box.HORIZONTALS,
        padding=(0, 1),
    )


def build_event_funnel() -> Panel:
    today = _today()

    def cnt(sql, params=()):
        return int(_s(sql, params, default=0) or 0)

    touches_t  = cnt("SELECT COUNT(*) FROM events WHERE event_type='zone_touch_event' AND DATE(created_at)=?", (today,))
    analysis_t = cnt("SELECT COUNT(*) FROM events WHERE event_type='analysis_started_event' AND DATE(created_at)=?", (today,))
    signals_t  = cnt("SELECT COUNT(*) FROM signals WHERE DATE(created_at)=?", (today,))
    approved_t = cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=1 AND DATE(created_at)=?", (today,))
    rejected_t = cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=0 AND DATE(created_at)=?", (today,))
    exec_t     = cnt("SELECT COUNT(*) FROM trades WHERE success=1 AND dry_run=0 AND DATE(created_at)=?", (today,))
    fail_t     = cnt("SELECT COUNT(*) FROM trades WHERE success=0 AND dry_run=0 AND DATE(created_at)=?", (today,))

    # Conversion rates
    def rate(num, denom):
        return f"{num/denom*100:.0f}%" if denom > 0 else "—"

    t = Table(box=box.SIMPLE, show_header=True, header_style="bold white", padding=(0, 1))
    t.add_column("Stage", style="cyan", width=22)
    t.add_column("Today", justify="right", width=7)
    t.add_column("Conv %", justify="right", width=8)
    t.add_column("Visual", width=16)

    max_val = max(touches_t, 1)
    t.add_row("1. Zone Touches",    str(touches_t),  "—",
              f"[blue]{_pct_bar(touches_t/max_val*100)}[/blue]")
    t.add_row("2. Analysis Started", str(analysis_t), rate(analysis_t, touches_t),
              f"[cyan]{_pct_bar(analysis_t/max_val*100)}[/cyan]")
    t.add_row("3. GPT Signals",     str(signals_t),  rate(signals_t, analysis_t),
              f"[yellow]{_pct_bar(signals_t/max_val*100)}[/yellow]")
    t.add_row("4. Risk Approved",   str(approved_t), rate(approved_t, signals_t),
              f"[green]{_pct_bar(approved_t/max_val*100)}[/green]")
    t.add_row("4. Risk Rejected",   str(rejected_t), rate(rejected_t, signals_t),
              f"[red]{_pct_bar(rejected_t/max_val*100)}[/red]")
    t.add_row("5. Executed",        str(exec_t),     rate(exec_t, approved_t),
              f"[green bold]{_pct_bar(exec_t/max_val*100)}[/green bold]")
    if fail_t > 0:
        t.add_row("5. Exec Failed",    str(fail_t),     rate(fail_t, approved_t),
                  f"[red bold]{_pct_bar(fail_t/max_val*100)}[/red bold]")

    return Panel(t, title="[bold]Event Funnel (Today)[/bold]", border_style="blue")


def build_agent_health() -> Panel:
    def last(*types):
        ph = ",".join("?" * len(types))
        return _s(f"SELECT MAX(created_at) FROM events WHERE event_type IN ({ph})", tuple(types))

    agents = {
        "SRMapper":      last("zones_refreshed_event", "zone_event"),
        "PriceWatcher":  last("zone_touch_event"),
        "AnalysisAgent": last("signal_generated_event", "analysis_started_event"),
        "TradeMonitor":  last("trade_closed_event", "breakeven_moved_event", "trailing_updated_event"),
        "RiskAgent":     last("risk_evaluated_event"),
        "Executor":      last("trade_executed_event"),
    }

    t = Table(box=box.SIMPLE, show_header=True, header_style="bold white", padding=(0, 1))
    t.add_column("Agent", style="cyan", width=16)
    t.add_column("Last Active", width=18)
    t.add_column("Status", width=10)

    for name, ts in agents.items():
        ago_str = _ago(ts)
        if ts is None:
            status = "[dim]no data[/dim]"
        else:
            try:
                parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                secs = (datetime.now(tz=timezone.utc) - parsed).total_seconds()
                if secs < 120:
                    status = "[green]alive[/green]"
                elif secs < 600:
                    status = "[yellow]idle[/yellow]"
                else:
                    status = "[red]stale[/red]"
            except Exception:
                status = "[dim]?[/dim]"
        t.add_row(name, ago_str, status)

    return Panel(t, title="[bold]Agent Health[/bold]", border_style="green")


def build_pnl() -> Panel:
    today = _today()
    week_start = _week_start()

    def pnl_sum(where, params):
        return float(_s(
            "SELECT COALESCE(SUM(realized_pnl),0) FROM trades "
            "WHERE dry_run=0 AND close_time IS NOT NULL AND realized_pnl IS NOT NULL " + where,
            params, default=0.0
        ) or 0.0)

    pnl_today = pnl_sum("AND DATE(close_time)=?", (today,))
    pnl_week  = pnl_sum("AND DATE(close_time)>=?", (week_start,))
    pnl_all   = pnl_sum("", ())

    loss_pct   = abs(min(pnl_today, 0.0)) / DAILY_LOSS_LIMIT   * 100 if pnl_today < 0 else 0
    profit_pct = max(pnl_today, 0.0)      / DAILY_PROFIT_TARGET * 100 if pnl_today > 0 else 0

    col = "green" if pnl_today >= 0 else "red"
    wcol = "green" if pnl_week >= 0 else "red"
    acol = "green" if pnl_all >= 0 else "red"

    lines = [
        f"Today P&L:  [{col}][bold]${pnl_today:+.2f}[/bold][/{col}]  "
        f"(limit -${DAILY_LOSS_LIMIT:.0f} / target +${DAILY_PROFIT_TARGET:.0f})",
        f"  Loss used:   {_pct_bar(loss_pct)} {loss_pct:.0f}%",
        f"  Target used: {_pct_bar(profit_pct)} {profit_pct:.0f}%",
        f"Week P&L:   [{wcol}]${pnl_week:+.2f}[/{wcol}]",
        f"All-time:   [{acol}]${pnl_all:+.2f}[/{acol}]",
    ]
    return Panel("\n".join(lines), title="[bold]P&L Tracker[/bold]", border_style="yellow")


def build_active_zones() -> Panel:
    zones = _q(
        """
        SELECT id, symbol, timeframe, zone_type, price_center, price_upper,
               price_lower, strength, created_at
        FROM zones WHERE is_active=1
        ORDER BY symbol, timeframe, price_center
        """
    )

    if not zones:
        return Panel("[dim]No active zones in database.[/dim]",
                     title="[bold]Active S/R Zones[/bold]", border_style="magenta")

    t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold white", padding=(0, 1))
    t.add_column("ID",     width=5,  justify="right")
    t.add_column("Symbol", width=9)
    t.add_column("TF",     width=4)
    t.add_column("Type",   width=11)
    t.add_column("Center",  width=11, justify="right")
    t.add_column("Upper",   width=11, justify="right")
    t.add_column("Lower",   width=11, justify="right")
    t.add_column("Width %", width=8,  justify="right")
    t.add_column("Str",    width=4,  justify="right")
    t.add_column("Created", width=18)

    for z in zones:
        center = float(z["price_center"])
        upper  = float(z["price_upper"])
        lower  = float(z["price_lower"])
        width_pct = (upper - lower) / center * 100 if center > 0 else 0
        ztype = z["zone_type"]
        color = "cyan" if ztype == "support" else "magenta"
        t.add_row(
            str(z["id"]),
            z["symbol"],
            z["timeframe"],
            f"[{color}]{ztype}[/{color}]",
            f"{center:.5f}",
            f"{upper:.5f}",
            f"{lower:.5f}",
            f"{width_pct:.3f}%",
            str(z["strength"]),
            _ago(z["created_at"]),
        )

    return Panel(t, title=f"[bold]Active S/R Zones ({len(zones)})[/bold]", border_style="magenta")


def build_recent_touches() -> Panel:
    rows = _q(
        """
        SELECT id, symbol, payload, created_at
        FROM events
        WHERE event_type = 'zone_touch_event'
        ORDER BY created_at DESC
        LIMIT 10
        """
    )

    if not rows:
        return Panel("[dim]No zone touches recorded yet.[/dim]",
                     title="[bold]Recent Zone Touches[/bold]", border_style="blue")

    t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold white", padding=(0, 1))
    t.add_column("When",      width=14)
    t.add_column("Symbol",    width=9)
    t.add_column("Zone ID",   width=8, justify="right")
    t.add_column("Type",      width=11)
    t.add_column("Center",    width=11, justify="right")
    t.add_column("Mid Price", width=11, justify="right")
    t.add_column("Proximity", width=10, justify="right")
    t.add_column("TF",        width=4)
    t.add_column("Strength",  width=8, justify="right")

    for r in rows:
        payload = {}
        try:
            payload = json.loads(r["payload"] or "{}")
        except Exception:
            pass

        center    = payload.get("price_center", 0)
        mid_price = payload.get("mid_price", payload.get("bid", 0))
        zone_id   = payload.get("zone_id", "?")
        ztype     = payload.get("zone_type", "?")
        tf        = payload.get("timeframe", "?")
        strength  = payload.get("zone_strength", payload.get("strength", "?"))

        prox = ""
        if center and mid_price and float(center) > 0:
            prox_pct = abs(float(mid_price) - float(center)) / float(center) * 100
            color = "green" if prox_pct < 0.05 else "yellow" if prox_pct < 0.2 else "white"
            prox = f"[{color}]{prox_pct:.3f}%[/{color}]"

        color = "cyan" if ztype == "support" else "magenta"
        t.add_row(
            _ago(r["created_at"]),
            r["symbol"],
            str(zone_id),
            f"[{color}]{ztype}[/{color}]",
            f"{float(center):.5f}" if center else "—",
            f"{float(mid_price):.5f}" if mid_price else "—",
            prox,
            str(tf),
            str(strength),
        )

    return Panel(t, title="[bold]Recent Zone Touches[/bold]", border_style="blue")


def build_latest_signals() -> Panel:
    rows = _q(
        """
        SELECT s.*, rd.approved, rd.reason, rd.volume, rd.rr_ok, rd.max_trades_ok,
               rd.correlation_ok, rd.daily_loss_ok, rd.weekly_loss_ok
        FROM signals s
        LEFT JOIN risk_decisions rd ON rd.signal_id = s.id
        ORDER BY s.created_at DESC
        LIMIT 5
        """
    )

    if not rows:
        return Panel("[dim]No signals generated yet.[/dim]",
                     title="[bold]Latest GPT Signals + Indicators[/bold]", border_style="yellow")

    sections = []
    for i, s in enumerate(rows):
        entry      = float(s["entry"] or 0)
        sl         = float(s["stop_loss"] or 0)
        tp         = float(s["take_profit"] or 0)
        risk       = abs(entry - sl)
        reward     = abs(tp - entry)
        rr         = reward / risk if risk > 0 else 0
        direction  = s["direction"] or "?"
        confidence = float(s["confidence"] or 0)
        approved   = s.get("approved")

        dir_color = "green" if direction == "BUY" else "red"
        conf_color = "green" if confidence >= MIN_CONFIDENCE else "yellow"
        rr_color   = "green" if rr >= MIN_RR else "red"

        # Indicator calculations breakdown
        ema21     = s.get("ema21")
        ema50     = s.get("ema50")
        rsi14     = s.get("rsi14")
        macd_line = s.get("macd_line")
        macd_sig  = s.get("macd_signal")
        macd_hist = s.get("macd_hist")

        # EMA trend
        ema_trend = ""
        if ema21 is not None and ema50 is not None:
            if float(ema21) > float(ema50):
                ema_trend = "[green]EMA21 > EMA50 (uptrend)[/green]"
            else:
                ema_trend = "[red]EMA21 < EMA50 (downtrend)[/red]"

        # RSI state
        rsi_state = ""
        if rsi14 is not None:
            r = float(rsi14)
            if r > 70:
                rsi_state = f"[red]overbought ({r:.1f})[/red]"
            elif r < 30:
                rsi_state = f"[cyan]oversold ({r:.1f})[/cyan]"
            else:
                rsi_state = f"[white]neutral ({r:.1f})[/white]"

        # MACD state
        macd_state = ""
        if macd_hist is not None:
            h = float(macd_hist)
            macd_state = f"[green]bullish[/green]" if h > 0 else f"[red]bearish[/red]"
            macd_state += f" (hist={h:.6f})"

        # Risk check breakdown
        risk_line = ""
        if approved is not None:
            checks = {
                "R:R":         (s.get("rr_ok"),          f"{rr:.2f} >= {MIN_RR}"),
                "Max Trades":  (s.get("max_trades_ok"),  f"< {MAX_OPEN_TRADES} open"),
                "Correlation": (s.get("correlation_ok"), "no correlated pair"),
                "Daily P&L":   (s.get("daily_loss_ok"),  f"within ±${DAILY_LOSS_LIMIT:.0f}/${DAILY_PROFIT_TARGET:.0f}"),
            }
            parts = []
            for name, (ok, detail) in checks.items():
                if ok is None:
                    continue
                c = "green" if ok else "red"
                icon = "✓" if ok else "✗"
                parts.append(f"[{c}]{icon} {name}[/{c}]")
            risk_line = "  " + "  ".join(parts) if parts else ""

        app_str = ""
        if approved is not None:
            app_str = "[green bold] APPROVED[/green bold]" if approved else "[red bold] REJECTED[/red bold]"

        vol_str = ""
        if s.get("volume") is not None and float(s.get("volume", 0)) > 0:
            vol = float(s["volume"])
            notional = vol * entry
            vol_str = f"  Lot: {vol:.5f} (≈${notional:.2f} notional)"

        lines = [
            f"#{s['id']} [{dir_color}]{direction}[/{dir_color}] {s['symbol']} "
            f"[dim]{_ago(s['created_at'])}[/dim]{app_str}",
            f"  Entry: {entry:.5f}  SL: {sl:.5f}  TP: {tp:.5f}",
            f"  R:R: [{rr_color}]{rr:.2f}[/{rr_color}] (risk={risk:.5f}, reward={reward:.5f})  "
            f"Confidence: [{conf_color}]{confidence:.0f}%[/{conf_color}] (min {MIN_CONFIDENCE:.0f}%)",
            f"  Indicators: EMA21={_fmt(ema21)}  EMA50={_fmt(ema50)}  {ema_trend}",
            f"  RSI14: {rsi_state}  MACD: {macd_state}",
        ]
        if macd_line is not None and macd_sig is not None:
            lines.append(f"  MACD line={float(macd_line):.6f}  signal={float(macd_sig):.6f}")
        if risk_line:
            lines.append(f"  Risk checks:{risk_line}")
        if vol_str:
            lines.append(vol_str)
        # Lot size formula display
        if entry > 0 and s.get("approved"):
            raw_lot = FIXED_TRADE_USD / entry
            lines.append(
                f"  Lot formula: ${FIXED_TRADE_USD:.0f} / {entry:.5f} = {raw_lot:.8f} raw → clamped to vol_min"
            )
        # GPT reasoning
        reasoning = s.get("reasoning", "")
        if reasoning:
            short = reasoning[:120] + "…" if len(reasoning) > 120 else reasoning
            lines.append(f"  [dim]GPT: {short}[/dim]")
        if approved == 0:
            reason = s.get("reason", "")
            if reason:
                lines.append(f"  [red]Rejected: {reason}[/red]")

        sections.append("\n".join(lines))
        if i < len(rows) - 1:
            sections.append("[dim]─[/dim]" * 40)

    return Panel(
        "\n".join(sections),
        title="[bold]Latest GPT Signals + Full Calculations[/bold]",
        border_style="yellow",
    )


def build_risk_details() -> Panel:
    rows = _q(
        """
        SELECT rd.*, s.entry, s.stop_loss, s.take_profit, s.ema21, s.ema50,
               s.rsi14, s.macd_line, s.macd_signal, s.macd_hist, s.confidence,
               s.direction as sig_direction, s.symbol as sig_symbol
        FROM risk_decisions rd
        LEFT JOIN signals s ON rd.signal_id = s.id
        ORDER BY rd.created_at DESC
        LIMIT 6
        """
    )

    if not rows:
        return Panel("[dim]No risk decisions yet.[/dim]",
                     title="[bold]Risk Gate — Detailed Decisions[/bold]", border_style="red")

    t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold white", padding=(0, 1))
    t.add_column("When",      width=13)
    t.add_column("Sym",       width=8)
    t.add_column("Dir",       width=5)
    t.add_column("Verdict",   width=9)
    t.add_column("R:R",       width=6,  justify="right")
    t.add_column("✓ R:R",     width=6,  justify="center")
    t.add_column("✓ Trades",  width=8,  justify="center")
    t.add_column("✓ Corr",    width=7,  justify="center")
    t.add_column("✓ DailyPL", width=9,  justify="center")
    t.add_column("Lot",       width=9,  justify="right")
    t.add_column("Reason",    width=35)

    for r in rows:
        entry  = float(r["entry"]      or 0)
        sl     = float(r["stop_loss"]  or 0)
        tp     = float(r["take_profit"] or 0)
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        rr     = reward / risk if risk > 0 else 0

        approved = bool(r["approved"])
        verdict_style = "green bold" if approved else "red bold"
        verdict = "APPROVED" if approved else "REJECTED"

        def check_icon(val):
            if val is None:
                return "[dim]?[/dim]"
            return "[green]✓[/green]" if val else "[red]✗[/red]"

        direction = r.get("direction") or r.get("sig_direction") or "?"
        dir_color = "green" if direction == "BUY" else "red"

        vol = float(r["volume"] or 0)
        lot_str = f"{vol:.5f}" if vol > 0 else "[dim]—[/dim]"
        reason = (r["reason"] or "")[:33]

        t.add_row(
            _ago(r["created_at"]),
            r["symbol"] or r.get("sig_symbol", "?"),
            f"[{dir_color}]{direction}[/{dir_color}]",
            f"[{verdict_style}]{verdict}[/{verdict_style}]",
            f"[{'green' if rr >= MIN_RR else 'red'}]{rr:.2f}[/{'green' if rr >= MIN_RR else 'red'}]",
            check_icon(r["rr_ok"]),
            check_icon(r["max_trades_ok"]),
            check_icon(r["correlation_ok"]),
            check_icon(r["daily_loss_ok"]),
            lot_str,
            reason,
        )

    return Panel(t, title="[bold]Risk Gate — Every Check Exposed[/bold]", border_style="red")


def build_trades() -> Panel:
    rows = _q(
        """
        SELECT id, symbol, direction, volume, entry, stop_loss, take_profit,
               order_id, fill_price, success, sl_tp_ok, dry_run,
               close_price, realized_pnl, created_at, close_time
        FROM trades
        ORDER BY created_at DESC
        LIMIT 8
        """
    )

    if not rows:
        return Panel("[dim]No trades yet.[/dim]",
                     title="[bold]Trade Execution History[/bold]", border_style="green")

    t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold white", padding=(0, 1))
    t.add_column("ID",      width=4,  justify="right")
    t.add_column("When",    width=13)
    t.add_column("Symbol",  width=9)
    t.add_column("Dir",     width=5)
    t.add_column("Vol",     width=8,  justify="right")
    t.add_column("Entry",   width=11, justify="right")
    t.add_column("SL",      width=11, justify="right")
    t.add_column("TP",      width=11, justify="right")
    t.add_column("Fill",    width=11, justify="right")
    t.add_column("SL/TP",   width=6,  justify="center")
    t.add_column("Status",  width=10)
    t.add_column("P&L",     width=9,  justify="right")

    for r in rows:
        direction = r["direction"]
        dir_color = "green" if direction == "BUY" else "red"
        success   = bool(r["success"])
        dry_run   = bool(r["dry_run"])
        pnl       = r.get("realized_pnl")

        if dry_run:
            status = "[dim]DRY-RUN[/dim]"
        elif success:
            status = "[green]FILLED[/green]"
        else:
            status = "[red]FAILED[/red]"

        pnl_str = "[dim]open[/dim]"
        if pnl is not None:
            pnl_color = "green" if float(pnl) >= 0 else "red"
            pnl_str = f"[{pnl_color}]${float(pnl):+.2f}[/{pnl_color}]"

        sltp_icon = "[dim]?[/dim]"
        if r["sl_tp_ok"] is not None:
            sltp_icon = "[green]✓[/green]" if r["sl_tp_ok"] else "[red]✗[/red]"

        entry = float(r["entry"] or 0)
        fill  = r.get("fill_price")
        fill_str = f"{float(fill):.5f}" if fill else "[dim]—[/dim]"

        t.add_row(
            str(r["id"]),
            _ago(r["created_at"]),
            r["symbol"],
            f"[{dir_color}]{direction}[/{dir_color}]",
            f"{float(r['volume']):.5f}",
            f"{entry:.5f}",
            f"{float(r['stop_loss']):.5f}",
            f"{float(r['take_profit']):.5f}",
            fill_str,
            sltp_icon,
            status,
            pnl_str,
        )

    return Panel(t, title="[bold]Trade Execution History[/bold]", border_style="green")


def build_validation_log() -> Panel:
    rows = _q(
        """
        SELECT id, symbol, timeframe, zone_type, zone_strength, ai_decision,
               confidence, entry, stop_loss, take_profit, risk_approved,
               risk_reason, order_id, fill_price, close_price, realized_pnl,
               trade_result, duration_sec, created_at, closed_at
        FROM validation_log
        ORDER BY created_at DESC
        LIMIT 6
        """
    )

    if not rows:
        return Panel("[dim]No validation log entries yet.[/dim]",
                     title="[bold]Trade Lifecycle (Full Chain)[/bold]", border_style="cyan")

    sections = []
    for r in rows:
        result = r["trade_result"] or "?"
        result_colors = {
            "WIN": "green bold", "LOSS": "red bold", "BREAKEVEN": "yellow",
            "REJECTED": "dim red", "OPEN": "cyan", "?": "dim",
        }
        rc = result_colors.get(result, "white")

        entry = float(r["entry"] or 0)
        sl    = float(r["stop_loss"] or 0)
        tp    = float(r["take_profit"] or 0)
        risk  = abs(entry - sl)
        reward = abs(tp - entry)
        rr    = reward / risk if risk > 0 else 0

        dir_color = "green" if (r["ai_decision"] or "") == "BUY" else "red"

        line1 = (
            f"#{r['id']} [{rc}]{result}[/{rc}]  "
            f"[{dir_color}]{r['ai_decision'] or '?'}[/{dir_color}] {r['symbol']} "
            f"[dim]{r['timeframe'] or '?'}[/dim]  "
            f"Conf: {float(r['confidence'] or 0):.0f}%  "
            f"Zone: {r['zone_type'] or '?'} (str={r['zone_strength'] or '?'})"
        )
        line2 = (
            f"  Entry={entry:.5f}  SL={sl:.5f}  TP={tp:.5f}  "
            f"R:R={rr:.2f}  "
            f"[dim]{_ago(r['created_at'])}[/dim]"
        )

        # Risk decision
        ra = r["risk_approved"]
        if ra is not None:
            ra_str = "[green]Risk: APPROVED[/green]" if ra else f"[red]Risk: REJECTED — {r['risk_reason'] or ''}[/red]"
            line2 += f"  {ra_str}"

        # Execution
        lines = [line1, line2]
        if r["order_id"]:
            fill = float(r["fill_price"] or 0)
            slippage = abs(fill - entry) if fill and entry else 0
            lines.append(
                f"  OrderID={r['order_id']}  Fill={fill:.5f}  "
                f"Slippage={slippage:.5f}"
            )

        # Close
        if r["close_price"]:
            pnl = float(r["realized_pnl"] or 0)
            dur = r["duration_sec"]
            dur_str = f"{dur//60}m {dur%60}s" if dur else "?"
            pnl_color = "green" if pnl >= 0 else "red"
            lines.append(
                f"  Close={float(r['close_price']):.5f}  "
                f"P&L=[{pnl_color}]${pnl:+.2f}[/{pnl_color}]  "
                f"Duration={dur_str}"
            )

        sections.append("\n".join(lines))
        sections.append("[dim]" + "─" * 60 + "[/dim]")

    return Panel(
        "\n".join(sections),
        title="[bold]Trade Lifecycle — Full Chain (Signal → Risk → Exec → Close)[/bold]",
        border_style="cyan",
    )


def build_preflight_analysis() -> Panel:
    rows = _q(
        """
        SELECT s.id, s.symbol, s.direction, s.ema21, s.ema50, s.rsi14,
               s.macd_hist, s.confidence, s.created_at,
               vl.zone_type, vl.zone_strength
        FROM signals s
        LEFT JOIN validation_log vl ON vl.signal_id = s.id
        ORDER BY s.created_at DESC
        LIMIT 5
        """
    )

    if not rows:
        return Panel("[dim]No signals to analyse yet.[/dim]",
                     title="[bold]Preflight Filter Analysis (What Passed vs Blocked)[/bold]",
                     border_style="magenta")

    sections = []
    for r in rows:
        ema21     = r.get("ema21")
        ema50     = r.get("ema50")
        rsi14     = r.get("rsi14")
        macd_hist = r.get("macd_hist")
        zone_type = (r.get("zone_type") or "?").lower()
        direction = r.get("direction", "?")

        lines = [f"Signal #{r['id']} — {direction} {r['symbol']} [dim]{_ago(r['created_at'])}[/dim]"]

        if None not in (ema21, ema50, rsi14, macd_hist):
            e21 = float(ema21)
            e50 = float(ema50)
            r14 = float(rsi14)
            mh  = float(macd_hist)

            lines.append(f"  Indicators: EMA21={e21:.5f}  EMA50={e50:.5f}  RSI14={r14:.1f}  MACD_hist={mh:.6f}")

            # Support zone preflight
            if zone_type == "support":
                ema_fail  = e21 < e50
                rsi_fail  = r14 > 65
                macd_fail = mh < 0
                would_block = ema_fail and rsi_fail and macd_fail
                lines.append(
                    f"  Preflight (support→BUY):"
                    f"  EMA21<EMA50=[{'red ✗' if ema_fail else 'green ✓'}]{'YES' if ema_fail else 'NO'}[/{'red ✗' if ema_fail else 'green ✓'}]"
                    f"  RSI>65=[{'red ✗' if rsi_fail else 'green ✓'}]{'YES' if rsi_fail else 'NO'}[/{'red ✗' if rsi_fail else 'green ✓'}]"
                    f"  MACD<0=[{'red ✗' if macd_fail else 'green ✓'}]{'YES' if macd_fail else 'NO'}[/{'red ✗' if macd_fail else 'green ✓'}]"
                )
                if would_block:
                    lines.append("  [red bold]→ Would have been BLOCKED by preflight (downtrend + bearish signals)[/red bold]")
                else:
                    lines.append("  [green]→ Passed preflight — GPT call proceeded[/green]")

            elif zone_type == "resistance":
                ema_fail  = e21 > e50
                rsi_fail  = r14 < 35
                macd_fail = mh > 0
                would_block = ema_fail and rsi_fail and macd_fail
                lines.append(
                    f"  Preflight (resistance→SELL):"
                    f"  EMA21>EMA50=[{'red ✗' if ema_fail else 'green ✓'}]{'YES' if ema_fail else 'NO'}[/{'red ✗' if ema_fail else 'green ✓'}]"
                    f"  RSI<35=[{'red ✗' if rsi_fail else 'green ✓'}]{'YES' if rsi_fail else 'NO'}[/{'red ✗' if rsi_fail else 'green ✓'}]"
                    f"  MACD>0=[{'red ✗' if macd_fail else 'green ✓'}]{'YES' if macd_fail else 'NO'}[/{'red ✗' if macd_fail else 'green ✓'}]"
                )
                if would_block:
                    lines.append("  [red bold]→ Would have been BLOCKED by preflight (uptrend + bullish signals)[/red bold]")
                else:
                    lines.append("  [green]→ Passed preflight — GPT call proceeded[/green]")
        else:
            lines.append("  [dim]Indicator data not available for this signal[/dim]")

        sections.append("\n".join(lines))
        sections.append("[dim]" + "─" * 60 + "[/dim]")

    return Panel(
        "\n".join(sections),
        title="[bold]Preflight Filter — Indicator Gate Logic Exposed[/bold]",
        border_style="magenta",
    )


def build_recent_log() -> Panel:
    try:
        with open(LOG_PATH, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = [ln.rstrip() for ln in lines[-20:] if ln.strip()]
    except FileNotFoundError:
        return Panel(f"[dim]Log not found: {LOG_PATH}[/dim]",
                     title="[bold]Bot Log (last 20 lines)[/bold]", border_style="dim")
    except Exception as e:
        return Panel(f"[red]Error reading log: {e}[/red]",
                     title="[bold]Bot Log[/bold]", border_style="dim")

    colored = []
    for line in tail[-15:]:
        if "ERROR" in line or "Exception" in line:
            colored.append(f"[red]{line}[/red]")
        elif "WARNING" in line or "WARN" in line:
            colored.append(f"[yellow]{line}[/yellow]")
        elif "approved=True" in line or "APPROVED" in line or "signal published" in line.lower():
            colored.append(f"[green]{line}[/green]")
        elif "rejected" in line.lower() or "rejected" in line or "REJECTED" in line:
            colored.append(f"[red dim]{line}[/red dim]")
        elif "AnalysisAgent" in line or "RiskAgent" in line:
            colored.append(f"[cyan]{line}[/cyan]")
        else:
            colored.append(f"[dim]{line}[/dim]")

    return Panel(
        "\n".join(colored),
        title=f"[bold]Bot Log — {LOG_PATH}[/bold]",
        border_style="dim",
    )


# ── Main render ──────────────────────────────────────────────────────────────

def render_all():
    layout = Layout()

    top_panels = Columns([
        build_event_funnel(),
        build_agent_health(),
        build_pnl(),
    ], expand=True)

    return "\n".join([
        str(build_header()),
        str(top_panels),
        str(build_active_zones()),
        str(build_recent_touches()),
        str(build_latest_signals()),
        str(build_preflight_analysis()),
        str(build_risk_details()),
        str(build_trades()),
        str(build_validation_log()),
        str(build_recent_log()),
    ])


def main():
    refresh_sec = 3
    console.print(f"[bold cyan]Debug Panel starting...[/bold cyan] Refreshing every {refresh_sec}s. Press Ctrl+C to exit.")
    time.sleep(0.5)

    with Live(console=console, refresh_per_second=1, screen=True) as live:
        while True:
            try:
                panels = [
                    build_header(),
                    Columns([build_event_funnel(), build_agent_health(), build_pnl()], expand=True),
                    build_active_zones(),
                    build_recent_touches(),
                    build_latest_signals(),
                    build_preflight_analysis(),
                    build_risk_details(),
                    build_trades(),
                    build_validation_log(),
                    build_recent_log(),
                ]
                from rich.console import Group
                live.update(Group(*panels))
            except Exception as e:
                live.update(Panel(f"[red]Render error: {e}[/red]"))
            time.sleep(refresh_sec)


if __name__ == "__main__":
    main()
