"""
dashboard/app.py — Read-only Flask observability dashboard for the trading bot.

Runs on port 5001 alongside the bot. NEVER writes to the database.

# ─── DB tables and columns read (SELECT-only) ────────────────────────────────
#
# events  (id, event_type, symbol, payload, created_at)
#   event_type values queried:
#     'zone_touch_event'       — zone-touch count (funnel), price_watcher health
#     'signal_generated_event' — analysis_agent health
#     'zones_refreshed_event'  — sr_mapper health (primary)
#     'zone_event'             — sr_mapper health (fallback)
#     'trade_closed_event'     — trade_monitor health
#
# signals  (id, symbol, direction, entry, stop_loss, take_profit,
#           confidence, reasoning, zone_id,
#           ema21, ema50, rsi14, macd_line, macd_signal, macd_hist,
#           created_at)
#   — funnel GPT-signal counts; R:R source for rejection details panel
#
# risk_decisions  (id, signal_id, symbol, direction, approved, reason,
#                  volume, rr_ok, max_trades_ok, correlation_ok,
#                  daily_loss_ok, weekly_loss_ok, created_at)
#   — funnel approved/rejected counts; risk rejections panel
#
# trades  (id, symbol, direction, volume, entry, stop_loss, take_profit,
#          order_id, fill_price, success, sl_tp_ok, error_msg, dry_run,
#          created_at, close_price, close_time, realized_pnl)
#   — funnel executed count; P&L sums; recent trades table
#
# ─────────────────────────────────────────────────────────────────────────────
"""

import logging
import re
import sys
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("dashboard")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Pull risk limits and DB path from bot config; fall back to safe defaults.
try:
    import config as _cfg
    DB_PATH = str(ROOT / _cfg.DB_PATH)
    DAILY_LOSS_LIMIT = float(_cfg.DAILY_LOSS_LIMIT_USD)
    DAILY_PROFIT_TARGET = float(_cfg.DAILY_PROFIT_TARGET_USD)
except Exception as exc:
    logger.warning("Could not import bot config (%s) — using defaults", exc)
    DB_PATH = str(ROOT / "trading_bot.db")
    DAILY_LOSS_LIMIT = 30.0
    DAILY_PROFIT_TARGET = 100.0

LOG_PATH = str(ROOT / "trading_bot.log")
# Matches "| INFO     |", "| ERROR    |", etc. in trading_bot.log lines.
_LOG_LEVEL_RE = re.compile(r"\|\s*(ERROR|WARNING|INFO|DEBUG)\s*\|")

app = Flask(__name__)


# ── DB helpers (read-only) ───────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    """Open a SQLite connection in query-only mode (prevents any writes)."""
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA query_only = ON")
    c.row_factory = sqlite3.Row
    return c


def _query(sql: str, params: tuple = ()) -> list:
    """SELECT → list of dicts. Returns [] on any error (DB not found, etc.)."""
    try:
        c = _conn()
        rows = [dict(r) for r in c.execute(sql, params).fetchall()]
        c.close()
        return rows
    except Exception as exc:
        logger.debug("Query failed: %s", exc)
        return []


def _scalar(sql: str, params: tuple = (), default=None):
    """SELECT returning one value. Returns default on any error."""
    try:
        c = _conn()
        row = c.execute(sql, params).fetchone()
        c.close()
        return (row[0] if row[0] is not None else default) if row else default
    except Exception as exc:
        logger.debug("Scalar failed: %s", exc)
        return default


def _today() -> str:
    return datetime.now(tz=timezone.utc).date().isoformat()


def _week_start() -> str:
    now = datetime.now(tz=timezone.utc)
    return (now - timedelta(days=now.weekday())).date().isoformat()


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/funnel")
def api_funnel():
    """Signal pipeline counts — zone touches → GPT signals → risk OK → executed."""
    today = _today()

    def cnt(sql, params=()):
        return int(_scalar(sql, params, default=0) or 0)

    return jsonify({
        "zone_touches": {
            "today": cnt("SELECT COUNT(*) FROM events WHERE event_type='zone_touch_event' AND DATE(created_at)=?", (today,)),
            "total": cnt("SELECT COUNT(*) FROM events WHERE event_type='zone_touch_event'"),
        },
        "signals": {
            "today": cnt("SELECT COUNT(*) FROM signals WHERE DATE(created_at)=?", (today,)),
            "total": cnt("SELECT COUNT(*) FROM signals"),
        },
        "risk_approved": {
            "today": cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=1 AND DATE(created_at)=?", (today,)),
            "total": cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=1"),
        },
        "risk_rejected": {
            "today": cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=0 AND DATE(created_at)=?", (today,)),
            "total": cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=0"),
        },
        "executed": {
            "today": cnt("SELECT COUNT(*) FROM trades WHERE success=1 AND dry_run=0 AND DATE(created_at)=?", (today,)),
            "total": cnt("SELECT COUNT(*) FROM trades WHERE success=1 AND dry_run=0"),
        },
        "exec_failed": {
            "today": cnt("SELECT COUNT(*) FROM trades WHERE success=0 AND dry_run=0 AND DATE(created_at)=?", (today,)),
            "total": cnt("SELECT COUNT(*) FROM trades WHERE success=0 AND dry_run=0"),
        },
    })


@app.route("/api/pnl")
def api_pnl():
    """Realized P&L totals and proximity to daily limits."""
    today = _today()
    week_start = _week_start()

    def pnl_sum(where_extra, params):
        return float(_scalar(
            "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades "
            "WHERE dry_run=0 AND close_time IS NOT NULL AND realized_pnl IS NOT NULL "
            + where_extra,
            params,
            default=0.0,
        ) or 0.0)

    pnl_today = pnl_sum("AND DATE(close_time)=?", (today,))
    pnl_week  = pnl_sum("AND DATE(close_time)>=?", (week_start,))
    pnl_all   = pnl_sum("", ())

    loss_used_pct   = abs(min(pnl_today, 0.0)) / DAILY_LOSS_LIMIT   * 100 if pnl_today < 0 else 0.0
    profit_used_pct = max(pnl_today, 0.0)       / DAILY_PROFIT_TARGET * 100 if pnl_today > 0 else 0.0

    return jsonify({
        "today":               round(pnl_today, 2),
        "week":                round(pnl_week, 2),
        "all_time":            round(pnl_all, 2),
        "daily_loss_limit":    DAILY_LOSS_LIMIT,
        "daily_profit_target": DAILY_PROFIT_TARGET,
        "loss_used_pct":       round(min(loss_used_pct, 100.0), 1),
        "profit_used_pct":     round(min(profit_used_pct, 100.0), 1),
        "warn_loss":  pnl_today <= -(DAILY_LOSS_LIMIT * 0.7),
        "hit_loss":   pnl_today <= -DAILY_LOSS_LIMIT,
        "warn_target": pnl_today >= DAILY_PROFIT_TARGET * 0.8,
        "hit_target":  pnl_today >= DAILY_PROFIT_TARGET,
    })


@app.route("/api/health")
def api_health():
    """Last-seen timestamp for each agent, derived from their event types."""
    def last(*event_types):
        ph = ",".join("?" * len(event_types))
        return _scalar(
            f"SELECT MAX(created_at) FROM events WHERE event_type IN ({ph})",
            tuple(event_types),
        )

    return jsonify({
        "sr_mapper":      last("zones_refreshed_event", "zone_event"),
        "price_watcher":  last("zone_touch_event"),
        "analysis_agent": last("signal_generated_event"),
        "trade_monitor":  last("trade_closed_event"),
    })


@app.route("/api/trades")
def api_trades():
    """25 most recent trade rows, newest first."""
    return jsonify(_query(
        """
        SELECT id, symbol, direction, volume, entry, stop_loss, take_profit,
               order_id, fill_price, success, sl_tp_ok, error_msg, dry_run,
               created_at, close_price, close_time, realized_pnl
        FROM trades
        ORDER BY created_at DESC
        LIMIT 25
        """
    ))


@app.route("/api/rejections")
def api_rejections():
    """25 most recent risk-gate rejections with computed R:R ratio."""
    rows = _query(
        """
        SELECT rd.id, rd.symbol, rd.direction, rd.reason, rd.created_at,
               rd.rr_ok, rd.max_trades_ok, rd.correlation_ok,
               rd.daily_loss_ok, rd.weekly_loss_ok,
               s.entry, s.stop_loss, s.take_profit
        FROM risk_decisions rd
        LEFT JOIN signals s ON rd.signal_id = s.id
        WHERE rd.approved = 0
        ORDER BY rd.created_at DESC
        LIMIT 25
        """
    )
    for r in rows:
        try:
            entry = r.get("entry")
            sl    = r.get("stop_loss")
            tp    = r.get("take_profit")
            if entry is not None and sl is not None and tp is not None:
                risk   = abs(float(entry) - float(sl))
                reward = abs(float(tp)    - float(entry))
                r["rr_ratio"] = round(reward / risk, 2) if risk > 0 else None
            else:
                r["rr_ratio"] = None
        except Exception:
            r["rr_ratio"] = None
    return jsonify(rows)


@app.route("/api/logs")
def api_logs():
    """Last 100 log lines from trading_bot.log, tagged by log level."""
    try:
        with open(LOG_PATH, encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        raw_tail = [ln.rstrip() for ln in all_lines[-200:] if ln.strip()]
        result = []
        current_level = "OTHER"
        for line in raw_tail:
            m = _LOG_LEVEL_RE.search(line)
            if m:
                current_level = m.group(1)
                level = current_level
            elif (
                line.startswith((" ", "\t"))
                or line.startswith("Traceback")
                or 'File "' in line[:40]
                or line.startswith("    ")
            ):
                level = current_level  # traceback lines inherit the preceding ERROR level
            else:
                level = "OTHER"
                current_level = "OTHER"
            result.append({"text": line, "level": level})
        return jsonify({"lines": result[-100:], "path": LOG_PATH})
    except FileNotFoundError:
        return jsonify({"lines": [{"text": f"Log not found: {LOG_PATH}", "level": "ERROR"}], "path": LOG_PATH})
    except Exception as exc:
        return jsonify({"lines": [{"text": f"Error reading log: {exc}", "level": "ERROR"}], "path": LOG_PATH})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
