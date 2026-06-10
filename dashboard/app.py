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

import json
import logging
import re
import sys
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template, request

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



# ── Debug / Transparency routes ──────────────────────────────────────────────

@app.route("/debug")
def debug_page():
    return render_template("debug.html")


@app.route("/visualizer")
def visualizer_page():
    return render_template("visualizer.html")


@app.route("/api/chart/<symbol>/<timeframe>")
def api_chart(symbol, timeframe):
    """
    Fetch historical candlestick data from MT5 for a symbol and timeframe.
    Returns JSON list of: { time, open, high, low, close }
    """
    try:
        from metatrader_client import MT5Client
        import config as _cfg
        
        count = int(request.args.get("count", 150))
        resolved = _cfg.resolve_symbol(symbol)
        
        client = MT5Client(_cfg.MT5_CONFIG)
        connected = client.connect()
        if not connected:
            return jsonify({"error": "Failed to connect to MT5"}), 500
        
        df = client.market.get_candles_latest(resolved, timeframe, count=count)
        client.disconnect()
        
        if df is None or len(df) == 0:
            return jsonify({"error": f"No candles found for {resolved} in timeframe {timeframe}"}), 404
            
        # Re-sort to ascending (oldest first)
        df_sorted = df.sort_values("time", ascending=True).reset_index(drop=True)
        
        candles = []
        for _, row in df_sorted.iterrows():
            t = row["time"]
            if hasattr(t, "timestamp"):
                t_sec = int(t.timestamp())
            elif isinstance(t, str):
                try:
                    dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                    t_sec = int(dt.timestamp())
                except ValueError:
                    t_sec = int(t)
            else:
                t_sec = int(t)
                
            candles.append({
                "time": t_sec,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"])
            })
            
        return jsonify({
            "symbol": symbol,
            "resolved": resolved,
            "timeframe": timeframe,
            "candles": candles
        })
    except Exception as exc:
        logger.exception("Failed in api_chart: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/debug/zones")
def api_debug_zones():
    """All active zones with full price details, width %, and touch history."""
    zones = _query(
        """
        SELECT z.*,
               (SELECT COUNT(*) FROM events e
                WHERE e.event_type='zone_touch_event'
                  AND e.symbol=z.symbol
                  AND json_extract(e.payload,'$.zone_id')=z.id) AS touch_count,
               (SELECT MAX(e.created_at) FROM events e
                WHERE e.event_type='zone_touch_event'
                  AND e.symbol=z.symbol
                  AND json_extract(e.payload,'$.zone_id')=z.id) AS last_touch
        FROM zones z
        WHERE z.is_active=1
        ORDER BY z.symbol, z.price_center
        """
    )
    for z in zones:
        c = z.get("price_center", 0) or 1
        u = z.get("price_upper", 0) or 0
        l = z.get("price_lower", 0) or 0
        z["width_pct"] = round((u - l) / c * 100, 4) if c else 0
    return jsonify(zones)


@app.route("/api/debug/touches")
def api_debug_touches():
    """Recent zone-touch events with full payload decoded."""
    rows = _query(
        """
        SELECT id, symbol, payload, created_at
        FROM events
        WHERE event_type='zone_touch_event'
        ORDER BY created_at DESC
        LIMIT 30
        """
    )
    result = []
    for r in rows:
        try:
            payload = json.loads(r.get("payload") or "{}")
        except Exception:
            payload = {}
        center    = payload.get("price_center", 0)
        mid_price = payload.get("mid_price", payload.get("bid", 0))
        prox_pct  = None
        if center and mid_price and float(center) > 0:
            prox_pct = round(abs(float(mid_price) - float(center)) / float(center) * 100, 4)
        result.append({
            "id":         r["id"],
            "symbol":     r["symbol"],
            "created_at": r["created_at"],
            "zone_id":    payload.get("zone_id"),
            "zone_type":  payload.get("zone_type"),
            "timeframe":  payload.get("timeframe"),
            "zone_strength": payload.get("zone_strength", payload.get("strength")),
            "price_center":  center,
            "mid_price":     mid_price,
            "bid":           payload.get("bid"),
            "ask":           payload.get("ask"),
            "proximity_pct": prox_pct,
        })
    return jsonify(result)


@app.route("/api/debug/signals")
def api_debug_signals():
    """Full signal details: indicators, GPT output, preflight state, geometry check."""
    rows = _query(
        """
        SELECT s.*,
               rd.approved, rd.reason as risk_reason, rd.volume,
               rd.rr_ok, rd.max_trades_ok, rd.correlation_ok,
               rd.daily_loss_ok, rd.weekly_loss_ok,
               vl.zone_type, vl.zone_strength, vl.trade_result,
               vl.fill_price, vl.close_price, vl.realized_pnl,
               vl.risk_approved, vl.order_id
        FROM signals s
        LEFT JOIN risk_decisions rd ON rd.signal_id = s.id
        LEFT JOIN validation_log vl ON vl.signal_id = s.id
        ORDER BY s.created_at DESC
        LIMIT 20
        """
    )
    for r in rows:
        entry  = float(r.get("entry")      or 0)
        sl     = float(r.get("stop_loss")  or 0)
        tp     = float(r.get("take_profit") or 0)
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        r["rr_ratio"]  = round(reward / risk, 3) if risk > 0 else None
        r["rr_needed"] = DAILY_LOSS_LIMIT  # expose config
        r["min_confidence"] = float(_cfg.MIN_CONFIDENCE) if hasattr(_cfg, "MIN_CONFIDENCE") else 65.0

        # EMA trend
        ema21 = r.get("ema21")
        ema50 = r.get("ema50")
        if ema21 and ema50:
            r["ema_trend"] = "uptrend" if float(ema21) > float(ema50) else "downtrend"
        else:
            r["ema_trend"] = None

        # RSI state
        rsi = r.get("rsi14")
        if rsi:
            rsi = float(rsi)
            r["rsi_state"] = "overbought" if rsi > 70 else "oversold" if rsi < 30 else "neutral"
        else:
            r["rsi_state"] = None

        # MACD state
        mh = r.get("macd_hist")
        r["macd_state"] = ("bullish" if float(mh) > 0 else "bearish") if mh is not None else None

        # Preflight logic
        ema21v = r.get("ema21")
        ema50v = r.get("ema50")
        rsi14v = r.get("rsi14")
        mhv    = r.get("macd_hist")
        zt     = (r.get("zone_type") or "").lower()
        preflight = {"zone_type": zt, "would_block": False, "reason": None}
        if None not in (ema21v, ema50v, rsi14v, mhv):
            e21 = float(ema21v); e50 = float(ema50v)
            r14 = float(rsi14v); mh2 = float(mhv)
            if zt == "support" and e21 < e50 and r14 > 65 and mh2 < 0:
                preflight["would_block"] = True
                preflight["reason"] = f"downtrend (EMA21<EMA50), not oversold (RSI={r14:.1f}>65), bearish MACD"
            elif zt == "resistance" and e21 > e50 and r14 < 35 and mh2 > 0:
                preflight["would_block"] = True
                preflight["reason"] = f"uptrend (EMA21>EMA50), not overbought (RSI={r14:.1f}<35), bullish MACD"
        r["preflight"] = preflight

        # Geometry check
        direction = r.get("direction", "")
        if direction == "BUY":
            r["geometry_ok"] = bool(sl < entry < tp) if all([sl, entry, tp]) else None
        elif direction == "SELL":
            r["geometry_ok"] = bool(tp < entry < sl) if all([sl, entry, tp]) else None
        else:
            r["geometry_ok"] = None

        # Lot size formula
        if entry > 0:
            try:
                fixed = float(_cfg.FIXED_TRADE_USD) if hasattr(_cfg, "FIXED_TRADE_USD") else 10.0
                r["lot_formula"] = {
                    "fixed_usd":  fixed,
                    "entry":      round(entry, 5),
                    "raw_lot":    round(fixed / entry, 8),
                    "note":       "raw_lot then floored to vol_step and clamped to [vol_min, vol_max]",
                }
            except Exception:
                r["lot_formula"] = None
        else:
            r["lot_formula"] = None

    return jsonify(rows)


@app.route("/api/debug/risk")
def api_debug_risk():
    """Risk decisions with all check booleans + exact calculation details."""
    rows = _query(
        """
        SELECT rd.*,
               s.entry, s.stop_loss, s.take_profit, s.confidence,
               s.ema21, s.ema50, s.rsi14, s.macd_hist,
               s.direction as sig_direction
        FROM risk_decisions rd
        LEFT JOIN signals s ON rd.signal_id = s.id
        ORDER BY rd.created_at DESC
        LIMIT 30
        """
    )
    for r in rows:
        entry  = float(r.get("entry")      or 0)
        sl     = float(r.get("stop_loss")  or 0)
        tp     = float(r.get("take_profit") or 0)
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        rr     = reward / risk if risk > 0 else 0
        r["rr_calculated"] = round(rr, 4)
        r["rr_threshold"]  = DAILY_LOSS_LIMIT  # note: this is MIN_RR actually
        try:
            r["rr_threshold"] = float(_cfg.MIN_RR)
        except Exception:
            pass
        r["rr_pass"] = rr >= (r["rr_threshold"] - 1e-9)

        vol = float(r.get("volume") or 0)
        if vol > 0 and entry > 0:
            r["notional_usd"] = round(vol * entry, 2)
        else:
            r["notional_usd"] = None
    return jsonify(rows)


@app.route("/api/debug/events")
def api_debug_events():
    """Recent 50 events with full payload, decoded."""
    rows = _query(
        """
        SELECT id, event_type, symbol, payload, created_at
        FROM events
        ORDER BY created_at DESC
        LIMIT 50
        """
    )
    result = []
    for r in rows:
        payload_decoded = {}
        try:
            payload_decoded = json.loads(r.get("payload") or "{}")
        except Exception:
            payload_decoded = {"raw": r.get("payload", "")}
        result.append({
            "id":         r["id"],
            "event_type": r["event_type"],
            "symbol":     r["symbol"],
            "created_at": r["created_at"],
            "payload":    payload_decoded,
        })
    return jsonify(result)


@app.route("/api/debug/summary")
def api_debug_summary():
    """Full system state snapshot in one call."""
    today = _today()
    week_start = _week_start()

    def cnt(sql, params=()):
        return int(_scalar(sql, params, default=0) or 0)

    def last(*types):
        ph = ",".join("?" * len(types))
        return _scalar(f"SELECT MAX(created_at) FROM events WHERE event_type IN ({ph})", tuple(types))

    return jsonify({
        "config": {
            "min_rr":              float(getattr(_cfg, "MIN_RR", 1.5)),
            "min_confidence":      float(getattr(_cfg, "MIN_CONFIDENCE", 65)),
            "max_open_trades":     int(getattr(_cfg, "MAX_OPEN_TRADES", 4)),
            "daily_loss_limit":    DAILY_LOSS_LIMIT,
            "daily_profit_target": DAILY_PROFIT_TARGET,
            "fixed_trade_usd":     float(getattr(_cfg, "FIXED_TRADE_USD", 10)),
            "execution_live":      bool(getattr(_cfg, "EXECUTION_LIVE", False)),
            "symbols":             list(getattr(_cfg, "SYMBOLS", [])),
            "tick_interval_sec":   float(getattr(_cfg, "TICK_INTERVAL_SEC", 2)),
            "breakeven_trigger":   float(getattr(_cfg, "BREAKEVEN_TRIGGER_PCT", 0.6)),
            "trailing_trigger":    float(getattr(_cfg, "TRAILING_TRIGGER_PCT", 0.8)),
        },
        "zones": {
            "active_count": cnt("SELECT COUNT(*) FROM zones WHERE is_active=1"),
            "total_count":  cnt("SELECT COUNT(*) FROM zones"),
        },
        "today": {
            "zone_touches":   cnt("SELECT COUNT(*) FROM events WHERE event_type='zone_touch_event' AND DATE(created_at)=?", (today,)),
            "analyses":       cnt("SELECT COUNT(*) FROM events WHERE event_type='analysis_started_event' AND DATE(created_at)=?", (today,)),
            "signals":        cnt("SELECT COUNT(*) FROM signals WHERE DATE(created_at)=?", (today,)),
            "risk_approved":  cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=1 AND DATE(created_at)=?", (today,)),
            "risk_rejected":  cnt("SELECT COUNT(*) FROM risk_decisions WHERE approved=0 AND DATE(created_at)=?", (today,)),
            "trades_exec":    cnt("SELECT COUNT(*) FROM trades WHERE success=1 AND dry_run=0 AND DATE(created_at)=?", (today,)),
        },
        "agent_last_seen": {
            "sr_mapper":      last("zones_refreshed_event", "zone_event"),
            "price_watcher":  last("zone_touch_event"),
            "analysis_agent": last("signal_generated_event", "analysis_started_event"),
            "risk_agent":     last("risk_evaluated_event"),
            "executor":       last("trade_executed_event"),
            "trade_monitor":  last("trade_closed_event", "breakeven_moved_event", "trailing_updated_event"),
        },
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
