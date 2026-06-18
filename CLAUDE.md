# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the bot

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and fill in credentials
copy .env.example .env

# Dry-run (no real orders placed)
# Set EXECUTION_LIVE=False in .env, then:
python main.py

# Live mode
# Set EXECUTION_LIVE=True in .env, then:
python main.py

# Run the observability dashboard (separate terminal, port 5001)
cd dashboard
pip install -r requirements.txt
python app.py
```

Logs go to both stdout and `trading_bot.log` (rotating, 5 MB max, 5 backups). The SQLite DB is `trading_bot.db`.

---

## Symbols & Timeframes

### Symbols
`EURUSD`, `XAUUSD` — configured in `config.SYMBOLS`.  
All agents loop over this list. Never hardcode a symbol anywhere.  
USDJPY is retained in `TICK_VALUE_FALLBACK` and `CORRELATED_PAIRS` but is **not** in the active `SYMBOLS` list.

### Timeframes
M5 and M15 only. All candle fetches, S/R scans, and indicator calculations are restricted to these two timeframes.  
MT5 timeframe keys: `"M5"`, `"M15"` — do **not** use `"5m"`, `"1h"`, or any other format.

### Concurrent Trade Limit
`MAX_OPEN_TRADES = 1` — account-wide across all symbols combined.

---

## Architecture

An event-driven, multi-threaded trading bot for MetaTrader 5 targeting EURUSD and XAUUSD on M5 and M15 timeframes. The pipeline is:

```
SRMapper ──zones (per symbol)──▶ PriceWatcher ──ZoneTouchEvent(symbol)──▶ AnalysisAgent (GPT)
                                                                                    │
                                                                          SignalGeneratedEvent(symbol)
                                                                                    │
                                                                               RiskAgent
                                                                                    │
                                                                          RiskEvaluatedEvent(symbol)
                                                                                    │
                                                                               Executor ──▶ MT5
                                                                                    │
                                                                          TradeExecutedEvent(symbol)
                                                                                    │
                                                                            TradeMonitor ──▶ DB
```

All inter-agent communication is through `core/event_bus.py` (in-process pub/sub). `core/db_consumer.py` subscribes to all event types and is the **only** DB writer — other agents only call read helpers on `Database`. The exception is `sr_mapper`, which calls `deactivate_zones_before()` directly on refresh (documented in-code).

### Threads

- `SRMapper` — scans S/R zones for all symbols on both M5 and M15 on startup; refreshes every 4 hours; signals `zones_ready` only after all symbols are done
- `PriceWatcher` — waits for `zones_ready`, then polls prices for all symbols every `TICK_INTERVAL_SEC` (default 2s) in a single loop
- `AnalysisAgent` — has its own queue + thread (max 10 events) so GPT API calls (2–10s) never block the tick loop
- `TradeMonitor` — polls MT5 every 30s for closed positions, emits `TradeClosedEvent`
- `RiskAgent` and `Executor` are synchronous event handlers (no dedicated threads)

The main thread runs a watchdog loop (every 60s) that reconnects MT5 if the connection drops and restarts any dead agent threads. Watchdog stale thresholds: SRMapper 150s, PriceWatcher `TICK_INTERVAL_SEC × 5`, AnalysisAgent 60s, TradeMonitor 90s.

### Key files

| File | Role |
|---|---|
| `config.py` | All tunable parameters — instruments, risk limits, zone settings, MT5 config, signal tracker thresholds |
| `core/events.py` | Pydantic schemas for every event type — all events carry a `symbol` field |
| `core/event_bus.py` | Thread-safe pub/sub; handlers run synchronously on publisher's thread |
| `core/database.py` | SQLite layer; read helpers for agents, write helpers for db_consumer only; zones table has `symbol` column |
| `core/db_consumer.py` | Sole DB writer; subscribes to all events |
| `core/notifier.py` | Optional Telegram alert helper; reads `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` from env |
| `core/analytics.py` | `AnalyticsEngine` — read-only performance report from `validation_log` (win rate, profit factor, pipeline conversion, per-symbol breakdown) |
| `core/signal_tracker.py` | Rolling win-rate tracker; pauses new signals when win rate falls below threshold |
| `indicators/calculator.py` | Pure pandas/numpy functions (EMA, RSI, MACD, swing detection, zone clustering) — no MT5 dependency |
| `agents/sr_mapper.py` | Loops all symbols; detects swing highs/lows on M5+M15; clusters into zones tagged with `symbol`; writes to DB via event bus |
| `agents/price_watcher.py` | Tick loop over all symbols; checks each symbol's price against that symbol's zones only; emits `ZoneTouchEvent(symbol)` |
| `agents/analysis_agent.py` | Queues `ZoneTouchEvent`s; pre-checks open trades; applies M15 trend pre-filter; calls GPT; emits `SignalGeneratedEvent` |
| `agents/risk_agent.py` | 5 risk checks; computes lot size per symbol using pip/point-value formula; emits `RiskEvaluatedEvent` |
| `agents/executor.py` | Places MT5 market orders (two-step: place then modify SL/TP) |
| `agents/trade_monitor.py` | Detects closed positions; records realized P&L |
| `prompts/system_prompt.txt` | GPT system prompt — forex/gold context, M5+M15 confluence logic, JSON output |
| `prompts/user_template.py` | Builds per-signal user prompt with M5 indicators + candles and M15 indicators + candles for the triggered symbol |
| `dashboard/app.py` | Read-only Flask observability UI on port 5001 |
| `scripts/analytics_report.py` | CLI wrapper for `AnalyticsEngine.print_report()` |
| `scripts/backtest_runner.py` | Offline backtest runner |
| `scripts/check_connection.py` | MT5 connectivity smoke test |
| `test_lot_sizing.py` | Offline unit tests for RiskAgent lot-sizing logic (12 cases, no MT5 required) |

---

## MT5 API (metatrader-mcp-server v0.5.1)

Methods live on **sub-clients**, not on `MT5Client` directly:

```python
client.market.get_candles_latest(symbol, timeframe, count=100)  # → pd.DataFrame newest-first
client.market.get_symbol_price(symbol)                          # → {bid, ask, last, volume, time}
client.market.get_symbol_info(symbol)                           # → {tick_value, tick_size, volume_min, volume_max, volume_step}
client.order.place_market_order(type=dir, symbol=sym, volume=v) # → {error, message, data}
client.order.modify_position(pos_id, stop_loss=sl, take_profit=tp)
client.order.get_all_positions()                                # → pd.DataFrame
client.account.get_balance() / get_equity()                     # → float
client.account.get_account_info()                               # → dict
```

**Critical constraints:**
- SL/TP requires two steps: `place_market_order` (returns position id in `data.order`) then `modify_position`
- Timeframe strings must be MT5 keys: `M5`, `M15`, `H1`, `H4`, `D1` — **not** `5m`/`1h` etc.
- Candle DataFrames are **newest-first**; always call `sort_candles_ascending()` from `indicators/calculator.py` before computing EMA/RSI/MACD
- Symbol names may need a broker suffix (e.g. `.r`); use `config.resolve_symbol()` before any MT5 call
- `get_symbol_info(symbol)` returns `tick_value`, `tick_size`, `volume_min`, `volume_max`, `volume_step` — used by RiskAgent for lot sizing

---

## Risk gate (RiskAgent)

A signal is rejected if any check fails:

1. R:R ratio `< MIN_RR` (default 2)
2. Open trades `>= MAX_OPEN_TRADES` (default 1) — account-wide across all symbols
3. Correlated pairs both open — EURUSD and USDJPY are correlated; configure in `CORRELATED_PAIRS`
4. Today's realized P&L `<= -DAILY_LOSS_LIMIT_USD` (default −$30)
5. Today's realized P&L `>= DAILY_PROFIT_TARGET_USD` (default $100)

### Lot sizing
Lot size is computed per symbol using pip/point value:

```python
sl_distance = abs(entry - sl)
tick_value  = symbol_info["trade_tick_value"]
tick_size   = symbol_info["trade_tick_size"]
lot = RISK_PER_TRADE_USD / ((tick_value / tick_size) * sl_distance)
lot = clamp(lot, volume_min, volume_max)
lot = round_to_step(lot, volume_step)
```

`RISK_PER_TRADE_USD` defaults to $10. If the risk-correct lot is below the broker minimum (`volume_min`), the trade is **rejected** rather than clamped up to the minimum — clamping would place a position risking more than `RISK_PER_TRADE_USD`. Rejections are logged with a distinct `SUB-MIN LOT REJECT` tag for per-symbol auditing.

Fallback tick values if `get_symbol_info` is unavailable (chosen to match the live broker's observed `tick_value/tick_size` ratio):
- EURUSD: `tick_value=1.0`, `tick_size=0.00001` (ratio 100,000)
- USDJPY: `tick_value=0.62`, `tick_size=0.001` (ratio ≈620)
- XAUUSD: `tick_value=0.1`, `tick_size=0.01` (ratio 10)

Weekly loss check: `weekly_loss_ok` field is hardcoded `True` (weekly P&L gate is disabled); it exists in `RiskEvaluatedEvent` for audit-log consistency only and has no effect on trade approval.

---

## GPT Analysis (AnalysisAgent)

For each `ZoneTouchEvent`, the agent processes as follows:

**Step 0 — Pre-check open trades** (before any GPT call): if `open_count >= MAX_OPEN_TRADES`, the event is dropped immediately without fetching candles or calling GPT.

**Step 1–4 — Fetch and compute:**
1. Fetch 100 M5 candles + 100 M15 candles for `event.symbol`
2. Compute EMA 21/50, RSI 14, MACD(12,26,9) on both timeframes
3. Build 5-bar candle snapshots for each timeframe

**Step 5a — M15 trend pre-filter** (saves GPT tokens): skip GPT call if M15 EMA21 vs EMA50 contradicts the zone type:
- Support zone + M15 bearish (EMA21 < EMA50) → skip
- Resistance zone + M15 bullish (EMA21 > EMA50) → skip

**Step 5b–7 — GPT call and validation:**
- Builds user prompt with M15 trend data + M5 entry data + zone details
- Calls GPT with `response_format=json_object`, `max_tokens=150`; retries up to 2× on network errors (exponential backoff: 2s, 4s); JSON parse errors are not retried
- Validates price geometry: BUY requires `SL < entry < TP`, SELL requires `TP < entry < SL`
- If `direction == "NONE"` or geometry is invalid, no event is emitted

**GPT JSON output schema:**
```json
{
  "direction": "BUY" | "SELL" | "NONE",
  "sl": 1.08450,
  "tp": 1.09100,
  "confidence": 7,
  "reason": "M15 bullish trend, M5 bounce confirmed at support"
}
```

Per-zone cooldown: 30s — the same (symbol, zone_id) pair is ignored for 30s after analysis to avoid rapid re-analysis.

Default model: `gpt-4o-mini` (override with `OPENAI_MODEL` env var).

---

## Signal Tracker (SignalTracker)

`core/signal_tracker.py` — thread-safe rolling win-rate tracker.

- `SIGNAL_TRACKER_WINDOW = 20` — number of recent closed trades to evaluate
- `SIGNAL_PAUSE_THRESHOLD = 0.40` — pause new signals if win rate drops below 40%
- `record_result(signal_id, won)` — called by TradeMonitor when a position closes
- `should_pause` property — returns `True` only after the window is full and win rate < threshold
- Returns `win_rate = 1.0` if no results yet (never blocks during warm-up)

---

## Notifier (Telegram)

`core/notifier.py` — optional Telegram alert helper. Reads `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` from environment. If either is missing, `send()` logs a WARNING and returns silently — no crash.

---

## Analytics Engine

`core/analytics.py` — read-only performance report from the `validation_log` table.

```python
from core.database import Database
from core.analytics import AnalyticsEngine

db = Database()
engine = AnalyticsEngine(db)
report = engine.compute_report()
engine.print_report(report)
```

Report sections: `summary` (total P&L, trade counts, avg duration), `performance` (win rate, avg R:R, profit factor, expectancy), `pipeline` (conversion rates: zone touch → analysis → signal → risk approved → executed), `by_zone_type`, `by_zone_strength` (weak 1–2, moderate 3–4, strong 5+), `by_timeframe`, `by_symbol`, `risk_gate_stats` (approval rate, top rejection reasons).

Run from CLI: `python scripts/analytics_report.py`

---

## Dashboard (Flask, port 5001)

`dashboard/app.py` — read-only observability UI. Opens a SQLite connection with `PRAGMA query_only = ON` — it cannot write to the DB.

```bash
cd dashboard && python app.py
# Browse to http://localhost:5001
```

**Routes:**
- `/` — main dashboard (signal funnel, P&L gauges, recent trades)
- `/debug` — debug panel (zones, touches, signals with full indicator/geometry details)
- `/visualizer` — live candlestick chart (fetches candles from MT5 directly)

**API endpoints (JSON):**
- `/api/funnel` — zone touches → signals → risk approved → executed counts (today + all-time)
- `/api/pnl` — today/week/all-time realized P&L, proximity to daily loss/profit limits
- `/api/health` — last-seen timestamp per agent (derived from event types)
- `/api/trades` — 25 most recent trade rows
- `/api/rejections` — 25 most recent risk-gate rejections with computed R:R
- `/api/logs` — last 100 lines from `trading_bot.log`, tagged by log level
- `/api/debug/zones` — all active zones with touch counts
- `/api/debug/touches` — recent zone-touch events with proximity %
- `/api/debug/signals` — full signal details: indicators, GPT output, geometry check, preflight state
- `/api/debug/risk` — risk decisions with all check booleans
- `/api/debug/events` — raw event audit log (last 50)
- `/api/debug/summary` — full system state snapshot (config, zones, today's counts, agent last-seen)

---

## Environment variables

See `.env.example`. Required: `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER`, `OPENAI_API_KEY`.  
Optional: `SYMBOL_SUFFIX`, `OPENAI_MODEL` (default `gpt-4o-mini`), `EXECUTION_LIVE` (default `True`), `DB_PATH`, `TICK_INTERVAL_SEC`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

---

## Testing without MT5

`indicators/calculator.py` has no MT5 dependency and can be unit-tested offline:

```python
python -c "from indicators.calculator import compute_all_indicators; print('OK')"
```

For lot-sizing logic, run the dedicated test (no MT5 required, 12 cases):

```bash
python test_lot_sizing.py
```

For core infrastructure, use `Database(":memory:")` to avoid touching the real DB file.
