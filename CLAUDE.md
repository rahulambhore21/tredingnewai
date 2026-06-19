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
`XAUUSD` — the only active symbol, configured in `config.SYMBOLS`.  
All agents loop over this list. Never hardcode a symbol anywhere.

### Timeframes
M5 and M15 only. All candle fetches, S/R scans, and indicator calculations are restricted to these two timeframes.  
MT5 timeframe keys: `"M5"`, `"M15"` — do **not** use `"5m"`, `"1h"`, or any other format.

### Concurrent Trade Limit
`MAX_DAILY_TRADES = 2` — per account per day. Each account tracks its own daily trade count independently.

---

## Architecture

An event-driven, multi-threaded trading bot for MetaTrader 5 targeting XAUUSD on M5 and M15 timeframes across 4 MT5 accounts in parallel. The pipeline is:

```
SRMapper ──zones (per symbol)──▶ PriceWatcher ──ZoneTouchEvent(symbol)──┬──▶ AnalysisAgent[acct1] (GPT, BUY-only)
                                   (shared, Account 1 client)            ├──▶ AnalysisAgent[acct2] (GPT, BUY-only)
                                                                          ├──▶ AnalysisAgent[acct3] (GPT, SELL-only)
                                                                          └──▶ AnalysisAgent[acct4] (GPT, SELL-only)
                                                                                        │
                                                                            SignalGeneratedEvent(symbol, account_id)
                                                                                        │
                                                                                   RiskAgent[acctN]
                                                                                        │
                                                                            RiskEvaluatedEvent(symbol, account_id)
                                                                                        │
                                                                               Executor[acctN] ──▶ MT5 (acctN)
                                                                                        │
                                                                            TradeExecutedEvent(symbol, account_id)
                                                                                        │
                                                                              TradeMonitor[acctN] ──▶ DB
```

SRMapper and PriceWatcher are shared singletons using Account 1's MT5 client. Each of the 4 accounts has its own AnalysisAgent, RiskAgent, Executor, and TradeMonitor instance, all running in parallel. All inter-agent communication is through `core/event_bus.py` (in-process pub/sub). `core/db_consumer.py` subscribes to all event types and is the **only** DB writer — other agents only call read helpers on `Database`. The exception is `sr_mapper`, which calls `deactivate_zones_before()` directly on refresh (documented in-code).

### Threads

- `SRMapper` — scans S/R zones for all symbols on both M5 and M15 on startup; refreshes every 4 hours; signals `zones_ready` only after all symbols are done; uses Account 1 MT5 client
- `PriceWatcher` — waits for `zones_ready`, then polls prices for all symbols every `TICK_INTERVAL_SEC` (default 2s) in a single loop; uses Account 1 MT5 client; broadcasts `ZoneTouchEvent` to all 4 account pipelines
- `AnalysisAgent[1–4]` — each has its own queue + thread (max 10 events) so GPT API calls (2–10s) never block the tick loop; each is bound to one `account_config`
- `TradeMonitor[1–4]` — one per account; polls that account's MT5 every 30s for closed positions, emits `TradeClosedEvent`
- `RiskAgent[1–4]` and `Executor[1–4]` are synchronous event handlers (no dedicated threads), one instance per account

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
| `agents/analysis_agent.py` | One instance per `account_config`; queues `ZoneTouchEvent`s; applies direction and daily-count pre-filters; calls GPT; emits `SignalGeneratedEvent` |
| `agents/risk_agent.py` | One instance per `account_config`; direction match + daily trade count checks; fixed lot sizing with clamp; emits `RiskEvaluatedEvent` |
| `agents/executor.py` | One instance per `account_config`; places MT5 market orders on that account (two-step: place then modify SL/TP) |
| `agents/trade_monitor.py` | One instance per `account_config`; detects closed positions on that account; records realized P&L |
| `prompts/system_prompt.txt` | GPT system prompt — gold context, M5+M15 confluence logic, JSON output |
| `prompts/user_template.py` | Builds per-signal user prompt with M5 indicators + candles and M15 indicators + candles for the triggered symbol |
| `dashboard/app.py` | Read-only Flask observability UI on port 5001 |
| `scripts/analytics_report.py` | CLI wrapper for `AnalyticsEngine.print_report()` |
| `scripts/backtest_runner.py` | Offline backtest runner |
| `scripts/check_connection.py` | MT5 connectivity smoke test |
| `test_lot_sizing.py` | Offline unit tests for RiskAgent lot-sizing logic (12 cases, no MT5 required); uses `make_agent()` helper; all cases exercise clamp behavior (no reject path) |

---

## Account Configuration

Four MT5 accounts run in parallel, loaded from environment variables:

| Env var pattern | Description |
|---|---|
| `MT5_LOGIN_1` … `MT5_LOGIN_4` | Account login numbers |
| `MT5_PASSWORD_1` … `MT5_PASSWORD_4` | Account passwords |
| `MT5_SERVER_1` … `MT5_SERVER_4` | Broker server names |
| `MT5_DIRECTION_1` … `MT5_DIRECTION_4` | Allowed trade direction: `BUY` or `SELL` |

**Direction assignment:**
- Accounts 1 & 2: `BUY`-only — only take long positions
- Accounts 3 & 4: `SELL`-only — only take short positions

**Shared infrastructure:** SRMapper and PriceWatcher use Account 1's MT5 client for all market-data fetches. Each account's AnalysisAgent, RiskAgent, Executor, and TradeMonitor connect to that account's own MT5 session.

**Daily trade cap:** `MAX_DAILY_TRADES = 2` per account per calendar day. Each account's trade count resets at midnight. A signal is dropped (before candle fetch) if the account has already reached its daily cap.

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

Each account's RiskAgent runs independently. A signal is rejected if any check fails:

1. **Direction match** — signal direction must match the account's allowed direction (`BUY`-only or `SELL`-only)
2. **Daily trade count** — reject if `daily_trade_count >= MAX_DAILY_TRADES` (default 2) for this account
3. **Daily loss limit** — today's realized P&L `<= -DAILY_LOSS_LIMIT_USD` (default −$30)
4. **Daily profit target** — today's realized P&L `>= DAILY_PROFIT_TARGET_USD` (default $100)

### Lot sizing
Lot size uses a fixed-risk approach: compute from `SL_USD`, then clamp; SL and TP distances are derived from the clamped lot.

```python
# 1. Compute raw lot from target SL risk
value_per_unit = tick_value / tick_size          # e.g. 10 for XAUUSD
raw_lot = SL_USD / (value_per_unit * sl_distance_initial)

# 2. Clamp to broker limits and config bounds
lot = clamp(raw_lot, max(LOT_MIN, volume_min), min(LOT_MAX, volume_max))
lot = round_to_step(lot, volume_step)

# 3. Derive actual SL/TP distances from clamped lot
sl_dist = SL_USD / (lot * value_per_unit)
tp_dist = TP_USD / (lot * value_per_unit)
sl = entry - sl_dist   # BUY
tp = entry + tp_dist   # BUY
```

`SL_USD` defaults to $50, `TP_USD` defaults to its configured value. `LOT_MIN = 0.05`, `LOT_MAX = 0.10`. Lots are **never rejected** on size — the clamp always produces a valid lot within bounds.

Fallback tick values if `get_symbol_info` is unavailable:
- XAUUSD: `tick_value=0.1`, `tick_size=0.01` (ratio 10)

Weekly loss check: `weekly_loss_ok` field is hardcoded `True` (weekly P&L gate is disabled); it exists in `RiskEvaluatedEvent` for audit-log consistency only and has no effect on trade approval.

---

## GPT Analysis (AnalysisAgent)

Each account has its own AnalysisAgent instance bound to an `account_config`. For each `ZoneTouchEvent`, the agent processes as follows:

**Pre-filter 0a — Daily trade cap** (before candle fetch): if `daily_trade_count >= MAX_DAILY_TRADES` for this account, drop the event immediately.

**Pre-filter 0b — Direction/zone compatibility** (before candle fetch): skip GPT call if the zone type contradicts the account's allowed direction:
- Resistance zone → no `BUY` account signal (price is likely to fall)
- Support zone → no `SELL` account signal (price is likely to rise)

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
- **Post-GPT direction filter**: if GPT direction ≠ account's allowed direction, drop the signal — no event emitted
- If `direction == "NONE"` or geometry is invalid, no event is emitted

**GPT JSON output schema:**
```json
{
  "direction": "BUY" | "SELL" | "NONE",
  "sl": 2650.00,
  "tp": 2680.00,
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

See `.env.example`. Required: `MT5_LOGIN_1`, `MT5_PASSWORD_1`, `MT5_SERVER_1`, `OPENAI_API_KEY`.  
Optional: `SYMBOL_SUFFIX`, `OPENAI_MODEL` (default `gpt-4o-mini`), `EXECUTION_LIVE` (default `True`), `DB_PATH`, `TICK_INTERVAL_SEC`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

**Multi-account variables (required for accounts 2–4):**

| Variable | Description |
|---|---|
| `MT5_LOGIN_1` … `MT5_LOGIN_4` | MT5 account login numbers |
| `MT5_PASSWORD_1` … `MT5_PASSWORD_4` | MT5 account passwords |
| `MT5_SERVER_1` … `MT5_SERVER_4` | Broker server names |
| `MT5_DIRECTION_1` … `MT5_DIRECTION_4` | `BUY` or `SELL` — allowed trade direction for each account |

**Risk / lot sizing variables:**

| Variable | Default | Description |
|---|---|---|
| `MAX_DAILY_TRADES` | `2` | Max trades per account per calendar day |
| `LOT_MIN` | `0.05` | Minimum allowed lot size (config floor, above broker minimum) |
| `LOT_MAX` | `0.10` | Maximum allowed lot size (config ceiling) |
| `SL_USD` | `50` | Target dollar risk used to compute raw lot size |
| `TP_USD` | configured | Target dollar profit used to derive TP distance from clamped lot |

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

The test suite uses a `make_agent()` helper to construct a RiskAgent with a mock `account_config`. All 12 cases exercise the clamp behavior — raw lots outside `[LOT_MIN, LOT_MAX]` are clamped, never rejected.

For core infrastructure, use `Database(":memory:")` to avoid touching the real DB file.
