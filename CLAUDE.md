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
```

Logs go to both stdout and `trading_bot.log`. The SQLite DB is `trading_bot.db`.

---

## Symbols & Timeframes

### Symbols
EURUSD, USDJPY, XAUUSD — configured in `config.SYMBOLS`.  
All agents loop over this list. Never hardcode a symbol anywhere.

### Timeframes
M5 and M15 only. All candle fetches, S/R scans, and indicator calculations are restricted to these two timeframes.  
MT5 timeframe keys: `"M5"`, `"M15"` — do **not** use `"5m"`, `"1h"`, or any other format.

### Concurrent Trade Limit
`MAX_OPEN_TRADES = 1` — account-wide across all symbols combined.

---

## Architecture

An event-driven, multi-threaded trading bot for MetaTrader 5 targeting EURUSD, USDJPY, and XAUUSD on M5 and M15 timeframes. The pipeline is:

```
SRMapper ──zones (per symbol)──▶ PriceWatcher ──ZoneTouchEvent(symbol)──▶ AnalysisAgent (GPT-4o)
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

- `SRMapper` — scans S/R zones for **all 3 symbols** on both M5 and M15 on startup; refreshes every 4 hours; signals `zones_ready` only after all symbols are done
- `PriceWatcher` — waits for `zones_ready`, then polls prices for **all 3 symbols** every `TICK_INTERVAL_SEC` (default 2s) in a single loop
- `AnalysisAgent` — has its own queue + thread so GPT API calls (2–10s) never block the tick loop; fetches M5 + M15 candles per signal
- `TradeMonitor` — polls MT5 every 30s for closed positions, emits `TradeClosedEvent`
- `RiskAgent` and `Executor` are synchronous event handlers (no dedicated threads)

The main thread runs a watchdog loop (every 60s) that reconnects MT5 if the connection drops and restarts any dead agent threads.

### Key files

| File | Role |
|---|---|
| `config.py` | All tunable parameters — instruments, risk limits, zone settings, MT5 config |
| `core/events.py` | Pydantic schemas for every event type — all events carry a `symbol` field |
| `core/event_bus.py` | Thread-safe pub/sub; handlers run synchronously on publisher's thread |
| `core/database.py` | SQLite layer; read helpers for agents, write helpers for db_consumer only; zones table has `symbol` column |
| `core/db_consumer.py` | Sole DB writer; subscribes to all events |
| `indicators/calculator.py` | Pure pandas/numpy functions (EMA, RSI, MACD, swing detection, zone clustering) — no MT5 dependency |
| `agents/sr_mapper.py` | Loops all symbols; detects swing highs/lows on M5+M15; clusters into zones tagged with `symbol`; writes to DB via event bus |
| `agents/price_watcher.py` | Tick loop over all 3 symbols; checks each symbol's price against that symbol's zones only; emits `ZoneTouchEvent(symbol)` |
| `agents/analysis_agent.py` | Queues `ZoneTouchEvent`s; fetches M5 + M15 candles for `event.symbol`; calls GPT-4o with both TF data; emits `SignalGeneratedEvent` |
| `agents/risk_agent.py` | 5 risk checks; computes lot size per symbol using pip/point-value formula; emits `RiskEvaluatedEvent` |
| `agents/executor.py` | Places MT5 market orders (two-step: place then modify SL/TP) |
| `agents/trade_monitor.py` | Detects closed positions; records realized P&L |
| `prompts/system_prompt.txt` | GPT-4o system prompt — forex/gold context, M5+M15 confluence logic, JSON output |
| `prompts/user_template.py` | Builds per-signal user prompt with M5 indicators + candles and M15 indicators + candles for the triggered symbol |

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

## GPT-4o Analysis (AnalysisAgent)

For each `ZoneTouchEvent`, the agent:
1. Fetches 100 candles on both M5 and M15 for `event.symbol`
2. Computes EMA 20/50, RSI 14, MACD on both timeframes
3. Passes M15 data (trend) + M5 data (entry confirmation) to GPT-4o
4. GPT-4o returns strictly:

```json
{
  "direction": "BUY" | "SELL" | "NONE",
  "sl": 1.08450,
  "tp": 1.09100,
  "confidence": 7,
  "reason": "M15 bullish trend, M5 bounce confirmed at support"
}
```

If `direction` is `"NONE"`, no `SignalGeneratedEvent` is emitted — the signal is silently dropped.

---

## Environment variables

See `.env.example`. Required: `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER`, `OPENAI_API_KEY`.  
Optional: `SYMBOL_SUFFIX`, `OPENAI_MODEL`, `EXECUTION_LIVE`, `DB_PATH`, `TICK_INTERVAL_SEC`.

---

## Testing without MT5

`indicators/calculator.py` has no MT5 dependency and can be unit-tested offline:

```python
python -c "from indicators.calculator import compute_all_indicators; print('OK')"
```

For core infrastructure, use `Database(":memory:")` to avoid touching the real DB file.