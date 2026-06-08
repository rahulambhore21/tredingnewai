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

## Architecture

An event-driven, multi-threaded trading bot for MetaTrader 5 (currently BTCUSD). The pipeline is:

```
SRMapper ──zones──▶ PriceWatcher ──ZoneTouchEvent──▶ AnalysisAgent (GPT-4o)
                                                            │
                                                  SignalGeneratedEvent
                                                            │
                                                       RiskAgent
                                                            │
                                                  RiskEvaluatedEvent
                                                            │
                                                        Executor ──▶ MT5
                                                            │
                                                  TradeExecutedEvent
                                                            │
                                                      TradeMonitor ──▶ DB
```

All inter-agent communication is through `core/event_bus.py` (in-process pub/sub). `core/db_consumer.py` subscribes to all event types and is the **only** DB writer — other agents only call read helpers on `Database`. The exception is `sr_mapper`, which calls `deactivate_zones_before()` directly on refresh (documented in-code).

### Threads

- `SRMapper` — scans S/R zones on startup, refreshes every 4 hours; signals `zones_ready` when done
- `PriceWatcher` — waits for `zones_ready`, then polls prices every `TICK_INTERVAL_SEC` (default 2s)
- `AnalysisAgent` — has its own queue + thread so GPT API calls (2–10s) never block the tick loop
- `TradeMonitor` — polls MT5 every 30s for closed positions, emits `TradeClosedEvent`
- `RiskAgent` and `Executor` are synchronous event handlers (no dedicated threads)

The main thread runs a watchdog loop (every 60s) that reconnects MT5 if the connection drops and restarts any dead agent threads.

### Key files

| File | Role |
|---|---|
| `config.py` | All tunable parameters — instruments, risk limits, zone settings, MT5 config |
| `core/events.py` | Pydantic schemas for every event type |
| `core/event_bus.py` | Thread-safe pub/sub; handlers run synchronously on publisher's thread |
| `core/database.py` | SQLite layer; read helpers for agents, write helpers for db_consumer only |
| `core/db_consumer.py` | Sole DB writer; subscribes to all events |
| `indicators/calculator.py` | Pure pandas/numpy functions (EMA, RSI, MACD, swing detection, zone clustering) — no MT5 dependency |
| `agents/sr_mapper.py` | Detects swing highs/lows, clusters into zones, writes to DB via event bus |
| `agents/price_watcher.py` | Tick loop; checks price against active zones; emits `ZoneTouchEvent` |
| `agents/analysis_agent.py` | Queues `ZoneTouchEvent`s; fetches candles; calls GPT-4o; emits `SignalGeneratedEvent` |
| `agents/risk_agent.py` | 5 risk checks; computes lot size; emits `RiskEvaluatedEvent` |
| `agents/executor.py` | Places MT5 market orders (two-step: place then modify SL/TP) |
| `agents/trade_monitor.py` | Detects closed positions; records realized P&L |
| `prompts/system_prompt.txt` | GPT-4o system prompt instructing JSON output |
| `prompts/user_template.py` | Builds the per-signal user prompt with indicators + candles |

## MT5 API (metatrader-mcp-server v0.5.1)

Methods live on **sub-clients**, not on `MT5Client` directly:

```python
client.market.get_candles_latest(symbol, timeframe, count=100)  # → pd.DataFrame newest-first
client.market.get_symbol_price(symbol)                          # → {bid, ask, last, volume, time}
client.order.place_market_order(type=dir, symbol=sym, volume=v) # → {error, message, data}
client.order.modify_position(pos_id, stop_loss=sl, take_profit=tp)
client.order.get_all_positions()                                # → pd.DataFrame
client.account.get_balance() / get_equity()                    # → float
client.account.get_account_info()                              # → dict
```

**Critical constraints:**
- SL/TP requires two steps: `place_market_order` (returns position id in `data.order`) then `modify_position`
- Timeframe strings must be MT5 keys: `M5`, `M15`, `H1`, `H4`, `D1` — **not** `5m`/`1h` etc.
- Candle DataFrames are **newest-first**; always call `sort_candles_ascending()` from `indicators/calculator.py` before computing EMA/RSI/MACD
- Symbol names may need a broker suffix (e.g. `.r`); use `config.resolve_symbol()` before any MT5 call

## Risk gate (in RiskAgent)

A signal is rejected if any check fails:
1. R:R ratio `< MIN_RR` (default 1.5)
2. Open trades `>= MAX_OPEN_TRADES` (default 3)
3. Correlated pairs both open (configurable in `CORRELATED_PAIRS`)
4. Today's realized P&L `<= -DAILY_LOSS_LIMIT_USD` (default −$30)
5. Today's realized P&L `>= DAILY_PROFIT_TARGET_USD` (default $100)

Lot size uses fixed $10 notional: `lot = FIXED_TRADE_USD / (entry × contract_size)`, clamped to broker limits (floor = `volume_min`, typically 0.01 for BTC). For BTC at ~$65k this always hits the volume_min floor — that is expected behaviour.

Weekly loss check: `weekly_loss_ok` field is hardcoded `True` (weekly P&L gate is disabled); it exists in `RiskEvaluatedEvent` for audit-log consistency only and has no effect on trade approval.

## Environment variables

See `.env.example`. Required: `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER`, `OPENAI_API_KEY`.  
Optional: `SYMBOL_SUFFIX`, `OPENAI_MODEL`, `EXECUTION_LIVE`, `DB_PATH`, `TICK_INTERVAL_SEC`.

## Testing without MT5

`indicators/calculator.py` has no MT5 dependency and can be unit-tested offline:

```python
python -c "from indicators.calculator import compute_all_indicators; print('OK')"
```

For core infrastructure, use `Database(":memory:")` to avoid touching the real DB file.
