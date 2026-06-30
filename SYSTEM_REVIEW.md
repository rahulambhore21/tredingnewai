# TradeAI Bot — Complete System Review

This document is a full manual-review guide: every component, every calculation, and one concrete trade walked through the entire pipeline from price tick to closed P&L.

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Startup Sequence](#2-startup-sequence)
3. [Component Deep-Dives](#3-component-deep-dives)
   - 3.1 [config.py — Central Parameters](#31-configpy--central-parameters)
   - 3.2 [SRMapper — Zone Detection](#32-srmapper--zone-detection)
   - 3.3 [PriceWatcher — Tick Loop](#33-pricewatcher--tick-loop)
   - 3.4 [AnalysisAgent — GPT Signal](#34-analysisagent--gpt-signal)
   - 3.5 [RiskAgent — Risk Gate & Lot Sizing](#35-riskagent--risk-gate--lot-sizing)
   - 3.6 [Executor — Order Placement](#36-executor--order-placement)
   - 3.7 [TradeMonitor — Close Detection](#37-trademonitor--close-detection)
   - 3.8 [EventBus — Pub/Sub Backbone](#38-eventbus--pubsub-backbone)
   - 3.9 [DBConsumer — Sole DB Writer](#39-dbconsumer--sole-db-writer)
   - 3.10 [SignalTracker — Win-Rate Gate](#310-signaltracker--win-rate-gate)
4. [Indicator Calculations](#4-indicator-calculations)
   - 4.1 [EMA](#41-ema)
   - 4.2 [RSI](#42-rsi)
   - 4.3 [MACD](#43-macd)
   - 4.4 [Swing Highs / Lows (Fractal Pivot)](#44-swing-highs--lows-fractal-pivot)
   - 4.5 [Zone Clustering](#45-zone-clustering)
5. [Lot Sizing — Full Math](#5-lot-sizing--full-math)
6. [GPT Prompt — What the AI Sees](#6-gpt-prompt--what-the-ai-sees)
7. [All Pre-Filters & Guards (in order)](#7-all-pre-filters--guards-in-order)
8. [Complete Sample Trade — End to End](#8-complete-sample-trade--end-to-end)
9. [Database Schema](#9-database-schema)
10. [Thread Map](#10-thread-map)
11. [Watchdog & Crash Recovery](#11-watchdog--crash-recovery)
12. [Dashboard & Analytics](#12-dashboard--analytics)

---

## 1. High-Level Architecture

```
main.py (supervisor)
  └─ spawns worker.py × 4 (one per MT5 account)

Inside each worker.py:
  ┌─────────────────────────────────────────────────────────────────┐
  │  SRMapper  ──zones──▶  PriceWatcher  ──ZoneTouchEvent──┐       │
  │  (Account 1 MT5)       (Account 1 MT5)                  │       │
  │                                                          │       │
  │                                          AnalysisAgent[acctN]   │
  │                                          (GPT, per-account dir) │
  │                                                  │               │
  │                                       SignalGeneratedEvent       │
  │                                                  │               │
  │                                          RiskAgent[acctN]       │
  │                                                  │               │
  │                                       RiskEvaluatedEvent        │
  │                                                  │               │
  │                                          Executor[acctN]        │
  │                                          (MT5 acctN)            │
  │                                                  │               │
  │                                       TradeExecutedEvent        │
  │                                                  │               │
  │                                        TradeMonitor[acctN]      │
  │                                        (polls MT5 every 30s)   │
  │                                                  │               │
  │                                       TradeClosedEvent          │
  │                                                  │               │
  │                                          DBConsumer (sole writer)│
  └─────────────────────────────────────────────────────────────────┘
```

**Key design rules:**
- `SRMapper` and `PriceWatcher` are **singletons** using Account 1's MT5 connection for all market data
- Each of 4 accounts has its own `AnalysisAgent`, `RiskAgent`, `Executor`, `TradeMonitor`
- All communication is through the **in-process EventBus** (pub/sub), never direct calls between agents
- `DBConsumer` is the **only** component that writes to SQLite — everything else only reads
- `main.py` is a supervisor that spawns subprocesses, detects crashes, and restarts workers

---

## 2. Startup Sequence

```
main.py
  1. _validate_config()   → checks ACCOUNTS loaded, OPENAI_API_KEY set
  2. _spawn(acct_id, dir) → subprocess.Popen("worker.py --account N --direction BUY/SELL")
  3. Monitor loop every 10s → if worker.poll() is not None → crashed → restart after 5s

worker.py (per account)
  1. Configure logging (rotating file 5MB × 5 + stdout)
  2. Database(DB_PATH) → _create_tables() + _migrate_tables()
  3. EventBus() → shared in-process
  4. DBConsumer(db, bus) → subscribes to all 7 event types
  5. MT5Client(login, password, server, terminal_path) → connect
  6. SRMapper(client1, bus, db).start()
       └─ Thread "SRMapper" → _scan_all() → zones_ready.set()
  7. PriceWatcher(client1, bus, db, zones_ready).start()
       └─ Thread "PriceWatcher" → waits zones_ready → tick loop
  8. AnalysisAgent(account_config, bus, db, signal_tracker).start()
       └─ Thread "AnalysisAgent[N]" → queue consumer
  9. RiskAgent(account_config, bus, db).start()  (synchronous handler)
  10. Executor(account_config, bus, notifier).start()  (synchronous handler)
  11. TradeMonitor(account_config, bus, db, signal_tracker).start()
        └─ Thread "TradeMonitor[N]" → polls MT5 every 30s
  12. Watchdog loop every 60s → checks heartbeats, reconnects MT5 if dead
```

---

## 3. Component Deep-Dives

### 3.1 `config.py` — Central Parameters

Everything tunable lives here. Key groups:

| Group | Key Parameters | Values |
|---|---|---|
| Symbol | `SYMBOLS`, `SYMBOL_SUFFIX` | `["XAUUSD"]`, broker suffix e.g. `.r` |
| Timeframes | `SR_TIMEFRAMES`, `ANALYSIS_CANDLE_COUNT` | `["M5","M15"]`, 100 candles |
| Risk | `SL_USD`, `TP_USD`, `LOT_MIN`, `LOT_MAX` | $50, $150, 0.05, 0.10 |
| Daily limits | `MAX_DAILY_TRADES`, `DAILY_LOSS_LIMIT_USD`, `DAILY_PROFIT_TARGET_USD` | 2, $30, $100 |
| Zones | `ZONE_TOUCH_PCT`, `ZONE_COOLDOWN_MIN`, `ZONE_REFRESH_HOURS` | 0.3%, 15 min, 4 h |
| Swing detection | `SWING_LOOKBACK`, `CLUSTER_TOLERANCE` | 5 bars, 0.15% |
| GPT | `OPENAI_MODEL`, `OPENAI_MAX_TOKENS` | `gpt-4o-mini`, 150 |
| Signal gate | `SIGNAL_TRACKER_WINDOW`, `SIGNAL_PAUSE_THRESHOLD` | 20 trades, 40% |
| Tick speed | `TICK_INTERVAL_SEC` | 2 seconds |

**Account loading** (`_load_account_configs`): Reads `MT5_LOGIN_1..4`, `MT5_PASSWORD_1..4`, `MT5_SERVER_1..4`, `MT5_DIRECTION_1..4` from `.env`. Any account without a login is silently skipped.

**Symbol resolution**: `resolve_symbol("XAUUSD")` → `"XAUUSD" + SYMBOL_SUFFIX`. If your broker uses `XAUUSDr`, set `SYMBOL_SUFFIX=r`.

---

### 3.2 `SRMapper` — Zone Detection

**Thread:** `SRMapper` (daemon)

**What it does:**  
Scans XAUUSD on M5 and M15, finds swing highs/lows, clusters them into support/resistance zones, writes zones to DB via EventBus, then sleeps 4 hours and repeats.

**Flow:**
```
_scan_all()
  for symbol in ["XAUUSD"]:
    for tf in ["M5", "M15"]:
      _scan_symbol_tf(symbol, tf)
        1. db.deactivate_zones_for_symbol(symbol, tf)   ← wipe old zones
        2. get_candles_latest(symbol, tf, count=200)     ← 200 candles newest-first
        3. find_swing_highs_lows(df, lookback=5)         ← fractal pivots
        4. cluster_zones(highs, "resistance", tol=0.0015)
           cluster_zones(lows,  "support",    tol=0.0015)
        5. for each zone: bus.publish(ZoneEvent(...))   ← DBConsumer writes to DB
        6. bus.publish(ZonesRefreshedEvent(...))
zones_ready.set()  ← unblocks PriceWatcher
```

**Heartbeat:** Updates `last_heartbeat = time.time()` on each loop iteration.  
**Watchdog stale threshold:** 150 seconds.

**Refresh sleep:** Sleeps `ZONE_REFRESH_HOURS × 3600` seconds in 30-second chunks (so the stop event can interrupt it).

---

### 3.3 `PriceWatcher` — Tick Loop

**Thread:** `PriceWatcher` (daemon)

**What it does:**  
Polls the current XAUUSD bid/ask every 2 seconds. For each tick, loads all active zones from DB and checks if the mid-price is within 0.3% of any zone center. Fires `ZoneTouchEvent` when a zone is touched (with 15-minute cooldown per zone).

**Flow per tick:**
```
while True:
  price = client.market.get_symbol_price("XAUUSD")
  mid = (bid + ask) / 2

  zones = db.get_active_zones("XAUUSD")
  touched = []
  for zone in zones:
    proximity = abs(mid - zone.price_center) / zone.price_center
    if proximity <= ZONE_TOUCH_PCT (0.003):
      if not in_cooldown(zone.zone_id):   # 15-min cooldown
        touched.append(zone)

  # Cluster dedup: if multiple zones touched simultaneously,
  # group by proximity → pick highest priority per cluster:
  #   M15 zone preferred over M5; higher strength preferred
  for best_zone in deduplicated(touched):
    _last_touch[zone_id] = now
    bus.publish(ZoneTouchEvent(symbol, zone_type, price_center, ...))

  sleep(TICK_INTERVAL_SEC)  # 2s
```

**Staleness check:** If `mid_price` unchanged for 10 consecutive ticks → WARNING log.  
**Silence check:** If no valid tick received for 60 seconds → ERROR log.  
**Heartbeat stale threshold:** `TICK_INTERVAL_SEC × 5 = 10s`.

---

### 3.4 `AnalysisAgent` — GPT Signal

**Thread:** `AnalysisAgent[N]` (one per account, daemon)  
**Queue size:** 10 events (drops overflow silently)

**What it does:**  
Receives `ZoneTouchEvent` from its queue, runs a series of pre-filters, fetches candle data, computes indicators, calls GPT-4o-mini, validates the response, and emits `SignalGeneratedEvent`.

**Pre-filter sequence (in order):**

```
Event arrives from queue
  │
  ▼
[Filter 0a] daily_trade_count >= MAX_DAILY_TRADES (2)?
  YES → drop, log "daily cap reached"
  │
  ▼
[Filter 0b-win] SignalTracker.should_pause?
  YES → drop, log "win-rate gate active"
  │
  ▼
[Filter 0c] Direction/Zone pre-filter
  BUY account + resistance zone → drop (price likely to fall from resistance)
  SELL account + support zone  → drop (price likely to rise from support)
  │
  ▼
[Filter 0d] open_trade_count >= MAX_OPEN_TRADES (1)?
  YES → drop
  │
  ▼
Fetch 100 M5 + 100 M15 candles for XAUUSD
Compute indicators on both (EMA21, EMA50, RSI14, MACD)
  │
  ▼
[Filter 0e] M15 EMA trend contradicts zone type?
  Support zone + M15 bearish (EMA21 < EMA50) → skip GPT call
  Resistance zone + M15 bullish (EMA21 > EMA50) → skip GPT call
  │
  ▼
Build user prompt (build_user_prompt())
Call GPT: model=gpt-4o-mini, max_tokens=150, response_format=json_object
  └─ Retry 2× on generic errors (2s, 4s backoff)
  └─ APITimeoutError / RateLimitError → return None immediately (no retry)
  │
  ▼
Parse JSON response: {direction, sl, tp, confidence, reason}
  │
  ▼
[Filter post-GPT-1] direction == "NONE"? → drop
  │
  ▼
[Filter post-GPT-2] direction != account's allowed direction?
  BUY account + GPT says "SELL" → drop
  │
  ▼
Refresh current price (re-fetch after GPT latency)
  │
  ▼
[Filter post-GPT-3] Geometry check
  BUY:  sl < entry < tp? (SL below entry, TP above)
  SELL: tp < entry < sl? (TP below entry, SL above)
  Invalid → drop
  │
  ▼
bus.publish(SignalGeneratedEvent(symbol, direction, entry, sl, tp,
            confidence, reasoning, zone_type, account_id,
            ema21, ema50, rsi14, macd_line, macd_signal, macd_hist))
```

**Per-zone cooldown:** 30 seconds — same `(symbol, zone_id)` within 30s → skip.

**GPT model:** `gpt-4o-mini` (override: `OPENAI_MODEL` env var)

---

### 3.5 `RiskAgent` — Risk Gate & Lot Sizing

**Mode:** Synchronous EventBus handler (no dedicated thread)  
**Filters by:** `event.account_id == self.account_id`

**Five checks (all must pass for approval):**

| # | Check | Condition | Flag |
|---|---|---|---|
| 1 | Direction match | Signal dir == account allowed dir | `direction_ok` |
| 2 | Daily trade count | `daily_count < MAX_DAILY_TRADES (2)` | `daily_count_ok` |
| 3 | Open trade count | `open_trades < MAX_OPEN_TRADES (1)` | `max_trades_ok` |
| 4 | Correlated pair | No correlated open trades | `correlation_ok` |
| 5 | Daily P&L limits | `-(DAILY_LOSS_LIMIT_USD=30) <= pnl <= (DAILY_PROFIT_TARGET_USD=100)` | `daily_loss_ok` |

P&L lookup: tries MT5 deal history first; falls back to `db.get_today_realized_pnl()`.

**Lot sizing (`_compute_lot_and_levels`):**

```python
# 1. Get symbol info from MT5
tick_value = 0.1    # per XAUUSD tick (fallback if MT5 unavailable)
tick_size  = 0.01   # minimum price movement for XAUUSD

value_per_unit = tick_value / tick_size   # = 0.1 / 0.01 = 10.0

# 2. GPT's SL distance (price difference)
gpt_sl_distance = abs(entry - gpt_sl)

# 3. Raw lot from target SL risk
raw_lot = SL_USD / (value_per_unit * gpt_sl_distance)
        = 50.0 / (10.0 * gpt_sl_distance)

# 4. Clamp to broker + config bounds
lot = clamp(raw_lot, max(LOT_MIN=0.05, volume_min), min(LOT_MAX=0.10, volume_max))
lot = round_down_to_step(lot, volume_step)

# 5. Recompute actual SL/TP distances from clamped lot
sl_dist = SL_USD / (lot * value_per_unit)   # = 50.0 / (lot * 10)
tp_dist = TP_USD / (lot * value_per_unit)   # = 150.0 / (lot * 10)

# 6. Compute final SL and TP prices
if direction == "BUY":
    sl = entry - sl_dist
    tp = entry + tp_dist
elif direction == "SELL":
    sl = entry + sl_dist
    tp = entry - tp_dist
```

**Note:** GPT's SL/TP are used **only** to determine `gpt_sl_distance`. The actual SL and TP prices sent to MT5 are always recalculated from `SL_USD` and `TP_USD` using the clamped lot.

**Outcome:** Publishes `RiskEvaluatedEvent` with `approved=True/False`, all check flags, computed `volume`, `stop_loss`, `take_profit`.

---

### 3.6 `Executor` — Order Placement

**Mode:** Synchronous EventBus handler  
**Triggers on:** `RiskEvaluatedEvent` where `approved=True` and `account_id` matches

**Duplicate guard:** Same `(symbol, direction, entry)` within 5 seconds → skip.

**Dry-run mode (`EXECUTION_LIVE=False`):**
```
Log "DRY RUN — would place ORDER direction=BUY vol=0.05 entry=2330.50 sl=2325.50 tp=2345.50"
Publish TradeExecutedEvent(success=True, dry_run=True, order_id=None)
```

**Live mode:**
```
Step 1: place_market_order(type=direction, symbol=symbol, volume=volume)
  Response: {error, message, data: {order: position_id}}

Step 2: modify_position(position_id, stop_loss=sl, take_profit=tp)
  On failure: send Telegram "⚠️ NAKED POSITION — SL/TP NOT SET"
              still publishes TradeExecutedEvent(success=True, sl_tp_modified=False)

Publish TradeExecutedEvent(
  symbol, direction, volume, entry, stop_loss, take_profit,
  success=True, order_id=position_id, fill_price=actual_fill,
  sl_tp_modified=True/False, dry_run=False
)
```

---

### 3.7 `TradeMonitor` — Close Detection

**Thread:** `TradeMonitor[N]` (one per account, daemon)  
**Poll interval:** 30 seconds

**Startup recovery:** On `start()`, calls `_recover_open_positions()` — fetches all current open positions from MT5 and populates `_tracked` dict. This handles bot restarts without losing track of open trades.

**How it detects closes:**
```
Every 30s:
  open_now  = set(pos.order_id for pos in client.order.get_all_positions())
  tracked   = set(self._tracked.keys())
  closed_ids = tracked - open_now

  for order_id in closed_ids:
    _handle_closed(order_id)
      → _get_close_deal(order_id)   ← queries MT5 deal history (last 2 days)
        returns: close_price, realized_pnl
      → signal_tracker.record_result(signal_id, won=pnl > 0)
      → bus.publish(TradeClosedEvent(order_id, close_price, realized_pnl, account_id))
      → del _tracked[order_id]
```

---

### 3.8 `EventBus` — Pub/Sub Backbone

**Thread safety:** `threading.Lock()` on subscribe/publish.

**Publish mechanics:**
```python
def publish(event):
    snapshot = dict(self._subscribers)  # copy under lock, then release
    for registered_type, handlers in snapshot.items():
        if issubclass(type(event), registered_type):
            for handler in handlers:
                try:
                    handler(event)       # synchronous — runs on publisher's thread
                except Exception:
                    logger.exception(...)  # handler crash doesn't break other handlers
```

**Critical implication:** Handlers like `RiskAgent` and `Executor` run **synchronously** on the publishing thread. Only `AnalysisAgent` and `TradeMonitor` have their own threads (queue-based). This means:

- `PriceWatcher` publishes `ZoneTouchEvent` → `AnalysisAgent` handler just enqueues it and returns immediately → tick loop is never blocked
- `AnalysisAgent` publishes `SignalGeneratedEvent` → `RiskAgent` runs synchronously → `RiskAgent` publishes `RiskEvaluatedEvent` → `Executor` runs synchronously — all on the AnalysisAgent thread

---

### 3.9 `DBConsumer` — Sole DB Writer

Subscribes to all 7 event types and is the **only** component that writes to SQLite:

| Event | DB Action |
|---|---|
| `ZoneEvent` | `insert_zone()` — creates or updates zone row |
| `ZonesRefreshedEvent` | `insert_event_log()` — audit log only |
| `ZoneTouchEvent` | `insert_event_log()` — records touch |
| `SignalGeneratedEvent` | `insert_signal()` + `insert_validation_log()` — creates validation tracking row |
| `RiskEvaluatedEvent` | `insert_risk_decision()` + `update_validation_log_risk()` |
| `TradeExecutedEvent` | `insert_trade()` + `update_validation_log_trade()` |
| `TradeClosedEvent` | `update_trade_close()` + `update_validation_log_close()` |

The `validation_log` table is what `AnalyticsEngine` uses — it joins signal → risk → trade → close into one row per signal, enabling full pipeline conversion analysis.

---

### 3.10 `SignalTracker` — Win-Rate Gate

**State:** Rolling deque of the last 20 `bool` results (won/lost).  
**Persistence:** `signal_tracker_state.json` — survives bot restarts.

```python
win_rate = sum(results) / len(results)     # 0.0 to 1.0

should_pause = (
    len(results) >= SIGNAL_TRACKER_WINDOW  # only after 20 trades
    and win_rate < SIGNAL_PAUSE_THRESHOLD  # below 40%
)
```

If `should_pause` returns `True`:
- `AnalysisAgent` checks this at Filter 0b (before any GPT call)
- No new signals are generated until win rate recovers above 40%
- During warm-up (< 20 trades), `should_pause` is always `False`

`record_result(signal_id, won)` is called by `TradeMonitor` when a position closes.

---

## 4. Indicator Calculations

All indicator math lives in `indicators/calculator.py`. It has **no MT5 dependency** and can be unit-tested offline.

### 4.1 EMA

```python
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()
```

This is the standard **exponential weighted moving average** using pandas' `ewm`. `adjust=False` means it uses recursive smoothing: `EMA[t] = α × price[t] + (1-α) × EMA[t-1]` where `α = 2 / (span + 1)`.

- EMA21: α = 2/22 ≈ 0.0909
- EMA50: α = 2/51 ≈ 0.0385

**Usage in the bot:**
- M15 EMA21 vs EMA50 → trend direction (bullish if EMA21 > EMA50)
- Used in pre-filter 0e: if M15 trend contradicts zone type → skip GPT

---

### 4.2 RSI

```python
def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("inf"))
    return 100 - (100 / (1 + rs))
```

**Wilder smoothing**: `alpha = 1/14 ≈ 0.0714`. The `replace(0, float("inf"))` handles the edge case where there are zero losses (RSI would be 100).

**Interpretation in system prompt:**
- RSI < 30 → oversold → supports BUY
- RSI > 70 → overbought → supports SELL
- 30–70 → neutral

---

### 4.3 MACD

```python
def macd(series, fast=12, slow=26, signal_period=9):
    ema_fast   = ema(series, fast)
    ema_slow   = ema(series, slow)
    macd_line  = ema_fast - ema_slow
    signal_line = ema_line.ewm(span=signal_period, adjust=False).mean()
    histogram  = macd_line - signal_line
    return macd_line, signal_line, histogram
```

- `macd_line > 0` → bullish momentum
- `histogram > 0` → momentum strengthening
- Used in the GPT prompt labeled as "MACD Bias"

---

### 4.4 Swing Highs / Lows (Fractal Pivot)

```python
def find_swing_highs_lows(df, lookback=5):
    # A candle is a swing HIGH if:
    #   its HIGH is strictly greater than the HIGH of all candles
    #   within lookback bars on BOTH sides (2*lookback neighbours)
    # A candle is a swing LOW if:
    #   its LOW is strictly less than the LOW of all candles
    #   within lookback bars on BOTH sides
```

With `lookback=5`, a pivot requires 5 bars before AND 5 bars after to be strictly lower/higher. This means the most recent 5 candles can never be pivots (we don't have the right-side lookback yet).

**From 200 candles**, this produces ~10–30 pivot points depending on market structure.

---

### 4.5 Zone Clustering

```python
def cluster_zones(prices, zone_type, tolerance=0.0015):
    # Sort prices
    # Greedy merge: if next price within tolerance% of cluster start → add to cluster
    # Else → finalize current cluster, start new one
    # For each cluster:
    #   center   = mean of all prices in cluster
    #   upper    = max + half_tolerance
    #   lower    = min - half_tolerance
    #   strength = count of pivots in cluster
```

`tolerance=0.0015` means pivots within 0.15% of each other are merged into one zone.

**Example:** If swing highs appear at 2330.00, 2330.50, 2331.00 → all within 0.15% of each other → one resistance zone with:
- center = 2330.50
- upper = 2331.00 + (0.15%/2) = ~2331.75
- lower = 2330.00 - (0.15%/2) = ~2328.25
- strength = 3

**Strength interpretation** (from analytics): weak 1–2, moderate 3–4, strong 5+

---

## 5. Lot Sizing — Full Math

Let's walk through the full calculation with concrete numbers.

**Given:**
- `SL_USD = 50.0` (target risk per trade)
- `TP_USD = 150.0` (target profit per trade)
- `LOT_MIN = 0.05`, `LOT_MAX = 0.10`
- XAUUSD: `tick_value = 0.1`, `tick_size = 0.01`
- GPT says: entry=2330.50, sl=2325.00, tp=2345.00
- `volume_step = 0.01` (broker minimum lot increment)

**Step 1: value_per_unit**
```
value_per_unit = tick_value / tick_size = 0.1 / 0.01 = 10.0
```
This means: for a 1-lot position in XAUUSD, each $0.01 price move = $10.00 P&L.

**Step 2: GPT SL distance**
```
gpt_sl_distance = |entry - gpt_sl| = |2330.50 - 2325.00| = 5.50
```

**Step 3: raw lot**
```
raw_lot = SL_USD / (value_per_unit × gpt_sl_distance)
        = 50.0 / (10.0 × 5.50)
        = 50.0 / 55.0
        = 0.909 lots
```

**Step 4: clamp**
```
clamp(0.909, min=0.05, max=0.10) → 0.10
round_down_to_step(0.10, step=0.01) → 0.10 lots
```
The raw lot (0.909) far exceeds `LOT_MAX`, so it gets clamped down to 0.10.

**Step 5: recompute actual SL/TP distances**
```
sl_dist = SL_USD / (lot × value_per_unit) = 50.0 / (0.10 × 10.0) = 50.0 / 1.0 = 50.0 pts
tp_dist = TP_USD / (lot × value_per_unit) = 150.0 / (0.10 × 10.0) = 150.0 / 1.0 = 150.0 pts
```

**Step 6: compute actual SL/TP prices (BUY)**
```
sl = entry - sl_dist = 2330.50 - 50.00 = 2280.50
tp = entry + tp_dist = 2330.50 + 150.00 = 2480.50
```

**Result:**
```
Volume: 0.10 lots
SL: 2280.50  (50 point risk = $50 if clamped lot used)
TP: 2480.50  (150 point profit = $150 if clamped lot used)
R:R = 150/50 = 3.0
```

**Verification:**
```
P&L if SL hit: 0.10 × 10.0 × 50.0 = $50.00 loss  ✓
P&L if TP hit: 0.10 × 10.0 × 150.0 = $150.00 gain ✓
```

**Important:** GPT's SL (2325.00) is only used to compute the raw lot. The actual SL sent to MT5 is 2280.50 — much wider — because the lot got clamped down.

---

## 6. GPT Prompt — What the AI Sees

### System Prompt (fixed, from `prompts/system_prompt.txt`)

```
You are an expert technical analyst and disciplined Forex/Gold trader specialising
in support and resistance trading. Your role is to evaluate potential trade setups
at key price zones using M5 and M15 timeframe confluence.

## Your responsibilities
1. Analyse the provided M15 data (trend context) and M5 data (entry confirmation).
2. Determine the trade direction based on:
   - Zone type (support → potential BUY, resistance → potential SELL)
   - M15 trend alignment (EMAs, MACD histogram)
   - M5 entry confirmation (RSI, recent candle behaviour)
3. If no valid setup exists, return direction "NONE".
4. Calculate stop-loss and take-profit levels with R:R >= 2.0.
5. Assign a confidence score from 0 to 10.

## Rules
- direction MUST be exactly "BUY", "SELL", or "NONE"
- sl for BUY must be BELOW current price; for SELL ABOVE
- tp for BUY must be ABOVE current price; for SELL BELOW
- R:R (|tp - price| / |price - sl|) MUST be at least 2.0
- confidence 0-4: weak; 5-7: moderate; 8-10: strong
```

### User Prompt (built per-event, from `prompts/user_template.py`)

```
Instrument: XAUUSD

## Current Price
  Bid:   2330.45000
  Ask:   2330.55000
  Mid:   2330.50000

## S/R Zone Being Tested
  Type:     SUPPORT
  Center:   2330.00000
  Upper:    2331.00000
  Lower:    2329.00000
  Strength: 3 pivots

## M15 (Trend Context) Indicators
  EMA21:      2328.50  |  EMA50: 2325.00  |  EMA Trend: BULLISH (EMA21 > EMA50)
  RSI-14:     48.20    |  Signal: Neutral
  MACD Line:  3.25     |  Signal: 2.80    |  Hist: 0.45  |  Bias: Bullish

## Recent M15 Candles (last 5, ascending)
  2024-01-15 12:00 | O:2326.50 H:2329.00 L:2326.00 C:2328.50
  2024-01-15 12:15 | O:2328.50 H:2330.50 L:2328.00 C:2330.00
  ...

## M5 (Entry Confirmation) Indicators
  EMA21:      2330.20  |  EMA50: 2329.50  |  EMA Trend: BULLISH
  RSI-14:     42.50    |  Signal: Neutral-Low
  MACD Line:  0.85     |  Signal: 0.70    |  Hist: 0.15  |  Bias: Bullish

## Recent M5 Candles (last 5, ascending)
  2024-01-15 13:20 | O:2330.50 H:2331.00 L:2329.50 C:2330.20
  ...

## Task
Price has just touched the SUPPORT zone at 2330.00000 for XAUUSD.
Use M15 for trend direction and M5 for entry confirmation.
Provide your trading signal as a strict JSON object:
{"direction": "BUY"|"SELL"|"NONE", "sl": float, "tp": float,
 "confidence": int(0-10), "reason": "string"}
```

### GPT Response

```json
{
  "direction": "BUY",
  "sl": 2325.00,
  "tp": 2345.00,
  "confidence": 7,
  "reason": "M15 bullish trend with EMA21 above EMA50, MACD positive. M5 bounce at support confirmed with RSI recovering from oversold territory."
}
```

---

## 7. All Pre-Filters & Guards (in order)

This table shows every gate in execution order:

| # | Stage | Component | Condition | Action |
|---|---|---|---|---|
| 1 | Zone touch | PriceWatcher | proximity > 0.3% | No touch event |
| 2 | Zone touch | PriceWatcher | Zone in 15-min cooldown | No touch event |
| 3 | Queue | AnalysisAgent | Queue full (>10 events) | Drop event |
| 4 | Pre-filter 0a | AnalysisAgent | daily_count >= 2 | Drop, no GPT |
| 5 | Pre-filter 0b | AnalysisAgent | SignalTracker.should_pause | Drop, no GPT |
| 6 | Pre-filter 0c | AnalysisAgent | BUY acct + resistance zone | Drop, no GPT |
| 7 | Pre-filter 0d | AnalysisAgent | open_trades >= 1 | Drop, no GPT |
| 8 | Pre-filter 0e | AnalysisAgent | M15 trend vs zone mismatch | Drop, no GPT |
| 9 | Zone cooldown | AnalysisAgent | Same zone_id < 30s ago | Drop, no GPT |
| 10 | GPT result | AnalysisAgent | direction == "NONE" | No signal event |
| 11 | Post-GPT dir | AnalysisAgent | GPT dir != account dir | No signal event |
| 12 | Geometry | AnalysisAgent | sl/tp geometry invalid | No signal event |
| 13 | Risk dir | RiskAgent | Signal dir != allowed dir | Rejected |
| 14 | Risk count | RiskAgent | daily_count >= 2 | Rejected |
| 15 | Risk open | RiskAgent | open_trades >= 1 | Rejected |
| 16 | Risk corr | RiskAgent | Correlated pair open | Rejected |
| 17 | Risk P&L | RiskAgent | pnl <= -$30 or >= $100 | Rejected |
| 18 | Duplicate | Executor | Same symbol+dir+entry < 5s | Skip |

Filters 4–12 are in AnalysisAgent (prevent unnecessary GPT calls). Filters 13–17 are in RiskAgent (final gate before execution). Filter 18 is the executor's last line of defence.

---

## 8. Complete Sample Trade — End to End

Let's trace a real trade from price tick to closed P&L.

**Setup:** Account 2, BUY-only. XAUUSD current price: 2330.50.

---

### Step 0: Bot Startup (T = 0:00)

```
SRMapper starts
  ← get_candles_latest("XAUUSD", "M15", count=200)
  ← find_swing_highs_lows(df, lookback=5)
     Finds swing lows at: 2329.80, 2330.20, 2330.10
  ← cluster_zones([2329.80, 2330.10, 2330.20], "support", tolerance=0.0015)
     0.0015 × 2330 ≈ 3.50 points tolerance
     All 3 within 0.40 points → cluster into one zone:
       center   = (2329.80+2330.10+2330.20)/3 = 2330.03
       upper    = 2330.20 + 1.75 = 2331.95
       lower    = 2329.80 - 1.75 = 2328.05
       strength = 3
  → bus.publish(ZoneEvent(symbol="XAUUSD", tf="M15", zone_type="support",
                price_center=2330.03, upper=2331.95, lower=2328.05, strength=3))
  → DBConsumer writes zone to DB with zone_id=42

zones_ready.set()   ← PriceWatcher unblocked
```

---

### Step 1: Zone Touch (T = 0:05)

```
PriceWatcher tick loop (every 2s):
  price = get_symbol_price("XAUUSD")
  bid=2330.45, ask=2330.55, mid=2330.50

  zones = db.get_active_zones("XAUUSD")
  For zone_id=42 (center=2330.03):
    proximity = |2330.50 - 2330.03| / 2330.03 = 0.47 / 2330.03 = 0.0002 = 0.02%
    0.02% < ZONE_TOUCH_PCT (0.3%) → TOUCHED

  cooldown check: zone_id=42 not in _last_touch → no cooldown
  _last_touch[42] = now

  bus.publish(ZoneTouchEvent(
    symbol="XAUUSD",
    zone_type="support",
    price_center=2330.03,
    price_upper=2331.95,
    price_lower=2328.05,
    zone_strength=3,
    bid=2330.45,
    ask=2330.55,
    mid_price=2330.50,
    zone_id=42,
    timeframe="M15"
  ))
```

`DBConsumer` logs the touch to `events` table.  
`AnalysisAgent[acct2]` receives event in its queue.

---

### Step 2: AnalysisAgent Pre-Filters (T = 0:05)

```
AnalysisAgent[acct2] dequeues ZoneTouchEvent:

[Filter 0a] daily_trade_count for acct2 today = 0 < MAX_DAILY_TRADES (2) → PASS

[Filter 0b] signal_tracker.should_pause:
  len(results) = 8 < SIGNAL_TRACKER_WINDOW (20) → False → PASS

[Filter 0c] direction="BUY", zone_type="support" → support zone is good for BUY → PASS

[Filter 0d] open_trade_count for acct2 = 0 < MAX_OPEN_TRADES (1) → PASS

Fetch candles:
  m15_df = get_candles_latest("XAUUSD", "M15", 100)  ← newest-first
  m5_df  = get_candles_latest("XAUUSD", "M5",  100)  ← newest-first

  sort_candles_ascending(m15_df)  ← flip to oldest-first for indicators
  sort_candles_ascending(m5_df)

Compute M15 indicators:
  ema21_m15 = 2328.50
  ema50_m15 = 2325.00
  → EMA21 > EMA50 → M15 BULLISH

[Filter 0e] support zone + M15 bullish (EMA21 > EMA50) → PASS (not contradicting)

Compute M5 indicators:
  ema21_m5   = 2330.20
  ema50_m5   = 2329.50
  rsi14_m5   = 42.50
  macd_m5    = (0.85, 0.70, 0.15)

Zone cooldown check: last analysis of zone_id=42 was never → PASS

Build user prompt → call GPT:
  model=gpt-4o-mini, max_tokens=150
  [API call ~2-4s]

GPT response:
{
  "direction": "BUY",
  "sl": 2325.00,
  "tp": 2345.00,
  "confidence": 7,
  "reason": "M15 bullish trend with EMA21 above EMA50. M5 bounce at support with RSI recovering."
}

[Filter post-GPT-1] direction = "BUY" ≠ "NONE" → PASS

[Filter post-GPT-2] direction = "BUY" == account direction "BUY" → PASS

Re-fetch price:
  price = get_symbol_price("XAUUSD")
  ask = 2330.56 (use ask for BUY entry)
  entry = 2330.56

[Filter post-GPT-3] Geometry check:
  BUY: sl (2325.00) < entry (2330.56) < tp (2345.00) → PASS ✓

bus.publish(SignalGeneratedEvent(
  symbol="XAUUSD",
  direction="BUY",
  entry=2330.56,
  stop_loss=2325.00,   ← GPT's values, will be overridden by RiskAgent
  take_profit=2345.00, ← GPT's values, will be overridden by RiskAgent
  confidence=7.0,
  reasoning="M15 bullish trend...",
  zone_type="support",
  zone_center=2330.03,
  account_id=2,
  zone_id=42,
  ema21=2330.20, ema50=2329.50, rsi14=42.50,
  macd_line=0.85, macd_signal=0.70, macd_hist=0.15
))
```

`DBConsumer` inserts signal row + creates `validation_log` row (val_id=15).

---

### Step 3: RiskAgent (T = 0:05, synchronous on AnalysisAgent thread)

```
RiskAgent[acct2].on_signal(event):
  event.account_id (2) == self.account_id (2) → process

[Check 1] direction_ok: "BUY" == allowed_direction "BUY" → True
[Check 2] daily_count_ok: daily_count (0) < MAX_DAILY_TRADES (2) → True
[Check 3] max_trades_ok: open_trade_count (0) < MAX_OPEN_TRADES (1) → True
[Check 4] correlation_ok: no EURUSD/USDJPY open → True
[Check 5] daily_loss_ok: today_pnl ($0.00) between -$30 and $100 → True

All checks PASS → approved = True

_compute_lot_and_levels(event):
  tick_value = 0.1 (from get_symbol_info, or fallback)
  tick_size  = 0.01
  value_per_unit = 0.1 / 0.01 = 10.0

  gpt_sl_distance = |2330.56 - 2325.00| = 5.56

  raw_lot = 50.0 / (10.0 × 5.56) = 50.0 / 55.6 = 0.899

  clamp(0.899, min=0.05, max=0.10) → 0.10
  round_to_step(0.10, step=0.01)  → 0.10

  sl_dist = 50.0 / (0.10 × 10.0) = 50.0 / 1.0 = 50.00 pts
  tp_dist = 150.0 / (0.10 × 10.0) = 150.0 / 1.0 = 150.00 pts

  BUY:
    sl = 2330.56 - 50.00 = 2280.56
    tp = 2330.56 + 150.00 = 2480.56

bus.publish(RiskEvaluatedEvent(
  symbol="XAUUSD",
  direction="BUY",
  entry=2330.56,
  stop_loss=2280.56,   ← risk-adjusted (50 pts)
  take_profit=2480.56, ← risk-adjusted (150 pts)
  volume=0.10,
  approved=True,
  reason="All checks passed",
  account_id=2,
  rr_ok=True, max_trades_ok=True, correlation_ok=True,
  daily_loss_ok=True, daily_count_ok=True, direction_ok=True
))
```

`DBConsumer` inserts risk_decision row + updates validation_log row 15.

---

### Step 4: Executor (T = 0:05, synchronous)

```
Executor[acct2].on_risk_evaluated(event):
  event.account_id (2) == self.account_id (2) → process
  event.approved == True → process

Duplicate check: (XAUUSD, BUY, 2330.56) not seen in last 5s → PASS

EXECUTION_LIVE = True → LIVE mode

Step 1: client2.order.place_market_order(
  type="BUY",
  symbol="XAUUSDr",         ← resolve_symbol("XAUUSD") = "XAUUSD" + ""
  volume=0.10
)
Response: {error: 0, message: "OK", data: {order: 98765432}}
  position_id = 98765432
  fill_price = 2330.58     ← actual fill (may differ slightly from ask)

Step 2: client2.order.modify_position(
  98765432,
  stop_loss=2280.56,
  take_profit=2480.56
)
Response: {error: 0, message: "OK"}
  sl_tp_modified = True

bus.publish(TradeExecutedEvent(
  symbol="XAUUSD",
  direction="BUY",
  volume=0.10,
  entry=2330.56,
  stop_loss=2280.56,
  take_profit=2480.56,
  success=True,
  account_id=2,
  order_id=98765432,
  fill_price=2330.58,
  sl_tp_modified=True,
  dry_run=False
))
```

`DBConsumer` inserts trade row + updates validation_log row 15.  
`TradeMonitor[acct2]` adds `98765432` to its `_tracked` dict.

---

### Step 5: Trade Open (T = 0:05 to T = 2:15)

```
TradeMonitor[acct2] polls every 30s:
  get_all_positions() → [{order_id: 98765432, symbol: XAUUSD, ...}]
  tracked = {98765432: {...}}
  open_now = {98765432}
  closed_ids = {} (empty) → no action

  [continues polling...]

XAUUSD rises from 2330.58 → 2450.00 → TP hit at 2480.56
```

---

### Step 6: Trade Close (T = 2:15)

```
TradeMonitor[acct2] polls at T=2:15:
  get_all_positions() → []   ← empty! Position closed by MT5 TP hit
  tracked = {98765432}
  open_now = {}
  closed_ids = {98765432}

  _handle_closed(98765432):
    _get_close_deal(98765432):
      ← MT5 deal history (last 2 days)
      → close_price = 2480.56
      → realized_pnl = (2480.56 - 2330.58) × 0.10 × 10.0
                     = 149.98 × 0.10 × 10.0
                     = $149.98   ← slight difference due to fill vs. requested entry

    signal_tracker.record_result(signal_id=15, won=True)
      results = deque([..., True])
      win_rate = sum/len → if ≥ 40% and < 20 trades → should_pause = False

    bus.publish(TradeClosedEvent(
      symbol="XAUUSD",
      direction="BUY",
      volume=0.10,
      entry_price=2330.58,
      order_id=98765432,
      account_id=2,
      close_price=2480.56,
      realized_pnl=149.98
    ))
    del _tracked[98765432]
```

`DBConsumer`:
- `update_trade_close(98765432, close_price=2480.56, close_time=now, pnl=149.98)`
- `update_validation_log_close(val_id=15, close_price=2480.56, realized_pnl=149.98, won=True)`

---

### Step 7: Telegram Alert

If `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set, the Executor sends:

```
🟢 TRADE EXECUTED — Account 2
BUY XAUUSD 0.10 lots
Entry: 2330.58 | SL: 2280.56 | TP: 2480.56
Risk: $50.00 | Target: $150.00 | R:R: 3.0
```

---

### Trade Summary

| Field | Value |
|---|---|
| Account | 2 (BUY-only) |
| Symbol | XAUUSD |
| Direction | BUY |
| Entry (filled) | 2330.58 |
| SL | 2280.56 (-50 pts) |
| TP | 2480.56 (+150 pts) |
| Lot size | 0.10 |
| Risk | $50.00 |
| Target profit | $150.00 |
| Actual P&L | $149.98 |
| R:R | 3.0 |
| GPT confidence | 7/10 |
| Duration | ~2h 10min |
| Zone touched | Support @ 2330.03 (strength=3) |

---

## 9. Database Schema

```sql
-- Active S/R zones
CREATE TABLE zones (
  id INTEGER PRIMARY KEY,
  symbol TEXT, timeframe TEXT,
  zone_type TEXT,           -- "support" or "resistance"
  price_center REAL, price_upper REAL, price_lower REAL,
  strength INTEGER,         -- number of pivots clustered
  is_active INTEGER,        -- 0 or 1
  created_at TEXT, updated_at TEXT
);

-- Full event audit log (every published event)
CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  event_type TEXT, symbol TEXT, account_id INTEGER,
  payload TEXT,             -- JSON blob of full event
  created_at TEXT
);

-- GPT signals
CREATE TABLE signals (
  id INTEGER PRIMARY KEY,
  symbol TEXT, direction TEXT,
  entry REAL, stop_loss REAL, take_profit REAL,
  confidence REAL, reasoning TEXT,
  zone_type TEXT, zone_center REAL,
  account_id INTEGER, zone_id INTEGER,
  ema21 REAL, ema50 REAL, rsi14 REAL,
  macd_line REAL, macd_signal REAL, macd_hist REAL,
  created_at TEXT
);

-- Risk decisions
CREATE TABLE risk_decisions (
  id INTEGER PRIMARY KEY,
  symbol TEXT, direction TEXT, approved INTEGER,
  reason TEXT, volume REAL,
  entry REAL, stop_loss REAL, take_profit REAL,
  account_id INTEGER,
  rr_ok INT, max_trades_ok INT, daily_loss_ok INT,
  direction_ok INT, daily_count_ok INT,
  created_at TEXT
);

-- Executed trades
CREATE TABLE trades (
  id INTEGER PRIMARY KEY,
  symbol TEXT, direction TEXT, volume REAL,
  entry REAL, stop_loss REAL, take_profit REAL,
  success INTEGER, order_id INTEGER, fill_price REAL,
  account_id INTEGER, dry_run INTEGER,
  close_price REAL, close_time TEXT, realized_pnl REAL,
  created_at TEXT
);

-- Full pipeline funnel tracker (one row per signal)
CREATE TABLE validation_log (
  id INTEGER PRIMARY KEY,
  symbol TEXT, direction TEXT, zone_type TEXT,
  zone_strength INTEGER, timeframe TEXT, account_id INTEGER,
  signal_id INTEGER, signal_confidence REAL,
  risk_approved INTEGER, risk_reason TEXT, risk_volume REAL,
  trade_success INTEGER, trade_order_id INTEGER,
  close_price REAL, realized_pnl REAL, won INTEGER,
  created_at TEXT
);

-- Daily trade count resets
CREATE TABLE daily_count_resets (
  account_id INTEGER PRIMARY KEY,
  last_reset_date TEXT
);
```

---

## 10. Thread Map

| Thread Name | Owner | Purpose | Heartbeat Stale |
|---|---|---|---|
| `SRMapper` | SRMapper | Zone scan + 4h refresh | 150s |
| `PriceWatcher` | PriceWatcher | 2s tick loop | `TICK_INTERVAL × 5 = 10s` |
| `AnalysisAgent[1]` | AnalysisAgent acct1 | GPT call queue consumer | 60s |
| `AnalysisAgent[2]` | AnalysisAgent acct2 | GPT call queue consumer | 60s |
| `AnalysisAgent[3]` | AnalysisAgent acct3 | GPT call queue consumer | 60s |
| `AnalysisAgent[4]` | AnalysisAgent acct4 | GPT call queue consumer | 60s |
| `TradeMonitor[1]` | TradeMonitor acct1 | 30s close poll | 90s |
| `TradeMonitor[2]` | TradeMonitor acct2 | 30s close poll | 90s |
| `TradeMonitor[3]` | TradeMonitor acct3 | 30s close poll | 90s |
| `TradeMonitor[4]` | TradeMonitor acct4 | 30s close poll | 90s |
| `Watchdog` (main) | main/worker | 60s health check + reconnect | — |

**RiskAgent and Executor have no threads** — they are synchronous handlers on the AnalysisAgent thread.

---

## 11. Watchdog & Crash Recovery

**Watchdog loop (every 60s in worker.py):**
```python
for agent in [sr_mapper, price_watcher, *analysis_agents, *trade_monitors]:
    elapsed = time.time() - agent.last_heartbeat
    if elapsed > stale_threshold[type(agent)]:
        logger.error("Agent %s stale (%ds) — restarting thread", name, elapsed)
        agent.stop()
        agent.start()  # new thread

# MT5 reconnect check
if not client.is_connected():
    client.reconnect()
```

**Supervisor crash recovery (main.py, every 10s):**
```python
if workers[account_id].poll() is not None:  # process exited
    time.sleep(5)
    workers[account_id] = _spawn(account_id, direction)
```

**TradeMonitor restart recovery:**  
On `start()`, calls `_recover_open_positions()` → re-populates `_tracked` from MT5. This means even if the entire process restarts, open positions are re-detected and not lost.

---

## 12. Dashboard & Analytics

### Flask Dashboard (port 5001)

```bash
cd dashboard && python app.py
```

Opens SQLite with `PRAGMA query_only = ON` — cannot accidentally write.

| Route | What you see |
|---|---|
| `/` | Signal funnel, P&L gauges, recent trades |
| `/debug` | Full signal details: indicators, GPT output, geometry |
| `/visualizer` | Live candlestick chart (fetches from MT5 directly) |
| `/api/funnel` | zone touches → analysis → signals → risk approved → executed |
| `/api/pnl` | Today / week / all-time P&L with daily limit proximity |
| `/api/health` | Last-seen timestamp per agent |
| `/api/rejections` | 25 most recent risk rejections with R:R |
| `/api/debug/signals` | Full indicator snapshot per signal |

### Analytics Report (CLI)

```bash
python scripts/analytics_report.py
```

Produces: win rate, profit factor, avg R:R, expectancy, pipeline conversion rates (zone touch → execution %), breakdown by zone type / strength / timeframe / symbol, risk gate stats (approval rate, top rejection reasons).

**Pipeline conversion example:**
```
Zone Touches     → 100%
Analysis Started → 65%  (35% dropped by pre-filters 0a–0e)
Signals Emitted  → 40%  (GPT said NONE or geometry invalid)
Risk Approved    → 30%  (of all touches)
Trades Executed  → 29%  (nearly all approved trades execute)
```

---

## Quick Reference: What Happens When Price Hits a Zone

```
Price ticks every 2s
  └─ Within 0.3% of zone center?
       └─ Not in 15-min cooldown?
            └─ ZoneTouchEvent published
                 └─ AnalysisAgent[acctN] queues it
                      └─ Not at daily cap (0a)?
                           └─ Win rate not paused (0b)?
                                └─ Zone type matches direction (0c)?
                                     └─ No open trade (0d)?
                                          └─ Fetch candles + compute indicators
                                               └─ M15 trend supports zone (0e)?
                                                    └─ Call GPT-4o-mini
                                                         └─ direction ≠ NONE?
                                                              └─ dir matches account?
                                                                   └─ geometry valid?
                                                                        └─ SignalGeneratedEvent
                                                                             └─ RiskAgent: all 5 checks?
                                                                                  └─ Lot sizing
                                                                                       └─ RiskEvaluatedEvent(approved)
                                                                                            └─ Executor
                                                                                                 └─ place_market_order
                                                                                                      └─ modify_position(sl, tp)
                                                                                                           └─ TradeExecutedEvent
                                                                                                                └─ TradeMonitor tracks it
                                                                                                                     └─ (30s polls)
                                                                                                                          └─ Position closed?
                                                                                                                               └─ TradeClosedEvent
                                                                                                                                    └─ SignalTracker.record_result
                                                                                                                                         └─ DB updated
```
