"""
scripts/backtest_runner.py — Historical backtest skeleton.

ARCHITECTURE OVERVIEW
---------------------
The live bot pipeline:

    SRMapper → PriceWatcher → AnalysisAgent → RiskAgent → Executor → TradeMonitor

For backtesting, each stage is replaced with a deterministic replay layer:

    HistoricalDataLoader        replaces MT5Client.market.get_candles_latest()
    ZoneReplayer                replaces SRMapper (reconstruct zones from history)
    TouchReplayer               replaces PriceWatcher (replay candle-by-candle touches)
    SimulatedAnalysisAgent      either calls real GPT-4o or replays logged decisions
    SimulatedRiskAgent          runs the real RiskAgent checks against simulated state
    PaperExecutor               replaces Executor (logs fills, no MT5 call)
    SimulatedTradeMonitor       applies SL/TP/trailing on historical price ticks

HOW TO RUN (once fully implemented)
------------------------------------
    python scripts/backtest_runner.py \
        --symbol XAUUSD \
        --start  2024-01-01 \
        --end    2024-06-01 \
        --tf     M15 \
        --use-cached-ai      # replay logged GPT decisions instead of calling API

IMPLEMENTATION STEPS
---------------------
Step 1 — HistoricalDataLoader
    - Accept (symbol, timeframe, start, end) → pd.DataFrame (ascending).
    - Source: CSV export from MT5 platform, or broker history API.
    - Key constraint: same column names as live MT5 client
      (time, open, high, low, close, volume), ascending order.

Step 2 — ZoneReplayer
    - For each replay_candle_index, use only candles[0:index] to reconstruct
      S/R zones (same swing_lookback and cluster_tolerance as config).
    - Refresh zones every ZONE_REFRESH_CANDLES candles to mirror the 4-hour refresh.
    - Output: List of zone dicts matching ZoneEvent structure.

Step 3 — TouchReplayer
    - Walk candles one-by-one. For each candle, check if high/low enters any
      active zone (same ZONE_TOUCH_PCT logic as PriceWatcher).
    - On touch: emit ZoneTouchEvent with bid=close, ask=close, mid_price=close.

Step 4 — SimulatedAnalysisAgent
    Mode A (live GPT): Use the real AnalysisAgent with the historical candles
      injected via a mock MT5Client. Useful for strategy research.
    Mode B (cached): Query the events table for SignalGeneratedEvent payloads
      for the same zone_id + touch_time window. Replay the original decision.
      Much cheaper — no API cost.

Step 5 — SimulatedRiskAgent
    - Run the real _check_rr / _check_correlation logic.
    - Simulated open-trade state: dict tracking currently-open paper positions.
    - Simulated daily P&L: accumulated from closed paper positions.

Step 6 — PaperExecutor
    - Accept an approved RiskEvaluatedEvent.
    - Record entry at next candle open (market order approximation).
    - Return a TradeExecutedEvent with fill_price = next_open.

Step 7 — SimulatedTradeMonitor
    - For each paper position, walk forward candles.
    - On each candle: check if SL/TP was hit (use low for SL, high for TP on BUY).
    - Apply breakeven and trailing logic identically to TradeMonitor._manage_stops().
    - Close position with realized P&L and emit TradeClosedEvent.

Step 8 — Results
    - All events flow through the real DBConsumer into a backtest-specific
      SQLite file (e.g., backtest_2024.db).
    - Run scripts/analytics_report.py --db backtest_2024.db to see results.

CURRENT STATUS: SKELETON (no live code yet)
-------------------------------------------
The classes below define the interfaces.  Implement each stub to activate the
backtest.  The event bus, database, and analytics engine are all reusable as-is.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 1 — Historical data loader
# ---------------------------------------------------------------------------

class HistoricalDataLoader:
    """
    Load OHLCV candle data from a CSV file exported from MT5.

    CSV format expected (MT5 platform export):
        <DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<TICKVOL>

    Usage:
        loader = HistoricalDataLoader("data/XAUUSD_M15_2024.csv")
        df = loader.load("2024-01-01", "2024-06-01")  # → ascending DataFrame
    """

    def __init__(self, csv_path: str) -> None:
        self._path = csv_path

    def load(self, start: str, end: str) -> pd.DataFrame:
        """
        Return candles in ascending time order between *start* and *end* (ISO dates).
        Column names must match: time, open, high, low, close, volume.
        """
        raise NotImplementedError("Implement: parse CSV, filter date range, return ascending df")


# ---------------------------------------------------------------------------
# Step 2 — Zone reconstructor
# ---------------------------------------------------------------------------

class ZoneReplayer:
    """
    Reconstruct S/R zones from a historical candle slice.

    Call rebuild(candles_so_far) to get the active zones as of that point in
    history.  Uses the same indicator functions as SRMapper.
    """

    def rebuild(self, candles: pd.DataFrame, timeframe: str) -> List[Dict]:
        """
        Return list of zone dicts from the candle slice.

        Raises NotImplementedError — implement using find_swing_highs_lows
        and cluster_zones from indicators/calculator.py.
        """
        raise NotImplementedError(
            "Implement: call find_swing_highs_lows(candles) → cluster_zones() "
            "with the same config params as SRMapper._scan_symbol_tf()"
        )


# ---------------------------------------------------------------------------
# Step 3 — Touch replayer
# ---------------------------------------------------------------------------

class TouchReplayer:
    """
    Walk historical candles bar-by-bar and emit synthetic ZoneTouchEvents.

    For each candle:
      - Check if candle high/low enters any active zone.
      - Use config.ZONE_TOUCH_PCT tolerance.
      - Respect zone cooldowns (config.ZONE_COOLDOWN_MIN).
    """

    def find_touches(
        self,
        candle: pd.Series,
        zones: List[Dict],
        last_touch_times: Dict[int, datetime],
    ) -> List[Dict]:
        """
        Return a list of zone dicts that were touched during *candle*.
        *last_touch_times* maps zone_id → last_touch_datetime for cooldown.

        Raises NotImplementedError.
        """
        raise NotImplementedError(
            "Implement: for each zone check if candle.high >= zone.price_lower "
            "and candle.low <= zone.price_upper (for support), etc."
        )


# ---------------------------------------------------------------------------
# Step 4 — Simulated analysis (mock MT5 client)
# ---------------------------------------------------------------------------

class MockMT5Client:
    """
    Minimal MT5Client replacement for backtesting.

    Serves pre-loaded historical candles to AnalysisAgent instead of live data.
    All write methods (place_market_order, modify_position) raise NotImplementedError
    to prevent accidental live calls during a backtest.
    """

    def __init__(self, candles: pd.DataFrame) -> None:
        self._candles = candles
        self.market   = self._MarketStub(candles)
        self.order    = self._OrderStub()
        self.account  = self._AccountStub()

    class _MarketStub:
        def __init__(self, candles: pd.DataFrame) -> None:
            self._candles = candles

        def get_candles_latest(self, symbol: str, timeframe: str, count: int = 100) -> pd.DataFrame:
            return self._candles.tail(count)

        def get_symbol_price(self, symbol: str) -> Dict:
            last = self._candles.iloc[-1]
            return {"bid": last["close"], "ask": last["close"], "last": last["close"]}

    class _OrderStub:
        def get_all_positions(self) -> Optional[pd.DataFrame]:
            return None  # paper executor manages position state separately

    class _AccountStub:
        def get_balance(self) -> float:
            return 10_000.0

        def get_equity(self) -> float:
            return 10_000.0


# ---------------------------------------------------------------------------
# Step 5 — Paper executor
# ---------------------------------------------------------------------------

class PaperExecutor:
    """
    Records fills without sending orders to a broker.

    fill_price = next candle's open price (market order approximation).
    """

    def execute(
        self,
        direction: str,
        symbol: str,
        volume: float,
        entry: float,
        stop_loss: float,
        take_profit: float,
        next_open: float,
    ) -> Dict:
        """
        Return a dict matching TradeExecutedEvent fields.
        *next_open* is the open of the candle immediately following the signal.
        """
        raise NotImplementedError(
            "Implement: return TradeExecutedEvent-compatible dict with "
            "fill_price=next_open, success=True, order_id=<synthetic int>"
        )


# ---------------------------------------------------------------------------
# Step 6 — Simulated trade monitor (SL/TP evaluation)
# ---------------------------------------------------------------------------

class SimulatedTradeMonitor:
    """
    Walks forward through historical candles to determine the exit of a paper trade.

    For BUY:
        - SL hit: candle low <= stop_loss  → close at stop_loss (worst case)
        - TP hit: candle high >= take_profit → close at take_profit

    For SELL:
        - SL hit: candle high >= stop_loss  → close at stop_loss
        - TP hit: candle low  <= take_profit → close at take_profit

    When both SL and TP are hit in the same candle, we assume SL hit first
    (conservative approach — matches live slippage reality).

    Trailing/breakeven logic: apply the same percentage triggers as
    TradeMonitor._manage_stops() but on the simulated price series.
    """

    def evaluate(
        self,
        entry: Dict,
        future_candles: pd.DataFrame,
    ) -> Dict:
        """
        Walk *future_candles* to find when the trade closed and return:
            {close_price, realized_pnl, close_index, trade_result}

        Raises NotImplementedError.
        """
        raise NotImplementedError(
            "Implement: walk candles, check SL/TP, apply breakeven/trailing, "
            "return close details"
        )


# ---------------------------------------------------------------------------
# Main backtest runner (skeleton)
# ---------------------------------------------------------------------------

def run_backtest(
    symbol: str,
    csv_path: str,
    start: str,
    end: str,
    timeframe: str = "M15",
    use_cached_ai: bool = False,
    output_db: str = "backtest_result.db",
) -> None:
    """
    Coordinate the full backtest pipeline.

    Args:
        symbol:        Base symbol (e.g. "XAUUSD").
        csv_path:      Path to exported MT5 CSV file.
        start:         ISO date string for backtest start.
        end:           ISO date string for backtest end.
        timeframe:     MT5 timeframe key ("M5", "M15", "H1", ...).
        use_cached_ai: If True, replay logged AI decisions from events table
                       instead of calling the OpenAI API.
        output_db:     SQLite file to write backtest results to.

    Steps once all stubs are implemented:
        1. Load candles via HistoricalDataLoader.
        2. Initialise EventBus + Database(output_db) + DBConsumer.
        3. For each candle index:
           a. Rebuild zones from candles[0:index] every ZONE_REFRESH_CANDLES.
           b. Run TouchReplayer.find_touches(candle, zones).
           c. For each touch: call AnalysisAgent (real or cached).
           d. Run SimulatedRiskAgent.evaluate(signal, simulated_state).
           e. If approved: PaperExecutor.execute(...) to get fill.
           f. SimulatedTradeMonitor.evaluate(fill, future_candles) to close.
           g. Publish TradeClosedEvent — db_consumer persists it.
        4. Print analytics_report.py on output_db.
    """
    print(f"Backtest skeleton — implement the stubs above to activate.")
    print(f"  symbol={symbol}  tf={timeframe}  {start} → {end}")
    print(f"  output_db={output_db}")
    print(f"  use_cached_ai={use_cached_ai}")
    print()
    print("Implementation checklist:")
    checklist = [
        "[ ] HistoricalDataLoader.load()",
        "[ ] ZoneReplayer.rebuild()",
        "[ ] TouchReplayer.find_touches()",
        "[ ] MockMT5Client wired to AnalysisAgent",
        "[ ] PaperExecutor.execute()",
        "[ ] SimulatedTradeMonitor.evaluate()",
        "[ ] Main candle-walk loop",
        "[ ] Wire DBConsumer to output_db",
    ]
    for item in checklist:
        print(f"  {item}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backtest runner skeleton")
    parser.add_argument("--symbol",   default="XAUUSD")
    parser.add_argument("--csv",      default="data/XAUUSD_M15.csv")
    parser.add_argument("--start",    default="2024-01-01")
    parser.add_argument("--end",      default="2024-06-01")
    parser.add_argument("--tf",       default="M15")
    parser.add_argument("--use-cached-ai", action="store_true")
    parser.add_argument("--output-db", default="backtest_result.db")
    args = parser.parse_args()

    run_backtest(
        symbol=args.symbol,
        csv_path=args.csv,
        start=args.start,
        end=args.end,
        timeframe=args.tf,
        use_cached_ai=args.use_cached_ai,
        output_db=args.output_db,
    )
