"""
config.py — Central configuration for the trading bot.
Loads credentials from .env via python-dotenv.
All agent modules import from here; do NOT hard-code secrets elsewhere.
"""

import os
from dotenv import load_dotenv

# Load .env file from the project root (next to this file)
load_dotenv()

# ---------------------------------------------------------------------------
# Instruments & symbol resolution
# ---------------------------------------------------------------------------

# Core instruments to trade
INSTRUMENTS = ["XAUUSD", "EURUSD", "USDJPY", "BTCUSD"]

# Optional broker-specific suffix appended to every symbol (e.g. ".r", ".m", "").
# Set SYMBOL_SUFFIX="" if your broker uses plain names.
SYMBOL_SUFFIX = os.getenv("SYMBOL_SUFFIX", "")


def resolve_symbol(base_symbol: str) -> str:
    """
    Return the broker-decorated symbol name by appending SYMBOL_SUFFIX.

    Example:
        resolve_symbol("EURUSD") → "EURUSD" (suffix="")
        resolve_symbol("EURUSD") → "EURUSD.r" (suffix=".r")
    """
    if base_symbol == "BTCUSD":
        return f"BTC{SYMBOL_SUFFIX}"
    return f"{base_symbol}{SYMBOL_SUFFIX}"


# ---------------------------------------------------------------------------
# Timeframes
# ---------------------------------------------------------------------------

# Map human-readable labels → MT5 timeframe keys (must match metatrader_client Timeframe class)
TIMEFRAMES = {
    "5m":  "M5",
    "15m": "M15",
    "1h":  "H1",
    "4h":  "H4",
    "1d":  "D1",
}

# Fallback timeframe for the analysis agent when a ZoneTouchEvent doesn't carry
# its own timeframe (normally overridden dynamically by the touched zone's TF)
ANALYSIS_TF = "M5"

# Timeframes scanned by the S/R mapper (in order: smallest → largest)
SR_TIMEFRAMES = ["M5", "M15"]

# Number of candles fetched per timeframe for S/R mapping
SR_CANDLE_COUNT = 200

# Number of candles fetched for analysis
ANALYSIS_CANDLE_COUNT = 100

# ---------------------------------------------------------------------------
# Trading window — currently disabled, bot may trade any time
# ---------------------------------------------------------------------------


def is_trading_hours() -> bool:
    """
    Trading-hours enforcement is disabled — the bot is allowed to trade
    at any time, any day.
    """
    return True


# ---------------------------------------------------------------------------
# Risk parameters
# ---------------------------------------------------------------------------

# Minimum risk-to-reward ratio required to pass risk check
MIN_RR = 1.5

# Maximum simultaneous open trades across all instruments
MAX_OPEN_TRADES = 4

# Pairs considered correlated; both cannot be open at the same time.
# EURUSD and USDJPY both carry USD exposure — block having both open.
CORRELATED_PAIRS = [("EURUSD", "USDJPY")]

# Daily realized-P&L gates (in account-currency dollars). New trades are
# blocked once today's realized P&L reaches the profit target or breaches
# the loss limit; existing open positions are left to hit their own SL/TP.
DAILY_PROFIT_TARGET_USD = 100.0
DAILY_LOSS_LIMIT_USD    = 30.0

# Target notional value per trade: volume ≈ FIXED_TRADE_USD / current_price
FIXED_TRADE_USD = 10.0

# For BTC at current prices (~$65k+), FIXED_TRADE_USD always computes a raw lot
# far below volume_min (e.g. 0.000153 vs 0.01), so the notional formula is
# pointless — this flag short-circuits it and sets lot = volume_min directly.
USE_VOLUME_MIN_FLOOR = True

# Lot-size safety bounds — used as a fallback floor/ceiling if broker symbol
# info can't be read, and as a hard cap regardless of the computed volume
LOT_MIN = 0.01
LOT_MAX = 0.10

# Minimum GPT confidence score (0–100) required to publish a signal
MIN_CONFIDENCE = 65

# ---------------------------------------------------------------------------
# Trailing / breakeven stop parameters
# ---------------------------------------------------------------------------

# Fraction of TP distance at which SL is moved to entry (breakeven).
# e.g. 0.6 means "when price is 60% of the way from entry to TP, protect capital"
BREAKEVEN_TRIGGER_PCT: float = 0.6

# Fraction of TP distance at which the SL starts trailing the price.
# Must be >= BREAKEVEN_TRIGGER_PCT.
TRAIL_TRIGGER_PCT: float = 0.8

# Trail distance expressed as a fraction of the original risk (entry-to-SL distance).
# e.g. 0.5 means the trail buffer = half the original stop distance
TRAIL_DISTANCE_RATIO: float = 0.5

# ---------------------------------------------------------------------------
# Signal tracker — rolling performance gate
# ---------------------------------------------------------------------------

# Number of most-recent closed trades to track
SIGNAL_TRACKER_WINDOW: int = 10

# Win-rate threshold below which new signals are paused (0.0–1.0)
SIGNAL_PAUSE_THRESHOLD: float = 0.3

# ---------------------------------------------------------------------------
# S/R zone parameters
# ---------------------------------------------------------------------------

# Price must be within this fraction of a zone centre to trigger ZoneTouchEvent
ZONE_TOUCH_PCT = 0.001          # 0.1 %

# Zone touch detection mode:
#   "wick"  — trigger when the current bid/ask mid is within ZONE_TOUCH_PCT of the zone centre
#   "close" — trigger when the last closed candle's close price is within the zone's price bounds
ZONE_TOUCH_MODE: str = os.getenv("ZONE_TOUCH_MODE", "wick")
if ZONE_TOUCH_MODE not in ("wick", "close"):
    raise ValueError(
        f"ZONE_TOUCH_MODE must be 'wick' or 'close', got {ZONE_TOUCH_MODE!r}"
    )

# Per-zone per-instrument cooldown in minutes to prevent repeated triggers
ZONE_COOLDOWN_MIN = 15

# How often (hours) the S/R mapper refreshes zones
ZONE_REFRESH_HOURS = 4

# Rolling-window half-width for swing high/low detection (candles on each side)
SWING_LOOKBACK = 5

# Maximum price distance between pivots to merge them into the same zone (fraction)
CLUSTER_TOLERANCE = 0.002       # 0.2 %

# ---------------------------------------------------------------------------
# MT5 connection configuration
# ---------------------------------------------------------------------------

MT5_CONFIG = {
    "login":    int(os.getenv("MT5_LOGIN", "0")),
    "password": os.getenv("MT5_PASSWORD", ""),
    "server":   os.getenv("MT5_SERVER", ""),
    # Optional: uncomment and set if the terminal is not on the system PATH
    # "path": os.getenv("MT5_PATH", ""),
}

# ---------------------------------------------------------------------------
# OpenAI / GPT-4o
# ---------------------------------------------------------------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o")

# Maximum tokens for the GPT-4o response (7-field JSON + brief reasoning).
# 300 was too tight and caused JSON truncation on verbose reasoning — raised to 600.
OPENAI_MAX_TOKENS = 600

# ---------------------------------------------------------------------------
# Execution flag
# ---------------------------------------------------------------------------

# Set EXECUTION_LIVE=False (in .env or environment) to enable dry-run mode:
# orders are logged but NOT sent to the broker.
EXECUTION_LIVE = os.getenv("EXECUTION_LIVE", "True").lower() not in ("false", "0", "no")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

# SQLite file path (relative to project root)
DB_PATH = os.getenv("DB_PATH", "trading_bot.db")

# ---------------------------------------------------------------------------
# Tick / polling intervals
# ---------------------------------------------------------------------------

# Seconds between price-watcher ticks (per instrument)
TICK_INTERVAL_SEC = float(os.getenv("TICK_INTERVAL_SEC", "2.0"))
