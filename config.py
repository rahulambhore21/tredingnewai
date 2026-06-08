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
INSTRUMENTS = ["XAUUSD"]

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
MAX_OPEN_TRADES = 3

# Pairs considered correlated; both cannot be open at the same time
CORRELATED_PAIRS = []

# Daily realized-P&L gates (in account-currency dollars). New trades are
# blocked once today's realized P&L reaches the profit target or breaches
# the loss limit; existing open positions are left to hit their own SL/TP.
DAILY_PROFIT_TARGET_USD = 100.0
DAILY_LOSS_LIMIT_USD    = 30.0

# Target notional value per trade: volume ≈ FIXED_TRADE_USD / current_price
FIXED_TRADE_USD = 10.0

# Lot-size safety bounds — used as a fallback floor/ceiling if broker symbol
# info can't be read, and as a hard cap regardless of the computed volume
LOT_MIN = 0.01
LOT_MAX = 0.10

# Minimum GPT confidence score (0–100) required to publish a signal
MIN_CONFIDENCE = 65

# ---------------------------------------------------------------------------
# S/R zone parameters
# ---------------------------------------------------------------------------

# Price must be within this fraction of a zone centre to trigger ZoneTouchEvent
ZONE_TOUCH_PCT = 0.001          # 0.1 %

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

# Maximum tokens for the GPT-4o response (JSON signal is small)
OPENAI_MAX_TOKENS = 512

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
