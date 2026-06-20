"""
config.py — Central configuration for the trading bot.
Loads credentials from .env via python-dotenv.
All agent modules import from here; do NOT hard-code secrets elsewhere.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Instruments & symbol resolution
# ---------------------------------------------------------------------------

SYMBOLS = ["XAUUSD"]

SYMBOL_SUFFIX = os.getenv("SYMBOL_SUFFIX", "")


def resolve_symbol(base_symbol: str) -> str:
    """Return the broker-decorated symbol name by appending SYMBOL_SUFFIX."""
    return f"{base_symbol}{SYMBOL_SUFFIX}"


# ---------------------------------------------------------------------------
# Timeframes
# ---------------------------------------------------------------------------

SR_TIMEFRAMES = ["M5", "M15"]

SR_CANDLE_COUNT = 200
ANALYSIS_CANDLE_COUNT = 100


def is_trading_hours() -> bool:
    return True


# ---------------------------------------------------------------------------
# Risk parameters
# ---------------------------------------------------------------------------

MIN_RR = 2
MAX_OPEN_TRADES = 1
CORRELATED_PAIRS = [("EURUSD", "USDJPY")]

DAILY_PROFIT_TARGET_USD = 100.0
DAILY_LOSS_LIMIT_USD    = 30.0

RISK_PER_TRADE_USD = 10.0  # legacy; per-account sizing now uses SL_USD/TP_USD

# Fixed per-trade size parameters for 4-account mode
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "2"))
LOT_MIN          = float(os.getenv("LOT_MIN", "0.05"))
LOT_MAX          = float(os.getenv("LOT_MAX", "0.10"))
SL_USD           = float(os.getenv("SL_USD",  "50.0"))
TP_USD           = float(os.getenv("TP_USD", "150.0"))

# Fallback tick values if get_symbol_info is unavailable.
# These are only used when the live get_symbol_info call fails, so the best
# estimate of what it WOULD have returned is the broker's own observed values
# (see trading_bot.log). The figure that matters for lot sizing is the ratio
# tick_value/tick_size = account-currency loss per 1.0 price-unit move per lot:
#   EURUSD  1.0 / 0.00001 = 100,000  (5-digit feed, standard 100k contract)
#   USDJPY  0.62 / 0.001  ≈ 620      (3-digit JPY feed; price-dependent, ~JPY150)
#   XAUUSD  0.1 / 0.01    = 10       (matches the live broker's observed ratio)
TICK_VALUE_FALLBACK = {
    "EURUSD": {"tick_value": 1.0,  "tick_size": 0.00001},
    "USDJPY": {"tick_value": 0.62, "tick_size": 0.001},
    "XAUUSD": {"tick_value": 0.1,  "tick_size": 0.01},
}

# ---------------------------------------------------------------------------
# S/R zone parameters
# ---------------------------------------------------------------------------

ZONE_TOUCH_PCT    = 0.003
ZONE_COOLDOWN_MIN = 15
ZONE_REFRESH_HOURS = 4
SWING_LOOKBACK    = 5
CLUSTER_TOLERANCE = 0.0015

# ---------------------------------------------------------------------------
# 4-account configuration
# ---------------------------------------------------------------------------

def _load_account_configs():
    accounts = []
    for i in range(1, 5):
        login_str = os.getenv(f"MT5_LOGIN_{i}", "")
        if not login_str:
            continue
        try:
            login_int = int(login_str)
        except ValueError:
            continue
        accounts.append({
            "account_id": i,
            "login":      login_int,
            "password":   os.getenv(f"MT5_PASSWORD_{i}", ""),
            "server":     os.getenv(f"MT5_SERVER_{i}", ""),
            "direction":  os.getenv(f"MT5_DIRECTION_{i}", "BUY").upper(),
        })
    return accounts


ACCOUNTS = _load_account_configs()

# ---------------------------------------------------------------------------
# OpenAI / GPT-4o
# ---------------------------------------------------------------------------

OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL      = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_MAX_TOKENS = 150

# ---------------------------------------------------------------------------
# Execution flag
# ---------------------------------------------------------------------------

EXECUTION_LIVE = os.getenv("EXECUTION_LIVE", "True").lower() not in ("false", "0", "no")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_PATH = os.getenv("DB_PATH", "trading_bot.db")

# ---------------------------------------------------------------------------
# Signal tracker (rolling win-rate gate)
# ---------------------------------------------------------------------------

SIGNAL_TRACKER_WINDOW     = 20    # number of recent trades to evaluate
SIGNAL_PAUSE_THRESHOLD    = 0.40  # pause new signals if win rate falls below 40%

# ---------------------------------------------------------------------------
# Tick / polling intervals
# ---------------------------------------------------------------------------

TICK_INTERVAL_SEC = float(os.getenv("TICK_INTERVAL_SEC", "2.0"))
