"""
indicators/calculator.py — Pure-function technical indicator library.

No MT5 dependency — all functions operate on pandas Series / DataFrames.
This makes unit-testing trivial and keeps the indicators reusable.

Key rule from the plan:
    metatrader_client returns candle DataFrames sorted NEWEST-FIRST.
    Always call sort_candles_ascending() before computing indicators so that
    pandas rolling/ewm operations proceed in chronological order.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Candle helpers
# ---------------------------------------------------------------------------

def sort_candles_ascending(df: pd.DataFrame) -> pd.DataFrame:
    """
    Re-sort a candle DataFrame into ascending time order (oldest first).

    metatrader_client.market.get_candles_latest() returns rows newest-first.
    This helper must be called before any indicator computation so that
    rolling / ewm windows work correctly.

    Args:
        df: Candle DataFrame with a 'time' column (datetime or sortable).

    Returns:
        DataFrame sorted ascending by 'time', index reset.
    """
    return df.sort_values("time", ascending=True).reset_index(drop=True)


# ---------------------------------------------------------------------------
# EMA
# ---------------------------------------------------------------------------

def ema(series: pd.Series, period: int) -> pd.Series:
    """
    Exponential Moving Average using pandas ewm (span=period).

    pandas ewm uses the standard EMA formula:
        alpha = 2 / (period + 1)
    adjust=False matches charting-platform behaviour (recursive, not corrective).

    Args:
        series: Numeric series in ascending chronological order.
        period: Smoothing period (e.g. 21, 50).

    Returns:
        pd.Series of EMA values aligned to the input index.
    """
    return series.ewm(span=period, adjust=False).mean()


def compute_ema21(df: pd.DataFrame) -> float:
    """Return the latest EMA-21 value from a candle DataFrame."""
    series = sort_candles_ascending(df)["close"]
    return float(ema(series, 21).iloc[-1])


def compute_ema50(df: pd.DataFrame) -> float:
    """Return the latest EMA-50 value from a candle DataFrame."""
    series = sort_candles_ascending(df)["close"]
    return float(ema(series, 50).iloc[-1])


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Relative Strength Index (Wilder's smoothing via ewm).

    Uses exponential moving average of gains and losses (alpha = 1/period),
    which matches the original Wilder RSI and most trading platforms.

    Args:
        series: Closing prices in ascending chronological order.
        period: RSI period (default 14).

    Returns:
        pd.Series of RSI values (0–100); first *period* rows may be NaN.
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    # Wilder smoothing: EMA with alpha = 1/period
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()

    # When avg_loss is exactly 0 (pure uptrend), RS = infinity → RSI = 100.
    # We replace 0 with NaN for the division, then fill those positions with 100.
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    # Fill any NaN that arose from 0-denominator: if avg_gain > 0 → RSI 100, else RSI 0
    nan_mask = rsi_series.isna()
    rsi_series = rsi_series.copy()
    rsi_series[nan_mask & (avg_gain > 0)] = 100.0
    rsi_series[nan_mask & (avg_gain <= 0)] = 0.0
    return rsi_series


def compute_rsi14(df: pd.DataFrame) -> float:
    """Return the latest RSI-14 value from a candle DataFrame."""
    series = sort_candles_ascending(df)["close"]
    return float(rsi(series, 14).iloc[-1])


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------

def macd(
    series: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    MACD indicator: MACD line, signal line, and histogram.

    Uses pandas ewm with adjust=False for both fast and slow EMAs.

    Args:
        series:        Closing prices in ascending chronological order.
        fast:          Fast EMA period (default 12).
        slow:          Slow EMA period (default 26).
        signal_period: Signal-line EMA period (default 9).

    Returns:
        Tuple of (macd_line, signal_line, histogram) pd.Series.
    """
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_macd(df: pd.DataFrame) -> Tuple[float, float, float]:
    """
    Return the latest (macd_line, signal_line, histogram) values.

    Args:
        df: Candle DataFrame (will be sorted ascending internally).

    Returns:
        Tuple[float, float, float]: (macd, signal, histogram) at latest bar.
    """
    series = sort_candles_ascending(df)["close"]
    ml, sl, hist = macd(series)
    return float(ml.iloc[-1]), float(sl.iloc[-1]), float(hist.iloc[-1])


# ---------------------------------------------------------------------------
# S/R pivot detection
# ---------------------------------------------------------------------------

def find_swing_highs_lows(
    df: pd.DataFrame,
    lookback: int = config.SWING_LOOKBACK,
) -> Tuple[List[float], List[float]]:
    """
    Detect swing high and swing low prices using a rolling-window pivot method.

    A bar at index i is a swing high if its 'high' is strictly greater than
    the *lookback* bars on either side.  Similarly for swing lows using 'low'.

    This is a simple, allocation-free fractal method that works on any timeframe.

    Args:
        df:       Candle DataFrame with 'high' and 'low' columns, ascending order.
        lookback: Number of bars on each side to compare (default from config).

    Returns:
        Tuple[List[float], List[float]]: (swing_highs, swing_lows) price lists.
    """
    df = sort_candles_ascending(df)
    highs: List[float] = []
    lows:  List[float] = []
    n = len(df)

    for i in range(lookback, n - lookback):
        window_highs = df["high"].iloc[i - lookback: i + lookback + 1]
        window_lows  = df["low"].iloc[i - lookback: i + lookback + 1]

        bar_high = df["high"].iloc[i]
        bar_low  = df["low"].iloc[i]

        # Swing high: this bar's high must be strictly greater than ALL
        # surrounding bars (2*lookback neighbours). Requiring only >= lookback
        # allows flat tops where some neighbours equal the high, producing
        # false S/R zones. Requiring >= 2*lookback ensures all others are lower.
        if bar_high == window_highs.max() and (window_highs < bar_high).sum() >= 2 * lookback:
            highs.append(float(bar_high))

        # Swing low: symmetric condition — strictly less than all neighbours.
        if bar_low == window_lows.min() and (window_lows > bar_low).sum() >= 2 * lookback:
            lows.append(float(bar_low))

    return highs, lows


# ---------------------------------------------------------------------------
# Zone clustering
# ---------------------------------------------------------------------------

def cluster_zones(
    prices: List[float],
    zone_type: str,
    tolerance: float = config.CLUSTER_TOLERANCE,
) -> List[Dict]:
    """
    Group nearby pivot prices into S/R zones.

    Algorithm:
        1. Sort prices.
        2. Greedily merge consecutive prices that are within *tolerance* of the
           first price in the current group.
        3. Each group becomes a zone with:
            - center  = mean of the group
            - upper   = max + half_tol
            - lower   = min - half_tol
            - strength = number of pivots that merged into this zone

    Args:
        prices:    List of pivot price levels (swing highs or lows).
        zone_type: "resistance" or "support" (stored for the ZoneEvent).
        tolerance: Fractional proximity threshold (e.g. 0.002 = 0.2 %).

    Returns:
        List of dicts with keys:
            price_center, price_upper, price_lower, strength, zone_type.
    """
    if not prices:
        return []

    sorted_prices = sorted(prices)
    zones: List[Dict] = []
    group: List[float] = [sorted_prices[0]]

    for price in sorted_prices[1:]:
        # Compare price to the anchor (first element of current group)
        anchor = group[0]
        if abs(price - anchor) / anchor <= tolerance:
            group.append(price)
        else:
            # Flush the current group as a zone
            zones.append(_make_zone(group, zone_type, tolerance))
            group = [price]

    # Flush the last group
    if group:
        zones.append(_make_zone(group, zone_type, tolerance))

    return zones


def _make_zone(group: List[float], zone_type: str, tolerance: float) -> Dict:
    """
    Convert a list of merged pivot prices into a zone dict.

    Args:
        group:     Sorted list of prices in this cluster.
        zone_type: "resistance" or "support".
        tolerance: Fractional half-width added to upper/lower boundaries.

    Returns:
        Dict with price_center, price_upper, price_lower, strength, zone_type.
    """
    center = float(np.mean(group))
    half_tol = center * tolerance
    return {
        "price_center": center,
        "price_upper":  float(max(group)) + half_tol,
        "price_lower":  float(min(group)) - half_tol,
        "strength":     len(group),
        "zone_type":    zone_type,
    }


# ---------------------------------------------------------------------------
# All-in-one indicator snapshot (used by analysis_agent)
# ---------------------------------------------------------------------------

def compute_all_indicators(df: pd.DataFrame) -> Dict:
    """
    Compute EMA21, EMA50, RSI14, and MACD(12,26,9) in a single call.

    Accepts a raw candle DataFrame as returned by metatrader_client
    (newest-first). Internally sorts ascending before computing.

    Args:
        df: Candle DataFrame with at least a 'close' column.

    Returns:
        Dict with keys: ema21, ema50, rsi14, macd_line, macd_signal, macd_hist.
        Each indicator is computed independently so a MACD failure does not
        zero out the EMA values that the trend filter depends on.
    """
    result: Dict = {
        "ema21": 0.0, "ema50": 0.0,
        "rsi14": 50.0,
        "macd_line": 0.0, "macd_signal": 0.0, "macd_hist": 0.0,
    }

    try:
        asc   = sort_candles_ascending(df)
        close = asc["close"]
    except Exception:
        logger.exception("compute_all_indicators: failed to sort/extract close — returning defaults")
        return result

    # EMA — critical: 0.0 triggers the early-return guard in AnalysisAgent
    try:
        result["ema21"] = float(ema(close, 21).iloc[-1])
        result["ema50"] = float(ema(close, 50).iloc[-1])
    except Exception:
        logger.exception("compute_all_indicators: EMA computation failed")

    # RSI — non-critical; GPT receives neutral 50.0 default on failure
    try:
        result["rsi14"] = float(rsi(close, 14).iloc[-1])
    except Exception:
        logger.exception("compute_all_indicators: RSI computation failed")

    # MACD — non-critical; GPT receives 0.0 defaults on failure
    try:
        ml, sl, hist          = macd(close)
        result["macd_line"]   = float(ml.iloc[-1])
        result["macd_signal"] = float(sl.iloc[-1])
        result["macd_hist"]   = float(hist.iloc[-1])
    except Exception:
        logger.exception("compute_all_indicators: MACD computation failed")

    return result
