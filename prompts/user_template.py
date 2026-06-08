"""
prompts/user_template.py — Builds the user-turn prompt sent to GPT-4o.

The analysis_agent calls build_user_prompt() to assemble the full context
string that accompanies the system prompt.  Keeping prompt construction here
makes it easy to iterate on the wording without touching agent logic.
"""

from typing import Dict, Optional


def build_user_prompt(
    symbol: str,
    zone: Dict,
    indicators: Dict,
    recent_candles: list,
    price: Dict,
    analysis_tf: str = "M5",
) -> str:
    """
    Construct the user-turn prompt for GPT-4o signal generation.

    Args:
        symbol:         Instrument symbol, e.g. "BTCUSD".
        zone:           Dict with keys: zone_type, price_center, price_upper,
                        price_lower, strength.
        indicators:     Dict with keys: ema21, ema50, rsi14, macd_line,
                        macd_signal, macd_hist.
        recent_candles: List of the 5 most-recent candle dicts (time, open,
                        high, low, close) for context on recent price action.
                        Entries are in ascending time order (oldest first).
        price:          Dict with keys: bid, ask, mid_price (current live price).
        analysis_tf:    Timeframe label the candles/indicators were computed on
                        (e.g. "M5" or "M15") — the timeframe of the touched zone.

    Returns:
        str: Formatted user prompt string ready to send to OpenAI.
    """
    # --- Zone section ---
    zone_type    = zone.get("zone_type", "unknown")
    zone_center  = zone.get("price_center", 0.0)
    zone_upper   = zone.get("price_upper", 0.0)
    zone_lower   = zone.get("price_lower", 0.0)
    zone_strength = zone.get("strength", 1)

    # --- Indicators section ---
    ema21       = indicators.get("ema21", 0.0)
    ema50       = indicators.get("ema50", 0.0)
    rsi14       = indicators.get("rsi14", 50.0)
    macd_line   = indicators.get("macd_line", 0.0)
    macd_signal = indicators.get("macd_signal", 0.0)
    macd_hist   = indicators.get("macd_hist", 0.0)

    # --- Current price ---
    bid       = price.get("bid", 0.0)
    ask       = price.get("ask", 0.0)
    mid_price = price.get("mid_price", (bid + ask) / 2 if bid and ask else 0.0)

    # --- Recent candles (last 5, formatted) ---
    candle_lines = []
    for c in recent_candles[-5:]:
        candle_lines.append(
            f"  {c.get('time', '')} | O:{c.get('open', 0):.5f} "
            f"H:{c.get('high', 0):.5f} L:{c.get('low', 0):.5f} C:{c.get('close', 0):.5f}"
        )
    candles_section = "\n".join(candle_lines) if candle_lines else "  (no candle data)"

    # --- Trend summary helper ---
    trend_label = "BULLISH" if ema21 > ema50 else "BEARISH"
    rsi_label   = (
        "OVERBOUGHT" if rsi14 > 70 else
        "OVERSOLD"   if rsi14 < 30 else
        "NEUTRAL"
    )
    macd_bias = "BULLISH" if macd_hist > 0 else "BEARISH"

    prompt = f"""Instrument: {symbol}

## Current Price
  Bid:   {bid:.5f}
  Ask:   {ask:.5f}
  Mid:   {mid_price:.5f}

## S/R Zone Being Tested
  Type:     {zone_type.upper()}
  Center:   {zone_center:.5f}
  Upper:    {zone_upper:.5f}
  Lower:    {zone_lower:.5f}
  Strength: {zone_strength} pivots

## Technical Indicators ({analysis_tf} timeframe, last 100 candles)
  EMA21:       {ema21:.5f}
  EMA50:       {ema50:.5f}
  EMA Trend:   {trend_label} (EMA21 {'>' if ema21 > ema50 else '<'} EMA50)
  RSI-14:      {rsi14:.2f}  [{rsi_label}]
  MACD Line:   {macd_line:.6f}
  MACD Signal: {macd_signal:.6f}
  MACD Hist:   {macd_hist:.6f}  [{macd_bias} momentum]

## Recent Price Action (last 5 {analysis_tf} candles, ascending)
{candles_section}

## Task
Price has just touched the {zone_type.upper()} zone at {zone_center:.5f} for {symbol}.
Based on the above data, provide your trading signal as a strict JSON object.
"""
    return prompt
