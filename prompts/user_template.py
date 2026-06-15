"""
prompts/user_template.py — Builds the user-turn prompt sent to GPT-4o.

Passes M15 data (trend context) + M5 data (entry confirmation) for the
triggered symbol. Called by analysis_agent after fetching both timeframes.
"""

from typing import Dict, List


def build_user_prompt(
    symbol: str,
    zone: Dict,
    m5_indicators: Dict,
    m5_candles: list,
    m15_indicators: Dict,
    m15_candles: list,
    price: Dict,
) -> str:
    """
    Construct the user-turn prompt for GPT-4o signal generation.

    Args:
        symbol:        Instrument symbol, e.g. "EURUSD".
        zone:          Dict with keys: zone_type, price_center, price_upper,
                       price_lower, strength.
        m5_indicators: Dict from compute_all_indicators on M5 candles.
        m5_candles:    List of 5 most-recent M5 candle dicts (ascending).
        m15_indicators: Dict from compute_all_indicators on M15 candles.
        m15_candles:   List of 5 most-recent M15 candle dicts (ascending).
        price:         Dict with keys: bid, ask, mid_price.

    Returns:
        str: Formatted user prompt string ready to send to OpenAI.
    """
    zone_type    = zone.get("zone_type", "unknown")
    zone_center  = zone.get("price_center", 0.0)
    zone_upper   = zone.get("price_upper", 0.0)
    zone_lower   = zone.get("price_lower", 0.0)
    zone_strength = zone.get("strength", 1)

    bid       = price.get("bid", 0.0)
    ask       = price.get("ask", 0.0)
    mid_price = price.get("mid_price", (bid + ask) / 2 if bid and ask else 0.0)

    def _fmt_indicators(ind: Dict, label: str) -> str:
        ema21       = ind.get("ema21", 0.0)
        ema50       = ind.get("ema50", 0.0)
        rsi14       = ind.get("rsi14", 50.0)
        macd_line   = ind.get("macd_line", 0.0)
        macd_signal = ind.get("macd_signal", 0.0)
        macd_hist   = ind.get("macd_hist", 0.0)
        trend_label = "BULLISH" if ema21 > ema50 else "BEARISH"
        rsi_label   = "OVERBOUGHT" if rsi14 > 70 else ("OVERSOLD" if rsi14 < 30 else "NEUTRAL")
        macd_bias   = "BULLISH" if macd_hist > 0 else "BEARISH"
        return (
            f"## {label} Indicators\n"
            f"  EMA21:       {ema21:.5f}\n"
            f"  EMA50:       {ema50:.5f}\n"
            f"  EMA Trend:   {trend_label}\n"
            f"  RSI-14:      {rsi14:.2f}  [{rsi_label}]\n"
            f"  MACD Line:   {macd_line:.6f}\n"
            f"  MACD Signal: {macd_signal:.6f}\n"
            f"  MACD Hist:   {macd_hist:.6f}  [{macd_bias} momentum]"
        )

    def _fmt_candles(candles: list, label: str) -> str:
        lines = []
        for c in candles[-5:]:
            lines.append(
                f"  {c.get('time', '')} | O:{c.get('open', 0):.5f} "
                f"H:{c.get('high', 0):.5f} L:{c.get('low', 0):.5f} C:{c.get('close', 0):.5f}"
            )
        body = "\n".join(lines) if lines else "  (no data)"
        return f"## Recent {label} Candles (last 5, ascending)\n{body}"

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

{_fmt_indicators(m15_indicators, "M15 (Trend Context)")}

{_fmt_candles(m15_candles, "M15")}

{_fmt_indicators(m5_indicators, "M5 (Entry Confirmation)")}

{_fmt_candles(m5_candles, "M5")}

## Task
Price has just touched the {zone_type.upper()} zone at {zone_center:.5f} for {symbol}.
Use M15 for trend direction and M5 for entry confirmation.
Provide your trading signal as a strict JSON object.
"""
    return prompt
