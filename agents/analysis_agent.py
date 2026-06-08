"""
agents/analysis_agent.py — GPT-4o signal generation agent.

Subscribes to ZoneTouchEvent.
Processes events on its own dedicated thread so GPT API calls (2–10 s each)
never block the PriceWatcher tick loop.

Fix applied: queue-based design — _on_zone_touch() enqueues and returns
immediately; _run_loop() on the agent's own thread drains the queue and does
all the heavy work (candle fetch, indicator calc, GPT call, bus publish).

Per-(symbol, zone_id) cooldown replaces the old per-symbol cooldown so that
different zones on the same instrument are never silently suppressed.
"""

import json
import logging
import os
import queue
import threading
import time
from typing import Dict, List, Optional, Tuple

from openai import OpenAI

from metatrader_client import MT5Client

import config
from core.event_bus import EventBus
from core.events import SignalGeneratedEvent, ZoneTouchEvent
from indicators.calculator import compute_all_indicators, sort_candles_ascending
from prompts.user_template import build_user_prompt

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "..", "prompts", "system_prompt.txt")


def _load_system_prompt() -> str:
    try:
        with open(_SYSTEM_PROMPT_PATH, "r", encoding="utf-8") as fh:
            return fh.read()
    except Exception:
        logger.exception("Failed to load system_prompt.txt — using empty system prompt")
        return ""


_SYSTEM_PROMPT: str = _load_system_prompt()


class AnalysisAgent:
    """
    GPT-4o powered trading signal generator.

    ZoneTouchEvents are put on an internal queue by the event handler
    (which runs on PriceWatcher's thread) and processed sequentially by
    this agent's own dedicated thread.  This means the tick loop is never
    blocked by network I/O to the OpenAI API.

    Injected dependencies:
        client:        Shared MT5Client for fetching candles.
        bus:           Shared EventBus (subscribe + publish).
        openai_client: Optional pre-built OpenAI client (useful for testing).
    """

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        openai_client: Optional[OpenAI] = None,
    ) -> None:
        self._client = client
        self._bus    = bus
        self._oai    = openai_client or OpenAI(api_key=config.OPENAI_API_KEY)

        # Per-(symbol, zone_id) cooldown so that different zones on the same
        # instrument are not suppressed by each other.
        self._last_analysis: Dict[Tuple[str, int], float] = {}
        self._analysis_cooldown_sec = 30.0

        # Bounded queue: if the agent falls behind, oldest events are dropped
        # rather than growing unbounded memory.
        self._event_queue: "queue.Queue[Optional[ZoneTouchEvent]]" = queue.Queue(maxsize=50)

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="AnalysisAgent",
            daemon=True,
        )

        self._bus.subscribe(ZoneTouchEvent, self._on_zone_touch)
        logger.info("AnalysisAgent initialised and subscribed to ZoneTouchEvent.")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the agent's processing thread."""
        logger.info("AnalysisAgent starting …")
        self._thread.start()

    def stop(self) -> None:
        """Signal the agent to stop and wait for the thread to exit."""
        logger.info("AnalysisAgent stopping …")
        self._stop_event.set()
        try:
            self._event_queue.put(None, timeout=1)   # sentinel to unblock get()
        except queue.Full:
            pass
        self._thread.join(timeout=30)

    def restart(self) -> None:
        """Restart a dead thread (called by the main watchdog)."""
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="AnalysisAgent", daemon=True
        )
        self._thread.start()
        logger.warning("AnalysisAgent thread restarted by watchdog.")

    # ------------------------------------------------------------------
    # Event handler — runs on PriceWatcher's thread; must be fast
    # ------------------------------------------------------------------

    def _on_zone_touch(self, event: ZoneTouchEvent) -> None:
        """
        Put the event on the processing queue and return immediately.
        If the queue is full, the event is dropped with a warning.
        """
        try:
            self._event_queue.put_nowait(event)
        except queue.Full:
            logger.warning(
                "AnalysisAgent queue full — dropping ZoneTouchEvent for %s zone_id=%s",
                event.symbol,
                event.zone_id,
            )

    # ------------------------------------------------------------------
    # Processing loop — runs on AnalysisAgent's own thread
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        logger.info("AnalysisAgent processing loop started.")
        while not self._stop_event.is_set():
            try:
                event = self._event_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if event is None:       # sentinel from stop()
                break

            try:
                self._handle_zone_touch(event)
            except Exception:
                logger.exception(
                    "AnalysisAgent._handle_zone_touch raised for %s", event.symbol
                )
        logger.info("AnalysisAgent processing loop stopped.")

    def _handle_zone_touch(self, event: ZoneTouchEvent) -> None:
        """
        Apply per-(symbol, zone_id) cooldown then dispatch to _process_zone_touch.
        Runs on AnalysisAgent's own thread — no lock needed for _last_analysis.
        """
        now = time.time()
        cooldown_key: Tuple[str, int] = (event.symbol, event.zone_id or 0)
        last = self._last_analysis.get(cooldown_key, 0.0)
        if (now - last) < self._analysis_cooldown_sec:
            logger.debug(
                "AnalysisAgent: %s zone_id=%s in cooldown (%.0fs remaining), skipping",
                event.symbol,
                event.zone_id,
                self._analysis_cooldown_sec - (now - last),
            )
            return

        self._last_analysis[cooldown_key] = now
        logger.info(
            "AnalysisAgent handling ZoneTouchEvent: %s %s center=%.5f",
            event.symbol,
            event.zone_type,
            event.price_center,
        )
        self._process_zone_touch(event, cooldown_key)

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _process_zone_touch(
        self,
        event: ZoneTouchEvent,
        cooldown_key: Tuple[str, int],
    ) -> None:
        """
        Fetch candles, compute indicators, call GPT-4o, validate, publish signal.
        On GPT API failure the cooldown is cleared so the next zone touch retries.
        """
        symbol_base = event.symbol
        symbol = config.resolve_symbol(symbol_base)

        # 1. Fetch candles on the timeframe whose zone was actually touched
        #    (falls back to config.ANALYSIS_TF if the event predates this field)
        analysis_tf = event.timeframe or config.ANALYSIS_TF
        df = self._client.market.get_candles_latest(
            symbol, analysis_tf, count=config.ANALYSIS_CANDLE_COUNT
        )
        if df is None or len(df) < 30:
            logger.warning(
                "AnalysisAgent: insufficient candle data for %s (%d rows)",
                symbol,
                0 if df is None else len(df),
            )
            return

        # 2. Compute indicators
        indicators = compute_all_indicators(df)
        logger.debug("AnalysisAgent indicators for %s: %s", symbol_base, indicators)

        # 3. Build recent candles list for the prompt (5 most-recent bars)
        asc_df = sort_candles_ascending(df)
        recent_candles: List[Dict] = [
            {
                "time":  str(row.get("time", "")),
                "open":  float(row.get("open",  0.0)),
                "high":  float(row.get("high",  0.0)),
                "low":   float(row.get("low",   0.0)),
                "close": float(row.get("close", 0.0)),
            }
            for _, row in asc_df.tail(5).iterrows()
        ]

        # 4. Build prompts
        zone_dict = {
            "zone_type":    event.zone_type,
            "price_center": event.price_center,
            "price_upper":  event.price_upper,
            "price_lower":  event.price_lower,
            "strength":     event.zone_strength,
        }
        price_dict = {"bid": event.bid, "ask": event.ask, "mid_price": event.mid_price}
        user_prompt = build_user_prompt(
            symbol=symbol_base,
            zone=zone_dict,
            indicators=indicators,
            recent_candles=recent_candles,
            price=price_dict,
            analysis_tf=analysis_tf,
        )

        # 5. Call GPT-4o
        gpt_response = self._call_gpt(user_prompt)
        if gpt_response is None:
            # API error — reset cooldown so the next touch can retry immediately
            self._last_analysis.pop(cooldown_key, None)
            return

        # 6. Validate confidence threshold
        confidence = float(gpt_response.get("confidence", 0))
        if confidence < config.MIN_CONFIDENCE:
            logger.info(
                "AnalysisAgent: signal for %s dropped — confidence %.1f < %.1f",
                symbol_base,
                confidence,
                config.MIN_CONFIDENCE,
            )
            return

        direction   = str(gpt_response.get("direction",   "")).upper()
        entry       = float(gpt_response.get("entry",      0.0))
        stop_loss   = float(gpt_response.get("stop_loss",  0.0))
        take_profit = float(gpt_response.get("take_profit",0.0))
        reasoning   = str(gpt_response.get("reasoning",   ""))

        if direction not in ("BUY", "SELL"):
            logger.warning(
                "AnalysisAgent: invalid direction '%s' from GPT-4o — discarding", direction
            )
            return
        if entry <= 0 or stop_loss <= 0 or take_profit <= 0:
            logger.warning("AnalysisAgent: invalid price levels from GPT-4o — discarding")
            return

        # 7. Publish signal
        signal = SignalGeneratedEvent(
            symbol=symbol_base,
            direction=direction,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            confidence=confidence,
            reasoning=reasoning,
            zone_type=event.zone_type,
            zone_center=event.price_center,
            zone_id=event.zone_id,
            ema21=indicators.get("ema21"),
            ema50=indicators.get("ema50"),
            rsi14=indicators.get("rsi14"),
            macd_line=indicators.get("macd_line"),
            macd_signal=indicators.get("macd_signal"),
            macd_hist=indicators.get("macd_hist"),
            mid_price=event.mid_price,
        )
        self._bus.publish(signal)
        logger.info(
            "AnalysisAgent published signal: %s %s entry=%.5f sl=%.5f tp=%.5f conf=%.1f",
            symbol_base,
            direction,
            entry,
            stop_loss,
            take_profit,
            confidence,
        )

    # ------------------------------------------------------------------
    # GPT-4o call
    # ------------------------------------------------------------------

    def _call_gpt(self, user_prompt: str) -> Optional[Dict]:
        """
        Call GPT-4o with the system + user prompt, parse the JSON response.
        Returns None on any error so the caller can reset the cooldown.
        """
        try:
            response = self._oai.chat.completions.create(
                model=config.OPENAI_MODEL,
                response_format={"type": "json_object"},
                max_tokens=config.OPENAI_MAX_TOKENS,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
            )
            raw_content = response.choices[0].message.content
            if not raw_content:
                logger.warning("AnalysisAgent: GPT-4o returned empty content")
                return None
            parsed = json.loads(raw_content)
            logger.debug("AnalysisAgent GPT response: %s", parsed)
            return parsed
        except json.JSONDecodeError as exc:
            logger.warning("AnalysisAgent: GPT-4o JSON parse error: %s", exc)
            return None
        except Exception:
            logger.exception("AnalysisAgent: OpenAI API call failed")
            return None
