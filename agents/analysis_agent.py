"""
agents/analysis_agent.py — GPT-4o signal generation agent (per-symbol instance).

Subscribes to ZoneTouchEvent and filters to the assigned symbol only.
Processes events on its own dedicated thread so GPT API calls (2–10 s each)
never block the PriceWatcher tick loop, and symbols never block each other.

Queue-based design: _on_zone_touch() enqueues and returns immediately;
_run_loop() on the agent's own thread drains the queue and does all heavy work
(candle fetch, indicator calc, GPT call, bus publish).

Per-zone_id cooldown prevents the same zone being re-analysed too quickly.
"""

import collections
import json
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Tuple

from openai import OpenAI

from metatrader_client import MT5Client

import config
from core.event_bus import EventBus
from core.events import SignalGeneratedEvent, ZoneTouchEvent
from core.signal_tracker import SignalTracker
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
    GPT-4o powered trading signal generator for a single symbol.

    ZoneTouchEvents for the assigned symbol are put on an internal queue by the
    event handler (which runs on PriceWatcher's thread) and processed by this
    agent's own dedicated thread — GPT calls never block the tick loop, and
    different symbols never block each other.

    Injected dependencies:
        client:        Shared MT5Client for fetching candles.
        bus:           Shared EventBus (subscribe + publish).
        symbol:        Base symbol this instance handles (e.g. "EURUSD").
        openai_client: Optional pre-built OpenAI client (useful for testing).
    """

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        symbol: str,
        openai_client: Optional[OpenAI] = None,
        signal_tracker: Optional["SignalTracker"] = None,
    ) -> None:
        self._client  = client
        self._bus     = bus
        self._symbol  = symbol
        self._oai     = openai_client or OpenAI(api_key=config.OPENAI_API_KEY)
        self._tracker = signal_tracker or SignalTracker()

        # Per-zone_id cooldown — different zones on the same instrument don't suppress each other.
        self._last_analysis: Dict[Tuple[str, int], float] = {}
        self._analysis_cooldown_sec = 30.0

        # Bounded deque (maxsize=10) with per-zone deduplication.
        # Protected by _q_cond; _q_pending_zones tracks which zone_id keys are
        # currently waiting so duplicates can be detected in O(1).
        self._Q_MAXSIZE = 10
        self._q_cond = threading.Condition()
        self._q_deque: "collections.deque[ZoneTouchEvent]" = collections.deque()
        self._q_pending_zones: set = set()  # set of (symbol, zone_id or 0) keys

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"AnalysisAgent-{symbol}",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

        self._bus.subscribe(ZoneTouchEvent, self._on_zone_touch)
        logger.info("AnalysisAgent[%s] initialised and subscribed to ZoneTouchEvent.", symbol)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the agent's processing thread."""
        logger.info("AnalysisAgent[%s] starting …", self._symbol)
        self._thread.start()

    def stop(self) -> None:
        """Signal the agent to stop and wait for the thread to exit."""
        logger.info("AnalysisAgent[%s] stopping …", self._symbol)
        self._stop_event.set()
        with self._q_cond:
            self._q_cond.notify()   # wake _run_loop if it is waiting
        self._thread.join(timeout=30)

    def restart(self) -> None:
        """Restart a dead thread (called by the main watchdog)."""
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"AnalysisAgent-{self._symbol}", daemon=True
        )
        self._thread.start()
        logger.warning("AnalysisAgent[%s] thread restarted by watchdog.", self._symbol)

    # ------------------------------------------------------------------
    # Event handler — runs on PriceWatcher's thread; must be fast
    # ------------------------------------------------------------------

    def _on_zone_touch(self, event: ZoneTouchEvent) -> None:
        """
        Enqueue the event and return immediately (called on PriceWatcher's thread).
        Events for other symbols are dropped immediately — each AnalysisAgent
        instance only processes its own symbol.

        Deduplication rules (evaluated under the queue lock):
        1. Same (symbol, zone_id) already pending → drop silently at DEBUG.
        2. Queue full, different zone → drop the oldest item (WARNING) then enqueue.
        3. Queue not full → enqueue normally.
        """
        if event.symbol != self._symbol:
            return

        zone_key = (event.symbol, event.zone_id or 0)
        with self._q_cond:
            if zone_key in self._q_pending_zones:
                logger.debug(
                    "AnalysisAgent: duplicate ZoneTouchEvent for %s zone_id=%s"
                    " already queued — dropping",
                    event.symbol,
                    event.zone_id,
                )
                return

            if len(self._q_deque) >= self._Q_MAXSIZE:
                dropped = self._q_deque.popleft()
                self._q_pending_zones.discard((dropped.symbol, dropped.zone_id or 0))
                logger.warning(
                    "AnalysisAgent queue full — dropping oldest ZoneTouchEvent"
                    " for %s zone_id=%s to make room for %s zone_id=%s",
                    dropped.symbol,
                    dropped.zone_id,
                    event.symbol,
                    event.zone_id,
                )

            self._q_deque.append(event)
            self._q_pending_zones.add(zone_key)
            self._q_cond.notify()

    # ------------------------------------------------------------------
    # Processing loop — runs on AnalysisAgent's own thread
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        logger.info("AnalysisAgent processing loop started.")
        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            event = None
            with self._q_cond:
                while not self._q_deque and not self._stop_event.is_set():
                    self._q_cond.wait(timeout=1.0)
                    self.last_heartbeat = time.time()  # stay alive while idle
                if self._q_deque:
                    event = self._q_deque.popleft()
                    self._q_pending_zones.discard((event.symbol, event.zone_id or 0))

            if event is None:
                continue

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

        # 1b. Fetch H4 candles for higher-timeframe context (50 candles)
        h4_df = self._client.market.get_candles_latest(symbol, "H4", count=50)
        h4_candles: List[Dict] = []
        if h4_df is not None and len(h4_df) >= 1:
            h4_asc = sort_candles_ascending(h4_df)
            h4_candles = [
                {
                    "time":   str(row.get("time", "")),
                    "open":   float(row.get("open",   0.0)),
                    "high":   float(row.get("high",   0.0)),
                    "low":    float(row.get("low",    0.0)),
                    "close":  float(row.get("close",  0.0)),
                    "volume": float(row.get("volume", 0.0)),
                }
                for _, row in h4_asc.tail(10).iterrows()
            ]
        else:
            logger.warning(
                "AnalysisAgent: could not fetch H4 candles for %s — HTF section will be empty",
                symbol,
            )

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
            h4_candles=h4_candles,
        )

        # 5. Call GPT-4o
        gpt_response = self._call_gpt(user_prompt, symbol=symbol_base, zone_id=event.zone_id)
        if gpt_response is None:
            # API error — reset cooldown so the next touch can retry immediately
            self._last_analysis.pop(cooldown_key, None)
            return

        # 6a. Log HTF alignment warning (does not block the signal)
        htf_alignment = gpt_response.get("htf_alignment")
        if htf_alignment is False:
            logger.warning(
                "AnalysisAgent: H4 trend does NOT align with proposed %s signal for %s"
                " zone_id=%s — proceeding anyway (htf_alignment=false)",
                gpt_response.get("direction", "?"),
                symbol_base,
                event.zone_id,
            )

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

        # 7. Check rolling win-rate gate before publishing
        if self._tracker.should_pause:
            logger.warning(
                "AnalysisAgent: signal for %s discarded — win rate %.0f%% is below"
                " pause threshold %.0f%%",
                symbol_base,
                self._tracker.win_rate * 100,
                config.SIGNAL_PAUSE_THRESHOLD * 100,
            )
            return

        # 8. Publish signal
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

    def _call_gpt(
        self,
        user_prompt: str,
        symbol: str = "",
        zone_id: Optional[int] = None,
    ) -> Optional[Dict]:
        """
        Call GPT-4o with the system + user prompt, parse the JSON response.
        Retries up to 2 times on network errors, timeouts, or JSON parse failures,
        with exponential backoff (2s, 4s). Returns None after all retries are
        exhausted so the caller can reset the cooldown.
        """
        max_retries = 2
        backoff_sec = 2.0
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries + 1):
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
                last_exc = exc
                logger.warning(
                    "AnalysisAgent: GPT-4o JSON parse error (attempt %d/%d) for"
                    " symbol=%s zone_id=%s: %s",
                    attempt + 1, max_retries + 1, symbol, zone_id, exc,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "AnalysisAgent: OpenAI API call failed (attempt %d/%d) for"
                    " symbol=%s zone_id=%s: %s",
                    attempt + 1, max_retries + 1, symbol, zone_id, exc,
                )

            if attempt < max_retries:
                wait = backoff_sec * (2 ** attempt)
                logger.debug(
                    "AnalysisAgent: retrying in %.0fs (attempt %d/%d)",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)

        logger.error(
            "AnalysisAgent: all %d GPT attempts failed for symbol=%s zone_id=%s —"
            " discarding signal. Last error: %s",
            max_retries + 1, symbol, zone_id, last_exc,
        )
        return None
