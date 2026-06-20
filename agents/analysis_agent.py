"""
agents/analysis_agent.py — GPT-4o signal generation agent.

Subscribes to ZoneTouchEvent for all symbols.
Processes events on its own dedicated thread so GPT API calls (2–10 s each)
never block the PriceWatcher tick loop.

For each ZoneTouchEvent:
  1. Pre-filters: daily trade count, account direction vs zone type
  2. Fetches 100 candles on both M5 and M15 for event.symbol
  3. Computes EMA 20/50, RSI 14, MACD on both timeframes
  4. Passes M15 data (trend) + M5 data (entry confirmation) to GPT-4o
  5. GPT-4o returns: direction (BUY/SELL/NONE), sl, tp, confidence (0-10), reason
  6. If direction is NONE or doesn't match account direction, signal is dropped
  7. Otherwise emits SignalGeneratedEvent with account_id
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
from core.database import Database
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

    ZoneTouchEvents (for any symbol) are put on an internal queue by the
    event handler and processed by this agent's own dedicated thread.
    GPT calls never block the tick loop.

    account_config must contain: account_id (int), direction ("BUY" or "SELL").
    """

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        db: Database,
        account_config: Dict,
        openai_client: Optional[OpenAI] = None,
    ) -> None:
        self._client        = client
        self._bus           = bus
        self._db            = db
        self._account_id    = int(account_config["account_id"])
        self._direction     = str(account_config["direction"]).upper()
        self._oai           = openai_client or OpenAI(api_key=config.OPENAI_API_KEY)

        self._last_analysis: Dict[Tuple[str, int], float] = {}
        self._analysis_cooldown_sec = 30.0

        self._Q_MAXSIZE = 10
        self._q_cond = threading.Condition()
        self._q_deque: "collections.deque[ZoneTouchEvent]" = collections.deque()
        self._q_pending_zones: set = set()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"AnalysisAgent-{self._account_id}",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

        self._bus.subscribe(ZoneTouchEvent, self._on_zone_touch)
        logger.info(
            "AnalysisAgent[acct=%d dir=%s] initialised and subscribed to ZoneTouchEvent.",
            self._account_id, self._direction,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        logger.info("AnalysisAgent[acct=%d] starting …", self._account_id)
        self._thread.start()

    def stop(self) -> None:
        logger.info("AnalysisAgent stopping …")
        self._stop_event.set()
        with self._q_cond:
            self._q_cond.notify()
        self._thread.join(timeout=30)

    def restart(self) -> None:
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"AnalysisAgent-{self._account_id}",
            daemon=True,
        )
        self._thread.start()
        logger.warning("AnalysisAgent[acct=%d] thread restarted by watchdog.", self._account_id)

    # ------------------------------------------------------------------
    # Event handler — runs on PriceWatcher's thread; must be fast
    # ------------------------------------------------------------------

    def _on_zone_touch(self, event: ZoneTouchEvent) -> None:
        """Enqueue the event and return immediately."""
        zone_key = (event.symbol, event.zone_id or 0)
        with self._q_cond:
            if zone_key in self._q_pending_zones:
                logger.debug(
                    "AnalysisAgent: duplicate ZoneTouchEvent for %s zone_id=%s already queued — dropping",
                    event.symbol, event.zone_id,
                )
                return

            if len(self._q_deque) >= self._Q_MAXSIZE:
                dropped = self._q_deque.popleft()
                self._q_pending_zones.discard((dropped.symbol, dropped.zone_id or 0))
                logger.warning(
                    "AnalysisAgent queue full — dropping oldest event for %s to make room for %s",
                    dropped.symbol, event.symbol,
                )

            self._q_deque.append(event)
            self._q_pending_zones.add(zone_key)
            self._q_cond.notify()

    # ------------------------------------------------------------------
    # Processing loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        logger.info("AnalysisAgent processing loop started.")
        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            event = None
            with self._q_cond:
                while not self._q_deque and not self._stop_event.is_set():
                    self._q_cond.wait(timeout=1.0)
                    self.last_heartbeat = time.time()
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
        now = time.time()
        cooldown_key: Tuple[str, int] = (event.symbol, event.zone_id or 0)
        last = self._last_analysis.get(cooldown_key, 0.0)
        if (now - last) < self._analysis_cooldown_sec:
            logger.debug(
                "AnalysisAgent: %s zone_id=%s in cooldown (%.0fs remaining), skipping",
                event.symbol, event.zone_id,
                self._analysis_cooldown_sec - (now - last),
            )
            return

        self._last_analysis[cooldown_key] = now
        logger.info(
            "AnalysisAgent handling ZoneTouchEvent: %s %s center=%.5f",
            event.symbol, event.zone_type, event.price_center,
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
        symbol_base = event.symbol
        symbol = config.resolve_symbol(symbol_base)

        # 0a. Skip if daily trade count for this account >= MAX_DAILY_TRADES
        try:
            daily_count = self._db.get_daily_trade_count(self._account_id)
        except Exception:
            logger.warning(
                "AnalysisAgent[acct=%d]: could not fetch daily trade count — skipping %s",
                self._account_id, symbol_base,
            )
            return

        if daily_count >= config.MAX_DAILY_TRADES:
            logger.info(
                "AnalysisAgent[acct=%d]: daily trade count %d >= MAX_DAILY_TRADES (%d) "
                "— skipping GPT call for %s",
                self._account_id, daily_count, config.MAX_DAILY_TRADES, symbol_base,
            )
            return

        # 0b. Direction pre-filter: skip GPT when zone type can't produce account direction.
        # support zones → BUY opportunity; resistance zones → SELL opportunity.
        zone_type_lower = event.zone_type.lower()
        if self._direction == "BUY" and zone_type_lower == "resistance":
            logger.info(
                "AnalysisAgent[acct=%d dir=BUY]: resistance zone — skipping GPT call for %s",
                self._account_id, symbol_base,
            )
            return
        if self._direction == "SELL" and zone_type_lower == "support":
            logger.info(
                "AnalysisAgent[acct=%d dir=SELL]: support zone — skipping GPT call for %s",
                self._account_id, symbol_base,
            )
            return

        # 0c. Skip GPT call if max open trades already reached (account-wide MT5 check)
        try:
            positions = self._client.order.get_all_positions()
            open_count = 0 if positions is None else len(positions)
        except Exception:
            logger.warning(
                "AnalysisAgent[acct=%d]: could not fetch open positions — skipping %s",
                self._account_id, symbol_base,
            )
            return

        if open_count >= config.MAX_OPEN_TRADES:
            logger.info(
                "AnalysisAgent[acct=%d]: %d open trade(s) >= MAX_OPEN_TRADES (%d) "
                "— skipping GPT call for %s",
                self._account_id, open_count, config.MAX_OPEN_TRADES, symbol_base,
            )
            return

        # 1. Fetch M5 candles
        m5_df = self._client.market.get_candles_latest(
            symbol, "M5", count=config.ANALYSIS_CANDLE_COUNT
        )
        if m5_df is None or len(m5_df) < 30:
            logger.warning(
                "AnalysisAgent: insufficient M5 candle data for %s (%d rows)",
                symbol, 0 if m5_df is None else len(m5_df),
            )
            return

        # 2. Fetch M15 candles
        m15_df = self._client.market.get_candles_latest(
            symbol, "M15", count=config.ANALYSIS_CANDLE_COUNT
        )
        if m15_df is None or len(m15_df) < 30:
            logger.warning(
                "AnalysisAgent: insufficient M15 candle data for %s (%d rows)",
                symbol, 0 if m15_df is None else len(m15_df),
            )
            return

        # 3. Compute indicators on both timeframes
        m5_indicators  = compute_all_indicators(m5_df)
        m15_indicators = compute_all_indicators(m15_df)

        # 4. Build recent candles lists (5 most-recent bars each)
        m5_asc  = sort_candles_ascending(m5_df)
        m15_asc = sort_candles_ascending(m15_df)

        def _candle_rows(asc_df) -> List[Dict]:
            return [
                {
                    "time":  str(row.get("time", "")),
                    "open":  float(row.get("open",  0.0)),
                    "high":  float(row.get("high",  0.0)),
                    "low":   float(row.get("low",   0.0)),
                    "close": float(row.get("close", 0.0)),
                }
                for _, row in asc_df.tail(5).iterrows()
            ]

        m5_candles  = _candle_rows(m5_asc)
        m15_candles = _candle_rows(m15_asc)

        # 5a. Pre-filter: skip GPT if M15 trend contradicts zone type (saves tokens)
        m15_ema21 = m15_indicators.get("ema21", 0.0)
        m15_ema50 = m15_indicators.get("ema50", 0.0)
        if m15_ema21 != 0.0 and m15_ema50 != 0.0:
            m15_bullish = m15_ema21 > m15_ema50
            if zone_type_lower == "support" and not m15_bullish:
                logger.info(
                    "AnalysisAgent[acct=%d]: M15 bearish at support for %s — skipping GPT call",
                    self._account_id, symbol_base,
                )
                return
            if zone_type_lower == "resistance" and m15_bullish:
                logger.info(
                    "AnalysisAgent[acct=%d]: M15 bullish at resistance for %s — skipping GPT call",
                    self._account_id, symbol_base,
                )
                return
        else:
            logger.warning(
                "AnalysisAgent[acct=%d]: EMA pre-filter skipped for %s — zero/invalid M15 indicator values",
                self._account_id, symbol_base,
            )
            return

        # 5b. Build prompts
        zone_dict  = {
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
            m5_indicators=m5_indicators,
            m5_candles=m5_candles,
            m15_indicators=m15_indicators,
            m15_candles=m15_candles,
            price=price_dict,
        )

        # 6. Call GPT-4o
        gpt_response = self._call_gpt(user_prompt, symbol=symbol_base, zone_id=event.zone_id)
        if gpt_response is None:
            return

        # 7. Parse response
        direction = str(gpt_response.get("direction", "")).upper()

        if direction == "NONE":
            logger.info(
                "AnalysisAgent[acct=%d]: GPT returned NONE for %s zone_id=%s — dropping signal",
                self._account_id, symbol_base, event.zone_id,
            )
            return

        if direction not in ("BUY", "SELL"):
            logger.warning(
                "AnalysisAgent[acct=%d]: invalid direction '%s' from GPT-4o — discarding",
                self._account_id, direction,
            )
            return

        if direction != self._direction:
            logger.info(
                "AnalysisAgent[acct=%d dir=%s]: GPT returned %s — direction mismatch, dropping",
                self._account_id, self._direction, direction,
            )
            return

        sl          = float(gpt_response.get("sl",  0.0))
        tp          = float(gpt_response.get("tp",  0.0))
        confidence  = float(gpt_response.get("confidence", 0))
        reason      = str(gpt_response.get("reason", ""))

        # Refresh entry price after the GPT call (which can take 2–10 s) to avoid
        # using a stale mid_price from when the zone was touched.
        try:
            tick = self._client.market.get_symbol_price(symbol)
            if tick:
                entry = float(tick["ask"]) if direction == "BUY" else float(tick["bid"])
            else:
                entry = event.mid_price
        except Exception:
            logger.warning(
                "AnalysisAgent[acct=%d]: get_symbol_price failed for %s — using event mid_price",
                self._account_id, symbol_base,
            )
            entry = event.mid_price

        if sl <= 0 or tp <= 0:
            logger.warning("AnalysisAgent: invalid price levels from GPT-4o — discarding")
            return

        if direction == "BUY" and not (sl < entry < tp):
            logger.warning(
                "AnalysisAgent: invalid BUY geometry SL=%.5f entry=%.5f TP=%.5f — discarding",
                sl, entry, tp,
            )
            return
        if direction == "SELL" and not (tp < entry < sl):
            logger.warning(
                "AnalysisAgent: invalid SELL geometry TP=%.5f entry=%.5f SL=%.5f — discarding",
                tp, entry, sl,
            )
            return

        signal = SignalGeneratedEvent(
            symbol=symbol_base,
            direction=direction,
            entry=entry,
            stop_loss=sl,
            take_profit=tp,
            confidence=confidence,
            reasoning=reason,
            zone_type=event.zone_type,
            zone_center=event.price_center,
            account_id=self._account_id,
            zone_id=event.zone_id,
            zone_strength=event.zone_strength,
            timeframe=event.timeframe,
            ema21=m5_indicators.get("ema21"),
            ema50=m5_indicators.get("ema50"),
            rsi14=m5_indicators.get("rsi14"),
            macd_line=m5_indicators.get("macd_line"),
            macd_signal=m5_indicators.get("macd_signal"),
            macd_hist=m5_indicators.get("macd_hist"),
            mid_price=event.mid_price,
        )
        self._bus.publish(signal)
        logger.info(
            "AnalysisAgent[acct=%d] published signal: %s %s entry=%.5f sl=%.5f tp=%.5f conf=%.1f",
            self._account_id, symbol_base, direction, entry, sl, tp, confidence,
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
        Retries up to 2 times on network errors or JSON parse failures.
        Returns None after all retries are exhausted.
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
                logger.warning(
                    "AnalysisAgent: GPT-4o JSON parse error — not retrying: %s", exc
                )
                return None
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "AnalysisAgent: OpenAI API call failed (attempt %d/%d): %s",
                    attempt + 1, max_retries + 1, exc,
                )

            if attempt < max_retries:
                wait = backoff_sec * (2 ** attempt)
                time.sleep(wait)

        logger.error(
            "AnalysisAgent: all %d GPT attempts failed for symbol=%s zone_id=%s — Last error: %s",
            max_retries + 1, symbol, zone_id, last_exc,
        )
        return None
