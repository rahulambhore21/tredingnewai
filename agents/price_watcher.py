"""
agents/price_watcher.py — Real-time price tick monitor (per-symbol instance).

Responsibilities:
1. Wait for the matching SRMapper to signal that zones are ready.
2. Poll the live price for the assigned symbol every TICK_INTERVAL_SEC seconds.
3. Load active S/R zones for the symbol from the DB.
4. If the current mid-price is within ZONE_TOUCH_PCT of a zone centre,
   and the zone has not been triggered within ZONE_COOLDOWN_MIN minutes
   (checked via an in-memory dict), publish a ZoneTouchEvent.

Thread model:
    One daemon thread per symbol instance runs _run_loop().
    Cooldown state is tracked in a dict:
        { zone_id: last_touch_utc_timestamp }
    This avoids DB reads in the hot loop for the cooldown check.
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import ZoneTouchEvent

logger = logging.getLogger(__name__)


class PriceWatcher:
    """
    Tick-level price monitor for a single symbol that triggers S/R zone touch events.

    Injected dependencies:
        client:      Shared MT5Client (already connected).
        bus:         Shared EventBus for publishing ZoneTouchEvents.
        db:          Shared Database for reading active zones.
        zones_ready: threading.Event from the matching SRMapper — watcher blocks until set.
        symbol:      Base symbol this instance watches (e.g. "EURUSD").
    """

    # Lower number = higher priority when multiple timeframes' zones overlap
    # at the same price level ("first touch wins": the faster timeframe is
    # treated as the one price reaches first).
    _TF_PRIORITY = {"M5": 0, "M15": 1}

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        db: Database,
        zones_ready: threading.Event,
        symbol: str,
    ) -> None:
        self._client      = client
        self._bus         = bus
        self._db          = db
        self._zones_ready = zones_ready
        self._symbol      = symbol

        # Cooldown tracker: key=zone_id, value=last trigger time (float)
        self._last_touch: Dict[Tuple[str, int], float] = {}
        self._cooldown_sec = config.ZONE_COOLDOWN_MIN * 60

        # Price-fetch failure suppression: log once per 5 min on repeated errors
        self._price_fail_last_logged: float = 0.0
        self._price_fail_log_interval = 300.0

        # Heartbeat: log alive status every 5 min even when no zones are touched
        self._last_price: Optional[float] = None
        self._last_zone_count: Optional[int] = None
        self._last_heartbeat: float = 0.0
        self._heartbeat_interval_sec: float = 300.0

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"PriceWatcher-{symbol}",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

    def start(self) -> None:
        """Start the price watcher thread."""
        logger.info("PriceWatcher[%s] starting …", self._symbol)
        self._thread.start()

    def stop(self) -> None:
        """Signal the watcher to stop and wait for the thread to exit."""
        logger.info("PriceWatcher[%s] stopping …", self._symbol)
        self._stop_event.set()
        self._thread.join(timeout=15)

    def restart(self) -> None:
        """Restart a dead thread (called by the main watchdog)."""
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"PriceWatcher-{self._symbol}", daemon=True
        )
        self._thread.start()
        logger.warning("PriceWatcher[%s] thread restarted by watchdog.", self._symbol)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """
        Main loop:
        1. Block until sr_mapper has finished the first zone scan.
        2. Enter the tick poll loop (sleep TICK_INTERVAL_SEC between ticks).
        Every iteration is wrapped in try/except to prevent thread death.
        """
        logger.info("PriceWatcher[%s] waiting for zones_ready …", self._symbol)
        self._zones_ready.wait()
        logger.info("PriceWatcher[%s] zones ready — starting tick loop", self._symbol)

        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            try:
                if config.is_trading_hours():
                    self._tick()
                else:
                    logger.debug("PriceWatcher: outside trading hours, sleeping …")
            except Exception:
                logger.exception("PriceWatcher tick raised — continuing")

            now = time.time()
            if (now - self._last_heartbeat) >= self._heartbeat_interval_sec:
                self._last_heartbeat = now
                mid = self._last_price
                nz  = self._last_zone_count
                price_str = f"mid={mid:.5f}" if mid is not None else "mid=n/a"
                zone_str  = f"{nz} zones" if nz is not None else "zones=n/a"
                logger.info("PriceWatcher[%s] alive — %s %s", self._symbol, price_str, zone_str)

            # Interruptible sleep
            self._stop_event.wait(timeout=config.TICK_INTERVAL_SEC)

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------

    def _tick(self) -> None:
        """Fetch price for this instance's symbol and fire ZoneTouchEvents as needed."""
        try:
            self._check_symbol(self._symbol)
        except Exception:
            logger.exception("PriceWatcher._check_symbol failed for %s", self._symbol)

    def _check_symbol(self, symbol_base: str) -> None:
        """
        Fetch the current price for *symbol_base* and check all active zones.

        Args:
            symbol_base: Base symbol name without broker suffix (e.g. "EURUSD").
        """
        symbol = config.resolve_symbol(symbol_base)

        # Fetch tick data
        try:
            tick = self._client.market.get_symbol_price(symbol)
        except Exception as exc:
            now = time.time()
            if (now - self._price_fail_last_logged) >= self._price_fail_log_interval:
                logger.warning("PriceWatcher: failed to get price for %s: %s", symbol, exc)
                self._price_fail_last_logged = now
            return

        bid = tick.get("bid", 0.0)
        ask = tick.get("ask", 0.0)
        if not bid or not ask:
            return
        mid = (bid + ask) / 2.0
        self._last_price = mid

        # Load active zones from DB (read-only)
        try:
            zones = self._db.get_active_zones(symbol_base)
        except Exception:
            logger.exception("PriceWatcher: DB read failed for %s", symbol_base)
            return
        self._last_zone_count = len(zones)

        # First-touch-wins selection across the M5/M15 timeframes:
        #   1. Keep only zones the current price is actually touching.
        #   2. Group touched zones into price clusters (within CLUSTER_TOLERANCE)
        #      so an M5 zone and an M15 zone at "the same" level don't both fire.
        #   3. Within each cluster, fire whichever zone is eligible first:
        #      not on cooldown > faster timeframe (M5 before M15) > higher strength.
        #      The losing zone's cooldown is left untouched so it can still fire
        #      on its own later (e.g. once the M5 zone's cooldown elapses).
        touched_zones = []
        if config.ZONE_TOUCH_MODE == "close":
            # Cache last closed-candle close per timeframe to avoid duplicate API calls
            # within a single tick.  Candles are returned newest-first, so index 1 is
            # the most recent *completed* candle (index 0 is the still-forming bar).
            close_cache: Dict[str, Optional[float]] = {}
            for z in zones:
                tf = z["timeframe"]
                if tf not in close_cache:
                    try:
                        df = self._client.market.get_candles_latest(symbol, tf, count=2)
                        close_cache[tf] = float(df.iloc[1]["close"]) if len(df) >= 2 else None
                    except Exception:
                        logger.warning(
                            "PriceWatcher: failed to fetch candle close for %s tf=%s", symbol, tf
                        )
                        close_cache[tf] = None
                close_price = close_cache.get(tf)
                if close_price is not None and z["price_lower"] <= close_price <= z["price_upper"]:
                    touched_zones.append(z)
        else:  # "wick" — existing behaviour: mid price within ZONE_TOUCH_PCT of zone centre
            for z in zones:
                price_center = z["price_center"]
                distance_pct = abs(mid - price_center) / price_center
                if distance_pct <= config.ZONE_TOUCH_PCT:
                    touched_zones.append(z)

        if not touched_zones:
            return

        clusters = []
        cluster_centers = []
        for z in touched_zones:
            center = z["price_center"]
            for i, c_center in enumerate(cluster_centers):
                if abs(center - c_center) / c_center <= config.CLUSTER_TOLERANCE:
                    clusters[i].append(z)
                    break
            else:
                cluster_centers.append(center)
                clusters.append([z])

        now_ts = time.time()

        def _selection_key(z):
            cooldown_key = (symbol_base, z["id"])
            last_ts = self._last_touch.get(cooldown_key, 0.0)
            on_cooldown = (now_ts - last_ts) < self._cooldown_sec
            tf_priority = self._TF_PRIORITY.get(z["timeframe"], 99)
            return (int(on_cooldown), tf_priority, -z["strength"])

        for cluster in clusters:
            cluster.sort(key=_selection_key)
            zone = cluster[0]

            zone_id      = zone["id"]
            price_center = zone["price_center"]
            zone_type    = zone["zone_type"]
            timeframe    = zone["timeframe"]

            # Check cooldown on the winning candidate
            cooldown_key = (symbol_base, zone_id)
            last_ts = self._last_touch.get(cooldown_key, 0.0)
            if (now_ts - last_ts) < self._cooldown_sec:
                logger.debug(
                    "PriceWatcher: zone %d (%s %s tf=%s) in cooldown, skipping cluster",
                    zone_id, symbol_base, zone_type, timeframe,
                )
                continue

            # Update cooldown tracker BEFORE publishing to prevent re-entry
            # if the event handler is slow
            self._last_touch[cooldown_key] = now_ts

            distance_pct = abs(mid - price_center) / price_center

            if config.ZONE_TOUCH_MODE == "close":
                logger.info(
                    "Zone touch (close): %s %s zone_id=%d tf=%s bounds=[%.5f,%.5f] mid=%.5f dist=%.4f%%",
                    symbol_base, zone_type, zone_id, timeframe,
                    zone["price_lower"], zone["price_upper"], mid, distance_pct * 100,
                )
            else:
                logger.info(
                    "Zone touch (wick): %s %s zone_id=%d tf=%s center=%.5f mid=%.5f dist=%.4f%%",
                    symbol_base, zone_type, zone_id, timeframe, price_center, mid,
                    distance_pct * 100,
                )

            try:
                event = ZoneTouchEvent(
                    symbol=symbol_base,
                    zone_type=zone_type,
                    price_center=price_center,
                    price_upper=zone["price_upper"],
                    price_lower=zone["price_lower"],
                    zone_strength=zone["strength"],
                    bid=bid,
                    ask=ask,
                    mid_price=mid,
                    zone_id=zone_id,
                    timeframe=timeframe,
                )
                self._bus.publish(event)
            except Exception:
                logger.exception(
                    "PriceWatcher: failed to publish ZoneTouchEvent for %s zone_id=%d",
                    symbol_base, zone_id,
                )
