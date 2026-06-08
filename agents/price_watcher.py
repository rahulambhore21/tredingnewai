"""
agents/price_watcher.py — Real-time price tick monitor.

Responsibilities:
1. Wait for sr_mapper to signal that zones are ready (zones_ready event).
2. Poll live prices for all instruments every TICK_INTERVAL_SEC seconds.
3. For each instrument, load active S/R zones from the DB.
4. If the current mid-price is within ZONE_TOUCH_PCT of a zone centre,
   and the zone has not been triggered within ZONE_COOLDOWN_MIN minutes
   (checked via an in-memory dict), publish a ZoneTouchEvent.
5. Only act during the configured trading window (08:00–20:00 UTC, Mon–Fri).

Thread model:
    One daemon thread per watcher instance runs _run_loop().
    Cooldown state is tracked in a dict:
        { (symbol, zone_id): last_touch_utc_timestamp }
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
    Tick-level price monitor that triggers S/R zone touch events.

    Injected dependencies:
        client:      Shared MT5Client (already connected).
        bus:         Shared EventBus for publishing ZoneTouchEvents.
        db:          Shared Database for reading active zones.
        zones_ready: threading.Event from SRMapper — watcher blocks until set.
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
    ) -> None:
        """
        Initialise the price watcher.

        Args:
            client:      Connected MT5Client instance.
            bus:         Shared EventBus.
            db:          Shared Database (read-only from this agent).
            zones_ready: Event set by SRMapper after the first zone scan.
        """
        self._client      = client
        self._bus         = bus
        self._db          = db
        self._zones_ready = zones_ready

        # Cooldown tracker: key=(symbol_base, zone_id), value=last trigger time (float)
        self._last_touch: Dict[Tuple[str, int], float] = {}
        self._cooldown_sec = config.ZONE_COOLDOWN_MIN * 60

        # Price-fetch failure suppression: log once, then silence for 5 min per symbol
        self._price_fail_last_logged: Dict[str, float] = {}
        self._price_fail_log_interval = 300.0

        # Heartbeat: log alive status every 5 min even when no zones are touched
        self._last_price: Dict[str, float] = {}
        self._last_zone_count: Dict[str, int] = {}
        self._last_heartbeat: float = 0.0
        self._heartbeat_interval_sec: float = 300.0

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="PriceWatcher",
            daemon=True,
        )

    def start(self) -> None:
        """Start the price watcher thread."""
        logger.info("PriceWatcher starting …")
        self._thread.start()

    def stop(self) -> None:
        """Signal the watcher to stop and wait for the thread to exit."""
        logger.info("PriceWatcher stopping …")
        self._stop_event.set()
        self._thread.join(timeout=15)

    def restart(self) -> None:
        """Restart a dead thread (called by the main watchdog)."""
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="PriceWatcher", daemon=True
        )
        self._thread.start()
        logger.warning("PriceWatcher thread restarted by watchdog.")

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
        logger.info("PriceWatcher waiting for zones_ready …")
        self._zones_ready.wait()
        logger.info("PriceWatcher zones ready — starting tick loop")

        while not self._stop_event.is_set():
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
                parts = []
                for sym in config.INSTRUMENTS:
                    mid = self._last_price.get(sym)
                    nz  = self._last_zone_count.get(sym)
                    price_str = f"mid={mid:.5f}" if mid else "mid=n/a"
                    zone_str  = f"{nz} zones" if nz is not None else "zones=n/a"
                    parts.append(f"{sym} {price_str} {zone_str}")
                logger.info("PriceWatcher alive — %s", " | ".join(parts))

            # Interruptible sleep
            self._stop_event.wait(timeout=config.TICK_INTERVAL_SEC)

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------

    def _tick(self) -> None:
        """
        Process one tick: for each instrument fetch price, check all active
        zones, and publish ZoneTouchEvent if conditions are met.
        """
        for symbol_base in config.INSTRUMENTS:
            try:
                self._check_symbol(symbol_base)
            except Exception:
                logger.exception("PriceWatcher._check_symbol failed for %s", symbol_base)

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
            last = self._price_fail_last_logged.get(symbol_base, 0.0)
            if (now - last) >= self._price_fail_log_interval:
                logger.warning("PriceWatcher: failed to get price for %s: %s", symbol, exc)
                self._price_fail_last_logged[symbol_base] = now
            return

        bid = tick.get("bid", 0.0)
        ask = tick.get("ask", 0.0)
        if not bid or not ask:
            return
        mid = (bid + ask) / 2.0
        self._last_price[symbol_base] = mid

        # Load active zones from DB (read-only)
        try:
            zones = self._db.get_active_zones(symbol_base)
        except Exception:
            logger.exception("PriceWatcher: DB read failed for %s", symbol_base)
            return
        self._last_zone_count[symbol_base] = len(zones)

        # First-touch-wins selection across the M5/M15 timeframes:
        #   1. Keep only zones the current price is actually touching.
        #   2. Group touched zones into price clusters (within CLUSTER_TOLERANCE)
        #      so an M5 zone and an M15 zone at "the same" level don't both fire.
        #   3. Within each cluster, fire whichever zone is eligible first:
        #      not on cooldown > faster timeframe (M5 before M15) > higher strength.
        #      The losing zone's cooldown is left untouched so it can still fire
        #      on its own later (e.g. once the M5 zone's cooldown elapses).
        touched_zones = []
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

            logger.info(
                "Zone touch: %s %s zone_id=%d tf=%s center=%.5f mid=%.5f dist=%.4f%%",
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
