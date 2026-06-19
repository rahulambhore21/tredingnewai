"""
agents/price_watcher.py — Real-time price tick monitor.

Waits for zones_ready, then polls prices for all 3 symbols every
TICK_INTERVAL_SEC in a single loop. Emits ZoneTouchEvent(symbol) when
the live price enters a stored S/R zone.
"""

import logging
import threading
import time
from typing import Dict, Optional, Tuple

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import ZoneTouchEvent

logger = logging.getLogger(__name__)


class PriceWatcher:
    """
    Tick-level price monitor for all configured symbols.

    Injected dependencies:
        client:      Shared MT5Client (already connected).
        bus:         Shared EventBus for publishing ZoneTouchEvents.
        db:          Shared Database for reading active zones.
        zones_ready: threading.Event from SRMapper — watcher blocks until set.
    """

    _TF_PRIORITY = {"M5": 0, "M15": 1}

    def __init__(
        self,
        client: MT5Client,
        bus: EventBus,
        db: Database,
        zones_ready: threading.Event,
    ) -> None:
        self._client      = client
        self._bus         = bus
        self._db          = db
        self._zones_ready = zones_ready

        self._last_touch: Dict[Tuple[str, int], float] = {}
        self._cooldown_sec = config.ZONE_COOLDOWN_MIN * 60

        self._price_fail_last_logged: Dict[str, float] = {}
        self._price_fail_log_interval = 300.0
        self._tick_count: Dict[str, int] = {}
        self._tick_log_interval = 30  # log an INFO summary every N ticks

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="PriceWatcher",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

    def start(self) -> None:
        logger.info("PriceWatcher starting …")
        self._thread.start()

    def stop(self) -> None:
        logger.info("PriceWatcher stopping …")
        self._stop_event.set()
        self._thread.join(timeout=15)

    def restart(self) -> None:
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
        logger.info("PriceWatcher waiting for zones_ready …")
        self._zones_ready.wait()
        logger.info("PriceWatcher zones ready — starting tick loop for all symbols")

        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            try:
                self._tick()
            except Exception:
                logger.exception("PriceWatcher tick raised — continuing")

            self._stop_event.wait(timeout=config.TICK_INTERVAL_SEC)

    # ------------------------------------------------------------------
    # Tick — poll all symbols
    # ------------------------------------------------------------------

    def _tick(self) -> None:
        for symbol_base in config.SYMBOLS:
            try:
                self._check_symbol(symbol_base)
            except Exception:
                logger.exception("PriceWatcher._check_symbol failed for %s", symbol_base)

    def _check_symbol(self, symbol_base: str) -> None:
        symbol = config.resolve_symbol(symbol_base)

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

        try:
            zones = self._db.get_active_zones(symbol_base)
        except Exception:
            logger.exception("PriceWatcher: DB read failed for %s", symbol_base)
            return

        tick_n = self._tick_count.get(symbol_base, 0) + 1
        self._tick_count[symbol_base] = tick_n

        if zones:
            nearest_dist_pct = min(
                abs(mid - z["price_center"]) / z["price_center"] for z in zones
            )
            logger.debug(
                "PriceWatcher tick: %s mid=%.5f zones=%d nearest=%.4f%% threshold=%.4f%%",
                symbol_base, mid, len(zones),
                nearest_dist_pct * 100, config.ZONE_TOUCH_PCT * 100,
            )
            if tick_n % self._tick_log_interval == 0:
                logger.info(
                    "PriceWatcher [tick %d]: %s mid=%.5f zones=%d "
                    "nearest_zone=%.4f%% touch_threshold=%.4f%%",
                    tick_n, symbol_base, mid, len(zones),
                    nearest_dist_pct * 100, config.ZONE_TOUCH_PCT * 100,
                )
        else:
            logger.debug(
                "PriceWatcher tick: %s mid=%.5f — no active zones in DB",
                symbol_base, mid,
            )
            if tick_n % self._tick_log_interval == 0:
                logger.warning(
                    "PriceWatcher [tick %d]: %s mid=%.5f — no active zones in DB",
                    tick_n, symbol_base, mid,
                )

        touched_zones = []
        for z in zones:
            price_center = z["price_center"]
            distance_pct = abs(mid - price_center) / price_center
            if distance_pct <= config.ZONE_TOUCH_PCT:
                touched_zones.append(z)

        if not touched_zones:
            return

        # Cluster touched zones and fire the highest-priority zone per cluster
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

            cooldown_key = (symbol_base, zone_id)
            last_ts = self._last_touch.get(cooldown_key, 0.0)
            if (now_ts - last_ts) < self._cooldown_sec:
                logger.debug(
                    "PriceWatcher: zone %d (%s %s tf=%s) in cooldown, skipping",
                    zone_id, symbol_base, zone_type, timeframe,
                )
                continue

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
