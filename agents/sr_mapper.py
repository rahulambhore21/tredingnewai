"""
agents/sr_mapper.py — Support & Resistance zone mapper (per-symbol instance).

Responsibilities:
1. On startup: scan all configured timeframes for the assigned symbol,
   detect swing highs/lows, cluster them into S/R zones, deactivate old
   zones in the DB, and publish ZoneEvent for each new zone so db_consumer
   stores them.
2. Run a background refresh every ZONE_REFRESH_HOURS hours so zones stay
   current without restarting the bot.
3. Signal price_watcher (via a threading.Event) that the initial scan is
   complete before the watcher starts.

Thread model:
    One daemon thread per symbol instance (self._thread) runs _run_loop(),
    which calls _scan_all() immediately, then sleeps ZONE_REFRESH_HOURS * 3600
    before repeating. The matching PriceWatcher blocks on self.zones_ready.
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import List

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import ZoneEvent, ZonesRefreshedEvent
from indicators.calculator import find_swing_highs_lows, cluster_zones

logger = logging.getLogger(__name__)


class SRMapper:
    """
    S/R zone scanner and publisher for a single symbol.

    Injected dependencies:
        client: Shared MT5Client (already connected).
        bus:    Shared EventBus for publishing ZoneEvents.
        db:     Shared Database for deactivating stale zones before refresh.
        symbol: Base symbol this instance is responsible for (e.g. "EURUSD").
    """

    def __init__(self, client: MT5Client, bus: EventBus, db: Database, symbol: str) -> None:
        self._client = client
        self._bus    = bus
        self._db     = db
        self._symbol = symbol

        # Matching PriceWatcher blocks on this until the first zone scan completes
        self.zones_ready = threading.Event()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"SRMapper-{symbol}",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

    def start(self) -> None:
        """Start the background mapper thread."""
        logger.info("SRMapper[%s] starting …", self._symbol)
        self._thread.start()

    def stop(self) -> None:
        """Signal the mapper to stop and wait for the thread to exit."""
        logger.info("SRMapper[%s] stopping …", self._symbol)
        self._stop_event.set()
        self._thread.join(timeout=30)

    def restart(self) -> None:
        """Restart a dead thread (called by the main watchdog)."""
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name=f"SRMapper-{self._symbol}", daemon=True
        )
        self._thread.start()
        logger.warning("SRMapper[%s] thread restarted by watchdog.", self._symbol)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """
        Main loop: scan immediately, then repeat every ZONE_REFRESH_HOURS.
        Every iteration body is wrapped in try/except so a single error
        (e.g. network blip) never kills the thread.
        """
        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            try:
                self._scan_all()
                self.zones_ready.set()      # unblock price_watcher
            except Exception:
                logger.exception("SRMapper._scan_all raised — will retry at next interval")
                self.zones_ready.set()      # unblock watcher even on failure

            # Sleep in 30-second chunks so we respond to stop signals promptly
            interval_sec = config.ZONE_REFRESH_HOURS * 3600
            elapsed = 0
            while elapsed < interval_sec and not self._stop_event.is_set():
                time.sleep(30)
                elapsed += 30
                self.last_heartbeat = time.time()

    # ------------------------------------------------------------------
    # Zone scanning
    # ------------------------------------------------------------------

    def _scan_all(self) -> None:
        """
        Scan all configured timeframes for this instance's symbol and publish ZoneEvents.
        """
        logger.info("SRMapper[%s] starting zone scan …", self._symbol)
        total_zones = 0

        symbol = config.resolve_symbol(self._symbol)
        for tf in config.SR_TIMEFRAMES:
            try:
                zones = self._scan_symbol_tf(symbol, self._symbol, tf)
                total_zones += zones
            except Exception:
                logger.exception(
                    "SRMapper._scan_symbol_tf failed for %s %s — skipping", symbol, tf
                )

        logger.info("SRMapper[%s] zone scan complete. Published %d zones.", self._symbol, total_zones)

    def _scan_symbol_tf(self, symbol: str, symbol_base: str, tf: str) -> int:
        """
        Scan a single symbol+timeframe, detect zones, deactivate old DB rows,
        and publish ZoneEvents for each new zone.

        Args:
            symbol:      Broker-resolved symbol name (e.g. "XAUUSD.r").
            symbol_base: Base symbol name without suffix (e.g. "XAUUSD").
            tf:          MT5 timeframe string (e.g. "H1").

        Returns:
            int: Number of zones published.
        """
        # Fetch candles — returns DataFrame sorted newest-first
        df = self._client.market.get_candles_latest(
            symbol, tf, count=config.SR_CANDLE_COUNT
        )
        if df is None or len(df) == 0:
            logger.warning("No candles returned for %s %s", symbol, tf)
            return 0

        # Detect swing pivots (find_swing_highs_lows sorts ascending internally)
        swing_highs, swing_lows = find_swing_highs_lows(df, lookback=config.SWING_LOOKBACK)

        logger.debug(
            "%s %s: %d swing highs, %d swing lows",
            symbol_base, tf, len(swing_highs), len(swing_lows),
        )

        # Cluster pivots into zones
        resistance_zones = cluster_zones(swing_highs, "resistance", tolerance=config.CLUSTER_TOLERANCE)
        support_zones    = cluster_zones(swing_lows,  "support",    tolerance=config.CLUSTER_TOLERANCE)
        all_zones        = resistance_zones + support_zones

        # Record the scan start time BEFORE publishing new zones.
        # Because the EventBus is synchronous, all db_consumer inserts happen
        # during the publish loop below (created_at >= scan_start).
        # We then deactivate only zones created BEFORE scan_start, eliminating
        # the gap window where price_watcher would see zero active zones.
        scan_start = datetime.now(tz=timezone.utc).isoformat()

        # Publish new zones first — db_consumer INSERTs them synchronously
        count = 0
        for z in all_zones:
            try:
                event = ZoneEvent(
                    symbol=symbol_base,
                    timeframe=tf,
                    zone_type=z["zone_type"],
                    price_center=z["price_center"],
                    price_upper=z["price_upper"],
                    price_lower=z["price_lower"],
                    strength=z["strength"],
                    is_active=True,
                )
                self._bus.publish(event)
                count += 1
            except Exception:
                logger.exception("Failed to publish ZoneEvent for %s %s", symbol_base, tf)

        # Emit ZonesRefreshedEvent — db_consumer calls deactivate_zones_before() on receipt
        self._bus.publish(ZonesRefreshedEvent(
            symbol=symbol_base,
            timeframe=tf,
            refreshed_at=datetime.now(tz=timezone.utc),
            zones_deactivated_before=datetime.fromisoformat(scan_start),
        ))

        logger.info(
            "%s %s: published %d zones (%d resistance, %d support)",
            symbol_base, tf, count, len(resistance_zones), len(support_zones),
        )
        return count
