"""
agents/sr_mapper.py — Support & Resistance zone mapper.

Scans S/R zones for XAUUSD only (single active symbol) on both M5 and M15 on startup.
Refreshes every ZONE_REFRESH_HOURS hours.
Signals zones_ready only after all symbols are done.
"""

import logging
import threading
import time
from datetime import datetime, timezone

from metatrader_client import MT5Client

import config
from core.database import Database
from core.event_bus import EventBus
from core.events import ZoneEvent, ZonesRefreshedEvent
from indicators.calculator import find_swing_highs_lows, cluster_zones

logger = logging.getLogger(__name__)


class SRMapper:
    """
    S/R zone scanner for all configured symbols.

    Loops all symbols on M5 and M15; detects swing highs/lows; clusters into
    zones tagged with symbol; writes to DB via event bus.
    Signals zones_ready only after all symbols are done.
    """

    def __init__(self, client: MT5Client, bus: EventBus, db: Database) -> None:
        self._client = client
        self._bus    = bus
        self._db     = db

        self.zones_ready = threading.Event()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="SRMapper",
            daemon=True,
        )
        self.last_heartbeat: float = time.time()

    def start(self) -> None:
        logger.info("SRMapper starting …")
        self._thread.start()

    def stop(self) -> None:
        logger.info("SRMapper stopping …")
        self._stop_event.set()
        self._thread.join(timeout=30)

    def restart(self) -> None:
        if self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="SRMapper", daemon=True
        )
        self._thread.start()
        logger.warning("SRMapper thread restarted by watchdog.")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self.last_heartbeat = time.time()
            try:
                self._scan_all()
                self.zones_ready.set()
            except Exception:
                logger.exception("SRMapper._scan_all raised — will retry at next interval")
                self.zones_ready.set()

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
        """Scan all configured symbols on all configured timeframes."""
        logger.info("SRMapper starting zone scan for all symbols …")
        total_zones = 0

        for symbol_base in config.SYMBOLS:
            symbol = config.resolve_symbol(symbol_base)
            for tf in config.SR_TIMEFRAMES:
                try:
                    zones = self._scan_symbol_tf(symbol, symbol_base, tf)
                    total_zones += zones
                except Exception:
                    logger.exception(
                        "SRMapper._scan_symbol_tf failed for %s %s — skipping", symbol, tf
                    )

        logger.info("SRMapper zone scan complete. Published %d zones total.", total_zones)

    def _scan_symbol_tf(self, symbol: str, symbol_base: str, tf: str) -> int:
        """
        Scan a single symbol+timeframe, detect zones, and atomically swap
        old DB rows for new ones.

        Deactivation happens BEFORE candle fetch so PriceWatcher never sees
        old zones mixed with new ones (a brief gap with no zones is safer
        than a window where both coexist and double-fire analysis).
        This direct DB write is the documented exception to the single-writer
        rule — sr_mapper owns the zone lifecycle.
        """
        # Atomically retire old zones before computing new ones.
        self._db.deactivate_zones_for_symbol(symbol_base, tf)

        df = self._client.market.get_candles_latest(
            symbol, tf, count=config.SR_CANDLE_COUNT
        )
        if df is None or len(df) == 0:
            logger.warning("No candles returned for %s %s", symbol, tf)
            return 0

        swing_highs, swing_lows = find_swing_highs_lows(df, lookback=config.SWING_LOOKBACK)

        logger.debug(
            "%s %s: %d swing highs, %d swing lows",
            symbol_base, tf, len(swing_highs), len(swing_lows),
        )

        resistance_zones = cluster_zones(swing_highs, "resistance", tolerance=config.CLUSTER_TOLERANCE)
        support_zones    = cluster_zones(swing_lows,  "support",    tolerance=config.CLUSTER_TOLERANCE)
        all_zones        = resistance_zones + support_zones

        refreshed_at = datetime.now(tz=timezone.utc)

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

        # Publish for audit-log purposes; deactivation already happened above.
        self._bus.publish(ZonesRefreshedEvent(
            symbol=symbol_base,
            timeframe=tf,
            refreshed_at=refreshed_at,
            zones_deactivated_before=refreshed_at,
        ))

        logger.info(
            "%s %s: published %d zones (%d resistance, %d support)",
            symbol_base, tf, count, len(resistance_zones), len(support_zones),
        )
        return count
