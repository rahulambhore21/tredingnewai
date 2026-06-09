"""
main.py — Trading bot entry point.

Boot sequence:
    1. Configure logging.
    2. Validate required environment variables.
    3. Connect to MT5 terminal via MT5Client.
    4. Initialise Database, EventBus, DBConsumer.
    5. Instantiate all agents (SRMapper, PriceWatcher, AnalysisAgent,
       RiskAgent, Executor) with shared client/bus/db.
    6. Start threads: SRMapper first (populates zones), then PriceWatcher
       (blocks internally until SRMapper signals zones_ready).
       AnalysisAgent, RiskAgent, and Executor are event-driven
       (no threads of their own; they react to events on the bus).
    7. Block on KeyboardInterrupt, then gracefully stop all threads and
       disconnect from MT5.

Run:
    python main.py

Toggle dry-run:
    Set EXECUTION_LIVE=False in your .env file or environment.
"""

import logging
import logging.handlers
import sys
import time

import config
from core.database import Database
from core.event_bus import EventBus
from core.db_consumer import DBConsumer
from core.signal_tracker import SignalTracker
from agents.sr_mapper import SRMapper
from agents.price_watcher import PriceWatcher
from agents.analysis_agent import AnalysisAgent
from agents.risk_agent import RiskAgent
from agents.executor import Executor
from agents.trade_monitor import TradeMonitor


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

def configure_logging() -> None:
    """
    Set up root logger to write to both stdout and a rolling file.
    Format includes timestamp, level, logger name, and message.
    """
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.handlers.RotatingFileHandler(
                "trading_bot.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            ),
        ],
    )
    # Quieten noisy third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


configure_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------

def _validate_config() -> None:
    """
    Check that critical config values are present before attempting to
    connect to external services.  Exits with a clear message if anything
    is missing so the user knows what to set in .env.
    """
    errors = []

    if not config.MT5_CONFIG.get("login") or config.MT5_CONFIG["login"] == 0:
        errors.append("MT5_LOGIN is not set (must be a non-zero integer).")
    if not config.MT5_CONFIG.get("password"):
        errors.append("MT5_PASSWORD is not set.")
    if not config.MT5_CONFIG.get("server"):
        errors.append("MT5_SERVER is not set.")
    if not config.OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY is not set.")

    if errors:
        logger.error("Configuration errors — cannot start:")
        for err in errors:
            logger.error("  • %s", err)
        logger.error("Create a .env file based on .env.example and try again.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Main entry point: boot all components, start threads, wait for shutdown.
    """
    logger.info("=" * 60)
    logger.info("Trading Bot starting up …")
    logger.info("EXECUTION_LIVE = %s", config.EXECUTION_LIVE)
    logger.info("Instruments: %s", config.INSTRUMENTS)
    logger.info("=" * 60)

    # --- Config validation ---
    _validate_config()

    # --- MT5 connection ---
    from metatrader_client import MT5Client

    client = MT5Client(config.MT5_CONFIG)
    logger.info("Connecting to MT5 terminal …")
    try:
        connected = client.connect()
        if not connected:
            logger.error("MT5Client.connect() returned False — aborting.")
            sys.exit(1)
    except Exception as exc:
        logger.error("MT5 connection failed: %s", exc)
        sys.exit(1)

    logger.info("MT5 connected successfully.")

    # --- Core infrastructure ---
    db       = Database(config.DB_PATH)
    bus      = EventBus()
    consumer = DBConsumer(db, bus)           # wires all subscriptions

    # --- Per-symbol agents (one thread each) ---
    signal_tracker = SignalTracker()
    sr_mappers:     dict = {}
    price_watchers: dict = {}
    analyses:       dict = {}

    for sym in config.INSTRUMENTS:
        sr_mappers[sym]     = SRMapper(client, bus, db, symbol=sym)
        price_watchers[sym] = PriceWatcher(
            client, bus, db,
            zones_ready=sr_mappers[sym].zones_ready,
            symbol=sym,
        )
        analyses[sym] = AnalysisAgent(
            client, bus, symbol=sym,
            signal_tracker=signal_tracker,
        )

    # --- Shared agents (event-driven, symbol-agnostic) ---
    risk          = RiskAgent(client, bus, db)   # subscribes to SignalGeneratedEvent
    executor      = Executor(client, bus)        # subscribes to RiskEvaluatedEvent
    trade_monitor = TradeMonitor(client, bus, signal_tracker=signal_tracker)

    # --- Start threads ---
    for sym in config.INSTRUMENTS:
        sr_mappers[sym].start()       # scans zones; sets zones_ready per symbol
        price_watchers[sym].start()   # waits for its symbol's zones_ready
        analyses[sym].start()         # dedicated GPT thread per symbol

    trade_monitor.start()   # polls all open positions every 30 s

    logger.info(
        "All threads started (%d symbols × 3 threads + TradeMonitor). Bot is running."
        " Press Ctrl+C to stop.",
        len(config.INSTRUMENTS),
    )
    logger.info("Trading hours enforcement: disabled — bot may trade any time")

    # --- Watchdog setup ---
    _WATCHDOG_AGENTS = []
    _HEARTBEAT_THRESHOLDS = {}
    for sym in config.INSTRUMENTS:
        _WATCHDOG_AGENTS.append((sr_mappers[sym],     f"SRMapper-{sym}"))
        _WATCHDOG_AGENTS.append((price_watchers[sym], f"PriceWatcher-{sym}"))
        _WATCHDOG_AGENTS.append((analyses[sym],       f"AnalysisAgent-{sym}"))
        _HEARTBEAT_THRESHOLDS[f"SRMapper-{sym}"]     = 150
        _HEARTBEAT_THRESHOLDS[f"PriceWatcher-{sym}"] = config.TICK_INTERVAL_SEC * 5
        _HEARTBEAT_THRESHOLDS[f"AnalysisAgent-{sym}"] = 60
    _WATCHDOG_AGENTS.append((trade_monitor, "TradeMonitor"))
    _HEARTBEAT_THRESHOLDS["TradeMonitor"] = 150

    _last_heartbeat = time.time()

    try:
        while True:
            time.sleep(60)   # check every minute

            # MT5 connection health check — attempt reconnect if lost
            try:
                info = client.account.get_account_info()
                if info is None:
                    raise RuntimeError("get_account_info returned None")
            except Exception:
                logger.warning("MT5 connection check failed — attempting reconnect …")
                try:
                    client.connect()
                    logger.info("MT5 reconnected successfully.")
                except Exception:
                    logger.exception("MT5 reconnect failed — will retry next cycle")

            # Heartbeat staleness check — alert if any agent loop has gone silent
            now = time.time()
            for agent_obj, label in _WATCHDOG_AGENTS:
                threshold = _HEARTBEAT_THRESHOLDS.get(label, 150)
                stale_sec = now - getattr(agent_obj, "last_heartbeat", now)
                if stale_sec > threshold:
                    logger.critical(
                        "HEARTBEAT STALE: %s has not updated its heartbeat for %.0f s"
                        " (threshold %.0f s) — agent may be stuck",
                        label, stale_sec, threshold,
                    )

            # Thread watchdog — restart any dead agent threads
            for agent_obj, label in _WATCHDOG_AGENTS:
                if not agent_obj._thread.is_alive():
                    logger.error("%s thread died — restarting", label)
                    agent_obj.restart()

            # Heartbeat log every 5 minutes
            now = time.time()
            if (now - _last_heartbeat) >= 300:
                for sym in config.INSTRUMENTS:
                    logger.info(
                        "Heartbeat[%s] — sr_mapper=%s  price_watcher=%s  analysis=%s",
                        sym,
                        sr_mappers[sym]._thread.is_alive(),
                        price_watchers[sym]._thread.is_alive(),
                        analyses[sym]._thread.is_alive(),
                    )
                logger.info("Heartbeat — trade_monitor=%s", trade_monitor._thread.is_alive())
                _last_heartbeat = now

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down gracefully …")

    # --- Shutdown ---
    for sym in config.INSTRUMENTS:
        analyses[sym].stop()
    trade_monitor.stop()
    for sym in config.INSTRUMENTS:
        price_watchers[sym].stop()
        sr_mappers[sym].stop()

    try:
        client.disconnect()
        logger.info("MT5 disconnected.")
    except Exception:
        logger.exception("Error disconnecting from MT5.")

    db.close()
    logger.info("Trading Bot stopped.")


if __name__ == "__main__":
    main()
