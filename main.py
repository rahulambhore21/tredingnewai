"""
main.py — Trading bot entry point.

Boot sequence:
    1. Configure logging.
    2. Validate required environment variables.
    3. Connect to MT5 terminal via MT5Client.
    4. Initialise Database, EventBus, DBConsumer.
    5. Instantiate all agents:
         SRMapper     — scans all symbols; signals zones_ready when done
         PriceWatcher — single loop over all symbols; waits for zones_ready
         AnalysisAgent — single queue+thread; handles all ZoneTouchEvents
         RiskAgent    — synchronous event handler
         Executor     — synchronous event handler
         TradeMonitor — polls every 30s for closed positions
    6. Start threads; watchdog loop checks MT5 connection and thread health.
    7. Graceful shutdown on Ctrl+C.

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
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


configure_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------

def _validate_config() -> None:
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
    logger.info("=" * 60)
    logger.info("Trading Bot starting up …")
    logger.info("EXECUTION_LIVE = %s", config.EXECUTION_LIVE)
    logger.info("Symbols: %s", config.SYMBOLS)
    logger.info("=" * 60)

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
    consumer = DBConsumer(db, bus)

    # --- Single-instance agents ---
    sr_mapper     = SRMapper(client, bus, db)
    price_watcher = PriceWatcher(client, bus, db, zones_ready=sr_mapper.zones_ready)
    analysis      = AnalysisAgent(client, bus)

    # --- Synchronous event-driven agents (no threads) ---
    risk     = RiskAgent(client, bus, db)
    executor = Executor(client, bus)

    # --- TradeMonitor — polls MT5 every 30s for closed positions ---
    trade_monitor = TradeMonitor(client, bus)

    # --- Start threads ---
    sr_mapper.start()       # scans zones for all symbols; sets zones_ready
    price_watcher.start()   # waits for zones_ready; polls all symbols
    analysis.start()        # dedicated GPT thread
    trade_monitor.start()   # polls closed positions every 30s

    logger.info(
        "All threads started (SRMapper, PriceWatcher, AnalysisAgent, TradeMonitor). Bot is running."
        " Press Ctrl+C to stop."
    )

    # --- Watchdog setup ---
    _WATCHDOG_AGENTS = [
        (sr_mapper,     "SRMapper",     150),
        (price_watcher, "PriceWatcher", config.TICK_INTERVAL_SEC * 5),
        (analysis,      "AnalysisAgent", 60),
        (trade_monitor, "TradeMonitor",  90),
    ]

    _last_heartbeat_log = time.time()

    try:
        while True:
            time.sleep(60)

            # MT5 connection health check
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

            # Thread watchdog — restart any dead agent threads
            now = time.time()
            for agent_obj, label, threshold in _WATCHDOG_AGENTS:
                stale_sec = now - getattr(agent_obj, "last_heartbeat", now)
                if stale_sec > threshold:
                    logger.critical(
                        "HEARTBEAT STALE: %s has not updated for %.0f s (threshold %.0f s)",
                        label, stale_sec, threshold,
                    )
                thread = getattr(agent_obj, "_thread", None)
                if thread is None or not thread.is_alive():
                    logger.error("%s thread died — restarting", label)
                    agent_obj.restart()

            # Heartbeat log every 5 minutes
            if (now - _last_heartbeat_log) >= 300:
                def _alive(agent: object) -> bool:
                    t = getattr(agent, "_thread", None)
                    return t is not None and t.is_alive()
                logger.info(
                    "Heartbeat — sr_mapper=%s  price_watcher=%s  analysis=%s  trade_monitor=%s",
                    _alive(sr_mapper),
                    _alive(price_watcher),
                    _alive(analysis),
                    _alive(trade_monitor),
                )
                _last_heartbeat_log = now

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down gracefully …")

    # --- Shutdown ---
    analysis.stop()
    trade_monitor.stop()
    price_watcher.stop()
    sr_mapper.stop()

    try:
        client.disconnect()
        logger.info("MT5 disconnected.")
    except Exception:
        logger.exception("Error disconnecting from MT5.")

    db.close()
    logger.info("Trading Bot stopped.")


if __name__ == "__main__":
    main()
