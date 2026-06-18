"""
main.py — Trading bot entry point (4-account mode).

Boot sequence:
    1. Configure logging.
    2. Validate 4 account configs from env vars.
    3. Connect 4 MT5 clients (one per account).
    4. Initialise shared infrastructure: Database, EventBus, DBConsumer.
    5. Start shared market agents (SRMapper, PriceWatcher) using account-1 client.
    6. Start 4 per-account pipelines: AnalysisAgent, RiskAgent, Executor, TradeMonitor.
    7. Watchdog loop: MT5 health checks + thread restarts.
    8. Graceful shutdown on Ctrl+C.

Toggle dry-run: Set EXECUTION_LIVE=False in .env.
"""

import logging
import logging.handlers
import sys
import time
from typing import Dict, List

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

    if not config.ACCOUNTS:
        errors.append(
            "No account configs found. Set MT5_LOGIN_1, MT5_PASSWORD_1, MT5_SERVER_1 "
            "(up to _4) in .env."
        )
    else:
        for acct in config.ACCOUNTS:
            i = acct["account_id"]
            if not acct["login"]:
                errors.append(f"MT5_LOGIN_{i} is not set or zero.")
            if not acct["password"]:
                errors.append(f"MT5_PASSWORD_{i} is not set.")
            if not acct["server"]:
                errors.append(f"MT5_SERVER_{i} is not set.")
            if acct["direction"] not in ("BUY", "SELL"):
                errors.append(
                    f"MT5_DIRECTION_{i} must be BUY or SELL (got '{acct['direction']}')."
                )

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
    logger.info("Trading Bot starting up (4-account mode) …")
    logger.info("EXECUTION_LIVE = %s", config.EXECUTION_LIVE)
    logger.info("Symbols: %s", config.SYMBOLS)
    logger.info("Accounts: %s", [(a["account_id"], a["direction"]) for a in config.ACCOUNTS])
    logger.info(
        "SL_USD=%.0f  TP_USD=%.0f  LOT_MIN=%.2f  LOT_MAX=%.2f  MAX_DAILY_TRADES=%d",
        config.SL_USD, config.TP_USD, config.LOT_MIN, config.LOT_MAX, config.MAX_DAILY_TRADES,
    )
    logger.info("=" * 60)

    _validate_config()

    from metatrader_client import MT5Client

    # --- Connect one MT5 client per account ---
    clients: Dict[int, MT5Client] = {}
    for acct in config.ACCOUNTS:
        i = acct["account_id"]
        mt5_cfg = {
            "login":    acct["login"],
            "password": acct["password"],
            "server":   acct["server"],
        }
        client = MT5Client(mt5_cfg)
        logger.info("Connecting account %d to MT5 (%s) …", i, acct["server"])
        try:
            if not client.connect():
                logger.error("MT5Client.connect() returned False for account %d — aborting.", i)
                sys.exit(1)
        except Exception as exc:
            logger.error("MT5 connection failed for account %d: %s", i, exc)
            sys.exit(1)
        clients[i] = client
        logger.info("Account %d connected.", i)

    # Use account 1's client for shared market-data agents (SRMapper, PriceWatcher)
    shared_client = clients[config.ACCOUNTS[0]["account_id"]]

    # --- Shared infrastructure ---
    db       = Database(config.DB_PATH)
    bus      = EventBus()
    consumer = DBConsumer(db, bus)

    # --- Shared market agents ---
    sr_mapper     = SRMapper(shared_client, bus, db)
    price_watcher = PriceWatcher(shared_client, bus, db, zones_ready=sr_mapper.zones_ready)

    # --- 4 per-account pipelines ---
    analyses:  List[AnalysisAgent] = []
    risks:     List[RiskAgent]     = []
    executors: List[Executor]      = []
    monitors:  List[TradeMonitor]  = []

    for acct in config.ACCOUNTS:
        i      = acct["account_id"]
        client = clients[i]

        analyses.append(AnalysisAgent(client, bus, db, acct))
        risks.append(RiskAgent(client, bus, db, acct))
        executors.append(Executor(client, bus, acct))
        monitors.append(TradeMonitor(client, bus, account_id=i))

    # --- Start shared threads ---
    sr_mapper.start()
    price_watcher.start()

    # --- Start per-account threads ---
    for analysis in analyses:
        analysis.start()
    for monitor in monitors:
        monitor.start()

    n = len(config.ACCOUNTS)
    logger.info(
        "All threads started: SRMapper, PriceWatcher, %d×AnalysisAgent, %d×TradeMonitor. "
        "Bot is running. Press Ctrl+C to stop.",
        n, n,
    )

    # --- Watchdog setup ---
    _WATCHDOG_SHARED = [
        (sr_mapper,     "SRMapper",     150),
        (price_watcher, "PriceWatcher", config.TICK_INTERVAL_SEC * 5),
    ]
    _WATCHDOG_PER_ACCOUNT = [
        (analysis, f"AnalysisAgent-{analysis._account_id}", 60)
        for analysis in analyses
    ] + [
        (monitor, f"TradeMonitor-{monitor._account_id}", 90)
        for monitor in monitors
    ]

    _last_heartbeat_log = time.time()

    try:
        while True:
            time.sleep(60)

            # MT5 connection health checks for all accounts
            for i, client in clients.items():
                try:
                    info = client.account.get_account_info()
                    if info is None:
                        raise RuntimeError("get_account_info returned None")
                except Exception:
                    logger.warning(
                        "MT5 connection check failed for account %d — attempting reconnect …", i
                    )
                    try:
                        client.connect()
                        logger.info("Account %d reconnected successfully.", i)
                    except Exception:
                        logger.exception("MT5 reconnect failed for account %d — will retry", i)

            # Thread watchdog — restart any dead agent threads
            now = time.time()
            for agent_obj, label, threshold in _WATCHDOG_SHARED + _WATCHDOG_PER_ACCOUNT:
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
                def _alive(agent) -> bool:
                    t = getattr(agent, "_thread", None)
                    return t is not None and t.is_alive()
                logger.info(
                    "Heartbeat — sr_mapper=%s  price_watcher=%s",
                    _alive(sr_mapper), _alive(price_watcher),
                )
                for analysis in analyses:
                    logger.info(
                        "  AnalysisAgent[acct=%d] alive=%s",
                        analysis._account_id, _alive(analysis),
                    )
                for monitor in monitors:
                    logger.info(
                        "  TradeMonitor[acct=%d] alive=%s",
                        monitor._account_id, _alive(monitor),
                    )
                _last_heartbeat_log = now

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down gracefully …")

    # --- Shutdown ---
    for analysis in analyses:
        analysis.stop()
    for monitor in monitors:
        monitor.stop()
    price_watcher.stop()
    sr_mapper.stop()

    for i, client in clients.items():
        try:
            client.disconnect()
            logger.info("Account %d MT5 disconnected.", i)
        except Exception:
            logger.exception("Error disconnecting account %d from MT5.", i)

    db.close()
    logger.info("Trading Bot stopped.")


if __name__ == "__main__":
    main()
