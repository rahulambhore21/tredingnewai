"""
worker.py — Single-account trading bot worker process.

Spawned by main.py, one process per MT5 account. Each worker connects
exclusively to its own MT5 terminal installation and runs the full agent
pipeline for that account in isolation.

Usage (normally launched by main.py, not directly):
    python worker.py --account <1-4> --direction <BUY|SELL>
"""

import argparse
import logging
import logging.handlers
import signal
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


def _configure_logging(account_id: int) -> None:
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.handlers.RotatingFileHandler(
                f"trading_bot_{account_id}.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            ),
        ],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def reconcile_stale_trades(db: "Database", mt5_client, account_id: int) -> None:
    """
    Runs once on startup. Finds trades that the DB thinks are still open
    (close_time IS NULL) but no longer exist as live MT5 positions, then
    marks them closed so get_open_trade_count() returns an accurate count.
    """
    logger = logging.getLogger(__name__)

    stale_order_ids = db.get_open_trades(account_id)
    if not stale_order_ids:
        logger.info(
            "Worker[acct=%d]: reconcile — no unclosed DB trades to check.", account_id
        )
        return

    try:
        positions_df = mt5_client.order.get_all_positions()
    except Exception:
        logger.exception(
            "Worker[acct=%d]: reconcile — could not fetch MT5 positions; skipping.", account_id
        )
        return

    live_ids: set = set()
    if positions_df is not None and len(positions_df) > 0:
        for id_col in ("id", "ticket"):
            if id_col in positions_df.columns:
                live_ids = {int(x) for x in positions_df[id_col].tolist()}
                break

    for order_id in stale_order_ids:
        if order_id not in live_ids:
            db.update_trade_close(order_id, close_price=0.0, realized_pnl=0.0)
            logger.info(
                "Worker[acct=%d]: reconciled stale DB trade order_id=%s "
                "— closed in MT5 but not recorded in DB.",
                account_id, order_id,
            )

    reconciled = len([oid for oid in stale_order_ids if oid not in live_ids])
    logger.info(
        "Worker[acct=%d]: reconcile complete — %d stale trade(s) closed, "
        "%d still live in MT5.",
        account_id, reconciled, len(stale_order_ids) - reconciled,
    )


def _thread_alive(agent) -> bool:
    t = getattr(agent, "_thread", None)
    return t is not None and t.is_alive()


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-account trading bot worker")
    parser.add_argument("--account",   type=int, required=True, choices=[1, 2, 3, 4])
    parser.add_argument("--direction", type=str, required=True, choices=["BUY", "SELL"])
    args = parser.parse_args()

    account_id = args.account
    direction  = args.direction.upper()

    _configure_logging(account_id)
    logger = logging.getLogger(__name__)

    # Register SIGTERM so the watchdog loop's KeyboardInterrupt handler fires
    # on both Ctrl+C and the supervisor's CTRL_BREAK_EVENT / SIGTERM signal.
    def _on_sigterm(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _on_sigterm)
    if hasattr(signal, "SIGBREAK"):          # Windows only
        signal.signal(signal.SIGBREAK, _on_sigterm)

    acct = next((a for a in config.ACCOUNTS if a["account_id"] == account_id), None)
    if acct is None:
        logger.error("No config found for account_id=%d — check MT5_LOGIN_%d in .env", account_id, account_id)
        sys.exit(1)

    terminal_path = acct.get("terminal_path") or ""
    db_path       = f"trading_bot_{account_id}.db"

    logger.info("=" * 60)
    logger.info("Worker[acct=%d dir=%s] starting …", account_id, direction)
    logger.info("  Terminal : %s", terminal_path or "(auto-detect)")
    logger.info("  DB       : %s", db_path)
    logger.info("  Server   : %s", acct["server"])
    logger.info("  Live     : %s", config.EXECUTION_LIVE)
    logger.info("=" * 60)

    from metatrader_client import MT5Client

    mt5_cfg = {
        "login":    acct["login"],
        "password": acct["password"],
        "server":   acct["server"],
        "path":     terminal_path or None,
    }
    client = MT5Client(mt5_cfg)

    logger.info("Worker[acct=%d]: connecting to MT5 terminal …", account_id)
    try:
        if not client.connect():
            logger.error("Worker[acct=%d]: MT5Client.connect() returned False", account_id)
            sys.exit(1)
    except Exception as exc:
        logger.error("Worker[acct=%d]: MT5 connection failed — %s", account_id, exc)
        sys.exit(1)
    logger.info("Worker[acct=%d]: connected.", account_id)

    db             = Database(db_path)
    bus            = EventBus()
    consumer       = DBConsumer(db, bus)
    signal_tracker = SignalTracker()

    reconcile_stale_trades(db, client, account_id)

    sr_mapper     = SRMapper(client, bus, db)
    price_watcher = PriceWatcher(client, bus, db, zones_ready=sr_mapper.zones_ready)
    analysis      = AnalysisAgent(client, bus, db, acct, signal_tracker=signal_tracker)
    risk          = RiskAgent(client, bus, db, acct)
    executor      = Executor(client, bus, acct)
    monitor       = TradeMonitor(client, bus, account_id=account_id, signal_tracker=signal_tracker)

    sr_mapper.start()
    price_watcher.start()
    analysis.start()
    monitor.start()

    logger.info(
        "Worker[acct=%d]: all threads started — SRMapper, PriceWatcher, "
        "AnalysisAgent, TradeMonitor. Running.",
        account_id,
    )

    _WATCHDOG = [
        (sr_mapper,     "SRMapper",                    150),
        (price_watcher, "PriceWatcher",                config.TICK_INTERVAL_SEC * 5),
        (analysis,      f"AnalysisAgent-{account_id}", 60),
        (monitor,       f"TradeMonitor-{account_id}",  90),
    ]

    _last_heartbeat_log = time.time()

    try:
        while True:
            time.sleep(60)

            # MT5 health check + reconnect
            try:
                if client.account.get_account_info() is None:
                    raise RuntimeError("get_account_info returned None")
            except Exception:
                logger.warning(
                    "Worker[acct=%d]: MT5 health check failed — reconnecting …", account_id
                )
                try:
                    client.connect()
                    logger.info("Worker[acct=%d]: MT5 reconnected.", account_id)
                except Exception:
                    logger.exception(
                        "Worker[acct=%d]: MT5 reconnect failed — will retry next cycle", account_id
                    )

            # Thread watchdog — restart stale or dead agents
            now = time.time()
            for agent_obj, label, threshold in _WATCHDOG:
                stale_sec = now - getattr(agent_obj, "last_heartbeat", now)
                if stale_sec > threshold:
                    logger.critical(
                        "HEARTBEAT STALE: %s not updated for %.0f s (threshold %.0f s)",
                        label, stale_sec, threshold,
                    )
                thread = getattr(agent_obj, "_thread", None)
                if thread is None or not thread.is_alive():
                    logger.error("%s thread died — restarting", label)
                    agent_obj.restart()

            # Periodic heartbeat log
            if (now - _last_heartbeat_log) >= 300:
                logger.info(
                    "Worker[acct=%d] heartbeat — "
                    "sr_mapper=%s  price_watcher=%s  analysis=%s  monitor=%s",
                    account_id,
                    _thread_alive(sr_mapper), _thread_alive(price_watcher),
                    _thread_alive(analysis),  _thread_alive(monitor),
                )
                _last_heartbeat_log = now

    except KeyboardInterrupt:
        logger.info("Worker[acct=%d]: shutdown signal received — stopping …", account_id)

    # Graceful shutdown in reverse startup order
    analysis.stop()
    monitor.stop()
    price_watcher.stop()
    sr_mapper.stop()

    try:
        client.disconnect()
        logger.info("Worker[acct=%d]: MT5 disconnected.", account_id)
    except Exception:
        logger.exception("Worker[acct=%d]: error disconnecting MT5", account_id)

    db.close()
    logger.info("Worker[acct=%d]: stopped.", account_id)


if __name__ == "__main__":
    main()
