"""
main.py — Supervisor process for the 4-account trading bot.

Spawns one worker.py subprocess per MT5 account. Each worker owns its own
MT5 terminal and database, eliminating the MT5 Python library's single-session
constraint. main.py monitors all workers and restarts any that crash.
A single Ctrl+C shuts all workers cleanly.
"""

import logging
import logging.handlers
import signal
import subprocess
import sys
import time
from typing import Dict

import config


def _configure_logging() -> None:
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.handlers.RotatingFileHandler(
                "trading_bot_supervisor.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            ),
        ],
    )


_configure_logging()
logger = logging.getLogger(__name__)


def _validate_config() -> None:
    errors = []
    if not config.ACCOUNTS:
        errors.append(
            "No account configs found. Set MT5_LOGIN_1 … MT5_LOGIN_4 in .env."
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
        sys.exit(1)


def _spawn(account_id: int, direction: str) -> subprocess.Popen:
    """Launch worker.py for one account in its own process group."""
    cmd = [
        sys.executable, "worker.py",
        "--account",   str(account_id),
        "--direction", direction,
    ]
    kwargs: Dict = {}
    if sys.platform == "win32":
        # CREATE_NEW_PROCESS_GROUP lets us send CTRL_BREAK_EVENT for graceful shutdown
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    proc = subprocess.Popen(cmd, **kwargs)
    logger.info(
        "Spawned worker[acct=%d dir=%s] pid=%d", account_id, direction, proc.pid
    )
    return proc


def _stop(account_id: int, proc: subprocess.Popen, grace: float = 15.0) -> None:
    """Send graceful shutdown to one worker; force-kill after `grace` seconds."""
    if proc.poll() is not None:
        return  # already dead
    logger.info("Stopping worker[acct=%d] pid=%d …", account_id, proc.pid)
    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGTERM)
    except OSError:
        logger.warning(
            "Signal delivery failed for worker[acct=%d] — calling terminate()", account_id
        )
        proc.terminate()
    try:
        proc.wait(timeout=grace)
        logger.info("Worker[acct=%d] exited cleanly.", account_id)
    except subprocess.TimeoutExpired:
        logger.warning(
            "Worker[acct=%d] did not exit within %.0f s — killing.", account_id, grace
        )
        proc.kill()
        proc.wait()


def main() -> None:
    logger.info("=" * 60)
    logger.info("Trading Bot Supervisor starting …")
    logger.info("EXECUTION_LIVE = %s", config.EXECUTION_LIVE)
    logger.info(
        "Accounts: %s",
        [(a["account_id"], a["direction"], a["terminal_path"]) for a in config.ACCOUNTS],
    )
    logger.info("=" * 60)

    _validate_config()

    # Spawn one worker per account
    workers: Dict[int, subprocess.Popen] = {}
    for acct in config.ACCOUNTS:
        workers[acct["account_id"]] = _spawn(acct["account_id"], acct["direction"])

    logger.info(
        "All %d workers spawned. Supervisor monitoring. Press Ctrl+C to stop.",
        len(workers),
    )

    try:
        while True:
            time.sleep(10)

            for acct in config.ACCOUNTS:
                account_id = acct["account_id"]
                proc       = workers[account_id]
                exit_code  = proc.poll()
                if exit_code is not None:
                    logger.error(
                        "Worker[acct=%d] exited with code %d — restarting in 5 s …",
                        account_id, exit_code,
                    )
                    time.sleep(5)
                    workers[account_id] = _spawn(account_id, acct["direction"])

    except KeyboardInterrupt:
        logger.info("Ctrl+C received — shutting down all workers …")

    for account_id, proc in workers.items():
        _stop(account_id, proc)

    logger.info("All workers stopped. Supervisor exiting.")


if __name__ == "__main__":
    main()
