"""
scripts/reset_daily.py — Manually reset the daily trade count for all accounts.

Run this when MAX_DAILY_TRADES has been reached but you want to allow new trades
today without waiting for midnight.  It records a reset row in each database's
daily_count_resets table; get_daily_trade_count() returns 0 for the rest of the day.

No trade rows are modified — the reset is recorded in a separate table.

Usage:
    python scripts/reset_daily.py

    # Reset a specific date instead of today (YYYY-MM-DD):
    python scripts/reset_daily.py --date 2026-06-22
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset daily trade count for all accounts")
    parser.add_argument(
        "--date",
        default=None,
        help="Date to reset in YYYY-MM-DD format (default: today UTC)",
    )
    args = parser.parse_args()

    date = args.date or datetime.now(tz=timezone.utc).date().isoformat()

    logger.info("Resetting daily trade counts — date=%s", date)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    for account_id in range(1, 5):
        db_path = os.path.join(project_root, f"trading_bot_{account_id}.db")
        if not os.path.exists(db_path):
            logger.warning("  account %d: %s not found — skipping", account_id, db_path)
            continue

        db = Database(db_path)
        count_before = db.get_daily_trade_count(account_id, date=date)
        db.reset_daily_trade_count(account_id, date=date)
        count_after = db.get_daily_trade_count(account_id, date=date)
        db.close()

        logger.info(
            "  account %d: count %d → %d  (db=%s)",
            account_id, count_before, count_after, os.path.basename(db_path),
        )

    logger.info("Done.")


if __name__ == "__main__":
    main()
