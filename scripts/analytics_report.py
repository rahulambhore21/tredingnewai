"""
scripts/analytics_report.py — CLI performance analytics report.

Usage:
    python scripts/analytics_report.py
    python scripts/analytics_report.py --db trading_bot.db
    python scripts/analytics_report.py --json          # output raw JSON
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure project root is on the path when run from any directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.analytics import AnalyticsEngine
from core.database import Database


def main() -> None:
    parser = argparse.ArgumentParser(description="Trading bot performance analytics")
    parser.add_argument(
        "--db",
        default="trading_bot.db",
        help="Path to the SQLite database file (default: trading_bot.db)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON instead of the formatted report",
    )
    args = parser.parse_args()

    db     = Database(args.db)
    engine = AnalyticsEngine(db)
    report = engine.compute_report()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        engine.print_report(report)

    db.close()


if __name__ == "__main__":
    main()
