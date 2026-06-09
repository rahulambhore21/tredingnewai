#!/usr/bin/env bash
# dashboard/run.sh — Start the Trading Bot Monitor on http://localhost:5001
# Run from the project root OR from inside the dashboard/ folder.

set -e

# Navigate to project root regardless of where this script is called from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=== Trading Bot Monitor ==="
echo "DB:  $(python -c "import config; print(config.DB_PATH)" 2>/dev/null || echo "trading_bot.db")"
echo "Log: trading_bot.log"
echo ""

# Install Flask if it is not already present.
python -c "import flask" 2>/dev/null || {
    echo "[setup] Flask not found — installing..."
    pip install flask
    echo ""
}

echo "[start] http://localhost:5001"
echo "[stop]  Ctrl+C"
echo ""

python dashboard/app.py
