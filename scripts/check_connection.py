"""
scripts/check_connection.py — Standalone MT5 connection and symbol sanity check.

Usage:
    python scripts/check_connection.py

Connects read-only (no orders), prints account info and symbol details for
every instrument in config.SYMBOLS, then disconnects cleanly.
"""

import sys
import os

# Allow imports from the project root whether run as `python scripts/...`
# or from the project root directly.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config
from metatrader_client import MT5Client


def main() -> None:
    print("=" * 60)
    print("MT5 Connection Check")
    print("=" * 60)

    client = MT5Client(config.MT5_CONFIG)
    try:
        connected = client.connect()
    except Exception as exc:
        print(f"[ERROR] MT5Client.connect() raised: {exc}")
        sys.exit(1)

    if not connected:
        print("[ERROR] MT5Client.connect() returned False — check credentials/server.")
        sys.exit(1)

    # Account info
    try:
        info = client.account.get_account_info()
        print("\n--- Account ---")
        print(f"  Login   : {info.get('login', 'N/A')}")
        print(f"  Balance : {info.get('balance', 'N/A')}")
        print(f"  Currency: {info.get('currency', 'N/A')}")
        print(f"  Server  : {info.get('server', 'N/A')}")
    except Exception as exc:
        print(f"[ERROR] get_account_info() failed: {exc}")

    # Symbol info for each configured instrument
    print("\n--- Symbols ---")
    for base in config.SYMBOLS:
        symbol = config.resolve_symbol(base)
        try:
            si = client.market.get_symbol_info(symbol)
            if si is None:
                print(
                    f"[WARNING] Symbol '{symbol}' not found. "
                    "Check broker suffix (e.g. EURUSD.r)"
                )
                continue

            # Try current price
            try:
                tick = client.market.get_symbol_price(symbol)
                bid = tick.get("bid", "N/A")
                ask = tick.get("ask", "N/A")
            except Exception:
                bid = ask = "N/A"

            print(f"\n  Symbol          : {si.get('name', symbol)}")
            print(f"  Bid / Ask       : {bid} / {ask}")
            print(f"  volume_min      : {si.get('volume_min', 'N/A')}")
            print(f"  volume_max      : {si.get('volume_max', 'N/A')}")
            print(f"  volume_step     : {si.get('volume_step', 'N/A')}")
            print(f"  contract_size   : {si.get('trade_contract_size', 'N/A')}")
        except Exception as exc:
            print(
                f"[WARNING] Symbol '{symbol}' not found. "
                "Check broker suffix (e.g. EURUSD.r)\n"
                f"  Detail: {exc}"
            )

    # Disconnect
    try:
        client.disconnect()
        print("\n[OK] Disconnected cleanly.")
    except Exception as exc:
        print(f"[WARNING] Disconnect raised: {exc}")

    print("=" * 60)


if __name__ == "__main__":
    main()
