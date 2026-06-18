"""
test_lot_sizing.py — Offline unit test for RiskAgent._compute_lot_and_levels.

No MT5 dependency: RiskAgent is constructed with fake client/bus/db, exactly
the way indicators/calculator.py is tested offline (see CLAUDE.md). Run with:

    python test_lot_sizing.py

Exercises EURUSD / USDJPY / XAUUSD against both real broker tick values
(observed in trading_bot.log) and the config fallback values.  Raw lots
outside [LOT_MIN, LOT_MAX] are clamped; the method always returns ok=True.
"""

import logging
import sys

import config
from agents.risk_agent import RiskAgent

# Show INFO so the distinct "Lot sizing" (accept) and "SUB-MIN LOT REJECT"
# (reject) log lines are both visible — demonstrates the item-1 logging.
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s | %(name)s | %(message)s")

# Ensure resolve_symbol() is an identity map for this test (no broker suffix).
config.SYMBOL_SUFFIX = ""


# --------------------------------------------------------------------------
# Fakes — no MT5, no event bus, no DB
# --------------------------------------------------------------------------

class FakeMarket:
    """get_symbol_info returns canned info, or raises to force the fallback path."""

    def __init__(self, info_map):
        self._info_map = info_map  # resolved-symbol -> info dict (or None to raise)

    def get_symbol_info(self, symbol):
        info = self._info_map.get(symbol, "MISSING")
        if info == "MISSING" or info is None:
            raise RuntimeError(f"get_symbol_info unavailable for {symbol} (forced fallback)")
        return info


class FakeClient:
    def __init__(self, info_map):
        self.market = FakeMarket(info_map)


class FakeBus:
    def subscribe(self, *args, **kwargs):
        pass

    def publish(self, *args, **kwargs):
        pass


class FakeDB:
    pass


def make_agent(info_map):
    account_config = {
        "account_id": 1,
        "direction": "BUY",  # doesn't matter for lot sizing tests
        "login": 0,
        "password": "",
        "server": "",
    }
    return RiskAgent(FakeClient(info_map), FakeBus(), FakeDB(), account_config)


def info(tick_value, tick_size, vmin=0.01, vmax=100.0, vstep=0.01):
    """Build a get_symbol_info-shaped dict using the real MT5 attribute names."""
    return {
        "trade_tick_value": tick_value,
        "trade_tick_size": tick_size,
        "volume_min": vmin,
        "volume_max": vmax,
        "volume_step": vstep,
    }


# --------------------------------------------------------------------------
# Real broker tick values (observed in trading_bot.log)
#   EURUSD  tick_value=1.0  tick_size=0.00001  -> ratio 100,000
#   USDJPY  tick_value=0.62 tick_size=0.001    -> ratio    ~620
#   XAUUSD  tick_value=0.1  tick_size=0.01     -> ratio       10
# --------------------------------------------------------------------------

REAL_INFO = {
    "EURUSD": info(1.0, 0.00001),
    "USDJPY": info(0.62, 0.001),
    "XAUUSD": info(0.1, 0.01),
}

# Empty map -> every get_symbol_info raises -> config.TICK_VALUE_FALLBACK used.
FALLBACK_INFO = {}


# --------------------------------------------------------------------------
# Test matrix: (label, info_map, symbol, sl_distance, expect_ok, expect_lot)
# entry is arbitrary; only abs(entry - sl) matters, so we pass entry and derive
# stop_loss = entry - sl_distance.
# --------------------------------------------------------------------------

ENTRY = {"EURUSD": 1.08500, "USDJPY": 150.000, "XAUUSD": 2400.00}

# Expected lots use SL_USD=50 / (ratio × sl_dist), clamped to [LOT_MIN=0.05, LOT_MAX=0.10].
# Raw lots above LOT_MAX clamp down; raw lots below LOT_MIN clamp up.  ok is always True.
#   EURUSD ratio=100 000 | USDJPY ratio=620 | XAUUSD ratio=10
CASES = [
    # ---- real values, raw_lot in range or above LOT_MAX → clamped to 0.10 ----
    ("EURUSD real / normal",         REAL_INFO,     "EURUSD", 0.0050, True,  0.10),
    ("XAUUSD real / sl=4.63",        REAL_INFO,     "XAUUSD", 4.63,   True,  0.10),
    ("XAUUSD real / sl=8.99",        REAL_INFO,     "XAUUSD", 8.99,   True,  0.10),
    ("USDJPY real / normal",         REAL_INFO,     "USDJPY", 0.30,   True,  0.10),
    # ---- real values, raw_lot below LOT_MIN → clamped up to 0.05 ----
    ("EURUSD real / huge SL",        REAL_INFO,     "EURUSD", 0.0200, True,  0.05),
    ("XAUUSD real / huge SL",        REAL_INFO,     "XAUUSD", 150.0,  True,  0.05),
    ("USDJPY real / huge SL",        REAL_INFO,     "USDJPY", 2.00,   True,  0.05),
    # ---- fallback values match real (proves fallback tick values are equivalent) ----
    ("EURUSD fallback / normal",     FALLBACK_INFO, "EURUSD", 0.0050, True,  0.10),
    ("USDJPY fallback / normal",     FALLBACK_INFO, "USDJPY", 0.30,   True,  0.10),
    # ---- fallback values, raw_lot below LOT_MIN → clamped up to 0.05 ----
    ("EURUSD fallback / huge SL",    FALLBACK_INFO, "EURUSD", 0.0200, True,  0.05),
    ("USDJPY fallback / huge SL",    FALLBACK_INFO, "USDJPY", 2.00,   True,  0.05),
    # ---- XAUUSD fallback ratio 10 matches real path ----
    ("XAUUSD fallback / sl=4.63",    FALLBACK_INFO, "XAUUSD", 4.63,   True,  0.10),
]


def run():
    passed = 0
    failed = 0
    print("\n" + "=" * 92)
    print(f"{'CASE':<28}{'SYMBOL':<8}{'sl_dist':>10}{'  exp':>7}{'  got':>7}{'   lot':>9}  RESULT")
    print("-" * 92)

    for label, info_map, symbol, sl_dist, expect_ok, expect_lot in CASES:
        agent = make_agent(info_map)
        entry = ENTRY[symbol]
        stop_loss = entry - sl_dist
        volume, _sl, _tp, lot_ok, reason = agent._compute_lot_and_levels(
            symbol, entry, stop_loss, "BUY"
        )

        ok_match = (lot_ok == expect_ok)
        lot_match = (abs(volume - expect_lot) < 1e-9)
        case_pass = ok_match and lot_match

        passed += case_pass
        failed += (not case_pass)

        print(
            f"{label:<28}{symbol:<8}{sl_dist:>10.5f}"
            f"{('ACCEPT' if expect_ok else 'REJECT'):>7}"
            f"{('ACCEPT' if lot_ok else 'REJECT'):>7}"
            f"{volume:>9.4f}  {'PASS' if case_pass else 'FAIL'}"
        )
        if not case_pass:
            print(f"    expected ok={expect_ok} lot={expect_lot}; got ok={lot_ok} lot={volume}")
            print(f"    reason: {reason}")

    print("-" * 92)
    print(f"RESULT: {passed} passed, {failed} failed")
    print("=" * 92 + "\n")
    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
