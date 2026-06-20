"""
tests/test_risk_agent.py — Unit tests for the risk_agent.py / database.py Bug #4 fix.

Bug #4: Database.get_today_realized_pnl() had no account_id parameter, so it summed
P&L from ALL accounts. A large loss on account 2 could incorrectly trigger the
daily-loss gate on account 1.
Fix: add an account_id parameter to get_today_realized_pnl() and filter the SQL
query with WHERE account_id = ?.

These tests use an in-memory SQLite database so the SQL query itself is exercised.
"""

import pytest
from datetime import datetime, timezone
from core.database import Database


class TestGetTodayRealizedPnlPerAccount:
    """Verify that daily P&L is scoped to a single account."""

    def setup_method(self):
        """
        Populate an in-memory DB with two trades on different accounts:
          account_id=1: realized_pnl = +100.0
          account_id=2: realized_pnl = -200.0
        """
        self.db = Database(":memory:")
        today = datetime.now(tz=timezone.utc).isoformat()

        # Account 1: profitable trade
        self.db._execute_write(
            """
            INSERT INTO trades
                (symbol, direction, volume, entry, stop_loss, take_profit,
                 success, dry_run, account_id, created_at, close_time, realized_pnl)
            VALUES ('XAUUSD', 'BUY', 0.05, 2000.0, 1990.0, 2030.0,
                    1, 0, 1, ?, ?, 100.0)
            """,
            (today, today),
        )

        # Account 2: large losing trade
        self.db._execute_write(
            """
            INSERT INTO trades
                (symbol, direction, volume, entry, stop_loss, take_profit,
                 success, dry_run, account_id, created_at, close_time, realized_pnl)
            VALUES ('XAUUSD', 'SELL', 0.05, 2000.0, 2010.0, 1970.0,
                    1, 0, 2, ?, ?, -200.0)
            """,
            (today, today),
        )

    def test_pnl_filtered_to_specific_account(self):
        """
        get_today_realized_pnl(account_id=1) must return only account 1's P&L
        (+100.0), not the combined total of both accounts (-100.0).
        """
        try:
            pnl = self.db.get_today_realized_pnl(account_id=1)
        except TypeError as exc:
            pytest.fail(
                f"BUG #4 NOT FIXED: get_today_realized_pnl() does not accept account_id. "
                f"Error: {exc}. "
                "Fix: add 'account_id: int' parameter and filter SQL with WHERE account_id=?."
            )

        assert pnl == pytest.approx(100.0), (
            f"BUG #4 NOT FIXED: expected PnL=100.0 for account 1, got {pnl}. "
            "Without account_id filter the combined PnL (-100.0) is returned."
        )

    def test_large_loss_on_account2_does_not_bleed_into_account1(self):
        """
        Account 2's -$200 loss must NOT appear in account 1's daily P&L.
        Without the fix, the unfiltered sum (-100.0) would incorrectly trigger
        account 1's daily-loss gate (DAILY_LOSS_LIMIT_USD = $30).
        """
        try:
            pnl_acc1 = self.db.get_today_realized_pnl(account_id=1)
        except TypeError:
            pytest.skip("get_today_realized_pnl() does not accept account_id yet (Bug #4 not fixed)")

        assert pnl_acc1 >= 0, (
            f"BUG #4 NOT FIXED: account 1 P&L = {pnl_acc1}, which is negative. "
            "This means account 2's loss leaked into account 1's P&L check."
        )

    def test_account2_pnl_is_independent(self):
        """
        get_today_realized_pnl(account_id=2) must return only account 2's P&L
        (-200.0) and must not be contaminated by account 1's profit.
        """
        try:
            pnl_acc2 = self.db.get_today_realized_pnl(account_id=2)
        except TypeError:
            pytest.skip("get_today_realized_pnl() does not accept account_id yet (Bug #4 not fixed)")

        assert pnl_acc2 == pytest.approx(-200.0), (
            f"Expected PnL=-200.0 for account 2, got {pnl_acc2}."
        )
