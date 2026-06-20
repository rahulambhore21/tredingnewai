"""
tests/test_db_consumer.py — Unit tests for the db_consumer.py Bug #2 fix.

Bug #2: _last_validation_id was keyed by symbol alone, so account 2's signal
could overwrite account 1's validation ID. When account 2's trade closed, it
looked up the wrong (or missing) validation row.
Fix: key the dict by (symbol, account_id) tuple so each account has its own slot.
"""

import pytest
from unittest.mock import MagicMock
from core.db_consumer import DBConsumer
from core.event_bus import EventBus
from core.events import SignalGeneratedEvent, TradeClosedEvent


def _make_mock_db(signal_id: int = 42, validation_id: int = 7):
    db = MagicMock()
    db.insert_signal.return_value = signal_id
    db.insert_validation_log.return_value = validation_id
    return db


def _make_signal_event(symbol: str = "XAUUSD", account_id: int = 1) -> SignalGeneratedEvent:
    return SignalGeneratedEvent(
        symbol=symbol,
        direction="BUY",
        entry=2000.0,
        stop_loss=1990.0,
        take_profit=2030.0,
        confidence=8.0,
        reasoning="M15 bullish at support",
        zone_type="support",
        zone_center=2000.0,
        account_id=account_id,
    )


def _make_closed_event(
    symbol: str = "XAUUSD",
    account_id: int = 1,
    order_id: int = 100,
    pnl: float = 50.0,
) -> TradeClosedEvent:
    return TradeClosedEvent(
        symbol=symbol,
        direction="BUY",
        volume=0.05,
        entry_price=2000.0,
        order_id=order_id,
        account_id=account_id,
        close_price=2010.0,
        realized_pnl=pnl,
    )


class TestDBConsumerValidationKey:
    """Tests for the (symbol, account_id) composite key in _last_validation_id."""

    def setup_method(self):
        self.db = _make_mock_db()
        self.bus = MagicMock(spec=EventBus)
        self.consumer = DBConsumer(self.db, self.bus)

    def test_validation_id_keyed_by_symbol_and_account_id(self):
        """
        Bug #2 fix: After processing SignalGeneratedEvent, _last_validation_id
        must store the val_id under the (symbol, account_id) tuple key,
        not a bare symbol string.
        """
        event = _make_signal_event(symbol="XAUUSD", account_id=1)
        self.consumer._on_signal(event)

        expected_key = ("XAUUSD", 1)
        assert expected_key in self.consumer._last_validation_id, (
            f"Expected key {expected_key!r} in _last_validation_id. "
            f"Actual keys: {list(self.consumer._last_validation_id.keys())}. "
            "Bug: key was stored as bare symbol, causing cross-account collisions."
        )
        assert self.consumer._last_validation_id[expected_key] == 7

    def test_trade_closed_calls_update_with_correct_val_id(self):
        """
        When TradeClosedEvent fires with matching (symbol, account_id),
        update_validation_log_close() must be called with the val_id
        stored for that account's signal.
        """
        self.consumer._on_signal(_make_signal_event(symbol="XAUUSD", account_id=1))
        self.consumer._on_trade_closed(_make_closed_event(symbol="XAUUSD", account_id=1, pnl=50.0))

        self.db.update_validation_log_close.assert_called_once()
        call_args = self.db.update_validation_log_close.call_args
        # Support both positional and keyword calling conventions
        actual_val_id = (
            call_args.kwargs.get("val_id")
            if call_args.kwargs
            else call_args.args[0]
        )
        assert actual_val_id == 7, (
            f"update_validation_log_close should receive val_id=7, got {actual_val_id}"
        )

    def test_different_account_id_does_not_update_wrong_row(self):
        """
        If TradeClosedEvent.account_id has no matching key in _last_validation_id,
        update_validation_log_close() must NOT be called.
        This verifies isolation between accounts.
        """
        # Account 1 signal stored
        self.consumer._on_signal(_make_signal_event(symbol="XAUUSD", account_id=1))

        # Account 2 close event — no validation ID was recorded for account 2
        self.consumer._on_trade_closed(
            _make_closed_event(symbol="XAUUSD", account_id=2, pnl=-30.0)
        )

        self.db.update_validation_log_close.assert_not_called()
