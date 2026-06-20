"""
tests/test_executor.py — Unit tests for the executor.py Bug #1 fix.

Bug #1: When place_market_order succeeds but modify_position fails, the
TradeExecutedEvent was published with success=False (tied to sl_tp_ok).
Fix: success should be True whenever the order was placed (position_id obtained),
regardless of whether the SL/TP attachment succeeded.
"""

import pytest
from unittest.mock import MagicMock, patch
import config
from agents.executor import Executor
from core.event_bus import EventBus
from core.events import RiskEvaluatedEvent, TradeExecutedEvent


def _make_risk_event(account_id: int = 1, direction: str = "BUY") -> RiskEvaluatedEvent:
    return RiskEvaluatedEvent(
        symbol="XAUUSD",
        direction=direction,
        entry=2000.0,
        stop_loss=1990.0,
        take_profit=2030.0,
        confidence=8.0,
        approved=True,
        reason="All checks passed",
        volume=0.05,
        account_id=account_id,
    )


def _make_executor(account_id: int = 1, direction: str = "BUY"):
    client = MagicMock()
    bus = MagicMock(spec=EventBus)
    cfg = {"account_id": account_id, "direction": direction}
    executor = Executor(client, bus, cfg)
    return executor, client, bus


class TestExecutorLivePath:
    """Tests for _execute_live — the two-step order placement path."""

    def test_place_succeeds_modify_fails_success_true_sl_tp_false(self):
        """
        Bug #1 fix: When place_market_order succeeds but modify_position fails,
        TradeExecutedEvent must have success=True and sl_tp_modified=False.
        The position WAS opened; only the SL/TP attachment failed.
        """
        executor, client, bus = _make_executor()

        mock_data = MagicMock()
        mock_data.order = 12345
        mock_data.price = 2001.0
        client.order.place_market_order.return_value = {
            "error": False,
            "message": "Order placed",
            "data": mock_data,
        }
        client.order.modify_position.return_value = {
            "error": True,
            "message": "modify_position failed: invalid stops",
        }

        event = _make_risk_event()
        with patch.object(config, "EXECUTION_LIVE", True):
            executor._execute_live(event)

        bus.publish.assert_called_once()
        published: TradeExecutedEvent = bus.publish.call_args[0][0]
        assert isinstance(published, TradeExecutedEvent)
        assert published.success is True, (
            "BUG #1 NOT FIXED: expected success=True when order placed but SL/TP modify failed. "
            f"Got success={published.success}. "
            "Fix: decouple 'success' from sl_tp_ok — set success=True whenever position_id is obtained."
        )
        assert published.sl_tp_modified is False, (
            f"Expected sl_tp_modified=False, got {published.sl_tp_modified}"
        )
        assert published.order_id == 12345

    def test_place_and_modify_both_succeed(self):
        """
        When both place_market_order and modify_position succeed,
        TradeExecutedEvent must have success=True and sl_tp_modified=True.
        """
        executor, client, bus = _make_executor()

        mock_data = MagicMock()
        mock_data.order = 99999
        mock_data.price = 2001.5
        client.order.place_market_order.return_value = {
            "error": False,
            "data": mock_data,
        }
        client.order.modify_position.return_value = {"error": False}

        event = _make_risk_event()
        with patch.object(config, "EXECUTION_LIVE", True):
            executor._execute_live(event)

        bus.publish.assert_called_once()
        published: TradeExecutedEvent = bus.publish.call_args[0][0]
        assert published.success is True
        assert published.sl_tp_modified is True
        assert published.order_id == 99999
        assert published.fill_price == pytest.approx(2001.5)

    def test_place_order_returns_error_success_false_no_order_id(self):
        """
        When place_market_order returns an error, TradeExecutedEvent must have
        success=False and order_id=None — no position was opened.
        """
        executor, client, bus = _make_executor()

        client.order.place_market_order.return_value = {
            "error": True,
            "message": "Insufficient margin",
        }

        event = _make_risk_event()
        with patch.object(config, "EXECUTION_LIVE", True):
            executor._execute_live(event)

        bus.publish.assert_called_once()
        published: TradeExecutedEvent = bus.publish.call_args[0][0]
        assert published.success is False
        assert published.order_id is None
