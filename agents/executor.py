"""
agents/executor.py — Order execution agent.

Subscribes to RiskEvaluatedEvent.
Only acts when approved=True.

Live execution (EXECUTION_LIVE=True):
    1. Call client.order.place_market_order(type=direction, symbol=..., volume=...)
    2. Verify response.error == False; extract data.order (position id).
    3. Call client.order.modify_position(pos_id, stop_loss=sl, take_profit=tp).
    4. Publish TradeExecutedEvent with all outcome fields.

Dry-run (EXECUTION_LIVE=False):
    Log the would-be order and publish TradeExecutedEvent(dry_run=True).

All errors are caught and logged; a failure never crashes the main loop.
"""

import logging
import time
from typing import Optional

_SL_TP_RETRIES = 3
_SL_TP_RETRY_DELAY_SEC = 1.0

from metatrader_client import MT5Client

import config
from core.event_bus import EventBus
from core.events import RiskEvaluatedEvent, TradeExecutedEvent

logger = logging.getLogger(__name__)


class Executor:
    """
    Order execution agent — the last stage of the pipeline.

    Injected dependencies:
        client: Shared MT5Client for placing orders (write).
        bus:    Shared EventBus for subscribing and publishing.
    """

    def __init__(self, client: MT5Client, bus: EventBus) -> None:
        """
        Initialise and register subscription.

        Args:
            client: Connected MT5Client.
            bus:    Shared EventBus.
        """
        self._client = client
        self._bus    = bus

        self._bus.subscribe(RiskEvaluatedEvent, self._on_risk_evaluated)
        logger.info(
            "Executor initialised — EXECUTION_LIVE=%s", config.EXECUTION_LIVE
        )

    # ------------------------------------------------------------------
    # Event handler
    # ------------------------------------------------------------------

    def _on_risk_evaluated(self, event: RiskEvaluatedEvent) -> None:
        """
        Handle a RiskEvaluatedEvent.
        Silently ignores rejected signals; executes approved ones.

        Args:
            event: Risk verdict from risk_agent.
        """
        if not event.approved:
            logger.debug(
                "Executor: signal rejected for %s — %s", event.symbol, event.reason
            )
            return

        logger.info(
            "Executor received approved signal: %s %s vol=%.2f entry=%.5f sl=%.5f tp=%.5f",
            event.symbol, event.direction, event.volume,
            event.entry, event.stop_loss, event.take_profit,
        )
        try:
            if config.EXECUTION_LIVE:
                self._execute_live(event)
            else:
                self._execute_dry_run(event)
        except Exception:
            logger.exception(
                "Executor raised during order processing for %s", event.symbol
            )
            self._publish_failure(event, "Unhandled executor exception")

    # ------------------------------------------------------------------
    # Dry-run path
    # ------------------------------------------------------------------

    def _execute_dry_run(self, event: RiskEvaluatedEvent) -> None:
        """
        Log the would-be order and publish a dry-run TradeExecutedEvent.

        Args:
            event: Approved risk event.
        """
        symbol = config.resolve_symbol(event.symbol)
        logger.info(
            "DRY RUN — would place %s %s vol=%.2f sl=%.5f tp=%.5f on %s",
            event.direction, symbol, event.volume,
            event.stop_loss, event.take_profit, symbol,
        )
        self._bus.publish(
            TradeExecutedEvent(
                symbol=event.symbol,
                direction=event.direction,
                volume=event.volume,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                success=True,
                order_id=None,
                fill_price=event.entry,    # nominal fill = signal entry
                sl_tp_modified=True,       # treated as applied in dry-run
                error_message=None,
                dry_run=True,
            )
        )

    # ------------------------------------------------------------------
    # Live execution path
    # ------------------------------------------------------------------

    def _execute_live(self, event: RiskEvaluatedEvent) -> None:
        """
        Place a live market order and attach SL/TP via modify_position.

        Step 1: place_market_order → get position_id from data.order
        Step 2: modify_position(position_id, stop_loss, take_profit)
        Publish TradeExecutedEvent with full outcome regardless of result.

        Args:
            event: Approved risk event.
        """
        symbol = config.resolve_symbol(event.symbol)

        # ----------------------------------------------------------
        # Step 1: Place market order
        # ----------------------------------------------------------
        logger.info(
            "Executor: placing %s %s vol=%.2f on %s",
            event.direction, symbol, event.volume, symbol,
        )
        order_result = self._client.order.place_market_order(
            type=event.direction,
            symbol=symbol,
            volume=event.volume,
        )

        if order_result is None:
            logger.error("Executor: place_market_order returned None for %s", symbol)
            self._publish_failure(event, "place_market_order returned None")
            return

        if order_result.get("error"):
            msg = order_result.get("message", "Unknown error")
            logger.error("Executor: order placement failed for %s — %s", symbol, msg)
            self._publish_failure(event, f"Order placement error: {msg}")
            return

        # Extract position id and fill price from the response data object
        data = order_result.get("data")
        if data is None:
            logger.error("Executor: order response data is None for %s", symbol)
            self._publish_failure(event, "Order data is None after placement")
            return

        position_id: Optional[int] = None
        fill_price:  Optional[float] = None

        # The data is a trade result namedtuple/object with .order and .price
        try:
            position_id = int(data.order)
            fill_price  = float(data.price)
        except (AttributeError, TypeError, ValueError) as exc:
            logger.error(
                "Executor: could not extract position_id/fill_price from data — %s", exc
            )
            # Publish partial success — order placed but cannot confirm details
            self._bus.publish(
                TradeExecutedEvent(
                    symbol=event.symbol,
                    direction=event.direction,
                    volume=event.volume,
                    entry=event.entry,
                    stop_loss=event.stop_loss,
                    take_profit=event.take_profit,
                    success=True,
                    order_id=None,
                    fill_price=None,
                    sl_tp_modified=False,
                    error_message=f"Could not parse position id: {exc}",
                    dry_run=False,
                )
            )
            return

        # A real fill always carries a non-zero order ticket and fill price.
        # order=0/price=0.0 means the broker rejected the trade (e.g. market
        # closed, trading disabled) even though the client library reported
        # success — it only checks mt5.last_error(), not response.retcode.
        # Treating this as a live position would chase SL/TP on a ticket that
        # doesn't exist and trigger a pointless "emergency close".
        if position_id == 0 or fill_price == 0.0:
            retcode = getattr(data, "retcode", None)
            broker_comment = getattr(data, "comment", "")
            logger.error(
                "Executor: broker rejected the order for %s — retcode=%s comment=%r "
                "(no position opened; likely market closed or trading disabled)",
                symbol, retcode, broker_comment,
            )
            self._bus.publish(
                TradeExecutedEvent(
                    symbol=event.symbol,
                    direction=event.direction,
                    volume=event.volume,
                    entry=event.entry,
                    stop_loss=event.stop_loss,
                    take_profit=event.take_profit,
                    success=False,
                    order_id=None,
                    fill_price=None,
                    sl_tp_modified=False,
                    error_message=f"Order rejected by broker (retcode={retcode}, {broker_comment})",
                    dry_run=False,
                )
            )
            return

        logger.info(
            "Executor: order placed — position_id=%d fill=%.5f",
            position_id, fill_price,
        )

        # ----------------------------------------------------------
        # Step 2: Attach SL/TP via modify_position (with retries)
        # A live position without SL/TP is a capital risk, so we retry
        # up to _SL_TP_RETRIES times before triggering an emergency close.
        # ----------------------------------------------------------
        sl_tp_ok = False
        sl_tp_error: Optional[str] = None

        for attempt in range(1, _SL_TP_RETRIES + 1):
            try:
                modify_result = self._client.order.modify_position(
                    position_id,
                    stop_loss=event.stop_loss,
                    take_profit=event.take_profit,
                )
                if modify_result is None:
                    sl_tp_error = "modify_position returned None"
                    logger.warning(
                        "Executor: SL/TP attempt %d/%d returned None for position %d",
                        attempt, _SL_TP_RETRIES, position_id,
                    )
                elif modify_result.get("error"):
                    sl_tp_error = modify_result.get("message", "modify_position error")
                    logger.warning(
                        "Executor: SL/TP attempt %d/%d failed for position %d — %s",
                        attempt, _SL_TP_RETRIES, position_id, sl_tp_error,
                    )
                else:
                    sl_tp_ok = True
                    logger.info(
                        "Executor: SL/TP set on attempt %d — position %d sl=%.5f tp=%.5f",
                        attempt, position_id, event.stop_loss, event.take_profit,
                    )
                    break
            except Exception as exc:
                sl_tp_error = str(exc)
                logger.warning(
                    "Executor: SL/TP attempt %d/%d raised for position %d — %s",
                    attempt, _SL_TP_RETRIES, position_id, exc,
                )

            if not sl_tp_ok and attempt < _SL_TP_RETRIES:
                time.sleep(_SL_TP_RETRY_DELAY_SEC)

        if not sl_tp_ok:
            logger.error(
                "Executor: all %d SL/TP attempts failed for position %d — initiating emergency close",
                _SL_TP_RETRIES, position_id,
            )
            self._emergency_close(position_id, symbol, event.direction, event.volume)

        # ----------------------------------------------------------
        # Publish outcome
        # ----------------------------------------------------------
        self._bus.publish(
            TradeExecutedEvent(
                symbol=event.symbol,
                direction=event.direction,
                volume=event.volume,
                entry=event.entry,
                stop_loss=event.stop_loss,
                take_profit=event.take_profit,
                success=sl_tp_ok,
                order_id=position_id,
                fill_price=fill_price,
                sl_tp_modified=sl_tp_ok,
                error_message=sl_tp_error,
                dry_run=False,
            )
        )

    # ------------------------------------------------------------------
    # Emergency close — last resort when SL/TP cannot be attached
    # ------------------------------------------------------------------

    def _emergency_close(
        self,
        position_id: int,
        symbol: str,
        direction: str,
        volume: float,
    ) -> None:
        """
        Place a market order in the opposite direction to close an unprotected
        position.  Logs CRITICAL if the close itself fails so the operator
        can act manually.
        """
        close_direction = "SELL" if direction == "BUY" else "BUY"
        logger.critical(
            "Executor: EMERGENCY CLOSE — position %d (%s %s vol=%.2f) "
            "has no SL/TP — placing %s to close",
            position_id, symbol, direction, volume, close_direction,
        )
        try:
            result = self._client.order.place_market_order(
                type=close_direction,
                symbol=symbol,
                volume=volume,
            )
            if result and not result.get("error"):
                logger.warning(
                    "Executor: emergency close succeeded for position %d", position_id
                )
            else:
                msg = result.get("message", "unknown") if result else "no response"
                logger.critical(
                    "Executor: emergency close FAILED for position %d (%s) — "
                    "MANUAL ACTION REQUIRED: open position has no SL/TP",
                    position_id, msg,
                )
        except Exception:
            logger.critical(
                "Executor: emergency close raised for position %d — MANUAL ACTION REQUIRED",
                position_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Helper: failure event
    # ------------------------------------------------------------------

    def _publish_failure(self, event: RiskEvaluatedEvent, reason: str) -> None:
        """
        Publish a failed TradeExecutedEvent for audit logging purposes.

        Args:
            event:  The risk event that triggered the attempt.
            reason: Description of what went wrong.
        """
        try:
            self._bus.publish(
                TradeExecutedEvent(
                    symbol=event.symbol,
                    direction=event.direction,
                    volume=event.volume,
                    entry=event.entry,
                    stop_loss=event.stop_loss,
                    take_profit=event.take_profit,
                    success=False,
                    order_id=None,
                    fill_price=None,
                    sl_tp_modified=False,
                    error_message=reason,
                    dry_run=not config.EXECUTION_LIVE,
                )
            )
        except Exception:
            logger.exception("Executor: failed to publish failure event for %s", event.symbol)
