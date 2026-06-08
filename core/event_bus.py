"""
core/event_bus.py — Thread-safe synchronous publish/subscribe EventBus.

Design decisions:
- Handlers are invoked synchronously on the publisher's thread.
  This keeps the design deterministic and avoids hidden queue latency.
  Any handler that needs to do heavy work should hand off to its own thread.
- A threading.Lock protects the subscriber registry during subscribe/publish.
- Each handler invocation is wrapped in try/except so a misbehaving subscriber
  never prevents other subscribers from receiving the event.
- subscribe() accepts a handler for *any* Event subclass (identified by the
  class type object) or the base Event class (receives every event).
"""

import logging
import threading
from typing import Callable, Dict, List, Type

from core.events import Event

logger = logging.getLogger(__name__)


class EventBus:
    """
    Simple in-process publish/subscribe bus.

    Usage:
        bus = EventBus()
        bus.subscribe(ZoneTouchEvent, my_handler)   # specific type
        bus.subscribe(Event, catch_all_handler)      # all events
        bus.publish(ZoneTouchEvent(...))
    """

    def __init__(self) -> None:
        # Map: event class → list of callables
        self._subscribers: Dict[Type[Event], List[Callable[[Event], None]]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # subscribe
    # ------------------------------------------------------------------

    def subscribe(
        self,
        event_type: Type[Event],
        handler: Callable[[Event], None],
    ) -> None:
        """
        Register *handler* to be called whenever an event of *event_type*
        (or a subclass of it) is published.

        Thread-safe: can be called from any thread before or after start.

        Args:
            event_type: The event class (e.g. ZoneTouchEvent).
            handler:    A callable accepting a single Event argument.
        """
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(handler)
            logger.debug(
                "Subscribed %s to %s",
                getattr(handler, "__qualname__", repr(handler)),
                event_type.__name__,
            )

    # ------------------------------------------------------------------
    # publish
    # ------------------------------------------------------------------

    def publish(self, event: Event) -> None:
        """
        Deliver *event* to every registered handler whose subscription type
        matches the event's class (exact match OR a base class of it).

        Each handler is called in the order of registration. If a handler
        raises an exception it is logged and skipped; remaining handlers still
        receive the event.

        Args:
            event: Any instance of Event (or a subclass).
        """
        # Snapshot the subscriber map under the lock so we do not hold the
        # lock during handler execution (handlers may themselves call publish).
        with self._lock:
            snapshot = dict(self._subscribers)

        event_cls = type(event)

        for registered_type, handlers in snapshot.items():
            # Match if the registered type is a base class of (or equal to)
            # the concrete event type — this lets "subscribe(Event, f)" act
            # as a catch-all while "subscribe(ZoneTouchEvent, g)" is specific.
            if not issubclass(event_cls, registered_type):
                continue

            for handler in handlers:
                try:
                    handler(event)
                except Exception:
                    logger.exception(
                        "Handler %s raised an exception processing %s — skipping",
                        getattr(handler, "__qualname__", repr(handler)),
                        event_cls.__name__,
                    )

        logger.debug("Published %s for %s", event_cls.__name__, getattr(event, "symbol", "–"))
