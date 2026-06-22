"""
core/signal_tracker.py — Rolling win-rate tracker for generated signals.

Tracks the last N closed trades and can pause signal generation when
the recent win rate falls below a configured threshold.

State is persisted to a JSON file (default: signal_tracker_state.json) so
the win-rate history survives bot restarts.  Pass ``state_file=None`` to
disable persistence (useful in tests and scripts).
"""

import collections
import json
import logging
import os
import threading
from typing import Optional

import config

logger = logging.getLogger(__name__)

_DEFAULT_STATE_FILE = "signal_tracker_state.json"


class SignalTracker:
    """
    Thread-safe rolling win-rate tracker.

    record_result() is called by TradeMonitor when a position closes.
    should_pause() is checked by AnalysisAgent before emitting signals.
    """

    def __init__(
        self,
        window: int = config.SIGNAL_TRACKER_WINDOW,
        pause_threshold: float = config.SIGNAL_PAUSE_THRESHOLD,
        state_file: Optional[str] = _DEFAULT_STATE_FILE,
    ) -> None:
        self._window = window
        self._pause_threshold = pause_threshold
        self._results: collections.deque = collections.deque(maxlen=window)
        self._lock = threading.Lock()
        self._state_file = state_file
        self._was_paused: bool = False  # tracks last logged pause state

        if state_file:
            self._load_state()

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        if not self._state_file or not os.path.exists(self._state_file):
            return
        try:
            with open(self._state_file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            results = [bool(r) for r in data.get("results", [])]
            with self._lock:
                self._results = collections.deque(results, maxlen=self._window)
            rate = self.win_rate
            logger.info(
                "SignalTracker: loaded state from disk — win_rate=%.0f%% for %d results",
                rate * 100,
                len(self._results),
            )
        except Exception:
            logger.exception(
                "SignalTracker: failed to load state from %s", self._state_file
            )

    def _save_state(self) -> None:
        if not self._state_file:
            return
        with self._lock:
            results = list(self._results)
        try:
            with open(self._state_file, "w", encoding="utf-8") as fh:
                json.dump({"results": results}, fh)
        except Exception:
            logger.exception(
                "SignalTracker: failed to save state to %s", self._state_file
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_result(self, signal_id: object, won: bool) -> None:
        with self._lock:
            self._results.append(won)

        self._save_state()

        # Log only when win rate crosses below the pause threshold
        rate = self.win_rate
        now_paused = (
            len(self._results) >= self._window and rate < self._pause_threshold
        )
        if now_paused and not self._was_paused:
            logger.warning(
                "SignalTracker: win_rate=%.0f%% dropped below %.0f%% threshold "
                "— new signals will be paused",
                rate * 100,
                self._pause_threshold * 100,
            )
        self._was_paused = now_paused

    @property
    def win_rate(self) -> float:
        with self._lock:
            if not self._results:
                return 1.0
            return sum(self._results) / len(self._results)

    @property
    def should_pause(self) -> bool:
        with self._lock:
            if len(self._results) < self._window:
                return False
            rate = sum(self._results) / len(self._results)
        return rate < self._pause_threshold
