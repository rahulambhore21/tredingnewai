"""
core/signal_tracker.py — Rolling win-rate tracker for generated signals.

Tracks the last N closed trades and can pause signal generation when
the recent win rate falls below a configured threshold.
"""

import collections
import threading

import config


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
    ) -> None:
        self._window = window
        self._pause_threshold = pause_threshold
        self._results: collections.deque = collections.deque(maxlen=window)
        self._lock = threading.Lock()

    def record_result(self, signal_id: object, won: bool) -> None:
        with self._lock:
            self._results.append(won)

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
