"""
tests/test_signal_tracker.py — Unit tests for the signal_tracker.py Bug #3 fix.

Bug #3: should_pause checked len < window correctly but the threshold comparison
was unreliable / win_rate computation was incorrect under certain conditions.
Fix: ensure the rolling deque, win_rate, and should_pause all behave correctly
across warm-up, pause, and restart scenarios.
"""

import pytest
from core.signal_tracker import SignalTracker

WINDOW = 10
THRESHOLD = 0.40


class TestSignalTracker:
    def test_should_pause_false_when_win_rate_above_threshold(self):
        """
        should_pause must return False when the rolling win rate is above the
        40% pause threshold, even after the window is fully populated.
        """
        tracker = SignalTracker(window=WINDOW, pause_threshold=THRESHOLD, state_file=None)
        # 7 wins + 3 losses = 70% — well above the 40% threshold
        for _ in range(7):
            tracker.record_result("sig", True)
        for _ in range(3):
            tracker.record_result("sig", False)

        assert not tracker.should_pause, (
            f"Expected should_pause=False with 70% win rate, got True. "
            f"win_rate={tracker.win_rate:.2f}"
        )

    def test_should_pause_true_when_win_rate_drops_below_threshold(self):
        """
        should_pause must return True once the window fills and the rolling
        win rate falls below the 40% pause threshold.
        """
        tracker = SignalTracker(window=WINDOW, pause_threshold=THRESHOLD, state_file=None)
        # 3 wins + 7 losses = 30% — below the 40% threshold
        for _ in range(3):
            tracker.record_result("sig", True)
        for _ in range(7):
            tracker.record_result("sig", False)

        assert tracker.should_pause, (
            f"Expected should_pause=True with 30% win rate, got False. "
            f"win_rate={tracker.win_rate:.2f}"
        )

    def test_should_pause_false_while_window_not_full(self):
        """
        should_pause must remain False during warm-up (fewer results than window),
        even if all recorded results are losses.
        """
        tracker = SignalTracker(window=WINDOW, pause_threshold=THRESHOLD, state_file=None)
        for _ in range(WINDOW - 1):  # one short of a full window
            tracker.record_result("sig", False)

        assert not tracker.should_pause, (
            "should_pause must be False while the window is not yet full."
        )

    def test_record_result_tracks_wins_and_losses_correctly(self):
        """
        record_result() must accumulate wins and losses correctly so that
        win_rate reflects the rolling proportion.
        """
        tracker = SignalTracker(window=WINDOW, pause_threshold=THRESHOLD, state_file=None)
        tracker.record_result("s1", True)   # win
        tracker.record_result("s2", False)  # loss
        tracker.record_result("s3", True)   # win

        expected = 2 / 3
        assert tracker.win_rate == pytest.approx(expected, rel=1e-3), (
            f"Expected win_rate≈{expected:.3f}, got {tracker.win_rate:.3f}"
        )

    def test_new_instance_resets_to_default_state(self):
        """
        When state_file=None (persistence disabled), a new SignalTracker instance
        starts with a blank slate: empty deque, win_rate=1.0, should_pause=False.
        This tests the in-memory-only code path (no disk I/O).
        """
        tracker1 = SignalTracker(window=WINDOW, pause_threshold=THRESHOLD, state_file=None)
        for _ in range(WINDOW):
            tracker1.record_result("sig", False)
        assert tracker1.should_pause is True, "Pre-condition: tracker1 should be paused"

        # Simulate restart with a fresh instance (state_file=None → no disk load)
        tracker2 = SignalTracker(window=WINDOW, pause_threshold=THRESHOLD, state_file=None)
        assert tracker2.should_pause is False, (
            "New instance with state_file=None should start with should_pause=False."
        )
        assert tracker2.win_rate == pytest.approx(1.0), (
            "New instance with state_file=None should return win_rate=1.0 "
            "when no results recorded yet."
        )
