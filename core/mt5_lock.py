"""
core/mt5_lock.py — Thread-safe proxy for MT5Client.

The MetaTrader 5 Python API is not thread-safe. Multiple agents (SRMapper,
PriceWatcher, AnalysisAgent, TradeMonitor, RiskAgent) all call the same
MT5Client from different threads. This proxy serialises every call under one
RLock so the underlying MT5 session is never accessed concurrently.

Usage (worker.py):
    from metatrader_client import MT5Client
    from core.mt5_lock import make_thread_safe

    _raw = MT5Client(cfg)
    client = make_thread_safe(_raw)    # wrap once; pass everywhere
    client.connect()                   # still works — goes through proxy
"""

import threading


class _LockedProxy:
    """
    Recursive attribute proxy that wraps every callable with a lock.

    - Callable attributes (methods) are wrapped so the lock is acquired
      before the call and released after.
    - Non-callable attributes (sub-client objects like client.market) are
      wrapped in a new _LockedProxy so their own methods are also locked.
    - AttributeError from the target propagates naturally so hasattr() works.
    """

    def __init__(self, target: object, lock: threading.RLock) -> None:
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_lock", lock)

    def __getattr__(self, name: str):
        target = object.__getattribute__(self, "_target")
        lock   = object.__getattribute__(self, "_lock")
        attr   = getattr(target, name)          # raises AttributeError if missing
        if callable(attr):
            def _locked_call(*args, **kwargs):
                with lock:
                    return attr(*args, **kwargs)
            return _locked_call
        return _LockedProxy(attr, lock)


def make_thread_safe(client) -> _LockedProxy:
    """
    Wrap *client* in a _LockedProxy backed by a single RLock.

    All subsequent calls through the returned proxy are serialised.
    RLock (re-entrant) is used so the same thread can acquire the lock
    multiple times without deadlocking (e.g., AnalysisAgent → RiskAgent →
    Executor all running on the AnalysisAgent processing thread).
    """
    lock = threading.RLock()
    return _LockedProxy(client, lock)
