"""
core/mt5_connection.py — MT5 connection wrapper with health-check and reconnect.
"""

import logging
import time

logger = logging.getLogger(__name__)


class MT5Connection:
    """
    Wraps an MT5Client instance, adding `is_connected()` and `reconnect()`.

    Used by the watchdog in main.py to verify and restore MT5 sessions instead
    of calling client.connect() / account.get_account_info() inline.
    """

    def __init__(self, client, account_id: int) -> None:
        self._client = client
        self._account_id = account_id
        self._healthy: bool = True

    @property
    def client(self):
        return self._client

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    def is_connected(self) -> bool:
        """Return True if the MT5 session is active (account_info() succeeds)."""
        try:
            info = self._client.account.get_account_info()
            return info is not None
        except Exception:
            return False

    def reconnect(self, max_attempts: int = 3, delay_seconds: float = 5.0) -> bool:
        """
        Re-initialize the MT5 connection up to *max_attempts* times.

        Logs each attempt at INFO level.  Returns True on the first success.
        If all attempts fail: logs CRITICAL and sets ``self._healthy = False``.
        """
        for attempt in range(1, max_attempts + 1):
            logger.info(
                "Reconnect attempt %d/%d for account %d",
                attempt, max_attempts, self._account_id,
            )
            try:
                if self._client.connect():
                    logger.info(
                        "MT5 reconnected on attempt %d/%d for account %d",
                        attempt, max_attempts, self._account_id,
                    )
                    self._healthy = True
                    return True
            except Exception as exc:
                logger.warning(
                    "Reconnect attempt %d/%d failed for account %d: %s",
                    attempt, max_attempts, self._account_id, exc,
                )
            if attempt < max_attempts:
                time.sleep(delay_seconds)

        logger.critical(
            "All %d reconnect attempts failed for account %d — marking connection unhealthy",
            max_attempts, self._account_id,
        )
        self._healthy = False
        return False
