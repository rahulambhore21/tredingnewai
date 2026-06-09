"""
core/notifier.py — Telegram notification helper.

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment variables.
If either is missing, send() logs a WARNING and returns silently.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class Notifier:
    def __init__(self) -> None:
        self._token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self._chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    def send(self, message: str) -> None:
        if not self._token or not self._chat_id:
            logger.warning(
                "Telegram notification skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set."
            )
            return
        url = _TELEGRAM_API.format(token=self._token)
        try:
            resp = requests.post(
                url,
                json={"chat_id": self._chat_id, "text": message},
                timeout=10,
            )
            resp.raise_for_status()
        except Exception:
            logger.exception("Failed to send Telegram notification.")
