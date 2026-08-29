"""Minimal Telegram Bot API client used for the dashboard's "send test
alert" button (and, if wired up later, real signal notifications).

Configured entirely via environment variables so credentials never live
in code:
  TELEGRAM_BOT_TOKEN - bot token from @BotFather
  TELEGRAM_CHAT_ID   - target chat/channel id (or @channelusername)

If either is unset, `enabled` is False and `send_message` returns a
descriptive error dict instead of raising - the API endpoint turns that
into a clear response for the dashboard button rather than a 500.
"""

import logging
import os
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("telegram_bot")

TELEGRAM_API_BASE = "https://api.telegram.org"


class TelegramNotifier:
    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        self.token = token if token is not None else os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id if chat_id is not None else os.getenv("TELEGRAM_CHAT_ID")

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def send_message(self, text: str) -> Dict[str, Any]:
        if not self.enabled:
            return {
                "ok": False,
                "error": "Telegram not configured - set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID",
            }
        url = f"{TELEGRAM_API_BASE}/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json={"chat_id": self.chat_id, "text": text})
            response.raise_for_status()
            return {"ok": True, "result": response.json()}
        except httpx.HTTPError as exc:
            logger.warning("Telegram send failed: %s", exc)
            return {"ok": False, "error": str(exc)}

    @staticmethod
    def format_signal_message(event: Dict[str, Any]) -> str:
        return (
            f"[DEMO/MOCK] {str(event.get('source', '?')).upper()} signal\n"
            f"Symbol: {event.get('symbol', '?')}\n"
            f"Timeframe: {event.get('timeframe', '?')}\n"
            f"Direction: {event.get('direction', '?')}\n"
            f"Time: {event.get('timestamp', '?')}\n"
            "This is synthetic demo data, not trading advice."
        )

    @staticmethod
    def test_message() -> str:
        return (
            "[TEST ALERT] Market Signals dashboard\n"
            "This confirms your Telegram bot/channel is configured "
            "correctly. No real signal was generated."
        )
