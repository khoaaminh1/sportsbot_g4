"""Send alert messages to Discord (webhook) and/or Telegram (bot). Both are optional."""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(
        self,
        discord_webhook_url: str = "",
        telegram_bot_token: str = "",
        telegram_chat_id: str = "",
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.discord = discord_webhook_url.strip()
        self.tg_token = telegram_bot_token.strip()
        self.tg_chat = telegram_chat_id.strip()
        self.transport = transport

    @property
    def channels(self) -> list[str]:
        out = []
        if self.discord:
            out.append("discord")
        if self.tg_token and self.tg_chat:
            out.append("telegram")
        return out

    def send(self, text: str) -> list[str]:
        """Send to every configured channel; return the channels that accepted it."""
        logger.warning("alert", extra={"text": text})
        sent = []
        with httpx.Client(timeout=15, transport=self.transport) as client:
            if self.discord:
                try:
                    r = client.post(self.discord, json={"content": text[:1900]})
                    if r.status_code < 300:
                        sent.append("discord")
                    else:
                        logger.error("discord_failed", extra={"status": r.status_code})
                except httpx.HTTPError as exc:
                    logger.error("discord_failed", extra={"error": str(exc)})
            if self.tg_token and self.tg_chat:
                try:
                    r = client.post(
                        f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                        json={"chat_id": self.tg_chat, "text": text[:4000]},
                    )
                    if r.status_code < 300:
                        sent.append("telegram")
                    else:
                        logger.error("telegram_failed", extra={"status": r.status_code})
                except httpx.HTTPError as exc:
                    logger.error("telegram_failed", extra={"error": type(exc).__name__})
        return sent
