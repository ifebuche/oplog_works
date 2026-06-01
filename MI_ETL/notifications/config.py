from __future__ import annotations

from dataclasses import dataclass


@dataclass
class NotificationConfig:
    slack_webhook_url: str | None = None
    slack_channel: str | None = None
    slack_title: str = "Data Pipeline Summary"
    google_chat_webhook_url: str | None = None
    notify_on_success: tuple[str, ...] = ("slack", "google_chat")
    notify_on_failure: tuple[str, ...] = ("slack", "google_chat")
