from __future__ import annotations

import logging

from .config import NotificationConfig
from .google_chat import send_google_chat_message
from .slack import send_slack_message
from .summary import create_pipeline_summary

logger = logging.getLogger(__name__)


class PipelineNotifier:
    def __init__(self, config: NotificationConfig):
        self._config = config

    def notify_run(self, run_metadata: dict, *, fail: bool = False) -> dict[str, tuple[bool, str]]:
        message = create_pipeline_summary(run_metadata, fail=fail)
        channels = self._config.notify_on_failure if fail else self._config.notify_on_success
        results: dict[str, tuple[bool, str]] = {}

        if "slack" in channels and self._config.slack_webhook_url:
            ok, msg = send_slack_message(
                message,
                self._config.slack_webhook_url,
                channel=self._config.slack_channel,
                title=self._config.slack_title,
            )
            results["slack"] = (ok, msg)
            if not ok:
                logger.warning("Slack notification failed: %s", msg)

        if "google_chat" in channels and self._config.google_chat_webhook_url:
            ok, msg = send_google_chat_message(message, self._config.google_chat_webhook_url)
            results["google_chat"] = (ok, msg)
            if not ok:
                logger.warning("Google Chat notification failed: %s", msg)

        return results
