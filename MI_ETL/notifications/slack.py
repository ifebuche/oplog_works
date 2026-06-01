from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def send_slack_message(message: str, webhook_url: str, channel: str | None = None, title: str = "Data Pipeline Summary"):
    try:
        import requests
    except ImportError as exc:
        return False, f"requests not installed: {exc}"

    payload: dict = {
        "attachments": [{"color": "#36a64f", "title": title, "text": message}],
    }
    if channel:
        payload["channel"] = channel

    response = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=30)
    if response.status_code != 200:
        return False, f"Slack error {response.status_code}: {response.text}"
    return True, "ok"
