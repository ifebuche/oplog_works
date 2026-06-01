from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def send_google_chat_message(message: str, webhook_url: str):
    try:
        import requests
    except ImportError as exc:
        return False, f"requests not installed: {exc}"

    response = requests.post(
        webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"text": message}),
        timeout=30,
    )
    if response.status_code == 200:
        return True, "ok"
    return False, f"Google Chat error {response.status_code}: {response.text}"
