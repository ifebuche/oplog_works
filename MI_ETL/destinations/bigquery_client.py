from __future__ import annotations

import base64
import json
from typing import Any, Optional, Tuple


def create_bigquery_client(creds: dict, project_id: Optional[str] = None) -> Tuple[bool, Any]:
    """Build a BigQuery client from a service-account-base64 secret payload."""
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError as exc:
        return False, f"google-cloud-bigquery not installed: {exc}"

    try:
        raw = creds["service-account-base64"]
        decoded = base64.b64decode(raw).decode("utf-8")
        credentials = service_account.Credentials.from_service_account_info(json.loads(decoded))
        if project_id:
            return True, bigquery.Client(credentials=credentials, project=project_id)
        return True, bigquery.Client(credentials=credentials)
    except Exception as exc:
        return False, str(exc)
