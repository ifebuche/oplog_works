from __future__ import annotations

import logging
from typing import Any, Optional

from ..core.models import ExtractResult, TableSpec

logger = logging.getLogger(__name__)

DEFAULT_PLATFORMS = ["mobile", "bo", "adv", "sme"]


class DynamoGsiExtractor:
    """Incremental extract from DynamoDB using a GSI and cursor sort key."""

    def __init__(self, dynamodb_resource: Any):
        self._dynamodb = dynamodb_resource

    def extract(
        self,
        spec: TableSpec,
        last_record_time_iso: str,
        quarter_summary: Optional[list[str]] = None,
    ) -> ExtractResult:
        from boto3.dynamodb.conditions import Key

        if not spec.gsi_index:
            return ExtractResult(ok=False, message=f"gsi_index required for {spec.source_name}")

        cursor_key = spec.cursor_key or "updated"
        table = self._dynamodb.Table(spec.source_name)
        all_items: list[dict] = []

        try:
            if spec.platform_partitions is not None or self._uses_platform_index(spec):
                platforms = spec.platform_partitions or DEFAULT_PLATFORMS
                for platform in platforms:
                    query_params = {
                        "IndexName": spec.gsi_index,
                        "KeyConditionExpression": Key("platform").eq(platform)
                        & Key(cursor_key).gt(last_record_time_iso),
                        "ScanIndexForward": False,
                    }
                    all_items.extend(self._paginate_query(table, query_params))
                return ExtractResult(ok=True, items=all_items, quarter_summary=None)

            partitions = quarter_summary or spec.cold_start_partitions
            if not partitions:
                return ExtractResult(ok=False, message="Missing GSI partitions (quarter_summary)")

            seen_partitions: dict[str, int] = {}
            for partition in partitions:
                query_params = {
                    "IndexName": spec.gsi_index,
                    "KeyConditionExpression": Key("pipeline-gsi-pk").eq(partition)
                    & Key(cursor_key).gt(last_record_time_iso),
                    "ScanIndexForward": False,
                }
                batch = self._paginate_query(table, query_params)
                all_items.extend(batch)
                if batch:
                    seen_partitions[partition] = seen_partitions.get(partition, 0) + len(batch)

            summary = list(seen_partitions.keys()) if seen_partitions else quarter_summary
            return ExtractResult(ok=True, items=all_items, quarter_summary=summary)
        except Exception as exc:
            message = f"Error extracting from '{spec.source_name}': {exc.__class__.__name__} - {exc}"
            logger.exception(message)
            return ExtractResult(ok=False, message=message)

    @staticmethod
    def _uses_platform_index(spec: TableSpec) -> bool:
        return spec.gsi_index == "platform-updated-index"

    @staticmethod
    def _paginate_query(table: Any, query_params: dict) -> list[dict]:
        items: list[dict] = []
        while True:
            response = table.query(**query_params)
            items.extend(response.get("Items", []))
            if "LastEvaluatedKey" not in response:
                break
            query_params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        return items
