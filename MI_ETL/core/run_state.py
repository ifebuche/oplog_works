from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Any, Optional, Protocol

from .helpers import iso_tz_to_epoch, now_now_str, number_to_decimal

logger = logging.getLogger(__name__)


class RunStateStore(Protocol):
    def get_last_success(self, table_name: str) -> Optional[dict]: ...

    def log_run(self, table_name: str, metadata: dict) -> None: ...


class DynamoRunStateStore:
    """Reads/writes pipeline run metadata in a DynamoDB table (e.g. data-pipe-runs)."""

    def __init__(
        self,
        dynamodb_resource: Any,
        runs_table_name: str,
        status_index_name: str = "table_name-status-index",
    ):
        self._table = dynamodb_resource.Table(runs_table_name)
        self._status_index = status_index_name

    def get_last_success(self, table_name: str) -> Optional[dict]:
        from boto3.dynamodb.conditions import Key

        response = self._table.query(
            IndexName=self._status_index,
            KeyConditionExpression=Key("table_name").eq(table_name) & Key("status").eq("success"),
            ScanIndexForward=False,
        )
        items = response.get("Items", [])
        if not items:
            return None
        return max(items, key=lambda x: x.get("latest_record_updated_value", ""))

    def log_run(self, table_name: str, metadata: dict) -> None:
        payload = dict(metadata)
        payload.setdefault("table_name", table_name)
        payload.setdefault("created", now_now_str())
        _, ready = number_to_decimal(payload)
        self._table.put_item(Item=ready)
        logger.info("Logged pipeline run for %s status=%s", table_name, payload.get("status"))


class MongoRunStateStore:
    """Per-collection run metadata in MongoDB."""

    def __init__(self, mongo_client: Any, db_name: str = "metadata", collection_name: str = "pipeline_runs"):
        self._collection = mongo_client[db_name][collection_name]
        self._mongo_client = mongo_client

    def get_last_success(self, table_name: str) -> Optional[dict]:
        from pymongo import DESCENDING

        doc = self._collection.find_one(
            {"table_name": table_name, "status": "success"},
            sort=[("latest_record_updated_value", DESCENDING)],
        )
        if doc:
            return doc
        legacy_coll = self._mongo_client["metadata"]["metadata"]
        legacy = legacy_coll.find_one({"loader": True}, sort=[("_id", DESCENDING)])
        if legacy and legacy.get("timestamp"):
            from bson import Timestamp

            ts = legacy["timestamp"]
            if isinstance(ts, Timestamp):
                iso_val = datetime.utcfromtimestamp(ts.time).isoformat()
            else:
                iso_val = str(ts)
            return {
                "table_name": table_name,
                "status": "success",
                "latest_record_updated_value": iso_val,
            }
        return None

    def log_run(self, table_name: str, metadata: dict) -> None:
        payload = dict(metadata)
        payload["table_name"] = table_name
        payload.setdefault("date", datetime.now())
        result = self._collection.insert_one(payload)
        if payload.get("status") == "success":
            self._collection.update_many(
                {"table_name": table_name, "status": "success", "_id": {"$ne": result.inserted_id}},
                {"$set": {"loader": False}},
            )
            self._collection.update_one({"_id": result.inserted_id}, {"$set": {"loader": True}})

    def get_oplog_timestamp(self, table_name: str):
        from bson import Timestamp

        last = self.get_last_success(table_name)
        if last and last.get("oplog_timestamp"):
            return last["oplog_timestamp"]
        if last and last.get("latest_record_updated_value"):
            try:
                epoch = iso_tz_to_epoch(str(last["latest_record_updated_value"]))
                return Timestamp(int(epoch), 0)
            except (ValueError, TypeError):
                pass
        return Timestamp(math.ceil(time.time()), 0)

    def record_successful_load(self, table_name: str, oplog_timestamp, latest_iso: Optional[str] = None):
        self.log_run(
            table_name,
            {
                "status": "success",
                "loader": True,
                "oplog_timestamp": oplog_timestamp,
                "latest_record_updated_value": latest_iso or now_now_str(),
            },
        )
