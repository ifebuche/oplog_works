from __future__ import annotations

import datetime
import logging
import re
from collections import defaultdict
from typing import Any, Optional

import pandas as pd
import pymongo
from bson import ObjectId, Timestamp

from ..core.models import ExtractResult, TableSpec
from ..systems.util import validate_date_format

logger = logging.getLogger(__name__)


class MongoOplogExtractor:
    """Extract changed documents via MongoDB oplog tail (bounded batch)."""

    def __init__(self, mongo_client: Any, db_name: str, extract_collections: Optional[list[str]] = None):
        self._client = mongo_client
        self._db_name = db_name
        self._extract_collections = extract_collections or []
        self._oplog = mongo_client.local.oplog.rs
        self._database = mongo_client[db_name]

    def extract(self, spec: TableSpec, filter_time: Timestamp, max_seconds: int = 3600) -> ExtractResult:
        collections = self._extract_collections or [spec.source_name]
        data_dict_insert: dict = defaultdict(list)
        data_dict_update: dict = defaultdict(list)
        extract_start = datetime.datetime.now()

        if len(collections) == 1 and collections[0] == spec.source_name:
            ns_filter = None
        else:
            ns_filter = "|".join(f"^{self._db_name}.{name}" for name in collections)

        query: dict = {"ts": {"$gt": filter_time}}
        if ns_filter:
            query["ns"] = {"$regex": ns_filter}

        cursor = self._oplog.find(
            query,
            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
            oplog_replay=True,
        )

        while cursor.alive:
            for doc in cursor:
                if doc.get("op") == "u":
                    collection_name = doc["ns"].split(".")[-1]
                    data_dict_update[collection_name].append(doc["o2"]["_id"])
                else:
                    collection_name = doc["ns"].split(".")[-1]
                    if doc.get("o"):
                        data_dict_insert[collection_name].append(doc["o"])

            if (datetime.datetime.now() - extract_start).total_seconds() > max_seconds:
                break
            break

        data_dict_update = {k: list(set(v)) for k, v in data_dict_update.items()}
        for key in list(data_dict_insert.keys()):
            if key in data_dict_update:
                unique_ids = set(data_dict_update[key])
                data_dict_insert[key] = [
                    doc for doc in data_dict_insert[key] if doc.get("_id") not in unique_ids
                ]

        for key, ids in data_dict_update.items():
            for doc in self._database[key].find({"_id": {"$in": ids}}):
                data_dict_insert[key].append(doc)

        items = data_dict_insert.get(spec.source_name, [])
        normalized = self._normalize_docs(items)
        return ExtractResult(ok=True, items=normalized)

    def extract_to_dataframes(
        self,
        db_name: str,
        filter_time: Timestamp,
        backfill: Optional[str] = None,
        extract_all: Optional[list[str]] = None,
    ) -> dict[str, pd.DataFrame]:
        """Legacy-shaped output: dict of collection name to DataFrame."""
        if backfill is not None:
            validate_date_format(backfill)
            parsed = datetime.datetime.strptime(backfill, "%Y/%m/%d")
            filter_time = Timestamp(parsed, inc=0)

        self._db_name = db_name
        self._database = self._client[db_name]
        self._extract_collections = extract_all or []

        spec = TableSpec(
            source_name=self._extract_collections[0] if len(self._extract_collections) == 1 else db_name,
            source_kind="mongo_oplog",
            destination_kind="redshift",
            warehouse_table="",
            primary_key="_id",
            schema_columns=[],
        )

        collections = self._extract_collections or None
        if collections:
            result: dict[str, pd.DataFrame] = {}
            for name in collections:
                spec.source_name = name
                extracted = self.extract(spec, filter_time)
                if extracted.items:
                    result[name] = pd.json_normalize(extracted.items, max_level=0)
                    for col in result[name].columns:
                        if len(result[name]) and isinstance(result[name][col].iloc[0], ObjectId):
                            result[name][col] = [str(v) for v in result[name][col]]
            return {k: v for k, v in result.items() if k not in ("metadata", "$cmd")}
        extracted = self.extract(spec, filter_time)
        if not extracted.items:
            return {}
        df = pd.json_normalize(extracted.items, max_level=0)
        return {spec.source_name: df}

    @staticmethod
    def _normalize_docs(docs: list[dict]) -> list[dict]:
        out = []
        for doc in docs:
            row = dict(doc)
            for key, val in row.items():
                if isinstance(val, ObjectId):
                    row[key] = str(val)
            out.append(row)
        return out
