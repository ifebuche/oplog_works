from __future__ import annotations

import logging
import time
from typing import Any, Optional

from ..notifications.notifier import PipelineNotifier
from ..transforms.registry import TransformRegistry
from .helpers import data_volume, iso_tz_to_epoch, now_now_str
from .models import RunOptions, TableSpec, TimeBudgetFn
from .run_state import RunStateStore
from .schema import SchemaAligner

logger = logging.getLogger(__name__)

MIN_TIME_BUDGET_MS = 60_000


class PipelineOrchestrator:
    def __init__(
        self,
        *,
        run_state: RunStateStore,
        extractors: dict[str, Any],
        loaders: dict[str, Any],
        transform_registry: TransformRegistry,
        schema_aligner: Optional[SchemaAligner] = None,
        notifier: Optional[PipelineNotifier] = None,
        time_budget_ms: Optional[TimeBudgetFn] = None,
        pre_load_hook: Optional[Any] = None,
    ):
        self._run_state = run_state
        self._extractors = extractors
        self._loaders = loaders
        self._transform_registry = transform_registry
        self._schema_aligner = schema_aligner or SchemaAligner()
        self._notifier = notifier
        self._time_budget_ms = time_budget_ms
        self._pre_load_hook = pre_load_hook

    def run_all(self, specs: list[TableSpec], options: Optional[RunOptions] = None) -> dict[str, dict]:
        options = options or RunOptions()
        results: dict[str, dict] = {}
        for spec in specs:
            if options.tables and spec.source_name not in options.tables:
                continue
            if not self._has_time_budget():
                logger.warning("Time budget exhausted; skipping remaining tables")
                break
            record = self.run_table(spec, options)
            if record:
                key = record.get("sub_table_name") or record.get("table_name") or spec.source_name
                results[key] = record
        return results

    def run_table(self, spec: TableSpec, options: Optional[RunOptions] = None) -> Optional[dict]:
        options = options or RunOptions()
        cursor_key = spec.cursor_key or "updated"
        last_runs = self._run_state.get_last_success(spec.source_name)

        if options.backfill_from:
            last_record_time = options.backfill_from
            quarter_summary = options.force_partitions or spec.cold_start_partitions
        elif not last_runs:
            last_record_time = spec.cold_start_cursor
            quarter_summary = spec.cold_start_partitions
        else:
            last_record_time = last_runs.get("latest_record_updated_value")
            quarter_summary = last_runs.get("quarter_summary")

        if not last_record_time:
            logger.error("No cursor for %s; configure cold_start_cursor", spec.source_name)
            return None

        metadata: dict = {
            "table_name": spec.source_name,
            "status": "success",
            "latest_record_updated_value": last_record_time,
            "last_run_time": last_runs.get("last_run_time") if last_runs else None,
            "quarter_summary": quarter_summary,
        }

        extract_start = time.time()
        extracted = self._extract(spec, last_record_time, quarter_summary, options)
        metadata["extraction_duration"] = round(time.time() - extract_start, 2)

        if not extracted.ok:
            metadata["status"] = "fail"
            metadata["extraction_status"] = "fail"
            metadata["extraction_status_reason"] = extracted.message
            self._run_state.log_run(spec.source_name, metadata)
            self._notify(metadata, fail=True)
            return metadata

        metadata["extraction_status"] = "success"
        metadata["record_count"] = len(extracted.items)
        metadata["quarter_summary"] = extracted.quarter_summary

        if len(extracted.items) == 0:
            metadata["extraction_status"] = "no data since last run"
            metadata["created"] = now_now_str()
            self._run_state.log_run(spec.source_name, metadata)
            return metadata

        data_size, data_size_mb = data_volume(extracted.items)
        metadata["data_size_b"] = data_size
        metadata["data_size_mb"] = data_size_mb
        metadata["latest_record_updated_value"] = extracted.items[0][cursor_key]
        if metadata.get("last_run_time"):
            try:
                metadata["last_run_time"] = iso_tz_to_epoch(str(metadata["latest_record_updated_value"]))
            except (ValueError, TypeError):
                metadata["last_run_time"] = now_now_str()
        else:
            metadata["last_run_time"] = now_now_str()

        transform_start = time.time()
        transformed = self._transform_registry.transform(spec.source_name, extracted.items)
        metadata["transform_duration"] = round(time.time() - transform_start, 2)

        if not transformed.ok:
            metadata["status"] = "fail"
            metadata["transform_status"] = "fail"
            metadata["transform_status_reason"] = transformed.message
            metadata["created"] = now_now_str()
            self._run_state.log_run(spec.source_name, metadata)
            self._notify(metadata, fail=True)
            return metadata

        metadata["transform_status"] = "success"

        if self._pre_load_hook:
            hook_ok, hook_msg = self._pre_load_hook()
            if not hook_ok:
                metadata["load_status"] = "fail"
                metadata["load_status_reason"] = hook_msg
                metadata["status"] = "fail"
                metadata["created"] = now_now_str()
                self._run_state.log_run(spec.source_name, metadata)
                self._notify(metadata, fail=True)
                return metadata

        loader = self._loaders.get(spec.destination_kind)
        if not loader:
            metadata["status"] = "fail"
            metadata["load_status_reason"] = f"No loader for {spec.destination_kind}"
            self._run_state.log_run(spec.source_name, metadata)
            return metadata

        last_success_metadata = None
        for target in transformed.load_targets:
            target_name = target.get("table_name")
            target_data = target.get("table") or []
            if not target_name or len(target_data) < 1:
                continue

            schema_columns = (spec.schema_map or {}).get(target_name, spec.schema_columns)
            aligned, align_meta = self._schema_aligner.align(target_name, target_data, schema_columns)
            load_meta = dict(metadata)
            load_meta.update(align_meta)
            load_meta["table_name"] = target_name
            load_meta["sub_table_name"] = target_name
            load_meta["record_count"] = len(aligned)
            ds, ds_mb = data_volume(aligned)
            load_meta["data_size_b"] = ds
            load_meta["data_size_mb"] = ds_mb

            load_start = time.time()
            if spec.destination_kind == "bigquery":
                bq_spec = TableSpec(
                    source_name=target_name,
                    source_kind=spec.source_kind,
                    destination_kind="bigquery",
                    warehouse_table=self._resolve_bq_table(spec, target_name),
                    primary_key=self._resolve_pk(spec, target_name),
                    schema_columns=spec.schema_columns,
                )
                result = loader.load(bq_spec, aligned)
            else:
                rs_spec = TableSpec(
                    source_name=target_name,
                    source_kind=spec.source_kind,
                    destination_kind=spec.destination_kind,
                    warehouse_table=target_name,
                    primary_key=spec.primary_key,
                    schema_columns=spec.schema_columns,
                )
                result = loader.load(rs_spec, aligned)

            load_meta["load_duration"] = round(time.time() - load_start)
            if not result.ok:
                load_meta["status"] = "fail"
                load_meta["load_status"] = "fail"
                load_meta["load_status_reason"] = result.message
                load_meta["created"] = now_now_str()
                self._run_state.log_run(spec.source_name, load_meta)
                self._notify(load_meta, fail=True)
                return load_meta

            load_meta["load_status"] = "success"
            load_meta["total_duration"] = round(
                (load_meta.get("extraction_duration") or 0)
                + (load_meta.get("transform_duration") or 0)
                + load_meta["load_duration"]
            )
            load_meta["created"] = now_now_str()
            self._run_state.log_run(spec.source_name, load_meta)
            self._notify(load_meta, fail=False)
            last_success_metadata = load_meta

        return last_success_metadata

    def _extract(self, spec: TableSpec, last_record_time: str, quarter_summary, options: RunOptions):
        extractor = self._extractors.get(spec.source_kind)
        if not extractor:
            from ..core.models import ExtractResult

            return ExtractResult(ok=False, message=f"No extractor for {spec.source_kind}")
        if spec.source_kind == "dynamo_gsi":
            return extractor.extract(spec, last_record_time, quarter_summary)
        from bson import Timestamp

        if options.backfill_from:
            from datetime import datetime

            parsed = datetime.strptime(options.backfill_from[:10].replace("-", "/"), "%Y/%m/%d")
            filter_time = Timestamp(parsed, inc=0)
        elif hasattr(self._run_state, "get_oplog_timestamp"):
            filter_time = self._run_state.get_oplog_timestamp(spec.source_name)  # type: ignore
        else:
            filter_time = Timestamp(int(time.time()) - 3600, 0)
        return extractor.extract(spec, filter_time)

    def _notify(self, metadata: dict, *, fail: bool):
        if self._notifier:
            self._notifier.notify_run(metadata, fail=fail)

    def _has_time_budget(self) -> bool:
        if not self._time_budget_ms:
            return True
        remaining = self._time_budget_ms()
        if remaining is None:
            return True
        return remaining > MIN_TIME_BUDGET_MS

    def _resolve_bq_table(self, spec: TableSpec, target_name: str) -> str:
        if spec.warehouse_table_map:
            return spec.warehouse_table_map.get(target_name, spec.warehouse_table)
        return spec.warehouse_table

    def _resolve_pk(self, spec: TableSpec, target_name: str) -> str:
        if spec.primary_key_map:
            return spec.primary_key_map.get(target_name, spec.primary_key)
        return spec.primary_key
