from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Optional


SourceKind = Literal["dynamo_gsi", "mongo_oplog"]
DestinationKind = Literal["bigquery", "redshift", "s3"]


@dataclass
class TableSpec:
    """User-defined pipeline configuration for one source table."""

    source_name: str
    source_kind: SourceKind
    destination_kind: DestinationKind
    warehouse_table: str
    primary_key: str
    schema_columns: list[str]
    cursor_key: Optional[str] = "updated"
    gsi_index: Optional[str] = None
    cold_start_cursor: Optional[str] = None
    cold_start_partitions: Optional[list[str]] = None
    platform_partitions: Optional[list[str]] = None
    warehouse_table_map: Optional[dict[str, str]] = None
    primary_key_map: Optional[dict[str, str]] = None
    schema_map: Optional[dict[str, list[str]]] = None


@dataclass
class RunOptions:
    tables: Optional[list[str]] = None
    backfill_from: Optional[str] = None
    force_partitions: Optional[list[str]] = None


@dataclass
class ExtractResult:
    ok: bool
    items: list[dict] = field(default_factory=list)
    quarter_summary: Optional[list[str]] = None
    message: Optional[str] = None


@dataclass
class TransformResult:
    ok: bool
    load_targets: list[dict] = field(default_factory=list)
    message: Optional[str] = None


@dataclass
class LoadResult:
    ok: bool
    message: str = ""
    table_name: Optional[str] = None


TimeBudgetFn = Callable[[], Optional[int]]
