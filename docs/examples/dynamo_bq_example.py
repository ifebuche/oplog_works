"""
Reference wiring for a DynamoDB -> BigQuery pipeline (copy into your service repo).

This file is NOT imported by MI_ETLx. Replace placeholders with production config.
"""

from MI_ETL.core.models import TableSpec
from MI_ETL.core.orchestrator import PipelineOrchestrator
from MI_ETL.core.run_state import DynamoRunStateStore
from MI_ETL.core.schema import SchemaAligner
from MI_ETL.destinations.bigquery import BigQueryLoader
from MI_ETL.notifications import NotificationConfig, PipelineNotifier
from MI_ETL.sources.dynamo_gsi import DynamoGsiExtractor
from MI_ETL.transforms.registry import TransformRegistry


def build_specs() -> list[TableSpec]:
    return [
        TableSpec(
            source_name="my-source-table",
            source_kind="dynamo_gsi",
            destination_kind="bigquery",
            warehouse_table="project.dataset.table",
            primary_key="id",
            schema_columns=["id", "updated"],
            gsi_index="pipeline-gsi-pk-updated-index",
            cursor_key="updated",
            cold_start_cursor="2025-11-01T00:00:00+00:00",
            cold_start_partitions=["2025q4", "2026q1"],
            warehouse_table_map={"my-source-table": "project.dataset.table"},
            primary_key_map={"my-source-table": "id"},
        ),
    ]


def build_orchestrator(dynamodb_resource, bq_client, transform_registry: TransformRegistry, notifier=None):
    return PipelineOrchestrator(
        run_state=DynamoRunStateStore(dynamodb_resource, "data-pipe-runs"),
        extractors={"dynamo_gsi": DynamoGsiExtractor(dynamodb_resource)},
        loaders={"bigquery": BigQueryLoader(bq_client)},
        transform_registry=transform_registry,
        schema_aligner=SchemaAligner(),
        notifier=notifier,
    )
