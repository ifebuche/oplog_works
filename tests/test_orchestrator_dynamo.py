import unittest
from unittest.mock import MagicMock, patch

from MI_ETL.core.models import ExtractResult, TableSpec
from MI_ETL.core.orchestrator import PipelineOrchestrator
from MI_ETL.core.run_state import DynamoRunStateStore
from MI_ETL.core.schema import SchemaAligner
from MI_ETL.sources.dynamo_gsi import DynamoGsiExtractor
from MI_ETL.transforms.registry import TransformRegistry


def _identity_transform(data):
    return True, [{"table_name": "test-table", "table": data}]


class TestOrchestratorDynamo(unittest.TestCase):
    def setUp(self):
        self.run_state = MagicMock(spec=DynamoRunStateStore)
        self.run_state.get_last_success.return_value = {
            "latest_record_updated_value": "2026-05-04T00:00:00+00:00",
            "quarter_summary": ["2026q2"],
        }
        self.extractor = MagicMock(spec=DynamoGsiExtractor)
        self.extractor.extract.return_value = ExtractResult(
            ok=True,
            items=[{"event_id": "e1", "updated": "2026-05-05T00:00:00+00:00"}],
            quarter_summary=["2026q2"],
        )
        registry = TransformRegistry()
        registry.register("test-table", _identity_transform)
        self.loader = MagicMock()
        self.loader.load.return_value = MagicMock(ok=True, message="ok", table_name="proj.ds.t")

        self.spec = TableSpec(
            source_name="test-table",
            source_kind="dynamo_gsi",
            destination_kind="bigquery",
            warehouse_table="proj.ds.t",
            primary_key="event_id",
            schema_columns=["event_id", "updated"],
            gsi_index="test-index",
            cursor_key="updated",
            cold_start_cursor="2025-11-01T00:00:00+00:00",
            cold_start_partitions=["2026q2"],
        )
        self.orchestrator = PipelineOrchestrator(
            run_state=self.run_state,
            extractors={"dynamo_gsi": self.extractor},
            loaders={"bigquery": self.loader},
            transform_registry=registry,
            schema_aligner=SchemaAligner(),
        )

    def test_success_path(self):
        result = self.orchestrator.run_table(self.spec)
        self.assertEqual(result["load_status"], "success")
        self.loader.load.assert_called_once()
        self.run_state.log_run.assert_called()

    def test_empty_extract_skips_load(self):
        self.extractor.extract.return_value = ExtractResult(ok=True, items=[], quarter_summary=["2026q2"])
        result = self.orchestrator.run_table(self.spec)
        self.assertEqual(result["extraction_status"], "no data since last run")
        self.loader.load.assert_not_called()

    def test_transform_failure(self):
        registry = TransformRegistry()
        self.orchestrator = PipelineOrchestrator(
            run_state=self.run_state,
            extractors={"dynamo_gsi": self.extractor},
            loaders={"bigquery": self.loader},
            transform_registry=registry,
            schema_aligner=SchemaAligner(),
        )
        result = self.orchestrator.run_table(self.spec)
        self.assertEqual(result["transform_status"], "fail")


class TestSchemaAligner(unittest.TestCase):
    def test_fills_missing_and_drops_extra(self):
        aligner = SchemaAligner()
        rows = [{"event_id": "1", "extra": "x"}]
        aligned, meta = aligner.align("t", rows, ["event_id", "updated"])
        self.assertIsNone(aligned[0]["updated"])
        self.assertNotIn("extra", aligned[0])
        self.assertIn("new_columns_seen", meta)


if __name__ == "__main__":
    unittest.main()
