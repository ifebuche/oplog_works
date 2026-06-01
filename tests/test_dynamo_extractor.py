import unittest
from unittest.mock import MagicMock

from MI_ETL.core.models import TableSpec
from MI_ETL.sources.dynamo_gsi import DynamoGsiExtractor


class TestDynamoGsiExtractor(unittest.TestCase):
    def test_missing_partitions_returns_error(self):
        dynamodb = MagicMock()
        table = MagicMock()
        table.query.return_value = {"Items": []}
        dynamodb.Table.return_value = table

        spec = TableSpec(
            source_name="tbl",
            source_kind="dynamo_gsi",
            destination_kind="bigquery",
            warehouse_table="p.d.t",
            primary_key="id",
            schema_columns=["id"],
            gsi_index="idx",
            cursor_key="updated",
        )
        extractor = DynamoGsiExtractor(dynamodb)
        result = extractor.extract(spec, "2026-01-01T00:00:00+00:00", quarter_summary=None)
        self.assertFalse(result.ok)


if __name__ == "__main__":
    unittest.main()
