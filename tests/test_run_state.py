import unittest
from unittest.mock import MagicMock

from MI_ETL.core.run_state import DynamoRunStateStore


class TestDynamoRunStateStore(unittest.TestCase):
    def test_get_last_success_picks_max_cursor(self):
        table = MagicMock()
        table.query.return_value = {
            "Items": [
                {"table_name": "t", "status": "success", "latest_record_updated_value": "2026-01-01"},
                {"table_name": "t", "status": "success", "latest_record_updated_value": "2026-06-01"},
            ]
        }
        resource = MagicMock()
        resource.Table.return_value = table
        store = DynamoRunStateStore(resource, "data-pipe-runs")
        last = store.get_last_success("t")
        self.assertEqual(last["latest_record_updated_value"], "2026-06-01")


if __name__ == "__main__":
    unittest.main()
