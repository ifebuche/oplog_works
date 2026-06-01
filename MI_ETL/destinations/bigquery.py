from __future__ import annotations

import logging
import uuid
from typing import Any

from ..core.models import LoadResult, TableSpec

logger = logging.getLogger(__name__)


class BigQueryLoader:
    def __init__(self, client: Any, batch_size: int = 5000):
        self._client = client
        self._batch_size = batch_size

    def load(self, spec: TableSpec, data: list[dict]) -> LoadResult:
        if not data:
            return LoadResult(ok=False, message="No data provided.", table_name=spec.warehouse_table)

        target_table_id = spec.warehouse_table
        pk = spec.primary_key
        temp_table_id = f"{target_table_id}_temp_{uuid.uuid4().hex[:8]}"

        try:
            self._client.query(
                f"""
                CREATE TABLE `{temp_table_id}`
                AS SELECT * FROM `{target_table_id}` WHERE FALSE
                """
            ).result()

            total_batches = (len(data) + self._batch_size - 1) // self._batch_size
            for i in range(0, len(data), self._batch_size):
                batch = data[i : i + self._batch_size]
                errors = self._client.insert_rows_json(temp_table_id, batch)
                if errors:
                    return LoadResult(
                        ok=False,
                        message=f"Failed batch {(i // self._batch_size) + 1}/{total_batches}: {errors}",
                        table_name=spec.warehouse_table,
                    )

            cols = list(data[0].keys())
            update_clause = ", ".join([f"T.`{col}` = S.`{col}`" for col in cols])
            insert_columns = ", ".join([f"`{col}`" for col in cols])
            insert_values = ", ".join([f"S.`{col}`" for col in cols])

            merge_query = f"""
                MERGE `{target_table_id}` T
                USING `{temp_table_id}` S
                ON T.`{pk}` = S.`{pk}`
                WHEN MATCHED THEN UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
            self._client.query(merge_query).result()
            self._client.delete_table(temp_table_id, not_found_ok=True)
            return LoadResult(ok=True, message="Upsert completed successfully.", table_name=spec.warehouse_table)
        except Exception as exc:
            try:
                self._client.delete_table(temp_table_id, not_found_ok=True)
            except Exception:
                pass
            return LoadResult(ok=False, message=str(exc), table_name=spec.warehouse_table)
