from __future__ import annotations

from typing import Any


class SchemaAligner:
    """Align incoming rows to a fixed destination schema."""

    def align(self, table_name: str, table_data: list[dict], schema_columns: list[str]) -> tuple[list[dict], dict]:
        if not table_data:
            return [], {}

        incoming_schema: list[str] = []
        for row in table_data:
            for key in row:
                if key not in incoming_schema:
                    incoming_schema.append(key)

        missing = [col for col in schema_columns if col not in incoming_schema]
        if missing:
            for row in table_data:
                for col in missing:
                    row[col] = None

        new_cols = [col for col in incoming_schema if col not in schema_columns]
        if new_cols:
            for row in table_data:
                for col in new_cols:
                    row.pop(col, None)

        meta: dict[str, Any] = {}
        if missing:
            meta["missing_columns"] = missing
        if new_cols:
            meta["new_columns_seen"] = new_cols
        return table_data, meta
