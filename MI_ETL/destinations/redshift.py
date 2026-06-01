from __future__ import annotations

import random
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text

from ..Error import OplogWorksError
from ..core.models import LoadResult, TableSpec
from ..systems.util import schema_validation


class RedshiftLoader:
    def __init__(self, engine: Any):
        self._engine = engine

    def load(self, spec: TableSpec, data: list[dict]) -> LoadResult:
        if not data:
            return LoadResult(ok=False, message="No data provided.", table_name=spec.warehouse_table)
        df = pd.DataFrame(data)
        ok, message, _ = self._insert_update_record(df, spec.warehouse_table, spec.primary_key)
        return LoadResult(ok=ok, message=message, table_name=spec.warehouse_table)

    def _insert_update_record(self, df: pd.DataFrame, target_table: str, pk: str = "_id"):
        engine = self._engine
        columns_to_drop = None
        random_number = random.randint(10, 10000)
        temp = f"{target_table}_temp{random_number}"

        try:
            if not inspect(engine).has_table(target_table):
                df.to_sql(target_table, engine, index=False, if_exists="append")
                return True, f"{target_table} created and loaded", columns_to_drop

            df, columns_to_drop = schema_validation(target_table, engine, df)
            create = f"create table if not exists {temp} (like {target_table});"
            drop = f"drop table {temp}"
            transact = f"""
                begin transaction;
                delete from {target_table} using {temp} where {target_table}.{pk} = {temp}.{pk};
                insert into {target_table} select * from {temp};
                drop table {temp};
                end transaction;
            """.replace("None", "NULL")

            with engine.begin() as conn:
                conn.execute(text(create))
            try:
                df.to_sql(temp, engine, index=False, if_exists="append")
            except Exception as exc:
                with engine.begin() as conn:
                    conn.execute(text(drop))
                raise OplogWorksError("insert_update_record", str(exc)) from exc

            with engine.begin() as conn:
                conn.execute(text(transact))
            return True, "Transaction successful!", columns_to_drop
        except OplogWorksError:
            raise
        except Exception as exc:
            try:
                with engine.begin() as conn:
                    conn.execute(text(drop))
            except Exception:
                pass
            return False, str(exc), columns_to_drop
