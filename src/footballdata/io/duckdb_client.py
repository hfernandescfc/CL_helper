from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

import duckdb
import pandas as pd

from footballdata.config import settings


class DuckDB:
    def __init__(self, db_path: str | None = None, read_only: bool = False):
        self.db_path = db_path or settings.DUCKDB_PATH
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(self.db_path, read_only=read_only)

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def exec_sql(self, sql: str) -> None:
        self._con.execute(sql)

    def exec_sql_file(self, path: str | Path) -> None:
        sql = Path(path).read_text(encoding="utf-8")
        self.exec_sql(sql)

    def _quote(self, identifier: str) -> str:
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def _ensure_columns(self, table: str) -> None:
        """Add any missing columns so MERGE doesn't fail when schema evolves."""
        incoming_cols = {row[0]: row[1] for row in self._con.execute("DESCRIBE incoming").fetchall()}
        table_cols = {row[0]: row[1] for row in self._con.execute(f"DESCRIBE {table}").fetchall()}

        missing = [(name, dtype) for name, dtype in incoming_cols.items() if name not in table_cols]
        for name, dtype in missing:
            self._con.execute(f"ALTER TABLE {table} ADD COLUMN {self._quote(name)} {dtype}")

    def upsert_df(self, table: str, df: pd.DataFrame | Iterable[Mapping], key_cols: list[str]) -> tuple[int, int]:
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        if df.empty:
            return 0, 0

        # Register incoming data as a DuckDB view
        self._con.register("incoming", df)

        # Ensure table exists with the incoming schema (first time)
        self._con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM incoming LIMIT 0;")
        self._ensure_columns(table)

        cols = [c for c in df.columns]
        on_clause = " AND ".join([f"t.{self._quote(c)} = s.{self._quote(c)}" for c in key_cols])
        update_set = ", ".join([f"{self._quote(c)} = s.{self._quote(c)}" for c in cols if c not in key_cols])
        insert_cols = ", ".join([self._quote(c) for c in cols])
        insert_vals = ", ".join([f"s.{self._quote(c)}" for c in cols])

        merge_sql = f"""
            MERGE INTO {table} t
            USING incoming s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """

        res = self._con.execute(merge_sql).fetchall()
        # DuckDB MERGE returns no row counts; we can approximate by comparing counts before/after if needed.
        # Here, return (0, 0) as unknown to keep interface stable.
        return 0, 0


def init_schemas() -> None:
    DuckDB().exec_sql_file("sql/utils/00_init.sql")
