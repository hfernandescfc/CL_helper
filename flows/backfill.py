from __future__ import annotations

from datetime import datetime

import pandas as pd
from prefect import flow, task

from footballdata.extract.football_data_api import fetch_matches_since
from footballdata.io.duckdb_client import DuckDB, init_schemas
from footballdata.transform.normalize import ensure_dtypes_matches
from footballdata.utils.logging import get_logger


logger = get_logger(__name__)
SEASON_2025_START = datetime(2025, 9, 1)


@task(retries=3, retry_delay_seconds=30, name="extract_matches_backfill")
def extract_matches_backfill_task(since: datetime, until: datetime | None = None):
    return fetch_matches_since(since, until=until)


@task(name="load_raw_backfill")
def load_raw_backfill_task(df: pd.DataFrame):
    if df is None:
        return 0, 0
    df = ensure_dtypes_matches(df)
    return DuckDB().upsert_df("raw.matches", df, key_cols=["id"])  # type: ignore[arg-type]


@task(name="transform_sql_backfill")
def transform_sql_backfill_task():
    con = DuckDB()
    exists = con.con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'matches'"
    ).fetchone()
    if not exists:
        logger.info("Tabela raw.matches não existe ainda; pulando transformações SQL.")
        return
    con.exec_sql_file("sql/silver/matches.sql")
    con.exec_sql_file("sql/gold/team_form.sql")


@flow(name="backfill")
def backfill(
    start: datetime | None = None,
    end: datetime | None = None,
):
    init_schemas()
    start = start or SEASON_2025_START
    df = extract_matches_backfill_task(start, end)
    load_raw_backfill_task(df)
    transform_sql_backfill_task()


if __name__ == "__main__":
    backfill()
