from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task

from footballdata.config import settings
from footballdata.extract.football_data_api import fetch_matches_since
from footballdata.io.duckdb_client import DuckDB, init_schemas
from footballdata.transform.normalize import ensure_dtypes_matches
from footballdata.utils.idempotency import get_high_watermark, set_high_watermark, set_last_success_at
from footballdata.utils.dates import utc_now
from footballdata.utils.logging import get_logger


logger = get_logger(__name__)

SOURCE = "football-data"
ENTITY = "matches"
ENTITY_KEY = "competition=ALL;season=ALL"


@task(retries=3, retry_delay_seconds=30, name="extract_matches")
def extract_matches_task(since):
    df = fetch_matches_since(since)
    return df


@task(name="load_raw_matches")
def load_raw_task(df: pd.DataFrame):
    if df is None:
        return 0, 0
    df = ensure_dtypes_matches(df)
    return DuckDB().upsert_df("raw.matches", df, key_cols=["id"])  # type: ignore[arg-type]


@task(name="transform_sql")
def transform_sql_task():
    con = DuckDB()
    exists = con.con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'matches'"
    ).fetchone()
    if not exists:
        logger.info("Tabela raw.matches não existe ainda; pulando transformações SQL.")
        return
    con.exec_sql_file("sql/silver/matches.sql")
    con.exec_sql_file("sql/gold/team_form.sql")


@flow(name="daily_etl")
def daily_etl():
    init_schemas()
    since = get_high_watermark(SOURCE, ENTITY, ENTITY_KEY)
    df = extract_matches_task(since)
    load_raw_task(df)
    transform_sql_task()
    now = utc_now()
    set_last_success_at(SOURCE, ENTITY, ENTITY_KEY, now)
    set_high_watermark(SOURCE, ENTITY, ENTITY_KEY, now)


if __name__ == "__main__":
    daily_etl()
