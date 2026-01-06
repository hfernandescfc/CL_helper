from __future__ import annotations

from prefect import flow, task

from footballdata.io.duckdb_client import DuckDB


@task(name="recompute_gold")
def recompute_gold_task():
    con = DuckDB()
    con.exec_sql_file("sql/gold/team_form.sql")


@flow(name="refresh_metrics")
def refresh_metrics():
    recompute_gold_task()


if __name__ == "__main__":
    refresh_metrics()

