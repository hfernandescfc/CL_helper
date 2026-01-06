from __future__ import annotations

from datetime import datetime

from footballdata.io.duckdb_client import DuckDB


TABLE = "meta.ingestion_watermarks_v2"


def _ensure_table(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS meta;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.ingestion_watermarks_v2 (
            source TEXT,
            entity TEXT,
            key TEXT,
            last_success_at TIMESTAMP,
            high_watermark TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (source, entity, key)
        );
        """
    )


def _get_value(source: str, entity: str, key: str, column: str) -> datetime | None:
    con = DuckDB().con
    _ensure_table(con)
    row = con.execute(
        f"SELECT {column} FROM {TABLE} WHERE source = ? AND entity = ? AND key = ?",
        [source, entity, key],
    ).fetchone()
    if row and row[0] is not None:
        return row[0]
    return None


def get_high_watermark(
    source: str,
    entity: str,
    key: str = "global",
    default: datetime | None = None,
) -> datetime:
    value = _get_value(source, entity, key, "high_watermark")
    if value is not None:
        return value
    return default or datetime(1970, 1, 1)


def get_last_success_at(
    source: str,
    entity: str,
    key: str = "global",
    default: datetime | None = None,
) -> datetime:
    value = _get_value(source, entity, key, "last_success_at")
    if value is not None:
        return value
    return default or datetime(1970, 1, 1)


def set_watermarks(
    source: str,
    entity: str,
    key: str = "global",
    last_success_at: datetime | None = None,
    high_watermark: datetime | None = None,
) -> None:
    con = DuckDB().con
    _ensure_table(con)
    con.execute(
        """
        INSERT INTO meta.ingestion_watermarks_v2 AS t
            (source, entity, key, last_success_at, high_watermark, updated_at)
        VALUES (?, ?, ?, ?, ?, now())
        ON CONFLICT (source, entity, key) DO UPDATE SET
            last_success_at = COALESCE(excluded.last_success_at, t.last_success_at),
            high_watermark = COALESCE(excluded.high_watermark, t.high_watermark),
            updated_at = now();
        """,
        [source, entity, key, last_success_at, high_watermark],
    )


def set_last_success_at(
    source: str,
    entity: str,
    key: str = "global",
    value: datetime | None = None,
) -> None:
    set_watermarks(source, entity, key, last_success_at=value)


def set_high_watermark(
    source: str,
    entity: str,
    key: str = "global",
    value: datetime | None = None,
) -> None:
    set_watermarks(source, entity, key, high_watermark=value)


def get_watermark(entity: str, default: datetime | None = None) -> datetime:
    return get_high_watermark("default", entity, "global", default)


def set_watermark(entity: str, value: datetime | None = None) -> None:
    set_high_watermark("default", entity, "global", value)
