from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, Iterator

import pandas as pd
import requests
import time

from footballdata.config import settings


BASE_URL = settings.FOOTBALL_DATA_BASE_URL.rstrip("/")
MAX_WINDOW_DAYS = 7


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if settings.FOOTBALL_DATA_API_KEY:
        headers["X-Auth-Token"] = settings.FOOTBALL_DATA_API_KEY
    return headers


def _chunk_date_range(start: date, end: date, window_days: int = MAX_WINDOW_DAYS) -> Iterator[tuple[date, date]]:
    if window_days < 1:
        raise ValueError("window_days must be >= 1")
    if start > end:
        start, end = end, start
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=window_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def fetch_matches_since(
    since: datetime,
    competitions: Iterable[str] | None = None,
    until: datetime | None = None,
) -> pd.DataFrame:
    """Fetch matches updated since a date, chunking requests to meet API limits."""

    start_date = since.date()
    end_date = (until or (datetime.utcnow() + timedelta(days=1))).date()
    all_frames: list[pd.DataFrame] = []

    for chunk_start, chunk_end in _chunk_date_range(start_date, end_date):
        params: dict[str, str] = {
            "dateFrom": chunk_start.isoformat(),
            "dateTo": chunk_end.isoformat(),
        }
        if competitions:
            params["competitions"] = ",".join(competitions)

        url = f"{BASE_URL}/matches"
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code == 429:
            # Respect rate limits: back off and retry once after waiting
            time.sleep(60)
            resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        matches = payload.get("matches", [])

        if not matches:
            continue
        chunk_df = pd.json_normalize(matches)
        chunk_df["extracted_at"] = pd.Timestamp.utcnow()
        chunk_df["source"] = "football-data.org"
        all_frames.append(chunk_df)

    if not all_frames:
        return pd.DataFrame()
    return pd.concat(all_frames, ignore_index=True)
