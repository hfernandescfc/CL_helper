from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
import requests
import time

from footballdata.config import settings


class OddsAPIError(RuntimeError):
    """Raised when Odds API requests fail or configuration is missing."""


class OddsAPIHTTPError(OddsAPIError):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class OddsAPIMeta:
    requests_remaining: int | None = None
    requests_used: int | None = None
    requests_last: int | None = None


BASE_URL = settings.ODDS_API_BASE_URL.rstrip("/")
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2



def _build_meta(headers: requests.structures.CaseInsensitiveDict[str]) -> OddsAPIMeta:
    def _safe_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return OddsAPIMeta(
        requests_remaining=_safe_int(headers.get("x-requests-remaining")),
        requests_used=_safe_int(headers.get("x-requests-used")),
        requests_last=_safe_int(headers.get("x-requests-last")),
    )


def _get(path: str, params: dict[str, str]) -> tuple[list[dict], OddsAPIMeta]:
    if not settings.ODDS_API_KEY:
        raise OddsAPIError("ODDS_API_KEY n?o configurada. Atualize o arquivo .env.")

    url = f"{BASE_URL}{path}"
    merged_params = dict(params)
    merged_params["apiKey"] = settings.ODDS_API_KEY

    def _error_detail(response: requests.Response | None) -> str:
        if response is None:
            return ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                return payload.get("message") or payload.get("error") or str(payload)
        except ValueError:
            pass
        return response.text

    attempts = 0
    while True:
        attempts += 1
        try:
            resp = requests.get(url, params=merged_params, timeout=30)
            resp.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            detail = _error_detail(exc.response)
            if status in {429, 500, 502, 503, 504} and attempts <= MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempts)
                continue
            if status == 404:
                raise OddsAPIHTTPError(
                    "Esporte n?o encontrado (404). Verifique se ODDS_API_SPORT_KEY est? correto ou ativo na Odds API.",
                    status_code=status,
                ) from exc
            if status == 422:
                raise OddsAPIHTTPError(
                    f"Par?metros inv?lidos (422) ao consultar a Odds API: {detail}",
                    status_code=status,
                ) from exc
            raise OddsAPIHTTPError(f"Falha ao consultar a Odds API: {detail or exc}", status_code=status) from exc
        except requests.RequestException as exc:
            if attempts <= MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS * attempts)
                continue
            raise OddsAPIError(f"Falha ao consultar a Odds API: {exc}") from exc

        payload = resp.json()
        meta = _build_meta(resp.headers)

        if not isinstance(payload, list):
            raise OddsAPIError(f"Resposta inesperada da Odds API: {payload}")

        return payload, meta


def fetch_champions_league_odds(
    regions: str = "eu",
    markets: Iterable[str] | None = None,
    odds_format: str = "decimal",
    date_format: str = "iso",
    include_links: bool | None = None,
    sport_key: str | None = None,
) -> tuple[pd.DataFrame, OddsAPIMeta]:
    """
    Fetch odds for UEFA Champions League fixtures using The Odds API.

    Returns a tuple containing the flattened DataFrame and quota metadata.
    """

    markets_param = ",".join(markets) if markets else "h2h"
    params: dict[str, str] = {
        "regions": regions,
        "markets": markets_param,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }
    if include_links is not None:
        params["includeLinks"] = str(include_links).lower()

    key = sport_key or settings.ODDS_API_SPORT_KEY
    payload, meta = _get(f"/sports/{key}/odds/", params=params)

    records: list[dict] = []
    for event in payload:
        commence_time = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        for bookmaker in event.get("bookmakers", []):
            bookmaker_last_update = pd.to_datetime(bookmaker.get("last_update"), utc=True, errors="coerce")
            for market in bookmaker.get("markets", []):
                market_key = market.get("key")
                if market_key is None:
                    continue
                for outcome in market.get("outcomes", []):
                    records.append(
                        {
                            "event_id": event.get("id"),
                            "sport_key": event.get("sport_key"),
                            "commence_time": commence_time,
                            "home_team": event.get("home_team"),
                            "away_team": event.get("away_team"),
                            "bookmaker_key": bookmaker.get("key"),
                            "bookmaker_title": bookmaker.get("title"),
                            "bookmaker_last_update": bookmaker_last_update,
                            "market_key": market_key,
                            "outcome_name": outcome.get("name"),
                            "outcome_price": outcome.get("price"),
                            "outcome_point": outcome.get("point"),
                            "outcome_description": outcome.get("description"),
                        }
                    )

    return pd.DataFrame.from_records(records), meta


__all__ = [
    "OddsAPIError",
    "OddsAPIHTTPError",
    "OddsAPIMeta",
    "fetch_champions_league_odds",
]
