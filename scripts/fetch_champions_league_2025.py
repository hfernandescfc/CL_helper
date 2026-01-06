import csv
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import urllib.parse
import urllib.request
import urllib.error


BASE_URL = "https://api.football-data.org/v4"
COMPETITION_CODE = "CL"


def get_token() -> Optional[str]:
    """Return API token from env var FOOTBALL_DATA_API_TOKEN, or None."""
    token = os.environ.get("FOOTBALL_DATA_API_TOKEN")
    if token:
        return token.strip()
    return None


def http_get(path: str, params: Optional[Dict[str, Any]] = None, token: Optional[str] = None) -> Dict[str, Any]:
    """Perform a GET request with retries and return parsed JSON."""
    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    if params:
        qs = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}?{qs}"

    headers = {
        "Accept": "application/json",
    }
    if token:
        headers["X-Auth-Token"] = token

    req = urllib.request.Request(url, headers=headers, method="GET")

    # Basic retry for rate-limits and transient errors
    attempts = 0
    while True:
        attempts += 1
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
                return json.loads(data.decode("utf-8"))
        except urllib.error.HTTPError as e:
            status = e.code
            body = e.read().decode("utf-8", errors="replace")
            if status in (429, 503) and attempts < 5:
                # Retry with exponential backoff
                time.sleep(min(2 ** attempts, 16))
                continue
            # Re-raise with more context
            raise RuntimeError(f"HTTP {status} for {url}: {body}") from e
        except urllib.error.URLError as e:
            if attempts < 5:
                time.sleep(min(2 ** attempts, 16))
                continue
            raise RuntimeError(f"Network error for {url}: {e}") from e


def ensure_output_dir(path: str = "output") -> str:
    os.makedirs(path, exist_ok=True)
    return path


def write_csv(path: str, rows: List[Dict[str, Any]], field_order: Optional[List[str]] = None) -> None:
    if not rows:
        # create empty file with header if provided
        with open(path, "w", newline="", encoding="utf-8") as f:
            if field_order:
                writer = csv.DictWriter(f, fieldnames=field_order)
                writer.writeheader()
            else:
                f.write("")
        return

    if field_order is None:
        # preserve a stable order: keys from first row
        field_order = list(rows[0].keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=field_order)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in field_order})


def flatten_standings(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for table in data.get("standings", []):
        stage = table.get("stage")
        group = table.get("group")
        ttype = table.get("type")
        for position in table.get("table", []) or []:
            team = position.get("team", {})
            rows.append({
                "stage": stage,
                "group": group,
                "type": ttype,
                "position": position.get("position"),
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "playedGames": position.get("playedGames"),
                "won": position.get("won"),
                "draw": position.get("draw"),
                "lost": position.get("lost"),
                "goalsFor": position.get("goalsFor"),
                "goalsAgainst": position.get("goalsAgainst"),
                "goalDifference": position.get("goalDifference"),
                "points": position.get("points"),
            })
    return rows


def flatten_teams(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for t in data.get("teams", []) or []:
        area = t.get("area", {})
        rows.append({
            "id": t.get("id"),
            "name": t.get("name"),
            "shortName": t.get("shortName"),
            "tla": t.get("tla"),
            "area_id": area.get("id"),
            "area_name": area.get("name"),
            "founded": t.get("founded"),
            "clubColors": t.get("clubColors"),
            "venue": t.get("venue"),
            "website": t.get("website"),
        })
    return rows


def flatten_matches(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for m in data.get("matches", []) or []:
        comp = m.get("competition", {})
        season = m.get("season", {})
        score = m.get("score", {})
        full = score.get("fullTime", {})
        half = score.get("halfTime", {})
        extra = score.get("extraTime", {})
        pens = score.get("penalties", {})
        home = m.get("homeTeam", {})
        away = m.get("awayTeam", {})
        rows.append({
            "id": m.get("id"),
            "utcDate": m.get("utcDate"),
            "status": m.get("status"),
            "matchday": m.get("matchday"),
            "stage": m.get("stage"),
            "group": m.get("group"),
            "competition": comp.get("name"),
            "season_startDate": season.get("startDate"),
            "season_endDate": season.get("endDate"),
            "homeTeam_id": home.get("id"),
            "homeTeam_name": home.get("name"),
            "awayTeam_id": away.get("id"),
            "awayTeam_name": away.get("name"),
            "score_winner": score.get("winner"),
            "score_duration": score.get("duration"),
            "ft_home": full.get("home"),
            "ft_away": full.get("away"),
            "ht_home": half.get("home"),
            "ht_away": half.get("away"),
            "et_home": extra.get("home"),
            "et_away": extra.get("away"),
            "p_home": pens.get("home"),
            "p_away": pens.get("away"),
        })
    return rows


def flatten_scorers(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for s in data.get("scorers", []) or []:
        player = s.get("player", {})
        team = s.get("team", {})
        rows.append({
            "player_id": player.get("id"),
            "player_name": player.get("name"),
            "nationality": player.get("nationality"),
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "goals": s.get("goals"),
            "assists": s.get("assists"),
            "penalties": s.get("penalties"),
        })
    return rows


def main() -> int:
    token = get_token()
    if not token:
        print(
            "ERRO: defina a variável de ambiente FOOTBALL_DATA_API_TOKEN com seu token da API.",
            file=sys.stderr,
        )
        print(
            "Ex.: PowerShell -> $env:FOOTBALL_DATA_API_TOKEN='seu_token_aqui'",
            file=sys.stderr,
        )
        return 2

    outdir = ensure_output_dir()

    # Competition info (basic)
    comp = http_get(f"competitions/{COMPETITION_CODE}", token=token)

    # Standings for season 2025 (if available for this competition)
    try:
        standings_data = http_get(
            f"competitions/{COMPETITION_CODE}/standings", params={"season": 2025}, token=token
        )
        standings_rows = flatten_standings(standings_data)
    except RuntimeError as e:
        # Not all competitions expose standings (e.g., CUP types can return 404)
        sys.stderr.write(
            (
                "Aviso: standings não disponível para CL 2025 (ou seu plano atual). "
                f"Detalhes: {e}\n"
            )
        )
        standings_rows = []
    write_csv(os.path.join(outdir, "cl_2025_standings.csv"), standings_rows)

    # Teams for season 2025
    try:
        teams_data = http_get(
            f"competitions/{COMPETITION_CODE}/teams", params={"season": 2025}, token=token
        )
        teams_rows = flatten_teams(teams_data)
    except RuntimeError as e:
        sys.stderr.write(
            f"Aviso: não foi possível obter times CL 2025: {e}\n"
        )
        teams_rows = []
    write_csv(os.path.join(outdir, "cl_2025_teams.csv"), teams_rows)

    # Matches in calendar year 2025
    try:
        matches_data = http_get(
            f"competitions/{COMPETITION_CODE}/matches",
            params={"dateFrom": "2025-01-01", "dateTo": "2025-12-31"},
            token=token,
        )
        matches_rows = flatten_matches(matches_data)
    except RuntimeError as e:
        sys.stderr.write(
            f"Aviso: não foi possível obter partidas CL no ano 2025: {e}\n"
        )
        matches_rows = []
    write_csv(os.path.join(outdir, "cl_2025_matches.csv"), matches_rows)

    # Top scorers for season 2025 (if available on plan)
    try:
        scorers_data = http_get(
            f"competitions/{COMPETITION_CODE}/scorers", params={"season": 2025, "limit": 50}, token=token
        )
        scorers_rows = flatten_scorers(scorers_data)
    except RuntimeError as e:
        # Some plans may not include scorers; create empty if forbidden/not available
        sys.stderr.write(f"Aviso: não foi possível obter artilharia: {e}\n")
        scorers_rows = []
    write_csv(os.path.join(outdir, "cl_2025_scorers.csv"), scorers_rows)

    # Summary JSON
    summary = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "competition": {
            "id": comp.get("id"),
            "name": comp.get("name"),
            "code": comp.get("code"),
            "type": comp.get("type"),
            "emblem": comp.get("emblem"),
            "area": comp.get("area"),
        },
        "season": 2025,
        "counts": {
            "standings_rows": len(standings_rows),
            "teams": len(teams_rows),
            "matches_2025": len(matches_rows),
            "scorers": len(scorers_rows),
        },
        "files": {
            "standings_csv": os.path.join(outdir, "cl_2025_standings.csv"),
            "teams_csv": os.path.join(outdir, "cl_2025_teams.csv"),
            "matches_csv": os.path.join(outdir, "cl_2025_matches.csv"),
            "scorers_csv": os.path.join(outdir, "cl_2025_scorers.csv"),
        },
    }

    with open(os.path.join(outdir, "cl_2025_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Dados salvos em:")
    for k, v in summary["files"].items():
        print(f" - {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
