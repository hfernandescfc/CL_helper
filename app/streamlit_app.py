from __future__ import annotations

import re
import unicodedata

import duckdb
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

from footballdata.config import settings
from footballdata.extract.odds_api import OddsAPIError, OddsAPIHTTPError, OddsAPIMeta, fetch_champions_league_odds

st.set_page_config(page_title="FootballData Dashboard", layout="wide")


DEBUG_ODDS = st.sidebar.checkbox("Debug odds matching", value=False)
if "odds_cache" not in st.session_state:
    st.session_state["odds_cache"] = None


@st.cache_resource
def get_con():
    return duckdb.connect("warehouse/warehouse.duckdb", read_only=True)


def _normalize_team_name(name: str | None) -> str:
    if not name:
        return ""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"\b(fc|cf|ac|club|the)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


@st.cache_data(ttl=1800)
def get_upcoming_cl_matches():
    headers = {"Accept": "application/json"}
    if settings.FOOTBALL_DATA_API_KEY:
        headers["X-Auth-Token"] = settings.FOOTBALL_DATA_API_KEY
    url = f"{settings.FOOTBALL_DATA_BASE_URL.rstrip('/')}/competitions/CL/matches"
    params = {"status": "SCHEDULED"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        st.warning(f"Não foi possível buscar a próxima rodada da API Football-Data: {exc}")
        return pd.DataFrame()

    matches = resp.json().get("matches", [])
    if not matches:
        return pd.DataFrame()
    df = pd.json_normalize(matches)
    df["utcDate"] = pd.to_datetime(df["utcDate"], utc=True, errors="coerce")
    df.rename(
        columns={
            "homeTeam.name": "home_team",
            "homeTeam.id": "home_team_id",
            "awayTeam.name": "away_team",
            "awayTeam.id": "away_team_id",
            "group": "group_name",
        },
        inplace=True,
    )

    df_valid = df[df["home_team"].notna() & df["away_team"].notna()]
    if df_valid.empty:
        target = df.sort_values("utcDate").head(20)
    else:
        valid_matchdays = df_valid["matchday"].dropna()
        if not valid_matchdays.empty:
            next_matchday = valid_matchdays.min()
            target = df_valid[df_valid["matchday"] == next_matchday]
        else:
            target = df_valid.sort_values("utcDate").head(20)

    target = target.copy().sort_values("utcDate").reset_index(drop=True)
    target["kickoff_local"] = target["utcDate"].dt.tz_convert("America/Sao_Paulo").dt.strftime("%d/%m %H:%M")

    return target[
        [
            "matchday",
            "kickoff_local",
            "utcDate",
            "home_team_id",
            "home_team",
            "away_team_id",
            "away_team",
            "stage",
        ]
    ]


def _build_odds_overview(odds_df: pd.DataFrame) -> pd.DataFrame:
    if odds_df.empty:
        return pd.DataFrame()
    overview = (
        odds_df.groupby("event_id")
        .agg(
            commence_time=("commence_time", "max"),
            home_team=("home_team", "first"),
            away_team=("away_team", "first"),
            markets=("market_key", lambda x: ", ".join(sorted(set(x.dropna())))),
            bookmakers=("bookmaker_key", lambda x: x.nunique()),
        )
        .reset_index()
    )
    overview["home_norm"] = overview["home_team"].apply(_normalize_team_name)
    overview["away_norm"] = overview["away_team"].apply(_normalize_team_name)
    return overview


@st.cache_data(ttl=600)
def get_cl_odds_data():
    requested_markets = ("h2h", "totals")
    try:
        df, meta = fetch_champions_league_odds(regions="eu", markets=requested_markets)
        used_markets = requested_markets
    except OddsAPIHTTPError as exc:
        if exc.status_code == 422 and len(requested_markets) > 1:
            fallback_markets = ("h2h",)
            df, meta = fetch_champions_league_odds(regions="eu", markets=fallback_markets)
            used_markets = fallback_markets
        else:
            raise
    return df, meta, used_markets


def build_moneyline_summary(fixtures_df: pd.DataFrame, odds_df: pd.DataFrame) -> pd.DataFrame:
    if fixtures_df.empty or odds_df.empty:
        return pd.DataFrame()

    fixtures = fixtures_df.copy()
    fixtures["home_norm"] = fixtures["home_team"].apply(_normalize_team_name)
    fixtures["away_norm"] = fixtures["away_team"].apply(_normalize_team_name)

    odds = odds_df.copy()
    odds = odds[odds["market_key"] == "h2h"]
    if odds.empty:
        return pd.DataFrame()
    odds["home_norm"] = odds["home_team"].apply(_normalize_team_name)
    odds["away_norm"] = odds["away_team"].apply(_normalize_team_name)

    fixtures_subset = fixtures[
        [
            "home_norm",
            "away_norm",
            "matchday",
            "stage",
            "kickoff_local",
            "home_team",
            "away_team",
        ]
    ].rename(
        columns={
            "home_team": "fixture_home_team",
            "away_team": "fixture_away_team",
        }
    )

    odds = odds.drop(columns=["home_team", "away_team"], errors="ignore")

    merged = odds.merge(
        fixtures_subset,
        on=["home_norm", "away_norm"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()

    def _map_outcome(row: pd.Series) -> str | None:
        outcome_norm = _normalize_team_name(row["outcome_name"])
        if outcome_norm == row["home_norm"]:
            return "Mandante"
        if outcome_norm == row["away_norm"]:
            return "Visitante"
        if outcome_norm in {"draw", "empate"}:
            return "Empate"
        return None

    merged["outcome_label"] = merged.apply(_map_outcome, axis=1)
    merged = merged[merged["outcome_label"].notna()]
    if merged.empty:
        return pd.DataFrame()

    grouped = merged.groupby("outcome_label")["outcome_price"].median()
    bookmaker_count = merged["bookmaker_key"].nunique()
    last_update = merged["bookmaker_last_update"].max()
    last_update_display = "-"
    if pd.notna(last_update):
        last_update_display = pd.to_datetime(last_update).strftime("%d/%m %H:%M")
    match_info = merged.iloc[0]
    summary = {
        "Rodada": match_info["matchday"],
        "Fase": match_info["stage"],
        "Data (BR)": match_info["kickoff_local"],
        "Partida": f"{match_info['fixture_home_team']} x {match_info['fixture_away_team']}",
        "Casas consideradas": bookmaker_count,
        "Atualizado (UTC)": last_update_display,
        "Mandante": grouped.get("Mandante"),
        "Empate": grouped.get("Empate"),
        "Visitante": grouped.get("Visitante"),
    }
    df = pd.DataFrame([summary])
    return df


def build_totals_table(fixtures_df: pd.DataFrame, odds_df: pd.DataFrame) -> pd.DataFrame:
    if fixtures_df.empty or odds_df.empty:
        return pd.DataFrame()

    fixtures = fixtures_df.copy()
    fixtures["home_norm"] = fixtures["home_team"].apply(_normalize_team_name)
    fixtures["away_norm"] = fixtures["away_team"].apply(_normalize_team_name)

    odds = odds_df.copy()
    odds = odds[odds["market_key"] == "totals"]
    if odds.empty:
        return pd.DataFrame()
    odds["home_norm"] = odds["home_team"].apply(_normalize_team_name)
    odds["away_norm"] = odds["away_team"].apply(_normalize_team_name)

    fixtures_subset = fixtures[
        [
            "home_norm",
            "away_norm",
            "matchday",
            "stage",
            "kickoff_local",
            "home_team",
            "away_team",
        ]
    ].rename(
        columns={
            "home_team": "fixture_home_team",
            "away_team": "fixture_away_team",
        }
    )

    odds = odds.drop(columns=["home_team", "away_team"], errors="ignore")

    merged = odds.merge(fixtures_subset, on=["home_norm", "away_norm"], how="inner")
    if merged.empty:
        return pd.DataFrame()

    records: list[dict] = []
    pivot = (
        merged.groupby(["outcome_point", "outcome_name"])["outcome_price"]
        .median()
        .unstack()
        .rename(columns=lambda c: c.title())
    )
    if pivot.empty:
        return pd.DataFrame()
    counts = merged.groupby("outcome_point")["bookmaker_key"].nunique()
    last_update = merged.groupby("outcome_point")["bookmaker_last_update"].max()
    pivot = pivot.reset_index().rename(columns={"outcome_point": "Linha (gols)"})
    match_row = merged.iloc[0]
    pivot["Rodada"] = match_row["matchday"]
    pivot["Fase"] = match_row["stage"]
    pivot["Data (BR)"] = match_row["kickoff_local"]
    pivot["Partida"] = f"{match_row['fixture_home_team']} x {match_row['fixture_away_team']}"
    pivot["Casas consideradas"] = pivot["Linha (gols)"].map(counts).fillna(0).astype(int)
    def _format_update(pt):
        val = last_update.get(pt)
        if pd.isna(val):
            return "-"
        return pd.to_datetime(val).strftime("%d/%m %H:%M")
    pivot["Atualizado (UTC)"] = pivot["Linha (gols)"].map(_format_update)
    ordered = pivot[
        [
            "Rodada",
            "Fase",
            "Data (BR)",
            "Partida",
            "Linha (gols)",
            "Over",
            "Under",
            "Casas consideradas",
            "Atualizado (UTC)",
        ]
    ]
    return ordered.sort_values(["Linha (gols)"])


def fetch_team_insights(team_id: int):
    stats_sql = """
        SELECT
            COUNT(*) AS matches_played,
            COALESCE(SUM(CASE WHEN home_team_id = ? THEN ft_home_goals ELSE ft_away_goals END), 0) AS goals_for,
            COALESCE(SUM(CASE WHEN home_team_id = ? THEN ft_away_goals ELSE ft_home_goals END), 0) AS goals_against
        FROM silver.matches
        WHERE competition_code = 'CL'
          AND status IN ('FINISHED','AWARDED')
          AND (home_team_id = ? OR away_team_id = ?);
    """
    stats = con.execute(stats_sql, [team_id] * 4).df().iloc[0]

    points_sql = """
        SELECT
            COALESCE(SUM(
                CASE
                    WHEN home_team_id = ? AND ft_home_goals > ft_away_goals THEN 3
                    WHEN away_team_id = ? AND ft_away_goals > ft_home_goals THEN 3
                    WHEN ft_home_goals = ft_away_goals THEN 1
                    ELSE 0
                END
            ), 0) AS points_total
        FROM silver.matches
        WHERE competition_code = 'CL'
          AND status IN ('FINISHED','AWARDED')
          AND (home_team_id = ? OR away_team_id = ?);
    """
    points_row = con.execute(points_sql, [team_id] * 4).df().iloc[0]

    location_sql = """
        SELECT
            SUM(CASE WHEN home_team_id = ? THEN ft_home_goals ELSE 0 END) AS gf_home,
            SUM(CASE WHEN home_team_id = ? THEN ft_away_goals ELSE 0 END) AS ga_home,
            COUNT(CASE WHEN home_team_id = ? THEN 1 END) AS games_home,
            SUM(CASE WHEN away_team_id = ? THEN ft_away_goals ELSE 0 END) AS gf_away,
            SUM(CASE WHEN away_team_id = ? THEN ft_home_goals ELSE 0 END) AS ga_away,
            COUNT(CASE WHEN away_team_id = ? THEN 1 END) AS games_away,
            COUNT(CASE WHEN home_team_id = ? AND ft_away_goals = 0 THEN 1 END) AS clean_home,
            COUNT(CASE WHEN away_team_id = ? AND ft_home_goals = 0 THEN 1 END) AS clean_away
        FROM silver.matches
        WHERE competition_code = 'CL'
          AND status IN ('FINISHED','AWARDED')
          AND (home_team_id = ? OR away_team_id = ?);
    """
    location_row = con.execute(location_sql, [team_id] * 10).df().iloc[0]
    def _avg(numer: float, denom: float) -> float | None:
        return float(numer) / float(denom) if denom and denom > 0 else None
    location_avgs = {
        "gf_home_avg": _avg(location_row["gf_home"], location_row["games_home"]),
        "ga_home_avg": _avg(location_row["ga_home"], location_row["games_home"]),
        "gf_away_avg": _avg(location_row["gf_away"], location_row["games_away"]),
        "ga_away_avg": _avg(location_row["ga_away"], location_row["games_away"]),
        "games_home": int(location_row["games_home"]),
        "games_away": int(location_row["games_away"]),
        "clean_home": int(location_row["clean_home"]),
        "clean_away": int(location_row["clean_away"]),
        "clean_total": int(location_row["clean_home"] + location_row["clean_away"]),
    }

    form_sql = """
        WITH ordered AS (
            SELECT
                match_id,
                match_utc_datetime,
                CASE
                    WHEN ft_home_goals = ft_away_goals THEN 'E'
                    WHEN home_team_id = ? AND ft_home_goals > ft_away_goals THEN 'V'
                    WHEN away_team_id = ? AND ft_away_goals > ft_home_goals THEN 'V'
                    ELSE 'D'
                END AS result
            FROM silver.matches
            WHERE competition_code = 'CL'
              AND status IN ('FINISHED','AWARDED')
              AND (home_team_id = ? OR away_team_id = ?)
            ORDER BY match_utc_datetime DESC
            LIMIT 3
        )
        SELECT string_agg(result, ' - ' ORDER BY match_utc_datetime DESC) AS form
        FROM ordered;
    """
    form_df = con.execute(form_sql, [team_id] * 4).df()
    form_display = form_df.iloc[0]["form"] if not form_df.empty else None

    matches_sql = """
        SELECT
            sm.match_utc_datetime,
            sm.home_team_name,
            sm.away_team_name,
            sm.ft_home_goals,
            sm.ft_away_goals,
            CASE
                WHEN sm.ft_home_goals = sm.ft_away_goals THEN 'Empate'
                WHEN sm.home_team_id = ? AND sm.ft_home_goals > sm.ft_away_goals THEN 'Vitória'
                WHEN sm.away_team_id = ? AND sm.ft_away_goals > sm.ft_home_goals THEN 'Vitória'
                ELSE 'Derrota'
            END AS resultado,
            adversary_stats.points_before AS pontos_adversario_pre_jogo
        FROM silver.matches sm
        LEFT JOIN (
            SELECT team_id, match_id, points_before
            FROM gold.team_form_rolling
            WHERE competition_code = 'CL'
        ) adversary_stats
          ON adversary_stats.match_id = sm.match_id
          AND adversary_stats.team_id = CASE WHEN sm.home_team_id = ? THEN sm.away_team_id ELSE sm.home_team_id END
        WHERE sm.competition_code = 'CL'
          AND sm.status IN ('FINISHED','AWARDED')
          AND (sm.home_team_id = ? OR sm.away_team_id = ?)
        ORDER BY sm.match_utc_datetime DESC;
    """
    matches_df = con.execute(matches_sql, [team_id] * 5).df()

    goals_chart_sql = """
        SELECT
            matchday,
            CASE WHEN home_team_id = ? THEN 'Mandante' ELSE 'Visitante' END AS location,
            SUM(CASE WHEN home_team_id = ? THEN ft_home_goals ELSE ft_away_goals END) AS goals_for,
            SUM(CASE WHEN home_team_id = ? THEN ft_away_goals ELSE ft_home_goals END) AS goals_against
        FROM silver.matches
        WHERE competition_code = 'CL'
          AND status IN ('FINISHED','AWARDED')
          AND (home_team_id = ? OR away_team_id = ?)
        GROUP BY matchday, location
        ORDER BY matchday;
    """
    goals_df = con.execute(goals_chart_sql, [team_id] * 5).df()

    last_games_sql = """
        SELECT
            match_utc_datetime,
            competition_name,
            home_team_name,
            away_team_name,
            ft_home_goals,
            ft_away_goals,
            CASE
                WHEN ft_home_goals = ft_away_goals THEN 'Empate'
                WHEN home_team_id = ? AND ft_home_goals > ft_away_goals THEN 'Vitória'
                WHEN away_team_id = ? AND ft_away_goals > ft_home_goals THEN 'Vitória'
                ELSE 'Derrota'
            END AS resultado
        FROM silver.matches
        WHERE status IN ('FINISHED','AWARDED')
          AND (home_team_id = ? OR away_team_id = ?)
        ORDER BY match_utc_datetime DESC
        LIMIT 5;
    """
    last_games_df = con.execute(last_games_sql, [team_id] * 4).df()

    return {
        "stats": stats,
        "form": form_display,
        "matches": matches_df,
        "goals": goals_df,
        "last_games": last_games_df,
        "location_avgs": location_avgs,
        "points_total": int(points_row["points_total"]),
    }


def render_team_insights(team_name: str, team_id: int, current_role: str | None = None):
    insights = fetch_team_insights(team_id)
    stats = insights["stats"]
    if stats["matches_played"] == 0:
        st.info(f"Ainda não há jogos finalizados para {team_name} na Champions.")
        return

    st.markdown(f"### {team_name}")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Jogos", int(stats["matches_played"]))
    col2.metric("Gols marcados", int(stats["goals_for"]))
    col3.metric("Gols sofridos", int(stats["goals_against"]))
    col4.metric("Pontos (CL)", insights.get("points_total", 0))

    if insights["form"]:
        st.metric("Forma recente (últimos 3)", insights["form"])

    loc_avgs = insights.get("location_avgs", {})
    if loc_avgs:
        st.markdown("#### Médias de gols (mandante/visitante)")
        col_home_avg, col_away_avg = st.columns(2)
        home_for = loc_avgs.get("gf_home_avg")
        home_against = loc_avgs.get("ga_home_avg")
        away_for = loc_avgs.get("gf_away_avg")
        away_against = loc_avgs.get("ga_away_avg")
        games_home = loc_avgs.get("games_home", 0)
        games_away = loc_avgs.get("games_away", 0)
        col_home_avg.metric(
            "Mandante: gols pró",
            f"{home_for:.2f}" if home_for is not None else "—",
            help=f"Média em {games_home} jogos como mandante",
        )
        col_home_avg.metric(
            "Mandante: gols contra",
            f"{home_against:.2f}" if home_against is not None else "—",
            help=f"Média em {games_home} jogos como mandante",
        )
        col_away_avg.metric(
            "Visitante: gols pró",
            f"{away_for:.2f}" if away_for is not None else "—",
            help=f"Média em {games_away} jogos como visitante",
        )
        col_away_avg.metric(
            "Visitante: gols contra",
            f"{away_against:.2f}" if away_against is not None else "—",
            help=f"Média em {games_away} jogos como visitante",
        )
        st.markdown("#### Jogos sem sofrer gols (clean sheets)")
        cs_home = loc_avgs.get("clean_home", 0)
        cs_away = loc_avgs.get("clean_away", 0)
        cs_total = loc_avgs.get("clean_total", 0)
        col_cs1, col_cs2, col_cs3 = st.columns(3)
        col_cs1.metric("Total clean sheets", cs_total)
        col_cs2.metric("Clean sheets (mandante)", cs_home)
        col_cs3.metric("Clean sheets (visitante)", cs_away)

    st.markdown("#### Jogos na Champions")
    matches_df = insights["matches"].copy()
    if not matches_df.empty:
        matches_df["Data/Hora"] = pd.to_datetime(matches_df["match_utc_datetime"]).dt.strftime("%d/%m %H:%M")
        matches_df["Separador"] = "x"
        matches_display = matches_df.rename(
            columns={
                "home_team_name": "Mandante",
                "ft_home_goals": "Gols Mandante",
                "ft_away_goals": "Gols Visitante",
                "away_team_name": "Visitante",
                "resultado": "Resultado",
                "pontos_adversario_pre_jogo": "Pts adversário antes",
            }
        )[
            [
                "Data/Hora",
                "Mandante",
                "Gols Mandante",
                "Separador",
                "Gols Visitante",
                "Visitante",
                "Resultado",
                "Pts adversário antes",
            ]
        ]
        st.dataframe(matches_display, use_container_width=True, hide_index=True)

    goals_df = insights["goals"]
    if not goals_df.empty:
        goals_df = goals_df.copy()
        goals_df["matchday"] = goals_df["matchday"].fillna("N/A")
        goals_df["location"] = goals_df["location"].fillna("Indefinido")
        goals_long = goals_df.melt(
            id_vars=["matchday", "location"],
            value_vars=["goals_for", "goals_against"],
            var_name="metric",
            value_name="gols",
        )
        metric_labels = {"goals_for": "Gols pró", "goals_against": "Gols contra"}
        goals_long["metric"] = goals_long["metric"].map(metric_labels)
        goals_long["metric_loc"] = goals_long["metric"] + " (" + goals_long["location"] + ")"

        def _color_map(role: str | None):
            base = {
                ("Gols pró", "Mandante"): "#2ca02c",
                ("Gols contra", "Mandante"): "#d62728",
                ("Gols pró", "Visitante"): "#9ecf9e",
                ("Gols contra", "Visitante"): "#e6b0b0",
            }
            if role is None:
                return {f"{m} ({loc})": color for (m, loc), color in base.items()}
            bright = {
                ("Gols pró", role): "#2ca02c",
                ("Gols contra", role): "#d62728",
            }
            muted = {
                ("Gols pró", "Mandante" if role == "Visitante" else "Visitante"): "#9ecf9e",
                ("Gols contra", "Mandante" if role == "Visitante" else "Visitante"): "#e6b0b0",
            }
            colors = bright | muted
            return {f"{m} ({loc})": color for (m, loc), color in colors.items()}

        color_map = _color_map(current_role)
        fig = px.bar(
            goals_long,
            x="matchday",
            y="gols",
            color="metric_loc",
            pattern_shape="location",
            pattern_shape_sequence=["", "\\"],
            barmode="group",
            labels={
                "matchday": "Rodada",
                "gols": "Gols",
                "metric_loc": "Métrica",
                "location": "Local",
            },
            title=f"Gols por rodada (CL) - {team_name}",
            color_discrete_map=color_map,
        )
        fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                title=None,
            )
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Últimos 5 jogos (todas as competições)")
    last_games_df = insights["last_games"].copy()
    if not last_games_df.empty:
        last_games_df["Data/Hora"] = pd.to_datetime(last_games_df["match_utc_datetime"]).dt.strftime("%d/%m %H:%M")
        last_games_df["Separador"] = "x"
        last_display = last_games_df.rename(
            columns={
                "home_team_name": "Mandante",
                "ft_home_goals": "Gols Mandante",
                "ft_away_goals": "Gols Visitante",
                "away_team_name": "Visitante",
                "resultado": "Resultado",
                "competition_name": "Competição",
            }
        )[
            [
                "Data/Hora",
                "Competição",
                "Mandante",
                "Gols Mandante",
                "Separador",
                "Gols Visitante",
                "Visitante",
                "Resultado",
            ]
        ]
        st.dataframe(last_display, use_container_width=True, hide_index=True)


st.title("FootballData Dashboard")

con = get_con()

st.subheader("Próxima rodada - UEFA Champions League")
upcoming_df = get_upcoming_cl_matches()
if upcoming_df.empty:
    st.info("Não há partidas agendadas ou ocorreu um erro ao consultar a API.")
else:
    st.dataframe(upcoming_df[["matchday", "kickoff_local", "home_team", "away_team", "stage"]], use_container_width=True, hide_index=True)
    fixture_options = [
        (idx, f"{row.home_team} x {row.away_team} ({row.kickoff_local}) - Rodada {row.matchday}")
        for idx, row in upcoming_df.iterrows()
    ]
    selected_option = st.selectbox(
        "Selecione uma partida para comparar estatísticas",
        fixture_options,
        format_func=lambda item: item[1],
    )
    match_idx = selected_option[0]
    match_row = upcoming_df.iloc[match_idx]
    selected_fixture_df = upcoming_df.iloc[[match_idx]]
    if pd.notna(match_row["home_team_id"]) and pd.notna(match_row["away_team_id"]):
        col_home, col_away = st.columns(2)
        with col_home:
            render_team_insights(match_row["home_team"], int(match_row["home_team_id"]), current_role="Mandante")
        with col_away:
            render_team_insights(match_row["away_team"], int(match_row["away_team_id"]), current_role="Visitante")
    else:
        st.info("A partida selecionada ainda não possui times definidos.")

    st.markdown("#### Odds das casas europeias (Odds API)")
    try:
        odds_raw_df, odds_meta, used_markets = get_cl_odds_data()
        st.session_state["odds_cache"] = {
            "df": odds_raw_df,
            "meta": odds_meta,
            "markets": used_markets,
            "captured_at": pd.Timestamp.utcnow(),
        }
    except OddsAPIError as exc:
        cache = st.session_state.get("odds_cache")
        if cache and isinstance(cache, dict) and "df" in cache:
            odds_raw_df = cache.get("df", pd.DataFrame())
            odds_meta = cache.get("meta", OddsAPIMeta())
            used_markets = cache.get("markets", tuple())
            cached_at = cache.get("captured_at")
            cached_at_display = "-"
            if pd.notna(cached_at):
                cached_at_display = pd.to_datetime(cached_at).strftime("%d/%m %H:%M")
            st.warning(
                f"Nao foi possivel consultar a Odds API: {exc}. "
                f"Usando o ultimo cache valido ({cached_at_display} UTC)."
            )
        else:
            st.warning(f"Nao foi possivel consultar a Odds API: {exc}")
            odds_raw_df = pd.DataFrame()
            odds_meta = OddsAPIMeta()
            used_markets = tuple()
    odds_overview = _build_odds_overview(odds_raw_df)
    if DEBUG_ODDS:
        st.caption(f"Odds rows: {len(odds_raw_df)} | eventos: {len(odds_overview)} | markets: {used_markets}")
        if not odds_overview.empty:
            st.dataframe(
                odds_overview[["event_id", "commence_time", "home_team", "away_team", "markets", "bookmakers"]].head(50),
                use_container_width=True,
                hide_index=True,
            )
    odds_table = build_moneyline_summary(selected_fixture_df, odds_raw_df)
    if DEBUG_ODDS and not odds_overview.empty:
        fixture_row = selected_fixture_df.iloc[0]
        fixture_home = _normalize_team_name(fixture_row["home_team"])
        fixture_away = _normalize_team_name(fixture_row["away_team"])
        exact = odds_overview[(odds_overview["home_norm"] == fixture_home) & (odds_overview["away_norm"] == fixture_away)]
        swapped = odds_overview[(odds_overview["home_norm"] == fixture_away) & (odds_overview["away_norm"] == fixture_home)]
        st.caption(f"Fixture norm: {fixture_home} x {fixture_away}")
        if exact.empty and not swapped.empty:
            st.info("Odds encontradas com mandante/visitante invertidos (verifique normalizacao).")
            st.dataframe(
                swapped[["event_id", "commence_time", "home_team", "away_team", "markets", "bookmakers"]],
                use_container_width=True,
                hide_index=True,
            )
        elif exact.empty:
            st.info("Nenhuma odd casa com o confronto selecionado; possivel falta de cobertura ou mismatch de nomes.")
        else:
            st.caption(f"Odds com match exato: {len(exact)} evento(s).")
            st.dataframe(
                exact[["event_id", "commence_time", "home_team", "away_team", "markets", "bookmakers"]],
                use_container_width=True,
                hide_index=True,
            )
    if odds_table.empty:
        st.info("Ainda não encontramos odds moneyline para a partida selecionada.")
    else:
        st.dataframe(
            odds_table,
            use_container_width=True,
            hide_index=True,
        )
        st.caption("Mediana das odds entre as casas de aposta disponíveis para este confronto.")

    st.markdown("##### Totais de gols (Over/Under)")
    totals_enabled = "totals" in used_markets
    if not totals_enabled:
        st.info("O mercado 'totals' não está disponível para este esporte/período na Odds API.")
    else:
        totals_table = build_totals_table(selected_fixture_df, odds_raw_df)
        if totals_table.empty:
            st.info("Este confronto ainda não possui linhas de totais disponíveis nas casas europeias.")
        else:
            st.dataframe(totals_table, use_container_width=True, hide_index=True)
            st.caption("Mediana das odds Over/Under por linha de gols entre as casas consultadas.")

    if odds_meta.requests_remaining is not None:
        st.caption(
            f"Odds API - Requests restantes: {odds_meta.requests_remaining} "
            f"(usadas: {odds_meta.requests_used or '-'} / última chamada: {odds_meta.requests_last or '-'})"
        )
