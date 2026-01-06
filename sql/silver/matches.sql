-- Silver: matches normalized from raw.matches
-- This is a template; adjust columns to your source shape

CREATE OR REPLACE TABLE silver.matches AS
SELECT
  CAST(m.id AS BIGINT)                           AS match_id,
  TRY_CAST(m."utcDate" AS TIMESTAMP)             AS match_utc_datetime,
  m.status                                       AS status,
  m.stage                                        AS stage,
  m."group"                                      AS group_name,
  m.matchday                                     AS matchday,
  m."season.id"                                  AS season_id,
  m."season.startDate"                           AS season_start_date,
  m."season.endDate"                             AS season_end_date,
  m."competition.id"                             AS competition_id,
  m."competition.code"                           AS competition_code,
  m."competition.name"                           AS competition_name,
  m."area.name"                                  AS area_name,
  m."homeTeam.id"                                AS home_team_id,
  m."homeTeam.name"                              AS home_team_name,
  m."awayTeam.id"                                AS away_team_id,
  m."awayTeam.name"                              AS away_team_name,
  m."score.fullTime.home"                        AS ft_home_goals,
  m."score.fullTime.away"                        AS ft_away_goals,
  m."score.winner"                               AS winner,
  m."lastUpdated"                                AS last_updated,
  m.extracted_at                                 AS extracted_at,
  m.source                                       AS source
FROM raw.matches m;

-- Indexes (DuckDB will use zone maps; explicit indexes are optional)
