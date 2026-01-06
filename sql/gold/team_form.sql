-- Gold: rolling team form (last 5 matches)
-- Depends on silver.matches

CREATE OR REPLACE TABLE gold.team_form_rolling AS
WITH base AS (
  SELECT
    match_id,
    competition_id,
    competition_name,
    competition_code,
    match_utc_datetime,
    home_team_id AS team_id,
    home_team_name AS team_name,
    CASE
      WHEN ft_home_goals > ft_away_goals THEN 3
      WHEN ft_home_goals = ft_away_goals THEN 1
      ELSE 0
    END AS points,
    'H' AS venue
  FROM silver.matches
  WHERE status IN ('FINISHED','AWARDED')
  UNION ALL
  SELECT
    match_id,
    competition_id,
    competition_name,
    competition_code,
    match_utc_datetime,
    away_team_id AS team_id,
    away_team_name AS team_name,
    CASE
      WHEN ft_away_goals > ft_home_goals THEN 3
      WHEN ft_away_goals = ft_home_goals THEN 1
      ELSE 0
    END AS points,
    'A' AS venue
  FROM silver.matches
  WHERE status IN ('FINISHED','AWARDED')
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY team_id, competition_code ORDER BY match_utc_datetime, match_id) AS rn,
    SUM(points) OVER (PARTITION BY team_id, competition_code ORDER BY match_utc_datetime, match_id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS last5_points,
    SUM(points) OVER (
        PARTITION BY team_id, competition_code
        ORDER BY match_utc_datetime, match_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS points_before
  FROM base
)
SELECT
  match_id,
  competition_id,
  competition_name,
  competition_code,
  match_utc_datetime,
  team_id,
  team_name,
  points,
  venue,
  rn,
  last5_points,
  COALESCE(points_before, 0) AS points_before
FROM ranked;
