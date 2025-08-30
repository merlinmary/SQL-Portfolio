# ##############################################################################
# Author: Merlin Mary John
# Query: This query pulls the game spreads and associated bookmaker details from
#        the tables, namely prediction_odds, bookmakers, market_categories, and
#        sports_stage_status_data. 
# 
# This is an advanced query example that includes GROUP BY, HAVING, COUNT, json 
# parsing,  INNER joins, CTEs(Common Table Expression), 
# and window_functions(ROW_NUMBER)
# 
# ##############################################################################

WITH 
  spread_counts AS (
    SELECT
      po.game_id,
      po.spread_val,
      po.odds_type AS home_or_away,
      COUNT(*) AS spread_count
    FROM 
      sport.prediction_odds po
    WHERE 
      po.market_category_id = '2'
      AND po."status" = 'Active'
    GROUP BY
      po.game_id,
      po.spread_val,
      po.odds_type
  ),
  popular_spread AS (
    SELECT
      game_id,
      spread_val,
      home_or_away,
      spread_count,
      ROW_NUMBER() OVER(
        PARTITION BY game_id, home_or_away 
        ORDER BY spread_count DESC
      ) AS rn
    FROM
      spread_counts
    WHERE 
      spread_count > 1  -- Ensure we have pairs
  ),
  go2 AS (
    SELECT DISTINCT ON (po.game_id)
      po.game_id,
      b.bookmaker_name as bookmaker_title,
      JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT(
          'odds_type', mc.market_category_name,
          'home', MAX(CASE 
              WHEN po.odds_type = 'Home' THEN po.spread_val::FLOAT 
            END),
          'away', MAX(CASE 
              WHEN po.odds_type = 'Away' THEN po.spread_val::FLOAT 
            END)
        )
      ) AS spread_val
    FROM sport.prediction_odds po
    INNER JOIN sport.bookmakers b
        ON po.bookmaker_id = b.bookmaker_id
    INNER JOIN sport.market_categories mc
        ON po.market_category_id = mc.market_category_id
    INNER JOIN popular_spread ps
        ON 
          ps.game_id = po.game_id
          AND ps.home_or_away = po.odds_type
          AND ps.spread_val::FLOAT = po.spread_val::FLOAT
          AND ps.rn = 1
    WHERE 
      po.market_category_id = '2'
      AND po."status" = 'Active'
    GROUP BY 
      po.game_id, 
      b.bookmaker_name, 
      mc.market_category_name
    HAVING 
      COUNT(*) = 2  -- Ensure both home and away are present
)
SELECT
    g.game_id AS "gameId",
    g.season_id AS "seasonId",
    g."data",
    g.created_at AS "createdAt",
    g.updated_at AS "updatedAt",
    g.processing_status AS "processingStatus",
    sssd.start_date AS "startDate",
    go2.spread_val,
    go2.bookmaker_title
FROM sport.games g
INNER JOIN sport.sports_stage_status_data sssd
    ON g.game_id = sssd.game_id
INNER JOIN go2
    ON go2.game_id = g.game_id
WHERE 
  sssd.league_id = '${challengeResp.games.seasons.leagues?.leagueId}'
  AND g.api_source_id = ${challengeResp.games.apiSourceId}
  AND g.season_id = ${challengeResp.games.seasonId}
ORDER BY "startDate" ASC;