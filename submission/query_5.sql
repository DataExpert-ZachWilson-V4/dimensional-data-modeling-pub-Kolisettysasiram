-- This query inserts data into the table actors_history_scd by comparing the data for the last year (1914) with the data for the current year (1915), identifying changes and updating accordingly.
INSERT INTO actors_history_scd (actor,actor_id,quality_class,is_active,start_date,end_date,current_year)
-- CTE 'last_season_scd' selects the data from the last year (1914) from the target table.
WITH
  last_season_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1914
  ),
  -- CTE 'current_season_scd' selects the data for the current year (1915) from the source table.
  current_season_scd AS (
    SELECT
      *
    FROM
      actors 
    WHERE
      current_year = 1915
  ),
  -- CTE 'combined' combines the data from the last season and the current season, identifying changes and creating a unified dataset.
  combined AS (
    SELECT
      COALESCE(ls.actor, cs.actor) AS actor,
      COALESCE(ls.actor_id, cs.actor_id) AS actor_id,
      COALESCE(ls.start_date, cs.current_year) AS start_date,
      COALESCE(ls.end_date, cs.current_year) AS end_date,
      CASE
        WHEN ls.is_active <> cs.is_active OR ls.quality_class <> cs.quality_class THEN 1
        WHEN ls.is_active = cs.is_active THEN 0
      END AS did_change,
      ls.is_active AS is_active_last_year,
      cs.is_active AS is_active_this_year,
      ls.quality_class AS ly_quality_class,
      cs.quality_class AS ts_quality_class,
      1915 AS current_year
    FROM
      last_season_scd ls
      FULL OUTER JOIN current_season_scd cs ON ls.actor_id = cs.actor_id
      AND ls.end_date + 1 = cs.current_year
  ),
  -- CTE 'changes' organizes the changes identified in the 'combined' dataset into arrays for insertion into the target table.
  changes AS (
    SELECT
      actor,
      actor_id,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[
          CAST(
            ROW(
              ly_quality_class,
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[
          CAST(
            ROW(ly_quality_class, is_active_last_year, start_date, end_date) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          ),
          CAST(
            ROW(
              ts_quality_class,
              is_active_this_year,
              current_year,
              current_year
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[
          CAST(
            ROW(
              COALESCE(ly_quality_class, ts_quality_class),
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
      END AS change_array
    FROM
      combined
  )
-- The main query selects the unpacked arrays from 'changes' and inserts them into the target table.
SELECT
  actor,
  actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (change_array) AS arr
