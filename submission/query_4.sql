-- Note : The main reason why I am only running historical loads up to 1914 is that the bootcamp.actor_films table contains data from the year 1914 to 2021. Since I loaded the cumulative table only up to 1914 in query2, I am performing historical loads up to the same year. To perform historical loads for all years, I would need to run query2 for each year individually. To make it simple and generic, I build cumulative table for 1914 then I am doing historical load till 1914.
-- This query inserts data into the table actors_history_scd, applying slowly changing dimension (SCD) logic to track changes over time.
INSERT INTO actors_history_scd (actor, actor_id, quality_class, is_active, start_date, end_date, current_year)
-- CTE 'lagged' calculates lagged values for is_active and quality_class columns to compare changes over time.
WITH
  lagged AS (
    SELECT
      actor,
      actor_id,
      CASE
        WHEN is_active THEN 1
        ELSE 0
      END AS is_active,
      CASE
        WHEN LAG(is_active, 1) OVER (
          PARTITION BY
            actor
          ORDER BY
            current_year
        ) THEN 1
        ELSE 0
      END AS is_active_last_year,
      quality_class,
      LAG(quality_class, 1) OVER (
          PARTITION BY
            actor
          ORDER BY
            current_year
        ) AS last_year_quality_class,
      current_year
    FROM
      actors
    WHERE
      current_year <= 1914
  ),
  -- CTE 'streaked' calculates streaks of unchanged values for is_active and quality_class to identify periods of stability.
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year or last_year_quality_class <> quality_class THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor
        ORDER BY
          current_year
      ) AS streak_identifier
    FROM
      lagged
  )
-- The main query selects the maximum quality_class and determines if the actor is active, then aggregates the data based on streaks.
SELECT
  actor,
  actor_id,
  MAX(quality_class) AS quality_class,
  MAX(is_active) = 1 AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  1914 AS current_year
FROM
  streaked
GROUP BY
  actor,
  actor_id,
  streak_identifier
