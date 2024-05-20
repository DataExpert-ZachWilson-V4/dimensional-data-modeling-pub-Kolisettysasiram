-- This query inserts data into the table sasiram410.actors_history_scd by comparing the data for the last year (1914) with the data for the current year (1915), identifying the changes and updating accordingly.
INSERT INTO sasiram410.actors_history_scd (actor, actor_id, quality_class, is_active, start_date, end_date, current_year)

-- CTE 'last_season_scd' selects the data from the last year (1914) from the target table.
WITH last_season_scd AS (
    SELECT
        actor,
        actor_id,
        quality_class,
        is_active,
        year(start_date) AS start_date, -- Extract the year from start_date
        year(end_date) AS end_date,     -- Extract the year from end_date
        current_year
    FROM
        sasiram410.actors_history_scd
    WHERE
        current_year = 1914
),

-- CTE 'current_season_scd' selects the data for the current year (1915) from the source table.
current_season_scd AS (
    SELECT
        *
    FROM
        sasiram410.actors
    WHERE
        current_year = 1915
),

-- CTE 'combined' combines the data from the last season and the current season, identifying changes and creating a unified dataset.
combined AS (
    SELECT
        COALESCE(ls.actor, cs.actor) AS actor, -- Use actor from last season, if null use current season actor
        COALESCE(ls.actor_id, cs.actor_id) AS actor_id, -- Use actor_id from last season, if null use current season actor_id
        COALESCE(ls.start_date, cs.current_year) AS start_date, -- Use start_date from last season, if null use current year
        COALESCE(ls.end_date, cs.current_year) AS end_date, -- Use end_date from last season, if null use current year
        CASE
            WHEN ls.is_active <> cs.is_active OR ls.quality_class <> cs.quality_class THEN 1 -- Identify if there is any change in is_active or quality_class
            WHEN ls.is_active = cs.is_active THEN 0 -- No change in is_active
        END AS did_change, -- Flag indicating if there was a change
        ls.is_active AS is_active_last_year, -- is_active status from last year
        cs.is_active AS is_active_this_year, -- is_active status from this year
        ls.quality_class AS ly_quality_class, -- quality_class from last year
        cs.quality_class AS ts_quality_class, -- quality_class from this year
        1915 AS current_year -- Set current year to 1915
    FROM
        last_season_scd ls
        FULL OUTER JOIN current_season_scd cs ON ls.actor_id = cs.actor_id
        AND ls.end_date + 1 = cs.current_year -- Join on actor_id and consecutive years
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
                        end_date + 1 -- Extend end_date by one year for continuous data
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                )
            ]
            WHEN did_change = 1 THEN ARRAY[
                CAST(
                    ROW(ly_quality_class, is_active_last_year, start_date, end_date) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                ),
                CAST(
                    ROW(
                        ts_quality_class,
                        is_active_this_year,
                        current_year,
                        current_year -- New entry for the current year with updated values
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                )
            ]
            WHEN did_change IS NULL THEN ARRAY[
                CAST(
                    ROW(
                        COALESCE(ly_quality_class, ts_quality_class), -- Use non-null quality_class
                        COALESCE(is_active_last_year, is_active_this_year), -- Use non-null is_active status
                        start_date,
                        end_date
                    ) AS ROW(
                        quality_class VARCHAR,
                        is_active BOOLEAN,
                        start_date INTEGER,
                        end_date INTEGER
                    )
                )
            ]
        END AS change_array -- Array of changes to be unnest
    FROM
        combined
)

-- The main query selects the unpacked arrays from 'changes' and inserts them into the target table.
SELECT
    actor,
    actor_id,
    arr.quality_class,
    arr.is_active,
	-- Here I took some liberity and coverted the year into date since in DDL query change is asked for start_date and end_date so I consider them as DATE type
    DATE(CONCAT(CAST(arr.start_date AS VARCHAR), '-01-01')) AS start_date, -- Convert start_date year to full date
    DATE(CONCAT(CAST(arr.end_date AS VARCHAR), '-12-31')) AS end_date, -- Convert end_date year to full date
    current_year
FROM
    changes
    CROSS JOIN UNNEST(change_array) AS arr -- Unnest the change array for insertion
