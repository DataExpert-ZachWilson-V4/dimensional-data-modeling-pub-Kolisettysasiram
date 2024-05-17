-- This query inserts data into the sasiram410.actors table based on the data from the previous year (1913) and the current year (1914).
INSERT INTO sasiram410.actors (actor_id, actor, films, quality_class, is_active, current_year)
-- CTE 'last_year_data' selects data from the previous year (1913) from the sasiram410.actors table.
WITH last_year_data AS (
    SELECT 
        actor_id,
        actor,
        films,
        quality_class,
        is_active,
        current_year
    FROM 
        sasiram410.actors
    WHERE
        current_year = 1913 -- Get data for the previous year from the actors table
),
-- CTE 'current_year_data' selects data for the current year (1914) from another table called "bootcamp.actor_films" which contains film data for actors
current_year_data AS (
    SELECT
    *
    FROM 
        bootcamp.actor_films
    WHERE
        year = 1914  --Get data for the previous year from the actors table
),
-- CTE 'current_year_data_agg' aggregates the data for the current year, calculating quality_class and is_active based on rating and film count.
current_year_data_agg AS (
    SELECT 
        actor_id,
        actor,
        year,
        ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
        CASE 
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
            WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        COUNT(*) > 0 AS is_active -- To find out whether actor is active or not we can count the number of movies he acted on that year
        FROM current_year_data 
        GROUP BY 
        actor_id, actor,year
        
)
-- Main query selects the combined data from the previous year and the current year, handling cases where data may be missing for either year.
SELECT 
	COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
	COALESCE(ls.actor, ts.actor) AS actor,
	CASE
			WHEN ts.films IS NULL THEN ls.films
			WHEN ts.films IS NOT NULL AND ls.films IS NULL THEN ts.films
			WHEN ts.films IS NOT NULL AND ls.films IS NOT NULL THEN ls.films || ts.films
	END AS films,  
	COALESCE(ts.quality_class,ls.quality_class) AS quality_class,
	COALESCE(ts.is_active, FALSE) AS is_active,
	COALESCE(ts.year , ls.current_year+1) AS current_year  
FROM
  last_year_data ls
  FULL OUTER JOIN current_year_data_agg ts ON ls.actor_id= ts.actor_id
