CREATE OR REPLACE TABLE sasiram410.actors (
    actor_id VARCHAR NOT NULL, -- Unique identifier for each actor
    actor VARCHAR, -- Name of the actor
    films ARRAY( -- Array of films the actor has participated in
        ROW(
            film VARCHAR, -- Name of the film
            votes INTEGER, -- Number of votes the film received
            rating DOUBLE, -- Rating of the film
            film_id VARCHAR -- Unique identifier for each film
        )
    ),
    quality_class VARCHAR, -- Classification of the actor's performance quality
    is_active BOOLEAN, -- Indicates whether the actor is currently active
    current_year INTEGER -- Year in which the data is current
)
WITH
(
    FORMAT = 'PARQUET', -- Data format for storage
    partitioning = ARRAY['current_year'] -- Partitioning key for optimization
)
