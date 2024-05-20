CREATE OR REPLACE TABLE sasiram410.actors_history_scd (
    actor VARCHAR, -- Name of the actor
    actor_id VARCHAR, -- Unique identifier for each actor
    quality_class VARCHAR, -- Classification of the actor's performance quality
    is_active BOOLEAN, -- Indicates whether the actor is currently active
    start_date DATE, -- Start date of the actor's history record
    end_date DATE, -- End date of the actor's history record
    current_year INTEGER -- Year in which the data is current
)
WITH
(
    FORMAT = 'PARQUET', -- Data format for storage
    partitioning = ARRAY['current_year'] -- Partitioning key for optimization
)
