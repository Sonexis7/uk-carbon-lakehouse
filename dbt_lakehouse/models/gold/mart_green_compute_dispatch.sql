{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * 
    FROM read_parquet('../lakehouse/gold/green_compute_decision_mart.parquet')
),

classified_dispatch AS (
    SELECT 
        region,
        forecast_gco2_kwh,
        carbon_index,
        renewable_share_perc,
        cleanliness_rank,
        compute_scheduling_guidance,
        CASE 
            WHEN forecast_gco2_kwh <= 80 THEN 'OPTIMAL'
            WHEN forecast_gco2_kwh <= 150 THEN 'STANDARD'
            ELSE 'DEFER'
        END AS dispatch_tier
    FROM source_data
)

SELECT * FROM classified_dispatch
