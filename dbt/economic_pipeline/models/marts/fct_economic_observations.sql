{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(country_code, observation_year, indicator_code)'
) }}

SELECT
    country_code,
    country_name,
    observation_year,
    indicator_code,
    indicator_name,
    indicator_value,
    source,
    updated_at
FROM {{ ref('stg_economic_observations') }}
WHERE indicator_value IS NOT NULL
