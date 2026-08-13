{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(country_code, indicator_code, observation_year)'
) }}

WITH ranked AS (

    SELECT
        id,
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        observation_year,
        indicator_value,
        source,
        created_at,
        updated_at,

        row_number() OVER (
            PARTITION BY
                id
            ORDER BY
                updated_at DESC
        ) AS rn

    FROM {{ source('raw', 'economic_observations') }}

    WHERE country_code IS NOT NULL
      AND indicator_code IS NOT NULL
      AND observation_year IS NOT NULL
)

SELECT
    id,
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    observation_year,
    indicator_value,
    source,
    created_at,
    updated_at

FROM ranked

WHERE rn = 1
