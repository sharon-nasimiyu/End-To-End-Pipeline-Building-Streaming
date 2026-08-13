CREATE TABLE IF NOT EXISTS economic_observations (
    id BIGSERIAL PRIMARY KEY,

    country_code VARCHAR(10) NOT NULL,
    country_name VARCHAR(150) NOT NULL,

    indicator_code VARCHAR(50) NOT NULL,
    indicator_name VARCHAR(255) NOT NULL,

    observation_year INTEGER NOT NULL,
    indicator_value DOUBLE PRECISION,

    source VARCHAR(100) NOT NULL DEFAULT 'world_bank',

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_economic_observation
        UNIQUE (
            country_code,
            indicator_code,
            observation_year
        )
);

CREATE INDEX IF NOT EXISTS idx_economic_country
    ON economic_observations(country_code);

CREATE INDEX IF NOT EXISTS idx_economic_indicator
    ON economic_observations(indicator_code);

CREATE INDEX IF NOT EXISTS idx_economic_year
    ON economic_observations(observation_year);

CREATE INDEX IF NOT EXISTS idx_economic_country_indicator_year
    ON economic_observations(
        country_code,
        indicator_code,
        observation_year
    );
