import logging
import os
from typing import Any

import psycopg2
import requests
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2/country"

INDICATORS = {
    "NY.GNP.PCAP.CD": "GNI per capita (current US$)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SP.POP.TOTL": "Population, total",
}

COUNTRIES = [
    "KEN",
    "UGA",
    "TZA",
    "RWA",
    "ZAF",
]


def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "assessment"),
        user=os.getenv("POSTGRES_USER", "assessment_user"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def fetch_indicator(
    country_code: str,
    indicator_code: str,
) -> list[dict[str, Any]]:
    url = (
        f"{WORLD_BANK_BASE_URL}/"
        f"{country_code}/indicator/{indicator_code}"
    )

    params = {
        "format": "json",
        "per_page": 100,
    }

    logger.info(
        "Fetching %s for %s",
        indicator_code,
        country_code,
    )

    response = requests.get(
        url,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    if len(payload) < 2 or payload[1] is None:
        return []

    return payload[1]


def upsert_observations(
    connection,
    observations: list[dict[str, Any]],
) -> int:

    if not observations:
        return 0

    query = """
        INSERT INTO economic_observations (
            country_code,
            country_name,
            indicator_code,
            indicator_name,
            observation_year,
            indicator_value,
            source
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            'world_bank'
        )
        ON CONFLICT (
            country_code,
            indicator_code,
            observation_year
        )
        DO UPDATE SET
            country_name = EXCLUDED.country_name,
            indicator_name = EXCLUDED.indicator_name,
            indicator_value = EXCLUDED.indicator_value,
            updated_at = CURRENT_TIMESTAMP
    """

    inserted = 0

    with connection.cursor() as cursor:

        for observation in observations:

            year = observation.get("date")
            value = observation.get("value")

            if not year:
                continue

            cursor.execute(
                query,
                (
                    observation["countryiso3code"],
                    observation["country"]["value"],
                    observation["indicator"]["id"],
                    observation["indicator"]["value"],
                    int(year),
                    value,
                ),
            )

            inserted += 1

    connection.commit()

    return inserted


def main():

    logger.info("Starting World Bank ingestion")

    connection = get_postgres_connection()

    total_records = 0

    try:

        for country in COUNTRIES:

            for indicator_code in INDICATORS:

                try:

                    observations = fetch_indicator(
                        country,
                        indicator_code,
                    )

                    count = upsert_observations(
                        connection,
                        observations,
                    )

                    total_records += count

                    logger.info(
                        "Loaded %s records for %s / %s",
                        count,
                        country,
                        indicator_code,
                    )

                except requests.RequestException as exc:

                    logger.error(
                        "API request failed for %s / %s: %s",
                        country,
                        indicator_code,
                        exc,
                    )

                except Exception:

                    logger.exception(
                        "Failed processing %s / %s",
                        country,
                        indicator_code,
                    )

    finally:

        connection.close()

    logger.info(
        "World Bank ingestion completed. Records processed: %s",
        total_records,
    )


if __name__ == "__main__":
    main()
