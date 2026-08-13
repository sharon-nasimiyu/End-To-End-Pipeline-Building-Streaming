import json

from kafka import KafkaConsumer
import clickhouse_connect


KAFKA_TOPIC = "cdc.public.economic_observations"


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="clickhouse-economic-observations-v2",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="clickhouse_password",
    database="analytics",
)


def insert_record(record, kafka_partition, kafka_offset):

    after = record.get("after")

    # Ignore DELETE events for now because the raw table
    # is currently designed to store the latest row payload.
    if not after:
        return

    source = record.get("source", {})

    event_ts_ms = source.get("ts_ms", 0)
    operation = record.get("op", "")

    client.insert(
        "economic_observations",
        [[
            after["id"],
            after["country_code"],
            after["country_name"],
            after["indicator_code"],
            after["indicator_name"],
            after["observation_year"],
            after["indicator_value"],
            after["source"],
            after["created_at"],
            after["updated_at"],
            operation,
            event_ts_ms,
            kafka_partition,
            kafka_offset,
        ]],
        column_names=[
            "id",
            "country_code",
            "country_name",
            "indicator_code",
            "indicator_name",
            "observation_year",
            "indicator_value",
            "source",
            "created_at",
            "updated_at",
            "cdc_op",
            "cdc_event_ts_ms",
            "kafka_partition",
            "kafka_offset",
        ],
    )


print(f"Listening to Kafka topic: {KAFKA_TOPIC}")


for message in consumer:

    try:

        record = message.value

        insert_record(
            record,
            message.partition,
            message.offset,
        )

        print(
            f"Inserted CDC event | "
            f"op={record.get('op')} | "
            f"partition={message.partition} | "
            f"offset={message.offset}"
        )

    except Exception as e:

        print(f"Error processing message: {e}")
