from __future__ import annotations

import os

from kafka import KafkaConsumer

from pizza_app.config import kafka_settings


def run_consumer() -> None:
    topic = os.getenv("PIZZA_CONSUMER_TOPIC", kafka_settings.incoming_orders_topic)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_settings.bootstrap_servers,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=os.getenv("PIZZA_CONSUMER_GROUP", "pizza-debug-consumer"),
    )

    print(f"[consumer] listening on {topic}")
    for msg in consumer:
        print(msg.value)


if __name__ == "__main__":
    run_consumer()
