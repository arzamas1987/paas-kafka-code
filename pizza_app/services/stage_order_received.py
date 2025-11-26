from __future__ import annotations

import os
import random
import time
from typing import Iterable

from kafka import KafkaProducer

from pizza_app.config import kafka_settings
from pizza_app.generator.order_factory import create_random_order


def _iter_orders(rng: random.Random, max_orders: int | None) -> Iterable[str]:
    count = 0
    while True:
        event = create_random_order(rng)
        yield event.model_dump_json()
        count += 1
        if max_orders is not None and count >= max_orders:
            break


def run_simulation(max_orders: int = 10) -> None:
    seed = os.getenv("PIZZA_RANDOM_SEED")
    rng = random.Random(seed if seed else None)
    for raw in _iter_orders(rng, max_orders):
        print(raw)


def run_kafka(max_orders: int = 10, delay_seconds: float = 1.0) -> None:
    seed = os.getenv("PIZZA_RANDOM_SEED")
    rng = random.Random(seed if seed else None)

    producer = KafkaProducer(
        bootstrap_servers=kafka_settings.bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    topic = kafka_settings.incoming_orders_topic

    for raw in _iter_orders(rng, max_orders):
        producer.send(topic, raw)
        print(f"[producer] sent: {raw}")
        time.sleep(delay_seconds)

    producer.flush()


if __name__ == "__main__":
    mode = os.getenv("RUN_MODE", "simulation")
    if mode == "kafka":
        run_kafka()
    else:
        run_simulation()
