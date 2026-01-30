from __future__ import annotations

import argparse
import os
import random
import time
from typing import Any, Dict

from kafka.errors import KafkaError

from pizza_app.generator.order_factory import create_random_order
from pizza_app.kafka_helpers import create_json_producer, get_bootstrap_servers


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return int(v)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="chapter13.action07_produce_incoming_orders",
        description="Produce random OrderReceived events to the incoming topic (Docker profile).",
    )
    p.add_argument(
        "--count",
        type=int,
        default=None,
        help="How many orders to produce. Overrides PIZZA_ORDER_COUNT if provided.",
    )
    p.add_argument(
        "--interval-ms",
        type=int,
        default=None,
        help="Delay between orders in milliseconds. Overrides PIZZA_ORDER_INTERVAL_MS if provided.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="RNG seed for reproducible orders. Overrides PIZZA_RNG_SEED if provided.",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()

    bootstrap = get_bootstrap_servers()
    topic = os.getenv("PIZZA_TOPIC_INCOMING", "incoming-orders")

    default_count = _env_int("PIZZA_ORDER_COUNT", 20)
    default_interval_ms = _env_int("PIZZA_ORDER_INTERVAL_MS", 500)
    default_seed = _env_int("PIZZA_RNG_SEED", 42)

    count = args.count if args.count is not None else default_count
    interval_ms = args.interval_ms if args.interval_ms is not None else default_interval_ms
    seed = args.seed if args.seed is not None else default_seed

    if count <= 0:
        raise SystemExit("[FAIL] --count must be a positive integer.")
    if interval_ms < 0:
        raise SystemExit("[FAIL] --interval-ms must be >= 0.")

    rng = random.Random(seed)
    producer = create_json_producer(bootstrap)

    print(
        f"[INFO] Producing {count} random orders to topic='{topic}' "
        f"interval-ms={interval_ms} seed={seed}"
    )

    for i in range(1, count + 1):
        model = create_random_order(rng)
        payload: Dict[str, Any] = model.model_dump(mode="json")
        key = payload["order_id"]

        try:
            future = producer.send(topic, key=key, value=payload)
            meta = future.get(timeout=15.0)
        except KafkaError as exc:
            print(f"[FAIL] Send failed key={key!r} error={exc}")
            break

        print(f"[OK] #{i} key={key!r} topic={meta.topic} p={meta.partition} offset={meta.offset}")

        if interval_ms > 0:
            time.sleep(interval_ms / 1000.0)

    producer.flush()
    producer.close()
    print("[OK] Producer finished cleanly.")


if __name__ == "__main__":
    main()
