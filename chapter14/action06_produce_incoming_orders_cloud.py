from __future__ import annotations

import argparse
import json
import random
import time
from typing import Any, Dict

from chapter14.confluent.cloud_config import CloudKafkaSettings, build_producer

from pizza_app.generator.order_factory import create_random_order


TOPIC_INCOMING = "incoming-orders"


def _extract_order_id(order_obj: Any, fallback: str) -> str:
    # create_random_order returns a Pydantic model in your repo.
    # Try common Pydantic access patterns without guessing the exact attribute shape.
    if hasattr(order_obj, "order_id"):
        val = getattr(order_obj, "order_id")
        if val is not None:
            return str(val)

    if hasattr(order_obj, "model_dump"):
        d = order_obj.model_dump()
        if isinstance(d, dict) and "order_id" in d and d["order_id"] is not None:
            return str(d["order_id"])

    if hasattr(order_obj, "model_dump_json"):
        try:
            d = json.loads(order_obj.model_dump_json())
            if isinstance(d, dict) and "order_id" in d and d["order_id"] is not None:
                return str(d["order_id"])
        except Exception:
            pass

    return fallback


def main() -> int:
    p = argparse.ArgumentParser(description="Produce random incoming orders to Confluent Cloud.")
    p.add_argument("--count", type=int, default=10)
    p.add_argument("--interval-ms", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    settings = CloudKafkaSettings.from_env()
    producer = build_producer(settings)

    rng = random.Random(args.seed)

    delivered = 0

    def on_delivery(err, msg) -> None:
        nonlocal delivered
        if err is not None:
            print(f"[FAIL] delivery_error={err}")
            return
        delivered += 1
        key = (msg.key() or b"").decode("utf-8", errors="replace")
        print(f"[OK] delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key='{key}'")

    print(f"[INFO] Producing {args.count} random orders to topic='{TOPIC_INCOMING}'")
    for i in range(args.count):
        order = create_random_order(rng)

        # Serialize to JSON bytes (Pydantic model)
        payload_json = order.model_dump_json()

        order_id = _extract_order_id(order, fallback=f"order-{i}")

        producer.produce(
            topic=TOPIC_INCOMING,
            key=order_id.encode("utf-8"),
            value=payload_json.encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        time.sleep(args.interval_ms / 1000.0)

    producer.flush(20)
    print(f"[OK] done produced={args.count} delivered={delivered}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
