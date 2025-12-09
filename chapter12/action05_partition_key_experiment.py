# chapter12/action05_partition_key_experiment.py

from __future__ import annotations

import logging
import os
import time
from typing import Dict, Iterable

from kafka.errors import KafkaError

from pizza_app.kafka_helpers import create_json_producer, get_bootstrap_servers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC_INCOMING_ORDERS = os.getenv("PIZZA_TOPIC_INCOMING", "incoming-orders")


def build_payload(order_id: str, step: int) -> Dict[str, object]:
    ts = int(time.time())
    return {
        "order_id": order_id,
        "event_type": "OrderReceived",
        "order_type": "delivery",
        "status": f"experiment-step-{step}",
        "timestamp": ts,
    }


def experiment(order_ids: Iterable[str], repeats: int = 3) -> None:
    bootstrap = get_bootstrap_servers()
    producer = create_json_producer(bootstrap)

    logger.info(
        "Running partition experiment on topic '%s' with order_ids=%s",
        TOPIC_INCOMING_ORDERS,
        list(order_ids),
    )

    for step in range(1, repeats + 1):
        for order_id in order_ids:
            payload = build_payload(order_id, step)

            try:
                future = producer.send(TOPIC_INCOMING_ORDERS, key=order_id, value=payload)
                meta = future.get(timeout=10.0)
            except KafkaError as exc:
                logger.error("Send failed: %s", exc)
                producer.close()
                return

            logger.info(
                "order_id=%s step=%d → partition=%s offset=%s",
                order_id,
                step,
                meta.partition,
                meta.offset,
            )

    producer.flush()
    producer.close()
    logger.info("Partition experiment finished.")


def main() -> None:
    """
    Action – Show how keys map to partitions.

    Usage:
        python -m chapter12.action05_partition_key_experiment
    """
    # Use short IDs here so they are easy to spot in CLI consumer output.
    experiment(order_ids=["A0001", "A0002", "A0003"], repeats=3)


if __name__ == "__main__":
    main()
