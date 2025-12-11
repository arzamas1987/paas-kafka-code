# pizza_app/services/stage_order_received.py

from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any, Dict

from kafka.errors import KafkaError

from pizza_app.kafka_helpers import create_json_producer, get_bootstrap_servers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC_INCOMING_ORDERS = os.getenv("PIZZA_TOPIC_INCOMING", "incoming-orders")

# --- Domain payload generation ------------------------------------------------

try:
    # Reuse the Chapter 10 generator if available
    from pizza_app.generator.order_factory import create_random_order_received  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - defensive fallback
    create_random_order_received = None  # type: ignore[assignment]


def build_domain_event() -> Dict[str, Any]:
    """
    Build a single OrderReceived-style event as a plain dict.

    If the Chapter 10 generator is available, we reuse it.
    Otherwise we fall back to a simple synthetic object.
    """
    if create_random_order_received is not None:
        event_model = create_random_order_received()
        # pydantic v2: model_dump(); v1: dict()
        if hasattr(event_model, "model_dump"):
            return event_model.model_dump()
        if hasattr(event_model, "dict"):
            return event_model.dict()  # type: ignore[no-any-return]
        # Worst-case, try JSON roundtrip
        return json.loads(event_model.json())

    # Fallback: very simple schema – safe even if the Pydantic models change.
    ts = int(time.time())
    return {
        "order_id": f"A{ts}",
        "event_type": "OrderReceived",
        "order_type": "delivery",
        "status": "received",
        "timestamp": ts,
    }


# --- Main loop -----------------------------------------------------------------


_running = True


def _handle_sigint(signum, frame) -> None:  # type: ignore[override]
    global _running
    logger.info("Received signal %s – shutting down OrderReceived producer.", signum)
    _running = False


def main() -> None:
    """
    OrderReceived producer service.

    This service acts as the *initiator* of the pizza lifecycle:
    every event it sends to `incoming-orders` starts a cascade
    through the validation, preparation, baking and ready-for-pickup services.

    It sends 1 event per second until interrupted.
    """
    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    bootstrap = get_bootstrap_servers()
    logger.info(
        "OrderReceived producer starting with bootstrap servers=%s, topic=%s",
        bootstrap,
        TOPIC_INCOMING_ORDERS,
    )

    producer = create_json_producer(bootstrap)

    sent = 0
    while _running:
        payload = build_domain_event()
        key = payload.get("order_id")

        try:
            future = producer.send(TOPIC_INCOMING_ORDERS, key=key, value=payload)
            record_metadata = future.get(timeout=10.0)
        except KafkaError as exc:
            logger.error("Failed to send record: %s", exc)
            break

        sent += 1
        logger.info(
            "[OrderReceived] Sent record #%d → topic=%s partition=%s offset=%s key=%r",
            sent,
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
            key,
        )

        time.sleep(1.0)

    logger.info("Flushing producer before exit.")
    producer.flush()
    producer.close()
    logger.info("OrderReceived producer stopped cleanly.")


if __name__ == "__main__":
    main()
