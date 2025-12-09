# pizza_app/services/lifecycle.py

from __future__ import annotations

import json
import logging
import os
import time
import threading
from typing import Any, Callable, Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from pizza_app.kafka_helpers import create_json_producer, get_bootstrap_servers

log_format = "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Topic names (can be overridden via environment variables)
# --------------------------------------------------------------------

TOPIC_INCOMING = os.getenv("PIZZA_TOPIC_INCOMING", "incoming-orders")
TOPIC_VALIDATED = os.getenv("PIZZA_TOPIC_VALIDATED", "order-validated")
TOPIC_PREPARED = os.getenv("PIZZA_TOPIC_PREPARED", "order-prepared")
TOPIC_BAKED = os.getenv("PIZZA_TOPIC_BAKED", "order-baked")
TOPIC_READY = os.getenv("PIZZA_TOPIC_READY", "order-ready-for-pickup")

# --------------------------------------------------------------------
# Domain event generation (reusing Chapter 10 if available)
# --------------------------------------------------------------------

try:
    # Preferred: explicit OrderReceived generator (if present in your repo)
    from pizza_app.generator.order_factory import (  # type: ignore[attr-defined]
        create_random_order_received,
    )
except Exception:  # pragma: no cover - defensive fallback
    create_random_order_received = None  # type: ignore[assignment]

try:
    # Older generic API used previously in stage_order_received.py
    from pizza_app.generator.order_factory import (  # type: ignore[attr-defined]
        create_random_order,
    )
except Exception:  # pragma: no cover - defensive
    create_random_order = None  # type: ignore[assignment]


def build_order_received() -> Dict[str, Any]:
    """
    Build a single OrderReceived-style event as a plain dict.

    Preference order:
      1) create_random_order_received() if available.
      2) create_random_order() if available.
      3) simple synthetic fallback.

    This keeps the function robust even if the generator API changes
    slightly between Chapter 10 / later refactors.
    """
    # 1) Explicit OrderReceived model
    if create_random_order_received is not None:
        model = create_random_order_received()
        if hasattr(model, "model_dump"):
            return model.model_dump()
        if hasattr(model, "dict"):
            return model.dict()  # type: ignore[no-any-return]
        return json.loads(model.json())

    # 2) Generic order model
    if create_random_order is not None:
        model = create_random_order()
        if hasattr(model, "model_dump"):
            return model.model_dump()
        if hasattr(model, "dict"):
            return model.dict()  # type: ignore[no-any-return]
        return json.loads(model.json())

    # 3) Last resort: minimal synthetic structure
    ts = int(time.time())
    return {
        "order_id": f"A{ts}",
        "event_type": "OrderReceived",
        "order_type": "delivery",
        "status": "received",
        "timestamp": ts,
    }


# --------------------------------------------------------------------
# Kafka consumer factory
# --------------------------------------------------------------------

def create_json_consumer(
    topic: str,
    *,
    group_id: str,
    bootstrap_servers: str,
    auto_offset_reset: str = "earliest",
) -> KafkaConsumer:
    """
    Create a simple JSON KafkaConsumer for a single topic.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=auto_offset_reset,
        key_deserializer=lambda m: m.decode("utf-8") if m is not None else None,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


# --------------------------------------------------------------------
# Shared producer loop for OrderReceived events
# --------------------------------------------------------------------

def run_order_received_producer(
    *,
    bootstrap_servers: Optional[str] = None,
    stop_event: Optional[threading.Event] = None,
    interval_sec: float = 1.0,
) -> None:
    """
    Regularly send new OrderReceived events into the incoming-orders topic.

    This function blocks until stop_event is set (if provided) or until a
    KeyboardInterrupt in a top-level script.
    """
    if bootstrap_servers is None:
        bootstrap_servers = get_bootstrap_servers()

    if stop_event is None:
        stop_event = threading.Event()

    producer = create_json_producer(bootstrap_servers)
    logger.info(
        "[producer] Sending OrderReceived events to '%s' every %.1f seconds.",
        TOPIC_INCOMING,
        interval_sec,
    )

    sent = 0
    try:
        while not stop_event.is_set():
            payload = build_order_received()
            key = payload.get("order_id")

            try:
                future = producer.send(TOPIC_INCOMING, key=key, value=payload)
                meta = future.get(timeout=10.0)
            except KafkaError as exc:  # pragma: no cover - network
                logger.error("[producer] Failed to send event key=%r: %s", key, exc)
                stop_event.set()
                break

            sent += 1
            logger.info(
                "[producer] #%d key=%r → %s (p=%s, offset=%s)",
                sent,
                key,
                TOPIC_INCOMING,
                meta.partition,
                meta.offset,
            )

            time.sleep(interval_sec)
    finally:
        logger.info("[producer] Flushing and closing producer.")
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        logger.info("[producer] Producer stopped cleanly.")


# --------------------------------------------------------------------
# Generic stage worker
# --------------------------------------------------------------------

TransformFn = Callable[[Dict[str, Any]], Dict[str, Any]]


def run_stage_worker(
    *,
    name: str,
    input_topic: str,
    output_topic: Optional[str],
    transform_fn: TransformFn,
    bootstrap_servers: Optional[str] = None,
    stop_event: Optional[threading.Event] = None,
    group_id: Optional[str] = None,
) -> None:
    """
    Generic worker that consumes from `input_topic`, applies `transform_fn`
    and optionally produces to `output_topic`.

    Blocks until stop_event is set (if provided) or until the process is
    interrupted in a top-level script.
    """
    if bootstrap_servers is None:
        bootstrap_servers = get_bootstrap_servers()

    if stop_event is None:
        stop_event = threading.Event()

    if group_id is None:
        group_id = f"pizza-{name}-service"

    logger.info(
        "[%s] Starting stage worker – input=%s output=%s group_id=%s",
        name,
        input_topic,
        output_topic or "<none>",
        group_id,
    )

    consumer = create_json_consumer(
        input_topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
    )

    producer = None
    if output_topic is not None:
        producer = create_json_producer(bootstrap_servers)

    try:
        for msg in consumer:
            if stop_event.is_set():
                logger.info("[%s] Stop flag set – exiting worker loop.", name)
                break

            key = msg.key
            payload = msg.value

            try:
                out_payload = transform_fn(payload)
            except Exception as exc:  # pragma: no cover - defensive
                logger.error(
                    "[%s] Failed to transform message key=%r offset=%s: %s",
                    name,
                    key,
                    msg.offset,
                    exc,
                )
                continue

            if output_topic is None or producer is None:
                # Last stage or log-only stage
                logger.info(
                    "[%s] Consumed final event key=%r from topic=%s partition=%s offset=%s",
                    name,
                    key,
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )
                continue

            try:
                future = producer.send(output_topic, key=key, value=out_payload)
                meta = future.get(timeout=10.0)
            except KafkaError as exc:  # pragma: no cover - network
                logger.error("[%s] Failed to forward event key=%r: %s", name, key, exc)
                stop_event.set()
                break

            logger.info(
                "[%s] key=%r %s → %s (p=%s, offset=%s)",
                name,
                key,
                input_topic,
                output_topic,
                meta.partition,
                meta.offset,
            )
    finally:
        logger.info("[%s] Closing consumer.", name)
        try:
            consumer.close()
        except Exception:
            pass

        if producer is not None:
            logger.info("[%s] Flushing and closing producer.", name)
            try:
                producer.flush()
                producer.close()
            except Exception:
                pass

        logger.info("[%s] Worker stopped cleanly.")


# --------------------------------------------------------------------
# Stage-specific transformation functions
# --------------------------------------------------------------------

def transform_to_validated(event: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(event)
    out["event_type"] = "OrderValidated"
    out["status"] = "validated"
    out["validated_at"] = time.time()
    return out


def transform_to_prepared(event: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(event)
    out["event_type"] = "OrderPrepared"
    out["status"] = "prepared"
    out["prepared_at"] = time.time()
    return out


def transform_to_baked(event: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(event)
    out["event_type"] = "OrderBaked"
    out["status"] = "baked"
    out["baked_at"] = time.time()
    return out


def transform_to_ready(event: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(event)
    out["event_type"] = "OrderReadyForPickup"
    out["status"] = "ready-for-pickup"
    out["ready_at"] = time.time()
    return out
