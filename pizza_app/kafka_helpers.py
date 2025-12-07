# pizza_app/kafka_helpers.py

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Iterable, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


DEFAULT_BOOTSTRAP = "localhost:9092"


def get_bootstrap_servers() -> str:
    """
    Resolve the Kafka bootstrap servers to use for local development.

    Environment variables:
        KAFKA_BOOTSTRAP_SERVERS:   Comma-separated host:port list (optional).
        KAFKA_PROFILE:             Used only for logging to show which profile is active.
    """
    profile = os.getenv("KAFKA_PROFILE", "local")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)

    logger.info("Using KAFKA_PROFILE=%s, bootstrap servers=%s", profile, bootstrap)
    return bootstrap


def create_json_producer(bootstrap_servers: Optional[str] = None) -> KafkaProducer:
    """
    Create a KafkaProducer that sends JSON values and string keys.
    """
    bootstrap = bootstrap_servers or get_bootstrap_servers()

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: None
        if k is None
        else (k if isinstance(k, bytes) else str(k).encode("utf-8")),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def create_json_consumer(
    topics: Iterable[str],
    *,
    group_id: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = True,
    extra_config: Optional[Dict[str, Any]] = None,
) -> KafkaConsumer:
    """
    Create a KafkaConsumer that expects JSON values and string keys.
    """
    bootstrap = bootstrap_servers or get_bootstrap_servers()

    config: Dict[str, Any] = {
        "bootstrap_servers": bootstrap,
        "group_id": group_id,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "key_deserializer": lambda b: None if b is None else b.decode("utf-8"),
        "value_deserializer": lambda b: json.loads(b.decode("utf-8")),
    }

    if extra_config:
        config.update(extra_config)

    consumer = KafkaConsumer(**config)
    consumer.subscribe(list(topics))
    return consumer


def check_connection(bootstrap_servers: Optional[str] = None) -> bool:
    """
    Try to connect to Kafka by listing topics once.

    This is intentionally simple and used in the 'connection check' action.
    """
    bootstrap = bootstrap_servers or get_bootstrap_servers()
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            request_timeout_ms=3000,
            metadata_max_age_ms=3000,
        )
        topics = consumer.topics()
        logger.info("Connected to Kafka broker(s) %s; %d topics visible.", bootstrap, len(topics))
        consumer.close()
        return True
    except NoBrokersAvailable as exc:
        logger.error("No Kafka broker available at %s (%s)", bootstrap, exc)
        return False


def format_record(msg) -> str:
    """
    Pretty-print a Kafka message object from kafka-python.

    Works for JSON payloads; values that are not dicts are rendered as-is.
    """
    key = getattr(msg, "key", None)
    partition = getattr(msg, "partition", "?")
    offset = getattr(msg, "offset", "?")
    value = getattr(msg, "value", None)

    if isinstance(value, dict):
        value_str = json.dumps(value, ensure_ascii=False)
    else:
        value_str = repr(value)

    return f"partition={partition} offset={offset} key={key!r} value={value_str}"
