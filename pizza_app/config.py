from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class KafkaSettings:
    profile: str
    bootstrap_servers: str
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    incoming_orders_topic: str = "incoming-orders"
    order_validated_topic: str = "order-validated"
    order_prepared_topic: str = "order-prepared"
    order_baked_topic: str = "order-baked"
    order_ready_topic: str = "order-ready-to-deliver"


def _load_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    return value


def get_kafka_settings() -> KafkaSettings:
    profile = _load_env("KAFKA_PROFILE", "local") or "local"

    if profile == "local":
        bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092"
        return KafkaSettings(profile=profile, bootstrap_servers=bootstrap)

    if profile == "docker":
        bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") or "kafka:9092"
        return KafkaSettings(profile=profile, bootstrap_servers=bootstrap)

    if profile == "cloud":
        bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "")
        if not bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS must be set for cloud profile")

        return KafkaSettings(
            profile=profile,
            bootstrap_servers=bootstrap,
            security_protocol=_load_env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=_load_env("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_username=_load_env("KAFKA_SASL_USERNAME"),
            sasl_password=_load_env("KAFKA_SASL_PASSWORD"),
        )

    bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092"
    return KafkaSettings(profile=profile, bootstrap_servers=bootstrap)


kafka_settings = get_kafka_settings()
