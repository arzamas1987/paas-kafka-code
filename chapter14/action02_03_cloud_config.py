from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient


def normalize_bootstrap_servers(raw: str) -> str:
    """
    Accept either:
      - pkc-xxx...:9092
      - pkc-xxx...:443
      - https://pkc-xxx...:443  (common copy/paste from UI)
    and return a value compatible with confluent-kafka.
    """
    s = raw.strip()
    if s.startswith("https://"):
        s = s[len("https://") :]
    if s.startswith("http://"):
        s = s[len("http://") :]
    return s


@dataclass(frozen=True)
class CloudKafkaSettings:
    bootstrap_servers: str
    api_key: str
    api_secret: str
    consumer_group: str

    @staticmethod
    def from_env() -> "CloudKafkaSettings":
        missing = []

        bs_raw = os.getenv("PIZZA_CLOUD_BOOTSTRAP_SERVERS")
        key = os.getenv("PIZZA_CLOUD_API_KEY")
        secret = os.getenv("PIZZA_CLOUD_API_SECRET")
        group = os.getenv("PIZZA_CLOUD_CONSUMER_GROUP", "ch14-action03-hello-cloud-demo")

        if not bs_raw:
            missing.append("PIZZA_CLOUD_BOOTSTRAP_SERVERS")
        if not key:
            missing.append("PIZZA_CLOUD_API_KEY")
        if not secret:
            missing.append("PIZZA_CLOUD_API_SECRET")

        if missing:
            raise RuntimeError(
                "Missing required environment variables: "
                + ", ".join(missing)
                + ". Use chapter14/action02_cloud.env.example as a template."
            )

        bs = normalize_bootstrap_servers(bs_raw)

        # Minimal sanity check: should look like host:port
        if "://" in bs or ":" not in bs:
            raise RuntimeError(
                "PIZZA_CLOUD_BOOTSTRAP_SERVERS must be host:port "
                "(for example pkc-xxxxx...:9092 or pkc-xxxxx...:443)."
            )

        return CloudKafkaSettings(
            bootstrap_servers=bs,
            api_key=key,
            api_secret=secret,
            consumer_group=group,
        )


def cloud_common_conf(settings: CloudKafkaSettings) -> Dict[str, str]:
    # Confluent Cloud baseline: SASL/PLAIN over TLS (SASL_SSL).
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": settings.api_key,
        "sasl.password": settings.api_secret,
        "ssl.endpoint.identification.algorithm": "https",
    }


def build_admin(settings: CloudKafkaSettings) -> AdminClient:
    return AdminClient(cloud_common_conf(settings))


def build_producer(settings: CloudKafkaSettings, client_id: str) -> Producer:
    conf = cloud_common_conf(settings)
    conf.update({"client.id": client_id})
    return Producer(conf)


def build_consumer(settings: CloudKafkaSettings, client_id: str, topics: list[str]) -> Consumer:
    conf = cloud_common_conf(settings)
    conf.update(
        {
            "client.id": client_id,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    c = Consumer(conf)
    c.subscribe(topics)
    return c
