from __future__ import annotations

import os
from dataclasses import dataclass

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient


@dataclass(frozen=True)
class CloudKafkaSettings:
    bootstrap_servers: str
    api_key: str
    api_secret: str

    @staticmethod
    def from_env() -> "CloudKafkaSettings":
        bootstrap = os.getenv("PIZZA_CLOUD_BOOTSTRAP_SERVERS", "").strip()
        api_key = os.getenv("PIZZA_CLOUD_API_KEY", "").strip()
        api_secret = os.getenv("PIZZA_CLOUD_API_SECRET", "").strip()

        missing = [k for k, v in {
            "PIZZA_CLOUD_BOOTSTRAP_SERVERS": bootstrap,
            "PIZZA_CLOUD_API_KEY": api_key,
            "PIZZA_CLOUD_API_SECRET": api_secret,
        }.items() if not v]

        if missing:
            raise SystemExit(
                "Missing required environment variables: "
                + ", ".join(missing)
                + ". Did you `source chapter14/action02_cloud.env`?"
            )

        return CloudKafkaSettings(
            bootstrap_servers=bootstrap,
            api_key=api_key,
            api_secret=api_secret,
        )

    def base_client_config(self) -> dict:
        # Confluent Cloud defaults: TLS + SASL/PLAIN
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.api_key,
            "sasl.password": self.api_secret,
            # Keep logs readable in a book/lab setting
            "client.id": "paas-ch14",
        }


def build_admin(settings: CloudKafkaSettings) -> AdminClient:
    return AdminClient(settings.base_client_config())


def build_producer(settings: CloudKafkaSettings) -> Producer:
    cfg = settings.base_client_config()
    # Delivery reports are your “observable facts”
    cfg.update({
        "enable.idempotence": True,
        "acks": "all",
    })
    return Producer(cfg)


def build_consumer(settings: CloudKafkaSettings, group_id: str, *, auto_offset_reset: str = "earliest") -> Consumer:
    cfg = settings.base_client_config()
    cfg.update({
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": True,
    })
    return Consumer(cfg)
