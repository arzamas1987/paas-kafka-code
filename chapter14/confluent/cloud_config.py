from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient


@dataclass(frozen=True)
class CloudKafkaSettings:
    bootstrap_servers: str
    api_key: str
    api_secret: str

    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"

    @staticmethod
    def from_env() -> "CloudKafkaSettings":
        bs = os.getenv("PIZZA_CLOUD_BOOTSTRAP_SERVERS", "").strip()
        key = os.getenv("PIZZA_CLOUD_API_KEY", "").strip()
        secret = os.getenv("PIZZA_CLOUD_API_SECRET", "").strip()

        if not bs:
            raise ValueError("Missing env var: PIZZA_CLOUD_BOOTSTRAP_SERVERS (example: pkc-...:9092)")
        if bs.startswith("http://") or bs.startswith("https://") or ":443" in bs:
            raise ValueError(
                "PIZZA_CLOUD_BOOTSTRAP_SERVERS must be host:port for Kafka (example: pkc-...:9092). "
                "Do not use https:// or :443."
            )
        if not key:
            raise ValueError("Missing env var: PIZZA_CLOUD_API_KEY")
        if not secret:
            raise ValueError("Missing env var: PIZZA_CLOUD_API_SECRET")

        return CloudKafkaSettings(bs, key, secret)

    def common_conf(self) -> Dict[str, str]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanisms": self.sasl_mechanism,
            "sasl.username": self.api_key,
            "sasl.password": self.api_secret,
        }


def build_admin(settings: CloudKafkaSettings) -> AdminClient:
    return AdminClient(settings.common_conf())


def build_producer(settings: CloudKafkaSettings) -> Producer:
    conf = settings.common_conf()
    conf.update(
        {
            "client.id": "paas-ch14-producer",
            # keep acknowledgements strong but avoid PID handshake for now
            "acks": "all",
            "enable.idempotence": False,
            # sensible retries for cloud networks
            "retries": 10,
            "retry.backoff.ms": 250,
            "socket.timeout.ms": 60000,
            "request.timeout.ms": 60000,
        }
    )
    return Producer(conf)


def build_consumer(settings: CloudKafkaSettings, group_id: str, *, auto_offset_reset: str = "earliest") -> Consumer:
    conf = settings.common_conf()
    conf.update(
        {
            "client.id": "paas-ch14-consumer",
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        }
    )
    return Consumer(conf)
