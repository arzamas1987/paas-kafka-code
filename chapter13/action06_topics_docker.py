"""
Chapter 13 — Action 6
Create or delete the Pizza lifecycle topics against the Docker-based Kafka cluster.

Run:
  python -m chapter13.action06_topics_docker --create
  python -m chapter13.action06_topics_docker --delete
"""

from __future__ import annotations

import argparse
import sys
import time

try:
    from confluent_kafka.admin import AdminClient, NewTopic
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(
        "Missing dependency 'confluent-kafka'. Install it with:\n"
        "  python -m pip install -r requirements.txt\n"
        "or:\n"
        "  python -m pip install confluent-kafka\n"
    ) from e


# Reuse the app’s contract (topic names) without mixing chapter modules.
try:
    from pizza_app.config import get_kafka_settings  # type: ignore
except Exception as e:
    raise RuntimeError(
        "Could not import pizza_app.config.get_kafka_settings(). "
        "Run this from the repo root and ensure pizza_app/ is on the Python path."
    ) from e


def _admin_client() -> AdminClient:
    settings = get_kafka_settings()
    return AdminClient({"bootstrap.servers": settings.bootstrap_servers})


def _pizza_topic_names() -> list[str]:
    s = get_kafka_settings()
    # Keep your existing name: order-ready-to-deliver
    return [
        s.incoming_orders_topic,
        s.order_validated_topic,
        s.order_prepared_topic,
        s.order_baked_topic,
        s.order_ready_topic,
    ]


def _create_topics(admin: AdminClient) -> None:
    # Simple defaults for a 3-broker lab
    topics = [
        NewTopic(
            topic=name,
            num_partitions=3,
            replication_factor=3,
            config={"min.insync.replicas": "2"},
        )
        for name in _pizza_topic_names()
    ]

    futures = admin.create_topics(topics, request_timeout=15)

    for name, fut in futures.items():
        try:
            fut.result()
            print(f"[OK] created: {name}")
        except Exception as e:
            msg = str(e)
            if "TOPIC_ALREADY_EXISTS" in msg or "TopicExistsError" in msg:
                print(f"[SKIP] exists: {name}")
            else:
                print(f"[ERR] create failed: {name} -> {e}")
                raise


def _delete_topics(admin: AdminClient) -> None:
    names = _pizza_topic_names()
    futures = admin.delete_topics(names, operation_timeout=30, request_timeout=15)

    for name, fut in futures.items():
        try:
            fut.result()
            print(f"[OK] delete requested: {name}")
        except Exception as e:
            msg = str(e)
            if "UNKNOWN_TOPIC_OR_PART" in msg:
                print(f"[SKIP] not found: {name}")
            else:
                print(f"[ERR] delete failed: {name} -> {e}")
                raise

    # Deletion is async; give the lab a moment.
    time.sleep(2.0)
    print("[INFO] topic deletion is asynchronous; if you recreate immediately and it fails, wait a few seconds and retry.")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--create", action="store_true", help="Create lifecycle topics (idempotent).")
    g.add_argument("--delete", action="store_true", help="Delete lifecycle topics (lab reset).")
    args = p.parse_args(argv)

    settings = get_kafka_settings()
    print(f"[INFO] profile={settings.profile} bootstrap={settings.bootstrap_servers}")

    admin = _admin_client()

    if args.create:
        _create_topics(admin)
    else:
        _delete_topics(admin)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
