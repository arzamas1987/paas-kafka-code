"""
Chapter 13 — Action 06
Create / delete / reset Pizza lifecycle topics against the Docker-based Kafka cluster.

Usage examples:
  # create (default)
  export KAFKA_PROFILE=docker
  python -m chapter13.action06_topics_docker --create

  # delete then create (clean topics)
  export KAFKA_PROFILE=docker
  python -m chapter13.action06_topics_docker --reset

  # delete only
  export KAFKA_PROFILE=docker
  python -m chapter13.action06_topics_docker --delete

Optional overrides:
  export KAFKA_BOOTSTRAP_SERVERS="localhost:19092,localhost:19093,localhost:19094"
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Iterable

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from pizza_app.config import get_kafka_settings


def _admin(bootstrap_servers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def _topic_names() -> list[str]:
    s = get_kafka_settings()
    return [
        s.incoming_orders_topic,
        s.order_validated_topic,
        s.order_prepared_topic,
        s.order_baked_topic,
        s.order_ready_topic,  # keep: "order-ready-to-deliver"
    ]


def _wait_futures(action: str, futures: dict, timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    errors: list[str] = []

    for name, fut in futures.items():
        remaining = max(1, int(deadline - time.time()))
        try:
            fut.result(timeout=remaining)
        except Exception as e:  # noqa: BLE001
            errors.append(f"{action} failed for topic '{name}': {e}")

    if errors:
        raise RuntimeError("\n".join(errors))


def create_topics(
    admin: AdminClient,
    topics: Iterable[str],
    partitions: int,
    replication_factor: int,
    min_isr: int,
) -> None:
    new_topics = [
        NewTopic(
            topic=t,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config={
                # Keep it explicit for a lab: safe writes + predictable behaviour
                "min.insync.replicas": str(min_isr),
                "cleanup.policy": "delete",
            },
        )
        for t in topics
    ]

    futures = admin.create_topics(new_topics, request_timeout=20)
    # Note: if topic exists, Confluent client raises an exception per-topic.
    # We treat "already exists" as non-fatal to make the action repeatable.
    non_fatal_markers = ("TOPIC_ALREADY_EXISTS", "TopicExistsError", "already exists")

    errors: list[str] = []
    for t, fut in futures.items():
        try:
            fut.result()
            print(f"[OK] created: {t}")
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if any(m in msg for m in non_fatal_markers):
                print(f"[SKIP] exists:  {t}")
            else:
                errors.append(f"[ERR] create failed for '{t}': {e}")

    if errors:
        raise RuntimeError("\n".join(errors))


def delete_topics(admin: AdminClient, topics: Iterable[str]) -> None:
    futures = admin.delete_topics(list(topics), operation_timeout=30, request_timeout=20)

    non_fatal_markers = (
        "UNKNOWN_TOPIC_OR_PART",
        "UnknownTopicOrPartition",
        "does not exist",
        "Unknown topic",
    )

    errors: list[str] = []
    for t, fut in futures.items():
        try:
            fut.result()
            print(f"[OK] delete requested: {t}")
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if any(m in msg for m in non_fatal_markers):
                print(f"[SKIP] missing:        {t}")
            else:
                errors.append(f"[ERR] delete failed for '{t}': {e}")

    if errors:
        raise RuntimeError("\n".join(errors))

    # Deletion is async in Kafka. Give the cluster a moment.
    time.sleep(2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--create", action="store_true", help="Create lifecycle topics (default).")
    mode.add_argument("--delete", action="store_true", help="Delete lifecycle topics.")
    mode.add_argument("--reset", action="store_true", help="Delete then re-create lifecycle topics.")

    p.add_argument("--partitions", type=int, default=3, help="Number of partitions per topic.")
    p.add_argument("--rf", type=int, default=3, help="Replication factor (3 recommended for 3-broker lab).")
    p.add_argument("--min-isr", type=int, default=2, help="min.insync.replicas (2 recommended).")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    settings = get_kafka_settings()
    bootstrap = settings.bootstrap_servers

    topics = _topic_names()
    print(f"[INFO] profile={settings.profile} bootstrap={bootstrap}")
    print(f"[INFO] topics={topics}")

    admin = _admin(bootstrap)

    # Default behaviour = --create
    do_create = args.create or (not args.delete and not args.reset)
    do_delete = args.delete or args.reset

    try:
        if do_delete:
            print("[INFO] deleting topics...")
            delete_topics(admin, topics)

        if do_create:
            print("[INFO] creating topics...")
            create_topics(
                admin,
                topics,
                partitions=args.partitions,
                replication_factor=args.rf,
                min_isr=args.min_isr,
            )

        print("[DONE] action06 complete.")
        return 0

    except (KafkaException, RuntimeError) as e:
        print(f"[FAIL] {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
