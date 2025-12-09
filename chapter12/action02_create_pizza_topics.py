# chapter12/action02_create_pizza_topics.py

from __future__ import annotations

import argparse
import logging
import os
from typing import List

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

from pizza_app.kafka_helpers import get_bootstrap_servers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_TOPICS: List[str] = [
    "incoming-orders",
    "order-validated",
    "order-prepared",
    "order-baked",
    "order-ready-for-pickup",
]


def build_new_topics(names: List[str]) -> List[NewTopic]:
    """
    Build NewTopic objects for the pizza lifecycle.

    Partitions and replication factor can be overridden by environment variables:
        PIZZA_TOPIC_PARTITIONS   (default: 1)
        PIZZA_TOPIC_REPLICATION  (default: 1)
    """
    partitions = int(os.getenv("PIZZA_TOPIC_PARTITIONS", "1"))
    replication = int(os.getenv("PIZZA_TOPIC_REPLICATION", "1"))

    topics: List[NewTopic] = []
    for name in names:
        topics.append(NewTopic(name=name, num_partitions=partitions, replication_factor=replication))
    return topics


def print_cli_commands(names: List[str], bootstrap: str) -> None:
    """
    Print equivalent kafka-topics.sh commands for educational purposes.
    """
    partitions = int(os.getenv("PIZZA_TOPIC_PARTITIONS", "1"))
    replication = int(os.getenv("PIZZA_TOPIC_REPLICATION", "1"))

    logger.info("Dry run; showing kafka-topics.sh commands only:\n")
    for name in names:
        cmd = (
            "bin/kafka-topics.sh --create "
            f"--topic {name} "
            f"--partitions {partitions} "
            f"--replication-factor {replication} "
            f"--bootstrap-server {bootstrap}"
        )
        print(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create the pizza lifecycle topics on the local Kafka cluster. "
            "By default this script calls the Kafka Admin API; "
            "use --dry-run-cli to print equivalent kafka-topics.sh commands."
        )
    )
    parser.add_argument(
        "--dry-run-cli",
        action="store_true",
        help="Do not create topics; only print kafka-topics.sh commands.",
    )
    return parser.parse_args()


def main() -> None:
    """
    Action – Create pizza lifecycle topics for the local Kafka cluster.

    Usage:
        python -m chapter12.action02_create_pizza_topics
        python -m chapter12.action02_create_pizza_topics --dry-run-cli
    """
    args = parse_args()

    bootstrap = get_bootstrap_servers()
    topic_names = DEFAULT_TOPICS

    if args.dry_run_cli:
        print_cli_commands(topic_names, bootstrap)
        return

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="pizza-topic-initialiser")

    topics = build_new_topics(topic_names)

    logger.info(
        "Creating topics %s on %s (partitions=%s, replication=%s)",
        topic_names,
        bootstrap,
        os.getenv("PIZZA_TOPIC_PARTITIONS", "1"),
        os.getenv("PIZZA_TOPIC_REPLICATION", "1"),
    )

    try:
        admin.create_topics(new_topics=topics, validate_only=False)
    except TopicAlreadyExistsError as exc:
        logger.warning("One or more topics already exist: %s", exc)
    except KafkaError as exc:
        logger.error("Topic creation failed: %s", exc)
        raise
    finally:
        admin.close()

    logger.info("Topic creation request sent. Use kafka-topics.sh --list to verify.")


if __name__ == "__main__":
    main()
