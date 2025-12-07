# chapter12/action05_consumer_group_debug.py

from __future__ import annotations

import argparse
import logging
import os
import signal
from typing import Optional

from pizza_app.kafka_helpers import create_json_consumer, format_record, get_bootstrap_servers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC_INCOMING_ORDERS = os.getenv("PIZZA_TOPIC_INCOMING", "incoming-orders")

_running = True


def _handle_sigint(signum, frame) -> None:  # type: ignore[override]
    global _running
    logger.info("Signal %s received – stopping consumer group debug loop.", signum)
    _running = False


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consumer-group debug tool for the Pizza-as-a-Service topics. "
            "Use it to observe committed offsets and lag while reading."
        )
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("PIZZA_DEBUG_GROUP", "pizza-debug-group"),
        help="Kafka consumer group id (default: %(default)s)",
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        help="Disable automatic commits (process-only mode).",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start reading from the earliest offset instead of latest.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    """
    Action 12.4 – Consumer groups & offsets (debug-style consumer).

    Usage:
        python -m chapter12.action05_consumer_group_debug --group-id pizza-debug --from-beginning
    """
    args = parse_args(argv)

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    enable_auto_commit = not args.no_commit

    logger.info(
        "Starting debug consumer – topic='%s', group_id='%s', auto_offset_reset=%s, auto_commit=%s",
        TOPIC_INCOMING_ORDERS,
        args.group_id,
        auto_offset_reset,
        enable_auto_commit,
    )

    consumer = create_json_consumer(
        [TOPIC_INCOMING_ORDERS],
        group_id=args.group_id,
        bootstrap_servers=get_bootstrap_servers(),
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
    )

    while _running:
        for msgs in consumer.poll(timeout_ms=1000).values():
            for record in msgs:
                logger.info("Record: %s", format_record(record))

        if not enable_auto_commit:
            # For interactive experiments it is handy to commit manually from time to time.
            consumer.commit()
            logger.debug("Offsets committed manually.")

    logger.info("Closing consumer.")
    consumer.close()


if __name__ == "__main__":
    main()
