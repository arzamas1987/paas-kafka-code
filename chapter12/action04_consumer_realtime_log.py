# chapter12/action04_consumer_realtime_log.py

from __future__ import annotations

import logging
import os
import signal

from pizza_app.kafka_helpers import create_json_consumer, format_record, get_bootstrap_servers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC_INCOMING_ORDERS = os.getenv("PIZZA_TOPIC_INCOMING", "incoming-orders")
GROUP_ID = os.getenv("PIZZA_CONSUMER_GROUP", "pizza-python-consumer")


_running = True


def _handle_sigint(signum, frame) -> None:  # type: ignore[override]
    global _running
    logger.info("Signal %s received – stopping consumer loop.", signum)
    _running = False


def main() -> None:
    """
    Action – Python consumer with realtime logs.

    Usage:
        python -m chapter12.action04_consumer_realtime_log
    """
    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    bootstrap = get_bootstrap_servers()
    consumer = create_json_consumer(
        [TOPIC_INCOMING_ORDERS],
        group_id=GROUP_ID,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    logger.info(
        "Consuming from topic '%s' as group_id='%s'. Ctrl+C to stop.",
        TOPIC_INCOMING_ORDERS,
        GROUP_ID,
    )

    while _running:
        for msg in consumer.poll(timeout_ms=1000).values():
            for record in msg:
                logger.info("Received %s", format_record(record))

    logger.info("Closing consumer.")
    consumer.close()


if __name__ == "__main__":
    main()
