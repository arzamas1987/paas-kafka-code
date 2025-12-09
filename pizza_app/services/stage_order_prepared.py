# pizza_app/services/stage_order_prepared.py

from __future__ import annotations

import logging
import signal
import threading

from pizza_app.kafka_helpers import get_bootstrap_servers
from pizza_app.services.lifecycle import (
    TOPIC_VALIDATED,
    TOPIC_PREPARED,
    run_stage_worker,
    transform_to_prepared,
)

logger = logging.getLogger(__name__)


def main() -> None:
    """
    Preparation service.

    Consumes OrderValidated events from order-validated and produces
    OrderPrepared events to order-prepared.
    """
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        logger.info("Received signal %s – stopping preparation service.", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    logger.info(
        "Preparation service starting with bootstrap servers=%s, input=%s, output=%s",
        bootstrap,
        TOPIC_VALIDATED,
        TOPIC_PREPARED,
    )

    run_stage_worker(
        name="prepare",
        input_topic=TOPIC_VALIDATED,
        output_topic=TOPIC_PREPARED,
        transform_fn=transform_to_prepared,
        bootstrap_servers=bootstrap,
        stop_event=stop_event,
        group_id="pizza-prepare-service",
    )

    logger.info("Preparation service stopped.")


if __name__ == "__main__":
    main()
