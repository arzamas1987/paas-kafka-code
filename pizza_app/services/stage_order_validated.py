# pizza_app/services/stage_order_validated.py

from __future__ import annotations

import logging
import signal
import threading

from pizza_app.kafka_helpers import get_bootstrap_servers
from pizza_app.services.lifecycle import (
    TOPIC_INCOMING,
    TOPIC_VALIDATED,
    run_stage_worker,
    transform_to_validated,
)

logger = logging.getLogger(__name__)


def main() -> None:
    """
    Validation service.

    Consumes OrderReceived events from incoming-orders and produces
    OrderValidated events to order-validated.
    """
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        logger.info("Received signal %s – stopping validation service.", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    logger.info(
        "Validation service starting with bootstrap servers=%s, input=%s, output=%s",
        bootstrap,
        TOPIC_INCOMING,
        TOPIC_VALIDATED,
    )

    run_stage_worker(
        name="validate",
        input_topic=TOPIC_INCOMING,
        output_topic=TOPIC_VALIDATED,
        transform_fn=transform_to_validated,
        bootstrap_servers=bootstrap,
        stop_event=stop_event,
        group_id="pizza-validate-service",
    )

    logger.info("Validation service stopped.")


if __name__ == "__main__":
    main()
