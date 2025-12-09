# pizza_app/services/stage_order_ready_for_pickup.py

from __future__ import annotations

import logging
import signal
import threading

from pizza_app.kafka_helpers import get_bootstrap_servers
from pizza_app.services.lifecycle import (
    TOPIC_BAKED,
    TOPIC_READY,
    run_stage_worker,
    transform_to_ready,
)

logger = logging.getLogger(__name__)


def main() -> None:
    """
    Ready-for-pickup service.

    Consumes OrderBaked events from order-baked and produces
    OrderReadyForPickup events to order-ready-for-pickup.
    """
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        logger.info("Received signal %s – stopping ready-for-pickup service.", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    logger.info(
        "Ready-for-pickup service starting with bootstrap servers=%s, input=%s, output=%s",
        bootstrap,
        TOPIC_BAKED,
        TOPIC_READY,
    )

    run_stage_worker(
        name="ready",
        input_topic=TOPIC_BAKED,
        output_topic=TOPIC_READY,
        transform_fn=transform_to_ready,
        bootstrap_servers=bootstrap,
        stop_event=stop_event,
        group_id="pizza-ready-service",
    )

    logger.info("Ready-for-pickup service stopped.")


if __name__ == "__main__":
    main()
