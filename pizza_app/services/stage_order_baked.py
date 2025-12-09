# pizza_app/services/stage_order_baked.py

from __future__ import annotations

import logging
import signal
import threading

from pizza_app.kafka_helpers import get_bootstrap_servers
from pizza_app.services.lifecycle import (
    TOPIC_PREPARED,
    TOPIC_BAKED,
    run_stage_worker,
    transform_to_baked,
)

logger = logging.getLogger(__name__)


def main() -> None:
    """
    Baking service.

    Consumes OrderPrepared events from order-prepared and produces
    OrderBaked events to order-baked.
    """
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        logger.info("Received signal %s – stopping baking service.", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    logger.info(
        "Baking service starting with bootstrap servers=%s, input=%s, output=%s",
        bootstrap,
        TOPIC_PREPARED,
        TOPIC_BAKED,
    )

    run_stage_worker(
        name="bake",
        input_topic=TOPIC_PREPARED,
        output_topic=TOPIC_BAKED,
        transform_fn=transform_to_baked,
        bootstrap_servers=bootstrap,
        stop_event=stop_event,
        group_id="pizza-bake-service",
    )

    logger.info("Baking service stopped.")


if __name__ == "__main__":
    main()
