# chapter12/action07_full_lifecycle_over_kafka.py

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from typing import List

from pizza_app.kafka_helpers import get_bootstrap_servers
from pizza_app.services.lifecycle import (
    TOPIC_INCOMING,
    TOPIC_VALIDATED,
    TOPIC_PREPARED,
    TOPIC_BAKED,
    TOPIC_READY,
    run_order_received_producer,
    run_stage_worker,
    transform_to_validated,
    transform_to_prepared,
    transform_to_baked,
    transform_to_ready,
)

log_format = "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)


def main() -> None:
    """
    Action 7 – Full Pizza Lifecycle over Kafka.

    Starts:
      * one producer that generates OrderReceived events (the initiator), and
      * four stage workers:
          - validation:   incoming-orders    → order-validated
          - preparation:  order-validated    → order-prepared
          - baking:       order-prepared     → order-baked
          - ready:        order-baked        → order-ready-for-pickup

    Startup order:
      1. Start all four stage workers (consumers) first.
      2. Wait a few seconds so consumer groups can stabilise.
      3. Start the OrderReceived producer (initiator) last.

    This reduces the noisy transient logs (NodeNotReadyError,
    MemberIdRequiredError) that appear when consumer groups are still
    joining while data is already flowing.

    The same lifecycle functions are reused by the standalone services under
    pizza_app.services.* so the architecture can be reused in Docker and
    Confluent-based chapters.
    """
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        logger.info("Received signal %s – initiating graceful shutdown.", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    logger.info(
        "Starting full lifecycle with KAFKA_PROFILE=%s, bootstrap servers=%s",
        os.getenv("KAFKA_PROFILE", "<not-set>"),
        bootstrap,
    )

    threads: List[threading.Thread] = []

    # ------------------------------------------------------------------
    # 1) Stage workers – start all consumers first
    # ------------------------------------------------------------------

    # Validation stage
    validation_thread = threading.Thread(
        target=run_stage_worker,
        name="Stage-Validate",
        kwargs={
            "name": "validate",
            "input_topic": TOPIC_INCOMING,
            "output_topic": TOPIC_VALIDATED,
            "transform_fn": transform_to_validated,
            "bootstrap_servers": bootstrap,
            "stop_event": stop_event,
            "group_id": "pizza-validate-service",
        },
        daemon=True,
    )
    threads.append(validation_thread)

    # Preparation stage
    preparation_thread = threading.Thread(
        target=run_stage_worker,
        name="Stage-Prepare",
        kwargs={
            "name": "prepare",
            "input_topic": TOPIC_VALIDATED,
            "output_topic": TOPIC_PREPARED,
            "transform_fn": transform_to_prepared,
            "bootstrap_servers": bootstrap,
            "stop_event": stop_event,
            "group_id": "pizza-prepare-service",
        },
        daemon=True,
    )
    threads.append(preparation_thread)

    # Baking stage
    baking_thread = threading.Thread(
        target=run_stage_worker,
        name="Stage-Bake",
        kwargs={
            "name": "bake",
            "input_topic": TOPIC_PREPARED,
            "output_topic": TOPIC_BAKED,
            "transform_fn": transform_to_baked,
            "bootstrap_servers": bootstrap,
            "stop_event": stop_event,
            "group_id": "pizza-bake-service",
        },
        daemon=True,
    )
    threads.append(baking_thread)

    # Ready-for-pickup stage
    ready_thread = threading.Thread(
        target=run_stage_worker,
        name="Stage-Ready",
        kwargs={
            "name": "ready",
            "input_topic": TOPIC_BAKED,
            "output_topic": TOPIC_READY,
            "transform_fn": transform_to_ready,
            "bootstrap_servers": bootstrap,
            "stop_event": stop_event,
            "group_id": "pizza-ready-service",
        },
        daemon=True,
    )
    threads.append(ready_thread)

    logger.info("Launching %d stage worker threads (consumers).", len(threads))
    for t in threads:
        t.start()

    # ------------------------------------------------------------------
    # 2) Give consumer groups time to stabilise
    # ------------------------------------------------------------------
    logger.info(
        "Stage workers started. Waiting 5 seconds for consumer groups to stabilise "
        "before starting the OrderReceived initiator."
    )
    time.sleep(5.0)

    # ------------------------------------------------------------------
    # 3) Initiator – OrderReceived producer started last
    # ------------------------------------------------------------------
    producer_thread = threading.Thread(
        target=run_order_received_producer,
        name="Producer-OrderReceived",
        kwargs={
            "bootstrap_servers": bootstrap,
            "stop_event": stop_event,
            "interval_sec": 1.0,
        },
        daemon=True,
    )
    threads.append(producer_thread)

    logger.info("Starting initiator thread: %s", producer_thread.name)
    producer_thread.start()

    # ------------------------------------------------------------------
    # Main loop – keep process alive until stop_event is set
    # ------------------------------------------------------------------
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    logger.info("Waiting for threads to stop...")
    for t in threads:
        t.join(timeout=10.0)

    logger.info("All threads stopped. Full lifecycle demo finished.")


if __name__ == "__main__":
    main()
