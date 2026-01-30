from __future__ import annotations

import os
import signal
import threading
import time

from pizza_app.kafka_helpers import get_bootstrap_servers
from pizza_app.services.lifecycle import run_stage_worker, transform_to_baked


def _delay_ms() -> int:
    v = os.getenv("PIZZA_DELAY_BAKE_MS", "0")
    return int(v)


def main() -> None:
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    in_topic = os.getenv("PIZZA_TOPIC_PREPARED", "order-prepared")
    out_topic = os.getenv("PIZZA_TOPIC_BAKED", "order-baked")
    group_id = os.getenv("PIZZA_GROUP_BAKE", "ch13-bake")
    delay = _delay_ms()

    def _tf(e: dict) -> dict:
        out = transform_to_baked(e)
        if delay > 0:
            time.sleep(delay / 1000.0)
        return out

    run_stage_worker(
        name="bake",
        input_topic=in_topic,
        output_topic=out_topic,
        transform_fn=_tf,
        bootstrap_servers=bootstrap,
        stop_event=stop_event,
        group_id=group_id,
    )


if __name__ == "__main__":
    main()
