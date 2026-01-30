from __future__ import annotations

import os
import signal
import threading

from pizza_app.kafka_helpers import create_json_consumer, format_record, get_bootstrap_servers


def main() -> None:
    stop_event = threading.Event()

    def _handle(sig, frame) -> None:  # type: ignore[override]
        stop_event.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    bootstrap = get_bootstrap_servers()
    topic = os.getenv("PIZZA_TOPIC_READY", "order-ready-for-pickup")
    group_id = os.getenv("PIZZA_GROUP_OBSERVER", "ch13-observer-ready")

    consumer = create_json_consumer([topic], group_id=group_id, bootstrap_servers=bootstrap, auto_offset_reset="earliest")

    print(f"[INFO] Observing topic='{topic}' group_id='{group_id}' (CTRL+C to stop)")
    try:
        for msg in consumer:
            if stop_event.is_set():
                break
            print(format_record(msg))
    finally:
        consumer.close()
        print("[OK] Observer stopped cleanly.")


if __name__ == "__main__":
    main()
