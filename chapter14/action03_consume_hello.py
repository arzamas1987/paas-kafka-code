from __future__ import annotations

import json
import os

from chapter14.confluent.cloud_config import CloudKafkaSettings, build_consumer

TOPIC = "hello-pizza"


def main() -> int:
    settings = CloudKafkaSettings.from_env()

    group_id = os.getenv("PIZZA_CLOUD_CONSUMER_GROUP", "ch14-action03-hello-cloud-demo").strip()
    consumer = build_consumer(settings, group_id=group_id, auto_offset_reset="earliest")
    consumer.subscribe([TOPIC])

    print(f"[INFO] Listening on topic='{TOPIC}' group='{group_id}'. Stop with Ctrl+C.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[WARN] consumer_error={msg.error()}")
                continue

            key = (msg.key() or b"").decode("utf-8", errors="replace")
            raw = (msg.value() or b"").decode("utf-8", errors="replace")

            try:
                payload = json.loads(raw)
            except Exception:
                payload = raw

            print(f"[OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key='{key}'")
            print(payload)
    except KeyboardInterrupt:
        print("[OK] Stopped by user.")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
