from __future__ import annotations

import json
import time

from chapter14.action02_03_cloud_config import CloudKafkaSettings, build_producer

TOPIC = "hello-pizza"


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
        return
    key = msg.key().decode("utf-8") if msg.key() else None
    print(f"[OK] Delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key}")


def main() -> int:
    settings = CloudKafkaSettings.from_env()
    producer = build_producer(settings, client_id="ch14-action03-producer-stream")

    total_messages = 10
    interval_seconds = 1

    print(f"[INFO] Producing {total_messages} messages in ~{total_messages} seconds.")
    for i in range(1, total_messages + 1):
        event = {
            "event_name": "HelloPizza",
            "event_version": 1,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload": {"message": f"hello pizza #{i}"},
        }

        key = f"HELLO-{i}".encode("utf-8")
        value = json.dumps(event, separators=(",", ":")).encode("utf-8")

        producer.produce(TOPIC, key=key, value=value, on_delivery=delivery_report)
        producer.flush(5)

        if i < total_messages:
            time.sleep(interval_seconds)

    print("[INFO] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
