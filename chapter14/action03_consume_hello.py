from __future__ import annotations

import json

from chapter14.action02_03_cloud_config import CloudKafkaSettings, build_consumer

TOPIC = "hello-pizza"


def main() -> int:
    settings = CloudKafkaSettings.from_env()
    consumer = build_consumer(settings, client_id="ch14-action03-consumer", topics=[TOPIC])

    print("[INFO] Listening on topic 'hello-pizza'. Press CTRL+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value_raw = msg.value().decode("utf-8") if msg.value() else ""
            try:
                value = json.loads(value_raw)
            except Exception:
                value = value_raw

            print(f"[OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key}")
            print(value)
    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
