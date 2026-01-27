from __future__ import annotations

import json
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from confluent_kafka import KafkaError, Message

from chapter14.ch14_cloud_config import CloudKafkaSettings, build_consumer, build_producer

TransformFn = Callable[[Dict[str, Any]], Dict[str, Any]]

_STOP = False


def _handle_stop(_sig: int, _frame) -> None:
    global _STOP
    _STOP = True


@dataclass(frozen=True)
class StageSpec:
    name: str
    input_topic: str
    output_topic: Optional[str]
    group_id: str
    transform_fn: TransformFn


def run_stage(spec: StageSpec) -> int:
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    settings = CloudKafkaSettings.from_env()
    consumer = build_consumer(settings, group_id=spec.group_id, auto_offset_reset="earliest")
    producer = build_producer(settings) if spec.output_topic else None

    consumer.subscribe([spec.input_topic])

    print(
        f"[OK] stage='{spec.name}' input='{spec.input_topic}' output='{spec.output_topic or '-'}' group='{spec.group_id}'"
    )
    print("[INFO] Stop with Ctrl+C.")
    print("")

    try:
        while not _STOP:
            msg: Optional[Message] = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[WARN] stage='{spec.name}' consumer_error={msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else ""
            raw = msg.value().decode("utf-8") if msg.value() else "{}"

            try:
                event = json.loads(raw)
            except Exception:
                print(f"[WARN] stage='{spec.name}' invalid_json key='{key}' offset={msg.offset()}")
                continue

            try:
                out_event = spec.transform_fn(event)
            except Exception as e:
                print(f"[WARN] stage='{spec.name}' transform_failed key='{key}' offset={msg.offset()} error={e}")
                continue

            if producer is None or spec.output_topic is None:
                print(
                    f"[FACT] stage='{spec.name}' final_consume topic={msg.topic()} partition={msg.partition()} "
                    f"offset={msg.offset()} key='{key}' event_type={out_event.get('event_type')}"
                )
                continue

            producer.produce(
                topic=spec.output_topic,
                key=key.encode("utf-8"),
                value=json.dumps(out_event, separators=(",", ":")).encode("utf-8"),
            )
            producer.poll(0)

            print(
                f"[FLOW] stage='{spec.name}' key='{key}' {spec.input_topic} -> {spec.output_topic} "
                f"in_offset={msg.offset()}"
            )

        return 0
    finally:
        try:
            if producer is not None:
                producer.flush(10)
        finally:
            consumer.close()
        print(f"[OK] stage='{spec.name}' stopped cleanly.")
