"""
Chapter 13 — Action 07
Run the Pizza lifecycle pipeline against the Docker-based Kafka cluster.

This chapter containerises Kafka only. The Pizza services run as normal Python processes.

Usage (recommended first run — one process, threaded):
  export KAFKA_PROFILE=docker
  export KAFKA_BOOTSTRAP_SERVERS=localhost:19092
  python -m chapter13.action07_run_pipeline_docker --mode orchestrator --orders 5

Usage (realistic — one service per terminal):
  # Terminal 1
  export KAFKA_PROFILE=docker
  python -m chapter13.action07_run_pipeline_docker --role validate

  # Terminal 2
  export KAFKA_PROFILE=docker
  python -m chapter13.action07_run_pipeline_docker --role prepare

  # Terminal 3
  export KAFKA_PROFILE=docker
  python -m chapter13.action07_run_pipeline_docker --role bake

  # Terminal 4
  export KAFKA_PROFILE=docker
  python -m chapter13.action07_run_pipeline_docker --role ready

  # Terminal 5 (start last)
  export KAFKA_PROFILE=docker
  python -m chapter13.action07_run_pipeline_docker --role initiate --orders 5
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import Consumer, Producer

from pizza_app.config import get_kafka_settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(order_id: str, event_type: str, order_type: str = "delivery") -> dict:
    # Minimal envelope aligned with your book’s intent (stable keys, JSON payload).
    # If your Chapter 10 model is richer, you can swap this later without changing topics.
    return {
        "order_id": order_id,
        "event_type": event_type,
        "order_type": order_type,
        "timestamp": utc_now_iso(),
        "correlation_id": str(uuid.uuid4()),
        "trace_id": str(uuid.uuid4()),
    }


def producer(bootstrap: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap})


def consumer(bootstrap: str, group_id: str) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    return c


def run_stage(
    *,
    bootstrap: str,
    group_id: str,
    in_topic: str,
    out_topic: str,
    in_event_type: str,
    out_event_type: str,
    stop_after: int | None = None,
) -> None:
    c = consumer(bootstrap, group_id)
    p = producer(bootstrap)

    c.subscribe([in_topic])
    processed = 0

    print(f"[{group_id}] consuming {in_topic} -> producing {out_topic}")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{group_id}] consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else ""
            payload = msg.value().decode("utf-8") if msg.value() else ""

            try:
                event = json.loads(payload)
            except json.JSONDecodeError:
                event = {"raw": payload}

            # Validate event type lightly (don’t crash the lab).
            if event.get("event_type") != in_event_type:
                # Still forward? For a learning lab, we log and continue.
                print(f"[{group_id}] skip unexpected event_type={event.get('event_type')} key={key}")
                continue

            next_event = dict(event)
            next_event["event_type"] = out_event_type
            next_event["timestamp"] = utc_now_iso()

            p.produce(out_topic, key=key.encode("utf-8"), value=json.dumps(next_event).encode("utf-8"))
            p.poll(0)
            processed += 1

            print(f"[{group_id}] {in_event_type} -> {out_event_type} order_id={key}")

            if stop_after is not None and processed >= stop_after:
                break

    finally:
        try:
            p.flush(5)
        except Exception:
            pass
        c.close()
        print(f"[{group_id}] stopped")


def run_initiator(*, bootstrap: str, out_topic: str, orders: int) -> None:
    p = producer(bootstrap)
    print(f"[initiator] producing {orders} orders -> {out_topic}")

    try:
        for i in range(1, orders + 1):
            order_id = f"order-{700 + i}"
            ev = make_event(order_id=order_id, event_type="OrderReceived", order_type="delivery")
            p.produce(out_topic, key=order_id.encode("utf-8"), value=json.dumps(ev).encode("utf-8"))
            p.poll(0)
            print(f"[initiator] produced OrderReceived order_id={order_id}")
            time.sleep(0.2)
    finally:
        p.flush(5)
        print("[initiator] done")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)

    p.add_argument("--mode", choices=["orchestrator", "role"], default="orchestrator")
    p.add_argument(
        "--role",
        choices=["initiate", "validate", "prepare", "bake", "ready"],
        help="Used when --mode role",
    )
    p.add_argument("--orders", type=int, default=5, help="Number of orders to send (initiator).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    s = get_kafka_settings()
    bootstrap = s.bootstrap_servers

    print(f"[INFO] profile={s.profile} bootstrap={bootstrap}")
    print(
        f"[INFO] topics: {s.incoming_orders_topic}, {s.order_validated_topic}, {s.order_prepared_topic}, {s.order_baked_topic}, {s.order_ready_topic}"
    )

    if args.mode == "role":
        if not args.role:
            raise SystemExit("Missing --role for --mode role")

        if args.role == "initiate":
            run_initiator(bootstrap=bootstrap, out_topic=s.incoming_orders_topic, orders=args.orders)
            return 0

        if args.role == "validate":
            run_stage(
                bootstrap=bootstrap,
                group_id="chapter13-validate",
                in_topic=s.incoming_orders_topic,
                out_topic=s.order_validated_topic,
                in_event_type="OrderReceived",
                out_event_type="OrderValidated",
            )
            return 0

        if args.role == "prepare":
            run_stage(
                bootstrap=bootstrap,
                group_id="chapter13-prepare",
                in_topic=s.order_validated_topic,
                out_topic=s.order_prepared_topic,
                in_event_type="OrderValidated",
                out_event_type="OrderPrepared",
            )
            return 0

        if args.role == "bake":
            run_stage(
                bootstrap=bootstrap,
                group_id="chapter13-bake",
                in_topic=s.order_prepared_topic,
                out_topic=s.order_baked_topic,
                in_event_type="OrderPrepared",
                out_event_type="OrderBaked",
            )
            return 0

        if args.role == "ready":
            run_stage(
                bootstrap=bootstrap,
                group_id="chapter13-ready",
                in_topic=s.order_baked_topic,
                out_topic=s.order_ready_topic,  # keep: order-ready-to-deliver
                in_event_type="OrderBaked",
                out_event_type="OrderReadyToDeliver",
            )
            return 0

    # Orchestrator mode (threads)
    threads: list[threading.Thread] = []

    threads.append(
        threading.Thread(
            target=run_stage,
            kwargs=dict(
                bootstrap=bootstrap,
                group_id="chapter13-validate",
                in_topic=s.incoming_orders_topic,
                out_topic=s.order_validated_topic,
                in_event_type="OrderReceived",
                out_event_type="OrderValidated",
            ),
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=run_stage,
            kwargs=dict(
                bootstrap=bootstrap,
                group_id="chapter13-prepare",
                in_topic=s.order_validated_topic,
                out_topic=s.order_prepared_topic,
                in_event_type="OrderValidated",
                out_event_type="OrderPrepared",
            ),
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=run_stage,
            kwargs=dict(
                bootstrap=bootstrap,
                group_id="chapter13-bake",
                in_topic=s.order_prepared_topic,
                out_topic=s.order_baked_topic,
                in_event_type="OrderPrepared",
                out_event_type="OrderBaked",
            ),
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=run_stage,
            kwargs=dict(
                bootstrap=bootstrap,
                group_id="chapter13-ready",
                in_topic=s.order_baked_topic,
                out_topic=s.order_ready_topic,
                in_event_type="OrderBaked",
                out_event_type="OrderReadyToDeliver",
            ),
            daemon=True,
        )
    )

    for t in threads:
        t.start()

    # Give consumers a moment to join groups before producing
    time.sleep(1.5)

    run_initiator(bootstrap=bootstrap, out_topic=s.incoming_orders_topic, orders=args.orders)

    print("[INFO] pipeline running; press Ctrl+C to stop (or run role mode per terminal).")
    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        print("\n[INFO] stopping orchestrator")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
