"""
Chapter 13 — Action 7
Run the Pizza pipeline against the Docker-based Kafka cluster.

Modes:
  - orchestrator: one process, threads (easy first run)
  - role: run exactly one service (validation/preparation/baking/ready/initiator)

Examples (host Python -> Docker Kafka via localhost:19092):
  export KAFKA_PROFILE=docker
  export KAFKA_BOOTSTRAP_SERVERS=localhost:19092

  # Orchestrator, keep producing forever (1–5s jitter)
  python -m chapter13.action07_run_pipeline_docker --mode orchestrator --interval-min 1 --interval-max 5

  # Orchestrator, produce exactly 5 orders and stop
  python -m chapter13.action07_run_pipeline_docker --mode orchestrator --max-orders 5

  # Role mode: run workers in separate terminals
  python -m chapter13.action07_run_pipeline_docker --mode role --role validation
  python -m chapter13.action07_run_pipeline_docker --mode role --role preparation
  python -m chapter13.action07_run_pipeline_docker --mode role --role baking
  python -m chapter13.action07_run_pipeline_docker --mode role --role ready

  # Role mode: initiator producing forever (1–5s)
  python -m chapter13.action07_run_pipeline_docker --mode role --role initiator --interval-min 1 --interval-max 5
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

try:
    from pizza_app.config import get_kafka_settings  # type: ignore
except Exception as e:
    raise RuntimeError(
        "Could not import pizza_app.config.get_kafka_settings(). "
        "Run this from the repo root and ensure pizza_app/ is on the Python path."
    ) from e


_STOP = threading.Event()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_event(order_id: str, event_type: str, order_type: str = "delivery") -> dict:
    # Keep the envelope compatible with your book’s contract (Chapter 10),
    # but don’t overcomplicate it here.
    return {
        "order_id": order_id,
        "event_type": event_type,
        "order_type": order_type,
        "timestamp": _now_iso(),
        "correlation_id": f"corr-{order_id}",
        "trace_id": f"trace-{order_id}",
    }


def _producer(bootstrap: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap})


def _consumer(bootstrap: str, group_id: str, topic: str) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    c.subscribe([topic])
    return c


def _deliver_report(err, msg) -> None:
    if err is not None:
        print(f"[ERR] delivery failed: {err}")
    # else: keep it quiet; pipeline logs are already noisy enough.


def _run_stage(
    name: str,
    bootstrap: str,
    in_topic: str,
    out_topic: str,
    group_id: str,
    out_event_type: str,
) -> None:
    cons = _consumer(bootstrap, group_id=group_id, topic=in_topic)
    prod = _producer(bootstrap)

    print(f"[{name}] consuming {in_topic} -> producing {out_topic} (group={group_id})")

    try:
        while not _STOP.is_set():
            msg = cons.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{name}] consumer error: {msg.error()}")
                continue

            key = msg.key() or b""
            try:
                payload = json.loads((msg.value() or b"{}").decode("utf-8"))
            except Exception:
                payload = {}

            # Transform minimally: update event_type + timestamp
            payload["event_type"] = out_event_type
            payload["timestamp"] = _now_iso()

            prod.produce(
                out_topic,
                key=key,
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=_deliver_report,
            )
            prod.poll(0)

    finally:
        try:
            cons.close()
        except Exception:
            pass
        try:
            prod.flush(5)
        except Exception:
            pass


def _run_initiator(
    bootstrap: str,
    out_topic: str,
    interval_min: float,
    interval_max: float,
    max_orders: int,
    burst_size: int,
    burst_pause: float,
) -> None:
    prod = _producer(bootstrap)
    print(
        f"[initiator] producing to {out_topic} "
        f"(interval={interval_min}-{interval_max}s, max_orders={max_orders}, burst={burst_size}/{burst_pause}s)"
    )

    produced = 0
    burst_count = 0

    while not _STOP.is_set():
        if max_orders > 0 and produced >= max_orders:
            break

        # order_id should remain stable & readable in logs
        produced += 1
        order_id = f"{int(time.time())}-{produced:04d}"
        event = _make_event(order_id=order_id, event_type="OrderReceived", order_type="delivery")

        # Use key=order_id to preserve ordering per order_id partition
        prod.produce(
            out_topic,
            key=order_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=_deliver_report,
        )
        prod.poll(0)
        print(f"[initiator] produced order_id={order_id}")

        # Burst mode: send N quickly, then pause
        if burst_size > 0:
            burst_count += 1
            if burst_count >= burst_size:
                burst_count = 0
                # Pause between bursts
                _sleep_with_stop(burst_pause)
                continue

        # Normal continuous mode: random jitter between min/max
        delay = random.uniform(interval_min, interval_max)
        _sleep_with_stop(delay)

    prod.flush(10)
    print("[initiator] stopped")


def _sleep_with_stop(seconds: float) -> None:
    # Sleep in small chunks so Ctrl+C stops quickly
    end = time.time() + max(0.0, seconds)
    while not _STOP.is_set() and time.time() < end:
        time.sleep(0.2)


def _install_signal_handlers() -> None:
    def _handler(sig, frame) -> None:  # noqa: ARG001
        _STOP.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main(argv: list[str] | None = None) -> int:
    _install_signal_handlers()

    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["orchestrator", "role"], required=True)

    p.add_argument("--role", choices=["initiator", "validation", "preparation", "baking", "ready"])
    p.add_argument("--interval-min", type=float, default=1.0)
    p.add_argument("--interval-max", type=float, default=5.0)
    p.add_argument("--max-orders", type=int, default=0, help="0 = run forever")
    p.add_argument("--burst-size", type=int, default=0, help="0 disables burst mode")
    p.add_argument("--burst-pause", type=float, default=10.0, help="pause seconds between bursts")

    args = p.parse_args(argv)

    if args.interval_min <= 0 or args.interval_max < args.interval_min:
        raise SystemExit("Invalid interval range. Use e.g. --interval-min 1 --interval-max 5")

    s = get_kafka_settings()
    bootstrap = s.bootstrap_servers
    print(f"[INFO] profile={s.profile} bootstrap={bootstrap}")

    # Topics from your existing config.py (incl. order-ready-to-deliver)
    t_in = s.incoming_orders_topic
    t_val = s.order_validated_topic
    t_prep = s.order_prepared_topic
    t_bake = s.order_baked_topic
    t_ready = s.order_ready_topic

    if args.mode == "role":
        if not args.role:
            raise SystemExit("--role is required when --mode role")

        if args.role == "initiator":
            _run_initiator(
                bootstrap=bootstrap,
                out_topic=t_in,
                interval_min=args.interval_min,
                interval_max=args.interval_max,
                max_orders=args.max_orders,
                burst_size=args.burst_size,
                burst_pause=args.burst_pause,
            )
            return 0

        if args.role == "validation":
            _run_stage("validation", bootstrap, t_in, t_val, "pizza-validation", "OrderValidated")
            return 0
        if args.role == "preparation":
            _run_stage("preparation", bootstrap, t_val, t_prep, "pizza-preparation", "OrderPrepared")
            return 0
        if args.role == "baking":
            _run_stage("baking", bootstrap, t_prep, t_bake, "pizza-baking", "OrderBaked")
            return 0
        if args.role == "ready":
            _run_stage("ready", bootstrap, t_bake, t_ready, "pizza-ready", "OrderReadyToDeliver")
            return 0

        raise SystemExit("Unknown role")

    # orchestrator mode: start all workers + initiator in one process (threads)
    threads: list[threading.Thread] = []

    threads.append(threading.Thread(target=_run_stage, args=("validation", bootstrap, t_in, t_val, "pizza-validation", "OrderValidated"), daemon=True))
    threads.append(threading.Thread(target=_run_stage, args=("preparation", bootstrap, t_val, t_prep, "pizza-preparation", "OrderPrepared"), daemon=True))
    threads.append(threading.Thread(target=_run_stage, args=("baking", bootstrap, t_prep, t_bake, "pizza-baking", "OrderBaked"), daemon=True))
    threads.append(threading.Thread(target=_run_stage, args=("ready", bootstrap, t_bake, t_ready, "pizza-ready", "OrderReadyToDeliver"), daemon=True))

    for t in threads:
        t.start()

    # Start producing after workers are up
    _run_initiator(
        bootstrap=bootstrap,
        out_topic=t_in,
        interval_min=args.interval_min,
        interval_max=args.interval_max,
        max_orders=args.max_orders,
        burst_size=args.burst_size,
        burst_pause=args.burst_pause,
    )

    # If max_orders was set, initiator exits; stop workers too.
    _STOP.set()
    for t in threads:
        t.join(timeout=3.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
