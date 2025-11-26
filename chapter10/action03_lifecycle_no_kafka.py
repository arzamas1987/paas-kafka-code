from __future__ import annotations

"""
Action 10.3 – Full lifecycle simulation without Kafka.

Every 10 seconds, a new order goes through:
- OrderReceived
- OrderValidated
- OrderPrepared
- OrderBaked
- OrderReadyForPickup

Each stage is emitted 1 second after the previous one.

Run with:
    python -m chapter10.action03_lifecycle_no_kafka
"""

import random
import time
from datetime import datetime, timezone

from pizza_app.generator.order_factory import create_random_order
from pizza_app.models.order_events import (
    OrderReceived,
    OrderValidated,
    OrderPrepared,
    OrderBaked,
    OrderReadyForPickup,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _log(event) -> None:
    """Print the event type and JSON payload on one line."""
    print(f"[{event.event_type}] {event.model_dump_json()}")


def build_validated(received: OrderReceived) -> OrderValidated:
    """Create an OrderValidated event from OrderReceived."""
    if received.order_type == "delivery":
        is_address_serviceable = True
        kitchen_window = "next-10-min"
    else:
        is_address_serviceable = None
        kitchen_window = None

    return OrderValidated(
        order_id=received.order_id,
        event_type="OrderValidated",
        order_type=received.order_type,  # type: ignore[arg-type]
        timestamp=_now(),
        correlation_id=received.correlation_id,
        trace_id=received.trace_id,
        customer_id=received.customer_id,
        items=received.items,
        address=received.address,
        is_address_serviceable=is_address_serviceable,
        kitchen_window=kitchen_window,
        validation_notes=[],
    )


def build_prepared(received: OrderReceived) -> OrderPrepared:
    """Create an OrderPrepared event from OrderReceived."""
    prep_started_at = _now()
    estimated_oven_slot = "slot-1"

    return OrderPrepared(
        order_id=received.order_id,
        event_type="OrderPrepared",
        order_type=received.order_type,  # type: ignore[arg-type]
        timestamp=prep_started_at,
        correlation_id=received.correlation_id,
        trace_id=received.trace_id,
        customer_id=received.customer_id,
        prep_started_at=prep_started_at,
        estimated_oven_slot=estimated_oven_slot,
        items=received.items,
    )


def build_baked(prepared: OrderPrepared) -> OrderBaked:
    """Create an OrderBaked event from OrderPrepared."""
    baked_at = _now()
    baked_items = [item.sku for item in prepared.items]

    return OrderBaked(
        order_id=prepared.order_id,
        event_type="OrderBaked",
        order_type=prepared.order_type,  # type: ignore[arg-type]
        timestamp=baked_at,
        correlation_id=prepared.correlation_id,
        trace_id=prepared.trace_id,
        customer_id=prepared.customer_id,
        baked_items=baked_items,
        baked_at=baked_at,
    )


def build_ready_for_pickup(baked: OrderBaked) -> OrderReadyForPickup:
    """Create an OrderReadyForPickup event from OrderBaked."""
    ready_at = _now()
    pickup_point = "kitchen-exit-1" if baked.order_type == "delivery" else "counter-1"

    return OrderReadyForPickup(
        order_id=baked.order_id,
        event_type="OrderReadyForPickup",
        order_type=baked.order_type,  # type: ignore[arg-type]
        timestamp=ready_at,
        correlation_id=baked.correlation_id,
        trace_id=baked.trace_id,
        customer_id=baked.customer_id,
        pickup_point=pickup_point,
        ready_at=ready_at,
    )


def run_lifecycle_demo(
    rng: random.Random | None = None,
    orders_to_simulate: int | None = None,
    chain_interval_seconds: float = 10.0,
    stage_delay_seconds: float = 1.0,
) -> None:
    """
    Simulate a full lifecycle for one order every `chain_interval_seconds`.
    Within each lifecycle, stages are spaced by `stage_delay_seconds`.
    """
    if rng is None:
        rng = random.Random()

    count = 0
    while True:
        start = time.time()

        received = create_random_order(rng)
        _log(received)

        time.sleep(stage_delay_seconds)

        validated = build_validated(received)
        _log(validated)

        time.sleep(stage_delay_seconds)

        prepared = build_prepared(received)
        _log(prepared)

        time.sleep(stage_delay_seconds)

        baked = build_baked(prepared)
        _log(baked)

        time.sleep(stage_delay_seconds)

        ready = build_ready_for_pickup(baked)
        _log(ready)

        elapsed = time.time() - start
        remaining = chain_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

        count += 1
        if orders_to_simulate is not None and count >= orders_to_simulate:
            break


def main() -> None:
    rng = random.Random()
    run_lifecycle_demo(
        rng=rng,
        orders_to_simulate=5,
        chain_interval_seconds=10.0,
        stage_delay_seconds=1.0,
    )


if __name__ == "__main__":
    main()
