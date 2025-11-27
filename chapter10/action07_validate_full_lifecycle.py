from __future__ import annotations

"""
Action 10.7 – Validate the full lifecycle via JSON roundtrip.

This script:
- builds a full lifecycle for one order:
    OrderReceived -> OrderValidated -> OrderPrepared
    -> OrderBaked -> OrderReadyForPickup
- serializes each stage to JSON
- validates each JSON back into the corresponding model

Run with:
    python -m chapter10.action07_validate_full_lifecycle
"""

import random
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


def build_validated(received: OrderReceived) -> OrderValidated:
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


def _roundtrip(label: str, model_cls, instance) -> None:
    print(f"\n=== {label} ===")
    json_payload = instance.model_dump_json()
    print("JSON:", json_payload)

    restored = model_cls.model_validate_json(json_payload)
    print("Restored:", restored)
    print("Equal by value? ->", instance == restored)


def main() -> None:
    rng = random.Random(2025)

    received = create_random_order(rng)
    validated = build_validated(received)
    prepared = build_prepared(received)
    baked = build_baked(prepared)
    ready = build_ready_for_pickup(baked)

    _roundtrip("OrderReceived", OrderReceived, received)
    _roundtrip("OrderValidated", OrderValidated, validated)
    _roundtrip("OrderPrepared", OrderPrepared, prepared)
    _roundtrip("OrderBaked", OrderBaked, baked)
    _roundtrip("OrderReadyForPickup", OrderReadyForPickup, ready)


if __name__ == "__main__":
    main()
