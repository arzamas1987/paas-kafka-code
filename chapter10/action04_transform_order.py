from __future__ import annotations

"""
Action 10.4 – Transform an order through the first stages (no Kafka).

This script takes an OrderReceived event and derives:
- OrderValidated
- OrderPrepared

Run with:
    python -m chapter10.action04_transform_order
"""

import random
from datetime import datetime, timezone

from pizza_app.generator.order_factory import create_random_order
from pizza_app.models.order_events import (
    OrderReceived,
    OrderValidated,
    OrderPrepared,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _log(event) -> None:
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


def main() -> None:
    rng = random.Random(42)

    received = create_random_order(rng)
    _log(received)

    validated = build_validated(received)
    _log(validated)

    prepared = build_prepared(received)
    _log(prepared)


if __name__ == "__main__":
    main()
