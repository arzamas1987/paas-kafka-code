from __future__ import annotations

import os
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from pizza_app.generator import menu
from pizza_app.generator.address import generate_address
from pizza_app.generator.order_counter import increment_and_store
from pizza_app.models.base import Item
from pizza_app.models.order_events import OrderReceived


COUNTER_FILE = Path(os.getenv("PIZZA_ORDER_COUNTER_FILE", "var/order_counter.json"))


def _build_items(rng: random.Random) -> List[Item]:
    num_pizzas = rng.randint(1, 3)
    items: List[Item] = []
    for _ in range(num_pizzas):
        sku = rng.choice(menu.PIZZAS).sku
        qty = rng.randint(1, 2)
        items.append(Item(sku=sku, qty=qty))

    if rng.random() < 0.6:
        drink_sku = rng.choice(menu.DRINKS).sku
        items.append(Item(sku=drink_sku, qty=1))

    return items


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_random_order(rng: random.Random | None = None) -> OrderReceived:
    if rng is None:
        rng = random.Random()

    order_type = rng.choices(["delivery", "onsite"], weights=[0.7, 0.3])[0]
    timestamp = _now()

    order_number = increment_and_store(COUNTER_FILE)
    order_id = f"A{order_number:04d}"

    items = _build_items(rng)

    address = None
    table_number = None
    customer_id = None

    if order_type == "delivery":
        address = generate_address(rng)
        customer_id = f"CUS-{rng.randint(100, 999)}"
    else:
        table_number = rng.randint(1, 20)

    return OrderReceived(
        order_id=order_id,
        event_type="OrderReceived",
        order_type=order_type,  # type: ignore[arg-type]
        items=items,
        address=address,
        table_number=table_number,
        timestamp=timestamp,
        correlation_id=f"COR-{order_id}",
        trace_id=f"TRACE-{order_id}",
        customer_id=customer_id,
    )
