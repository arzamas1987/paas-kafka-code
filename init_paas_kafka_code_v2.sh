#!/usr/bin/env bash
set -e

# Clean/create base dirs (do not delete .git)
mkdir -p pizza_app/models pizza_app/generator pizza_app/services chapter10 chapter11 chapter12 chapter13 tests var

# -------------------------
# pizza_app/__init__.py
# -------------------------
cat << 'PYEOF' > pizza_app/__init__.py
from .config import kafka_settings

__all__ = ["kafka_settings"]
PYEOF

# -------------------------
# pizza_app/models/__init__.py
# -------------------------
cat << 'PYEOF' > pizza_app/models/__init__.py
from .base import Item, Address, BaseEvent
from .order_events import (
    OrderReceived,
    OrderValidated,
    OrderPrepared,
    OrderBaked,
    OrderReadyForPickup,
    OrderPickedUp,
    OrderDelivered,
    OrderCompleted,
)

__all__ = [
    "Item",
    "Address",
    "BaseEvent",
    "OrderReceived",
    "OrderValidated",
    "OrderPrepared",
    "OrderBaked",
    "OrderReadyForPickup",
    "OrderPickedUp",
    "OrderDelivered",
    "OrderCompleted",
]
PYEOF

# -------------------------
# pizza_app/generator/__init__.py
# -------------------------
cat << 'PYEOF' > pizza_app/generator/__init__.py
"""
Generators for synthetic pizza data (menu, addresses, orders).
"""
PYEOF

# -------------------------
# pizza_app/services/__init__.py
# -------------------------
cat << 'PYEOF' > pizza_app/services/__init__.py
"""
Service-style modules that act like microservices.
"""
PYEOF

# -------------------------
# pizza_app/config.py
# -------------------------
cat << 'PYEOF' > pizza_app/config.py
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class KafkaSettings:
    profile: str
    bootstrap_servers: str
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    incoming_orders_topic: str = "incoming-orders"
    order_validated_topic: str = "order-validated"
    order_prepared_topic: str = "order-prepared"
    order_baked_topic: str = "order-baked"
    order_ready_topic: str = "order-ready-to-deliver"


def _load_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    return value


def get_kafka_settings() -> KafkaSettings:
    profile = _load_env("KAFKA_PROFILE", "local") or "local"

    if profile == "local":
        bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092"
        return KafkaSettings(profile=profile, bootstrap_servers=bootstrap)

    if profile == "docker":
        bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") or "kafka:9092"
        return KafkaSettings(profile=profile, bootstrap_servers=bootstrap)

    if profile == "cloud":
        bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "")
        if not bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS must be set for cloud profile")

        return KafkaSettings(
            profile=profile,
            bootstrap_servers=bootstrap,
            security_protocol=_load_env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=_load_env("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_username=_load_env("KAFKA_SASL_USERNAME"),
            sasl_password=_load_env("KAFKA_SASL_PASSWORD"),
        )

    bootstrap = _load_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092"
    return KafkaSettings(profile=profile, bootstrap_servers=bootstrap)


kafka_settings = get_kafka_settings()
PYEOF

# -------------------------
# pizza_app/models/base.py
# -------------------------
cat << 'PYEOF' > pizza_app/models/base.py
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, List

from pydantic import BaseModel, Field


class Item(BaseModel):
    sku: str
    qty: int


class Address(BaseModel):
    street: str
    city: str
    zone: str


class BaseEvent(BaseModel):
    order_id: str
    event_type: str = Field(..., description="Event type name, e.g. 'OrderReceived'")
    order_type: Literal["delivery", "onsite", "eat-in"]
    timestamp: datetime
    correlation_id: str
    trace_id: str
    customer_id: Optional[str] = None
PYEOF

# -------------------------
# pizza_app/models/order_events.py
# -------------------------
cat << 'PYEOF' > pizza_app/models/order_events.py
from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Literal

from pydantic import Field

from .base import BaseEvent, Item, Address


class OrderReceived(BaseEvent):
    event_type: Literal["OrderReceived"] = "OrderReceived"

    items: List[Item]
    address: Optional[Address] = None
    table_number: Optional[int] = None


class OrderValidated(BaseEvent):
    event_type: Literal["OrderValidated"] = "OrderValidated"

    items: List[Item]
    address: Optional[Address] = None
    is_address_serviceable: Optional[bool] = None
    kitchen_window: Optional[str] = None
    validation_notes: List[str] = Field(default_factory=list)


class OrderPrepared(BaseEvent):
    event_type: Literal["OrderPrepared"] = "OrderPrepared"

    prep_started_at: datetime
    estimated_oven_slot: Optional[str] = None
    items: List[Item]


class OrderBaked(BaseEvent):
    event_type: Literal["OrderBaked"] = "OrderBaked"

    baked_items: List[str]
    baked_at: datetime


class OrderReadyForPickup(BaseEvent):
    event_type: Literal["OrderReadyForPickup"] = "OrderReadyForPickup"

    pickup_point: str
    ready_at: datetime


class OrderPickedUp(BaseEvent):
    event_type: Literal["OrderPickedUp"] = "OrderPickedUp"

    courier_id: str
    picked_up_at: datetime


class OrderDelivered(BaseEvent):
    event_type: Literal["OrderDelivered"] = "OrderDelivered"

    delivered_at: datetime
    customer_feedback: Optional[str] = None


class OrderCompleted(BaseEvent):
    event_type: Literal["OrderCompleted"] = "OrderCompleted"

    completed_at: datetime
PYEOF

# -------------------------
# pizza_app/generator/menu.py
# -------------------------
cat << 'PYEOF' > pizza_app/generator/menu.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MenuItem:
    sku: str
    name: str


PIZZAS: List[MenuItem] = [
    MenuItem("pizza-margherita", "Pizza Margherita"),
    MenuItem("pizza-pepperoni", "Pizza Pepperoni"),
    MenuItem("pizza-funghi", "Pizza Funghi"),
    MenuItem("pizza-hawaii", "Pizza Hawaii"),
    MenuItem("pizza-quattro-formaggi", "Pizza Quattro Formaggi"),
    MenuItem("pizza-diavola", "Pizza Diavola"),
    MenuItem("pizza-veggie", "Pizza Vegetariana"),
    MenuItem("pizza-bbq-chicken", "Pizza BBQ Chicken"),
    MenuItem("pizza-tonno", "Pizza Tonno"),
    MenuItem("pizza-calzone", "Pizza Calzone"),
]

DRINKS: List[MenuItem] = [
    MenuItem("drink-cola", "Cola"),
    MenuItem("drink-water", "Water"),
    MenuItem("drink-sparkling-water", "Sparkling Water"),
    MenuItem("drink-lemonade", "Lemonade"),
]


def find_pizza_sku(position: int) -> str:
    return PIZZAS[position % len(PIZZAS)].sku
PYEOF

# -------------------------
# pizza_app/generator/address.py
# -------------------------
cat << 'PYEOF' > pizza_app/generator/address.py
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List

from pizza_app.models.base import Address


@dataclass(frozen=True)
class Street:
    name: str


STREETS: List[Street] = [
    Street("Oven Lane"),
    Street("Mozzarella Street"),
    Street("Pepperoni Road"),
    Street("Basil Avenue"),
    Street("Dough Boulevard"),
    Street("Tomato Alley"),
]

ZONES = ["north", "south", "east", "west", "center"]


def generate_address(rng: random.Random) -> Address:
    street = rng.choice(STREETS)
    house_number = rng.randint(1, 99)
    zone = rng.choice(ZONES)
    return Address(
        street=f"{street.name} {house_number}",
        city="Pizzaburg",
        zone=zone,
    )
PYEOF

# -------------------------
# pizza_app/generator/order_counter.py
# -------------------------
cat << 'PYEOF' > pizza_app/generator/order_counter.py
from __future__ import annotations

import json
from pathlib import Path


def load_counter(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data.get("last_order_id", 0))
    except Exception:
        return 0


def increment_and_store(path: Path) -> int:
    current = load_counter(path)
    new_value = current + 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_order_id": new_value}), encoding="utf-8")
    return new_value
PYEOF

# -------------------------
# pizza_app/generator/order_factory.py
# -------------------------
cat << 'PYEOF' > pizza_app/generator/order_factory.py
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
PYEOF

# -------------------------
# pizza_app/services/stage_order_received.py
# -------------------------
cat << 'PYEOF' > pizza_app/services/stage_order_received.py
from __future__ import annotations

import os
import random
import time
from typing import Iterable

from kafka import KafkaProducer

from pizza_app.config import kafka_settings
from pizza_app.generator.order_factory import create_random_order


def _iter_orders(rng: random.Random, max_orders: int | None = None) -> Iterable[str]:
    count = 0
    while True:
        event = create_random_order(rng)
        yield event.model_dump_json()
        count += 1
        if max_orders is not None and count >= max_orders:
            break


def run_simulation(max_orders: int = 10) -> None:
    seed = os.getenv("PIZZA_RANDOM_SEED")
    rng = random.Random()
    if seed is not None:
        rng.seed(int(seed))

    for raw in _iter_orders(rng, max_orders=max_orders):
        print(raw)


def run_kafka(max_orders: int = 10, delay_seconds: float = 1.0) -> None:
    seed = os.getenv("PIZZA_RANDOM_SEED")
    rng = random.Random()
    if seed is not None:
        rng.seed(int(seed))

    producer = KafkaProducer(
        bootstrap_servers=kafka_settings.bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    topic = kafka_settings.incoming_orders_topic

    for raw in _iter_orders(rng, max_orders=max_orders):
        producer.send(topic, raw)
        print(f"[producer] sent to {topic}: {raw}")
        time.sleep(delay_seconds)

    producer.flush()


if __name__ == "__main__":
    mode = os.getenv("RUN_MODE", "simulation")
    if mode == "kafka":
        run_kafka()
    else:
        run_simulation()
PYEOF

# -------------------------
# pizza_app/services/consumer_debug.py
# -------------------------
cat << 'PYEOF' > pizza_app/services/consumer_debug.py
from __future__ import annotations

import os

from kafka import KafkaConsumer

from pizza_app.config import kafka_settings


def run_consumer() -> None:
    topic = os.getenv("PIZZA_CONSUMER_TOPIC", kafka_settings.incoming_orders_topic)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_settings.bootstrap_servers,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=os.getenv("PIZZA_CONSUMER_GROUP", "pizza-debug-consumer"),
    )

    print(f"[consumer] Listening on topic '{topic}'...")
    for message in consumer:
        print(f"[consumer] {message.value}")


if __name__ == "__main__":
    run_consumer()
PYEOF

# -------------------------
# chapter10/dry_run_event_models.py
# -------------------------
cat << 'PYEOF' > chapter10/dry_run_event_models.py
from __future__ import annotations

import random

from pizza_app.generator.order_factory import create_random_order


def main() -> None:
    rng = random.Random(42)
    for _ in range(5):
        event = create_random_order(rng)
        print(event.model_dump_json())


if __name__ == "__main__":
    main()
PYEOF

# -------------------------
# chapter11/produce_orders_local_kafka.py
# -------------------------
cat << 'PYEOF' > chapter11/produce_orders_local_kafka.py
from __future__ import annotations

from pizza_app.services.stage_order_received import run_kafka


def main() -> None:
    run_kafka()


if __name__ == "__main__":
    main()
PYEOF

# -------------------------
# chapter11/consume_orders_local_kafka.py
# -------------------------
cat << 'PYEOF' > chapter11/consume_orders_local_kafka.py
from __future__ import annotations

from pizza_app.services.consumer_debug import run_consumer


def main() -> None:
    run_consumer()


if __name__ == "__main__":
    main()
PYEOF

# -------------------------
# requirements.txt
# -------------------------
cat << 'EOF' > requirements.txt
pydantic>=2.8
kafka-python>=2.0.2
python-dotenv>=1.0
EOF

# -------------------------
# .gitignore
# -------------------------
cat << 'EOF' > .gitignore
__pycache__/
*.py[cod]
*.pyo
*.pyd
.env
.env.*
.venv/
venv/
var/
.pytest_cache/
.mypy_cache/
.idea/
.vscode/
EOF

# -------------------------
# README.md
# -------------------------
cat << 'EOF' > README.md
paas-kafka-code
================

This repository contains the Python code for the Pizza-as-a-Service Kafka edition.

- Shared application package: pizza_app/
- Chapter-specific entrypoints: chapter10/, chapter11/, ...

Quick start:

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Dry-run event generation (no Kafka):

    python chapter10/dry_run_event_models.py

Produce events to a local Kafka running on localhost:9092:

    export RUN_MODE=kafka
    python chapter11/produce_orders_local_kafka.py

Consume events from the incoming-orders topic:

    python chapter11/consume_orders_local_kafka.py
EOF

echo "Done. Now create a venv, install requirements, and run chapter10/dry_run_event_models.py."
