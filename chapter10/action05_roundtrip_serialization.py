from __future__ import annotations

"""
Action 10.5 – Serialization roundtrip.

This script shows how an OrderReceived event:
- is turned into JSON
- and then reconstructed from JSON

Run with:
    python -m chapter10.action05_roundtrip_serialization
"""

import random

from pizza_app.generator.order_factory import create_random_order
from pizza_app.models.order_events import OrderReceived


def main() -> None:
    rng = random.Random(123)

    event = create_random_order(rng)
    print("Original model:")
    print(event)

    json_payload = event.model_dump_json()
    print("\nAs JSON string:")
    print(json_payload)

    restored = OrderReceived.model_validate_json(json_payload)
    print("\nRestored model:")
    print(restored)

    print("\nEqual by value? ->", event == restored)


if __name__ == "__main__":
    main()
