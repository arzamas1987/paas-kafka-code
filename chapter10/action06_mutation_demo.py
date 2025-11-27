from __future__ import annotations

"""
Action 10.6 – Mutation demo (why events should be treated as immutable).

This script:
- creates an OrderReceived event
- mutates its items in-place
- shows how this can silently change data "behind your back"

Run with:
    python -m chapter10.action06_mutation_demo
"""

import random

from pizza_app.generator.order_factory import create_random_order
from pizza_app.models.base import Item


def main() -> None:
    rng = random.Random(7)

    event = create_random_order(rng)
    print("Original event JSON:")
    print(event.model_dump_json())

    # BAD PRACTICE: mutate the event in place
    print("\n--- Mutating the event in place (ANTI-PATTERN) ---")
    event.items.append(Item(sku="pizza-margherita", qty=10))
    print("After first mutation:")
    print(event.model_dump_json())

    # Show that references share the same object
    print("\n--- Copying the reference (same object) ---")
    same_reference = event
    same_reference.items.append(Item(sku="pizza-pepperoni", qty=5))

    print("Original event after second mutation:")
    print(event.model_dump_json())

    print(
        "\nNOTE: Both 'event' and 'same_reference' point to the same object.\n"
        "In an event-driven system, this kind of mutation can cause subtle bugs.\n"
        "In later chapters we always create NEW events instead of mutating old ones."
    )


if __name__ == "__main__":
    main()
