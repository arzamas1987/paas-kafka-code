from __future__ import annotations

"""
Action 10.1 – Dry run: generate sample OrderReceived events.

Run with:
    python -m chapter10.action01_dry_run_events
"""

import random

from pizza_app.generator.order_factory import create_random_order


def main() -> None:
    rng = random.Random(42)

    events_to_generate = 5

    for _ in range(events_to_generate):
        event = create_random_order(rng)
        print(event.model_dump_json())


if __name__ == "__main__":
    main()
