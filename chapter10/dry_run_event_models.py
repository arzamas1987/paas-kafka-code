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
