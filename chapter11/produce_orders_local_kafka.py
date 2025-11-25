from __future__ import annotations

from pizza_app.services.stage_order_received import run_kafka


def main() -> None:
    run_kafka()


if __name__ == "__main__":
    main()
