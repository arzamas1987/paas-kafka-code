# chapter12/action01_check_connection.py

from __future__ import annotations

import logging

from pizza_app.kafka_helpers import check_connection, get_bootstrap_servers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main() -> None:
    """
    Action 12.1 – Check Python ↔ Kafka connectivity for the active profile.

    Usage:
        python -m chapter12.action01_check_connection
    """
    bootstrap = get_bootstrap_servers()
    ok = check_connection(bootstrap)

    if not ok:
        # The book will explain the common causes (broker not running, wrong port, etc.)
        raise SystemExit(
            f"Could not connect to Kafka at {bootstrap}. "
            "Make sure the single-node broker from Chapter 11 is running."
        )


if __name__ == "__main__":
    main()
