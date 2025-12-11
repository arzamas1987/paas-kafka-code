# chapter12/action03_producer_incoming_orders.py

"""
Action 3 – Producer for `incoming-orders`.

This module is only a thin wrapper around the real producer service
`pizza_app.services.stage_order_received`.

Usage: python -m chapter12.action03_producer_incoming_orders

Keeping the behaviour in the service module makes it reusable later
for Docker containers and Confluent deployments, while this file
remains a simple "how to run it from Chapter 12" entry point.
"""

from pizza_app.services.stage_order_received import main as run_order_received_producer


def main() -> None:
    run_order_received_producer()


if __name__ == "__main__":
    main()
