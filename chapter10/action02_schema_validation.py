from __future__ import annotations

"""
Action 10.2 – Schema validation examples for OrderReceived.

This script demonstrates how Pydantic enforces the event contract.
It tries to load several JSON payloads — some valid, some invalid —
and prints the resulting errors.

Run with:
    python -m chapter10.action02_schema_validation
"""

import json
from datetime import datetime, timezone

from pizza_app.models.order_events import OrderReceived


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def validate_json(label: str, payload: str) -> None:
    """Try to validate a JSON string using the OrderReceived model."""
    _print_header(label)
    print("Input JSON:", payload)

    try:
        obj = OrderReceived.model_validate_json(payload)
        print("✓ VALID event")
        print("Parsed model:", obj.model_dump_json())
    except Exception as e:
        print("✗ INVALID event")
        print("Error message:")
        print(e)


def main() -> None:
    # 1. Correct/valid payload
    valid_payload = json.dumps(
        {
            "order_id": "A9001",
            "event_type": "OrderReceived",
            "order_type": "delivery",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": "COR-9001",
            "trace_id": "TRACE-9001",
            "customer_id": "CUS-100",
            "items": [
                {"sku": "pizza-margherita", "qty": 1},
                {"sku": "drink-cola", "qty": 1},
            ],
            "address": {
                "street": "Tomato Alley 1",
                "city": "Pizzaburg",
                "zone": "west",
            },
            "table_number": None,
        }
    )
    validate_json("1. VALID EVENT", valid_payload)

    # 2. Missing required field: order_id
    missing_order_id = json.dumps(
        {
            "event_type": "OrderReceived",
            "order_type": "delivery",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": "COR-9002",
            "trace_id": "TRACE-9002",
            "items": [],
            "address": None,
            "table_number": None,
        }
    )
    validate_json("2. MISSING order_id", missing_order_id)

    # 3. Wrong type for 'qty' (string instead of int)
    qty_string = json.dumps(
        {
            "order_id": "A9003",
            "event_type": "OrderReceived",
            "order_type": "delivery",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": "COR-9003",
            "trace_id": "TRACE-9003",
            "items": [
                {"sku": "pizza-funghi", "qty": "two"},  # WRONG
            ],
            "address": None,
            "table_number": None,
        }
    )
    validate_json("3. WRONG TYPE for qty", qty_string)

    # 4. Wrong enum: order_type must be "delivery" | "onsite" | "eat-in"
    wrong_enum = json.dumps(
        {
            "order_id": "A9004",
            "event_type": "OrderReceived",
            "order_type": "pickup",  # WRONG ENUM
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": "COR-9004",
            "trace_id": "TRACE-9004",
            "items": [],
            "address": None,
            "table_number": None,
        }
    )
    validate_json("4. WRONG ENUM for order_type", wrong_enum)

    # 5. Wrong structure: address must be object or null
    wrong_address = json.dumps(
        {
            "order_id": "A9005",
            "event_type": "OrderReceived",
            "order_type": "delivery",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlation_id": "COR-9005",
            "trace_id": "TRACE-9005",
            "items": [],
            "address": "I am not an object",  # WRONG TYPE
            "table_number": None,
        }
    )
    validate_json("5. WRONG TYPE for address", wrong_address)

    # 6. Multiple errors at once
    multiple_errors = json.dumps(
        {
            "order_id": 999,  # wrong type
            "event_type": "OrderReceived",
            "order_type": "eat-in",
            "timestamp": "not-a-timestamp",  # wrong format
            "correlation_id": 123,  # wrong type
            "trace_id": True,  # wrong type
            "items": [
                {"sku": 999, "qty": "abc"},  # wrong types
            ],
            "address": {
                "street": 5,  # wrong type
                "city": None,  # wrong type
                "zone": False,  # wrong type
            },
            "table_number": "table-3",  # wrong type
        }
    )
    validate_json("6. MULTIPLE ERRORS", multiple_errors)


if __name__ == "__main__":
    main()
