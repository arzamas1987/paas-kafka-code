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
