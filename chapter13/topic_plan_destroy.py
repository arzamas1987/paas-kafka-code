from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List

import yaml
from confluent_kafka.admin import AdminClient


@dataclass(frozen=True)
class TopicRef:
    name: str


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"[FAIL] Missing required environment variable: {name}")
    return value


def _load_plan(path: str) -> tuple[str, List[TopicRef]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise SystemExit("[FAIL] Topic plan YAML must be a mapping at the top level.")

    bootstrap_env = raw.get("bootstrap_env", "KAFKA_BOOTSTRAP_SERVERS")
    bootstrap = _require_env(str(bootstrap_env))

    topics_raw = raw.get("topics")
    if not isinstance(topics_raw, list):
        raise SystemExit("[FAIL] 'topics' must be a list.")

    refs: List[TopicRef] = []
    for i, t in enumerate(topics_raw, start=1):
        if not isinstance(t, dict):
            raise SystemExit(f"[FAIL] topics[{i}] must be a mapping.")
        name = t.get("name")
        if not isinstance(name, str) or not name.strip():
            raise SystemExit(f"[FAIL] topics[{i}].name must be a non-empty string.")
        refs.append(TopicRef(name=name))

    return bootstrap, refs


def _admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})


def main(argv: List[str]) -> int:
    if len(argv) not in (2, 3):
        print("Usage: python -m chapter13.topic_plan_destroy <topic-plan.yaml> --yes")
        return 2

    plan_path = argv[1]
    confirmed = len(argv) == 3 and argv[2] == "--yes"
    if not confirmed:
        print("[FAIL] Destructive operation. Re-run with --yes to confirm.")
        return 2

    bootstrap, refs = _load_plan(plan_path)
    admin = _admin(bootstrap)

    topics = [r.name for r in refs]
    futures = admin.delete_topics(topics, operation_timeout=30, request_timeout=30)

    ok = True
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"[OK] Requested delete topic='{name}'")
        except Exception as e:
            print(f"[FAIL] Delete topic='{name}' error={e}")
            ok = False

    time.sleep(1.0)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
