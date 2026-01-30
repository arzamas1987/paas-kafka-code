from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml
from confluent_kafka.admin import AdminClient, NewTopic


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    config: Dict[str, str]


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"[FAIL] Missing required environment variable: {name}")
    return value


def _load_plan(path: str) -> tuple[str, List[TopicSpec]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise SystemExit("[FAIL] Topic plan YAML must be a mapping at the top level.")

    bootstrap_env = raw.get("bootstrap_env", "KAFKA_BOOTSTRAP_SERVERS")
    bootstrap = _require_env(str(bootstrap_env))

    topics_raw = raw.get("topics")
    if not isinstance(topics_raw, list):
        raise SystemExit("[FAIL] 'topics' must be a list.")

    specs: List[TopicSpec] = []
    for i, t in enumerate(topics_raw, start=1):
        if not isinstance(t, dict):
            raise SystemExit(f"[FAIL] topics[{i}] must be a mapping (got: {type(t).__name__}).")

        name = t.get("name")
        partitions = t.get("partitions")
        rf = t.get("replication_factor", 1)
        config = t.get("config", {})

        if not isinstance(name, str) or not name.strip():
            raise SystemExit(f"[FAIL] topics[{i}].name must be a non-empty string.")
        if not isinstance(partitions, int) or partitions <= 0:
            raise SystemExit(f"[FAIL] topics[{i}].partitions must be a positive integer.")
        if not isinstance(rf, int) or rf <= 0:
            raise SystemExit(f"[FAIL] topics[{i}].replication_factor must be a positive integer.")
        if config is None:
            config = {}
        if not isinstance(config, dict):
            raise SystemExit(f"[FAIL] topics[{i}].config must be a mapping.")
        config_str: Dict[str, str] = {str(k): str(v) for k, v in config.items()}

        specs.append(TopicSpec(name=name, partitions=partitions, replication_factor=rf, config=config_str))

    return bootstrap, specs


def _admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print("Usage: python -m chapter13.topic_plan_apply <topic-plan.yaml>")
        return 2

    plan_path = argv[1]
    bootstrap, specs = _load_plan(plan_path)
    admin = _admin(bootstrap)

    existing = admin.list_topics(timeout=10).topics
    to_create: List[NewTopic] = []
    for s in specs:
        if s.name in existing:
            md = existing[s.name]
            pcount = len(md.partitions) if md and md.partitions is not None else "?"
            print(f"[OK] Exists topic='{s.name}' partitions={pcount}")
            continue
        to_create.append(
            NewTopic(
                topic=s.name,
                num_partitions=s.partitions,
                replication_factor=s.replication_factor,
                config=s.config,
            )
        )

    if not to_create:
        print("[OK] No topics to create. Plan is already satisfied.")
        return 0

    futures = admin.create_topics(to_create, request_timeout=15)

    for nt, fut in zip(to_create, futures.values()):
        try:
            fut.result()
            print(f"[OK] Created topic='{nt.topic}' partitions={nt.num_partitions} rf={nt.replication_factor}")
        except Exception as e:
            print(f"[FAIL] Create topic='{nt.topic}' error={e}")
            return 1

    time.sleep(1.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
