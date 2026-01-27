from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml
from confluent_kafka.admin import NewTopic, ConfigResource, RESOURCE_TOPIC

from chapter14.ch14_cloud_config import CloudKafkaSettings, build_admin


def _load_plan(path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Topic plan must be a YAML mapping.")
    topics = raw.get("topics", [])
    if not isinstance(topics, list) or not topics:
        raise ValueError("Topic plan must contain a non-empty 'topics:' list.")
    defaults = raw.get("defaults", {}) or {}
    return defaults, topics


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print("Usage: python -m chapter14.topic_plan_apply <plan.yaml>")
        return 2

    plan_path = Path(argv[1])
    defaults, topics = _load_plan(plan_path)

    d_partitions = int(defaults.get("partitions", 3))
    d_rf = int(defaults.get("replication_factor", 3))
    d_cfg = defaults.get("config", {}) or {}

    settings = CloudKafkaSettings.from_env()
    admin = build_admin(settings)

    md = admin.list_topics(timeout=10)
    existing = set((md.topics or {}).keys())

    to_create: List[NewTopic] = []
    for t in topics:
        name = str(t.get("name", "")).strip()
        if not name:
            raise ValueError("Each topic entry must have a non-empty 'name'.")
        if name in existing:
            continue

        partitions = int(t.get("partitions", d_partitions))
        rf = int(t.get("replication_factor", d_rf))
        cfg = dict(d_cfg)
        cfg.update(t.get("config", {}) or {})

        to_create.append(NewTopic(name, num_partitions=partitions, replication_factor=rf, config=cfg))

    if to_create:
        futures = admin.create_topics(to_create, request_timeout=20)

        # IMPORTANT: futures keys are topic-name strings, not NewTopic objects.
        for topic_name, fut in futures.items():
            try:
                fut.result()
                requested = next((nt.num_partitions for nt in to_create if nt.topic == topic_name), None)
                if requested is None:
                    print(f"[OK] Created topic='{topic_name}'")
                else:
                    print(f"[OK] Created topic='{topic_name}' partitions={requested}")
            except Exception as e:
                msg = str(e)
                if "TOPIC_ALREADY_EXISTS" in msg or "already exists" in msg.lower():
                    print(f"[OK] Topic already exists: '{topic_name}'")
                else:
                    print(f"[FAIL] Create topic='{topic_name}' error={e}")
                    return 1
    else:
        print("[OK] No missing topics. All topics already exist.")

    time.sleep(1.0)

    print("")
    print("Facts (name, partitions):")
    md2 = admin.list_topics(timeout=10)
    for t in topics:
        name = str(t["name"]).strip()
        tm = (md2.topics or {}).get(name)
        if tm is None:
            print(f"- topic='{name}' status=MISSING")
            continue
        partitions = len(tm.partitions or {})
        print(f"- topic='{name}' partitions={partitions}")

    cfg_keys = sorted(list((d_cfg or {}).keys()))
    if cfg_keys:
        print("")
        print("Facts (selected configs from defaults.config):")
        resources = [ConfigResource(RESOURCE_TOPIC, str(t["name"]).strip()) for t in topics]
        described = admin.describe_configs(resources)

        for res, fut in described.items():
            try:
                cfg = fut.result(timeout=20)
                picked = {k: (cfg[k].value if k in cfg else None) for k in cfg_keys}
                print(f"- topic='{res.name}' {picked}")
            except Exception as e:
                print(f"- topic='{res.name}' describe_configs_error={e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
