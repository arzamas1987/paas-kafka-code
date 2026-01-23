# chapter14/apply_topic_plan.py
# Generic: create/validate topics from a plan file (used by Action 03 and later actions).

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

from confluent_kafka import KafkaException

from chapter14.action02_03_cloud_config import CloudKafkaSettings, build_admin
from chapter14.topic_plan import load_topic_plan, to_new_topics


DEFAULT_REPLICATION_FACTOR = 3


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("[ERROR] Missing plan path. Usage: python -m chapter14.apply_topic_plan <path/to/topic-plan.yaml>")
        return 2

    plan_path = Path(argv[1])

    settings = CloudKafkaSettings.from_env()
    admin = build_admin(settings)

    specs = load_topic_plan(plan_path)
    new_topics = to_new_topics(specs, replication_factor=DEFAULT_REPLICATION_FACTOR)

    futures = admin.create_topics(new_topics, request_timeout=30)

    any_failed = False
    for nt in new_topics:
        fut = futures.get(nt.topic)
        if fut is None:
            print(f"[ERROR] No result returned for topic='{nt.topic}'")
            any_failed = True
            continue

        try:
            fut.result(timeout=30)
            print(f"[OK] Created topic='{nt.topic}' partitions={nt.num_partitions}")
        except KafkaException as e:
            msg = str(e)
            if "TOPIC_ALREADY_EXISTS" in msg or "TopicExistsError" in msg:
                print(f"[OK] Topic already exists: '{nt.topic}'")
            else:
                print(f"[ERROR] Failed to create topic '{nt.topic}': {e}")
                any_failed = True
        except Exception as e:
            print(f"[ERROR] Failed to create topic '{nt.topic}': {e}")
            any_failed = True

    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
