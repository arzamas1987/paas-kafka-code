# chapter14/destroy_topic_plan.py
# Generic topic-plan destroy: deletes the topics listed in a plan file.
# Safety latch: requires --yes or PIZZA_CONFIRM_DELETE="YES".

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List

from confluent_kafka import KafkaException

from chapter14.action02_03_cloud_config import CloudKafkaSettings, build_admin
from chapter14.topic_plan import load_topic_plan


def confirmed(argv: List[str]) -> bool:
    if "--yes" in argv:
        return True
    return os.getenv("PIZZA_CONFIRM_DELETE", "").strip().upper() == "YES"


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("[ERROR] Missing plan path. Usage: python -m chapter14.destroy_topic_plan <path/to/topic-plan.yaml> --yes")
        return 2

    if not confirmed(argv):
        print("[ABORT] Deletion not confirmed. Re-run with '--yes' or set PIZZA_CONFIRM_DELETE='YES'.")
        return 2

    plan_path = Path(argv[1])

    settings = CloudKafkaSettings.from_env()
    admin = build_admin(settings)

    specs = load_topic_plan(plan_path)
    topic_names = [s.name for s in specs]

    if not topic_names:
        print("[OK] No topics listed in the plan.")
        return 0

    futures = admin.delete_topics(topic_names, request_timeout=30)

    any_failed = False
    for name in topic_names:
        fut = futures.get(name)
        if fut is None:
            print(f"[ERROR] No result returned for topic='{name}'")
            any_failed = True
            continue

        try:
            fut.result(timeout=30)
            print(f"[OK] Deleted topic '{name}'")
        except KafkaException as e:
            msg = str(e)
            if "UNKNOWN_TOPIC_OR_PART" in msg or "UnknownTopicOrPartition" in msg:
                print(f"[OK] Topic does not exist: '{name}'")
            else:
                print(f"[ERROR] Failed to delete topic '{name}': {e}")
                any_failed = True
        except Exception as e:
            print(f"[ERROR] Failed to delete topic '{name}': {e}")
            any_failed = True

    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
