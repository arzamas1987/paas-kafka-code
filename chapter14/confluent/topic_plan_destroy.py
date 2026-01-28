from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import yaml

from chapter14.confluent.cloud_config import CloudKafkaSettings, build_admin


def _load_topic_names(path: Path) -> List[str]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "topics" not in raw:
        raise ValueError("Topic plan must contain 'topics:' list.")
    topics = raw["topics"]
    if not isinstance(topics, list) or not topics:
        raise ValueError("Topic plan must contain a non-empty 'topics:' list.")

    names: List[str] = []
    for t in topics:
        if isinstance(t, dict) and "name" in t:
            names.append(str(t["name"]).strip())

    if not names:
        raise ValueError("No topic names found.")
    return names


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print("Usage: python -m chapter14.topic_plan_destroy <plan.yaml>")
        return 2

    plan_path = Path(argv[1])
    topics = _load_topic_names(plan_path)

    settings = CloudKafkaSettings.from_env()
    admin = build_admin(settings)

    futures = admin.delete_topics(topics, request_timeout=20)

    ok = True
    for topic_name, fut in futures.items():
        try:
            fut.result()
            print(f"[OK] Deleted topic='{topic_name}'")
        except Exception as e:
            msg = str(e)
            if "UNKNOWN_TOPIC_OR_PART" in msg or "unknown topic" in msg.lower():
                print(f"[OK] Topic already absent: '{topic_name}'")
            else:
                print(f"[FAIL] Delete topic='{topic_name}' error={e}")
                ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
