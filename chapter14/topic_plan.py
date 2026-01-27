# chapter14/topic_plan.py
# Shared topic-plan helpers for Chapter 14 (used by multiple actions).

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml
from confluent_kafka.admin import NewTopic


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: Dict[str, str]


def load_topic_plan(plan_path: Path) -> List[TopicSpec]:
    if not plan_path.exists():
        raise FileNotFoundError(f"Topic plan not found: {plan_path}")

    with plan_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("Invalid topic plan: expected a YAML mapping at the top level.")

    topics = data.get("topics")
    if not isinstance(topics, list):
        raise ValueError("Invalid topic plan: expected a top-level 'topics' list.")

    specs: List[TopicSpec] = []
    for i, t in enumerate(topics, start=1):
        if not isinstance(t, dict):
            raise ValueError(f"Invalid topic entry #{i}: expected a mapping.")

        name = t.get("name")
        partitions = t.get("partitions")
        config = t.get("config", {})

        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Invalid topic entry #{i}: 'name' must be a non-empty string.")
        if not isinstance(partitions, int) or partitions <= 0:
            raise ValueError(f"Invalid topic entry '{name}': 'partitions' must be a positive integer.")
        if config is None:
            config = {}
        if not isinstance(config, dict):
            raise ValueError(f"Invalid topic entry '{name}': 'config' must be a mapping/dict.")

        config_str: Dict[str, str] = {}
        for k, v in config.items():
            if k is None:
                continue
            config_str[str(k)] = "" if v is None else str(v)

        specs.append(TopicSpec(name=name.strip(), partitions=partitions, config=config_str))

    return specs


def to_new_topics(specs: List[TopicSpec], replication_factor: int) -> List[NewTopic]:
    if replication_factor <= 0:
        raise ValueError("replication_factor must be a positive integer.")
    return [
        NewTopic(
            topic=s.name,
            num_partitions=s.partitions,
            replication_factor=replication_factor,
            config=s.config,
        )
        for s in specs
    ]
