from __future__ import annotations

from chapter14.confluent.cloud_stage_worker import StageSpec, run_stage
from pizza_app.services import lifecycle


def _identity(event):
    return event


def main() -> int:
    spec = StageSpec(
        name="observe-ready",
        input_topic=lifecycle.TOPIC_READY,
        output_topic=None,
        group_id="ch14-action07-observer-ready",
        transform_fn=_identity,
    )
    return run_stage(spec)


if __name__ == "__main__":
    raise SystemExit(main())
