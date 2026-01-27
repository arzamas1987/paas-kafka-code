from __future__ import annotations

from chapter14.cloud_stage_worker import StageSpec, run_stage
from pizza_app.services import lifecycle


def main() -> int:
    spec = StageSpec(
        name="prepared",
        input_topic=lifecycle.TOPIC_VALIDATED,
        output_topic=lifecycle.TOPIC_PREPARED,
        group_id="ch14-action07-prepared",
        transform_fn=lifecycle.transform_to_prepared,
    )
    return run_stage(spec)


if __name__ == "__main__":
    raise SystemExit(main())
