from __future__ import annotations

from chapter14.confluent.cloud_stage_worker import StageSpec, run_stage
from pizza_app.services import lifecycle


def main() -> int:
    spec = StageSpec(
        name="validated",
        input_topic=lifecycle.TOPIC_INCOMING,
        output_topic=lifecycle.TOPIC_VALIDATED,
        group_id="ch14-action07-validated",
        transform_fn=lifecycle.transform_to_validated,
    )
    return run_stage(spec)


if __name__ == "__main__":
    raise SystemExit(main())
