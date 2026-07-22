from __future__ import annotations

from dev_health_ops.sync.datasets import (
    CostClass,
    DatasetKey,
    WatermarkBehavior,
    get_dataset_spec,
)


def test_jira_incidents_are_a_canonical_incremental_medium_dataset() -> None:
    spec = get_dataset_spec("jira", DatasetKey.INCIDENTS.value)

    assert spec is not None
    assert spec.legacy_targets == frozenset({"operational"})
    assert spec.default_cost_class is CostClass.MEDIUM
    assert spec.watermark_behavior is WatermarkBehavior.INCREMENTAL
    assert spec.processor_flags == {}
