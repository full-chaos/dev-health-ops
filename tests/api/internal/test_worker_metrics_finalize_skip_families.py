"""CHAOS-4290: the finalize bridge request accepts skip_families.

Before this change the validator REFUSED it outright --
``if self.operation != "partition" and self.skip_families: raise`` -- so a Go
FinalizeHandler that started naming its native families would have had the
request rejected (a 422, which the Go classifier surfaces as Refused). The Go
side therefore could not skip anything on finalize even in principle.
"""

from __future__ import annotations

import uuid

import pytest

from dev_health_ops.api.internal.worker_metrics import DailyMetricsExecutionRequest

RUN_ID = uuid.uuid4()
PARTITION_ID = uuid.uuid4()


def test_finalize_accepts_skip_families() -> None:
    request = DailyMetricsExecutionRequest(
        operation="finalize", run_id=RUN_ID, skip_families=["ic_finalize"]
    )
    assert request.skip_families == ["ic_finalize"]


def test_partition_still_accepts_skip_families() -> None:
    """The pre-existing contract is untouched by the widening."""
    request = DailyMetricsExecutionRequest(
        operation="partition",
        run_id=RUN_ID,
        partition_id=PARTITION_ID,
        skip_families=["team_wellbeing"],
    )
    assert request.skip_families == ["team_wellbeing"]


def test_finalize_still_refuses_a_partition_id() -> None:
    """Widening skip_families must not loosen the partition_id pairing.

    Without this, a change that relaxed the whole validator would pass the two
    tests above and silently drop an unrelated invariant.
    """
    with pytest.raises(
        ValueError, match="partition_id must be supplied only for partition"
    ):
        DailyMetricsExecutionRequest(
            operation="finalize", run_id=RUN_ID, partition_id=PARTITION_ID
        )


def test_finalize_without_skip_families_defaults_to_empty() -> None:
    request = DailyMetricsExecutionRequest(operation="finalize", run_id=RUN_ID)
    assert request.skip_families == []
