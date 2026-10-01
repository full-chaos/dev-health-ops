"""CHAOS-7476: the Python ClickHouse sink writers for the daily metrics tables stay deleted.

``write_repo_metrics``, ``write_commit_metrics``, ``write_user_metrics`` and
``write_ic_landscape_rolling`` passed a row with an empty ``org_id`` to ClickHouse unchanged when
the sink had no org id (CHAOS-7241). Their callers were deleted with the Python daily compute
(CHAOS-5308); the Go ``repo_user_commit`` and ``ic_finalize`` families own these tables and refuse
an empty organization. A writer that comes back must be written with that refusal, so this test
fails until the names are removed from this list on purpose.
"""

from __future__ import annotations

import pytest

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

DELETED_WRITERS = (
    "write_repo_metrics",
    "write_commit_metrics",
    "write_user_metrics",
    "write_ic_landscape_rolling",
)


@pytest.mark.parametrize("name", DELETED_WRITERS)
def test_sink_no_longer_exposes_the_daily_metrics_writer(name: str) -> None:
    assert not hasattr(ClickHouseMetricsSink, name)
    assert not hasattr(BaseMetricsSink, name)
