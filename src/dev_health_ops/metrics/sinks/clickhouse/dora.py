"""
DoraMixin — DORA metrics write methods.

Tables: dora_metrics_daily.

CHAOS-4288 deleted this mixin's testops_period_comparisons/
testops_metric_baselines/testops_maturity_bands/testops_metric_anomalies/
testops_metric_correlations/testops_benchmark_insights write methods --
benchmarking's Python compute (src/dev_health_ops/metrics/benchmarking/) is
gone entirely, the native BenchmarkingFinalizeExecutor has no fallback left
to feed these.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import DORAMetricsRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class DoraMixin(_ClickHouseSinkBase):
    """Mixin for DORA write methods."""

    def write_dora_metrics(self, rows: Sequence[DORAMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "dora_metrics_daily",
            [
                "repo_id",
                "day",
                "metric_name",
                "value",
                "computed_at",
                "org_id",
            ],
            rows,
        )
