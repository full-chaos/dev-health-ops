"""
DoraMixin — DORA metrics and testops benchmark/comparison write methods.

Tables: dora_metrics_daily, testops_period_comparisons,
        testops_metric_baselines, testops_maturity_bands,
        testops_metric_anomalies, testops_metric_correlations,
        testops_benchmark_insights.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import DORAMetricsRecord
from dev_health_ops.metrics.testops_schemas import (
    BenchmarkAnomalyRecord,
    BenchmarkBaselineRecord,
    BenchmarkInsightRecord,
    MaturityBandRecord,
    MetricCorrelationRecord,
    PeriodComparisonRecord,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class DoraMixin(_ClickHouseSinkBase):
    """Mixin for DORA and benchmark/comparison write methods."""

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

    def write_period_comparisons(self, rows: Sequence[PeriodComparisonRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_period_comparisons",
            [
                "metric_name",
                "scope_type",
                "scope_key",
                "current_period_start",
                "current_period_end",
                "comparison_period_start",
                "comparison_period_end",
                "current_value",
                "comparison_value",
                "absolute_delta",
                "percentage_change",
                "trend_direction",
                "org_id",
                "computed_at",
            ],
            rows,
        )

    def write_benchmark_baselines(
        self, rows: Sequence[BenchmarkBaselineRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_metric_baselines",
            [
                "metric_name",
                "scope_type",
                "scope_key",
                "period_start",
                "period_end",
                "mean_value",
                "median_value",
                "p25_value",
                "p75_value",
                "p90_value",
                "std_dev",
                "sample_size",
                "org_id",
                "computed_at",
            ],
            rows,
        )

    def write_maturity_bands(self, rows: Sequence[MaturityBandRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_maturity_bands",
            [
                "metric_name",
                "scope_type",
                "scope_key",
                "period_start",
                "period_end",
                "value",
                "percentile_rank",
                "maturity_band",
                "confidence",
                "org_id",
                "computed_at",
            ],
            rows,
        )

    def write_benchmark_anomalies(self, rows: Sequence[BenchmarkAnomalyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_metric_anomalies",
            [
                "metric_name",
                "scope_type",
                "scope_key",
                "day",
                "value",
                "baseline_value",
                "z_score",
                "anomaly_type",
                "direction",
                "severity",
                "volatility_score",
                "org_id",
                "computed_at",
            ],
            rows,
        )

    def write_metric_correlations(
        self, rows: Sequence[MetricCorrelationRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_metric_correlations",
            [
                "metric_name",
                "paired_metric_name",
                "scope_type",
                "scope_key",
                "period_start",
                "period_end",
                "coefficient",
                "p_value",
                "sample_size",
                "is_significant",
                "interpretation",
                "org_id",
                "computed_at",
            ],
            rows,
        )

    def write_benchmark_insights(self, rows: Sequence[BenchmarkInsightRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_benchmark_insights",
            [
                "insight_id",
                "insight_type",
                "scope_type",
                "scope_key",
                "metric_name",
                "paired_metric_name",
                "period_start",
                "period_end",
                "severity",
                "summary",
                "evidence_json",
                "org_id",
                "computed_at",
            ],
            rows,
        )
