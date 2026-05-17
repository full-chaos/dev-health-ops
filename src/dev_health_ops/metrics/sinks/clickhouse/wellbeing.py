"""
WellbeingMixin — user metrics, quality drag, and pipeline stability.

Tables: user_metrics_daily, testops_quality_drag, testops_pipeline_stability.
"""
from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from dev_health_ops.metrics.schemas import UserMetricsDailyRecord
from dev_health_ops.metrics.testops_schemas import (
    PipelineStabilityRecord,
    QualityDragRecord,
)
from dev_health_ops.metrics.sinks.clickhouse._insert import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)


class WellbeingMixin:
    """Mixin for wellbeing/quality-of-developer-life write methods."""

    if TYPE_CHECKING:
        client: Any
        org_id: str

        def _insert_rows(
            self,
            table: str,
            columns: list[str],
            rows: Any,
            batch_size: int = DEFAULT_BATCH_SIZE,
        ) -> None: ...

    def write_user_metrics(self, rows: Sequence[UserMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "user_metrics_daily",
            [
                "repo_id",
                "day",
                "author_email",
                "commits_count",
                "loc_added",
                "loc_deleted",
                "files_changed",
                "large_commits_count",
                "avg_commit_size_loc",
                "prs_authored",
                "prs_merged",
                "avg_pr_cycle_hours",
                "median_pr_cycle_hours",
                "pr_cycle_p75_hours",
                "pr_cycle_p90_hours",
                "prs_with_first_review",
                "pr_first_review_p50_hours",
                "pr_first_review_p90_hours",
                "pr_review_time_p50_hours",
                "pr_pickup_time_p50_hours",
                "reviews_given",
                "changes_requested_given",
                "reviews_received",
                "review_reciprocity",
                "team_id",
                "team_name",
                "active_hours",
                "weekend_days",
                "identity_id",
                "loc_touched",
                "prs_opened",
                "work_items_completed",
                "work_items_active",
                "delivery_units",
                "cycle_p50_hours",
                "cycle_p90_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_quality_drag(self, rows: Sequence[QualityDragRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_quality_drag",
            [
                "repo_id",
                "day",
                "drag_hours",
                "failure_rework_hours",
                "flake_investigation_hours",
                "queue_wait_hours",
                "retry_overhead_hours",
                "factors_json",
                "team_id",
                "service_id",
                "org_id",
                "computed_at",
            ],
            rows,
        )

    def write_pipeline_stability(self, rows: Sequence[PipelineStabilityRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "testops_pipeline_stability",
            [
                "repo_id",
                "day",
                "stability_index",
                "success_rate_7d",
                "success_rate_trend",
                "failure_clustering_score",
                "median_recovery_time_seconds",
                "team_id",
                "service_id",
                "org_id",
                "computed_at",
            ],
            rows,
        )
