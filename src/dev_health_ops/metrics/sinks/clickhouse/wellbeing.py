"""
WellbeingMixin — user metrics write methods.

Tables: user_metrics_daily.

CHAOS-5245 deleted write_quality_drag/write_pipeline_stability (testops_risk's
Python compute+write) -- the native Go executor (CHAOS-4294) has no fallback
left.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import UserMetricsDailyRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class WellbeingMixin(_ClickHouseSinkBase):
    """Mixin for wellbeing/quality-of-developer-life write methods."""

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
                "pr_interruption_load",
                "context_spread_count",
                "review_request_load",
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
