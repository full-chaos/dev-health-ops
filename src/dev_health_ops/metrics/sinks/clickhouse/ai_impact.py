from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import AIImpactMetricsDailyRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


_COLUMNS = [
    "org_id",
    "team_id",
    "repo_id",
    "work_type",
    "day",
    "attribution_bucket",
    "prs_total",
    "prs_merged",
    "ai_assisted_prs",
    "agent_created_prs",
    "human_prs",
    "unknown_prs",
    "ai_assisted_pr_ratio",
    "agent_created_pr_count",
    "cycle_time_avg_hours",
    "baseline_cycle_time_avg_hours",
    "ai_cycle_time_delta_hours",
    "reviews_per_pr",
    "baseline_reviews_per_pr",
    "ai_review_amplification",
    "changes_requested_per_pr",
    "rework_prs",
    "rework_drag_rate",
    "followup_commits_count",
    "revert_prs",
    "revert_rate",
    "incidents_count",
    "incident_drag_rate",
    "test_gap_prs",
    "test_gap_rate",
    "leverage_prs_component",
    "leverage_cycle_time_component",
    "leverage_review_component",
    "leverage_rework_component",
    "leverage_test_component",
    "leverage_incident_component",
    "computed_at",
]


class AIImpactMixin(_ClickHouseSinkBase):
    def write_ai_impact_metrics(
        self, rows: Sequence[AIImpactMetricsDailyRecord]
    ) -> None:
        if not rows:
            return

        class _RowAdapter:
            def __init__(self, row: AIImpactMetricsDailyRecord) -> None:
                self.org_id = row.org_id
                self.team_id = row.team_id or ""
                self.repo_id = row.repo_id
                self.work_type = row.work_type
                self.day = row.day
                self.attribution_bucket = row.attribution_bucket
                self.prs_total = row.prs_total
                self.prs_merged = row.prs_merged
                self.ai_assisted_prs = row.ai_assisted_prs
                self.agent_created_prs = row.agent_created_prs
                self.human_prs = row.human_prs
                self.unknown_prs = row.unknown_prs
                self.ai_assisted_pr_ratio = row.ai_assisted_pr_ratio
                self.agent_created_pr_count = row.agent_created_pr_count
                self.cycle_time_avg_hours = row.cycle_time_avg_hours
                self.baseline_cycle_time_avg_hours = row.baseline_cycle_time_avg_hours
                self.ai_cycle_time_delta_hours = row.ai_cycle_time_delta_hours
                self.reviews_per_pr = row.reviews_per_pr
                self.baseline_reviews_per_pr = row.baseline_reviews_per_pr
                self.ai_review_amplification = row.ai_review_amplification
                self.changes_requested_per_pr = row.changes_requested_per_pr
                self.rework_prs = row.rework_prs
                self.rework_drag_rate = row.rework_drag_rate
                self.followup_commits_count = row.followup_commits_count
                self.revert_prs = row.revert_prs
                self.revert_rate = row.revert_rate
                self.incidents_count = row.incidents_count
                self.incident_drag_rate = row.incident_drag_rate
                self.test_gap_prs = row.test_gap_prs
                self.test_gap_rate = row.test_gap_rate
                self.leverage_prs_component = row.leverage.prs_component
                self.leverage_cycle_time_component = row.leverage.cycle_time_component
                self.leverage_review_component = row.leverage.review_component
                self.leverage_rework_component = row.leverage.rework_component
                self.leverage_test_component = row.leverage.test_component
                self.leverage_incident_component = row.leverage.incident_component
                self.computed_at = row.computed_at

        # _insert_rows expects dataclasses, so build explicit dictionaries here.
        matrix = []
        for row in rows:
            adapter = _RowAdapter(row)
            matrix.append([getattr(adapter, column) for column in _COLUMNS])
        self.client.insert("ai_impact_metrics_daily", matrix, column_names=_COLUMNS)
