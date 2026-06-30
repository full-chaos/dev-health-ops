"""
WorkGraphMixin — work graph, work items, repo/file/commit metrics, and forecasts.

Tables: repo_metrics_daily, ic_landscape_rolling_30d, file_metrics_daily,
        commit_metrics, team_metrics_daily, work_item_metrics_daily,
        work_item_user_metrics_daily, work_item_cycle_times,
        work_item_state_durations_daily, work_item_dependencies,
        work_item_reopen_events, work_item_interactions, sprints, worklogs,
        review_edges_daily, file_complexity_snapshots, repo_complexity_daily,
        file_hotspot_daily, work_graph_edges, work_graph_issue_pr,
        work_graph_pr_commit, work_items, work_item_transitions,
        capacity_forecasts.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from dev_health_ops.metrics.schemas import (
    CapacityForecastRecord,
    CommitMetricsRecord,
    FileComplexitySnapshot,
    FileHotspotDaily,
    FileMetricsRecord,
    ICLandscapeRollingRecord,
    RepoComplexityDaily,
    RepoMetricsDailyRecord,
    ReviewEdgeDailyRecord,
    TeamMetricsDailyRecord,
    WorkGraphEdgeRecord,
    WorkGraphIssuePRRecord,
    WorkGraphPRCommitRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    Worklog,
)

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class WorkGraphMixin(_ClickHouseSinkBase):
    """Mixin for work graph, work items, git metrics, and forecast write methods."""

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "repo_metrics_daily",
            [
                "repo_id",
                "day",
                "commits_count",
                "total_loc_touched",
                "avg_commit_size_loc",
                "large_commit_ratio",
                "prs_merged",
                "median_pr_cycle_hours",
                "pr_cycle_p75_hours",
                "pr_cycle_p90_hours",
                "prs_with_first_review",
                "pr_first_review_p50_hours",
                "pr_first_review_p90_hours",
                "pr_review_time_p50_hours",
                "pr_pickup_time_p50_hours",
                "large_pr_ratio",
                "pr_rework_ratio",
                "pr_size_p50_loc",
                "pr_size_p90_loc",
                "pr_comments_per_100_loc",
                "pr_reviews_per_100_loc",
                "rework_churn_ratio_30d",
                "single_owner_file_ratio_30d",
                "review_load_top_reviewer_ratio",
                "bus_factor",
                "code_ownership_gini",
                "mttr_hours",
                "change_failure_rate",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_ic_landscape_rolling(
        self, rows: Sequence[ICLandscapeRollingRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "ic_landscape_rolling_30d",
            [
                "repo_id",
                "as_of_day",
                "identity_id",
                "team_id",
                "map_name",
                "x_raw",
                "y_raw",
                "x_norm",
                "y_norm",
                "churn_loc_30d",
                "delivery_units_30d",
                "cycle_p50_30d_hours",
                "wip_max_30d",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_file_metrics(self, rows: Sequence[FileMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "file_metrics_daily",
            [
                "repo_id",
                "day",
                "path",
                "churn",
                "contributors",
                "commits_count",
                "hotspot_score",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "commit_metrics",
            [
                "repo_id",
                "commit_hash",
                "day",
                "author_email",
                "total_loc",
                "files_changed",
                "size_bucket",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "team_metrics_daily",
            [
                "day",
                "team_id",
                "team_name",
                "commits_count",
                "after_hours_commits_count",
                "weekend_commits_count",
                "after_hours_commit_ratio",
                "weekend_commit_ratio",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_item_metrics(
        self, rows: Sequence[WorkItemMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_metrics_daily",
            [
                "day",
                "provider",
                "work_scope_id",
                "team_id",
                "team_name",
                "items_started",
                "items_completed",
                "items_started_unassigned",
                "items_completed_unassigned",
                "wip_count_end_of_day",
                "wip_unassigned_end_of_day",
                "cycle_time_p50_hours",
                "cycle_time_p90_hours",
                "lead_time_p50_hours",
                "lead_time_p90_hours",
                "wip_age_p50_hours",
                "wip_age_p90_hours",
                "bug_completed_ratio",
                "story_points_completed",
                "new_bugs_count",
                "new_items_count",
                "defect_intro_rate",
                "wip_congestion_ratio",
                "predictability_score",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_item_user_metrics(
        self, rows: Sequence[WorkItemUserMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_user_metrics_daily",
            [
                "day",
                "provider",
                "work_scope_id",
                "user_identity",
                "team_id",
                "team_name",
                "items_started",
                "items_completed",
                "wip_count_end_of_day",
                "cycle_time_p50_hours",
                "cycle_time_p90_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_item_cycle_times(
        self, rows: Sequence[WorkItemCycleTimeRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_cycle_times",
            [
                "work_item_id",
                "provider",
                "day",
                "work_scope_id",
                "team_id",
                "team_name",
                "assignee",
                "type",
                "status",
                "created_at",
                "started_at",
                "completed_at",
                "cycle_time_hours",
                "lead_time_hours",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_item_state_durations(
        self, rows: Sequence[WorkItemStateDurationDailyRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_state_durations_daily",
            [
                "day",
                "provider",
                "work_scope_id",
                "team_id",
                "team_name",
                "status",
                "duration_hours",
                "items_touched",
                "avg_wip",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_work_item_dependencies(self, rows: Sequence[WorkItemDependency]) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_dependencies",
            [
                "source_work_item_id",
                "target_work_item_id",
                "relationship_type",
                "relationship_type_raw",
                "last_synced",
                "org_id",
            ],
            rows,
        )

    def write_work_item_reopen_events(
        self, rows: Sequence[WorkItemReopenEvent]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_reopen_events",
            [
                "work_item_id",
                "occurred_at",
                "from_status",
                "to_status",
                "from_status_raw",
                "to_status_raw",
                "actor",
                "last_synced",
                "org_id",
            ],
            rows,
        )

    def write_work_item_interactions(
        self, rows: Sequence[WorkItemInteractionEvent]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_item_interactions",
            [
                "work_item_id",
                "provider",
                "interaction_type",
                "occurred_at",
                "actor",
                "body_length",
                "last_synced",
                "org_id",
            ],
            rows,
        )

    def write_sprints(self, rows: Sequence[Sprint]) -> None:
        if not rows:
            return
        persisted_rows = [
            replace(sprint, native_team_key=sprint.native_team_key or "")
            for sprint in rows
        ]
        self._insert_rows(
            "sprints",
            [
                "provider",
                "sprint_id",
                "native_team_key",
                "name",
                "state",
                "started_at",
                "ended_at",
                "completed_at",
                "last_synced",
                "org_id",
            ],
            persisted_rows,
        )

    def write_worklogs(self, rows: Sequence[Worklog]) -> None:
        if not rows:
            return
        self._insert_rows(
            "worklogs",
            [
                "work_item_id",
                "provider",
                "worklog_id",
                "author",
                "started_at",
                "time_spent_seconds",
                "created_at",
                "updated_at",
                "last_synced",
                "org_id",
            ],
            rows,
        )

    def write_review_edges(self, rows: Sequence[ReviewEdgeDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "review_edges_daily",
            [
                "repo_id",
                "day",
                "reviewer",
                "author",
                "reviews_count",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_file_complexity_snapshots(
        self, rows: Sequence[FileComplexitySnapshot]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "file_complexity_snapshots",
            [
                "repo_id",
                "as_of_day",
                "ref",
                "file_path",
                "language",
                "loc",
                "functions_count",
                "cyclomatic_total",
                "cyclomatic_avg",
                "high_complexity_functions",
                "very_high_complexity_functions",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_repo_complexity_daily(self, rows: Sequence[RepoComplexityDaily]) -> None:
        if not rows:
            return
        self._insert_rows(
            "repo_complexity_daily",
            [
                "repo_id",
                "day",
                "loc_total",
                "cyclomatic_total",
                "cyclomatic_per_kloc",
                "high_complexity_functions",
                "very_high_complexity_functions",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    def write_file_hotspot_daily(self, rows: Sequence[FileHotspotDaily]) -> None:
        if not rows:
            return
        self._insert_rows(
            "file_hotspot_daily",
            [
                "repo_id",
                "day",
                "file_path",
                "churn_loc_30d",
                "churn_commits_30d",
                "cyclomatic_total",
                "cyclomatic_avg",
                "blame_concentration",
                "risk_score",
                "computed_at",
                "org_id",
            ],
            rows,
        )

    # -------------------------------------------------------------------------
    # Work graph (derived relationships)
    # -------------------------------------------------------------------------

    def write_work_graph_edges(self, rows: Sequence[WorkGraphEdgeRecord]) -> None:
        if not rows:
            return
        column_names = [
            "edge_id",
            "source_type",
            "source_id",
            "target_type",
            "target_id",
            "edge_type",
            "repo_id",
            "provider",
            "provenance",
            "confidence",
            "evidence",
            "discovered_at",
            "last_synced",
            "event_ts",
            "day",
            "org_id",
        ]
        for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
            data = []
            for r in chunk:
                data.append(
                    {
                        "edge_id": r.edge_id,
                        "source_type": r.source_type,
                        "source_id": r.source_id,
                        "target_type": r.target_type,
                        "target_id": r.target_id,
                        "edge_type": r.edge_type,
                        "repo_id": str(r.repo_id) if r.repo_id else None,
                        "provider": r.provider,
                        "provenance": r.provenance,
                        "confidence": r.confidence,
                        "evidence": r.evidence,
                        "discovered_at": _dt_to_clickhouse_datetime(r.discovered_at),
                        "last_synced": _dt_to_clickhouse_datetime(r.last_synced),
                        "event_ts": _dt_to_clickhouse_datetime(r.event_ts),
                        "day": r.day,
                        "org_id": r.org_id,
                    }
                )
            matrix = [[row[col] for col in column_names] for row in data]
            self.client.insert("work_graph_edges", matrix, column_names=column_names)

    def write_work_graph_issue_pr(self, rows: Sequence[WorkGraphIssuePRRecord]) -> None:
        if not rows:
            return
        column_names = [
            "repo_id",
            "work_item_id",
            "pr_number",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
            "org_id",
        ]
        for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
            data = []
            for r in chunk:
                data.append(
                    {
                        "repo_id": str(r.repo_id),
                        "work_item_id": r.work_item_id,
                        "pr_number": r.pr_number,
                        "confidence": r.confidence,
                        "provenance": r.provenance,
                        "evidence": r.evidence,
                        "last_synced": _dt_to_clickhouse_datetime(r.last_synced),
                        "org_id": r.org_id,
                    }
                )
            matrix = [[row[col] for col in column_names] for row in data]
            self.client.insert("work_graph_issue_pr", matrix, column_names=column_names)

    def write_work_graph_pr_commit(
        self, rows: Sequence[WorkGraphPRCommitRecord]
    ) -> None:
        if not rows:
            return
        column_names = [
            "repo_id",
            "pr_number",
            "commit_hash",
            "confidence",
            "provenance",
            "evidence",
            "last_synced",
            "org_id",
        ]
        for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
            data = []
            for r in chunk:
                data.append(
                    {
                        "repo_id": str(r.repo_id),
                        "pr_number": r.pr_number,
                        "commit_hash": r.commit_hash,
                        "confidence": r.confidence,
                        "provenance": r.provenance,
                        "evidence": r.evidence,
                        "last_synced": _dt_to_clickhouse_datetime(r.last_synced),
                        "org_id": r.org_id,
                    }
                )
            matrix = [[row[col] for col in column_names] for row in data]
            self.client.insert(
                "work_graph_pr_commit", matrix, column_names=column_names
            )

    def write_work_items(self, work_items: Sequence[Any]) -> None:
        """Write raw work items to the work_items table."""
        if not work_items:
            return

        synced_at = datetime.now(timezone.utc)
        rows = []

        for item in work_items:
            # Handle both dict and WorkItem objects
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None, obj=item: getattr(obj, k, default)
            )

            rows.append(
                {
                    "repo_id": uuid.UUID(str(get("repo_id")))
                    if get("repo_id")
                    else uuid.UUID(int=0),
                    "work_item_id": str(get("work_item_id")),
                    "provider": str(get("provider") or ""),
                    "title": str(get("title") or ""),
                    "type": str(get("type") or ""),
                    "status": str(get("status") or ""),
                    "status_raw": str(get("status_raw") or ""),
                    "project_key": str(get("project_key") or ""),
                    "project_id": str(get("project_id") or ""),
                    "native_team_key": str(get("native_team_key") or ""),
                    "project_name": str(get("project_name") or ""),
                    "assignees": get("assignees") or [],
                    "reporter": str(get("reporter") or ""),
                    "created_at": _dt_to_clickhouse_datetime(get("created_at")),
                    "updated_at": _dt_to_clickhouse_datetime(get("updated_at")),
                    "started_at": _dt_to_clickhouse_datetime(get("started_at")),
                    "completed_at": _dt_to_clickhouse_datetime(get("completed_at")),
                    "closed_at": _dt_to_clickhouse_datetime(get("closed_at")),
                    "labels": get("labels") or [],
                    "story_points": get("story_points"),
                    "sprint_id": str(get("sprint_id") or ""),
                    "sprint_name": str(get("sprint_name") or ""),
                    "parent_id": str(get("parent_id") or ""),
                    "epic_id": str(get("epic_id") or ""),
                    "url": str(get("url") or ""),
                    "last_synced": _dt_to_clickhouse_datetime(synced_at),
                    "org_id": item["org_id"] if is_dict else item.org_id,
                }
            )

        column_names = [
            "repo_id",
            "work_item_id",
            "provider",
            "title",
            "type",
            "status",
            "status_raw",
            "project_key",
            "project_id",
            "native_team_key",
            "project_name",
            "assignees",
            "reporter",
            "created_at",
            "updated_at",
            "started_at",
            "completed_at",
            "closed_at",
            "labels",
            "story_points",
            "sprint_id",
            "sprint_name",
            "parent_id",
            "epic_id",
            "url",
            "last_synced",
            "org_id",
        ]
        for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
            matrix = [[row[col] for col in column_names] for row in chunk]
            self.client.insert("work_items", matrix, column_names=column_names)

    def write_work_item_transitions(self, transitions: Sequence[Any]) -> None:
        """Write raw work item transitions to the work_item_transitions table."""
        if not transitions:
            return

        synced_at = datetime.now(timezone.utc)
        rows = []

        for item in transitions:
            # Handle both dict and WorkItemStatusTransition objects
            is_dict = isinstance(item, dict)
            get = (
                item.get
                if is_dict
                else lambda k, default=None, obj=item: getattr(obj, k, default)
            )

            repo_id_val = get("repo_id")
            if repo_id_val:
                if isinstance(repo_id_val, str):
                    repo_id_val = uuid.UUID(repo_id_val)
            else:
                repo_id_val = uuid.UUID(int=0)

            rows.append(
                {
                    "repo_id": repo_id_val,
                    "work_item_id": str(get("work_item_id")),
                    "occurred_at": _dt_to_clickhouse_datetime(get("occurred_at"))
                    if get("occurred_at")
                    else datetime.now(timezone.utc),
                    "from_status": str(get("from_status") or ""),
                    "to_status": str(get("to_status") or ""),
                    "from_status_raw": str(get("from_status_raw") or ""),
                    "to_status_raw": str(get("to_status_raw") or ""),
                    "actor": str(get("actor") or ""),
                    "last_synced": _dt_to_clickhouse_datetime(synced_at),
                    "org_id": item["org_id"] if is_dict else item.org_id,
                }
            )

        column_names = [
            "repo_id",
            "work_item_id",
            "occurred_at",
            "from_status",
            "to_status",
            "from_status_raw",
            "to_status_raw",
            "actor",
            "last_synced",
            "org_id",
        ]
        for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
            matrix = [[row[col] for col in column_names] for row in chunk]
            self.client.insert(
                "work_item_transitions", matrix, column_names=column_names
            )

    def write_capacity_forecasts(self, rows: Sequence[CapacityForecastRecord]) -> None:
        if not rows:
            return
        column_names = [
            "forecast_id",
            "computed_at",
            "team_id",
            "work_scope_id",
            "backlog_size",
            "target_items",
            "target_date",
            "history_days",
            "simulation_count",
            "p50_days",
            "p85_days",
            "p95_days",
            "p50_date",
            "p85_date",
            "p95_date",
            "p50_items",
            "p85_items",
            "p95_items",
            "throughput_mean",
            "throughput_stddev",
            "insufficient_history",
            "high_variance",
            "org_id",
        ]
        for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
            data = []
            for r in chunk:
                data.append(
                    {
                        "forecast_id": r.forecast_id,
                        "computed_at": _dt_to_clickhouse_datetime(r.computed_at),
                        "team_id": r.team_id,
                        "work_scope_id": r.work_scope_id,
                        "backlog_size": r.backlog_size,
                        "target_items": r.target_items,
                        "target_date": r.target_date,
                        "history_days": r.history_days,
                        "simulation_count": r.simulation_count,
                        "p50_days": r.p50_days,
                        "p85_days": r.p85_days,
                        "p95_days": r.p95_days,
                        "p50_date": r.p50_date,
                        "p85_date": r.p85_date,
                        "p95_date": r.p95_date,
                        "p50_items": r.p50_items,
                        "p85_items": r.p85_items,
                        "p95_items": r.p95_items,
                        "throughput_mean": r.throughput_mean,
                        "throughput_stddev": r.throughput_stddev,
                        "insufficient_history": 1 if r.insufficient_history else 0,
                        "high_variance": 1 if r.high_variance else 0,
                        "org_id": r.org_id,
                    }
                )
            matrix = [[row[col] for col in column_names] for row in data]
            self.client.insert("capacity_forecasts", matrix, column_names=column_names)
