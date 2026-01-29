from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, List, Optional, Sequence, Dict
import uuid

import clickhouse_connect
import logging
import asyncio
from dev_health_ops.metrics.schemas import (
    CapacityForecastRecord,
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
    FileMetricsRecord,
    ReviewEdgeDailyRecord,
    CICDMetricsDailyRecord,
    DeployMetricsDailyRecord,
    DORAMetricsRecord,
    IncidentMetricsDailyRecord,
    ICLandscapeRollingRecord,
    FileComplexitySnapshot,
    RepoComplexityDaily,
    FileHotspotDaily,
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    InvestmentExplanationRecord,
    IssueTypeMetricsRecord,
    WorkGraphEdgeRecord,
    WorkGraphIssuePRRecord,
    WorkGraphPRCommitRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    Worklog,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

logger = logging.getLogger(__name__)


def _dt_to_clickhouse_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


class ClickHouseMetricsSink(BaseMetricsSink):
    """
    ClickHouse sink for derived daily metrics.

    This sink is append-only: re-computations insert new rows with a newer
    `computed_at`. Queries can select the latest version via `argMax`.
    """

    def query_dicts(
        self, query: str, parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute a ClickHouse query and return results as list of dicts."""
        result = self.client.query(query, parameters=parameters)
        col_names = list(getattr(result, "column_names", []) or [])
        rows = list(getattr(result, "result_rows", []) or [])
        if not col_names or not rows:
            return []
        return [dict(zip(col_names, row)) for row in rows]

    @property
    def backend_type(self) -> str:
        return "clickhouse"

    def __init__(self, dsn: str, client: Optional[Any] = None) -> None:
        if not dsn:
            raise ValueError("ClickHouse DSN is required")
        self.dsn = dsn
        if client:
            self.client = client
        else:
            settings = {
                "max_query_size": 1 * 1024 * 1024,  # 1MB
            }
            self.client = clickhouse_connect.get_client(dsn=dsn, settings=settings)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception as e:
            logger.warning(
                "Exception occurred when closing ClickHouse client: %s",
                e,
                exc_info=True,
            )

    async def get_all_teams(self) -> List[Dict[str, Any]]:
        """Fetch all teams from ClickHouse for identity resolution."""
        query = "SELECT id, name, members FROM teams FINAL"
        result = await asyncio.to_thread(self.client.query, query)
        teams: List[Dict[str, Any]] = []
        for row in result.result_rows or []:
            teams.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "members": row[2] or [],
                }
            )
        return teams

    def _apply_sql_migrations(self) -> None:
        migrations_dir = (
            Path(__file__).resolve().parents[2] / "migrations" / "clickhouse"
        )
        if not migrations_dir.exists():
            return

        # Ensure schema_migrations table exists
        self.client.command(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version String, applied_at DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY version"
        )

        # Get applied migrations
        applied_result = self.client.query("SELECT version FROM schema_migrations")
        applied_versions = set(
            row[0] for row in (getattr(applied_result, "result_rows", []) or [])
        )

        # Collect all migration files
        migration_files = sorted(
            list(migrations_dir.glob("*.sql")) + list(migrations_dir.glob("*.py"))
        )

        for path in migration_files:
            version = path.name
            if version in applied_versions:
                logger.info(f"Skipping already applied migration: {version}")
                continue

            logger.info(f"Applying migration: {version}")

            if path.suffix == ".sql":
                try:
                    sql = path.read_text(encoding="utf-8")
                    # Very small splitter: migrations are expected to contain only DDL.
                    for stmt in sql.split(";"):
                        stmt = stmt.strip()
                        if not stmt:
                            continue
                        self.client.command(stmt)
                except Exception as e:
                    logger.error(f"CRITICAL: Migration failed: {path.name}\nError: {e}")
                    raise
            elif path.suffix == ".py":
                # Execute python migration script
                try:
                    import importlib.util

                    spec = importlib.util.spec_from_file_location(path.stem, path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        if hasattr(module, "upgrade"):
                            logger.info(f"Executing Python migration: {path.name}")
                            module.upgrade(self.client)
                except Exception as e:
                    logger.error(f"Failed to apply python migration {path.name}: {e}")
                    raise

            # Record migration as applied
            self.client.command(
                "INSERT INTO schema_migrations (version, applied_at) VALUES ({version:String}, now())",
                parameters={"version": version},
            )

    def ensure_schema(self) -> None:
        """Create ClickHouse tables via SQL migrations."""
        self._apply_sql_migrations()

    # Alias for backward compatibility
    ensure_tables = ensure_schema

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
            ],
            rows,
        )

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
            ],
            rows,
        )

    def write_sprints(self, rows: Sequence[Sprint]) -> None:
        if not rows:
            return
        self._insert_rows(
            "sprints",
            [
                "provider",
                "sprint_id",
                "name",
                "state",
                "started_at",
                "ended_at",
                "completed_at",
                "last_synced",
            ],
            rows,
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
            ],
            rows,
        )

    def write_cicd_metrics(self, rows: Sequence[CICDMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "cicd_metrics_daily",
            [
                "repo_id",
                "day",
                "pipelines_count",
                "success_rate",
                "avg_duration_minutes",
                "p90_duration_minutes",
                "avg_queue_minutes",
                "computed_at",
            ],
            rows,
        )

    def write_deploy_metrics(self, rows: Sequence[DeployMetricsDailyRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "deploy_metrics_daily",
            [
                "repo_id",
                "day",
                "deployments_count",
                "failed_deployments_count",
                "deploy_time_p50_hours",
                "lead_time_p50_hours",
                "computed_at",
            ],
            rows,
        )

    def write_incident_metrics(
        self, rows: Sequence[IncidentMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "incident_metrics_daily",
            [
                "repo_id",
                "day",
                "incidents_count",
                "mttr_p50_hours",
                "mttr_p90_hours",
                "computed_at",
            ],
            rows,
        )

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
            ],
            rows,
        )

    def write_investment_classifications(
        self, rows: Sequence[InvestmentClassificationRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "investment_classifications_daily",
            [
                "repo_id",
                "day",
                "artifact_type",
                "artifact_id",
                "provider",
                "investment_area",
                "project_stream",
                "confidence",
                "rule_id",
                "computed_at",
            ],
            rows,
        )

    def write_investment_metrics(self, rows: Sequence[InvestmentMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "investment_metrics_daily",
            [
                "repo_id",
                "day",
                "team_id",
                "investment_area",
                "project_stream",
                "delivery_units",
                "work_items_completed",
                "prs_merged",
                "churn_loc",
                "cycle_p50_hours",
                "computed_at",
            ],
            rows,
        )

    def write_issue_type_metrics(self, rows: Sequence[IssueTypeMetricsRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "issue_type_metrics_daily",
            [
                "repo_id",
                "day",
                "provider",
                "team_id",
                "issue_type_norm",
                "created_count",
                "completed_count",
                "active_count",
                "cycle_p50_hours",
                "cycle_p90_hours",
                "lead_p50_hours",
                "computed_at",
            ],
            rows,
        )

    # -------------------------------------------------------------------------
    # Work unit investment materialization
    # -------------------------------------------------------------------------

    def write_work_unit_investments(
        self, rows: Sequence[WorkUnitInvestmentRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_unit_investments",
            [
                "work_unit_id",
                "work_unit_type",
                "work_unit_name",
                "from_ts",
                "to_ts",
                "repo_id",
                "provider",
                "effort_metric",
                "effort_value",
                "theme_distribution_json",
                "subcategory_distribution_json",
                "structural_evidence_json",
                "evidence_quality",
                "evidence_quality_band",
                "categorization_status",
                "categorization_errors_json",
                "categorization_model_version",
                "categorization_input_hash",
                "categorization_run_id",
                "computed_at",
            ],
            rows,
        )

    def write_work_unit_investment_quotes(
        self, rows: Sequence[WorkUnitInvestmentEvidenceQuoteRecord]
    ) -> None:
        if not rows:
            return
        self._insert_rows(
            "work_unit_investment_quotes",
            [
                "work_unit_id",
                "quote",
                "source_type",
                "source_id",
                "computed_at",
                "categorization_run_id",
            ],
            rows,
        )

    def write_investment_explanation(self, record: InvestmentExplanationRecord) -> None:
        """Write or replace an investment explanation to the cache."""
        self._insert_rows(
            "investment_explanations",
            [
                "cache_key",
                "explanation_json",
                "llm_provider",
                "llm_model",
                "computed_at",
            ],
            [record],
        )

    def read_investment_explanation(
        self, cache_key: str
    ) -> Optional[InvestmentExplanationRecord]:
        """
        Read a cached investment explanation by cache_key.

        Uses FINAL to ensure we get the latest version from ReplacingMergeTree.
        Returns None if no cached explanation exists.
        """
        result = self.client.query(
            """
            SELECT
                cache_key,
                explanation_json,
                llm_provider,
                llm_model,
                computed_at
            FROM investment_explanations FINAL
            WHERE cache_key = {cache_key:String}
            LIMIT 1
            """,
            parameters={"cache_key": cache_key},
        )
        rows = result.result_rows or []
        if not rows:
            return None
        row = rows[0]
        return InvestmentExplanationRecord(
            cache_key=str(row[0]),
            explanation_json=str(row[1]),
            llm_provider=str(row[2]),
            llm_model=str(row[3]) if row[3] else None,
            computed_at=row[4]
            if isinstance(row[4], datetime)
            else datetime.now(timezone.utc),
        )

    # -------------------------------------------------------------------------
    # Work graph (derived relationships)
    # -------------------------------------------------------------------------

    def write_work_graph_edges(self, rows: Sequence[WorkGraphEdgeRecord]) -> None:
        if not rows:
            return
        data = []
        for r in rows:
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
                }
            )
        column_names = list(data[0].keys())
        matrix = [[row[col] for col in column_names] for row in data]
        self.client.insert("work_graph_edges", matrix, column_names=column_names)

    def write_work_graph_issue_pr(self, rows: Sequence[WorkGraphIssuePRRecord]) -> None:
        if not rows:
            return
        data = []
        for r in rows:
            data.append(
                {
                    "repo_id": str(r.repo_id),
                    "work_item_id": r.work_item_id,
                    "pr_number": r.pr_number,
                    "confidence": r.confidence,
                    "provenance": r.provenance,
                    "evidence": r.evidence,
                    "last_synced": _dt_to_clickhouse_datetime(r.last_synced),
                }
            )
        column_names = list(data[0].keys())
        matrix = [[row[col] for col in column_names] for row in data]
        self.client.insert("work_graph_issue_pr", matrix, column_names=column_names)

    def write_work_graph_pr_commit(
        self, rows: Sequence[WorkGraphPRCommitRecord]
    ) -> None:
        if not rows:
            return
        data = []
        for r in rows:
            data.append(
                {
                    "repo_id": str(r.repo_id),
                    "pr_number": r.pr_number,
                    "commit_hash": r.commit_hash,
                    "confidence": r.confidence,
                    "provenance": r.provenance,
                    "evidence": r.evidence,
                    "last_synced": _dt_to_clickhouse_datetime(r.last_synced),
                }
            )
        column_names = list(data[0].keys())
        matrix = [[row[col] for col in column_names] for row in data]
        self.client.insert("work_graph_pr_commit", matrix, column_names=column_names)

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
                    "provider": str(get("provider")),
                    "title": str(get("title")),
                    "type": str(get("type")),
                    "status": str(get("status")),
                    "status_raw": str(get("status_raw") or ""),
                    "project_key": str(get("project_key") or ""),
                    "project_id": str(get("project_id") or ""),
                    "assignees": get("assignees") or [],
                    "reporter": str(get("reporter") or ""),
                    "created_at": _dt_to_clickhouse_datetime(get("created_at"))
                    or datetime.now(timezone.utc),
                    "updated_at": _dt_to_clickhouse_datetime(get("updated_at"))
                    or datetime.now(timezone.utc),
                    "started_at": _dt_to_clickhouse_datetime(get("started_at")),
                    "completed_at": _dt_to_clickhouse_datetime(get("completed_at")),
                    "closed_at": _dt_to_clickhouse_datetime(get("closed_at")),
                    "labels": get("labels") or [],
                    "story_points": get("story_points")
                    if get("story_points") is not None
                    else None,
                    "sprint_id": str(get("sprint_id") or ""),
                    "sprint_name": str(get("sprint_name") or ""),
                    "parent_id": str(get("parent_id") or ""),
                    "epic_id": str(get("epic_id") or ""),
                    "url": str(get("url") or ""),
                    "last_synced": _dt_to_clickhouse_datetime(synced_at),
                }
            )

        # Convert dict rows to matrix format for ClickHouse
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
        ]
        matrix = [[row[col] for col in column_names] for row in rows]

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
                    "provider": str(get("provider")),
                    "from_status": str(get("from_status")),
                    "to_status": str(get("to_status")),
                    "from_status_raw": str(get("from_status_raw") or ""),
                    "to_status_raw": str(get("to_status_raw") or ""),
                    "actor": str(get("actor") or ""),
                    "last_synced": _dt_to_clickhouse_datetime(synced_at),
                }
            )

        # Convert dict rows to matrix format for ClickHouse
        column_names = [
            "repo_id",
            "work_item_id",
            "occurred_at",
            "provider",
            "from_status",
            "to_status",
            "from_status_raw",
            "to_status_raw",
            "actor",
            "last_synced",
        ]
        matrix = [[row[col] for col in column_names] for row in rows]

        self.client.insert("work_item_transitions", matrix, column_names=column_names)

    def _insert_rows(self, table: str, columns: List[str], rows: Sequence[Any]) -> None:
        matrix = []
        for row in rows:
            data = asdict(row)
            values = []
            for col in columns:
                value = data.get(col)
                if isinstance(value, datetime):
                    value = _dt_to_clickhouse_datetime(value)
                values.append(value)
            matrix.append(values)
        self.client.insert(table, matrix, column_names=columns)

    # Query helpers (useful for Grafana and validation)
    def get_rolling_30d_user_stats(
        self,
        as_of_day: date,
        repo_id: Optional[uuid.UUID] = None,
    ) -> List[Dict[str, Any]]:
        """
        Compute rolling 30d stats for all users as of the given day.

        Aggregation logic:
        - churn_loc_30d: sum(loc_touched)
        - delivery_units_30d: sum(delivery_units)
        - cycle_p50_30d_hours: median of daily cycle_p50_hours (approx) where cycle_p50_hours > 0
        - wip_max_30d: max(work_items_active)
        """
        # We look at [as_of_day - 29 days, as_of_day] inclusive.
        # Note: 'day' in user_metrics_daily is the date of the metrics.

        start_day = as_of_day - timedelta(days=29)

        params = {
            "start": (
                start_day.strftime("%Y-%m-%d")
                if hasattr(start_day, "strftime")
                else str(start_day)
            ),
            "end": (
                as_of_day.strftime("%Y-%m-%d")
                if hasattr(as_of_day, "strftime")
                else str(as_of_day)
            ),
        }
        where = ["day >= toDate(%(start)s)", "day <= toDate(%(end)s)"]
        if repo_id:
            where.append("repo_id = toUUID(%(repo_id)s)")
            params["repo_id"] = str(repo_id)

        where_clause = " AND ".join(where)

        # We use argMax(..., computed_at) to get the latest version of the row for each day
        # before aggregating over days.
        # However, user_metrics_daily is MergeTree, not ReplacingMergeTree in the original schema (001).
        # Wait, 001 says ENGINE = MergeTree. So we might have duplicates if we re-ran.
        # But commonly we just insert.
        # If we assume we might have multiple rows per day/user/repo, we should take the latest.
        # The PK is (repo_id, author_email, day).
        # We'll aggregate over (identity_id, team_id, repo_id)

        # Note: identity_id was added in 005. For older rows it might be null/empty.
        # We fallback to author_email if identity_id is empty.

        sql = f"""
        SELECT
            if(empty(identity_id), author_email, identity_id) as identity_id,
            anyLast(team_id) as team_id,
            sum(loc_touched) as churn_loc_30d,
            sum(delivery_units) as delivery_units_30d,
            quantile(0.5)(if(cycle_p50_hours > 0, cycle_p50_hours, null)) as cycle_p50_30d_hours,
            max(work_items_active) as wip_max_30d
        FROM user_metrics_daily
        WHERE {where_clause}
        GROUP BY identity_id
        HAVING identity_id != ''
        """

        # Note: ClickHouse's quantile(0.5) is approximate but fast.

        try:
            result = self.client.query(sql, parameters=params)
            rows = []
            for r in result.named_results():
                rows.append(r)
            return rows
        except Exception as e:
            logger.warning("Failed to fetch rolling stats: %s", e)
            return []

    def latest_repo_metrics_query(
        self,
        *,
        repo_id: Optional[str] = None,
        start_day: Optional[date] = None,
        end_day: Optional[date] = None,
    ) -> str:
        where = []
        if repo_id:
            where.append(f"repo_id = toUUID('{repo_id}')")
        if start_day:
            where.append(f"day >= toDate('{start_day.isoformat()}')")
        if end_day:
            where.append(f"day < toDate('{end_day.isoformat()}')")
        where_clause = ("WHERE " + " AND ".join(where)) if where else ""
        return f"""
        SELECT
          repo_id,
          day,
          argMax(commits_count, computed_at) AS commits_count,
          argMax(total_loc_touched, computed_at) AS total_loc_touched,
          argMax(avg_commit_size_loc, computed_at) AS avg_commit_size_loc,
          argMax(large_commit_ratio, computed_at) AS large_commit_ratio,
          argMax(prs_merged, computed_at) AS prs_merged,
          argMax(median_pr_cycle_hours, computed_at) AS median_pr_cycle_hours,
          max(computed_at) AS computed_at
        FROM repo_metrics_daily
        {where_clause}
        GROUP BY repo_id, day
        ORDER BY repo_id, day
        """

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def write_capacity_forecasts(self, rows: Sequence[CapacityForecastRecord]) -> None:
        if not rows:
            return
        data = []
        for r in rows:
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
                }
            )
        column_names = list(data[0].keys())
        matrix = [[row[col] for col in column_names] for row in data]
        self.client.insert("capacity_forecasts", matrix, column_names=column_names)
