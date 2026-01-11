from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone, timedelta
from typing import Sequence, List, Dict, Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from metrics.schemas import (
    CommitMetricsRecord,
    RepoMetricsDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    FileMetricsRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
    ReviewEdgeDailyRecord,
    CICDMetricsDailyRecord,
    DeployMetricsDailyRecord,
    IncidentMetricsDailyRecord,
    ICLandscapeRollingRecord,
    FileComplexitySnapshot,
    RepoComplexityDaily,
    FileHotspotDaily,
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from metrics.sinks.base import BaseMetricsSink


def _dt_to_sqlite(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat()
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat()


class SQLiteMetricsSink(BaseMetricsSink):
    """SQLite sink for derived daily metrics (idempotent upserts by primary key)."""

    @property
    def backend_type(self) -> str:
        return "sqlite"

    def __init__(self, db_url: str) -> None:
        if not db_url:
            raise ValueError("SQLite DB URL is required")
        if "sqlite+aiosqlite://" in db_url:
            db_url = db_url.replace("sqlite+aiosqlite://", "sqlite://", 1)
        self.engine: Engine = create_engine(db_url, echo=False)
        self._wi_metrics_has_work_scope: bool = True
        self._wi_user_metrics_has_work_scope: bool = True
        self._wi_cycle_has_work_scope: bool = True
        self._wi_state_has_work_scope: bool = True

    def close(self) -> None:
        self.engine.dispose()

    def ensure_tables(self) -> None:
        stmts = [
            """
            CREATE TABLE IF NOT EXISTS repo_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              commits_count INTEGER NOT NULL,
              total_loc_touched INTEGER NOT NULL,
              avg_commit_size_loc REAL NOT NULL,
              large_commit_ratio REAL NOT NULL,
              prs_merged INTEGER NOT NULL,
              median_pr_cycle_hours REAL NOT NULL,
              pr_cycle_p75_hours REAL NOT NULL DEFAULT 0.0,
              pr_cycle_p90_hours REAL NOT NULL DEFAULT 0.0,
              prs_with_first_review INTEGER NOT NULL DEFAULT 0,
              pr_first_review_p50_hours REAL,
              pr_first_review_p90_hours REAL,
              pr_review_time_p50_hours REAL,
              pr_pickup_time_p50_hours REAL,
              large_pr_ratio REAL NOT NULL DEFAULT 0.0,
              pr_rework_ratio REAL NOT NULL DEFAULT 0.0,
              pr_size_p50_loc REAL,
              pr_size_p90_loc REAL,
              pr_comments_per_100_loc REAL,
              pr_reviews_per_100_loc REAL,
              rework_churn_ratio_30d REAL NOT NULL DEFAULT 0.0,
              single_owner_file_ratio_30d REAL NOT NULL DEFAULT 0.0,
              review_load_top_reviewer_ratio REAL NOT NULL DEFAULT 0.0,
              bus_factor INTEGER NOT NULL DEFAULT 0,
              code_ownership_gini REAL NOT NULL DEFAULT 0.0,
              mttr_hours REAL,
              change_failure_rate REAL NOT NULL DEFAULT 0.0,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              author_email TEXT NOT NULL,
              commits_count INTEGER NOT NULL,
              loc_added INTEGER NOT NULL,
              loc_deleted INTEGER NOT NULL,
              files_changed INTEGER NOT NULL,
              large_commits_count INTEGER NOT NULL,
              avg_commit_size_loc REAL NOT NULL,
              prs_authored INTEGER NOT NULL,
              prs_merged INTEGER NOT NULL,
              avg_pr_cycle_hours REAL NOT NULL,
              median_pr_cycle_hours REAL NOT NULL,
              pr_cycle_p75_hours REAL NOT NULL DEFAULT 0.0,
              pr_cycle_p90_hours REAL NOT NULL DEFAULT 0.0,
              prs_with_first_review INTEGER NOT NULL DEFAULT 0,
              pr_first_review_p50_hours REAL,
              pr_first_review_p90_hours REAL,
              pr_review_time_p50_hours REAL,
              pr_pickup_time_p50_hours REAL,
              reviews_given INTEGER NOT NULL DEFAULT 0,
              changes_requested_given INTEGER NOT NULL DEFAULT 0,
              reviews_received INTEGER NOT NULL DEFAULT 0,
              review_reciprocity REAL NOT NULL DEFAULT 0.0,
              team_id TEXT,
              team_name TEXT,
              active_hours REAL NOT NULL DEFAULT 0.0,
              weekend_days INTEGER NOT NULL DEFAULT 0,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, author_email, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS commit_metrics (
              repo_id TEXT NOT NULL,
              commit_hash TEXT NOT NULL,
              day TEXT NOT NULL,
              author_email TEXT NOT NULL,
              total_loc INTEGER NOT NULL,
              files_changed INTEGER NOT NULL,
              size_bucket TEXT NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day, author_email, commit_hash)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS team_metrics_daily (
              day TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              commits_count INTEGER NOT NULL,
              after_hours_commits_count INTEGER NOT NULL,
              weekend_commits_count INTEGER NOT NULL,
              after_hours_commit_ratio REAL NOT NULL,
              weekend_commit_ratio REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (team_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS file_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              path TEXT NOT NULL,
              churn INTEGER NOT NULL,
              contributors INTEGER NOT NULL,
              commits_count INTEGER NOT NULL,
              hotspot_score REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day, path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_metrics_daily (
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              items_started INTEGER NOT NULL,
              items_completed INTEGER NOT NULL,
              items_started_unassigned INTEGER NOT NULL,
              items_completed_unassigned INTEGER NOT NULL,
              wip_count_end_of_day INTEGER NOT NULL,
              wip_unassigned_end_of_day INTEGER NOT NULL,
              cycle_time_p50_hours REAL,
              cycle_time_p90_hours REAL,
              lead_time_p50_hours REAL,
              lead_time_p90_hours REAL,
              wip_age_p50_hours REAL,
              wip_age_p90_hours REAL,
              bug_completed_ratio REAL NOT NULL,
              story_points_completed REAL NOT NULL,
              new_bugs_count INTEGER NOT NULL DEFAULT 0,
              new_items_count INTEGER NOT NULL DEFAULT 0,
              defect_intro_rate REAL NOT NULL DEFAULT 0.0,
              wip_congestion_ratio REAL NOT NULL DEFAULT 0.0,
              predictability_score REAL NOT NULL DEFAULT 0.0,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, day, team_id, work_scope_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_user_metrics_daily (
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              user_identity TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              items_started INTEGER NOT NULL,
              items_completed INTEGER NOT NULL,
              wip_count_end_of_day INTEGER NOT NULL,
              cycle_time_p50_hours REAL,
              cycle_time_p90_hours REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, work_scope_id, user_identity, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_cycle_times (
              work_item_id TEXT NOT NULL,
              provider TEXT NOT NULL,
              day TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              team_id TEXT,
              team_name TEXT,
              assignee TEXT,
              type TEXT NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL,
              started_at TEXT,
              completed_at TEXT,
              cycle_time_hours REAL,
              lead_time_hours REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, work_item_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_item_state_durations_daily (
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              work_scope_id TEXT NOT NULL,
              team_id TEXT NOT NULL,
              team_name TEXT NOT NULL,
              status TEXT NOT NULL,
              duration_hours REAL NOT NULL,
              items_touched INTEGER NOT NULL,
              avg_wip REAL NOT NULL DEFAULT 0.0,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, work_scope_id, team_id, status, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS review_edges_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              reviewer TEXT NOT NULL,
              author TEXT NOT NULL,
              reviews_count INTEGER NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, reviewer, author, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cicd_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              pipelines_count INTEGER NOT NULL,
              success_rate REAL NOT NULL,
              avg_duration_minutes REAL,
              p90_duration_minutes REAL,
              avg_queue_minutes REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS deploy_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              deployments_count INTEGER NOT NULL,
              failed_deployments_count INTEGER NOT NULL,
              deploy_time_p50_hours REAL,
              lead_time_p50_hours REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS incident_metrics_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              incidents_count INTEGER NOT NULL,
              mttr_p50_hours REAL,
              mttr_p90_hours REAL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS ic_landscape_rolling_30d (
              repo_id TEXT NOT NULL,
              as_of_day TEXT NOT NULL,
              identity_id TEXT NOT NULL,
              team_id TEXT,
              map_name TEXT NOT NULL,
              x_raw REAL NOT NULL,
              y_raw REAL NOT NULL,
              x_norm REAL NOT NULL,
              y_norm REAL NOT NULL,
              churn_loc_30d INTEGER NOT NULL DEFAULT 0,
              delivery_units_30d INTEGER NOT NULL DEFAULT 0,
              cycle_p50_30d_hours REAL NOT NULL DEFAULT 0.0,
              wip_max_30d INTEGER NOT NULL DEFAULT 0,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, map_name, as_of_day, identity_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS file_complexity_snapshots (
              repo_id TEXT NOT NULL,
              as_of_day TEXT NOT NULL,
              ref TEXT NOT NULL,
              file_path TEXT NOT NULL,
              language TEXT,
              loc INTEGER NOT NULL,
              functions_count INTEGER NOT NULL,
              cyclomatic_total INTEGER NOT NULL,
              cyclomatic_avg REAL NOT NULL,
              high_complexity_functions INTEGER NOT NULL,
              very_high_complexity_functions INTEGER NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, as_of_day, file_path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS repo_complexity_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              loc_total INTEGER NOT NULL,
              cyclomatic_total INTEGER NOT NULL,
              cyclomatic_per_kloc REAL NOT NULL,
              high_complexity_functions INTEGER NOT NULL,
              very_high_complexity_functions INTEGER NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS file_hotspot_daily (
              repo_id TEXT NOT NULL,
              day TEXT NOT NULL,
              file_path TEXT NOT NULL,
              churn_loc_30d INTEGER NOT NULL,
              churn_commits_30d INTEGER NOT NULL,
              cyclomatic_total INTEGER NOT NULL,
              cyclomatic_avg REAL NOT NULL,
              blame_concentration REAL,
              risk_score REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (repo_id, day, file_path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS investment_classifications_daily (
              repo_id TEXT,
              day TEXT NOT NULL,
              artifact_type TEXT NOT NULL,
              artifact_id TEXT NOT NULL,
              provider TEXT NOT NULL,
              investment_area TEXT NOT NULL,
              project_stream TEXT,
              confidence REAL NOT NULL,
              rule_id TEXT NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (provider, artifact_type, artifact_id, day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS investment_metrics_daily (
              repo_id TEXT,
              day TEXT NOT NULL,
              team_id TEXT,
              investment_area TEXT NOT NULL,
              project_stream TEXT,
              delivery_units INTEGER NOT NULL,
              work_items_completed INTEGER NOT NULL,
              prs_merged INTEGER NOT NULL,
              churn_loc INTEGER NOT NULL,
              cycle_p50_hours REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (day, investment_area, team_id, project_stream)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS issue_type_metrics_daily (
              repo_id TEXT,
              day TEXT NOT NULL,
              provider TEXT NOT NULL,
              team_id TEXT NOT NULL,
              issue_type_norm TEXT NOT NULL,
              created_count INTEGER NOT NULL,
              completed_count INTEGER NOT NULL,
              active_count INTEGER NOT NULL,
              cycle_p50_hours REAL NOT NULL,
              cycle_p90_hours REAL NOT NULL,
              lead_p50_hours REAL NOT NULL,
              computed_at TEXT NOT NULL,
              PRIMARY KEY (day, provider, team_id, issue_type_norm)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_repo_metrics_daily_day ON repo_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_user_metrics_daily_day ON user_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_commit_metrics_day ON commit_metrics(day)",
            "CREATE INDEX IF NOT EXISTS idx_team_metrics_daily_day ON team_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_work_item_metrics_daily_day ON work_item_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_file_metrics_daily_day ON file_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_work_item_state_durations_daily_day ON work_item_state_durations_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_review_edges_daily_day ON review_edges_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_cicd_metrics_daily_day ON cicd_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_deploy_metrics_daily_day ON deploy_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_incident_metrics_daily_day ON incident_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_ic_landscape_rolling_30d_day ON ic_landscape_rolling_30d(as_of_day)",
            "CREATE INDEX IF NOT EXISTS idx_file_complexity_snapshots_day ON file_complexity_snapshots(as_of_day)",
            "CREATE INDEX IF NOT EXISTS idx_repo_complexity_daily_day ON repo_complexity_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_file_hotspot_daily_day ON file_hotspot_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_investment_classifications_daily_day ON investment_classifications_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_investment_metrics_daily_day ON investment_metrics_daily(day)",
            "CREATE INDEX IF NOT EXISTS idx_issue_type_metrics_daily_day ON issue_type_metrics_daily(day)",
        ]
        with self.engine.begin() as conn:
            for stmt in stmts:
                conn.execute(text(stmt))
            # Best-effort upgrades for older SQLite schemas (no destructive migrations):
            # - Add work_scope_id columns
            # - Add UNIQUE indexes so ON CONFLICT(...) upserts work
            # - Add new columns for IC metrics

            # New User Metrics Columns
            for col, type_ in [
                ("identity_id", "TEXT"),
                ("loc_touched", "INTEGER NOT NULL DEFAULT 0"),
                ("prs_opened", "INTEGER NOT NULL DEFAULT 0"),
                ("work_items_completed", "INTEGER NOT NULL DEFAULT 0"),
                ("work_items_active", "INTEGER NOT NULL DEFAULT 0"),
                ("delivery_units", "INTEGER NOT NULL DEFAULT 0"),
                ("cycle_p50_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("cycle_p90_hours", "REAL NOT NULL DEFAULT 0.0"),
            ]:
                if not self._table_has_column(conn, "user_metrics_daily", col):
                    conn.execute(
                        text(f"ALTER TABLE user_metrics_daily ADD COLUMN {col} {type_}")
                    )

            if not self._table_has_column(
                conn, "work_item_metrics_daily", "work_scope_id"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN work_scope_id TEXT"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "items_started_unassigned"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN items_started_unassigned INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "items_completed_unassigned"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN items_completed_unassigned INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "wip_unassigned_end_of_day"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily ADD COLUMN wip_unassigned_end_of_day INTEGER NOT NULL DEFAULT 0"
                    )
                )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uidx_work_item_metrics_daily_scope "
                    "ON work_item_metrics_daily(provider, day, team_id, work_scope_id)"
                )
            )

            if not self._table_has_column(
                conn, "work_item_user_metrics_daily", "work_scope_id"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_user_metrics_daily ADD COLUMN work_scope_id TEXT"
                    )
                )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uidx_work_item_user_metrics_daily_scope "
                    "ON work_item_user_metrics_daily(provider, work_scope_id, user_identity, day)"
                )
            )

            if not self._table_has_column(
                conn, "work_item_cycle_times", "work_scope_id"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_cycle_times ADD COLUMN work_scope_id TEXT"
                    )
                )
            if not self._table_has_column(conn, "user_metrics_daily", "active_hours"):
                conn.execute(
                    text(
                        "ALTER TABLE user_metrics_daily "
                        "ADD COLUMN active_hours REAL NOT NULL DEFAULT 0.0"
                    )
                )
            if not self._table_has_column(conn, "user_metrics_daily", "weekend_days"):
                conn.execute(
                    text(
                        "ALTER TABLE user_metrics_daily "
                        "ADD COLUMN weekend_days INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "new_bugs_count"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily "
                        "ADD COLUMN new_bugs_count INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "new_items_count"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily "
                        "ADD COLUMN new_items_count INTEGER NOT NULL DEFAULT 0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "defect_intro_rate"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily "
                        "ADD COLUMN defect_intro_rate REAL NOT NULL DEFAULT 0.0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "wip_congestion_ratio"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily "
                        "ADD COLUMN wip_congestion_ratio REAL NOT NULL DEFAULT 0.0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_metrics_daily", "predictability_score"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_metrics_daily "
                        "ADD COLUMN predictability_score REAL NOT NULL DEFAULT 0.0"
                    )
                )
            if not self._table_has_column(
                conn, "work_item_state_durations_daily", "avg_wip"
            ):
                conn.execute(
                    text(
                        "ALTER TABLE work_item_state_durations_daily "
                        "ADD COLUMN avg_wip REAL NOT NULL DEFAULT 0.0"
                    )
                )

            self._wi_metrics_has_work_scope = self._table_has_column(
                conn, "work_item_metrics_daily", "work_scope_id"
            )
            self._wi_user_metrics_has_work_scope = self._table_has_column(
                conn, "work_item_user_metrics_daily", "work_scope_id"
            )
            self._wi_cycle_has_work_scope = self._table_has_column(
                conn, "work_item_cycle_times", "work_scope_id"
            )
            self._wi_state_has_work_scope = self._table_has_column(
                conn, "work_item_state_durations_daily", "work_scope_id"
            )

            # Upgrades for repo_metrics_daily
            for col, type_ in [
                ("pr_cycle_p75_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_cycle_p90_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("prs_with_first_review", "INTEGER NOT NULL DEFAULT 0"),
                ("pr_first_review_p50_hours", "REAL"),
                ("pr_first_review_p90_hours", "REAL"),
                ("pr_review_time_p50_hours", "REAL"),
                ("pr_pickup_time_p50_hours", "REAL"),
                ("large_pr_ratio", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_rework_ratio", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_size_p50_loc", "REAL"),
                ("pr_size_p90_loc", "REAL"),
                ("pr_comments_per_100_loc", "REAL"),
                ("pr_reviews_per_100_loc", "REAL"),
                ("rework_churn_ratio_30d", "REAL NOT NULL DEFAULT 0.0"),
                ("single_owner_file_ratio_30d", "REAL NOT NULL DEFAULT 0.0"),
                ("review_load_top_reviewer_ratio", "REAL NOT NULL DEFAULT 0.0"),
                ("bus_factor", "INTEGER NOT NULL DEFAULT 0"),
                ("code_ownership_gini", "REAL NOT NULL DEFAULT 0.0"),
                ("mttr_hours", "REAL"),
                ("change_failure_rate", "REAL NOT NULL DEFAULT 0.0"),
            ]:
                if not self._table_has_column(conn, "repo_metrics_daily", col):
                    conn.execute(
                        text(f"ALTER TABLE repo_metrics_daily ADD COLUMN {col} {type_}")
                    )

            # Upgrades for user_metrics_daily
            for col, type_ in [
                ("pr_cycle_p75_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("pr_cycle_p90_hours", "REAL NOT NULL DEFAULT 0.0"),
                ("prs_with_first_review", "INTEGER NOT NULL DEFAULT 0"),
                ("pr_first_review_p50_hours", "REAL"),
                ("pr_first_review_p90_hours", "REAL"),
                ("pr_review_time_p50_hours", "REAL"),
                ("pr_pickup_time_p50_hours", "REAL"),
                ("reviews_given", "INTEGER NOT NULL DEFAULT 0"),
                ("changes_requested_given", "INTEGER NOT NULL DEFAULT 0"),
                ("reviews_received", "INTEGER NOT NULL DEFAULT 0"),
                ("review_reciprocity", "REAL NOT NULL DEFAULT 0.0"),
                ("team_id", "TEXT"),
                ("team_name", "TEXT"),
            ]:
                if not self._table_has_column(conn, "user_metrics_daily", col):
                    conn.execute(
                        text(f"ALTER TABLE user_metrics_daily ADD COLUMN {col} {type_}")
                    )

    def ensure_schema(self) -> None:
        """Create SQLite tables via DDL statements."""
        self.ensure_tables()

    @staticmethod
    def _table_has_column(conn, table: str, column: str) -> bool:
        try:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        except Exception:
            return False
        cols = {str(r[1]) for r in rows if len(r) >= 2}
        return column in cols

    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO repo_metrics_daily (
              repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
              large_commit_ratio, prs_merged, median_pr_cycle_hours,
              pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review,
              pr_first_review_p50_hours, pr_first_review_p90_hours, pr_review_time_p50_hours, pr_pickup_time_p50_hours,
              large_pr_ratio, pr_rework_ratio,
              pr_size_p50_loc, pr_size_p90_loc, pr_comments_per_100_loc, pr_reviews_per_100_loc,
              rework_churn_ratio_30d, single_owner_file_ratio_30d, review_load_top_reviewer_ratio,
              bus_factor, code_ownership_gini,
              mttr_hours, change_failure_rate,
              computed_at
            ) VALUES (
              :repo_id, :day, :commits_count, :total_loc_touched, :avg_commit_size_loc,
              :large_commit_ratio, :prs_merged, :median_pr_cycle_hours,
              :pr_cycle_p75_hours, :pr_cycle_p90_hours, :prs_with_first_review,
              :pr_first_review_p50_hours, :pr_first_review_p90_hours, :pr_review_time_p50_hours, :pr_pickup_time_p50_hours,
              :large_pr_ratio, :pr_rework_ratio,
              :pr_size_p50_loc, :pr_size_p90_loc, :pr_comments_per_100_loc, :pr_reviews_per_100_loc,
              :rework_churn_ratio_30d, :single_owner_file_ratio_30d, :review_load_top_reviewer_ratio,
              :bus_factor, :code_ownership_gini,
              :mttr_hours, :change_failure_rate,
              :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              commits_count=excluded.commits_count,
              total_loc_touched=excluded.total_loc_touched,
              avg_commit_size_loc=excluded.avg_commit_size_loc,
              large_commit_ratio=excluded.large_commit_ratio,
              prs_merged=excluded.prs_merged,
              median_pr_cycle_hours=excluded.median_pr_cycle_hours,
              pr_cycle_p75_hours=excluded.pr_cycle_p75_hours,
              pr_cycle_p90_hours=excluded.pr_cycle_p90_hours,
              prs_with_first_review=excluded.prs_with_first_review,
              pr_first_review_p50_hours=excluded.pr_first_review_p50_hours,
              pr_first_review_p90_hours=excluded.pr_first_review_p90_hours,
              pr_review_time_p50_hours=excluded.pr_review_time_p50_hours,
              pr_pickup_time_p50_hours=excluded.pr_pickup_time_p50_hours,
              large_pr_ratio=excluded.large_pr_ratio,
              pr_rework_ratio=excluded.pr_rework_ratio,
              pr_size_p50_loc=excluded.pr_size_p50_loc,
              pr_size_p90_loc=excluded.pr_size_p90_loc,
              pr_comments_per_100_loc=excluded.pr_comments_per_100_loc,
              pr_reviews_per_100_loc=excluded.pr_reviews_per_100_loc,
              rework_churn_ratio_30d=excluded.rework_churn_ratio_30d,
              single_owner_file_ratio_30d=excluded.single_owner_file_ratio_30d,
              review_load_top_reviewer_ratio=excluded.review_load_top_reviewer_ratio,
              bus_factor=excluded.bus_factor,
              code_ownership_gini=excluded.code_ownership_gini,
              mttr_hours=excluded.mttr_hours,
              change_failure_rate=excluded.change_failure_rate,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._repo_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_user_metrics(self, rows: Sequence[UserMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO user_metrics_daily (
              repo_id, day, author_email, commits_count, loc_added, loc_deleted,
              files_changed, large_commits_count, avg_commit_size_loc,
              prs_authored, prs_merged, avg_pr_cycle_hours, median_pr_cycle_hours,
              pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review,
              pr_first_review_p50_hours, pr_first_review_p90_hours, pr_review_time_p50_hours, pr_pickup_time_p50_hours,
              reviews_given, changes_requested_given, reviews_received, review_reciprocity, team_id, team_name,
              active_hours, weekend_days, identity_id, loc_touched, prs_opened, work_items_completed, work_items_active,
              delivery_units, cycle_p50_hours, cycle_p90_hours, computed_at
            ) VALUES (
              :repo_id, :day, :author_email, :commits_count, :loc_added, :loc_deleted,
              :files_changed, :large_commits_count, :avg_commit_size_loc,
              :prs_authored, :prs_merged, :avg_pr_cycle_hours, :median_pr_cycle_hours,
              :pr_cycle_p75_hours, :pr_cycle_p90_hours, :prs_with_first_review,
              :pr_first_review_p50_hours, :pr_first_review_p90_hours, :pr_review_time_p50_hours, :pr_pickup_time_p50_hours,
              :reviews_given, :changes_requested_given, :reviews_received, :review_reciprocity, :team_id, :team_name,
              :active_hours, :weekend_days, :identity_id, :loc_touched, :prs_opened, :work_items_completed, :work_items_active,
              :delivery_units, :cycle_p50_hours, :cycle_p90_hours, :computed_at
            )
            ON CONFLICT(repo_id, author_email, day) DO UPDATE SET
              commits_count=excluded.commits_count,
              loc_added=excluded.loc_added,
              loc_deleted=excluded.loc_deleted,
              files_changed=excluded.files_changed,
              large_commits_count=excluded.large_commits_count,
              avg_commit_size_loc=excluded.avg_commit_size_loc,
              prs_authored=excluded.prs_authored,
              prs_merged=excluded.prs_merged,
              avg_pr_cycle_hours=excluded.avg_pr_cycle_hours,
              median_pr_cycle_hours=excluded.median_pr_cycle_hours,
              pr_cycle_p75_hours=excluded.pr_cycle_p75_hours,
              pr_cycle_p90_hours=excluded.pr_cycle_p90_hours,
              prs_with_first_review=excluded.prs_with_first_review,
              pr_first_review_p50_hours=excluded.pr_first_review_p50_hours,
              pr_first_review_p90_hours=excluded.pr_first_review_p90_hours,
              pr_review_time_p50_hours=excluded.pr_review_time_p50_hours,
              pr_pickup_time_p50_hours=excluded.pr_pickup_time_p50_hours,
              reviews_given=excluded.reviews_given,
              changes_requested_given=excluded.changes_requested_given,
              reviews_received=excluded.reviews_received,
              review_reciprocity=excluded.review_reciprocity,
              team_id=excluded.team_id,
              team_name=excluded.team_name,
              active_hours=excluded.active_hours,
              weekend_days=excluded.weekend_days,
              identity_id=excluded.identity_id,
              loc_touched=excluded.loc_touched,
              prs_opened=excluded.prs_opened,
              work_items_completed=excluded.work_items_completed,
              work_items_active=excluded.work_items_active,
              delivery_units=excluded.delivery_units,
              cycle_p50_hours=excluded.cycle_p50_hours,
              cycle_p90_hours=excluded.cycle_p90_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._user_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO commit_metrics (
              repo_id, commit_hash, day, author_email, total_loc, files_changed, size_bucket, computed_at
            ) VALUES (
              :repo_id, :commit_hash, :day, :author_email, :total_loc, :files_changed, :size_bucket, :computed_at
            )
            ON CONFLICT(repo_id, day, author_email, commit_hash) DO UPDATE SET
              total_loc=excluded.total_loc,
              files_changed=excluded.files_changed,
              size_bucket=excluded.size_bucket,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._commit_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_file_metrics(self, rows: Sequence[FileMetricsRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO file_metrics_daily (
              repo_id, day, path, churn, contributors, commits_count, hotspot_score, computed_at
            ) VALUES (
              :repo_id, :day, :path, :churn, :contributors, :commits_count, :hotspot_score, :computed_at
            )
            ON CONFLICT(repo_id, day, path) DO UPDATE SET
              churn=excluded.churn,
              contributors=excluded.contributors,
              commits_count=excluded.commits_count,
              hotspot_score=excluded.hotspot_score,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._file_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _file_row(self, row: FileMetricsRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "path": str(data["path"]),
            "churn": int(data["churn"]),
            "contributors": int(data["contributors"]),
            "commits_count": int(data["commits_count"]),
            "hotspot_score": float(data["hotspot_score"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _repo_row(self, row: RepoMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "commits_count": int(data["commits_count"]),
            "total_loc_touched": int(data["total_loc_touched"]),
            "avg_commit_size_loc": float(data["avg_commit_size_loc"]),
            "large_commit_ratio": float(data["large_commit_ratio"]),
            "prs_merged": int(data["prs_merged"]),
            "median_pr_cycle_hours": float(data["median_pr_cycle_hours"]),
            "pr_cycle_p75_hours": float(data.get("pr_cycle_p75_hours", 0.0) or 0.0),
            "pr_cycle_p90_hours": float(data.get("pr_cycle_p90_hours", 0.0) or 0.0),
            "prs_with_first_review": int(data.get("prs_with_first_review", 0) or 0),
            "pr_first_review_p50_hours": data.get("pr_first_review_p50_hours"),
            "pr_first_review_p90_hours": data.get("pr_first_review_p90_hours"),
            "pr_review_time_p50_hours": data.get("pr_review_time_p50_hours"),
            "pr_pickup_time_p50_hours": data.get("pr_pickup_time_p50_hours"),
            "large_pr_ratio": float(data.get("large_pr_ratio", 0.0) or 0.0),
            "pr_rework_ratio": float(data.get("pr_rework_ratio", 0.0) or 0.0),
            "pr_size_p50_loc": data.get("pr_size_p50_loc"),
            "pr_size_p90_loc": data.get("pr_size_p90_loc"),
            "pr_comments_per_100_loc": data.get("pr_comments_per_100_loc"),
            "pr_reviews_per_100_loc": data.get("pr_reviews_per_100_loc"),
            "rework_churn_ratio_30d": float(
                data.get("rework_churn_ratio_30d", 0.0) or 0.0
            ),
            "single_owner_file_ratio_30d": float(
                data.get("single_owner_file_ratio_30d", 0.0) or 0.0
            ),
            "review_load_top_reviewer_ratio": float(
                data.get("review_load_top_reviewer_ratio", 0.0) or 0.0
            ),
            "bus_factor": int(data.get("bus_factor", 0) or 0),
            "code_ownership_gini": float(data.get("code_ownership_gini", 0.0) or 0.0),
            "mttr_hours": data.get("mttr_hours"),
            "change_failure_rate": float(data.get("change_failure_rate", 0.0) or 0.0),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _user_row(self, row: UserMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "author_email": str(data["author_email"]),
            "commits_count": int(data["commits_count"]),
            "loc_added": int(data["loc_added"]),
            "loc_deleted": int(data["loc_deleted"]),
            "files_changed": int(data["files_changed"]),
            "large_commits_count": int(data["large_commits_count"]),
            "avg_commit_size_loc": float(data["avg_commit_size_loc"]),
            "prs_authored": int(data["prs_authored"]),
            "prs_merged": int(data["prs_merged"]),
            "avg_pr_cycle_hours": float(data["avg_pr_cycle_hours"]),
            "median_pr_cycle_hours": float(data["median_pr_cycle_hours"]),
            "pr_cycle_p75_hours": float(data.get("pr_cycle_p75_hours", 0.0) or 0.0),
            "pr_cycle_p90_hours": float(data.get("pr_cycle_p90_hours", 0.0) or 0.0),
            "prs_with_first_review": int(data.get("prs_with_first_review", 0) or 0),
            "pr_first_review_p50_hours": data.get("pr_first_review_p50_hours"),
            "pr_first_review_p90_hours": data.get("pr_first_review_p90_hours"),
            "pr_review_time_p50_hours": data.get("pr_review_time_p50_hours"),
            "pr_pickup_time_p50_hours": data.get("pr_pickup_time_p50_hours"),
            "reviews_given": int(data.get("reviews_given", 0) or 0),
            "changes_requested_given": int(data.get("changes_requested_given", 0) or 0),
            "reviews_received": int(data.get("reviews_received", 0) or 0),
            "review_reciprocity": float(data.get("review_reciprocity", 0.0) or 0.0),
            "team_id": data.get("team_id"),
            "team_name": data.get("team_name"),
            "active_hours": float(data.get("active_hours", 0.0) or 0.0),
            "weekend_days": int(data.get("weekend_days", 0) or 0),
            "identity_id": str(data.get("identity_id") or "")
            or str(data["author_email"]),
            "loc_touched": int(data.get("loc_touched", 0) or 0),
            "prs_opened": int(data.get("prs_opened", 0) or 0),
            "work_items_completed": int(data.get("work_items_completed", 0) or 0),
            "work_items_active": int(data.get("work_items_active", 0) or 0),
            "delivery_units": int(data.get("delivery_units", 0) or 0),
            "cycle_p50_hours": float(data.get("cycle_p50_hours", 0.0) or 0.0),
            "cycle_p90_hours": float(data.get("cycle_p90_hours", 0.0) or 0.0),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _review_edge_row(self, row: ReviewEdgeDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "reviewer": str(data["reviewer"]),
            "author": str(data["author"]),
            "reviews_count": int(data["reviews_count"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _cicd_row(self, row: CICDMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "pipelines_count": int(data["pipelines_count"]),
            "success_rate": float(data["success_rate"]),
            "avg_duration_minutes": data.get("avg_duration_minutes"),
            "p90_duration_minutes": data.get("p90_duration_minutes"),
            "avg_queue_minutes": data.get("avg_queue_minutes"),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _deploy_row(self, row: DeployMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "deployments_count": int(data["deployments_count"]),
            "failed_deployments_count": int(data["failed_deployments_count"]),
            "deploy_time_p50_hours": data.get("deploy_time_p50_hours"),
            "lead_time_p50_hours": data.get("lead_time_p50_hours"),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _incident_row(self, row: IncidentMetricsDailyRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "incidents_count": int(data["incidents_count"]),
            "mttr_p50_hours": data.get("mttr_p50_hours"),
            "mttr_p90_hours": data.get("mttr_p90_hours"),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def write_ic_landscape_rolling(
        self, rows: Sequence[ICLandscapeRollingRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO ic_landscape_rolling_30d (
              repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm,
              churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d, computed_at
            ) VALUES (
              :repo_id, :as_of_day, :identity_id, :team_id, :map_name, :x_raw, :y_raw, :x_norm, :y_norm,
              :churn_loc_30d, :delivery_units_30d, :cycle_p50_30d_hours, :wip_max_30d, :computed_at
            )
            ON CONFLICT(repo_id, map_name, as_of_day, identity_id) DO UPDATE SET
              team_id=excluded.team_id,
              x_raw=excluded.x_raw,
              y_raw=excluded.y_raw,
              x_norm=excluded.x_norm,
              y_norm=excluded.y_norm,
              churn_loc_30d=excluded.churn_loc_30d,
              delivery_units_30d=excluded.delivery_units_30d,
              cycle_p50_30d_hours=excluded.cycle_p50_30d_hours,
              wip_max_30d=excluded.wip_max_30d,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                "repo_id": str(data["repo_id"]),
                "as_of_day": data["as_of_day"].isoformat(),
                "identity_id": str(data["identity_id"]),
                "team_id": str(data["team_id"] or ""),
                "map_name": str(data["map_name"]),
                "x_raw": float(data["x_raw"]),
                "y_raw": float(data["y_raw"]),
                "x_norm": float(data["x_norm"]),
                "y_norm": float(data["y_norm"]),
                "churn_loc_30d": int(data["churn_loc_30d"]),
                "delivery_units_30d": int(data["delivery_units_30d"]),
                "cycle_p50_30d_hours": float(data["cycle_p50_30d_hours"]),
                "wip_max_30d": int(data["wip_max_30d"]),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def get_rolling_30d_user_stats(
        self,
        as_of_day: date,
        repo_id: Optional[
            str
        ] = None,  # repo_id in sqlite sink is usually handled upstream but we support filtering
    ) -> List[Dict[str, Any]]:
        """
        Compute rolling 30d stats by aggregating daily rows in Python.
        """
        start_day = as_of_day - timedelta(days=29)
        start_str = start_day.isoformat()
        end_str = as_of_day.isoformat()

        # Select raw rows
        query = """
        SELECT
            COALESCE(identity_id, author_email) as identity_id,
            team_id,
            loc_touched,
            delivery_units,
            cycle_p50_hours,
            work_items_active
        FROM user_metrics_daily
        WHERE day >= :start AND day <= :end
        """
        params: Dict[str, Any] = {"start": start_str, "end": end_str}

        if repo_id:
            query += " AND repo_id = :repo_id"
            params["repo_id"] = str(repo_id)

        rows = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()

        # Aggregate in Python
        aggs: Dict[str, Dict[str, Any]] = {}

        for r in rows:
            identity_id = r[0]
            team_id = r[1]
            loc_touched = r[2] or 0
            delivery_units = r[3] or 0
            cycle_p50 = r[4] or 0.0
            wip = r[5] or 0

            if identity_id not in aggs:
                aggs[identity_id] = {
                    "identity_id": identity_id,
                    "team_id": team_id,  # Take first found team_id
                    "churn_loc_30d": 0,
                    "delivery_units_30d": 0,
                    "wip_max_30d": 0,
                    "cycle_p50_values": [],
                }

            entry = aggs[identity_id]
            entry["churn_loc_30d"] += loc_touched
            entry["delivery_units_30d"] += delivery_units
            entry["wip_max_30d"] = max(entry["wip_max_30d"], wip)
            if cycle_p50 > 0:
                entry["cycle_p50_values"].append(cycle_p50)

            # Update team_id if missing (simple last-wins or non-null wins strategy)
            if not entry["team_id"] and team_id:
                entry["team_id"] = team_id

        # Finalize
        results = []
        for identity, data in aggs.items():
            cycle_vals = data.pop("cycle_p50_values")
            median_cycle = 0.0
            if cycle_vals:
                cycle_vals.sort()
                mid = len(cycle_vals) // 2
                if len(cycle_vals) % 2 == 1:
                    median_cycle = cycle_vals[mid]
                else:
                    median_cycle = (cycle_vals[mid - 1] + cycle_vals[mid]) / 2.0

            data["cycle_p50_30d_hours"] = median_cycle
            results.append(data)

        return results

    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO team_metrics_daily (
              day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
              after_hours_commit_ratio, weekend_commit_ratio, computed_at
            ) VALUES (
              :day, :team_id, :team_name, :commits_count, :after_hours_commits_count, :weekend_commits_count,
              :after_hours_commit_ratio, :weekend_commit_ratio, :computed_at
            )
            ON CONFLICT(team_id, day) DO UPDATE SET
              team_name=excluded.team_name,
              commits_count=excluded.commits_count,
              after_hours_commits_count=excluded.after_hours_commits_count,
              weekend_commits_count=excluded.weekend_commits_count,
              after_hours_commit_ratio=excluded.after_hours_commit_ratio,
              weekend_commit_ratio=excluded.weekend_commit_ratio,
              computed_at=excluded.computed_at
            """
        )
        payload = [asdict(r) for r in rows]
        for doc in payload:
            doc["day"] = doc["day"].isoformat()
            doc["computed_at"] = _dt_to_sqlite(doc["computed_at"])
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_work_item_metrics(
        self, rows: Sequence[WorkItemMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        if self._wi_metrics_has_work_scope:
            stmt = text(
                """
                INSERT INTO work_item_metrics_daily (
                  day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
                  items_started_unassigned, items_completed_unassigned, wip_unassigned_end_of_day,
                  cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
                  wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
                  new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at
                ) VALUES (
                  :day, :provider, :work_scope_id, :team_id, :team_name, :items_started, :items_completed, :wip_count_end_of_day,
                  :items_started_unassigned, :items_completed_unassigned, :wip_unassigned_end_of_day,
                  :cycle_time_p50_hours, :cycle_time_p90_hours, :lead_time_p50_hours, :lead_time_p90_hours,
                  :wip_age_p50_hours, :wip_age_p90_hours, :bug_completed_ratio, :story_points_completed,
                  :new_bugs_count, :new_items_count, :defect_intro_rate, :wip_congestion_ratio, :predictability_score, :computed_at
                )
                ON CONFLICT(provider, day, team_id, work_scope_id) DO UPDATE SET
                  team_name=excluded.team_name,
                  items_started=excluded.items_started,
                  items_completed=excluded.items_completed,
                  items_started_unassigned=excluded.items_started_unassigned,
                  items_completed_unassigned=excluded.items_completed_unassigned,
                  wip_count_end_of_day=excluded.wip_count_end_of_day,
                  wip_unassigned_end_of_day=excluded.wip_unassigned_end_of_day,
                  cycle_time_p50_hours=excluded.cycle_time_p50_hours,
                  cycle_time_p90_hours=excluded.cycle_time_p90_hours,
                  lead_time_p50_hours=excluded.lead_time_p50_hours,
                  lead_time_p90_hours=excluded.lead_time_p90_hours,
                  wip_age_p50_hours=excluded.wip_age_p50_hours,
                  wip_age_p90_hours=excluded.wip_age_p90_hours,
                  bug_completed_ratio=excluded.bug_completed_ratio,
                  story_points_completed=excluded.story_points_completed,
                  new_bugs_count=excluded.new_bugs_count,
                  new_items_count=excluded.new_items_count,
                  defect_intro_rate=excluded.defect_intro_rate,
                  wip_congestion_ratio=excluded.wip_congestion_ratio,
                  predictability_score=excluded.predictability_score,
                  computed_at=excluded.computed_at
                """
            )
        else:
            # Legacy schema used `repo_id` as the scope column.
            stmt = text(
                """
                INSERT INTO work_item_metrics_daily (
                  day, provider, repo_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
                  items_started_unassigned, items_completed_unassigned, wip_unassigned_end_of_day,
                  cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
                  wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
                  new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at
                ) VALUES (
                  :day, :provider, :repo_id, :team_id, :team_name, :items_started, :items_completed, :wip_count_end_of_day,
                  :items_started_unassigned, :items_completed_unassigned, :wip_unassigned_end_of_day,
                  :cycle_time_p50_hours, :cycle_time_p90_hours, :lead_time_p50_hours, :lead_time_p90_hours,
                  :wip_age_p50_hours, :wip_age_p90_hours, :bug_completed_ratio, :story_points_completed,
                  :new_bugs_count, :new_items_count, :defect_intro_rate, :wip_congestion_ratio, :predictability_score, :computed_at
                )
                ON CONFLICT(provider, day, team_id, repo_id) DO UPDATE SET
                  team_name=excluded.team_name,
                  items_started=excluded.items_started,
                  items_completed=excluded.items_completed,
                  items_started_unassigned=excluded.items_started_unassigned,
                  items_completed_unassigned=excluded.items_completed_unassigned,
                  wip_count_end_of_day=excluded.wip_count_end_of_day,
                  wip_unassigned_end_of_day=excluded.wip_unassigned_end_of_day,
                  cycle_time_p50_hours=excluded.cycle_time_p50_hours,
                  cycle_time_p90_hours=excluded.cycle_time_p90_hours,
                  lead_time_p50_hours=excluded.lead_time_p50_hours,
                  lead_time_p90_hours=excluded.lead_time_p90_hours,
                  wip_age_p50_hours=excluded.wip_age_p50_hours,
                  wip_age_p90_hours=excluded.wip_age_p90_hours,
                  bug_completed_ratio=excluded.bug_completed_ratio,
                  story_points_completed=excluded.story_points_completed,
                  new_bugs_count=excluded.new_bugs_count,
                  new_items_count=excluded.new_items_count,
                  defect_intro_rate=excluded.defect_intro_rate,
                  wip_congestion_ratio=excluded.wip_congestion_ratio,
                  predictability_score=excluded.predictability_score,
                  computed_at=excluded.computed_at
                """
            )
        payload = []
        for row in rows:
            data = asdict(row)
            base = {
                **data,
                "day": data["day"].isoformat(),
                "team_id": str(data.get("team_id") or ""),
                "team_name": str(data.get("team_name") or ""),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            }
            if self._wi_metrics_has_work_scope:
                base["work_scope_id"] = str(data.get("work_scope_id") or "")
            else:
                base["repo_id"] = str(data.get("work_scope_id") or "")
            payload.append(base)
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_work_item_user_metrics(
        self, rows: Sequence[WorkItemUserMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO work_item_user_metrics_daily (
              day, provider, work_scope_id, user_identity, team_id, team_name, items_started, items_completed, wip_count_end_of_day,
              cycle_time_p50_hours, cycle_time_p90_hours, computed_at
            ) VALUES (
              :day, :provider, :work_scope_id, :user_identity, :team_id, :team_name, :items_started, :items_completed, :wip_count_end_of_day,
              :cycle_time_p50_hours, :cycle_time_p90_hours, :computed_at
            )
            ON CONFLICT(provider, work_scope_id, user_identity, day) DO UPDATE SET
              team_id=excluded.team_id,
              team_name=excluded.team_name,
              items_started=excluded.items_started,
              items_completed=excluded.items_completed,
              wip_count_end_of_day=excluded.wip_count_end_of_day,
              cycle_time_p50_hours=excluded.cycle_time_p50_hours,
              cycle_time_p90_hours=excluded.cycle_time_p90_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                **data,
                "day": data["day"].isoformat(),
                "work_scope_id": str(data.get("work_scope_id") or ""),
                "team_id": str(data.get("team_id") or ""),
                "team_name": str(data.get("team_name") or ""),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_work_item_cycle_times(
        self, rows: Sequence[WorkItemCycleTimeRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO work_item_cycle_times (
              work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type, status,
              created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at
            ) VALUES (
              :work_item_id, :provider, :day, :work_scope_id, :team_id, :team_name, :assignee, :type, :status,
              :created_at, :started_at, :completed_at, :cycle_time_hours, :lead_time_hours, :computed_at
            )
            ON CONFLICT(provider, work_item_id) DO UPDATE SET
              day=excluded.day,
              work_scope_id=excluded.work_scope_id,
              team_id=excluded.team_id,
              team_name=excluded.team_name,
              assignee=excluded.assignee,
              type=excluded.type,
              status=excluded.status,
              created_at=excluded.created_at,
              started_at=excluded.started_at,
              completed_at=excluded.completed_at,
              cycle_time_hours=excluded.cycle_time_hours,
              lead_time_hours=excluded.lead_time_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                **data,
                "day": data["day"].isoformat(),
                "work_scope_id": str(data.get("work_scope_id") or ""),
                "created_at": _dt_to_sqlite(data["created_at"]),
                "started_at": _dt_to_sqlite(data["started_at"])
                if data.get("started_at")
                else None,
                "completed_at": _dt_to_sqlite(data["completed_at"])
                if data.get("completed_at")
                else None,
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _commit_row(self, row: CommitMetricsRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "commit_hash": str(data["commit_hash"]),
            "day": data["day"].isoformat(),
            "author_email": str(data["author_email"]),
            "total_loc": int(data["total_loc"]),
            "files_changed": int(data["files_changed"]),
            "size_bucket": str(data["size_bucket"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def write_work_item_state_durations(
        self, rows: Sequence[WorkItemStateDurationDailyRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO work_item_state_durations_daily (
              day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at
            ) VALUES (
              :day, :provider, :work_scope_id, :team_id, :team_name, :status, :duration_hours, :items_touched, :avg_wip, :computed_at
            )
            ON CONFLICT(provider, work_scope_id, team_id, status, day) DO UPDATE SET
              team_name=excluded.team_name,
              duration_hours=excluded.duration_hours,
              items_touched=excluded.items_touched,
              avg_wip=excluded.avg_wip,
              computed_at=excluded.computed_at
            """
        )
        payload = []
        for row in rows:
            data = asdict(row)
            payload.append({
                **data,
                "day": data["day"].isoformat(),
                "work_scope_id": str(data.get("work_scope_id") or ""),
                "team_id": str(data.get("team_id") or ""),
                "team_name": str(data.get("team_name") or ""),
                "computed_at": _dt_to_sqlite(data["computed_at"]),
            })
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_review_edges(self, rows: Sequence[ReviewEdgeDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO review_edges_daily (
              repo_id, day, reviewer, author, reviews_count, computed_at
            ) VALUES (
              :repo_id, :day, :reviewer, :author, :reviews_count, :computed_at
            )
            ON CONFLICT(repo_id, reviewer, author, day) DO UPDATE SET
              reviews_count=excluded.reviews_count,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._review_edge_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_cicd_metrics(self, rows: Sequence[CICDMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO cicd_metrics_daily (
              repo_id, day, pipelines_count, success_rate, avg_duration_minutes,
              p90_duration_minutes, avg_queue_minutes, computed_at
            ) VALUES (
              :repo_id, :day, :pipelines_count, :success_rate, :avg_duration_minutes,
              :p90_duration_minutes, :avg_queue_minutes, :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              pipelines_count=excluded.pipelines_count,
              success_rate=excluded.success_rate,
              avg_duration_minutes=excluded.avg_duration_minutes,
              p90_duration_minutes=excluded.p90_duration_minutes,
              avg_queue_minutes=excluded.avg_queue_minutes,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._cicd_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_deploy_metrics(self, rows: Sequence[DeployMetricsDailyRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO deploy_metrics_daily (
              repo_id, day, deployments_count, failed_deployments_count,
              deploy_time_p50_hours, lead_time_p50_hours, computed_at
            ) VALUES (
              :repo_id, :day, :deployments_count, :failed_deployments_count,
              :deploy_time_p50_hours, :lead_time_p50_hours, :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              deployments_count=excluded.deployments_count,
              failed_deployments_count=excluded.failed_deployments_count,
              deploy_time_p50_hours=excluded.deploy_time_p50_hours,
              lead_time_p50_hours=excluded.lead_time_p50_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._deploy_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_incident_metrics(
        self, rows: Sequence[IncidentMetricsDailyRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO incident_metrics_daily (
              repo_id, day, incidents_count, mttr_p50_hours, mttr_p90_hours, computed_at
            ) VALUES (
              :repo_id, :day, :incidents_count, :mttr_p50_hours, :mttr_p90_hours, :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              incidents_count=excluded.incidents_count,
              mttr_p50_hours=excluded.mttr_p50_hours,
              mttr_p90_hours=excluded.mttr_p90_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._incident_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_file_complexity_snapshots(
        self, rows: Sequence[FileComplexitySnapshot]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO file_complexity_snapshots (
              repo_id, as_of_day, ref, file_path, language, loc, functions_count,
              cyclomatic_total, cyclomatic_avg, high_complexity_functions,
              very_high_complexity_functions, computed_at
            ) VALUES (
              :repo_id, :as_of_day, :ref, :file_path, :language, :loc, :functions_count,
              :cyclomatic_total, :cyclomatic_avg, :high_complexity_functions,
              :very_high_complexity_functions, :computed_at
            )
            ON CONFLICT(repo_id, as_of_day, file_path) DO UPDATE SET
              ref=excluded.ref,
              language=excluded.language,
              loc=excluded.loc,
              functions_count=excluded.functions_count,
              cyclomatic_total=excluded.cyclomatic_total,
              cyclomatic_avg=excluded.cyclomatic_avg,
              high_complexity_functions=excluded.high_complexity_functions,
              very_high_complexity_functions=excluded.very_high_complexity_functions,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._complexity_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_repo_complexity_daily(self, rows: Sequence[RepoComplexityDaily]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO repo_complexity_daily (
              repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc,
              high_complexity_functions, very_high_complexity_functions, computed_at
            ) VALUES (
              :repo_id, :day, :loc_total, :cyclomatic_total, :cyclomatic_per_kloc,
              :high_complexity_functions, :very_high_complexity_functions, :computed_at
            )
            ON CONFLICT(repo_id, day) DO UPDATE SET
              loc_total=excluded.loc_total,
              cyclomatic_total=excluded.cyclomatic_total,
              cyclomatic_per_kloc=excluded.cyclomatic_per_kloc,
              high_complexity_functions=excluded.high_complexity_functions,
              very_high_complexity_functions=excluded.very_high_complexity_functions,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._repo_complexity_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def write_file_hotspot_daily(self, rows: Sequence[FileHotspotDaily]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO file_hotspot_daily (
              repo_id, day, file_path, churn_loc_30d, churn_commits_30d,
              cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, computed_at
            ) VALUES (
              :repo_id, :day, :file_path, :churn_loc_30d, :churn_commits_30d,
              :cyclomatic_total, :cyclomatic_avg, :blame_concentration, :risk_score, :computed_at
            )
            ON CONFLICT(repo_id, day, file_path) DO UPDATE SET
              churn_loc_30d=excluded.churn_loc_30d,
              churn_commits_30d=excluded.churn_commits_30d,
              cyclomatic_total=excluded.cyclomatic_total,
              cyclomatic_avg=excluded.cyclomatic_avg,
              blame_concentration=excluded.blame_concentration,
              risk_score=excluded.risk_score,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._hotspot_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _complexity_row(self, row: FileComplexitySnapshot) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "as_of_day": data["as_of_day"].isoformat(),
            "ref": str(data["ref"]),
            "file_path": str(data["file_path"]),
            "language": str(data.get("language") or ""),
            "loc": int(data["loc"]),
            "functions_count": int(data["functions_count"]),
            "cyclomatic_total": int(data["cyclomatic_total"]),
            "cyclomatic_avg": float(data["cyclomatic_avg"]),
            "high_complexity_functions": int(data["high_complexity_functions"]),
            "very_high_complexity_functions": int(
                data["very_high_complexity_functions"]
            ),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _repo_complexity_row(self, row: RepoComplexityDaily) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "loc_total": int(data["loc_total"]),
            "cyclomatic_total": int(data["cyclomatic_total"]),
            "cyclomatic_per_kloc": float(data["cyclomatic_per_kloc"]),
            "high_complexity_functions": int(data["high_complexity_functions"]),
            "very_high_complexity_functions": int(
                data["very_high_complexity_functions"]
            ),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def _hotspot_row(self, row: FileHotspotDaily) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]),
            "day": data["day"].isoformat(),
            "file_path": str(data["file_path"]),
            "churn_loc_30d": int(data["churn_loc_30d"]),
            "churn_commits_30d": int(data["churn_commits_30d"]),
            "cyclomatic_total": int(data["cyclomatic_total"]),
            "cyclomatic_avg": float(data["cyclomatic_avg"]),
            "blame_concentration": data.get("blame_concentration"),
            "risk_score": float(data["risk_score"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    # -------------------------------------------------------------------------
    # Investment / Issue Type metrics
    # -------------------------------------------------------------------------

    def write_investment_classifications(
        self, rows: Sequence[InvestmentClassificationRecord]
    ) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO investment_classifications_daily (
              repo_id, day, artifact_type, artifact_id, provider,
              investment_area, project_stream, confidence, rule_id, computed_at
            ) VALUES (
              :repo_id, :day, :artifact_type, :artifact_id, :provider,
              :investment_area, :project_stream, :confidence, :rule_id, :computed_at
            )
            ON CONFLICT (provider, artifact_type, artifact_id, day) DO UPDATE SET
              repo_id=excluded.repo_id,
              investment_area=excluded.investment_area,
              project_stream=excluded.project_stream,
              confidence=excluded.confidence,
              rule_id=excluded.rule_id,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._investment_classification_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _investment_classification_row(
        self, row: InvestmentClassificationRecord
    ) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]) if data["repo_id"] else None,
            "day": data["day"].isoformat(),
            "artifact_type": str(data["artifact_type"]),
            "artifact_id": str(data["artifact_id"]),
            "provider": str(data["provider"]),
            "investment_area": str(data["investment_area"]),
            "project_stream": data["project_stream"],
            "confidence": float(data["confidence"]),
            "rule_id": str(data["rule_id"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def write_investment_metrics(self, rows: Sequence[InvestmentMetricsRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO investment_metrics_daily (
              repo_id, day, team_id, investment_area, project_stream,
              delivery_units, work_items_completed, prs_merged, churn_loc,
              cycle_p50_hours, computed_at
            ) VALUES (
              :repo_id, :day, :team_id, :investment_area, :project_stream,
              :delivery_units, :work_items_completed, :prs_merged, :churn_loc,
              :cycle_p50_hours, :computed_at
            )
            ON CONFLICT (day, investment_area, team_id, project_stream) DO UPDATE SET
              repo_id=excluded.repo_id,
              delivery_units=excluded.delivery_units,
              work_items_completed=excluded.work_items_completed,
              prs_merged=excluded.prs_merged,
              churn_loc=excluded.churn_loc,
              cycle_p50_hours=excluded.cycle_p50_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._investment_metrics_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _investment_metrics_row(self, row: InvestmentMetricsRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]) if data["repo_id"] else None,
            "day": data["day"].isoformat(),
            "team_id": data["team_id"],
            "investment_area": str(data["investment_area"]),
            "project_stream": data["project_stream"],
            "delivery_units": int(data["delivery_units"]),
            "work_items_completed": int(data["work_items_completed"]),
            "prs_merged": int(data["prs_merged"]),
            "churn_loc": int(data["churn_loc"]),
            "cycle_p50_hours": float(data["cycle_p50_hours"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def write_issue_type_metrics(self, rows: Sequence[IssueTypeMetricsRecord]) -> None:
        if not rows:
            return
        stmt = text(
            """
            INSERT INTO issue_type_metrics_daily (
              repo_id, day, provider, team_id, issue_type_norm,
              created_count, completed_count, active_count,
              cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at
            ) VALUES (
              :repo_id, :day, :provider, :team_id, :issue_type_norm,
              :created_count, :completed_count, :active_count,
              :cycle_p50_hours, :cycle_p90_hours, :lead_p50_hours, :computed_at
            )
            ON CONFLICT (day, provider, team_id, issue_type_norm) DO UPDATE SET
              repo_id=excluded.repo_id,
              created_count=excluded.created_count,
              completed_count=excluded.completed_count,
              active_count=excluded.active_count,
              cycle_p50_hours=excluded.cycle_p50_hours,
              cycle_p90_hours=excluded.cycle_p90_hours,
              lead_p50_hours=excluded.lead_p50_hours,
              computed_at=excluded.computed_at
            """
        )
        payload = [self._issue_type_metrics_row(r) for r in rows]
        with self.engine.begin() as conn:
            conn.execute(stmt, payload)

    def _issue_type_metrics_row(self, row: IssueTypeMetricsRecord) -> dict:
        data = asdict(row)
        return {
            "repo_id": str(data["repo_id"]) if data["repo_id"] else None,
            "day": data["day"].isoformat(),
            "provider": str(data["provider"]),
            "team_id": str(data["team_id"]),
            "issue_type_norm": str(data["issue_type_norm"]),
            "created_count": int(data["created_count"]),
            "completed_count": int(data["completed_count"]),
            "active_count": int(data["active_count"]),
            "cycle_p50_hours": float(data["cycle_p50_hours"]),
            "cycle_p90_hours": float(data["cycle_p90_hours"]),
            "lead_p50_hours": float(data["lead_p50_hours"]),
            "computed_at": _dt_to_sqlite(data["computed_at"]),
        }

    def write_work_unit_investments(
        self, rows: Sequence[WorkUnitInvestmentRecord]
    ) -> None:
        raise NotImplementedError(
            "Work unit investment materialization is not supported for SQLite"
        )

    def write_work_unit_investment_quotes(
        self, rows: Sequence[WorkUnitInvestmentEvidenceQuoteRecord]
    ) -> None:
        raise NotImplementedError(
            "Work unit investment evidence quotes are not supported for SQLite"
        )
