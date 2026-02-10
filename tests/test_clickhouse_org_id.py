"""Tests verifying org_id flows through schemas, sinks, and storage."""

import uuid
from dataclasses import asdict, fields
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

from dev_health_ops.metrics.schemas import (
    CICDMetricsDailyRecord,
    CommitMetricsRecord,
    DeployMetricsDailyRecord,
    DORAMetricsRecord,
    FileComplexitySnapshot,
    FileHotspotDaily,
    FileMetricsRecord,
    ICLandscapeRollingRecord,
    IncidentMetricsDailyRecord,
    InvestmentClassificationRecord,
    InvestmentExplanationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    RepoComplexityDaily,
    RepoMetricsDailyRecord,
    ReviewEdgeDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkGraphEdgeRecord,
    WorkGraphIssuePRRecord,
    WorkGraphPRCommitRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.models.work_items import (
    Sprint,
    Worklog,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
)


# ---------------------------------------------------------------------------
# 1. All schema dataclasses have org_id with correct default
# ---------------------------------------------------------------------------

ALL_SCHEMA_CLASSES = [
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
    CommitMetricsRecord,
    FileMetricsRecord,
    TeamMetricsDailyRecord,
    WorkItemMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
    WorkItemCycleTimeRecord,
    WorkItemStateDurationDailyRecord,
    ReviewEdgeDailyRecord,
    CICDMetricsDailyRecord,
    DeployMetricsDailyRecord,
    IncidentMetricsDailyRecord,
    DORAMetricsRecord,
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
    WorkUnitInvestmentRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
]

ALL_WORK_ITEM_CLASSES = [
    Sprint,
    Worklog,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
]


@pytest.mark.parametrize(
    "cls", ALL_SCHEMA_CLASSES + ALL_WORK_ITEM_CLASSES, ids=lambda c: c.__name__
)
def test_dataclass_has_org_id_field(cls):
    """Every metrics/work-item dataclass must have an org_id field."""
    field_names = [f.name for f in fields(cls)]
    assert "org_id" in field_names, f"{cls.__name__} missing org_id field"


@pytest.mark.parametrize(
    "cls", ALL_SCHEMA_CLASSES + ALL_WORK_ITEM_CLASSES, ids=lambda c: c.__name__
)
def test_dataclass_org_id_defaults_to_default(cls):
    """org_id must default to 'default' so existing code keeps working."""
    for f in fields(cls):
        if f.name == "org_id":
            assert f.default == "default", (
                f"{cls.__name__}.org_id default is {f.default!r}, expected 'default'"
            )
            break


# ---------------------------------------------------------------------------
# 2. org_id appears in asdict() output (critical for _insert_rows helper)
# ---------------------------------------------------------------------------


def test_repo_metrics_asdict_contains_org_id():
    row = RepoMetricsDailyRecord(
        repo_id=uuid.uuid4(),
        day=date(2025, 1, 1),
        commits_count=1,
        total_loc_touched=10,
        avg_commit_size_loc=10.0,
        large_commit_ratio=0.0,
        prs_merged=0,
        median_pr_cycle_hours=0.0,
        computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    d = asdict(row)
    assert "org_id" in d
    assert d["org_id"] == "default"


def test_repo_metrics_custom_org_id():
    row = RepoMetricsDailyRecord(
        repo_id=uuid.uuid4(),
        day=date(2025, 1, 1),
        commits_count=1,
        total_loc_touched=10,
        avg_commit_size_loc=10.0,
        large_commit_ratio=0.0,
        prs_merged=0,
        median_pr_cycle_hours=0.0,
        computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        org_id="acme-corp",
    )
    assert row.org_id == "acme-corp"
    assert asdict(row)["org_id"] == "acme-corp"


# ---------------------------------------------------------------------------
# 3. ClickHouse sink includes org_id in column lists
# ---------------------------------------------------------------------------


def test_clickhouse_sink_write_repo_metrics_includes_org_id():
    """write_repo_metrics must pass org_id column to _insert_rows."""
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    with patch.object(ClickHouseMetricsSink, "__init__", lambda self: None):
        sink = ClickHouseMetricsSink()
        sink.client = MagicMock()

        row = RepoMetricsDailyRecord(
            repo_id=uuid.uuid4(),
            day=date(2025, 1, 1),
            commits_count=1,
            total_loc_touched=10,
            avg_commit_size_loc=10.0,
            large_commit_ratio=0.0,
            prs_merged=0,
            median_pr_cycle_hours=0.0,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
            org_id="test-org",
        )
        sink.write_repo_metrics([row])

        assert sink.client.insert.called
        call_args = sink.client.insert.call_args
        column_names = call_args[1].get("column_names") or call_args[0][2]
        assert "org_id" in column_names, "org_id not in ClickHouse column list"

        matrix = call_args[0][1]
        org_id_idx = list(column_names).index("org_id")
        assert matrix[0][org_id_idx] == "test-org"


def test_clickhouse_sink_write_work_graph_edges_includes_org_id():
    """Pattern B: write_work_graph_edges must include org_id in both column_names and data."""
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    with patch.object(ClickHouseMetricsSink, "__init__", lambda self: None):
        sink = ClickHouseMetricsSink()
        sink.client = MagicMock()

        row = WorkGraphEdgeRecord(
            edge_id="e1",
            source_type="issue",
            source_id="i1",
            target_type="pr",
            target_id="p1",
            edge_type="linked",
            repo_id=uuid.uuid4(),
            provider="github",
            provenance="api",
            confidence=1.0,
            evidence="test",
            discovered_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            last_synced=datetime(2025, 1, 1, tzinfo=timezone.utc),
            event_ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
            day=date(2025, 1, 1),
            org_id="edge-org",
        )
        sink.write_work_graph_edges([row])

        call_args = sink.client.insert.call_args
        column_names = call_args[1].get("column_names") or call_args[0][2]
        assert "org_id" in column_names

        matrix = call_args[0][1]
        org_id_idx = list(column_names).index("org_id")
        assert matrix[0][org_id_idx] == "edge-org"


# ---------------------------------------------------------------------------
# 4. SQLite/SQLAlchemy sink DDL includes org_id and writes it
# ---------------------------------------------------------------------------


def test_sqlite_sink_org_id_column_exists(tmp_path):
    """ensure_tables must create org_id column in all tables."""
    from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink

    db_path = tmp_path / "test.db"
    sink = SQLiteMetricsSink(f"sqlite:///{db_path}")
    try:
        sink.ensure_tables()

        with sink.engine.begin() as conn:
            result = conn.execute(
                text("PRAGMA table_info(repo_metrics_daily)")
            ).fetchall()
            col_names = [row[1] for row in result]
            assert "org_id" in col_names, (
                f"org_id not in repo_metrics_daily columns: {col_names}"
            )
    finally:
        sink.close()


def test_sqlite_sink_writes_org_id(tmp_path):
    """write_repo_metrics must persist org_id value."""
    from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink

    db_path = tmp_path / "test.db"
    sink = SQLiteMetricsSink(f"sqlite:///{db_path}")
    try:
        sink.ensure_tables()
        repo_id = uuid.uuid4()
        row = RepoMetricsDailyRecord(
            repo_id=repo_id,
            day=date(2025, 1, 1),
            commits_count=1,
            total_loc_touched=10,
            avg_commit_size_loc=10.0,
            large_commit_ratio=0.0,
            prs_merged=0,
            median_pr_cycle_hours=0.0,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
            org_id="acme-corp",
        )
        sink.write_repo_metrics([row])

        with sink.engine.begin() as conn:
            result = conn.execute(
                text("SELECT org_id FROM repo_metrics_daily WHERE repo_id = :rid"),
                {"rid": str(repo_id)},
            ).fetchone()

        assert result is not None
        assert result[0] == "acme-corp"
    finally:
        sink.close()


def test_sqlite_sink_org_id_defaults(tmp_path):
    """When org_id is not explicitly set, it should default to 'default'."""
    from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink

    db_path = tmp_path / "test.db"
    sink = SQLiteMetricsSink(f"sqlite:///{db_path}")
    try:
        sink.ensure_tables()
        repo_id = uuid.uuid4()
        row = RepoMetricsDailyRecord(
            repo_id=repo_id,
            day=date(2025, 1, 1),
            commits_count=1,
            total_loc_touched=10,
            avg_commit_size_loc=10.0,
            large_commit_ratio=0.0,
            prs_merged=0,
            median_pr_cycle_hours=0.0,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_repo_metrics([row])

        with sink.engine.begin() as conn:
            result = conn.execute(
                text("SELECT org_id FROM repo_metrics_daily WHERE repo_id = :rid"),
                {"rid": str(repo_id)},
            ).fetchone()

        assert result is not None
        assert result[0] == "default"
    finally:
        sink.close()


# ---------------------------------------------------------------------------
# 5. SQL migration file covers all expected tables
# ---------------------------------------------------------------------------


def test_migration_024_covers_all_tables():
    """024_add_org_id.sql must ALTER TABLE for every analytics table."""
    import os

    migration_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "src",
        "dev_health_ops",
        "migrations",
        "clickhouse",
        "024_add_org_id.sql",
    )
    with open(migration_path) as f:
        content = f.read()

    expected_tables = [
        "repo_metrics_daily",
        "user_metrics_daily",
        "commit_metrics",
        "team_metrics_daily",
        "file_metrics_daily",
        "work_item_metrics_daily",
        "work_item_user_metrics_daily",
        "work_item_cycle_times",
        "work_item_state_durations_daily",
        "review_edges_daily",
        "cicd_metrics_daily",
        "deploy_metrics_daily",
        "incident_metrics_daily",
        "dora_metrics_daily",
        "ic_landscape_rolling_30d",
        "file_complexity_snapshots",
        "repo_complexity_daily",
        "file_hotspot_daily",
        "investment_classifications_daily",
        "investment_metrics_daily",
        "issue_type_metrics_daily",
        "work_unit_investments",
        "work_unit_investment_quotes",
        "investment_explanations",
        "work_graph_edges",
        "work_graph_issue_pr",
        "work_graph_pr_commit",
        "work_items",
        "work_item_transitions",
        "work_item_dependencies",
        "work_item_reopen_events",
        "work_item_interactions",
        "sprints",
        "worklogs",
        "capacity_forecasts",
        "teams",
        "repos",
    ]

    for table in expected_tables:
        assert f"ALTER TABLE {table}" in content, (
            f"Migration 024 missing ALTER TABLE for {table}"
        )
