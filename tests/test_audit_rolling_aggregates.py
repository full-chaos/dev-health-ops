from datetime import date, datetime, timezone
import uuid

from dev_health_ops.audit.rolling_aggregates import (
    ROLLING_TABLE_SPECS,
    run_rolling_aggregates_audit,
)
from dev_health_ops.metrics.schemas import (
    RepoMetricsDailyRecord,
    UserMetricsDailyRecord,
    WorkItemMetricsDailyRecord,
)
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink


def test_rolling_aggregates_no_data(tmp_path):
    db_path = tmp_path / "rolling_empty.db"
    db_url = f"sqlite:///{db_path}"
    sink = SQLiteMetricsSink(db_url)
    sink.ensure_tables()
    sink.close()

    report = run_rolling_aggregates_audit(db_url=db_url, as_of=date.today())
    for spec in ROLLING_TABLE_SPECS:
        table = spec["table"]
        assert report["tables"][table]["status"] == "no_data"
    assert report["overall_ok"] is True


def test_rolling_aggregates_ok(tmp_path):
    db_path = tmp_path / "rolling_ok.db"
    db_url = f"sqlite:///{db_path}"
    sink = SQLiteMetricsSink(db_url)
    sink.ensure_tables()

    repo_id = uuid.uuid4()
    day = date(2025, 1, 15)
    computed_at = datetime.now(timezone.utc)

    sink.write_repo_metrics(
        [
            RepoMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                commits_count=2,
                total_loc_touched=50,
                avg_commit_size_loc=25.0,
                large_commit_ratio=0.1,
                prs_merged=1,
                median_pr_cycle_hours=12.0,
                computed_at=computed_at,
            )
        ]
    )
    sink.write_user_metrics(
        [
            UserMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                author_email="alice@example.com",
                commits_count=1,
                loc_added=10,
                loc_deleted=2,
                files_changed=1,
                large_commits_count=0,
                avg_commit_size_loc=12.0,
                prs_authored=1,
                prs_merged=1,
                avg_pr_cycle_hours=5.0,
                median_pr_cycle_hours=5.0,
                computed_at=computed_at,
                identity_id="alice@example.com",
                loc_touched=12,
                delivery_units=1,
                cycle_p50_hours=5.0,
            )
        ]
    )
    sink.write_work_item_metrics(
        [
            WorkItemMetricsDailyRecord(
                day=day,
                provider="synthetic",
                work_scope_id="acme/demo-app",
                team_id="alpha",
                team_name="Alpha Team",
                items_started=1,
                items_completed=1,
                items_started_unassigned=0,
                items_completed_unassigned=0,
                wip_count_end_of_day=1,
                wip_unassigned_end_of_day=0,
                cycle_time_p50_hours=24.0,
                cycle_time_p90_hours=48.0,
                lead_time_p50_hours=24.0,
                lead_time_p90_hours=48.0,
                wip_age_p50_hours=12.0,
                wip_age_p90_hours=24.0,
                bug_completed_ratio=0.1,
                story_points_completed=3.0,
                computed_at=computed_at,
            )
        ]
    )
    sink.close()

    report = run_rolling_aggregates_audit(db_url=db_url, as_of=day)
    for spec in ROLLING_TABLE_SPECS:
        table = spec["table"]
        assert report["tables"][table]["status"] == "ok"
    assert report["overall_ok"] is True
