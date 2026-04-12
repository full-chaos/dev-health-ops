from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from dev_health_ops.metrics.compute_testops import (
    compute_coverage_metrics_daily,
    compute_pipeline_metrics_daily,
    compute_test_metrics_daily,
)


def test_compute_pipeline_metrics_daily_groups_by_repo_and_team_service():
    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_b = uuid4()

    records = compute_pipeline_metrics_daily(
        day=day,
        pipeline_runs=[
            {
                "repo_id": repo_a,
                "run_id": "001",
                "provider": "github_actions",
                "status": "success",
                "queued_at": datetime(2026, 2, 18, 9, 58, tzinfo=timezone.utc),
                "started_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 2, 18, 10, 5, tzinfo=timezone.utc),
                "retry_count": 0,
                "team_id": "team-a",
                "service_id": "svc-a",
            },
            {
                "repo_id": repo_a,
                "run_id": "002",
                "provider": "github_actions",
                "status": "failed",
                "queued_at": datetime(2026, 2, 18, 10, 55, tzinfo=timezone.utc),
                "started_at": datetime(2026, 2, 18, 11, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 2, 18, 11, 20, tzinfo=timezone.utc),
                "retry_count": 1,
                "team_id": "team-a",
                "service_id": "svc-a",
            },
            {
                "repo_id": repo_b,
                "run_id": "010",
                "provider": "github_actions",
                "status": "cancelled",
                "queued_at": datetime(2026, 2, 18, 7, 59, tzinfo=timezone.utc),
                "started_at": datetime(2026, 2, 18, 8, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 2, 18, 8, 0, tzinfo=timezone.utc),
                "retry_count": 0,
            },
            {
                "repo_id": repo_a,
                "run_id": "old",
                "provider": "github_actions",
                "status": "success",
                "queued_at": None,
                "started_at": datetime(2026, 2, 17, 23, 59, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 2, 18, 0, 5, tzinfo=timezone.utc),
            },
        ],
        job_runs=[],
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    rec_a = next(r for r in records if r.repo_id == repo_a)
    assert rec_a.team_id == "team-a"
    assert rec_a.service_id == "svc-a"
    assert rec_a.pipelines_count == 2
    assert rec_a.success_count == 1
    assert rec_a.failure_count == 1
    assert rec_a.cancelled_count == 0
    assert rec_a.success_rate == pytest.approx(0.5)
    assert rec_a.failure_rate == pytest.approx(0.5)
    assert rec_a.cancel_rate == pytest.approx(0.0)
    assert rec_a.rerun_rate == pytest.approx(0.5)
    assert rec_a.median_duration_seconds == pytest.approx(750.0)
    assert rec_a.p95_duration_seconds == pytest.approx(1155.0)
    assert rec_a.avg_queue_seconds == pytest.approx(210.0)
    assert rec_a.p95_queue_seconds == pytest.approx(291.0)

    rec_b = next(r for r in records if r.repo_id == repo_b)
    assert rec_b.pipelines_count == 1
    assert rec_b.cancelled_count == 1
    assert rec_b.median_duration_seconds == pytest.approx(0.0)
    assert rec_b.avg_queue_seconds == pytest.approx(60.0)


def test_compute_pipeline_metrics_daily_returns_empty_for_no_matching_rows():
    records = compute_pipeline_metrics_daily(
        day=date(2026, 2, 18),
        pipeline_runs=[],
        job_runs=[],
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    assert records == []


def test_compute_test_metrics_daily_detects_flakes_and_failure_recurrence():
    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_b = uuid4()

    suite_results = [
        {
            "repo_id": repo_a,
            "run_id": "run-a-current",
            "suite_id": "suite-a-1",
            "suite_name": "suite-a-1",
            "total_count": 4,
            "passed_count": 2,
            "failed_count": 1,
            "skipped_count": 1,
            "error_count": 0,
            "quarantined_count": 1,
            "retried_count": 1,
            "duration_seconds": 30.0,
            "started_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 10, 1, tzinfo=timezone.utc),
            "team_id": "team-a",
            "service_id": "svc-a",
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-current-2",
            "suite_id": "suite-a-2",
            "suite_name": "suite-a-2",
            "total_count": 2,
            "passed_count": 1,
            "failed_count": 1,
            "skipped_count": 0,
            "error_count": 1,
            "quarantined_count": 0,
            "retried_count": 1,
            "duration_seconds": 50.0,
            "started_at": datetime(2026, 2, 18, 11, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 11, 2, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-history",
            "suite_id": "suite-a-old",
            "suite_name": "suite-a-old",
            "total_count": 1,
            "passed_count": 0,
            "failed_count": 1,
            "skipped_count": 0,
            "duration_seconds": 20.0,
            "started_at": datetime(2026, 2, 17, 11, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 17, 11, 1, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_b,
            "run_id": "run-b-current",
            "suite_id": "suite-b-1",
            "suite_name": "suite-b-1",
            "total_count": 1,
            "passed_count": 1,
            "failed_count": 0,
            "skipped_count": 0,
            "duration_seconds": 0.0,
            "started_at": datetime(2026, 2, 18, 9, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 9, 0, tzinfo=timezone.utc),
        },
    ]

    case_results = [
        {
            "repo_id": repo_a,
            "run_id": "run-a-current",
            "suite_id": "suite-a-1",
            "case_id": "a1-1",
            "case_name": "test_flaky",
            "status": "failed",
            "duration_seconds": 5.0,
            "retry_attempt": 0,
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-current",
            "suite_id": "suite-a-1",
            "case_id": "a1-1-retry",
            "case_name": "test_flaky",
            "status": "passed",
            "duration_seconds": 4.0,
            "retry_attempt": 1,
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-current",
            "suite_id": "suite-a-1",
            "case_id": "a1-2",
            "case_name": "test_recurring_failure",
            "status": "failed",
            "duration_seconds": 2.0,
            "retry_attempt": 0,
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-current",
            "suite_id": "suite-a-1",
            "case_id": "a1-3",
            "case_name": "test_skipped",
            "status": "skipped",
            "duration_seconds": 0.0,
            "retry_attempt": 0,
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-current-2",
            "suite_id": "suite-a-2",
            "case_id": "a2-1",
            "case_name": "test_clean_pass",
            "status": "passed",
            "duration_seconds": 1.0,
            "retry_attempt": 0,
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-current-2",
            "suite_id": "suite-a-2",
            "case_id": "a2-2",
            "case_name": "test_quarantined",
            "status": "quarantined",
            "duration_seconds": 1.0,
            "retry_attempt": 0,
        },
        {
            "repo_id": repo_a,
            "run_id": "run-a-history",
            "suite_id": "suite-a-old",
            "case_id": "a-old-1",
            "case_name": "test_recurring_failure",
            "status": "failed",
            "duration_seconds": 1.0,
            "retry_attempt": 0,
        },
        {
            "repo_id": repo_b,
            "run_id": "run-b-current",
            "suite_id": "suite-b-1",
            "case_id": "b1-1",
            "case_name": "test_repo_b",
            "status": "passed",
            "duration_seconds": 0.0,
            "retry_attempt": 0,
        },
    ]

    records = compute_test_metrics_daily(
        day=day,
        suite_results=suite_results,
        case_results=case_results,
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    rec_a = next(r for r in records if r.repo_id == repo_a)
    assert rec_a.total_cases == 6
    assert rec_a.passed_count == 3
    assert rec_a.failed_count == 3
    assert rec_a.skipped_count == 1
    assert rec_a.quarantined_count == 1
    assert rec_a.pass_rate == pytest.approx(0.5)
    assert rec_a.failure_rate == pytest.approx(0.5)
    assert rec_a.flake_rate == pytest.approx(0.2)
    assert rec_a.retry_dependency_rate == pytest.approx(0.2)
    assert rec_a.total_suites == 2
    assert rec_a.suite_duration_p50_seconds == pytest.approx(40.0)
    assert rec_a.suite_duration_p95_seconds == pytest.approx(49.0)
    assert rec_a.failure_recurrence_score == pytest.approx(0.5)
    assert rec_a.team_id == "team-a"
    assert rec_a.service_id == "svc-a"

    rec_b = next(r for r in records if r.repo_id == repo_b)
    assert rec_b.total_cases == 1
    assert rec_b.pass_rate == pytest.approx(1.0)
    assert rec_b.flake_rate == pytest.approx(0.0)
    assert rec_b.retry_dependency_rate == pytest.approx(0.0)
    assert rec_b.suite_duration_p50_seconds == pytest.approx(0.0)


def test_compute_test_metrics_daily_handles_empty_inputs():
    records = compute_test_metrics_daily(
        day=date(2026, 2, 18),
        suite_results=[],
        case_results=[],
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    assert records == []


def test_compute_coverage_metrics_daily_uses_latest_snapshot_and_delta():
    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_b = uuid4()

    records = compute_coverage_metrics_daily(
        day=day,
        snapshots=[
            {
                "repo_id": repo_a,
                "run_id": "001",
                "snapshot_id": "snap-1",
                "lines_total": 100,
                "lines_covered": 80,
                "line_coverage_pct": 80.0,
                "branch_coverage_pct": 70.0,
                "team_id": "team-a",
            },
            {
                "repo_id": repo_a,
                "run_id": "002",
                "snapshot_id": "snap-2",
                "lines_total": 100,
                "lines_covered": 85,
                "line_coverage_pct": 85.0,
                "branch_coverage_pct": 75.0,
                "team_id": "team-a",
            },
            {
                "repo_id": repo_b,
                "run_id": "010",
                "snapshot_id": "snap-10",
                "lines_total": 50,
                "lines_covered": 45,
                "line_coverage_pct": 90.0,
                "branch_coverage_pct": None,
            },
        ],
        prior_snapshots=[
            {
                "repo_id": repo_a,
                "run_id": "000",
                "snapshot_id": "snap-old",
                "lines_total": 100,
                "lines_covered": 82,
                "line_coverage_pct": 82.0,
                "branch_coverage_pct": 72.0,
            }
        ],
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    rec_a = next(r for r in records if r.repo_id == repo_a)
    assert rec_a.line_coverage_pct == pytest.approx(85.0)
    assert rec_a.branch_coverage_pct == pytest.approx(75.0)
    assert rec_a.lines_total == 100
    assert rec_a.lines_covered == 85
    assert rec_a.coverage_delta_pct == pytest.approx(3.0)
    assert rec_a.uncovered_files_count == 0
    assert rec_a.coverage_regression_count == 0

    rec_b = next(r for r in records if r.repo_id == repo_b)
    assert rec_b.coverage_delta_pct is None
    assert rec_b.branch_coverage_pct is None


def test_compute_testops_metrics_resolve_team_via_repo_resolver_when_row_team_id_missing():
    """CHAOS-1191: resolver fallback populates team_id when raw rows lack it."""
    from dev_health_ops.providers.teams import build_repo_pattern_resolver

    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_team_resolver = build_repo_pattern_resolver(
        [{"id": "team-x", "name": "Team X", "repo_patterns": ["acme/api"]}]
    )
    repo_names_by_id = {repo_a: "acme/api"}
    computed_at = datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc)

    pipeline_records = compute_pipeline_metrics_daily(
        day=day,
        pipeline_runs=[
            {
                "repo_id": repo_a,
                "run_id": "001",
                "provider": "github_actions",
                "status": "success",
                "queued_at": datetime(2026, 2, 18, 9, 58, tzinfo=timezone.utc),
                "started_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 2, 18, 10, 5, tzinfo=timezone.utc),
                "retry_count": 0,
            }
        ],
        job_runs=[],
        computed_at=computed_at,
        repo_team_resolver=repo_team_resolver,
        repo_names_by_id=repo_names_by_id,
    )
    assert pipeline_records[0].team_id == "team-x"

    test_records = compute_test_metrics_daily(
        day=day,
        suite_results=[
            {
                "repo_id": repo_a,
                "run_id": "r1",
                "started_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 2, 18, 10, 1, tzinfo=timezone.utc),
                "total_count": 1,
                "passed_count": 1,
                "failed_count": 0,
                "error_count": 0,
                "skipped_count": 0,
                "quarantined_count": 0,
                "duration_seconds": 60,
            }
        ],
        case_results=[],
        computed_at=computed_at,
        repo_team_resolver=repo_team_resolver,
        repo_names_by_id=repo_names_by_id,
    )
    assert test_records[0].team_id == "team-x"

    coverage_records = compute_coverage_metrics_daily(
        day=day,
        snapshots=[
            {
                "repo_id": repo_a,
                "run_id": "r1",
                "snapshot_id": "s1",
                "line_coverage_pct": 80.0,
                "branch_coverage_pct": 70.0,
                "lines_total": 100,
                "lines_covered": 80,
            }
        ],
        prior_snapshots=None,
        computed_at=computed_at,
        repo_team_resolver=repo_team_resolver,
        repo_names_by_id=repo_names_by_id,
    )
    assert coverage_records[0].team_id == "team-x"


def test_compute_coverage_metrics_daily_returns_empty_for_no_snapshots():
    records = compute_coverage_metrics_daily(
        day=date(2026, 2, 18),
        snapshots=[],
        prior_snapshots=None,
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    assert records == []
