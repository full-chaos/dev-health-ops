"""Execute the production Python testops_risk compute path on a fixed case.

Mirrors job_daily.py's own call sequence (compute_pipeline_metrics_daily ->
compute_test_metrics_daily -> compute_coverage_metrics_daily ->
compute_release_confidence/compute_quality_drag/compute_pipeline_stability),
scoped to ONE repo and ONE day -- the exact scope TestopsRiskExecutor's
per-repo loop gives its own Go port (CHAOS-4294). Prints canonical JSON so
the Go oracle test (testops_risk_native_test.go) can compare row-for-row,
including factors_json, without re-deriving expected values by hand.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_testops import (
    compute_coverage_metrics_daily,
    compute_pipeline_metrics_daily,
    compute_test_metrics_daily,
)
from dev_health_ops.metrics.compute_testops_risk import (
    compute_pipeline_stability,
    compute_quality_drag,
    compute_release_confidence,
)

ORG_ID = "00000000-0000-4000-8000-000000000009"
REPO_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
DAY = date(2026, 8, 15)
COMPUTED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)


def dt(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 15, hour, minute, second, tzinfo=timezone.utc)


# Two (team_id, service_id) groups for the SAME repo/day -- exercises both
# the "last group wins" collapse compute_release_confidence/compute_quality_drag
# apply via their repo_id-only dict keys, AND pipeline_stability's per-repo
# grouping, which (unlike those two) keeps BOTH groups as separate
# "days_data" entries for the SAME calendar day (compute_testops_risk.py:191-193
# groups purely by repo_id, never by team/service).
PIPELINE_RUNS = [
    {
        "repo_id": REPO_ID,
        "run_id": "run-1",
        "status": "success",
        "queued_at": dt(9, 0),
        "started_at": dt(9, 1),
        "finished_at": dt(9, 11),
        "duration_seconds": None,
        "queue_seconds": None,
        "retry_count": 0,
        "team_id": "team-a",
        "service_id": "svc-a",
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-2",
        "status": "failed",
        "queued_at": dt(10, 0),
        "started_at": dt(10, 2),
        "finished_at": dt(10, 20),
        "duration_seconds": None,
        "queue_seconds": None,
        "retry_count": 1,
        "team_id": "team-a",
        "service_id": "svc-a",
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-3",
        "status": "success",
        "queued_at": dt(11, 0),
        "started_at": dt(11, 3),
        "finished_at": dt(11, 9),
        "duration_seconds": None,
        "queue_seconds": None,
        "retry_count": 0,
        "team_id": "team-b",
        "service_id": "svc-b",
        "org_id": ORG_ID,
    },
]

SUITE_ROWS = [
    {
        "repo_id": REPO_ID,
        "run_id": "run-1",
        "suite_id": "suite-1",
        "suite_name": "unit",
        "total_count": 10,
        "passed_count": 8,
        "failed_count": 2,
        "skipped_count": 0,
        "error_count": 0,
        "quarantined_count": 1,
        "retried_count": 1,
        "duration_seconds": 42.0,
        "started_at": dt(9, 1),
        "finished_at": dt(9, 5),
        "team_id": "team-a",
        "service_id": "svc-a",
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-2",
        "suite_id": "suite-2",
        "suite_name": "integration",
        "total_count": 5,
        "passed_count": 3,
        "failed_count": 2,
        "skipped_count": 0,
        "error_count": 0,
        "quarantined_count": 0,
        "retried_count": 0,
        "duration_seconds": 88.0,
        "started_at": dt(10, 2),
        "finished_at": dt(10, 10),
        "team_id": "team-a",
        "service_id": "svc-a",
        "org_id": ORG_ID,
    },
]

CASE_ROWS = [
    {
        "repo_id": REPO_ID,
        "run_id": "run-1",
        "suite_id": "suite-1",
        "case_id": "c1",
        "case_name": "test_flaky",
        "status": "passed",
        "duration_seconds": 1.0,
        "retry_attempt": 1,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-1",
        "suite_id": "suite-1",
        "case_id": "c1",
        "case_name": "test_flaky",
        "status": "failed",
        "duration_seconds": 1.0,
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-2",
        "suite_id": "suite-2",
        "case_id": "c2",
        "case_name": "test_recurrent_failure",
        "status": "failed",
        "duration_seconds": 2.0,
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-2",
        "suite_id": "suite-2",
        "case_id": "c3",
        "case_name": "test_stable",
        "status": "passed",
        "duration_seconds": 0.5,
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
]

# Fed directly as compute_test_metrics_daily's own input parameter -- the SQL
# aggregate that derives this set (load_testops_historical_failed_case_names)
# is a loader concern with its own ClickHouse-backed integration test, not
# something a pure-function oracle can exercise without a live database.
HISTORICAL_FAILED_NAMES_BY_REPO = {REPO_ID: {"test_recurrent_failure"}}

COVERAGE_SNAPSHOTS = [
    {
        "repo_id": REPO_ID,
        "run_id": "run-1",
        "snapshot_id": "snap-1",
        "lines_total": 1000,
        "lines_covered": 800,
        "line_coverage_pct": 80.0,
        "branch_coverage_pct": 70.0,
        "team_id": "team-a",
        "service_id": "svc-a",
        "org_id": ORG_ID,
    },
]
PRIOR_COVERAGE_SNAPSHOTS = [
    {
        "repo_id": REPO_ID,
        "run_id": "run-0",
        "snapshot_id": "snap-0",
        "lines_total": 1000,
        "lines_covered": 850,
        "line_coverage_pct": 85.0,
        "branch_coverage_pct": 72.0,
        "team_id": "team-a",
        "service_id": "svc-a",
        "org_id": ORG_ID,
    },
]


def canonical(records: list) -> list[dict]:
    result = []
    for record in records:
        row = asdict(record)
        row.pop("computed_at", None)
        row["repo_id"] = str(row["repo_id"])
        row["day"] = row["day"].isoformat()
        result.append(row)
    return result


pipeline_metrics = compute_pipeline_metrics_daily(
    day=DAY, pipeline_runs=PIPELINE_RUNS, job_runs=[], computed_at=COMPUTED_AT
)
test_metrics = compute_test_metrics_daily(
    day=DAY,
    suite_results=SUITE_ROWS,
    case_results=CASE_ROWS,
    computed_at=COMPUTED_AT,
    historical_failed_names_by_repo=HISTORICAL_FAILED_NAMES_BY_REPO,
)
coverage_metrics = compute_coverage_metrics_daily(
    day=DAY,
    snapshots=COVERAGE_SNAPSHOTS,
    prior_snapshots=PRIOR_COVERAGE_SNAPSHOTS,
    computed_at=COMPUTED_AT,
)

release_confidence = compute_release_confidence(
    day=DAY,
    pipeline_metrics=pipeline_metrics,
    test_metrics=test_metrics,
    coverage_metrics=coverage_metrics,
    computed_at=COMPUTED_AT,
)
quality_drag = compute_quality_drag(
    day=DAY,
    pipeline_metrics=pipeline_metrics,
    test_metrics=test_metrics,
    computed_at=COMPUTED_AT,
)
pipeline_stability = compute_pipeline_stability(
    day=DAY, pipeline_metrics_7d=pipeline_metrics, computed_at=COMPUTED_AT
)

print(
    json.dumps(
        {
            "release_confidence": canonical(release_confidence),
            "quality_drag": canonical(quality_drag),
            "pipeline_stability": canonical(pipeline_stability),
        }
    )
)
