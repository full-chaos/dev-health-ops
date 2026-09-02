"""Execute the production compute_test_metrics_daily on REAL rows.

Fixture is test_suite_results/test_case_results rows read from the shared
dev stack's ClickHouse, org 70d529e0-3c06-4597-8480-794fd02328b6, repo
920f9442-07df-4217-4dc4-c5833c0b8268, day 2026-08-23 -- pinned as a frozen
literal, same rationale as python_pipeline_metrics_oracle.py.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_testops import compute_test_metrics_daily

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
REPO_ID = uuid.UUID("920f9442-07df-4217-4dc4-c5833c0b8268")
DAY = date(2026, 8, 23)
COMPUTED_AT = datetime(2026, 8, 23, 23, 0, 0, tzinfo=timezone.utc)

SUITE_ROWS = [
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "suite_name": "pytest",
        "total_count": 7,
        "passed_count": 5,
        "failed_count": 0,
        "skipped_count": 2,
        "error_count": 0,
        "quarantined_count": 0,
        "duration_seconds": 11.953,
        "started_at": datetime(2026, 8, 23, 19, 55, 43, 892000, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 8, 23, 19, 55, 55, 845000, tzinfo=timezone.utc),
        "team_id": None,
        "service_id": None,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32671439506",
        "suite_id": "7209e391dcc3588bd0c8e3f7dc73c738ff4dfd979144aacded7086ee4fa0cfeb",
        "suite_name": "pytest",
        "total_count": 7,
        "passed_count": 5,
        "failed_count": 0,
        "skipped_count": 2,
        "error_count": 0,
        "quarantined_count": 0,
        "duration_seconds": 12.022,
        "started_at": datetime(2026, 8, 23, 22, 43, 55, 801000, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 8, 23, 22, 44, 7, 823000, tzinfo=timezone.utc),
        "team_id": None,
        "service_id": None,
        "org_id": ORG_ID,
    },
]

CASE_ROWS = [
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c1",
        "case_name": "test_list_authenticated_user_repos_includes_private",
        "status": "passed",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c2",
        "case_name": "test_list_public_repos_from_github_org",
        "status": "passed",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c3",
        "case_name": "test_search_public_repos",
        "status": "passed",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c4",
        "case_name": "test_list_public_repos_from_user",
        "status": "passed",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c5",
        "case_name": "test_github_invalid_token",
        "status": "passed",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c6",
        "case_name": "test_access_private_repo_without_token",
        "status": "skipped",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32662748666",
        "suite_id": "2e34e3fced433b4bbbe0311f167f8678eeeeba42ed97539e95f96c6e6d24abd9",
        "case_id": "c7",
        "case_name": "test_access_private_repo_with_valid_token",
        "status": "skipped",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "32671439506",
        "suite_id": "7209e391dcc3588bd0c8e3f7dc73c738ff4dfd979144aacded7086ee4fa0cfeb",
        "case_id": "c8",
        "case_name": "test_search_public_repos",
        "status": "passed",
        "retry_attempt": 0,
        "org_id": ORG_ID,
    },
]

records = compute_test_metrics_daily(
    day=DAY,
    suite_results=SUITE_ROWS,
    case_results=CASE_ROWS,
    computed_at=COMPUTED_AT,
    historical_failed_names_by_repo={},
)
rows = []
for record in records:
    row = asdict(record)
    row.pop("computed_at", None)
    row["repo_id"] = str(row["repo_id"])
    row["day"] = row["day"].isoformat()
    rows.append(row)
print(json.dumps(rows))
