"""Execute the production compute_coverage_metrics_daily on REAL rows.

Fixture is coverage_snapshots rows read from the shared dev stack's
ClickHouse, org 70d529e0-3c06-4597-8480-794fd02328b6, repo
d29d160a-95fe-5b45-d4c1-fd1f5427b772, current day 2026-08-25 with prior day
2026-08-24 -- pinned as a frozen literal, same rationale as the pipeline
oracle.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_testops import compute_coverage_metrics_daily
from dev_health_ops.metrics.testops_schemas import CoverageSnapshotRow

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
REPO_ID = uuid.UUID("d29d160a-95fe-5b45-d4c1-fd1f5427b772")
DAY = date(2026, 8, 25)
COMPUTED_AT = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)

CURRENT_SNAPSHOTS: list[CoverageSnapshotRow] = [
    {
        "repo_id": REPO_ID,
        "run_id": "32793481613",
        "snapshot_id": "79959989f28edea99e50b1bdf6168d3ec22233e471d710a7f6dc32552f81986e",
        "lines_total": 14236,
        "lines_covered": 8401,
        "line_coverage_pct": 59.01236302332116,
        "branch_coverage_pct": 53.07480008491968,
        "team_id": None,
        "service_id": "src",
        "org_id": ORG_ID,
    },
]
PRIOR_SNAPSHOTS: list[CoverageSnapshotRow] = [
    {
        "repo_id": REPO_ID,
        "run_id": "32768344924",
        "snapshot_id": "1063c0023418eb49fd545fe9e2228aea380ba0a07c4c2dac312d3a95500f075c",
        "lines_total": 14235,
        "lines_covered": 8400,
        "line_coverage_pct": 59.00948366701792,
        "branch_coverage_pct": 53.06815768985774,
        "team_id": None,
        "service_id": "src",
        "org_id": ORG_ID,
    },
]

records = compute_coverage_metrics_daily(
    day=DAY,
    snapshots=CURRENT_SNAPSHOTS,
    prior_snapshots=PRIOR_SNAPSHOTS,
    computed_at=COMPUTED_AT,
)
rows = []
for record in records:
    row = asdict(record)
    row.pop("computed_at", None)
    row["repo_id"] = str(row["repo_id"])
    row["day"] = row["day"].isoformat()
    rows.append(row)
print(json.dumps(rows))
