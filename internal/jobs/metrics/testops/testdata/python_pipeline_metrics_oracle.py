"""Execute the production compute_pipeline_metrics_daily on a REAL row.

Fixture is one ci_pipeline_runs row read from the shared dev stack's
ClickHouse, org 70d529e0-3c06-4597-8480-794fd02328b6 (the local real-data
proof org), repo d4f322ad-2102-1fbf-8425-7400573194f7, day 2026-08-27 --
pinned here as a frozen literal (not re-queried at test time) so this test
is reproducible without live ClickHouse access. Mirrors the Go oracle test
in compute_test.go: pin real production-shaped data once, then keep both
sides in sync by hand.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_testops import compute_pipeline_metrics_daily
from dev_health_ops.metrics.testops_schemas import PipelineRunExtendedRow

ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
REPO_ID = uuid.UUID("d4f322ad-2102-1fbf-8425-7400573194f7")
DAY = date(2026, 8, 27)
COMPUTED_AT = datetime(2026, 8, 27, 20, 0, 0, tzinfo=timezone.utc)

PIPELINE_RUNS: list[PipelineRunExtendedRow] = [
    {
        "repo_id": REPO_ID,
        "run_id": "33109458314",
        # provider is required by PipelineRunExtendedRow's type but not read
        # by compute_pipeline_metrics_daily (compute_testops.py never
        # touches it) -- not part of this ClickHouse row pull, a plausible
        # value satisfies the type without affecting the oracle's output.
        "provider": "github_actions",
        "status": "success",
        "queued_at": datetime(2026, 8, 27, 19, 39, 4, tzinfo=timezone.utc),
        "started_at": datetime(2026, 8, 27, 19, 39, 4, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 8, 27, 19, 54, 46, tzinfo=timezone.utc),
        "duration_seconds": 942.0,
        "queue_seconds": 0.0,
        "retry_count": 0,
        "team_id": None,
        "service_id": None,
        "org_id": ORG_ID,
    },
]

records = compute_pipeline_metrics_daily(
    day=DAY, pipeline_runs=PIPELINE_RUNS, job_runs=[], computed_at=COMPUTED_AT
)
rows = []
for record in records:
    row = asdict(record)
    row.pop("computed_at", None)
    row["repo_id"] = str(row["repo_id"])
    row["day"] = row["day"].isoformat()
    rows.append(row)
print(json.dumps(rows))
