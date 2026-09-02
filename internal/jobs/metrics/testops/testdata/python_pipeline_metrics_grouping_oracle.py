"""Execute compute_pipeline_metrics_daily on codex adversarial review round
1's (CHAOS-4294) own P1 repro: two pipeline runs for the SAME repo/team,
one with service_id=None and one with service_id="" -- these must remain
TWO separate groups (Python's dict key is the raw (repo_id, team_id,
service_id) tuple; None != ""), not merge into one.

Also exercises P2 from the same round: a row with team_id=None on a repo
whose name matches a repo_team_resolver pattern must resolve to that
pattern's team, not stay None.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone

from dev_health_ops.metrics.compute_testops import compute_pipeline_metrics_daily
from dev_health_ops.providers.teams import build_repo_pattern_resolver

ORG_ID = "00000000-0000-4000-8000-000000000009"
REPO_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
DAY = date(2026, 8, 15)
COMPUTED_AT = datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)
REPO_NAME = "acme/service"


def dt(hour: int) -> datetime:
    return datetime(2026, 8, 15, hour, 0, 0, tzinfo=timezone.utc)


PIPELINE_RUNS = [
    {
        "repo_id": REPO_ID,
        "run_id": "run-null-service",
        "status": "success",
        "queued_at": None,
        "started_at": dt(9),
        "finished_at": dt(9),
        "duration_seconds": 60.0,
        "queue_seconds": None,
        "retry_count": 0,
        "team_id": None,
        "service_id": None,
        "org_id": ORG_ID,
    },
    {
        "repo_id": REPO_ID,
        "run_id": "run-empty-service",
        "status": "failure",
        "queued_at": None,
        "started_at": dt(10),
        "finished_at": dt(10),
        "duration_seconds": 90.0,
        "queue_seconds": None,
        "retry_count": 0,
        "team_id": None,
        "service_id": "",
        "org_id": ORG_ID,
    },
]

repo_team_resolver = build_repo_pattern_resolver(
    [{"id": "team-pattern", "name": "Team Pattern", "repo_patterns": ["acme/*"]}]
)
repo_names_by_id = {REPO_ID: REPO_NAME}

records = compute_pipeline_metrics_daily(
    day=DAY,
    pipeline_runs=PIPELINE_RUNS,
    job_runs=[],
    computed_at=COMPUTED_AT,
    repo_team_resolver=repo_team_resolver,
    repo_names_by_id=repo_names_by_id,
)
rows = []
for record in records:
    row = asdict(record)
    row.pop("computed_at", None)
    row["repo_id"] = str(row["repo_id"])
    row["day"] = row["day"].isoformat()
    rows.append(row)
print(json.dumps(rows))
