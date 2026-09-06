"""Execute the production compute_pipeline_metrics_daily on a queue-seconds
fixture chosen to SEPARATE naive summation from CPython's own sum().

CHAOS-4284. compute_testops.py computes avg_queue_seconds as
`sum(queues) / len(queues)`, and since CPython 3.12 (gh-100425) the builtin
sum() applies Neumaier compensated summation to floats. Go's
internal/jobs/metrics/testops.mean() used to be a plain `total += value`
loop, which is NOT equivalent -- internal/pythonparity/sum.go measures the
two disagreeing on 16% of random 2-8 element inputs.

CHAOS-4294's own pipeline oracle (python_pipeline_metrics_oracle.py) could
not catch that: its fixture is a SINGLE run with queue_seconds 0.0, and a
one-element mean is identical under either algorithm. This oracle exists
specifically to close that gap, so the naive implementation cannot come back
silently.

The fixture is ten runs of queue_seconds 0.1, the canonical separating case:

    sum([0.1] * 10)  ->  1.0                  (CPython, Neumaier)
    naive loop       ->  0.9999999999999999

so avg_queue_seconds is 0.1 under Python and 0.09999999999999999 under a
naive Go loop -- a difference this test asserts on directly.

Durations are held CONSTANT across the ten runs on purpose: median/p95 sort
their input and are order- and algorithm-insensitive, so avg_queue_seconds
is the only field that can move, which is what makes this oracle a targeted
probe rather than a general regression net.
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

# 0.1 is not exactly representable in binary64, which is precisely why
# repeated addition of it accumulates a rounding error a compensated sum
# recovers and a naive sum does not.
QUEUE_SECONDS = 0.1
RUN_COUNT = 10

PIPELINE_RUNS: list[PipelineRunExtendedRow] = [
    {
        "repo_id": REPO_ID,
        "run_id": f"neumaier-{index}",
        "provider": "github_actions",
        "status": "success",
        "queued_at": datetime(2026, 8, 27, 19, 39, 4, tzinfo=timezone.utc),
        "started_at": datetime(2026, 8, 27, 19, 39, 4, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 8, 27, 19, 54, 46, tzinfo=timezone.utc),
        "duration_seconds": 942.0,
        "queue_seconds": QUEUE_SECONDS,
        "retry_count": 0,
        "team_id": None,
        "service_id": None,
        "org_id": ORG_ID,
    }
    for index in range(RUN_COUNT)
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
