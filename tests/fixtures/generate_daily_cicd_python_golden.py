"""Golden generator for the cicd family (CHAOS-4292).

Frozen output of the REAL Python compute path -- compute_cicd_metrics_daily
(src/dev_health_ops/metrics/compute_cicd.py) -- against a small synthetic
PipelineRunRow dataset covering:

  - repo A: two in-window runs, one success one failure (status casing/
    whitespace exercised too), both with queued_at/finished_at set --
    exercises pipelines_count, success_rate, avg/p90 duration, avg queue.
  - repo A: a run whose started_at falls BEFORE the target day's window --
    the loader would never fetch this in production (it filters by
    finished_at), but this generator feeds it directly to
    compute_cicd_metrics_daily to prove the function's OWN started_at
    re-filter drops it independently of the loader (the DOUBLE WINDOW
    FILTER documented on internal/jobs/metrics/daily/cicd's package doc
    comment) -- must not appear in repo A's pipelines_count.
  - repo A: a run with finished_at set but queued_at None -- duration_minutes
    is appended, avg_queue_minutes stays None (no queue sample at all).
  - repo A: a clock-skew run whose finished_at is BEFORE started_at
    (negative duration) -- must be counted in pipelines_count (and
    success_rate if its status matches) but excluded from avg/p90 duration.
  - repo B: a single run with an unrecognized status ("running") -- not
    counted as success; finished_at/queued_at both None -- no duration/queue
    samples, so avg_duration_minutes/p90_duration_minutes/avg_queue_minutes
    are all None for this repo.
  - repo C: zero runs at all in the window -- must produce NO record (not a
    pipelines_count=0 row) -- see the package doc comment.

Regenerate with `python tests/fixtures/generate_daily_cicd_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.compute_cicd import compute_cicd_metrics_daily
from dev_health_ops.metrics.schemas import PipelineRunRow

OUTPUT = Path(__file__).with_name("daily_cicd_python_golden.json")

REPO_A = UUID("00000000-0000-4000-8000-00000000000a")
REPO_B = UUID("00000000-0000-4000-8000-00000000000b")

DAY = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


def _dt(hour: int, minute: int = 0, day: int = 24) -> datetime:
    return datetime(2026, 8, day, hour, minute, 0, tzinfo=timezone.utc)


PIPELINE_RUNS: list[PipelineRunRow] = [
    # repo A: success, in window, full timestamps.
    {
        "repo_id": REPO_A,
        "run_id": "a-success",
        "status": "  Success  ",  # whitespace + mixed case, must normalize
        "queued_at": _dt(9, 55),
        "started_at": _dt(10, 0),
        "finished_at": _dt(10, 10),
    },
    # repo A: failed, in window, full timestamps.
    {
        "repo_id": REPO_A,
        "run_id": "a-failed",
        "status": "failed",
        "queued_at": _dt(11, 50),
        "started_at": _dt(12, 0),
        "finished_at": _dt(12, 30),
    },
    # repo A: started_at BEFORE the window -- must be dropped by
    # compute_cicd_metrics_daily's own re-filter.
    {
        "repo_id": REPO_A,
        "run_id": "a-out-of-window",
        "status": "success",
        "queued_at": _dt(23, 0, day=23),
        "started_at": _dt(23, 30, day=23),
        "finished_at": _dt(0, 30),
    },
    # repo A: finished_at set, queued_at None -- duration counted, no queue sample.
    {
        "repo_id": REPO_A,
        "run_id": "a-no-queue",
        "status": "succeeded",
        "queued_at": None,
        "started_at": _dt(13, 0),
        "finished_at": _dt(13, 5),
    },
    # repo A: clock skew -- finished_at BEFORE started_at (negative duration),
    # counted in pipelines_count/success_rate, excluded from durations.
    {
        "repo_id": REPO_A,
        "run_id": "a-clock-skew",
        "status": "passed",
        "queued_at": _dt(13, 55),
        "started_at": _dt(14, 0),
        "finished_at": _dt(13, 59),
    },
    # repo B: unrecognized status, no finished_at/queued_at at all.
    {
        "repo_id": REPO_B,
        "run_id": "b-running",
        "status": "running",
        "queued_at": None,
        "started_at": _dt(15, 0),
        "finished_at": None,
    },
]


def _serialize(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def main() -> str:
    records = compute_cicd_metrics_daily(
        day=DAY, pipeline_runs=PIPELINE_RUNS, computed_at=COMPUTED_AT
    )
    document = {
        "records": [
            {field: _serialize(value) for field, value in record.__dict__.items()}
            for record in records
        ]
    }
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    import sys

    rendered = main()
    if "--stdout" in sys.argv:
        sys.stdout.write(rendered)
    else:
        OUTPUT.write_text(rendered)
        print(f"wrote {OUTPUT}")
