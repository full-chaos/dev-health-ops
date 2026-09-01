"""Generate/verify the frozen `deploy` metrics.daily family Python golden
(CHAOS-4293).

Mirrors tests/fixtures/generate_daily_wellbeing_python_golden.py's shape for
one family: compute_deploy_metrics_daily
(src/dev_health_ops/metrics/compute_deployments.py:53) is the production
Python this repo is porting to Go
(internal/jobs/metrics/numerical/deploy.go's ComputeDeployMetrics). This
generator is the single source both the frozen golden and the live rot guard
(internal/jobs/metrics/numerical/deploy_golden_rot_guard_test.go) render
from, so the two can never independently drift out of sync with each other --
only the frozen file can drift from a CHANGED production Python, which the
rot guard exists to catch.
"""

from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, cast

from dev_health_ops.metrics.compute_deployments import (
    compute_deploy_metrics_daily,
)
from dev_health_ops.metrics.schemas import DeploymentRow

OUTPUT = Path(__file__).with_name("daily_deploy_python_golden.json")

REPO_A = uuid.UUID("00000000-0000-4000-8000-00000000000a")
REPO_B = uuid.UUID("00000000-0000-4000-8000-00000000000b")
DAY = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 25, tzinfo=timezone.utc)


def _dt(hour: int, minute: int = 0, *, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)


def _deployment(
    *,
    repo_id: uuid.UUID,
    deployment_id: str,
    status: str | None,
    deployed_at: datetime | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    merged_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "repo_id": repo_id,
        "deployment_id": deployment_id,
        "status": status,
        "environment": "production",
        "started_at": started_at,
        "finished_at": finished_at,
        "deployed_at": deployed_at,
        "merged_at": merged_at,
    }


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    def iso(value: datetime | None) -> str | None:
        return None if value is None else value.isoformat().replace("+00:00", "Z")

    return {
        "repo_id": str(row["repo_id"]),
        "deployment_id": row["deployment_id"],
        "status": row["status"],
        "started_at": iso(row["started_at"]),
        "finished_at": iso(row["finished_at"]),
        "deployed_at": iso(row["deployed_at"]),
        "merged_at": iso(row["merged_at"]),
    }


def _case(
    *, label: str, day: date, deployments: list[dict[str, Any]]
) -> dict[str, Any]:
    records = compute_deploy_metrics_daily(
        day=day,
        deployments=cast(Sequence[DeploymentRow], deployments),
        computed_at=COMPUTED_AT,
    )
    return {
        "label": label,
        "day": day.isoformat(),
        "deployments": [_serialize_row(row) for row in deployments],
        "expected": [
            {
                "repo_id": str(r.repo_id),
                "deployments_count": r.deployments_count,
                "failed_deployments_count": r.failed_deployments_count,
                "deploy_time_p50_hours": r.deploy_time_p50_hours,
                "lead_time_p50_hours": r.lead_time_p50_hours,
            }
            for r in records
        ],
    }


def _cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []

    # 1. Single successful deployment, no timing data -> both percentiles None.
    cases.append(
        _case(
            label="single_success_no_timing",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d1",
                    status="success",
                    deployed_at=_dt(12),
                )
            ],
        )
    )

    # 2. Provider-agnostic failure vocabulary: GitHub 'failure'/'error',
    #    GitLab 'failed'/'canceled' must ALL count as failed -- and a
    #    non-failure status ('success') must not.
    cases.append(
        _case(
            label="provider_agnostic_failure_statuses",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d2",
                    status="failure",
                    deployed_at=_dt(1),
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d3",
                    status="error",
                    deployed_at=_dt(2),
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d4",
                    status="failed",
                    deployed_at=_dt(3),
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d5",
                    status="canceled",
                    deployed_at=_dt(4),
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d6",
                    status="success",
                    deployed_at=_dt(5),
                ),
                # Mixed case + whitespace must still normalize.
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d7",
                    status="  FAILURE  ",
                    deployed_at=_dt(6),
                ),
            ],
        )
    )

    # 3. Deploy time computed from started_at/finished_at; lead time from
    #    merged_at -> deployed_at. Two deployments give an interpolated p50.
    cases.append(
        _case(
            label="deploy_time_and_lead_time_two_samples",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d8",
                    status="success",
                    deployed_at=_dt(12),
                    started_at=_dt(10),
                    finished_at=_dt(11),  # 1 hour
                    merged_at=_dt(9),  # 3 hour lead time
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d9",
                    status="success",
                    deployed_at=_dt(14),
                    started_at=_dt(10),
                    finished_at=_dt(13),  # 3 hours
                    merged_at=_dt(8),  # 6 hour lead time
                ),
            ],
        )
    )

    # 4. deployed_at missing -> falls back to started_at, both for the
    #    window check AND as the deployed_at used in lead-time math.
    cases.append(
        _case(
            label="deployed_at_falls_back_to_started_at",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d10",
                    status="success",
                    deployed_at=None,
                    started_at=_dt(9),
                    merged_at=_dt(7),  # lead time = 2h against fallback deployed_at
                )
            ],
        )
    )

    # 5. Row entirely outside the day window (deployed_at before start) is
    #    excluded -> zero rows for that repo.
    cases.append(
        _case(
            label="outside_window_excluded",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d11",
                    status="success",
                    deployed_at=_dt(23, 0, day=date(2026, 8, 23)),
                )
            ],
        )
    )

    # 6. Negative duration/lead time (bad data / clock skew) is silently
    #    dropped from the percentile input, not clamped to zero -- the
    #    deployment itself still counts.
    cases.append(
        _case(
            label="negative_duration_and_lead_time_dropped",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d12",
                    status="success",
                    deployed_at=_dt(9),
                    started_at=_dt(11),
                    finished_at=_dt(10),  # negative duration
                    merged_at=_dt(10),  # merged AFTER deployed_at -> negative lead time
                )
            ],
        )
    )

    # 7. Multiple repos in one partition sort by repo_id ascending.
    cases.append(
        _case(
            label="multiple_repos_sorted",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_B,
                    deployment_id="d13",
                    status="success",
                    deployed_at=_dt(10),
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d14",
                    status="success",
                    deployed_at=_dt(11),
                ),
            ],
        )
    )

    # 8. Odd-count percentile sample (3 durations) exercises the
    #    interpolation branch's exact-rank case.
    cases.append(
        _case(
            label="odd_sample_percentile",
            day=DAY,
            deployments=[
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d15",
                    status="success",
                    deployed_at=_dt(1),
                    started_at=_dt(0),
                    finished_at=_dt(1),  # 1h
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d16",
                    status="success",
                    deployed_at=_dt(6),
                    started_at=_dt(0),
                    finished_at=_dt(2),  # 2h
                ),
                _deployment(
                    repo_id=REPO_A,
                    deployment_id="d17",
                    status="success",
                    deployed_at=_dt(8),
                    started_at=_dt(0),
                    finished_at=_dt(9),  # 9h
                ),
            ],
        )
    )

    return cases


def render() -> str:
    value = {
        "schema_version": 1,
        "deploy": _cases(),
    }
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Render to stdout instead of writing the checked-in file. The "
            "live rot guard (internal/jobs/metrics/numerical) uses this to "
            "compare what TODAY's production Python produces against the "
            "frozen file, so a drift is reported as a diff rather than a "
            "bare exit code."
        ),
    )
    args = parser.parse_args()
    rendered = render()
    if args.stdout:
        print(rendered, end="")
        return 0
    if args.check:
        return 0 if OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
