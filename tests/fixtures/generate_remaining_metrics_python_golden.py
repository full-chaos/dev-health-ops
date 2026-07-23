from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from dev_health_ops.analytics.complexity import FileComplexity
from dev_health_ops.metrics.compute_capacity import (
    ThroughputHistory,
    ThroughputSample,
    compute_percentiles,
)
from dev_health_ops.metrics.compute_dora import compute_dora_metrics_daily
from dev_health_ops.metrics.job_complexity_db import _build_snapshots
from dev_health_ops.metrics.release_impact import _compute_confidence
from dev_health_ops.metrics.schemas import DeploymentRow, IncidentRow

OUTPUT = Path(__file__).with_name("remaining_metrics_python_golden.json")
REPO_A = "00000000-0000-4000-8000-00000000000a"
REPO_B = "00000000-0000-4000-8000-00000000000b"
DAY = date(2026, 7, 20)
COMPUTED_AT = datetime(2026, 7, 21, tzinfo=timezone.utc)


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _dora() -> list[dict[str, Any]]:
    deployments: list[dict[str, Any]] = [
        {
            "repo_id": REPO_B,
            "status": "success",
            "deployed_at": _timestamp("2026-07-20T14:00:00Z"),
            "started_at": None,
            "merged_at": _timestamp("2026-07-20T12:00:00Z"),
        },
        {
            "repo_id": REPO_A,
            "status": " FAILURE ",
            "deployed_at": _timestamp("2026-07-20T10:00:00Z"),
            "started_at": None,
            "merged_at": _timestamp("2026-07-20T08:00:00Z"),
        },
        {
            "repo_id": REPO_A,
            "status": "canceled",
            "deployed_at": None,
            "started_at": _timestamp("2026-07-20T12:00:00Z"),
            "merged_at": _timestamp("2026-07-20T09:00:00Z"),
        },
        {
            "repo_id": REPO_A,
            "status": "success",
            "deployed_at": _timestamp("2026-07-19T23:59:59Z"),
            "started_at": None,
            "merged_at": _timestamp("2026-07-19T22:00:00Z"),
        },
        {
            "repo_id": REPO_B,
            "status": "success",
            "deployed_at": _timestamp("2026-07-20T18:00:00Z"),
            "started_at": None,
            "merged_at": _timestamp("2026-07-20T19:00:00Z"),
        },
    ]
    incidents: list[dict[str, Any]] = [
        {
            "repo_id": REPO_A,
            "started_at": _timestamp("2026-07-20T07:00:00Z"),
            "resolved_at": _timestamp("2026-07-20T09:00:00Z"),
        },
        {
            "repo_id": REPO_A,
            "started_at": _timestamp("2026-07-20T10:00:00Z"),
            "resolved_at": _timestamp("2026-07-20T14:00:00Z"),
        },
        {
            "repo_id": REPO_B,
            "started_at": _timestamp("2026-07-20T16:00:00Z"),
            "resolved_at": _timestamp("2026-07-20T15:00:00Z"),
        },
    ]
    rows = compute_dora_metrics_daily(
        day=DAY,
        deployments=cast(Sequence[DeploymentRow], deployments),
        incidents=cast(Sequence[IncidentRow], incidents),
        computed_at=COMPUTED_AT,
    )
    return [
        {
            "day": DAY.isoformat(),
            "deployments": [
                {
                    key: (
                        value.isoformat().replace("+00:00", "Z")
                        if isinstance(value, datetime)
                        else value or ""
                    )
                    for key, value in row.items()
                }
                for row in deployments
            ],
            "incidents": [
                {
                    key: (
                        value.isoformat().replace("+00:00", "Z")
                        if isinstance(value, datetime)
                        else value
                    )
                    for key, value in row.items()
                }
                for row in incidents
            ],
            "expected": [
                {
                    "RepoID": str(row.repo_id),
                    "Name": row.metric_name,
                    "Value": row.value,
                }
                for row in rows
            ],
        }
    ]


def _capacity() -> list[dict[str, Any]]:
    cases = [
        ([0, 1, 2, 3, 8], [1, 2, 3, 4, 5, 6, 7], [0, 5, 15, 50, 85, 95, 100]),
        ([5], [], [50, 85, 95]),
    ]
    result: list[dict[str, Any]] = []
    for history, values, percentiles in cases:
        throughput = ThroughputHistory(
            [ThroughputSample(day=DAY, items_completed=value) for value in history]
        )
        result.append(
            {
                "history": history,
                "values": values,
                "percentiles": percentiles,
                "expected": compute_percentiles(values, percentiles),
                "mean": throughput.mean,
                "stddev": throughput.stddev,
            }
        )
    return result


def _complexity() -> list[dict[str, Any]]:
    cases = [
        [
            {
                "LOC": 100,
                "CyclomaticTotal": 20,
                "HighComplexity": 2,
                "VeryHighComplexity": 1,
            },
            {
                "LOC": 400,
                "CyclomaticTotal": 30,
                "HighComplexity": 3,
                "VeryHighComplexity": 0,
            },
        ],
        [
            {
                "LOC": 0,
                "CyclomaticTotal": 7,
                "HighComplexity": 1,
                "VeryHighComplexity": 1,
            }
        ],
        [],
    ]
    result = []
    for case_index, files in enumerate(cases):
        production_files = [
            FileComplexity(
                file_path=f"src/file_{index}.py",
                language="python",
                loc=row["LOC"],
                functions_count=1,
                cyclomatic_total=row["CyclomaticTotal"],
                cyclomatic_avg=float(row["CyclomaticTotal"]),
                high_complexity_functions=row["HighComplexity"],
                very_high_complexity_functions=row["VeryHighComplexity"],
            )
            for index, row in enumerate(files)
        ]
        _, repo_daily = _build_snapshots(
            repo_id=UUID(REPO_A),
            day=DAY,
            ref_value=f"fixture-{case_index}",
            file_results=production_files,
            computed_at=COMPUTED_AT,
            org_id="org-golden",
        )
        result.append(
            {
                "files": files,
                "expected": {
                    "LOCTotal": repo_daily.loc_total,
                    "CyclomaticTotal": repo_daily.cyclomatic_total,
                    "CyclomaticPerKLOC": repo_daily.cyclomatic_per_kloc,
                    "HighComplexity": repo_daily.high_complexity_functions,
                    "VeryHighComplexity": repo_daily.very_high_complexity_functions,
                },
            }
        )
    return result


def _release_confidence() -> list[dict[str, Any]]:
    cases = [(1.0, 300, 0, 300), (0.5, 150, 1, 300), (-1.0, 0, 5, 300), (2.0, 1, 0, 0)]
    return [
        {
            "coverage": coverage,
            "total_sessions": sessions,
            "concurrent_deploys": concurrent,
            "minimum_sessions": minimum,
            "expected": _compute_confidence(coverage, sessions, concurrent, minimum),
        }
        for coverage, sessions, concurrent, minimum in cases
    ]


def render() -> str:
    value = {
        "schema_version": 1,
        "dora": _dora(),
        "capacity": _capacity(),
        "complexity": _complexity(),
        "release_confidence": _release_confidence(),
    }
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        return 0 if OUTPUT.read_text() == rendered else 1
    OUTPUT.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
