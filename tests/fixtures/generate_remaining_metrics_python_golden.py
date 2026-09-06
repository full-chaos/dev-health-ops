from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

from dev_health_ops.metrics.compute_capacity import (
    ThroughputHistory,
    ThroughputSample,
    compute_percentiles,
)
from dev_health_ops.metrics.release_impact import _compute_confidence

OUTPUT = Path(__file__).with_name("remaining_metrics_python_golden.json")
DAY = date(2026, 7, 20)


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
        "capacity": _capacity(),
        "release_confidence": _release_confidence(),
    }
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Render to stdout instead of writing the checked-in file. The live "
            "rot guard (internal/jobs/metrics/numerical) uses this to compare "
            "what TODAY's production Python produces against the frozen file, "
            "so a drift is reported as a diff rather than a bare exit code."
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
