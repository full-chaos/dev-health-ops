"""Golden corpus for the recommendations signal helpers, from the live interpreter.

Run from the ops worktree:

    .venv/bin/python internal/jobs/metrics/remaining/testdata/generate_recommendations_signals_golden.py --stdout

Mirrors under test: remaining.Gini, remaining.LinearSlope.
References: src/dev_health_ops/recommendations/loader.py:69 (_gini) and
rules/saturation.py:44 (_linear_slope, byte-identical twin at
rules/sustainability_risk.py:44).

Both feed CATEGORICAL outputs -- Gini against REVIEWER_GINI_THRESHOLD 0.6,
LinearSlope against the two 0.1 slope thresholds -- so a last-bit difference is
a stored recommendation row that either exists or does not. Values travel as hex
float literals: a float-parity corpus serialised through decimal text cannot
distinguish the bugs it exists to catch.

Axes varied deliberately, since a corpus is blind to any axis it holds constant:
  cardinality  -- 0,1,2,3 entries (the guard boundaries) through 100
  magnitude    -- 1e-8 .. 1e15, and mixed within one input
  sign         -- negatives and -0.0, which the `v > 0` filter must exclude
  ties         -- repeated values, where sort order among equals must not matter
  specials     -- NaN and +/-Inf, which must be filtered or must propagate
  degenerate   -- all-zero (total == 0.0 -> 0.0), single positive (-> None)
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import sys
import unicodedata
from pathlib import Path

OUTPUT_PATH = Path(__file__).parent / "recommendations_signals_golden.json"


def _gini(values: list[float]) -> float | None:
    positives = [v for v in values if v > 0]
    if len(positives) < 2:
        return None
    total = sum(positives)
    if total == 0.0:
        return 0.0
    n = len(positives)
    sorted_vals = sorted(positives)
    cumulative = sum((i + 1) * v for i, v in enumerate(sorted_vals))
    return (2.0 * cumulative) / (n * total) - (n + 1.0) / n


def _linear_slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def _inputs() -> list[list[float]]:
    nan, inf = float("nan"), float("inf")
    cases: list[list[float]] = [
        [],
        [1.0],
        [1.0, 2.0],
        [0.0, 0.0],
        [0.0, 0.0, 0.0],
        [-0.0, -0.0],
        [-1.0, -2.0],
        [5.0, 0.0, 0.0],
        [1.0, 1.0, 1.0, 1.0],
        [2.0, 2.0],
        [1.0, 1e15],
        [1e-8, 1e-8, 1e-8],
        [0.1, 0.2, 0.30000000000000004, 0.4, 0.5, 0.6, 0.7],
        # the measured threshold-flip series from the slope receipt
        [0.1, 0.0, 1.0, 0.8, 0.2],
        [0.4, 0.0, 0.097341, 1.0, 0.4],
        [0.5, 0.0, 0.009807, 1.0, 0.5],
        [nan, 1.0, 2.0],
        [inf, 1.0],
        [-inf, 1.0],
        [nan, nan],
        [1.0, -1.0, 1.0, -1.0],
        [1e-323, 5e-324],
        [1.7976931348623157e308, 1.0],
    ]
    # deterministic sweeps: cardinality x magnitude, seeded so the file is stable
    import random

    random.seed(20260902)
    for count in (3, 5, 7, 14, 30, 60, 100):
        for scale in (1.0, 100.0, 1e5, 1e-4):
            cases.append([round(random.uniform(0, scale), 6) for _ in range(count)])
            cases.append(
                [round(random.uniform(-scale, scale), 6) for _ in range(count)]
            )
    return cases


def _hex(value: float) -> str:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return float.hex(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--out", default=str(OUTPUT_PATH))
    args = parser.parse_args()

    entries = []
    for values in _inputs():
        gini = _gini(values)
        entries.append(
            {
                "values_hex": [_hex(v) for v in values],
                "gini_hex": None if gini is None else _hex(gini),
                "gini_is_none": gini is None,
                "slope_hex": _hex(_linear_slope(values)),
            }
        )

    document = {
        "_marker": "recommendations-signals-golden",
        "_generator": (
            "internal/jobs/metrics/remaining/testdata/"
            "generate_recommendations_signals_golden.py"
        ),
        "generating_interpreter": {
            "python_version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "unicode_version": unicodedata.unidata_version,
            "machine": platform.machine(),
            "float_repr_style": sys.float_repr_style,
        },
        "cases": entries,
    }
    text = json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.stdout:
        sys.stdout.write(text)
    else:
        Path(args.out).write_text(text)


if __name__ == "__main__":
    main()
