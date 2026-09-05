"""Golden generator for the compounding_risk family (CHAOS-4287).

Frozen output of the REAL Python compute path -- ``compute_compounding_risk``
(src/dev_health_ops/metrics/compounding_risk.py:319) -- over a synthetic
``CompoundingInputs`` corpus chosen to hit every branch the native Go port has
to reproduce bit-for-bit:

  - all-present, mid-range: the ordinary weighted-sum path.
  - EXACTLY on each severity boundary (0.40 and 0.65): the four normalized
    components are chosen so the weighted sum lands on the threshold. These are
    the rows that decide whether Go's FMA contraction matters in practice --
    the buckets are `>=`, so a one-ulp difference in the sum moves the row to
    the adjacent severity. See compute.go's FMA-barrier comment.
  - long-fraction inputs: a weighted sum whose four products all have full
    53-bit mantissas, so a fused multiply-add would round differently from
    CPython's separately-rounded multiply-then-add.
  - each required input missing in turn (rework_churn / complexity_delta /
    review_latency_p90h / both ownership signals) -- score None, severity
    "unknown", and the row still emitted.
  - single_owner_ratio alone, and ownership_gini alone: either is acceptable
    for the ownership component; both absent is not.
  - ownership tie: single_owner_ratio == ownership_gini, pinning max()'s
    first-wins order.
  - negative inputs on all three reference-normalized signals: falling
    complexity is not risk, so `max(0.0, x)` clamps before the divide.
  - inputs above their reference: clamp01 saturates the component at 1.0.
  - bus_factor present and absent: pure metadata, never part of the formula,
    but persisted.
  - a saturating all-ones row (severity "high") and an all-zero row
    (severity "low", score exactly 0.0).

Regenerate with `python tests/fixtures/generate_daily_compounding_risk_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.compounding_risk import (
    CompoundingInputs,
    compute_compounding_risk,
)

OUTPUT = Path(__file__).with_name("daily_compounding_risk_python_golden.json")

ORG_ID = "org-compounding-golden"
DAY = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)

REPO = "00000000-0000-4000-8000-0000000000"


def _repo(suffix: str) -> str:
    return str(UUID(REPO + suffix))


# (scope_id_suffix, inputs) -- scope_id ordering here IS the frozen row order.
CASES: list[tuple[str, CompoundingInputs]] = [
    # Ordinary mid-range row, every signal present.
    (
        "01",
        CompoundingInputs(
            rework_churn=0.15,
            complexity_delta=0.05,
            review_latency_p90h=12.0,
            single_owner_ratio=0.35,
            ownership_gini=0.20,
            bus_factor=3.0,
        ),
    ),
    # EXACTLY on the "elevated" boundary: every component normalizes to 0.4,
    # so the weighted sum is 0.30*0.4 + 0.30*0.4 + 0.20*0.4 + 0.20*0.4.
    # churn 0.12/0.30, complexity 0.08/0.20, review 19.2/48.0, ownership 0.4.
    (
        "02",
        CompoundingInputs(
            rework_churn=0.12,
            complexity_delta=0.08,
            review_latency_p90h=19.2,
            single_owner_ratio=0.4,
            ownership_gini=None,
            bus_factor=2.0,
        ),
    ),
    # EXACTLY on the "high" boundary: every component normalizes to 0.65.
    # churn 0.195/0.30, complexity 0.13/0.20, review 31.2/48.0, ownership 0.65.
    (
        "03",
        CompoundingInputs(
            rework_churn=0.195,
            complexity_delta=0.13,
            review_latency_p90h=31.2,
            single_owner_ratio=0.65,
            ownership_gini=None,
            bus_factor=1.0,
        ),
    ),
    # Long-fraction inputs: four full-mantissa products in the weighted sum.
    (
        "04",
        CompoundingInputs(
            rework_churn=0.1234567890123457,
            complexity_delta=0.0987654321098765,
            review_latency_p90h=17.371717171717171,
            single_owner_ratio=0.3141592653589793,
            ownership_gini=0.2718281828459045,
            bus_factor=2.7182818284590452,
        ),
    ),
    # Missing rework_churn -> unknown.
    (
        "05",
        CompoundingInputs(
            rework_churn=None,
            complexity_delta=0.05,
            review_latency_p90h=12.0,
            single_owner_ratio=0.35,
            ownership_gini=0.20,
            bus_factor=3.0,
        ),
    ),
    # Missing complexity_delta -> unknown.
    (
        "06",
        CompoundingInputs(
            rework_churn=0.15,
            complexity_delta=None,
            review_latency_p90h=12.0,
            single_owner_ratio=0.35,
            ownership_gini=0.20,
            bus_factor=3.0,
        ),
    ),
    # Missing review_latency_p90h -> unknown.
    (
        "07",
        CompoundingInputs(
            rework_churn=0.15,
            complexity_delta=0.05,
            review_latency_p90h=None,
            single_owner_ratio=0.35,
            ownership_gini=0.20,
            bus_factor=3.0,
        ),
    ),
    # Both ownership signals missing -> unknown (and ownership_norm None).
    (
        "08",
        CompoundingInputs(
            rework_churn=0.15,
            complexity_delta=0.05,
            review_latency_p90h=12.0,
            single_owner_ratio=None,
            ownership_gini=None,
            bus_factor=None,
        ),
    ),
    # ownership_gini alone is enough.
    (
        "09",
        CompoundingInputs(
            rework_churn=0.15,
            complexity_delta=0.05,
            review_latency_p90h=12.0,
            single_owner_ratio=None,
            ownership_gini=0.55,
            bus_factor=None,
        ),
    ),
    # Ownership tie: max() keeps the first candidate, pinning the fold order.
    (
        "10",
        CompoundingInputs(
            rework_churn=0.15,
            complexity_delta=0.05,
            review_latency_p90h=12.0,
            single_owner_ratio=0.44,
            ownership_gini=0.44,
            bus_factor=4.0,
        ),
    ),
    # Negative inputs on all three reference-normalized signals: clamped to 0
    # BEFORE the divide. Falling complexity is not risk.
    (
        "11",
        CompoundingInputs(
            rework_churn=-0.5,
            complexity_delta=-0.25,
            review_latency_p90h=-6.0,
            single_owner_ratio=0.10,
            ownership_gini=None,
            bus_factor=8.0,
        ),
    ),
    # Above reference on every signal: each component saturates at 1.0, so the
    # weighted sum is exactly 1.0 and severity is "high".
    (
        "12",
        CompoundingInputs(
            rework_churn=0.9,
            complexity_delta=0.8,
            review_latency_p90h=200.0,
            single_owner_ratio=1.5,
            ownership_gini=2.0,
            bus_factor=1.0,
        ),
    ),
    # All-zero: score exactly 0.0, severity "low".
    (
        "13",
        CompoundingInputs(
            rework_churn=0.0,
            complexity_delta=0.0,
            review_latency_p90h=0.0,
            single_owner_ratio=0.0,
            ownership_gini=0.0,
            bus_factor=0.0,
        ),
    ),
]


def _serialize(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def main() -> str:
    records = [
        compute_compounding_risk(
            day=DAY,
            scope="repo",
            scope_id=_repo(suffix),
            org_id=ORG_ID,
            inputs=inputs,
            computed_at=COMPUTED_AT,
        )
        for suffix, inputs in CASES
    ]
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
