"""Generate the effort-selection golden for CHAOS-4441.

Drives `materialize._effort_from_work_unit` itself -- imported, never imitated.

WHAT MAKES THIS MORE THAN A PRIORITY CHAIN
------------------------------------------
The tiers are gated on `total > 0`, which is a FALL-THROUGH rather than a
validity check, and three consequences follow that a `!= 0` or "has data"
spelling would get wrong:

  * a NEGATIVE total falls through to the next tier
  * a NaN total falls through too, since `nan > 0` is false
  * all tiers non-positive yields ("churn_loc", 0.0) -- naming churn_loc even
    for a unit with no commits and no PRs

Plus two collection behaviours: a missing id contributes 0.0 rather than being
skipped, and DUPLICATE ids double-count, because the sum walks the id list and
not the churn map.

Usage:
    uv run python tests/fixtures/generate_effort_golden.py [--stdout]
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.materialize import _effort_from_work_unit

OUTPUT_PATH = Path(__file__).parent / "effort_python_golden.json"

NAN = float("nan")
INF = float("inf")


def _num(value: float) -> Any:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return value


def _scenarios() -> list[dict[str, Any]]:
    return [
        {"label": "empty"},
        # Priority, one tier at a time and all together.
        {
            "label": "commit_wins_over_all",
            "commit_ids": ["c"],
            "commit_churn": {"c": 10.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": 20.0},
            "issue_ids": ["i"],
            "active_hours": {"i": 30.0},
        },
        {
            "label": "commit_zero_falls_to_pr",
            "commit_ids": ["c"],
            "commit_churn": {"c": 0.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": 20.0},
            "issue_ids": ["i"],
            "active_hours": {"i": 30.0},
        },
        {
            "label": "commit_and_pr_zero_falls_to_active",
            "commit_ids": ["c"],
            "commit_churn": {"c": 0.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": 0.0},
            "issue_ids": ["i"],
            "active_hours": {"i": 30.0},
        },
        {"label": "only_active_hours", "issue_ids": ["i"], "active_hours": {"i": 3.5}},
        {"label": "only_pr_churn", "pr_ids": ["p"], "pr_churn": {"p": 2.0}},
        # The > 0 gate as a fall-through.
        {
            "label": "negative_commit_falls_to_pr",
            "commit_ids": ["c"],
            "commit_churn": {"c": -5.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": 20.0},
        },
        {
            "label": "negative_commit_and_pr_falls_to_active",
            "commit_ids": ["c"],
            "commit_churn": {"c": -5.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": -1.0},
            "issue_ids": ["i"],
            "active_hours": {"i": 4.0},
        },
        {
            "label": "all_negative_yields_churn_loc_zero",
            "commit_ids": ["c"],
            "commit_churn": {"c": -5.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": -1.0},
            "issue_ids": ["i"],
            "active_hours": {"i": -2.0},
        },
        {
            "label": "negatives_cancel_to_exactly_zero_falls_through",
            "commit_ids": ["a", "b"],
            "commit_churn": {"a": 5.0, "b": -5.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": 9.0},
        },
        # Collection behaviour.
        {
            "label": "missing_id_contributes_zero",
            "commit_ids": ["nope"],
            "commit_churn": {"c": 10.0},
            "pr_ids": ["p"],
            "pr_churn": {"p": 4.0},
        },
        {
            "label": "duplicate_ids_double_count",
            "commit_ids": ["c", "c"],
            "commit_churn": {"c": 10.0},
        },
        {
            "label": "duplicate_ids_triple_count",
            "commit_ids": ["c", "c", "c"],
            "commit_churn": {"c": 1.5},
        },
        {
            "label": "many_ids_sum_in_order",
            "commit_ids": [f"c{n}" for n in range(10)],
            "commit_churn": {f"c{n}": 0.1 for n in range(10)},
        },
        # Non-finite.
        {
            "label": "nan_commit_falls_through",
            "commit_ids": ["c"],
            "commit_churn": {"c": NAN},
            "pr_ids": ["p"],
            "pr_churn": {"p": 7.0},
        },
        {
            "label": "nan_everywhere_yields_zero",
            "commit_ids": ["c"],
            "commit_churn": {"c": NAN},
            "pr_ids": ["p"],
            "pr_churn": {"p": NAN},
            "issue_ids": ["i"],
            "active_hours": {"i": NAN},
        },
        {
            "label": "inf_commit_is_selected",
            "commit_ids": ["c"],
            "commit_churn": {"c": INF},
        },
        {
            "label": "negative_inf_falls_through",
            "commit_ids": ["c"],
            "commit_churn": {"c": -INF},
            "pr_ids": ["p"],
            "pr_churn": {"p": 3.0},
        },
        {
            "label": "inf_minus_inf_is_nan_falls_through",
            "commit_ids": ["a", "b"],
            "commit_churn": {"a": INF, "b": -INF},
            "pr_ids": ["p"],
            "pr_churn": {"p": 6.0},
        },
        # Float associativity: the same values in a different order.
        {
            "label": "assoc_order_one",
            "commit_ids": ["a", "b", "c"],
            "commit_churn": {"a": 0.1, "b": 0.2, "c": 0.3},
        },
        {
            "label": "assoc_order_two",
            "commit_ids": ["c", "b", "a"],
            "commit_churn": {"a": 0.1, "b": 0.2, "c": 0.3},
        },
        # --- the SUMMATION axis ---
        # CPython's sum() is Neumaier-compensated (3.12+). Below three summands
        # the compensation is always zero, so every case above is blind to it.
        # These are chosen so a naive `total +=` loop gives a different answer.
        {
            "label": "many_summands_compensation_matters",
            "commit_ids": [f"c{n}" for n in range(20)],
            "commit_churn": {f"c{n}": 0.1 for n in range(20)},
        },
        {
            "label": "hundred_summands",
            "commit_ids": [f"c{n}" for n in range(100)],
            "commit_churn": {f"c{n}": 0.1 for n in range(100)},
        },
        {
            "label": "wide_magnitude_spread_swallows_addends",
            "commit_ids": ["big"] + [f"s{n}" for n in range(10)] + ["neg"],
            "commit_churn": {
                **{"big": 1e16},
                **{f"s{n}": 1.0 for n in range(10)},
                **{"neg": -1e16},
            },
        },
        {
            "label": "alternating_magnitudes",
            "commit_ids": [f"x{n}" for n in range(12)],
            "commit_churn": {
                f"x{n}": (1e12 if n % 2 == 0 else 1e-12) for n in range(12)
            },
        },
        # The tier decision itself turning on the last bit: a churn list that
        # sums to a hair above or below zero depending on the algorithm.
        {
            "label": "near_cancellation_decides_the_tier",
            "commit_ids": ["a", "b", "c"],
            "commit_churn": {"a": 1e16, "b": 1.0, "c": -1e16},
            "pr_ids": ["p"],
            "pr_churn": {"p": 42.0},
        },
        # A tiny positive total still wins its tier.
        {
            "label": "smallest_positive_wins",
            "commit_ids": ["c"],
            "commit_churn": {"c": 5e-324},
            "pr_ids": ["p"],
            "pr_churn": {"p": 100.0},
        },
    ]


def main() -> None:
    cases: list[dict[str, Any]] = []
    for scenario in _scenarios():
        kwargs: dict[str, Any] = {
            "issue_ids": scenario.get("issue_ids", []),
            "pr_ids": scenario.get("pr_ids", []),
            "commit_ids": scenario.get("commit_ids", []),
            "pr_churn": scenario.get("pr_churn", {}),
            "commit_churn": scenario.get("commit_churn", {}),
            "active_hours": scenario.get("active_hours", {}),
        }
        metric, value = _effort_from_work_unit(**kwargs)
        cases.append(
            {
                "label": scenario["label"],
                "issue_ids": kwargs["issue_ids"],
                "pr_ids": kwargs["pr_ids"],
                "commit_ids": kwargs["commit_ids"],
                "pr_churn": {k: _num(v) for k, v in kwargs["pr_churn"].items()},
                "commit_churn": {k: _num(v) for k, v in kwargs["commit_churn"].items()},
                "active_hours": {k: _num(v) for k, v in kwargs["active_hours"].items()},
                "metric": metric,
                "value": _num(value),
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_effort_golden.py. Do not hand-edit."
        ),
        "_note": (
            "The tiers are gated on `total > 0`, a FALL-THROUGH: negative and NaN "
            "totals move to the next tier rather than being selected. Duplicate ids "
            "double-count because the sum walks the id list, not the churn map."
        ),
        "cases": cases,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return
    OUTPUT_PATH.write_text(rendered)
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(cases)}")


if __name__ == "__main__":
    main()
