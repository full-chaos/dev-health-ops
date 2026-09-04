"""Generate the repo-effort-allocation golden for CHAOS-4441.

Drives `materialize._allocate_repo_effort` itself -- imported, never
imitated (plan.md section 5b's audit question).

Same import cost as generate_effort_golden.py, and excluded from the CI
live-oracle closure for the identical reason: importing
work_graph.investment.materialize transitively pulls in httpx2, which the
--no-deps oracle closure does not pin (see excludedGenerators in
internal/jobs/workgraph/units/live_python_corpus_guard_test.go).

WHAT THIS CORPUS IS AIMED AT, BEYOND generate_effort_golden.py's OWN AXES
---------------------------------------------------------------------------
_allocate_repo_effort shares _effort_from_work_unit's fall-through
discipline (a tier fires only when ITS OWN total is strictly positive) but
adds two behaviours that function does not have at all:

  * PER-REPO aggregation and a SORT: churn is grouped by the repo parsed out
    of each commit/PR id, and the returned rows are ordered by
    sorted(dict.items()) -- i.e. by the repo-id STRING, not by the order
    repos were first seen. A corpus that only ever sees one repo can never
    exercise the sort.
  * the per-repo TOTAL is a plain `total += churn` accumulator, NOT
    Python's sum() -- see internal/jobs/workgraph/units/repoeffort.go's
    AllocateRepoEffort doc comment for why this matters for a Go port
    reaching for the wrong helper.
  * an unparseable id does NOT silently drop out. `repo_id, _ =
    parse_commit_from_id(commit_id)` yields None, and `repo_key = str(repo_id)
    if repo_id else ""` maps that to the empty string -- which is a perfectly
    ordinary dict key, so its churn accumulates into an "" bucket the same
    way a real repo's churn accumulates into its UUID string bucket. At the
    end `_parse_repo_id(repo_key or None)` turns "" back into None, so the
    output row carries repo_id=None with a REAL, non-empty allocation_weight
    -- an "unattributed effort" row, not an absent one. This was NOT the
    intuitive first guess writing this generator (silently dropped seemed
    more likely) and is exactly why this corpus drives the real function
    instead of asserting from reading its source.

Usage:
    uv run python tests/fixtures/generate_repo_effort_allocation_golden.py [--stdout]
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.materialize import _allocate_repo_effort

OUTPUT_PATH = Path(__file__).parent / "repo_effort_allocation_python_golden.json"

NAN = float("nan")

REPO_A = "11111111-1111-4111-8111-111111111111"
REPO_B = "22222222-2222-4222-8222-222222222222"
REPO_C = "00000000-0000-4000-8000-000000000000"


def _commit_id(repo: str, hash_: str) -> str:
    return f"{repo}@{hash_}"


def _pr_id(repo: str, number: int) -> str:
    return f"{repo}#pr{number}"


def _num(value: float) -> Any:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return value


def _scenarios() -> list[dict[str, Any]]:
    return [
        {"label": "empty"},
        {
            "label": "single_commit_single_repo",
            "commit_ids": [_commit_id(REPO_A, "h1")],
            "commit_churn": {_commit_id(REPO_A, "h1"): 10.0},
        },
        {
            "label": "multiple_commits_same_repo_accumulate",
            "commit_ids": [_commit_id(REPO_A, "h1"), _commit_id(REPO_A, "h2")],
            "commit_churn": {
                _commit_id(REPO_A, "h1"): 3.0,
                _commit_id(REPO_A, "h2"): 4.0,
            },
        },
        {
            # Insertion order is B, A, C -- the golden must show them sorted
            # by repo-id string, proving the sort and not just insertion.
            "label": "multiple_repos_sorted_by_key",
            "commit_ids": [
                _commit_id(REPO_B, "hb"),
                _commit_id(REPO_A, "ha"),
                _commit_id(REPO_C, "hc"),
            ],
            "commit_churn": {
                _commit_id(REPO_B, "hb"): 2.0,
                _commit_id(REPO_A, "ha"): 6.0,
                _commit_id(REPO_C, "hc"): 2.0,
            },
        },
        {
            "label": "zero_churn_commit_excluded_from_repo_map",
            "commit_ids": [_commit_id(REPO_A, "h1"), _commit_id(REPO_B, "h2")],
            "commit_churn": {
                _commit_id(REPO_A, "h1"): 0.0,
                _commit_id(REPO_B, "h2"): 5.0,
            },
        },
        {
            "label": "negative_commit_total_falls_to_pr",
            "commit_ids": [_commit_id(REPO_A, "h1")],
            "commit_churn": {_commit_id(REPO_A, "h1"): -5.0},
            "pr_ids": [_pr_id(REPO_B, 1)],
            "pr_churn": {_pr_id(REPO_B, 1): 20.0},
        },
        {
            # A LONE negative commit (the case above) can't distinguish
            # "commitTotal sums ALL churn, including negatives" (correct --
            # see AllocateRepoEffort's own `total += churn` before the
            # positive-only per-repo skip) from "commitTotal sums only
            # POSITIVE churn": with a single negative value, a positive-only
            # accumulator sees nothing at all and a real accumulator sees a
            # negative number -- both are <= 0, so both fall to PR the same
            # way. MIXING a positive and a negative commit makes the two
            # diverge: the real total here is 4 + (-5) = -1 (<= 0, falls to
            # PR); a positive-only accumulator would see only +4 (> 0,
            # WRONGLY selects commit_churn as the source). CHAOS-4441 r2's
            # own finding on this exact gap.
            "label": "mixed_positive_and_negative_commit_total_falls_to_pr",
            "commit_ids": [_commit_id(REPO_A, "h1"), _commit_id(REPO_A, "h2")],
            "commit_churn": {
                _commit_id(REPO_A, "h1"): 4.0,
                _commit_id(REPO_A, "h2"): -5.0,
            },
            "pr_ids": [_pr_id(REPO_B, 1)],
            "pr_churn": {_pr_id(REPO_B, 1): 9.0},
        },
        {
            # Same [+4,-5] mixed-sign shape as above, ids REVERSED so the
            # LAST commit processed is the POSITIVE one. The case above
            # alone can't tell "commitTotal accumulates every value" (
            # correct) apart from a "gate on the LAST commit's own sign/
            # value only" regression -- both happen to end on a negative
            # last value there, so both wrongly-implemented and correctly-
            # implemented versions fall to PR either way. Reversing the
            # order makes a last-value-gate regression WRONGLY select the
            # commit tier (last value +4, positive) while the real
            # accumulator still correctly totals -1 and falls to PR
            # (CHAOS-4441 r3 finding).
            "label": "mixed_positive_and_negative_commit_total_falls_to_pr_reversed_order",
            "commit_ids": [_commit_id(REPO_A, "h1"), _commit_id(REPO_A, "h2")],
            "commit_churn": {
                _commit_id(REPO_A, "h1"): -5.0,
                _commit_id(REPO_A, "h2"): 4.0,
            },
            "pr_ids": [_pr_id(REPO_B, 1)],
            "pr_churn": {_pr_id(REPO_B, 1): 9.0},
        },
        {
            "label": "nan_commit_total_falls_to_pr",
            "commit_ids": [_commit_id(REPO_A, "h1")],
            "commit_churn": {_commit_id(REPO_A, "h1"): NAN},
            "pr_ids": [_pr_id(REPO_B, 1)],
            "pr_churn": {_pr_id(REPO_B, 1): 7.0},
        },
        {
            "label": "commit_and_pr_nonpositive_falls_to_active_hours",
            "commit_ids": [_commit_id(REPO_A, "h1")],
            "commit_churn": {_commit_id(REPO_A, "h1"): 0.0},
            "pr_ids": [_pr_id(REPO_B, 1)],
            "pr_churn": {_pr_id(REPO_B, 1): -1.0},
            "effort_metric": "active_hours",
            "effort_value": 8.0,
        },
        {
            "label": "active_hours_metric_but_zero_value_yields_empty",
            "effort_metric": "active_hours",
            "effort_value": 0.0,
        },
        {
            "label": "churn_metric_with_no_commits_or_prs_yields_empty",
            "effort_metric": "churn_loc",
            "effort_value": 0.0,
        },
        {
            "label": "pr_repos_sorted_multiple",
            "pr_ids": [_pr_id(REPO_B, 2), _pr_id(REPO_A, 1)],
            "pr_churn": {_pr_id(REPO_B, 2): 3.0, _pr_id(REPO_A, 1): 9.0},
        },
        {
            # An id that fails to parse still contributes to the TIER total
            # (the loop adds churn unconditionally) but never reaches a
            # repo bucket -- so the tier fires yet the allocation list can
            # come back empty if it was the ONLY commit.
            "label": "unparseable_commit_id_counts_toward_total_but_no_bucket",
            "commit_ids": ["not-a-canonical-id"],
            "commit_churn": {"not-a-canonical-id": 5.0},
        },
        {
            "label": "unparseable_commit_alongside_a_real_one",
            "commit_ids": [_commit_id(REPO_A, "h1"), "not-a-canonical-id"],
            "commit_churn": {
                _commit_id(REPO_A, "h1"): 4.0,
                "not-a-canonical-id": 6.0,
            },
        },
        {
            "label": "duplicate_commit_ids_double_count_into_same_repo",
            "commit_ids": [_commit_id(REPO_A, "h1"), _commit_id(REPO_A, "h1")],
            "commit_churn": {_commit_id(REPO_A, "h1"): 3.0},
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
            "effort_metric": scenario.get("effort_metric", "churn_loc"),
            "effort_value": scenario.get("effort_value", 0.0),
        }
        allocations = _allocate_repo_effort(**kwargs)
        cases.append(
            {
                "label": scenario["label"],
                "commit_ids": kwargs["commit_ids"],
                "pr_ids": kwargs["pr_ids"],
                "commit_churn": {k: _num(v) for k, v in kwargs["commit_churn"].items()},
                "pr_churn": {k: _num(v) for k, v in kwargs["pr_churn"].items()},
                "effort_metric": kwargs["effort_metric"],
                "effort_value": _num(kwargs["effort_value"]),
                "allocations": [
                    {
                        "repo_id": str(repo_id) if repo_id is not None else None,
                        "repo_effort": _num(repo_effort),
                        "allocation_weight": _num(allocation_weight),
                        "allocation_source": allocation_source,
                    }
                    for repo_id, repo_effort, allocation_weight, allocation_source in allocations
                ],
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_repo_effort_allocation_golden.py. "
            "Do not hand-edit."
        ),
        "_note": (
            "Rows are ordered by sorted(dict.items()) on the repo-id string. "
            "Per-repo totals are a plain `total += churn` accumulator, not sum() -- "
            "see AllocateRepoEffort's doc comment in repoeffort.go."
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
