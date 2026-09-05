"""Golden generator for compounding_risk's TEAM scope (CHAOS-5084).

Frozen output of the REAL Python aggregator -- ``_build_team_rows``
(src/dev_health_ops/metrics/compounding_risk.py:554) -- over a synthetic
per-repo ``CompoundingInputs`` corpus grouped by team, chosen to exercise the
aggregation behaviour ``compoundingrisk.BuildTeamRows`` has to reproduce
bit-for-bit on the Go side:

  - a two-repo team where every input is present on both repos: an ordinary
    mean, feeding all four normalized components through the same weighted
    sum as a repo row.
  - a three-repo team, so the mean's ``pythonparity.Sum``-backed division has
    more than two summands per component -- see MeanOrNone's own doc comment
    on the axis compensated summation actually bites on.
  - a repo missing one required input (rework_churn) inside an otherwise
    complete team: ``_mean_or_none`` skips the None and averages the
    remaining repos for THAT component only, so the team's score can still
    be non-None even though one contributing repo's own repo-scope row would
    be "unknown".
  - a team where every repo lacks BOTH ownership signals: the team's
    ownership component (and therefore the whole score) is None, exactly
    like a repo row would be.
  - a single-repo team: the mean is that repo's own values verbatim, the
    trivial case that could hide an off-by-one in mean-of-one.
  - a repo present in ``repo_inputs`` but ABSENT from ``repo_to_team``
    (unresolved): must not silently create a phantom team, and must not
    affect any OTHER team's aggregation.

ROW ORDER IS PART OF THE FIXTURE, not incidental: ``_build_team_rows``
iterates ``repo_inputs.items()`` in dict-insertion order, and Python's
Neumaier-compensated ``sum()`` is not order-invariant at the bit level (see
pythonparity.Sum's doc comment) -- CASES below is ordered by repo_id
ascending on purpose, the same order compoundingrisk.LoadRepoMetricsForOrgDay's
own explicit ``ORDER BY repo_id`` produces on the Go side, so the Go test
feeding BuildTeamRows must present its RepoInputs slice in that identical
order for this fixture to be a valid bit-exact oracle.

Regenerate with `python tests/fixtures/generate_daily_compounding_risk_team_python_golden.py`.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from dev_health_ops.metrics.compounding_risk import (
    DEFAULT_THRESHOLDS,
    DEFAULT_WEIGHTS,
    CompoundingInputs,
    _build_team_rows,
)

OUTPUT = Path(__file__).with_name("daily_compounding_risk_team_python_golden.json")

ORG_ID = "org-compounding-team-golden"
DAY = date(2026, 8, 24)
COMPUTED_AT = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)

REPO = "00000000-0000-4000-8000-0000000000"


def _repo(suffix: str) -> str:
    return str(UUID(REPO + suffix))


# (repo_id_suffix, team_id_or_None, inputs). team_id=None means "present in
# repo_inputs but unresolved" -- excluded from repo_to_team entirely, the
# same shape CompoundingRiskTeamExecutor produces for a repo teamresolve
# could not attribute. Ordered by repo_id suffix ascending -- see the module
# docstring for why this order is load-bearing.
CASES: list[tuple[str, str | None, CompoundingInputs]] = [
    # team-alpha: two complete repos, ordinary mean.
    (
        "01",
        "team-alpha",
        CompoundingInputs(
            rework_churn=0.10,
            complexity_delta=0.04,
            review_latency_p90h=10.0,
            single_owner_ratio=0.30,
            ownership_gini=0.20,
            bus_factor=3.0,
        ),
    ),
    (
        "02",
        "team-alpha",
        CompoundingInputs(
            rework_churn=0.20,
            complexity_delta=0.06,
            review_latency_p90h=14.0,
            single_owner_ratio=0.40,
            ownership_gini=0.30,
            bus_factor=5.0,
        ),
    ),
    # Unresolved: present in repo_inputs, absent from repo_to_team. Must not
    # create a phantom team and must not perturb team-alpha's mean.
    (
        "03",
        None,
        CompoundingInputs(
            rework_churn=0.99,
            complexity_delta=0.99,
            review_latency_p90h=99.0,
            single_owner_ratio=0.99,
            ownership_gini=0.99,
            bus_factor=9.0,
        ),
    ),
    # team-beta: three repos (a >2-summand mean per component), and the SECOND
    # repo is missing rework_churn -- _mean_or_none skips it for that
    # component only, not the whole row.
    (
        "04",
        "team-beta",
        CompoundingInputs(
            rework_churn=0.05,
            complexity_delta=0.02,
            review_latency_p90h=8.0,
            single_owner_ratio=0.15,
            ownership_gini=0.10,
            bus_factor=2.0,
        ),
    ),
    (
        "05",
        "team-beta",
        CompoundingInputs(
            rework_churn=None,
            complexity_delta=0.03,
            review_latency_p90h=9.0,
            single_owner_ratio=0.18,
            ownership_gini=0.12,
            bus_factor=4.0,
        ),
    ),
    (
        "06",
        "team-beta",
        CompoundingInputs(
            rework_churn=0.11,
            complexity_delta=0.04,
            review_latency_p90h=11.0,
            single_owner_ratio=0.22,
            ownership_gini=0.14,
            bus_factor=6.0,
        ),
    ),
    # team-gamma: every repo lacks BOTH ownership signals -- the team's
    # ownership_norm (and therefore the score) must be None, same as a repo
    # row would be.
    (
        "07",
        "team-gamma",
        CompoundingInputs(
            rework_churn=0.08,
            complexity_delta=0.03,
            review_latency_p90h=6.0,
            single_owner_ratio=None,
            ownership_gini=None,
            bus_factor=None,
        ),
    ),
    (
        "08",
        "team-gamma",
        CompoundingInputs(
            rework_churn=0.09,
            complexity_delta=0.05,
            review_latency_p90h=7.0,
            single_owner_ratio=None,
            ownership_gini=None,
            bus_factor=None,
        ),
    ),
    # team-solo: a single-repo team -- the mean must equal that repo's own
    # values verbatim.
    (
        "09",
        "team-solo",
        CompoundingInputs(
            rework_churn=0.17,
            complexity_delta=0.07,
            review_latency_p90h=13.0,
            single_owner_ratio=0.28,
            ownership_gini=0.19,
            bus_factor=1.0,
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
    repo_inputs = {_repo(suffix): inputs for suffix, _team, inputs in CASES}
    repo_to_team = {
        _repo(suffix): team for suffix, team, _inputs in CASES if team is not None
    }
    records = _build_team_rows(
        day=DAY,
        org_id=ORG_ID,
        repo_inputs=repo_inputs,
        repo_to_team=repo_to_team,
        computed_at=COMPUTED_AT,
        weights=DEFAULT_WEIGHTS,
        thresholds=DEFAULT_THRESHOLDS,
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
