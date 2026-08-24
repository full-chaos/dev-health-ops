"""Live-ClickHouse acceptance proof for CHAOS-4241.

chris's ruling: default investment weight is a COUNT of attributed work
units, not lines of code. Before this fix, ``Measure.COUNT`` for the
investment path (``api/graphql/sql/validate.py``) computed
``SUM(subcategory_kv.2 * effort_value)`` -- effort_value being LOC churn --
so a single 10,000-line-churn PR would dominate nine 10-line-churn PRs
~99%/1%, even though by attributed-work-unit count the split should be
10%/90%. Mock-based SQL-string tests (``tests/graphql/test_compiler.py``)
already pin the fixed expression (``SUM(subcategory_kv.2)``); only a live
engine proves the actual numeric split against real ClickHouse.

Seeds two teams via ``work_item_team_attributions``:
* Team BigPR: ONE work unit, effort_metric='churn_loc', effort_value=10000.0
* Team SmallPRs: NINE work units, effort_metric='churn_loc', effort_value=10.0 each

Asserts, against REAL ClickHouse, through the real compile -> execute
pipeline (``compile_sankey`` + ``query_dicts``):
* measure="count" (the default) -> Team BigPR gets 1.0, Team SmallPRs gets
  9.0 -> a 10%/90% share, not ~99%/1%.
* measure="churn_loc" (the explicit LOC alternative) -> the OLD ~99%/1%
  split still exists and is reachable, proving LOC was not deleted, only
  demoted to an explicit opt-in.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``. Provision an
ISOLATED scratch DB first, e.g.::

    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "CREATE DATABASE IF NOT EXISTS ci_live_4241"
    CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_live_4241 \\
        .venv/bin/python -m pytest tests/api/test_investment_work_unit_weight_live.py -m clickhouse
    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "DROP DATABASE ci_live_4241"
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pytest

from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason=(
            "Requires CLICKHOUSE_URI pointed at an ISOLATED scratch DB, e.g. "
            "clickhouse://ch:ch@localhost:8123/ci_live_4241"
        ),
    ),
]

FROM_TS = datetime(2026, 1, 5, tzinfo=timezone.utc)
TO_TS = datetime(2026, 1, 6, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 2, 1)

BIG_TEAM = "Team BigPR"
SMALL_TEAM = "Team SmallPRs"


def _scratch_db() -> str:
    assert CLICKHOUSE_URI is not None
    return (urlparse(CLICKHOUSE_URI).path or "").lstrip("/")


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    # Safety rule (repo policy): ensure_schema(force=True) rebuilds tables, so
    # this live test must NEVER touch the real local 'default' database.
    db = _scratch_db()
    if db in ("", "default"):
        pytest.skip(
            "refusing to run against the 'default' database; point CLICKHOUSE_URI "
            "at an isolated scratch DB (e.g. .../ci_live_4241)"
        )
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    s.ensure_schema(force=True)
    yield s
    s.close()


def _wui_cols() -> list[str]:
    return [
        "work_unit_id",
        "from_ts",
        "to_ts",
        "repo_id",
        "effort_metric",
        "effort_value",
        "subcategory_distribution_json",
        "structural_evidence_json",
        "computed_at",
        "org_id",
    ]


def _cleanup(sink: Any, org_id: str) -> None:
    for table in (
        "work_unit_investments",
        "work_unit_repo_effort",
        "work_item_cycle_times",
        "work_item_team_attributions",
    ):
        sink.client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{o:String}} "
            "SETTINGS mutations_sync=2",
            parameters={"o": org_id},
        )


def _seed_ten_prs_two_teams(sink: Any, org: str) -> None:
    """One 10,000-line-churn PR (Team BigPR) + nine 10-line-churn PRs (Team SmallPRs)."""
    feature = {"Feature Delivery.product": 1.0}

    wui_rows = []
    attribution_rows: list[WorkItemTeamAttributionRecord] = []

    big_issue = "BIG-1"
    wui_rows.append(
        [
            "wu-big",
            FROM_TS,
            TO_TS,
            None,
            "churn_loc",
            10000.0,
            feature,
            f'{{"issues": ["{big_issue}"]}}',
            COMPUTED_AT,
            org,
        ]
    )
    attribution_rows.append(
        WorkItemTeamAttributionRecord(
            work_item_id=big_issue,
            provider="github",
            source="native_team",
            is_primary=1,
            confidence="high",
            evidence="native_team_key=big-pr-team",
            computed_at=COMPUTED_AT,
            repo_id=uuid.uuid4(),
            team_id="team-big",
            team_name=BIG_TEAM,
            org_id=org,
        )
    )

    for i in range(9):
        small_issue = f"SMALL-{i}"
        wui_rows.append(
            [
                f"wu-small-{i}",
                FROM_TS,
                TO_TS,
                None,
                "churn_loc",
                10.0,
                feature,
                f'{{"issues": ["{small_issue}"]}}',
                COMPUTED_AT,
                org,
            ]
        )
        attribution_rows.append(
            WorkItemTeamAttributionRecord(
                work_item_id=small_issue,
                provider="github",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=small-prs-team",
                computed_at=COMPUTED_AT,
                repo_id=uuid.uuid4(),
                team_id="team-small",
                team_name=SMALL_TEAM,
                org_id=org,
            )
        )

    sink.client.insert("work_unit_investments", wui_rows, column_names=_wui_cols())
    sink.write_work_item_team_attributions(attribution_rows)


async def _team_node_values(sink: Any, org: str, *, measure: str) -> dict[str, float]:
    from dev_health_ops.api.graphql.sql.compiler import SankeyRequest, compile_sankey
    from dev_health_ops.api.queries.client import query_dicts

    nodes_qs, _edges_qs = compile_sankey(
        SankeyRequest(
            path=["theme", "team"],
            measure=measure,
            start_date=START_DATE,
            end_date=END_DATE,
            use_investment=True,
        ),
        org_id=org,
    )

    values: dict[str, float] = {}
    for sql, params in nodes_qs:
        for row in await query_dicts(sink, sql, params):
            if str(row["dimension"]) == "TEAM":
                values[str(row["node_id"])] = float(row["value"])
    return values


async def test_default_count_measure_splits_by_work_unit_not_loc(sink):
    """CHAOS-4241 acceptance: default (COUNT) weight is a work-unit count.

    One 10,000-line-churn PR + nine 10-line-churn PRs across two teams must
    split 10%/90% by work-unit count, not ~99%/1% by LOC churn.
    """
    org = f"test-chaos-4241-count-{uuid.uuid4()}"
    try:
        _seed_ten_prs_two_teams(sink, org)

        values = await _team_node_values(sink, org, measure="count")
        assert values, "expected TEAM nodes for both teams"

        big = values.get(BIG_TEAM, 0.0)
        small = values.get(SMALL_TEAM, 0.0)
        total = big + small
        assert total > 0, values

        big_share = big / total
        small_share = small / total

        # Work-unit count: BigPR contributes exactly 1 unit, SmallPRs exactly
        # 9 units, regardless of their 10000:10 LOC churn ratio.
        assert big == pytest.approx(1.0), values
        assert small == pytest.approx(9.0), values
        assert big_share == pytest.approx(0.10, abs=0.01), values
        assert small_share == pytest.approx(0.90, abs=0.01), values

        # The regression this test guards against: the OLD behavior would
        # have put BigPR's share above 99%.
        assert big_share < 0.5, (
            "COUNT measure is still LOC-weighted -- BigPR's single PR is "
            f"dominating the split ({big_share:.1%}), same bug as CHAOS-4241"
        )
    finally:
        _cleanup(sink, org)


async def test_explicit_churn_loc_measure_still_available_as_alternative(sink):
    """LOC must remain reachable, but only via the explicit CHURN_LOC measure."""
    org = f"test-chaos-4241-loc-{uuid.uuid4()}"
    try:
        _seed_ten_prs_two_teams(sink, org)

        values = await _team_node_values(sink, org, measure="churn_loc")
        big = values.get(BIG_TEAM, 0.0)
        small = values.get(SMALL_TEAM, 0.0)
        total = big + small
        assert total > 0, values

        # LOC churn: 10000 vs 9*10=90 -> BigPR dominates, ~99.1%/0.9%.
        assert big == pytest.approx(10000.0), values
        assert small == pytest.approx(90.0), values
        big_share = big / total
        assert big_share > 0.99, (
            f"expected the explicit LOC measure to reproduce the old skew, got {big_share:.1%}"
        )
    finally:
        _cleanup(sink, org)


def _wure_cols() -> list[str]:
    return [
        "work_unit_id",
        "repo_id",
        "effort_metric",
        "effort_value",
        "allocation_weight",
        "allocation_source",
        "computed_at",
        "org_id",
    ]


def _seed_coverage_fixture(sink: Any, org: str) -> None:
    """Three work units exercising both coverage bugs codex found in review:

    * wu-single: TeamA, scalar repo (repo_single) -- one row, unambiguous.
    * wu-multi: TeamB, effort allocated 60/40 across TWO repos via
      work_unit_repo_effort -- fans out to 2 rows in the coverage query's
      `wure` LEFT JOIN. A plain count() over those rows double-counts this
      ONE work unit as two.
    * wu-none: no team attribution row at all (team unassigned), scalar
      repo_id=NULL and no allocation row (repo unassigned). The investment
      dimension's REPO column formats a missing repo as '' (never SQL NULL),
      so a naive `{repo_col} IS NOT NULL` predicate wrongly marks this row
      "assigned".

    True (fixed) expectation, weighting each work unit as exactly 1 (COUNT
    measure): team_coverage = 2/3 (wu-single, wu-multi assigned; wu-none not)
    and repo_coverage = 2/3 (same two have a real repo; wu-none does not).
    The OLD buggy coverage query would compute team_coverage = 3/4 (row-count
    over the fanned wu-multi rows) and repo_coverage = 4/4 = 1.0 (every row,
    including wu-none's, misread as repo-assigned).
    """
    feature = {"Feature Delivery.product": 1.0}
    repo_single = uuid.uuid4()
    repo_m1 = uuid.uuid4()
    repo_m2 = uuid.uuid4()

    single_issue = "SINGLE-1"
    multi_issue = "MULTI-1"
    # wu-none has NO issue reference at all -- structural_evidence_json is
    # empty, so the team-resolution join has nothing to attribute and the
    # unit resolves to 'unassigned', exactly like a genuinely untracked unit.

    sink.client.insert(
        "work_unit_investments",
        [
            [
                "wu-single",
                FROM_TS,
                TO_TS,
                repo_single,
                "churn_loc",
                50.0,
                feature,
                f'{{"issues": ["{single_issue}"]}}',
                COMPUTED_AT,
                org,
            ],
            [
                "wu-multi",
                FROM_TS,
                TO_TS,
                None,
                "churn_loc",
                100.0,
                feature,
                f'{{"issues": ["{multi_issue}"]}}',
                COMPUTED_AT,
                org,
            ],
            [
                "wu-none",
                FROM_TS,
                TO_TS,
                None,
                "churn_loc",
                20.0,
                feature,
                "{}",
                COMPUTED_AT,
                org,
            ],
        ],
        column_names=_wui_cols(),
    )

    sink.client.insert(
        "work_unit_repo_effort",
        [
            [
                "wu-multi",
                repo_m1,
                "churn_loc",
                60.0,
                0.6,
                "structural",
                COMPUTED_AT,
                org,
            ],
            [
                "wu-multi",
                repo_m2,
                "churn_loc",
                40.0,
                0.4,
                "structural",
                COMPUTED_AT,
                org,
            ],
        ],
        column_names=_wure_cols(),
    )

    sink.write_work_item_team_attributions(
        [
            WorkItemTeamAttributionRecord(
                work_item_id=single_issue,
                provider="github",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=team-a",
                computed_at=COMPUTED_AT,
                repo_id=repo_single,
                team_id="team-a",
                team_name="Team A",
                org_id=org,
            ),
            WorkItemTeamAttributionRecord(
                work_item_id=multi_issue,
                provider="github",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=team-b",
                computed_at=COMPUTED_AT,
                repo_id=repo_m1,
                team_id="team-b",
                team_name="Team B",
                org_id=org,
            ),
        ]
    )


async def _resolve_coverage(sink: Any, org: str):
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.graphql.models.inputs import (
        AnalyticsRequestInput,
        DateRangeInput,
        DimensionInput,
        MeasureInput,
        SankeyRequestInput,
    )
    from dev_health_ops.api.graphql.resolvers import analytics as analytics_resolver

    batch = AnalyticsRequestInput(
        sankey=SankeyRequestInput(
            path=[DimensionInput.TEAM, DimensionInput.REPO],
            measure=MeasureInput.COUNT,
            date_range=DateRangeInput(start_date=START_DATE, end_date=END_DATE),
            use_investment=True,
        ),
        use_investment=True,
    )
    context = GraphQLContext(org_id=org, db_url=CLICKHOUSE_URI or "", client=sink)
    result = await analytics_resolver.resolve_analytics(context, batch)
    assert result.sankey is not None and result.sankey.coverage is not None, (
        result.sankey
    )
    return result.sankey.coverage


async def test_coverage_matches_work_unit_count_not_fanned_rows(sink):
    """CHAOS-4241 codex finding (HIGH): coverage must count each work unit
    ONCE, not once per repo-allocation row, and a repo-less unit must never
    read as repo-assigned. See `_seed_coverage_fixture` for the exact fixture
    and expected math (2/3 for both cards, not 3/4 / 4/4).
    """
    org = f"test-chaos-4241-coverage-{uuid.uuid4()}"
    try:
        _seed_coverage_fixture(sink, org)

        coverage = await _resolve_coverage(sink, org)

        assert coverage.team_coverage == pytest.approx(2 / 3, abs=0.01), coverage
        assert coverage.repo_coverage == pytest.approx(2 / 3, abs=0.01), coverage

        # Guard against the specific regressions codex flagged: neither card
        # may read as "everything assigned" (the old repo_col IS NOT NULL /
        # fan-out bugs both trend toward inflated coverage).
        assert coverage.team_coverage < 0.99, coverage
        assert coverage.repo_coverage < 0.99, coverage
    finally:
        _cleanup(sink, org)


def _seed_zero_effort_fixture(sink: Any, org: str) -> None:
    """Two team-assigned work units with effort_value=0 (codex round 2):

    * wu-zero-single: ONE repo-allocation row, effort_value=0.
    * wu-zero-multi: TWO repo-allocation rows (fanned by the wure LEFT JOIN),
      effort_value=0.

    Both must still count as exactly ONE work unit each in COUNT-weighted
    coverage -- the Sankey's SUM(subcategory_kv.2) never touches
    effort_value, so it counts them as 1 regardless. Dividing
    repo_effort_value / effort_value (0) would silently drop wu-zero-single
    entirely and, without the 1/N-rows fallback, either drop or double-count
    wu-zero-multi depending on how many allocation rows it fanned into.
    """
    feature = {"Feature Delivery.product": 1.0}
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    repo_c = uuid.uuid4()

    single_issue = "ZERO-SINGLE-1"
    multi_issue = "ZERO-MULTI-1"

    sink.client.insert(
        "work_unit_investments",
        [
            [
                "wu-zero-single",
                FROM_TS,
                TO_TS,
                None,
                "churn_loc",
                0.0,
                feature,
                f'{{"issues": ["{single_issue}"]}}',
                COMPUTED_AT,
                org,
            ],
            [
                "wu-zero-multi",
                FROM_TS,
                TO_TS,
                None,
                "churn_loc",
                0.0,
                feature,
                f'{{"issues": ["{multi_issue}"]}}',
                COMPUTED_AT,
                org,
            ],
        ],
        column_names=_wui_cols(),
    )

    sink.client.insert(
        "work_unit_repo_effort",
        [
            [
                "wu-zero-single",
                repo_a,
                "churn_loc",
                0.0,
                1.0,
                "structural",
                COMPUTED_AT,
                org,
            ],
            [
                "wu-zero-multi",
                repo_b,
                "churn_loc",
                0.0,
                0.5,
                "structural",
                COMPUTED_AT,
                org,
            ],
            [
                "wu-zero-multi",
                repo_c,
                "churn_loc",
                0.0,
                0.5,
                "structural",
                COMPUTED_AT,
                org,
            ],
        ],
        column_names=_wure_cols(),
    )

    sink.write_work_item_team_attributions(
        [
            WorkItemTeamAttributionRecord(
                work_item_id=single_issue,
                provider="github",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=team-zero",
                computed_at=COMPUTED_AT,
                repo_id=repo_a,
                team_id="team-zero",
                team_name="Team Zero",
                org_id=org,
            ),
            WorkItemTeamAttributionRecord(
                work_item_id=multi_issue,
                provider="github",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=team-zero",
                computed_at=COMPUTED_AT,
                repo_id=repo_b,
                team_id="team-zero",
                team_name="Team Zero",
                org_id=org,
            ),
        ]
    )


async def test_coverage_zero_effort_units_still_count_as_one(sink):
    """CHAOS-4241 codex round 2 (HIGH): a zero-effort work unit must still
    contribute exactly 1 to COUNT-weighted coverage, whether it has one
    repo-allocation row or several (the fan-out must not double-count it
    either). See `_seed_zero_effort_fixture`.
    """
    org = f"test-chaos-4241-zero-effort-{uuid.uuid4()}"
    try:
        _seed_zero_effort_fixture(sink, org)

        coverage = await _resolve_coverage(sink, org)

        # Both units are team- and repo-assigned -> both cards must read
        # 1.0, not <1.0 (units silently dropped by the /0 division) and not
        # >1.0-equivalent skew from the multi-repo unit's fan-out.
        assert coverage.team_coverage == pytest.approx(1.0, abs=0.01), coverage
        assert coverage.repo_coverage == pytest.approx(1.0, abs=0.01), coverage
    finally:
        _cleanup(sink, org)
