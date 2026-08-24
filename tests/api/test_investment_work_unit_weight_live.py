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
