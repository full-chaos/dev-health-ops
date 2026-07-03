"""Live-ClickHouse behavioral proof for CHAOS-2833.

The Investment Sankey previously resolved `TEAM` labels by joining
`work_item_cycle_times`, which does not carry every provider's work items (it
is a cycle-time rollup, not the authoritative attribution table). Work items
with a primary row in `work_item_team_attributions` but no matching
`work_item_cycle_times` row collapsed to the false `TEAM:unassigned` node even
though attribution existed -- the exact false-unassigned gap described in the
CHAOS-2833 evidence (189 work units, `teamCoverage` ~0.86 instead of ~1.0).

This file seeds ONLY `work_item_team_attributions` (never `work_item_cycle_times`)
for a work item referenced by a work unit's `structural_evidence_json.issues`,
and proves against REAL ClickHouse that:

* the REST investment Sankey/coverage fetchers (`api/queries/investment.py`)
  resolve the attributed team, not 'unassigned', and `missing_team` is zero;
* the GraphQL Sankey compiler (`api/graphql/sql/compiler.py`) resolves the
  SAME attributed team label for the TEAM dimension (REST/GraphQL parity);
* the work item id is Jira-shaped (`PROJ-42`), not Linear-shaped, proving the
  join is provider-agnostic (org-scoped `work_item_id` only, no provider
  filter) per the platform's provider x entity coverage contract.

Mock-based SQL-shape tests (`test_investment_team_attribution_sql.py`,
`graphql/test_graphql_investment_team_attribution_sql.py`) only string-match
the compiled SQL; only a live engine proves the join actually resolves rows
end-to-end (mirrors the argMax-proof rationale in `ops/AGENTS.md`).

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``. Provision an
ISOLATED scratch DB first, e.g.::

    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "CREATE DATABASE IF NOT EXISTS ci_live_2833"
    CLICKHOUSE_URI=clickhouse://ch:ch@localhost:8123/ci_live_2833 \\
        .venv/bin/python -m pytest tests/api/test_investment_team_attribution_live.py -m clickhouse
    docker exec dev-health-clickhouse-1 clickhouse-client --query \\
        "DROP DATABASE ci_live_2833"
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pytest

import dev_health_ops.api.queries.investment as investment_queries
from dev_health_ops.metrics.schemas import WorkItemTeamAttributionRecord

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason=(
            "Requires CLICKHOUSE_URI pointed at an ISOLATED scratch DB, e.g. "
            "clickhouse://ch:ch@localhost:8123/ci_live_2833"
        ),
    ),
]

FROM_TS = datetime(2026, 1, 5, tzinfo=timezone.utc)
TO_TS = datetime(2026, 1, 6, tzinfo=timezone.utc)
COMPUTED_AT = datetime(2026, 1, 7, tzinfo=timezone.utc)
# Query window strictly containing [FROM_TS, TO_TS).
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 2, 1, tzinfo=timezone.utc)
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 2, 1)

# Jira-shaped, deliberately NOT Linear -- non-Linear work items carry
# `native_team_key=None`, so this proves the join resolves via the
# autoimport-populated `work_item_team_attributions` row regardless of
# provider (never Linear-only coverage; see AGENTS.md provider x entity
# contract).
ATTRIBUTED_ISSUE_ID = "PROJ-42"
ATTRIBUTED_TEAM_NAME = "Attributed Team"
STALE_ISSUE_ID = "PROJ-STALE-42"


def _scratch_db() -> str:
    assert CLICKHOUSE_URI is not None
    return (urlparse(CLICKHOUSE_URI).path or "").lstrip("/")


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    # Safety rule (repo policy): ``ensure_schema(force=True)`` rebuilds tables,
    # so this live test must NEVER touch the real local ``default`` database.
    db = _scratch_db()
    if db in ("", "default"):
        pytest.skip(
            "refusing to run against the 'default' database; point CLICKHOUSE_URI "
            "at an isolated scratch DB (e.g. .../ci_live_2833)"
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


def _seed_attributed_work_unit(sink: Any, org: str, *, repo_id: uuid.UUID) -> None:
    feature = {"Feature Delivery.product": 1.0}
    evidence = f'{{"issues": ["{ATTRIBUTED_ISSUE_ID}"]}}'

    sink.client.insert(
        "work_unit_investments",
        [
            [
                "wu-attr",
                FROM_TS,
                TO_TS,
                None,
                "fte_days",
                100.0,
                feature,
                evidence,
                COMPUTED_AT,
                org,
            ]
        ],
        column_names=_wui_cols(),
    )

    # CHAOS-2833: seed ONLY the primary attribution row -- deliberately no
    # `work_item_cycle_times` row for this work item -- so a pass here can
    # only be explained by the fix reading `work_item_team_attributions`.
    sink.write_work_item_team_attributions(
        [
            WorkItemTeamAttributionRecord(
                work_item_id=ATTRIBUTED_ISSUE_ID,
                provider="jira",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=attributed-team",
                computed_at=COMPUTED_AT,
                repo_id=repo_id,
                team_id="team-attributed",
                team_name=ATTRIBUTED_TEAM_NAME,
                org_id=org,
            )
        ]
    )


def _seed_unassigned_latest_work_unit(
    sink: Any, org: str, *, repo_id: uuid.UUID
) -> None:
    feature = {"Feature Delivery.product": 1.0}
    evidence = f'{{"issues": ["{STALE_ISSUE_ID}"]}}'

    sink.client.insert(
        "work_unit_investments",
        [
            [
                "wu-stale-clear",
                FROM_TS,
                TO_TS,
                None,
                "fte_days",
                100.0,
                feature,
                evidence,
                COMPUTED_AT,
                org,
            ]
        ],
        column_names=_wui_cols(),
    )

    sink.write_work_item_team_attributions(
        [
            WorkItemTeamAttributionRecord(
                work_item_id=STALE_ISSUE_ID,
                provider="jira",
                source="native_team",
                is_primary=1,
                confidence="high",
                evidence="native_team_key=old-team",
                computed_at=COMPUTED_AT,
                repo_id=repo_id,
                team_id="old-team",
                team_name="Old Team",
                org_id=org,
            ),
            WorkItemTeamAttributionRecord(
                work_item_id=STALE_ISSUE_ID,
                provider="jira",
                source="unassigned",
                is_primary=1,
                confidence="low",
                evidence="latest snapshot has no owning team",
                computed_at=datetime(2026, 1, 8, tzinfo=timezone.utc),
                repo_id=repo_id,
                team_id=None,
                team_name=None,
                org_id=org,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_rest_investment_sankey_resolves_primary_attribution_not_cycle_times(
    sink,
):
    org = f"test-chaos-2833-rest-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    try:
        _seed_attributed_work_unit(sink, org, repo_id=repo_id)

        team_edges = await investment_queries.fetch_investment_team_edges(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        by_team = {row["target"]: float(row["value"]) for row in team_edges}
        assert by_team.get(ATTRIBUTED_TEAM_NAME) == pytest.approx(100.0), by_team
        assert "unassigned" not in by_team, by_team

        repo_team_rows = await investment_queries.fetch_investment_repo_team_edges(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        assert repo_team_rows, repo_team_rows
        assert all(row["team"] == ATTRIBUTED_TEAM_NAME for row in repo_team_rows), (
            repo_team_rows
        )

        counts = await investment_queries.fetch_investment_unassigned_counts(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        assert counts["missing_team"] == 0, counts
    finally:
        _cleanup(sink, org)


@pytest.mark.asyncio
async def test_rest_investment_sankey_uses_latest_primary_tuple_with_null_team(
    sink,
):
    org = f"test-chaos-2833-null-latest-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    try:
        _seed_unassigned_latest_work_unit(sink, org, repo_id=repo_id)

        team_edges = await investment_queries.fetch_investment_team_edges(
            sink,
            start_ts=START,
            end_ts=END,
            scope_filter="",
            scope_params={},
            org_id=org,
        )
        by_team = {row["target"]: float(row["value"]) for row in team_edges}
        assert by_team.get("unassigned") == pytest.approx(100.0), by_team
        assert "Old Team" not in by_team, by_team
    finally:
        _cleanup(sink, org)


@pytest.mark.asyncio
async def test_graphql_sankey_compiler_resolves_same_primary_attribution(sink):
    """REST/GraphQL parity: compile_sankey's TEAM join must resolve the SAME
    attributed team label as the REST fetchers above, from the SAME fixture.
    """
    from dev_health_ops.api.graphql.sql.compiler import SankeyRequest, compile_sankey
    from dev_health_ops.api.queries.client import query_dicts

    org = f"test-chaos-2833-graphql-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    try:
        _seed_attributed_work_unit(sink, org, repo_id=repo_id)

        nodes_qs, edges_qs = compile_sankey(
            SankeyRequest(
                path=["theme", "team"],
                measure="count",
                start_date=START_DATE,
                end_date=END_DATE,
                use_investment=True,
            ),
            org_id=org,
        )

        node_ids_by_dim: dict[str, set[str]] = {}
        for sql, params in nodes_qs:
            for row in await query_dicts(sink, sql, params):
                node_ids_by_dim.setdefault(str(row["dimension"]), set()).add(
                    str(row["node_id"])
                )

        team_nodes = node_ids_by_dim.get("TEAM", set())
        assert ATTRIBUTED_TEAM_NAME in team_nodes, node_ids_by_dim
        assert "unassigned" not in team_nodes, node_ids_by_dim

        edge_targets: set[str] = set()
        for sql, params in edges_qs:
            for row in await query_dicts(sink, sql, params):
                if str(row["target_dimension"]) == "TEAM":
                    edge_targets.add(str(row["target"]))

        assert ATTRIBUTED_TEAM_NAME in edge_targets, edge_targets
        assert "unassigned" not in edge_targets, edge_targets
    finally:
        _cleanup(sink, org)
