"""Unit tests for the work-UNIT team-attribution resolver (CHAOS-2600 CS7).

The SQL does the cross-id-space join + precedence collapse; these tests pin the
Python mapping and the query SHAPE (org scoping, latest-complete-run guard, the
node_id=work_item_id join key, the source-precedence CASE, latest-compute
snapshot, and filter wiring). The actual aggregation correctness is proven in
``tests/test_work_unit_attribution_live.py`` against real ClickHouse, because a
mock that returns pre-aggregated rows cannot exercise the GROUP BY / argMin.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.outputs import (
    TeamAttributionConfidence,
    TeamAttributionSource,
)
from dev_health_ops.api.graphql.resolvers.team_attribution import (
    resolve_work_unit_team_attributions,
)

_QUERY_DICTS = "dev_health_ops.api.queries.client.query_dicts"

# Realistic, DISTINCT id shapes so a matching-id mock can never paper over the
# work_unit_id-vs-work_item_id bug again: a unit id is a 64-char content hash, a
# member work item id is a provider key.
_UNIT_ID = "fb3a1e0094" + "0" * 54
_WORK_ITEM_ID = "linear:CHAOS-1053"


class MockClient:
    pass


@pytest.fixture
def mock_context():
    return GraphQLContext(
        org_id="test-org",
        db_url="clickhouse://localhost:8123/default",
        client=MockClient(),
    )


def _row(
    work_unit_id: str = _UNIT_ID,
    team_id: str | None = "CHAOS",
    team_name: str | None = "Fullchaos",
    source: str = "native_team",
    confidence: str = "high",
    member_count: int = 5,
) -> dict[str, Any]:
    return {
        "work_unit_id": work_unit_id,
        "team_id": team_id,
        "team_name": team_name,
        "source": source,
        "confidence": confidence,
        "member_count": member_count,
    }


@pytest.mark.asyncio
async def test_maps_row_to_unit_attribution(mock_context):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [_row()]
        result = await resolve_work_unit_team_attributions(
            mock_context, work_unit_ids=[_UNIT_ID]
        )
    assert len(result) == 1
    attr = result[0]
    assert attr.work_unit_id == _UNIT_ID
    assert attr.team_id == "CHAOS"
    assert attr.team_name == "Fullchaos"
    assert attr.source == TeamAttributionSource.NATIVE_TEAM
    assert attr.confidence == TeamAttributionConfidence.HIGH
    # is_primary is always True — this IS the unit's selected team.
    assert attr.is_primary is True
    assert attr.member_count == 5
    # Evidence is synthesised from the count + team + source.
    assert "5 member work item(s)" in attr.evidence
    assert "Fullchaos" in attr.evidence
    assert "native_team" in attr.evidence


@pytest.mark.asyncio
async def test_null_team_becomes_none_and_evidence_degrades(mock_context):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [
            _row(team_id=None, team_name=None, source="unassigned", confidence="none")
        ]
        result = await resolve_work_unit_team_attributions(mock_context)
    assert result[0].team_id is None
    assert result[0].team_name is None
    assert result[0].source == TeamAttributionSource.UNASSIGNED
    assert "no team" in result[0].evidence


@pytest.mark.asyncio
async def test_unknown_enum_values_degrade_safely(mock_context):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [
            _row(source="some_future_source", confidence="ultra")
        ]
        result = await resolve_work_unit_team_attributions(mock_context)
    assert result[0].source == TeamAttributionSource.UNASSIGNED
    assert result[0].confidence == TeamAttributionConfidence.NONE


@pytest.mark.asyncio
async def test_query_joins_id_spaces_scoped_and_bounded(mock_context):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = []
        await resolve_work_unit_team_attributions(
            mock_context, work_unit_ids=[_UNIT_ID], team_id="CHAOS"
        )
    _client, sql, params = mock_query.call_args.args
    # The cross-id-space join that the CS7 bug got wrong: membership node_id to
    # attribution work_item_id (NOT work_unit_id to work_item_id).
    assert "ON m.node_id = a.work_item_id" in sql
    # Latest COMPLETE membership run guard via the SHARED protocol (CHAOS-2433):
    # argMax marker, the seeded '__legacy__' branch (migration 048 — migrated
    # tenants), and the empty-string guard (no complete run -> no membership). A
    # plain run_id=argMax reimplementation drops badges for migrated orgs.
    assert "work_unit_membership_runs" in sql
    assert "argMax(run_id, completed_at)" in sql
    assert "__legacy__" in sql
    assert "latest_run.latest_run_id != ''" in sql
    # Attribution read mirrors the per-item resolver: FINAL + latest-compute
    # snapshot so a re-org's retired candidate rows can't surface.
    assert "work_item_team_attributions FINAL" in sql
    assert "is_primary = 1" in sql
    assert "max(computed_at)" in sql
    # Source precedence collapse + org scope + caller filters.
    assert "multiIf(" in sql
    assert "org_id = %(org_id)s" in sql
    assert "m.work_unit_id IN %(work_unit_ids)s" in sql
    assert "team_id = %(team_id)s" in sql
    assert "LIMIT %(limit)s" in sql
    assert params["org_id"] == "test-org"
    assert params["work_unit_ids"] == [_UNIT_ID]
    assert params["team_id"] == "CHAOS"


@pytest.mark.asyncio
async def test_team_filter_applies_to_winner_not_snapshot(mock_context):
    # The team filter must constrain the FINAL per-unit winner, never the
    # latest-compute snapshot subquery (else a re-org that moved a unit off a team
    # could be masked by an old same-team row) — mirrors the per-item rule.
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = []
        await resolve_work_unit_team_attributions(mock_context, team_id="CHAOS")
    _client, sql, _params = mock_query.call_args.args
    # Isolate the latest-compute snapshot SUBQUERY by its distinctive header
    # (anchoring on plain "max(computed_at)" is ambiguous — the legacy run join
    # also uses it). The team filter is applied at the outermost per-unit level, so
    # it must NOT appear inside this subquery.
    anchor = "SELECT work_item_id, max(computed_at)"
    assert anchor in sql
    snapshot_subquery = sql.split(anchor, 1)[1].split("GROUP BY work_item_id", 1)[0]
    assert "team_id" not in snapshot_subquery
    # But it IS present overall — on the final winner.
    assert "team_id = %(team_id)s" in sql
