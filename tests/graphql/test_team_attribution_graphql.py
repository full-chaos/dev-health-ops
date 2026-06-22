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
    resolve_work_item_team_attributions,
)

_QUERY_DICTS = "dev_health_ops.api.queries.client.query_dicts"


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
    work_item_id: str = "linear:CHAOS-1",
    provider: str = "linear",
    team_id: str | None = "team-a",
    team_name: str | None = "Team A",
    source: str = "native_team",
    confidence: str = "high",
    is_primary: int = 1,
    evidence: str = "native_team_key=CHAOS",
) -> dict[str, Any]:
    return {
        "work_item_id": work_item_id,
        "provider": provider,
        "team_id": team_id,
        "team_name": team_name,
        "source": source,
        "confidence": confidence,
        "is_primary": is_primary,
        "evidence": evidence,
    }


@pytest.mark.asyncio
async def test_maps_rows_to_provenance(mock_context):
    rows = [
        _row(source="native_team", confidence="high", is_primary=1),
        _row(
            source="manual_fallback",
            confidence="manual",
            is_primary=0,
            evidence="scope_type=repo",
        ),
    ]
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = rows
        result = await resolve_work_item_team_attributions(
            mock_context, work_item_ids=["linear:CHAOS-1"]
        )
    assert len(result) == 2
    assert result[0].source == TeamAttributionSource.NATIVE_TEAM
    assert result[0].confidence == TeamAttributionConfidence.HIGH
    assert result[0].is_primary is True
    assert result[0].team_id == "team-a"
    assert result[1].source == TeamAttributionSource.MANUAL_FALLBACK
    assert result[1].confidence == TeamAttributionConfidence.MANUAL
    assert result[1].is_primary is False
    assert result[1].evidence == "scope_type=repo"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ch_value,expected",
    [
        ("native_team", TeamAttributionSource.NATIVE_TEAM),
        ("issue_project", TeamAttributionSource.ISSUE_PROJECT),
        ("project_ownership", TeamAttributionSource.PROJECT_OWNERSHIP),
        ("repo_ownership", TeamAttributionSource.REPO_OWNERSHIP),
        ("assignee_membership", TeamAttributionSource.ASSIGNEE_MEMBERSHIP),
        ("linked_issue", TeamAttributionSource.LINKED_ISSUE),
        ("manual_fallback", TeamAttributionSource.MANUAL_FALLBACK),
        ("unassigned", TeamAttributionSource.UNASSIGNED),
    ],
)
async def test_every_source_enum_maps(mock_context, ch_value, expected):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [_row(source=ch_value)]
        result = await resolve_work_item_team_attributions(mock_context)
    assert result[0].source == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ch_value,expected",
    [
        ("high", TeamAttributionConfidence.HIGH),
        ("medium", TeamAttributionConfidence.MEDIUM),
        ("low", TeamAttributionConfidence.LOW),
        ("manual", TeamAttributionConfidence.MANUAL),
        ("none", TeamAttributionConfidence.NONE),
    ],
)
async def test_every_confidence_enum_maps(mock_context, ch_value, expected):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [_row(confidence=ch_value)]
        result = await resolve_work_item_team_attributions(mock_context)
    assert result[0].confidence == expected


@pytest.mark.asyncio
async def test_unknown_enum_values_degrade_safely(mock_context):
    # A future ClickHouse enum value the API predates must not 500 the query.
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [
            _row(source="some_future_source", confidence="ultra")
        ]
        result = await resolve_work_item_team_attributions(mock_context)
    assert result[0].source == TeamAttributionSource.UNASSIGNED
    assert result[0].confidence == TeamAttributionConfidence.NONE


@pytest.mark.asyncio
async def test_null_team_id_becomes_none(mock_context):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = [
            _row(team_id=None, team_name=None, source="unassigned", confidence="none")
        ]
        result = await resolve_work_item_team_attributions(mock_context)
    assert result[0].team_id is None
    assert result[0].team_name is None


@pytest.mark.asyncio
async def test_query_is_org_scoped_final_and_bounded(mock_context):
    with patch(_QUERY_DICTS, new_callable=AsyncMock) as mock_query:
        mock_query.return_value = []
        await resolve_work_item_team_attributions(
            mock_context, work_item_ids=["a", "b"], team_id="team-x"
        )
    _client, sql, params = mock_query.call_args.args
    assert "work_item_team_attributions FINAL" in sql
    assert "org_id = %(org_id)s" in sql
    assert "work_item_id IN %(work_item_ids)s" in sql
    assert "team_id = %(team_id)s" in sql
    assert "LIMIT %(limit)s" in sql
    # Latest-compute snapshot guard: stale (team_id, source) candidate rows from an
    # older compute must not surface as a second is_primary / stale source
    # (CHAOS-2605 codex finding). The team filter must NOT scope the snapshot.
    assert "max(computed_at)" in sql
    assert "GROUP BY work_item_id" in sql
    snapshot = sql.split("max(computed_at)", 1)[1]
    assert "team_id = %(team_id)s" not in snapshot
    assert params["org_id"] == "test-org"
    assert params["work_item_ids"] == ["a", "b"]
    assert params["team_id"] == "team-x"
