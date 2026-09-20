"""Tests for the Python AI GraphQL resolver that remains (aiOpportunities).

The other AI analytics operations are served by query-api and are covered by
its Go tests; their Strawberry fields raise.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.ai import (
    AIAttributionScopeInput,
    AIOpportunity,
    AIOpportunityKind,
    AIScopeInput,
)
from dev_health_ops.api.graphql.resolvers.ai import (
    _resolve_repo_ref,
    resolve_ai_opportunities,
)

ORG_ID = "org-test"
REPO_ID = UUID("11111111-1111-1111-1111-111111111111")
TEAM_ID = "team-a"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _ctx() -> GraphQLContext:
    """Build a minimal GraphQLContext with org scoping and a stub client."""
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/default")
    ctx.client = MagicMock()
    return ctx


# -----------------------------------------------------------------------------
# aiOpportunities
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_opportunities_returns_ready_empty_contract():
    detector = MagicMock()
    detector.detect = AsyncMock(return_value=[])
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.AIOpportunityDetector",
        return_value=detector,
    ):
        result = await resolve_ai_opportunities(_ctx())

    assert result.org_id == ORG_ID
    assert result.recommendations == []
    # CHAOS-2188: detector_ready signals the detector ran successfully, NOT that
    # it found candidates.  Empty recommendations is valid; False would mislead
    # the frontend into showing "not connected".
    assert result.detector_ready is True


@pytest.mark.asyncio
async def test_opportunities_delegates_scope_limit_and_returns_evidence():
    opportunity = AIOpportunity(
        opportunity_id="stable-id",
        kind=AIOpportunityKind.HIGH_REWORK,
        repo_id=str(REPO_ID),
        team_id=TEAM_ID,
        title="High AI rework in repo",
        rationale="AI-assisted PRs had a 33% rework rate vs 10% for human PRs.",
        score=0.8,
        evidence_refs=[f"ai_impact_metrics_daily:rework_rate:{REPO_ID}"],
        work_graph_drilldowns=[],
    )
    detector = MagicMock()
    detector.detect = AsyncMock(return_value=[opportunity])
    scope = AIScopeInput(repo_id=str(REPO_ID), team_id=TEAM_ID)
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.AIOpportunityDetector",
        return_value=detector,
    ):
        result = await resolve_ai_opportunities(_ctx(), scope=scope, limit=1)

    detector.detect.assert_awaited_once_with(org_id=ORG_ID, scope=scope, limit=1)
    assert result.detector_ready is True
    assert result.recommendations == [opportunity]
    assert result.recommendations[0].evidence_refs


@pytest.mark.asyncio
async def test_opportunities_unresolved_repo_slug_returns_empty_without_detector():
    detector_cls = MagicMock()
    scope = AIScopeInput(repo_id="full-chaos/missing-repo")
    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.ai.AIOpportunityDetector",
            detector_cls,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_ref",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        result = await resolve_ai_opportunities(_ctx(), scope=scope)

    detector_cls.assert_not_called()
    assert result.recommendations == []
    assert result.detector_ready is True


@pytest.mark.asyncio
async def test_opportunities_resolves_repo_slug_before_detector():
    detector = MagicMock()
    detector.detect = AsyncMock(return_value=[])
    scope = AIScopeInput(repo_id="full-chaos/dev-health-ops", team_id=TEAM_ID)
    with (
        patch(
            "dev_health_ops.api.graphql.resolvers.ai.AIOpportunityDetector",
            return_value=detector,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_ref",
            new_callable=AsyncMock,
            return_value=REPO_ID,
        ) as mock_resolve,
    ):
        await resolve_ai_opportunities(_ctx(), scope=scope, limit=7)

    mock_resolve.assert_awaited_once()
    call_scope = detector.detect.await_args.kwargs["scope"]
    assert call_scope.repo_id == str(REPO_ID)
    assert call_scope.team_id == TEAM_ID
    assert detector.detect.await_args.kwargs["limit"] == 7


# -----------------------------------------------------------------------------
# repo reference resolution
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_repo_ref_accepts_slug_from_filter_options():
    ctx = _ctx()
    slug = "full-chaos/dev-health-ops"
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.query_dicts",
        new_callable=AsyncMock,
        return_value=[{"id": str(REPO_ID)}],
    ) as mock_query:
        resolved = await _resolve_repo_ref(ctx.client, ORG_ID, slug)

    assert resolved == REPO_ID
    await_args = mock_query.await_args
    assert await_args is not None
    query = await_args.args[1]
    params = await_args.args[2]
    assert "SELECT id" in query
    assert "FROM repos" in query
    assert "org_id = {org_id:String}" in query
    assert "repo = {slug:String}" in query
    assert "ORDER BY toString(id)" in query
    assert params == {"org_id": ORG_ID, "slug": slug}


@pytest.mark.asyncio
async def test_resolve_repo_ref_duplicate_slug_rows_use_query_order():
    ctx = _ctx()
    slug = "full-chaos/dev-health-ops"
    other_repo_id = UUID("22222222-2222-2222-2222-222222222222")
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.query_dicts",
        new_callable=AsyncMock,
        return_value=[{"id": str(REPO_ID)}, {"id": str(other_repo_id)}],
    ) as mock_query:
        resolved = await _resolve_repo_ref(ctx.client, ORG_ID, slug)

    await_args = mock_query.await_args
    assert await_args is not None
    assert "ORDER BY toString(id)" in await_args.args[1]
    assert resolved == REPO_ID


@pytest.mark.asyncio
async def test_resolve_repo_ref_uuid_bypasses_lookup():
    ctx = _ctx()
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.query_dicts", new_callable=AsyncMock
    ) as mock_query:
        resolved = await _resolve_repo_ref(ctx.client, ORG_ID, str(REPO_ID))

    assert resolved == REPO_ID
    mock_query.assert_not_awaited()


# -----------------------------------------------------------------------------
# CHAOS-2188 — AI-23: detector_ready reflects real candidate availability
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_detector_ready_true_when_no_recommendations():
    """detector_ready must be True even when the detector finds no candidates.

    An empty result means "no opportunities right now", not "detector broken".
    Setting it False would cause the frontend to display a misleading
    "detector not connected" state after a clean but empty run.
    """
    detector = MagicMock()
    detector.detect = AsyncMock(return_value=[])
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.AIOpportunityDetector",
        return_value=detector,
    ):
        result = await resolve_ai_opportunities(_ctx())

    assert result.detector_ready is True


@pytest.mark.asyncio
async def test_detector_ready_true_when_recommendations_exist():
    """detector_ready must be True when the detector finds real candidates."""
    opportunity = AIOpportunity(
        opportunity_id="abc123",
        kind=AIOpportunityKind.HIGH_REVIEW_LOAD,
        repo_id=str(REPO_ID),
        team_id=None,
        title="High review load",
        rationale="ratio > 1.5",
        score=0.7,
        evidence_refs=["ai_impact_metrics_daily:reviews_per_pr:repo-1"],
        work_graph_drilldowns=[],
    )
    detector = MagicMock()
    detector.detect = AsyncMock(return_value=[opportunity])
    with patch(
        "dev_health_ops.api.graphql.resolvers.ai.AIOpportunityDetector",
        return_value=detector,
    ):
        result = await resolve_ai_opportunities(_ctx())

    assert result.detector_ready is True
    assert len(result.recommendations) == 1


# -----------------------------------------------------------------------------
# CHAOS-2744 -- aiAttributionOverview
# -----------------------------------------------------------------------------


def test_ai_attribution_scope_input_does_not_expose_work_type():
    """CHAOS-2744 (Oracle NO-GO): ai_attribution_resolved has no work_type
    column, so AIAttributionScopeInput must not expose the field at all --
    accepting-and-ignoring it was the original bug (silent no-op filter).
    Use the shared AIScopeInput (which does carry work_type) for queries
    backed by tables that actually have that column.
    """
    with pytest.raises(TypeError):
        AIAttributionScopeInput(work_type="bug")  # type: ignore[call-arg]
