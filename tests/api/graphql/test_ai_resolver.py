"""Contract tests for AI workflow analytics GraphQL resolvers.

Three states are exercised for every resolver:

* **empty** — the loader returns no rows.  The contract must still
  populate every required field, never raise, and set
  ``data_available=False``.
* **partial** — only some buckets have data (e.g. AI work landed but no
  human baseline yet).  The contract must surface the populated bucket
  rows and leave the missing side empty rather than synthesising values.
* **populated** — every bucket has rollup rows.  The contract must
  aggregate weighted averages, emit deltas, and tag
  ``data_available=True``.

The tests stub the ClickHouse-backed loaders so they can run in unit
mode without a ClickHouse instance.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.ai import (
    AIAttributionBucketInput,
    AIAttributionScopeInput,
    AIDateRangeInput,
    AIOpportunity,
    AIOpportunityKind,
    AIScopeInput,
    AIWorkflowRootTypeInput,
)
from dev_health_ops.api.graphql.resolvers.ai import (
    resolve_ai_attributed_prs,
    resolve_ai_attribution_overview,
    resolve_ai_comparison,
    resolve_ai_governance_summary,
    resolve_ai_impact_summary,
    resolve_ai_opportunities,
    resolve_ai_review_load,
    resolve_ai_risk_breakdown,
    resolve_ai_workflow_drilldown,
)
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.metrics.ai_impact import AttributionBucket
from dev_health_ops.metrics.schemas import (
    AIImpactMetricsDailyRecord,
    AIOperatingLeverageComponents,
)

ORG_ID = "org-test"
REPO_ID = UUID("11111111-1111-1111-1111-111111111111")
TEAM_ID = "team-a"
DAY_START = date(2026, 5, 1)
DAY_END = date(2026, 5, 7)
COMPUTED_AT = datetime(2026, 5, 7, 12, tzinfo=timezone.utc)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _ctx() -> GraphQLContext:
    """Build a minimal GraphQLContext with org scoping and a stub client."""
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/default")
    ctx.client = MagicMock()
    return ctx


def _leverage(prs_component: float = 0.1) -> AIOperatingLeverageComponents:
    return AIOperatingLeverageComponents(
        prs_component=prs_component,
        cycle_time_component=0.05,
        review_component=-0.02,
        rework_component=-0.01,
        test_component=-0.005,
        incident_component=-0.0,
    )


def _record(
    *,
    bucket: AttributionBucket,
    day: date,
    prs_total: int = 10,
    prs_merged: int = 8,
    ai_assisted_prs: int = 0,
    agent_created_prs: int = 0,
    human_prs: int = 0,
    unknown_prs: int = 0,
    cycle_time_avg_hours: float | None = 24.0,
    reviews_per_pr: float | None = 1.5,
    rework_drag_rate: float | None = 0.1,
    revert_rate: float | None = 0.05,
    incident_drag_rate: float | None = 0.02,
    test_gap_rate: float | None = 0.2,
    ai_review_amplification: float | None = None,
    ai_cycle_time_delta_hours: float | None = None,
    ai_assisted_pr_ratio: float | None = None,
    followup_commits_count: int = 0,
) -> AIImpactMetricsDailyRecord:
    return AIImpactMetricsDailyRecord(
        org_id=ORG_ID,
        team_id=TEAM_ID,
        repo_id=REPO_ID,
        work_type="pull_request",
        day=day,
        attribution_bucket=bucket.value,
        prs_total=prs_total,
        prs_merged=prs_merged,
        ai_assisted_prs=ai_assisted_prs,
        agent_created_prs=agent_created_prs,
        human_prs=human_prs,
        unknown_prs=unknown_prs,
        ai_assisted_pr_ratio=ai_assisted_pr_ratio,
        agent_created_pr_count=agent_created_prs,
        cycle_time_avg_hours=cycle_time_avg_hours,
        baseline_cycle_time_avg_hours=None,
        ai_cycle_time_delta_hours=ai_cycle_time_delta_hours,
        reviews_per_pr=reviews_per_pr,
        baseline_reviews_per_pr=None,
        ai_review_amplification=ai_review_amplification,
        changes_requested_per_pr=0.3,
        rework_prs=int(prs_total * (rework_drag_rate or 0)),
        rework_drag_rate=rework_drag_rate,
        followup_commits_count=followup_commits_count,
        revert_prs=int(prs_total * (revert_rate or 0)),
        revert_rate=revert_rate,
        incidents_count=int(prs_total * (incident_drag_rate or 0)),
        incident_drag_rate=incident_drag_rate,
        test_gap_prs=int(prs_total * (test_gap_rate or 0)),
        test_gap_rate=test_gap_rate,
        leverage=_leverage(),
        computed_at=COMPUTED_AT,
    )


def _populated_rows() -> list[AIImpactMetricsDailyRecord]:
    """One row per bucket on a single day."""
    return [
        _record(
            bucket=AttributionBucket.AI_ASSISTED,
            day=DAY_START,
            prs_total=20,
            prs_merged=16,
            ai_assisted_prs=20,
            ai_assisted_pr_ratio=0.5,
            ai_review_amplification=1.4,
            ai_cycle_time_delta_hours=-3.0,
            followup_commits_count=6,
        ),
        _record(
            bucket=AttributionBucket.AGENT_CREATED,
            day=DAY_START,
            agent_created_prs=4,
            prs_total=4,
            prs_merged=3,
        ),
        _record(
            bucket=AttributionBucket.HUMAN,
            day=DAY_START,
            human_prs=12,
            prs_total=12,
            prs_merged=11,
            cycle_time_avg_hours=30.0,
            reviews_per_pr=1.0,
            rework_drag_rate=0.05,
            revert_rate=0.02,
            incident_drag_rate=0.0,
            test_gap_rate=0.1,
        ),
        _record(
            bucket=AttributionBucket.UNKNOWN,
            day=DAY_START,
            unknown_prs=2,
            prs_total=2,
            prs_merged=1,
        ),
    ]


def _partial_rows() -> list[AIImpactMetricsDailyRecord]:
    """Only the AI-side bucket populated."""
    return [
        _record(
            bucket=AttributionBucket.AI_ASSISTED,
            day=DAY_START,
            ai_assisted_prs=6,
            ai_assisted_pr_ratio=1.0,
            ai_review_amplification=1.6,
        )
    ]


def _patch_loader(rows: list[AIImpactMetricsDailyRecord]) -> Any:
    """Patch the AIImpactClickHouseLoader to return ``rows``."""
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact.AIImpactClickHouseLoader.load_ai_impact_metrics",
        new_callable=AsyncMock,
        return_value=rows,
    )


def _patch_reviewer_concentration(
    reviewer_gini: float | None,
    reviewer_count: int,
) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact."
        "AIImpactClickHouseLoader.load_reviewer_concentration",
        new_callable=AsyncMock,
        return_value=(reviewer_gini, reviewer_count),
    )


def _range() -> AIDateRangeInput:
    return AIDateRangeInput(start_date=DAY_START, end_date=DAY_END)


# -----------------------------------------------------------------------------
# aiImpactSummary
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_impact_summary_empty_state_returns_stable_contract():
    with _patch_loader([]):
        result = await resolve_ai_impact_summary(_ctx(), _range())

    assert result.org_id == ORG_ID
    assert result.total_prs == 0
    assert result.ai_assisted_prs == 0
    assert result.ai_assisted_pr_ratio is None
    assert result.by_bucket == []
    assert result.daily == []
    assert result.missing_states == []
    assert result.data_available is False
    assert result.computed_at is None


@pytest.mark.asyncio
async def test_impact_summary_partial_returns_only_populated_bucket():
    with _patch_loader(_partial_rows()):
        result = await resolve_ai_impact_summary(_ctx(), _range())

    assert result.data_available is True
    assert len(result.by_bucket) == 1
    assert result.by_bucket[0].bucket == AttributionBucket.AI_ASSISTED.value
    assert result.by_bucket[0].ai_review_amplification == pytest.approx(1.6)
    assert result.ai_assisted_prs == 6
    assert result.human_prs == 0


@pytest.mark.asyncio
async def test_impact_summary_populated_aggregates_all_buckets():
    with _patch_loader(_populated_rows()):
        result = await resolve_ai_impact_summary(_ctx(), _range())

    assert result.data_available is True
    bucket_names = {row.bucket for row in result.by_bucket}
    assert bucket_names == {
        AttributionBucket.AI_ASSISTED.value,
        AttributionBucket.AGENT_CREATED.value,
        AttributionBucket.HUMAN.value,
        AttributionBucket.UNKNOWN.value,
    }
    assert result.ai_assisted_prs == 20
    assert result.agent_created_prs == 4
    assert result.human_prs == 12
    assert result.unknown_prs == 2
    assert result.total_prs == 20 + 4 + 12 + 2
    assert [state.key for state in result.missing_states] == ["unknown_attribution"]
    # ai_assisted_pr_ratio is the volume-weighted average across all rows.
    assert result.ai_assisted_pr_ratio is not None
    assert result.computed_at == COMPUTED_AT


@pytest.mark.asyncio
async def test_impact_summary_respects_bucket_filter():
    scope = AIScopeInput(
        buckets=[AIAttributionBucketInput.AGENT_CREATED],
    )
    with _patch_loader(_populated_rows()):
        result = await resolve_ai_impact_summary(_ctx(), _range(), scope)

    assert {row.bucket for row in result.by_bucket} == {
        AttributionBucket.AGENT_CREATED.value
    }
    assert all(d.bucket == AttributionBucket.AGENT_CREATED.value for d in result.daily)


# -----------------------------------------------------------------------------
# aiComparison
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_comparison_empty_returns_zeroed_sides():
    with _patch_loader([]):
        result = await resolve_ai_comparison(_ctx(), _range())

    assert result.data_available is False
    assert result.ai_side.prs_total == 0
    assert result.baseline_side.prs_total == 0
    assert result.delta.cycle_time_delta_hours is None


@pytest.mark.asyncio
async def test_comparison_partial_leaves_baseline_side_empty():
    with _patch_loader(_partial_rows()):
        result = await resolve_ai_comparison(_ctx(), _range())

    assert result.data_available is True
    assert result.ai_side.prs_total == 10  # _partial_rows uses default prs_total=10
    assert result.baseline_side.prs_total == 0
    # delta requires both sides — partial inputs yield None per field.
    assert result.delta.cycle_time_delta_hours is None
    assert result.delta.rework_rate_delta is None


@pytest.mark.asyncio
async def test_comparison_populated_emits_delta():
    with _patch_loader(_populated_rows()):
        result = await resolve_ai_comparison(_ctx(), _range())

    assert result.data_available is True
    assert result.ai_side.prs_total == 24  # ai_assisted (20) + agent_created (4)
    assert result.baseline_side.prs_total == 12
    assert result.delta.cycle_time_delta_hours is not None
    assert result.delta.reviews_per_pr_delta is not None


# -----------------------------------------------------------------------------
# aiReviewLoad
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_load_empty_state():
    with _patch_loader([]), _patch_reviewer_concentration(None, 0):
        result = await resolve_ai_review_load(_ctx(), _range())

    assert result.data_available is False
    assert result.by_bucket == []
    assert result.daily == []
    assert result.reviewer_concentration.data_available is False
    assert result.reviewer_concentration.reviewer_count == 0
    assert [state.key for state in result.missing_states] == ["reviewer_concentration"]


@pytest.mark.asyncio
async def test_review_load_populated_aggregates_by_bucket():
    with _patch_loader(_populated_rows()), _patch_reviewer_concentration(0.42, 5):
        result = await resolve_ai_review_load(_ctx(), _range())

    assert result.data_available is True
    bucket_lookup = {row.bucket: row for row in result.by_bucket}
    # 20 PRs * 1.5 reviews_per_pr = 30 reviews for ai_assisted bucket.
    assert bucket_lookup[AttributionBucket.AI_ASSISTED.value].reviews_total == 30
    # Review amplification only populated for ai_assisted in fixture.
    assert bucket_lookup[
        AttributionBucket.AI_ASSISTED.value
    ].review_amplification == pytest.approx(1.4)
    assert (
        bucket_lookup[
            AttributionBucket.AI_ASSISTED.value
        ].post_first_review_pushes_count
        == 6
    )
    assert bucket_lookup[
        AttributionBucket.AI_ASSISTED.value
    ].post_first_review_pushes_per_pr == pytest.approx(0.3)
    assert result.reviewer_concentration.data_available is True
    assert result.reviewer_concentration.reviewer_count == 5
    assert result.reviewer_concentration.reviewer_gini == pytest.approx(0.42)
    # Engagement loader is not stubbed here, so CHAOS-2194 fields are honestly
    # unavailable: None values plus an explicit missing state — never zeros.
    assert [state.key for state in result.missing_states] == ["review_engagement"]
    ai_bucket = bucket_lookup[AttributionBucket.AI_ASSISTED.value]
    assert ai_bucket.pickup_latency_hours is None
    assert ai_bucket.review_comments_per_loc is None


# -----------------------------------------------------------------------------
# aiRiskBreakdown
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_risk_breakdown_empty_state():
    with _patch_loader([]):
        result = await resolve_ai_risk_breakdown(_ctx(), _range())

    assert result.data_available is False
    assert result.by_bucket == []
    assert {state.key for state in result.missing_states} == {
        "hotspot_overlap",
        "complexity_overlap",
    }


@pytest.mark.asyncio
async def test_risk_breakdown_populated_computes_rates():
    with _patch_loader(_populated_rows()):
        result = await resolve_ai_risk_breakdown(_ctx(), _range())

    assert result.data_available is True
    bucket_lookup = {row.bucket: row for row in result.by_bucket}
    ai_row = bucket_lookup[AttributionBucket.AI_ASSISTED.value]
    # rework_drag_rate fixture = 0.1, so rework_prs = 1 / 10 total = 0.1
    assert ai_row.rework_rate == pytest.approx(0.1)
    assert ai_row.revert_rate == pytest.approx(0.05)
    assert ai_row.test_gap_rate == pytest.approx(0.2)
    assert {state.key for state in result.missing_states} == {
        "hotspot_overlap",
        "complexity_overlap",
    }


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


# -----------------------------------------------------------------------------
# aiGovernanceSummary
# -----------------------------------------------------------------------------


class _FakeGovernanceCoverage:
    def __init__(self) -> None:
        self.day = DAY_START
        self.team_id = TEAM_ID
        self.repo_id = REPO_ID
        self.ai_artifacts = 10
        self.declared_artifacts = 8
        self.human_reviewed_prs = 7
        self.security_scanned_prs = 6
        self.in_policy_artifacts = 5
        self.declaration_coverage = 0.8
        self.human_review_coverage = 0.7
        self.security_scan_coverage = 0.6
        self.in_policy_coverage = 0.5


class _FakeGovernanceViolation:
    def __init__(self) -> None:
        self.rule_id = "MISSING_AI_DECLARATION"
        self.severity = "warning"
        self.subject_type = "pull_request"
        self.subject_id = "repo/123"
        self.team_id = TEAM_ID
        self.repo_id = REPO_ID
        self.observed_at = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
        self.evidence = '{"declared_ai": false}'


@pytest.mark.asyncio
async def test_governance_summary_empty():
    # CHAOS-2211: load_coverage / load_violations are now async; use AsyncMock.
    with patch(
        "dev_health_ops.audit.ai_governance.loaders.AIGovernanceLoader"
    ) as mock_loader:
        mock_loader.return_value.load_coverage = AsyncMock(return_value=[])
        mock_loader.return_value.load_violations = AsyncMock(return_value=[])
        result = await resolve_ai_governance_summary(_ctx(), _range())

    assert result.data_available is False
    assert result.coverage == []
    assert result.recent_violations == []


@pytest.mark.asyncio
async def test_governance_summary_populated():
    with patch(
        "dev_health_ops.audit.ai_governance.loaders.AIGovernanceLoader"
    ) as mock_loader:
        mock_loader.return_value.load_coverage = AsyncMock(
            return_value=[_FakeGovernanceCoverage()]
        )
        mock_loader.return_value.load_violations = AsyncMock(
            return_value=[_FakeGovernanceViolation()]
        )
        result = await resolve_ai_governance_summary(_ctx(), _range())

    assert result.data_available is True
    assert result.coverage[0].declaration_coverage == pytest.approx(0.8)
    assert result.recent_violations[0].rule_id == "MISSING_AI_DECLARATION"
    assert result.recent_violations[0].repo_id == str(REPO_ID)


# -----------------------------------------------------------------------------
# aiWorkflowDrilldown
# -----------------------------------------------------------------------------


class _FakeNode:
    def __init__(self, node_type: str, node_id: str) -> None:
        self.node_type = node_type
        self.node_id = node_id


class _FakeEdge:
    def __init__(self) -> None:
        self.edge_id = "edge-1"
        self.source_type = "issue"
        self.source_id = "issue-1"
        self.target_type = "ai_workflow_run"
        self.target_id = "run-1"
        self.edge_type = "has_ai_workflow"
        self.confidence = 0.9
        self.source = "pr_label"
        self.evidence = "label:ai-assisted"
        self.provider = "github"
        self.repo_id = str(REPO_ID)


class _FakeTraversal:
    def __init__(self) -> None:
        self.root_type = "issue"
        self.root_id = "issue-1"
        self.nodes = [
            _FakeNode("issue", "issue-1"),
            _FakeNode("ai_workflow_run", "run-1"),
        ]
        self.edges = [_FakeEdge()]
        self.partial = False


@pytest.mark.asyncio
async def test_workflow_drilldown_empty():
    fake = _FakeTraversal()
    fake.nodes = []
    fake.edges = []
    with patch(
        "dev_health_ops.work_graph.ai_workflow.load_ai_workflow_graph",
        new_callable=AsyncMock,
        return_value=fake,
    ):
        result = await resolve_ai_workflow_drilldown(
            _ctx(), AIWorkflowRootTypeInput.ISSUE, "issue-1"
        )

    assert result.data_available is False
    assert result.nodes == []
    assert result.edges == []


@pytest.mark.asyncio
async def test_workflow_drilldown_populated():
    with patch(
        "dev_health_ops.work_graph.ai_workflow.load_ai_workflow_graph",
        new_callable=AsyncMock,
        return_value=_FakeTraversal(),
    ):
        result = await resolve_ai_workflow_drilldown(
            _ctx(), AIWorkflowRootTypeInput.ISSUE, "issue-1"
        )

    assert result.data_available is True
    assert len(result.nodes) == 2
    assert result.edges[0].edge_type == "has_ai_workflow"
    assert result.edges[0].provider == "github"


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_impact_summary_rejects_reversed_date_range():
    bad_range = AIDateRangeInput(start_date=DAY_END, end_date=DAY_START)
    with pytest.raises(ValueError, match="end_date must be >= start_date"):
        await resolve_ai_impact_summary(_ctx(), bad_range)


@pytest.mark.asyncio
async def test_workflow_drilldown_rejects_empty_root_id():
    with pytest.raises(ValueError, match="root_id is required"):
        await resolve_ai_workflow_drilldown(_ctx(), AIWorkflowRootTypeInput.ISSUE, "")


# -----------------------------------------------------------------------------
# aiAttributedPrs
# -----------------------------------------------------------------------------


def _attribution_row(
    *,
    number: int,
    kind: str = "copilot",
    work_type: str = "pull_request",
    team_id: str | None = None,
    title: str | None = None,
    merged_at: datetime | None = None,
):
    return {
        "repo_id": REPO_ID,
        "number": number,
        "kind": kind,
        "work_type": work_type,
        "team_id": team_id,
        "title": title,
        "merged_at": merged_at,
    }


def _patch_pr_loader(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact.AIImpactClickHouseLoader.load_ai_pr_attributions",
        new_callable=AsyncMock,
        return_value=rows,
    )


@pytest.mark.asyncio
async def test_ai_attributed_prs_empty_state():
    with _patch_pr_loader([]):
        result = await resolve_ai_attributed_prs(_ctx(), _range())

    assert result.org_id == ORG_ID
    assert result.rows == []
    assert result.total == 0
    assert result.has_more is False
    assert result.data_available is False


@pytest.mark.asyncio
async def test_ai_attributed_prs_populated_maps_fields():
    merged = datetime(2026, 5, 4, 9, tzinfo=timezone.utc)
    rows = [
        _attribution_row(
            number=101,
            kind="copilot",
            title="Add feature flag",
            merged_at=merged,
        ),
        _attribution_row(number=102, kind="cursor", title="Refactor auth"),
    ]
    with _patch_pr_loader(rows):
        result = await resolve_ai_attributed_prs(_ctx(), _range())

    assert result.data_available is True
    assert result.has_more is False
    assert result.total == 2
    assert [r.number for r in result.rows] == [101, 102]
    assert result.rows[0].repo_id == str(REPO_ID)
    assert result.rows[0].title == "Add feature flag"
    assert result.rows[0].kind == "copilot"
    assert result.rows[0].merged_at == merged
    assert result.rows[1].merged_at is None


@pytest.mark.asyncio
async def test_ai_attributed_prs_reports_has_more_and_pages():
    # Loader returns limit+1 rows to signal more pages.
    rows = [_attribution_row(number=200 + i) for i in range(51)]
    with _patch_pr_loader(rows) as mock_load:
        result = await resolve_ai_attributed_prs(_ctx(), _range(), limit=50, offset=0)

    assert mock_load.await_args.kwargs["limit"] == 51
    assert mock_load.await_args.kwargs["offset"] == 0
    assert len(result.rows) == 50
    assert result.has_more is True


@pytest.mark.asyncio
async def test_ai_attributed_prs_passes_repo_scope_to_loader():
    scope = AIScopeInput(repo_id=str(REPO_ID))
    with _patch_pr_loader([]) as mock_load:
        await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    assert mock_load.await_args.kwargs["repo_id"] == REPO_ID


@pytest.mark.asyncio
async def test_ai_attributed_prs_filters_by_work_type_in_memory():
    rows = [
        _attribution_row(number=1, work_type="bug"),
        _attribution_row(number=2, work_type="feature"),
        _attribution_row(number=3, work_type="bug"),
    ]
    scope = AIScopeInput(work_type="bug")
    with _patch_pr_loader(rows):
        result = await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    assert [r.number for r in result.rows] == [1, 3]


@pytest.mark.asyncio
async def test_ai_attributed_prs_clamps_oversize_limit():
    rows = [_attribution_row(number=i) for i in range(10)]
    with _patch_pr_loader(rows) as mock_load:
        await resolve_ai_attributed_prs(_ctx(), _range(), limit=5000, offset=-5)

    # Limit is clamped to _MAX_AI_ATTRIBUTED_PRS_PAGE (200) plus 1 for has_more probe.
    assert mock_load.await_args.kwargs["limit"] == 201
    assert mock_load.await_args.kwargs["offset"] == 0


@pytest.mark.asyncio
async def test_ai_attributed_prs_rejects_reversed_date_range():
    bad_range = AIDateRangeInput(start_date=DAY_END, end_date=DAY_START)
    with pytest.raises(ValueError, match="end_date must be >= start_date"):
        await resolve_ai_attributed_prs(_ctx(), bad_range)


# -----------------------------------------------------------------------------
# CHAOS-2184 — AI-19: team_id scoping for AI attributed PRs
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ai_attributed_prs_team_scope_filters_by_team_id():
    """Rows whose resolved team_id matches the scope pass; others are dropped.

    Uses two distinct repos mapped to different teams so we can verify that
    only the team-a repo's rows survive the filter.
    """
    REPO_B = UUID("22222222-2222-2222-2222-222222222222")
    rows = [
        _attribution_row(number=1),  # REPO_ID → resolved to team-a
        {**_attribution_row(number=2), "repo_id": REPO_B},  # team-b
        _attribution_row(number=3),  # REPO_ID → team-a
        {**_attribution_row(number=4), "repo_id": REPO_B},  # team-b
    ]
    scope = AIScopeInput(team_id="team-a")
    repo_team_map = {str(REPO_ID): "team-a", str(REPO_B): "team-b"}
    with (
        _patch_pr_loader(rows),
        # Force the catalogs-unavailable fallback so this test keeps pinning
        # the Wave-1 in-memory filter; the SQL prefilter path is covered by
        # test_ai_attributed_prs_team_scope_filters_in_sql_before_limit.
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_team_repo_ids",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_team_map",
            new_callable=AsyncMock,
            return_value=repo_team_map,
        ),
    ):
        result = await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    assert result.data_available is True
    assert [r.number for r in result.rows] == [1, 3]
    assert all(r.team_id == "team-a" for r in result.rows)


@pytest.mark.asyncio
async def test_ai_attributed_prs_no_team_scope_returns_all():
    """Without a team_id scope all rows are returned regardless of their team."""
    rows = [
        _attribution_row(number=10, team_id="team-x"),
        _attribution_row(number=11, team_id=None),
    ]
    with _patch_pr_loader(rows):
        result = await resolve_ai_attributed_prs(_ctx(), _range())

    assert [r.number for r in result.rows] == [10, 11]


@pytest.mark.asyncio
async def test_ai_attributed_prs_team_scope_resolves_via_repo_pattern():
    """Team id must be resolved via RepoPatternTeamResolver, not pre-populated SQL.

    The SQL loader always returns empty team_id because teams.repo_patterns
    is Array(String) of fnmatch glob patterns over repo full-names (not UUIDs).
    The resolver calls _resolve_repo_team_map to annotate each row before the
    team filter runs.
    """
    REPO_B = UUID("22222222-2222-2222-2222-222222222222")
    # All rows start with empty team_id — as the SQL loader produces them.
    rows = [
        _attribution_row(number=1, team_id=None),  # REPO_ID → team-a via pattern
        _attribution_row(number=2, team_id=None),  # REPO_ID → team-a via pattern
        {**_attribution_row(number=3, team_id=None), "repo_id": REPO_B},  # team-b
    ]
    scope = AIScopeInput(team_id="team-a")
    # Simulate the pattern resolver resolving two repos to different teams.
    repo_team_map = {str(REPO_ID): "team-a", str(REPO_B): "team-b"}
    with (
        _patch_pr_loader(rows),
        # Catalogs unavailable → in-memory fallback path (see note in
        # test_ai_attributed_prs_team_scope_filters_by_team_id).
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_team_repo_ids",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_team_map",
            new_callable=AsyncMock,
            return_value=repo_team_map,
        ),
    ):
        result = await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    # Only rows belonging to team-a (REPO_ID) pass through.
    assert [r.number for r in result.rows] == [1, 2]
    assert all(r.team_id == "team-a" for r in result.rows)


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
# CHAOS-2211 — AI-30: governance loader methods are awaited
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_governance_summary_awaits_async_loader_methods():
    """Verify both load_coverage and load_violations are called as coroutines."""
    coverage_mock = AsyncMock(return_value=[_FakeGovernanceCoverage()])
    violations_mock = AsyncMock(return_value=[_FakeGovernanceViolation()])
    with patch(
        "dev_health_ops.audit.ai_governance.loaders.AIGovernanceLoader"
    ) as mock_loader:
        mock_loader.return_value.load_coverage = coverage_mock
        mock_loader.return_value.load_violations = violations_mock
        result = await resolve_ai_governance_summary(_ctx(), _range())

    coverage_mock.assert_awaited_once()
    violations_mock.assert_awaited_once()
    assert result.data_available is True
    assert result.coverage[0].ai_artifacts == 10
    assert result.recent_violations[0].rule_id == "MISSING_AI_DECLARATION"


# -----------------------------------------------------------------------------
# CHAOS-2194 — Review Load pickup latency + review comments per LOC
# -----------------------------------------------------------------------------


def _patch_engagement(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact."
        "AIImpactClickHouseLoader.load_review_engagement",
        new_callable=AsyncMock,
        return_value=rows,
    )


def _engagement_row(
    *,
    bucket: str,
    day: date,
    prs_with_first_review: int = 5,
    pickup_latency_hours: float | None = 4.0,
    review_comments_total: int = 20,
    loc_total: int = 1000,
) -> dict[str, Any]:
    return {
        "bucket": bucket,
        "day": day,
        "prs_with_first_review": prs_with_first_review,
        "pickup_latency_hours": pickup_latency_hours,
        "review_comments_total": review_comments_total,
        "loc_total": loc_total,
    }


@pytest.mark.asyncio
async def test_review_load_merges_engagement_fields():
    engagement = [
        _engagement_row(
            bucket=AttributionBucket.AI_ASSISTED.value,
            day=DAY_START,
            pickup_latency_hours=4.0,
            review_comments_total=20,
            loc_total=1000,
        ),
        _engagement_row(
            bucket=AttributionBucket.HUMAN.value,
            day=DAY_START,
            pickup_latency_hours=8.0,
            review_comments_total=10,
            loc_total=500,
        ),
    ]
    with (
        _patch_loader(_populated_rows()),
        _patch_reviewer_concentration(0.42, 5),
        _patch_engagement(engagement),
    ):
        result = await resolve_ai_review_load(_ctx(), _range())

    bucket_lookup = {row.bucket: row for row in result.by_bucket}
    ai_row = bucket_lookup[AttributionBucket.AI_ASSISTED.value]
    assert ai_row.pickup_latency_hours == pytest.approx(4.0)
    assert ai_row.review_comments_per_loc == pytest.approx(0.02)
    human_row = bucket_lookup[AttributionBucket.HUMAN.value]
    assert human_row.pickup_latency_hours == pytest.approx(8.0)
    assert human_row.review_comments_per_loc == pytest.approx(0.02)
    # Buckets without engagement rows stay None (unavailable ≠ zero).
    unknown_row = bucket_lookup[AttributionBucket.UNKNOWN.value]
    assert unknown_row.pickup_latency_hours is None
    assert unknown_row.review_comments_per_loc is None
    # Daily rows merge by (bucket, day).
    ai_daily = [
        r for r in result.daily if r.bucket == AttributionBucket.AI_ASSISTED.value
    ]
    assert ai_daily[0].pickup_latency_hours == pytest.approx(4.0)
    # No engagement missing state when engagement data exists.
    assert "review_engagement" not in {s.key for s in result.missing_states}


@pytest.mark.asyncio
async def test_review_load_engagement_zero_loc_yields_none_not_zero():
    engagement = [
        _engagement_row(
            bucket=AttributionBucket.AI_ASSISTED.value,
            day=DAY_START,
            prs_with_first_review=0,
            pickup_latency_hours=None,
            review_comments_total=0,
            loc_total=0,
        ),
    ]
    with (
        _patch_loader(_populated_rows()),
        _patch_reviewer_concentration(0.42, 5),
        _patch_engagement(engagement),
    ):
        result = await resolve_ai_review_load(_ctx(), _range())

    bucket_lookup = {row.bucket: row for row in result.by_bucket}
    ai_row = bucket_lookup[AttributionBucket.AI_ASSISTED.value]
    assert ai_row.pickup_latency_hours is None
    assert ai_row.review_comments_per_loc is None


@pytest.mark.asyncio
async def test_review_load_engagement_skipped_for_work_type_scope():
    """work_type cannot be applied to raw PR rows — engagement must be
    reported unavailable rather than silently mis-scoped."""
    with (
        _patch_loader(_populated_rows()),
        _patch_reviewer_concentration(0.42, 5),
        _patch_engagement([_engagement_row(bucket="ai_assisted", day=DAY_START)]),
    ):
        result = await resolve_ai_review_load(
            _ctx(), _range(), AIScopeInput(work_type="bug")
        )

    bucket_lookup = {row.bucket: row for row in result.by_bucket}
    ai_row = bucket_lookup[AttributionBucket.AI_ASSISTED.value]
    assert ai_row.pickup_latency_hours is None
    assert "review_engagement" in {s.key for s in result.missing_states}


# -----------------------------------------------------------------------------
# CHAOS-2185 — Hotspot / complexity overlap
# -----------------------------------------------------------------------------


def _patch_hotspot_overlap(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact."
        "AIImpactClickHouseLoader.load_hotspot_overlap",
        new_callable=AsyncMock,
        return_value=rows,
    )


def _patch_complexity_overlap(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact."
        "AIImpactClickHouseLoader.load_complexity_overlap",
        new_callable=AsyncMock,
        return_value=rows,
    )


@pytest.mark.asyncio
async def test_risk_breakdown_populated_overlaps_drop_missing_states():
    hotspot_raw = [
        {
            "bucket": "ai_assisted",
            "prs_total": 10,
            "prs_touching_hotspots": 4,
            "avg_hotspot_risk_score": 0.73,
        }
    ]
    complexity_raw = [
        {
            "bucket": "ai_assisted",
            "prs_total": 10,
            "prs_touching_high_complexity": 2,
        }
    ]
    with (
        _patch_loader(_populated_rows()),
        _patch_hotspot_overlap(hotspot_raw),
        _patch_complexity_overlap(complexity_raw),
    ):
        result = await resolve_ai_risk_breakdown(_ctx(), _range())

    assert len(result.hotspot_overlap) == 1
    hs = result.hotspot_overlap[0]
    assert hs.bucket == "ai_assisted"
    assert hs.prs_total == 10
    assert hs.prs_touching_hotspots == 4
    assert hs.hotspot_overlap_rate == pytest.approx(0.4)
    assert hs.avg_hotspot_risk_score == pytest.approx(0.73)
    cx = result.complexity_overlap[0]
    assert cx.complexity_overlap_rate == pytest.approx(0.2)
    # Real data present → missing states must be gone.
    assert {s.key for s in result.missing_states} == set()


@pytest.mark.asyncio
async def test_risk_breakdown_hotspot_nan_average_becomes_null():
    hotspot_raw = [
        {
            "bucket": "agent_created",
            "prs_total": 1,
            "prs_touching_hotspots": 0,
            "avg_hotspot_risk_score": float("nan"),
        }
    ]
    complexity_raw = [
        {
            "bucket": "agent_created",
            "prs_total": 1,
            "prs_touching_high_complexity": 0,
        }
    ]
    with (
        _patch_loader(_populated_rows()),
        _patch_hotspot_overlap(hotspot_raw),
        _patch_complexity_overlap(complexity_raw),
    ):
        result = await resolve_ai_risk_breakdown(_ctx(), _range())

    assert len(result.hotspot_overlap) == 1
    hs = result.hotspot_overlap[0]
    assert hs.bucket == "agent_created"
    assert hs.prs_total == 1
    assert hs.prs_touching_hotspots == 0
    assert hs.hotspot_overlap_rate == pytest.approx(0.0)
    assert hs.avg_hotspot_risk_score is None


@pytest.mark.asyncio
async def test_risk_breakdown_graphql_serializes_nan_average_as_null():
    hotspot_raw = [
        {
            "bucket": "agent_created",
            "prs_total": 1,
            "prs_touching_hotspots": 0,
            "avg_hotspot_risk_score": float("nan"),
        }
    ]
    complexity_raw = [
        {
            "bucket": "agent_created",
            "prs_total": 1,
            "prs_touching_high_complexity": 0,
        }
    ]
    query = """
    query AIRiskBreakdown($orgId: String!, $dateRange: AIDateRangeInput!) {
      aiRiskBreakdown(orgId: $orgId, dateRange: $dateRange) {
        hotspotOverlap {
          bucket
          prsTotal
          prsTouchingHotspots
          hotspotOverlapRate
          avgHotspotRiskScore
        }
      }
    }
    """
    variables = {
        "orgId": ORG_ID,
        "dateRange": {
            "startDate": DAY_START.isoformat(),
            "endDate": DAY_END.isoformat(),
        },
    }
    with (
        _patch_loader(_populated_rows()),
        _patch_hotspot_overlap(hotspot_raw),
        _patch_complexity_overlap(complexity_raw),
    ):
        result = await schema.execute(
            query,
            variable_values=variables,
            context_value=_ctx(),
        )

    assert result.errors is None
    assert result.data is not None
    assert result.data["aiRiskBreakdown"]["hotspotOverlap"] == [
        {
            "bucket": "agent_created",
            "prsTotal": 1,
            "prsTouchingHotspots": 0,
            "hotspotOverlapRate": 0.0,
            "avgHotspotRiskScore": None,
        }
    ]


@pytest.mark.asyncio
async def test_risk_breakdown_empty_overlaps_keep_missing_states():
    with (
        _patch_loader(_populated_rows()),
        _patch_hotspot_overlap([]),
        _patch_complexity_overlap([]),
    ):
        result = await resolve_ai_risk_breakdown(_ctx(), _range())

    assert result.hotspot_overlap == []
    assert result.complexity_overlap == []
    assert {s.key for s in result.missing_states} == {
        "hotspot_overlap",
        "complexity_overlap",
    }


# -----------------------------------------------------------------------------
# CHAOS-2186 — Impact per-repo / per-team breakdown
# -----------------------------------------------------------------------------


def _patch_repo_labels(labels: dict[str, str]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact."
        "AIImpactClickHouseLoader.load_repo_labels",
        new_callable=AsyncMock,
        return_value=labels,
    )


def _patch_team_labels(labels: dict[str, str]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_impact."
        "AIImpactClickHouseLoader.load_team_labels",
        new_callable=AsyncMock,
        return_value=labels,
    )


@pytest.mark.asyncio
async def test_impact_summary_builds_repo_and_team_breakdowns():
    with (
        _patch_loader(_populated_rows()),
        _patch_repo_labels({str(REPO_ID): "acme/api"}),
        _patch_team_labels({TEAM_ID: "Team A"}),
    ):
        result = await resolve_ai_impact_summary(_ctx(), _range())

    assert len(result.repo_breakdown) == 1
    repo_row = result.repo_breakdown[0]
    assert repo_row.scope_id == str(REPO_ID)
    assert repo_row.scope_label == "acme/api"
    # AI buckets: ai_assisted 20 + agent_created 4 = 24 of 38 total PRs.
    assert repo_row.ai_prs_total == 24
    assert repo_row.ai_assisted_pr_ratio == pytest.approx(24 / 38)
    # AI rework 0.1 (both AI rows) − human rework 0.05 = +0.05.
    assert repo_row.rework_rate_delta == pytest.approx(0.05)
    team_row = result.team_breakdown[0]
    assert team_row.scope_id == TEAM_ID
    assert team_row.scope_label == "Team A"
    assert "scope_breakdown" not in {s.key for s in result.missing_states}


@pytest.mark.asyncio
async def test_impact_summary_breakdowns_fall_back_to_ids_and_missing_state():
    """Label lookups failing must degrade to ids; an all-human window emits
    the scope_breakdown missing state instead of fabricated zero rows."""
    human_only = [
        _record(
            bucket=AttributionBucket.HUMAN,
            day=DAY_START,
            human_prs=12,
            prs_total=12,
            prs_merged=11,
        )
    ]
    with (
        _patch_loader(human_only),
        _patch_repo_labels({}),
        _patch_team_labels({}),
    ):
        result = await resolve_ai_impact_summary(_ctx(), _range())

    assert result.repo_breakdown == []
    assert result.team_breakdown == []
    assert "scope_breakdown" in {s.key for s in result.missing_states}


@pytest.mark.asyncio
async def test_impact_summary_empty_has_empty_breakdowns_without_missing_state():
    with _patch_loader([]):
        result = await resolve_ai_impact_summary(_ctx(), _range())

    assert result.repo_breakdown == []
    assert result.team_breakdown == []
    assert "scope_breakdown" not in {s.key for s in result.missing_states}


# -----------------------------------------------------------------------------
# CHAOS-2180 Wave 2 — dense team-scoped pagination for attributed PRs
# -----------------------------------------------------------------------------


def _patch_team_repo_ids(value: Any) -> Any:
    return patch(
        "dev_health_ops.api.graphql.resolvers.ai._resolve_team_repo_ids",
        new_callable=AsyncMock,
        return_value=value,
    )


@pytest.mark.asyncio
async def test_ai_attributed_prs_team_scope_filters_in_sql_before_limit():
    """Team scope must reach the loader as repo_ids so the SQL LIMIT applies
    to the already-filtered universe (dense pages)."""
    rows = [_attribution_row(number=1), _attribution_row(number=2)]
    scope = AIScopeInput(team_id="team-a")
    repo_team_map = {str(REPO_ID): "team-a"}
    with (
        _patch_pr_loader(rows) as mock_load,
        _patch_team_repo_ids([REPO_ID]),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_team_map",
            new_callable=AsyncMock,
            return_value=repo_team_map,
        ),
    ):
        result = await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    assert mock_load.await_args.kwargs["repo_ids"] == [REPO_ID]
    assert [r.number for r in result.rows] == [1, 2]


@pytest.mark.asyncio
async def test_ai_attributed_prs_team_with_no_repos_returns_empty():
    scope = AIScopeInput(team_id="team-empty")
    with (
        _patch_pr_loader([_attribution_row(number=1)]) as mock_load,
        _patch_team_repo_ids([]),
    ):
        result = await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    mock_load.assert_not_awaited()
    assert result.rows == []
    assert result.data_available is False
    assert result.has_more is False


@pytest.mark.asyncio
async def test_ai_attributed_prs_unresolvable_team_falls_back_to_memory_filter():
    """When the team catalogs cannot be loaded (None), the resolver keeps the
    Wave-1 in-memory filter instead of wrongly returning an empty page."""
    REPO_B = UUID("22222222-2222-2222-2222-222222222222")
    rows = [
        _attribution_row(number=1),
        {**_attribution_row(number=2), "repo_id": REPO_B},
    ]
    scope = AIScopeInput(team_id="team-a")
    repo_team_map = {str(REPO_ID): "team-a", str(REPO_B): "team-b"}
    with (
        _patch_pr_loader(rows) as mock_load,
        _patch_team_repo_ids(None),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_team_map",
            new_callable=AsyncMock,
            return_value=repo_team_map,
        ),
    ):
        result = await resolve_ai_attributed_prs(_ctx(), _range(), scope)

    assert mock_load.await_args.kwargs["repo_ids"] is None
    assert [r.number for r in result.rows] == [1]


# -----------------------------------------------------------------------------
# CHAOS-2744 -- aiAttributionOverview
# -----------------------------------------------------------------------------


def _mix_row(kind: str, count: int) -> dict[str, Any]:
    return {"kind": kind, "count": count}


def _evidence_row(
    *,
    subject_id: str,
    subject_type: str = "pull_request",
    kind: str = "ai_assisted",
    source: str = "pr_label",
    confidence: float = 0.9,
    actor: str | None = "github-copilot",
    evidence: str = '{"label": "ai-assisted"}',
    observed_at: datetime | None = None,
    repo_id: UUID | None = REPO_ID,
    provider: str = "github",
    team_id: str | None = None,
) -> dict[str, Any]:
    return {
        "subject_type": subject_type,
        "subject_id": subject_id,
        "repo_id": repo_id,
        "provider": provider,
        "kind": kind,
        "source": source,
        "confidence": confidence,
        "actor": actor,
        "evidence": evidence,
        "observed_at": observed_at or COMPUTED_AT,
        "team_id": team_id,
    }


def _patch_mix_loader(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_attribution"
        ".AIAttributionClickHouseLoader.load_mix",
        new_callable=AsyncMock,
        return_value=rows,
    )


def _patch_evidence_loader(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ai_attribution"
        ".AIAttributionClickHouseLoader.load_evidence",
        new_callable=AsyncMock,
        return_value=rows,
    )


@pytest.mark.asyncio
async def test_ai_attribution_overview_empty_state():
    with _patch_mix_loader([]), _patch_evidence_loader([]):
        result = await resolve_ai_attribution_overview(_ctx(), _range())

    assert result.org_id == ORG_ID
    assert result.mix == []
    assert result.rows == []
    assert result.total_attributed == 0
    assert result.has_more is False
    assert result.data_available is False


@pytest.mark.asyncio
async def test_ai_attribution_overview_populated_maps_fields_and_provenance():
    mix_rows = [_mix_row("ai_assisted", 3), _mix_row("agent_created", 1)]
    evidence_rows = [
        _evidence_row(subject_id="101", kind="ai_assisted", confidence=0.9),
        _evidence_row(
            subject_id="102",
            kind="agent_created",
            source="bot_author",
            actor="claude-agent",
            confidence=0.75,
            evidence='{"bot": "claude-agent"}',
        ),
    ]
    with _patch_mix_loader(mix_rows), _patch_evidence_loader(evidence_rows):
        result = await resolve_ai_attribution_overview(_ctx(), _range())

    assert result.data_available is True
    assert result.total_attributed == 4
    assert [m.kind for m in result.mix] == ["ai_assisted", "agent_created"]
    assert [m.count for m in result.mix] == [3, 1]
    assert result.mix[0].share == pytest.approx(0.75)
    assert result.mix[1].share == pytest.approx(0.25)

    assert len(result.rows) == 2
    first = result.rows[0]
    assert first.subject_type == "pull_request"
    assert first.subject_id == "101"
    assert first.repo_id == str(REPO_ID)
    assert first.provider == "github"
    assert first.kind == "ai_assisted"
    # Every row must carry full provenance -- never blank source/evidence.
    assert first.source == "pr_label"
    assert first.confidence == 0.9
    assert first.evidence == '{"label": "ai-assisted"}'

    second = result.rows[1]
    assert second.source == "bot_author"
    assert second.actor == "claude-agent"
    assert second.confidence == 0.75


@pytest.mark.asyncio
async def test_ai_attribution_overview_mix_has_no_synthesized_human_bucket():
    """ai_attribution_resolved never carries an inferred human count --
    only kinds that a detector actually emitted a signal for. A synthesized
    'human' bucket here would silently duplicate aiImpactSummary's PR-
    population-based methodology with a different, undocumented one.
    """
    mix_rows = [_mix_row("ai_assisted", 2), _mix_row("unknown", 1)]
    with _patch_mix_loader(mix_rows), _patch_evidence_loader([]):
        result = await resolve_ai_attribution_overview(_ctx(), _range())

    assert "human" not in {m.kind for m in result.mix}


@pytest.mark.asyncio
async def test_ai_attribution_overview_reports_has_more_and_pages():
    rows = [_evidence_row(subject_id=str(200 + i)) for i in range(51)]
    with (
        _patch_mix_loader([_mix_row("ai_assisted", 51)]),
        _patch_evidence_loader(rows) as mock_load,
    ):
        result = await resolve_ai_attribution_overview(
            _ctx(), _range(), limit=50, offset=0
        )

    assert mock_load.await_args.kwargs["limit"] == 51
    assert mock_load.await_args.kwargs["offset"] == 0
    assert len(result.rows) == 50
    assert result.has_more is True


@pytest.mark.asyncio
async def test_ai_attribution_overview_passes_repo_scope_to_loaders():
    scope = AIAttributionScopeInput(repo_id=str(REPO_ID))
    with (
        _patch_mix_loader([]) as mock_mix,
        _patch_evidence_loader([]) as mock_evidence,
    ):
        await resolve_ai_attribution_overview(_ctx(), _range(), scope)

    assert mock_mix.await_args.kwargs["repo_id"] == REPO_ID
    assert mock_evidence.await_args.kwargs["repo_id"] == REPO_ID


@pytest.mark.asyncio
async def test_ai_attribution_overview_bucket_scope_filters_kind_in_sql():
    """CHAOS-2744 (Oracle NO-GO): scope.buckets must reach the loaders as a
    kind filter -- not be silently dropped. Regression coverage for the
    finding that aiAttributionOverview never applied scope.buckets."""
    scope = AIAttributionScopeInput(
        buckets=[
            AIAttributionBucketInput.AI_ASSISTED,
            AIAttributionBucketInput.AGENT_CREATED,
        ]
    )
    with (
        _patch_mix_loader([]) as mock_mix,
        _patch_evidence_loader([]) as mock_evidence,
    ):
        await resolve_ai_attribution_overview(_ctx(), _range(), scope)

    assert mock_mix.await_args.kwargs["kinds"] == ["ai_assisted", "agent_created"]
    assert mock_evidence.await_args.kwargs["kinds"] == [
        "ai_assisted",
        "agent_created",
    ]


@pytest.mark.asyncio
async def test_ai_attribution_overview_no_bucket_scope_omits_kinds_filter():
    with (
        _patch_mix_loader([]) as mock_mix,
        _patch_evidence_loader([]) as mock_evidence,
    ):
        await resolve_ai_attribution_overview(_ctx(), _range())

    assert mock_mix.await_args.kwargs["kinds"] is None
    assert mock_evidence.await_args.kwargs["kinds"] is None


def test_ai_attribution_scope_input_does_not_expose_work_type():
    """CHAOS-2744 (Oracle NO-GO): ai_attribution_resolved has no work_type
    column, so AIAttributionScopeInput must not expose the field at all --
    accepting-and-ignoring it was the original bug (silent no-op filter).
    Use the shared AIScopeInput (which does carry work_type) for queries
    backed by tables that actually have that column.
    """
    with pytest.raises(TypeError):
        AIAttributionScopeInput(work_type="bug")  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_ai_attribution_overview_rejects_reversed_date_range():
    bad_range = AIDateRangeInput(start_date=DAY_END, end_date=DAY_START)
    with pytest.raises(ValueError, match="end_date must be >= start_date"):
        await resolve_ai_attribution_overview(_ctx(), bad_range)


@pytest.mark.asyncio
async def test_ai_attribution_overview_team_with_no_repos_returns_empty():
    scope = AIAttributionScopeInput(team_id="team-empty")
    with (
        _patch_mix_loader([_mix_row("ai_assisted", 1)]) as mock_mix,
        _patch_evidence_loader([_evidence_row(subject_id="1")]) as mock_evidence,
        _patch_team_repo_ids([]),
    ):
        result = await resolve_ai_attribution_overview(_ctx(), _range(), scope)

    mock_mix.assert_not_awaited()
    mock_evidence.assert_not_awaited()
    assert result.mix == []
    assert result.rows == []
    assert result.data_available is False


@pytest.mark.asyncio
async def test_ai_attribution_overview_team_scope_filters_in_sql_before_limit():
    """Team scope must reach both loaders as repo_ids so the SQL LIMIT
    applies to the already-filtered universe (dense pages), mirroring
    resolve_ai_attributed_prs (CHAOS-2180 Wave 2)."""
    rows = [_evidence_row(subject_id="1"), _evidence_row(subject_id="2")]
    scope = AIAttributionScopeInput(team_id="team-a")
    repo_team_map = {str(REPO_ID): "team-a"}
    with (
        _patch_mix_loader([_mix_row("ai_assisted", 2)]) as mock_mix,
        _patch_evidence_loader(rows) as mock_evidence,
        _patch_team_repo_ids([REPO_ID]),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_team_map",
            new_callable=AsyncMock,
            return_value=repo_team_map,
        ),
    ):
        result = await resolve_ai_attribution_overview(_ctx(), _range(), scope)

    assert mock_mix.await_args.kwargs["repo_ids"] == [REPO_ID]
    assert mock_evidence.await_args.kwargs["repo_ids"] == [REPO_ID]
    assert [r.subject_id for r in result.rows] == ["1", "2"]
    assert all(r.team_id == "team-a" for r in result.rows)


@pytest.mark.asyncio
async def test_ai_attribution_overview_team_scope_drops_rows_outside_team():
    """Defense in depth: even if a row's resolved team doesn't match the
    scope, it must be dropped rather than leaking cross-team evidence."""
    REPO_B = UUID("22222222-2222-2222-2222-222222222222")
    rows = [
        _evidence_row(subject_id="1", repo_id=REPO_ID),
        _evidence_row(subject_id="2", repo_id=REPO_B),
    ]
    scope = AIAttributionScopeInput(team_id="team-a")
    repo_team_map = {str(REPO_ID): "team-a", str(REPO_B): "team-b"}
    with (
        _patch_mix_loader([_mix_row("ai_assisted", 2)]),
        _patch_evidence_loader(rows),
        _patch_team_repo_ids(None),
        patch(
            "dev_health_ops.api.graphql.resolvers.ai._resolve_repo_team_map",
            new_callable=AsyncMock,
            return_value=repo_team_map,
        ),
    ):
        result = await resolve_ai_attribution_overview(_ctx(), _range(), scope)

    assert [r.subject_id for r in result.rows] == ["1"]
    assert result.rows[0].team_id == "team-a"
