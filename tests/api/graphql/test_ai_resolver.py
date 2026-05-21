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
    AIDateRangeInput,
    AIOpportunity,
    AIOpportunityKind,
    AIScopeInput,
    AIWorkflowRootTypeInput,
)
from dev_health_ops.api.graphql.resolvers.ai import (
    resolve_ai_attributed_prs,
    resolve_ai_comparison,
    resolve_ai_governance_summary,
    resolve_ai_impact_summary,
    resolve_ai_opportunities,
    resolve_ai_review_load,
    resolve_ai_risk_breakdown,
    resolve_ai_workflow_drilldown,
)
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
    assert result.missing_states == []


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
    with patch(
        "dev_health_ops.audit.ai_governance.loaders.AIGovernanceLoader"
    ) as mock_loader:
        mock_loader.return_value.load_coverage.return_value = []
        mock_loader.return_value.load_violations.return_value = []
        result = await resolve_ai_governance_summary(_ctx(), _range())

    assert result.data_available is False
    assert result.coverage == []
    assert result.recent_violations == []


@pytest.mark.asyncio
async def test_governance_summary_populated():
    with patch(
        "dev_health_ops.audit.ai_governance.loaders.AIGovernanceLoader"
    ) as mock_loader:
        mock_loader.return_value.load_coverage.return_value = [
            _FakeGovernanceCoverage()
        ]
        mock_loader.return_value.load_violations.return_value = [
            _FakeGovernanceViolation()
        ]
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
