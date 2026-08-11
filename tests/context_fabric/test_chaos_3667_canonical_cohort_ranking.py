"""CHAOS-3667 W4 slice 2: canonical-service-backed cohort ranking.

These tests deliberately exercise the adapter below the packet assembler. A
cohort is not a scalar score: canonical services return independent findings
and their own source-state disclosures, so the adapter keeps those facts
attached to each member while applying the qualification rule and a
deterministic per-dimension ordering.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest

from dev_health_ops.api.dev.canonical_enrichment import (
    CanonicalEnrichment,
    EnrichmentGap,
)
from dev_health_ops.api.dev.contracts import (
    DevMetricRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.deficiency import (
    OperationalDeficiencyInventory,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    DimensionState,
    HealthDimension,
)
from dev_health_ops.api.dev.health_profile_synthesis import HealthProfileResult
from dev_health_ops.api.dev.investigation_contract import (
    CohortExclusionReason,
    CohortInclusionBasis,
    ComparisonDimension,
)
from dev_health_ops.api.dev.status_change_service import StatusSnapshotResult
from dev_health_ops.context_fabric.graph_arm.cohort import (
    CohortCandidate,
    CohortProposal,
)
from dev_health_ops.context_fabric.graph_arm.cohort_ranking import (
    CanonicalCohortRankingAdapter,
    CohortRankDisposition,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

_ORG_ID = "org_cohort_ranking_test"
_PERMISSION = "permission-fingerprint"
_NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=_ORG_ID,
        direct_scope=DirectScope.ORGANIZATION,
        time_range=DevTimeRange(
            start=_NOW - timedelta(days=14), end=_NOW, timezone="UTC"
        ),
        comparison_range=DevTimeRange(
            start=_NOW - timedelta(days=28),
            end=_NOW - timedelta(days=14),
            timezone="UTC",
        ),
    )


def _proposal(*member_ids: str, kind: GraphEntityKind = GraphEntityKind.PROJECT):
    return CohortProposal(
        subject_id="",
        members=tuple(
            CohortCandidate(
                canonical_id=member_id,
                kind=kind,
                display_label=member_id,
                basis_anchors=(
                    (CohortInclusionBasis.SAME_PORTFOLIO, ("portfolio_a",)),
                ),
            )
            for member_id in member_ids
        ),
        exclusions=(),
        dimensions=(ComparisonDimension.CYCLE_TIME,),
        truncated=False,
        truncated_count=0,
        authorization_filtered_count=0,
    )


def _observation(
    rule_id: str,
    *,
    observed_state: SourceRequirementState = SourceRequirementState.AVAILABLE_CURRENT,
    coverage: float = 1.0,
    denominator_present: bool = True,
    attribution_present: bool = True,
    data_semantics: str = "measured_zero",
) -> SimpleNamespace:
    return SimpleNamespace(
        observed_states=(observed_state,),
        data_semantics=data_semantics,
        coverage=coverage,
        current_value=10.0,
        denominator_present=denominator_present,
        attribution_present=attribution_present,
        rule_id=rule_id,
    )


def _finding(
    rule_id: str,
    dimension: HealthDimension,
    state: DimensionState,
    *,
    suppressed_reason: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        rule_id=rule_id,
        dimension=dimension,
        state=state,
        suppressed_reason=suppressed_reason,
        evidence_ref_ids=(f"evidence:{rule_id}",),
    )


def _profile(
    observations: tuple[SimpleNamespace, ...],
    *,
    launch_findings: tuple[SimpleNamespace, ...] = (),
    shadow_findings: tuple[SimpleNamespace, ...] = (),
    suppressed_findings: tuple[SimpleNamespace, ...] = (),
) -> SimpleNamespace:
    return SimpleNamespace(
        observations=observations,
        observations_by_rule={
            observation.rule_id: observation for observation in observations
        },
        launch_findings=launch_findings,
        shadow_findings=shadow_findings,
        suppressed_findings=suppressed_findings,
    )


def _enrichment(
    *,
    health: object,
    workload: object = EnrichmentGap.NOT_APPLICABLE,
    readiness: object = EnrichmentGap.NOT_APPLICABLE,
    status: object = EnrichmentGap.NOT_APPLICABLE,
    metrics: tuple[object, ...] = (),
) -> CanonicalEnrichment:
    return CanonicalEnrichment(
        status=cast(StatusSnapshotResult | EnrichmentGap, status),
        health=cast(HealthProfileResult | EnrichmentGap, health),
        workload=cast(HealthProfileResult | EnrichmentGap, workload),
        readiness=cast(OperationalDeficiencyInventory | EnrichmentGap, readiness),
        metrics=cast(tuple[DevMetricRef, ...], metrics),
    )


@dataclass
class _FakeEnrichment:
    by_subject: dict[str, CanonicalEnrichment]
    calls: list[str] = field(default_factory=list)

    async def enrich(self, *, org_id, permission_fingerprint, scope, now=None):
        assert org_id == _ORG_ID
        assert permission_fingerprint == _PERMISSION
        subject_id = scope.entity_refs[0].entity_id
        self.calls.append(subject_id)
        return self.by_subject[subject_id]


def _adapter(source: _FakeEnrichment) -> CanonicalCohortRankingAdapter:
    return CanonicalCohortRankingAdapter(source)


@pytest.mark.asyncio
async def test_healthy_and_single_noisy_members_are_excluded_but_unknown_is_kept() -> (
    None
):
    healthy_a = _observation("healthy_a")
    healthy_b = _observation("healthy_b")
    noisy_observation = _observation("noisy_review")
    noisy = _finding(
        "noisy_review", HealthDimension.REVIEW_CI_PRESSURE, DimensionState.AT_RISK
    )
    pressure_a = _observation("pressure_review")
    pressure_b = _observation("pressure_delivery")
    pressure = (
        _finding(
            "pressure_review",
            HealthDimension.REVIEW_CI_PRESSURE,
            DimensionState.AT_RISK,
        ),
        _finding(
            "pressure_delivery",
            HealthDimension.DELIVERY_FLOW,
            DimensionState.CRITICAL,
        ),
    )
    source = _FakeEnrichment(
        {
            "project_healthy": _enrichment(health=_profile((healthy_a, healthy_b))),
            "project_noisy": _enrichment(
                health=_profile((noisy_observation,), shadow_findings=(noisy,))
            ),
            "project_pressure": _enrichment(
                health=_profile((pressure_a, pressure_b), shadow_findings=pressure)
            ),
            "project_unknown": _enrichment(health=EnrichmentGap.UNAVAILABLE),
        }
    )

    result = await _adapter(source).rank(
        proposal=_proposal(
            "project_unknown",
            "project_noisy",
            "project_healthy",
            "project_pressure",
        ),
        authorized_entity_ids=frozenset(source.by_subject),
        scope=_scope(),
        permission_fingerprint=_PERMISSION,
        now=_NOW,
    )

    assert source.calls == [
        "project_healthy",
        "project_noisy",
        "project_pressure",
        "project_unknown",
    ]
    assert [item.candidate.canonical_id for item in result.ranked_members] == [
        "project_pressure",
        "project_unknown",
    ]
    assert [item.disposition for item in result.ranked_members] == [
        CohortRankDisposition.INCLUDED,
        CohortRankDisposition.UNKNOWN,
    ]
    exclusions = {item.canonical_id: item for item in result.exclusions}
    assert set(exclusions) == {"project_healthy", "project_noisy"}
    assert (
        exclusions["project_healthy"].reason
        is CohortExclusionReason.EXCLUDED_BY_QUESTION
    )
    assert "no canonical pressure" in exclusions["project_healthy"].rationale
    assert (
        exclusions["project_noisy"].reason is CohortExclusionReason.EXCLUDED_BY_QUESTION
    )
    assert "single" in exclusions["project_noisy"].rationale


@pytest.mark.asyncio
async def test_stale_denominator_and_coverage_states_are_retained() -> None:
    stale_observation = _observation(
        "stale_capacity",
        observed_state=SourceRequirementState.AVAILABLE_STALE,
        coverage=0.4,
        denominator_present=False,
        data_semantics="no_data",
    )
    stale_finding = _finding(
        "stale_capacity",
        HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        DimensionState.AT_RISK,
    )
    stale_metric = SimpleNamespace(
        metric_ref_id="metric:stale",
        metric_id="avg_wip",
        freshness=FreshnessState.STALE,
        coverage=0.4,
        value=12.0,
        evidence_ref_ids=["metric-source:stale"],
    )
    source = _FakeEnrichment(
        {
            "project_stale": _enrichment(
                health=_profile((stale_observation,), shadow_findings=(stale_finding,)),
                metrics=(stale_metric,),
            ),
            "project_not_applicable": _enrichment(
                health=EnrichmentGap.NOT_APPLICABLE,
                workload=EnrichmentGap.NOT_APPLICABLE,
                readiness=EnrichmentGap.NOT_APPLICABLE,
                status=EnrichmentGap.NOT_APPLICABLE,
            ),
        }
    )

    result = await _adapter(source).rank(
        proposal=_proposal("project_stale", "project_not_applicable"),
        authorized_entity_ids=frozenset(source.by_subject),
        scope=_scope(),
        permission_fingerprint=_PERMISSION,
        now=_NOW,
    )

    stale = next(
        item
        for item in result.ranked_members
        if item.candidate.canonical_id == "project_stale"
    )
    stale_signal = next(
        item for item in stale.signals if item.signal_id == "health:stale_capacity"
    )
    assert stale_signal.observed_states == (SourceRequirementState.AVAILABLE_STALE,)
    assert stale_signal.coverage == 0.4
    assert stale_signal.denominator_present is False
    assert stale_signal.data_semantics == "no_data"
    assert stale_signal.state is DimensionState.AT_RISK
    metric_signal = next(item for item in stale.signals if item.source == "metrics")
    assert metric_signal.freshness is FreshnessState.STALE
    assert metric_signal.coverage == 0.4

    not_applicable = next(
        item
        for item in result.ranked_members
        if item.candidate.canonical_id == "project_not_applicable"
    )
    assert not_applicable.disposition is CohortRankDisposition.UNKNOWN
    assert {signal.observed_states[0] for signal in not_applicable.signals} == {
        SourceRequirementState.NOT_APPLICABLE
    }


@pytest.mark.asyncio
async def test_unknown_observation_is_not_collapsed_to_healthy() -> None:
    unknown = _observation(
        "unknown_capacity",
        observed_state=SourceRequirementState.AVAILABLE_UNKNOWN,
        data_semantics="no_data",
    )
    source = _FakeEnrichment(
        {"project_unknown_observation": _enrichment(health=_profile((unknown,)))}
    )

    result = await _adapter(source).rank(
        proposal=_proposal("project_unknown_observation"),
        authorized_entity_ids=frozenset(source.by_subject),
        scope=_scope(),
        permission_fingerprint=_PERMISSION,
        now=_NOW,
    )

    member = result.ranked_members[0]
    signal = next(
        item for item in member.signals if item.signal_id.endswith("unknown_capacity")
    )
    assert member.disposition is CohortRankDisposition.UNKNOWN
    assert signal.state is DimensionState.UNKNOWN
    assert signal.observed_states == (SourceRequirementState.AVAILABLE_UNKNOWN,)


@pytest.mark.asyncio
async def test_ranking_is_deterministic_per_dimension_with_no_composite_score() -> None:
    def pressured(subject: str) -> CanonicalEnrichment:
        first = _observation(f"{subject}:review")
        second = _observation(f"{subject}:delivery")
        findings = (
            _finding(
                f"{subject}:review",
                HealthDimension.REVIEW_CI_PRESSURE,
                DimensionState.AT_RISK,
            ),
            _finding(
                f"{subject}:delivery",
                HealthDimension.DELIVERY_FLOW,
                DimensionState.AT_RISK,
            ),
        )
        return _enrichment(health=_profile((second, first), shadow_findings=findings))

    source_a = _FakeEnrichment(
        {subject: pressured(subject) for subject in ("project_b", "project_a")}
    )
    source_b = _FakeEnrichment(
        {subject: pressured(subject) for subject in ("project_a", "project_b")}
    )
    proposal = _proposal("project_b", "project_a")

    first = await _adapter(source_a).rank(
        proposal=proposal,
        authorized_entity_ids=frozenset(source_a.by_subject),
        scope=_scope(),
        permission_fingerprint=_PERMISSION,
        now=_NOW,
    )
    second = await _adapter(source_b).rank(
        proposal=proposal,
        authorized_entity_ids=frozenset(source_b.by_subject),
        scope=_scope(),
        permission_fingerprint=_PERMISSION,
        now=_NOW,
    )

    assert [item.candidate.canonical_id for item in first.ranked_members] == [
        "project_a",
        "project_b",
    ]
    assert [item.candidate.canonical_id for item in first.ranked_members] == [
        item.candidate.canonical_id for item in second.ranked_members
    ]
    assert not hasattr(first.ranked_members[0], "score")
    assert not hasattr(first.ranked_members[0], "composite_score")


@pytest.mark.asyncio
async def test_removing_one_pressure_dimension_flips_the_member_to_noisy_exclusion() -> (
    None
):
    first = _observation("review")
    second = _observation("delivery")
    review = _finding(
        "review", HealthDimension.REVIEW_CI_PRESSURE, DimensionState.AT_RISK
    )
    delivery = _finding(
        "delivery", HealthDimension.DELIVERY_FLOW, DimensionState.AT_RISK
    )

    async def rank_with(findings: tuple[SimpleNamespace, ...]):
        source = _FakeEnrichment(
            {
                "project_mutation": _enrichment(
                    health=_profile((first, second), shadow_findings=findings)
                )
            }
        )
        return await _adapter(source).rank(
            proposal=_proposal("project_mutation"),
            authorized_entity_ids=frozenset(source.by_subject),
            scope=_scope(),
            permission_fingerprint=_PERMISSION,
            now=_NOW,
        )

    included = await rank_with((review, delivery))
    noisy = await rank_with((review,))
    assert [item.candidate.canonical_id for item in included.ranked_members] == [
        "project_mutation"
    ]
    assert not included.exclusions
    assert not noisy.ranked_members
    assert noisy.exclusions[0].reason is CohortExclusionReason.EXCLUDED_BY_QUESTION
    assert "single" in noisy.exclusions[0].rationale


@pytest.mark.asyncio
async def test_unauthorized_cohort_member_is_not_called_or_named() -> None:
    visible = _profile((_observation("visible_a"), _observation("visible_b")))
    hidden = _profile((_observation("hidden_a"), _observation("hidden_b")))
    source = _FakeEnrichment(
        {
            "project_visible": _enrichment(health=visible),
            "project_hidden": _enrichment(health=hidden),
        }
    )

    result = await _adapter(source).rank(
        proposal=_proposal("project_hidden", "project_visible"),
        authorized_entity_ids=frozenset({"project_visible"}),
        scope=_scope(),
        permission_fingerprint=_PERMISSION,
        now=_NOW,
    )

    assert source.calls == ["project_visible"]
    assert result.authorization_filtered_count == 1
    disclosed_ids = {item.candidate.canonical_id for item in result.ranked_members} | {
        item.canonical_id for item in result.exclusions
    }
    assert "project_hidden" not in disclosed_ids
