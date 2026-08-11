"""CHAOS-3669: lossless public projection of canonical cohort ranking facts."""

from dev_health_ops.api.dev.canonical_enrichment import EnrichmentGap
from dev_health_ops.api.dev.contracts import (
    DevAnswerCohortDisposition,
    DevAnswerCohortSignalSource,
    DevAnswerEnrichmentGap,
    DevAnswerEvidenceSourceClass,
    DevAnswerPressureDimension,
    DevAnswerPressureState,
    DevAnswerSourceRequirementState,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.health_rules import (
    DimensionState,
    HealthDimension,
)
from dev_health_ops.api.dev.graph_investigation_query import CohortDiscoveryFamily
from dev_health_ops.api.dev.investigation_contract import (
    AskDevInvestigationPacket,
    CohortCompleteness,
    CohortExclusion,
    CohortExclusionReason,
    InvestigationSubjectKind,
)
from dev_health_ops.api.dev.investigation_corpus.reference import reference_packet
from dev_health_ops.api.dev.orchestrator import _public_cohort_slot
from dev_health_ops.context_fabric.graph_arm.cohort import CohortCandidate
from dev_health_ops.context_fabric.graph_arm.cohort_ranking import (
    CanonicalCohortSignal,
    CohortRankDisposition,
    CohortRankedMember,
    CohortRankingResult,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind


def test_public_vocabularies_are_total_over_internal_ranking_vocabularies() -> None:
    assert {item.value for item in DevAnswerSourceRequirementState} == {
        item.value for item in SourceRequirementState
    }
    assert {item.value for item in DevAnswerPressureDimension} == {
        item.value for item in HealthDimension
    }
    assert {item.value for item in DevAnswerPressureState} == {
        item.value for item in DimensionState
    }
    assert {item.value for item in DevAnswerEnrichmentGap} == {
        item.value for item in EnrichmentGap
    }
    assert {item.value for item in DevAnswerEvidenceSourceClass} == {
        item.value for item in SourceClass
    }
    assert {item.value for item in DevAnswerCohortSignalSource} == {
        "status",
        "health",
        "workload",
        "readiness",
        "metrics",
        "canonical_enrichment",
    }


def test_projection_preserves_rank_signal_quality_and_unknown_disposition() -> None:
    payload = reference_packet("S03_shared_dependency_portfolio_risk")
    payload["organization_id"] = "org_projection_test"
    packet = AskDevInvestigationPacket.model_validate(payload)
    packet_member = packet.comparison_cohort.members[0]
    signal = CanonicalCohortSignal(
        signal_id="health:capacity",
        source="health",
        observed_states=(SourceRequirementState.AVAILABLE_STALE,),
        data_semantics="no_data",
        freshness=FreshnessState.STALE,
        coverage=0.4,
        denominator_present=False,
        attribution_present=False,
        dimension=HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        state=DimensionState.UNKNOWN,
        evidence_ref_ids=("evidence:capacity",),
        evidence_source_classes=(SourceClass.WORK_GRAPH,),
        limitation="partial canonical coverage",
        gap=EnrichmentGap.NO_DATA,
    )
    ranked = CohortRankedMember(
        candidate=CohortCandidate(
            canonical_id=packet_member.canonical_id,
            kind=GraphEntityKind.PROJECT,
            display_label=packet_member.display_label,
            basis_anchors=tuple((basis, ()) for basis in packet_member.inclusion_basis),
        ),
        signals=(signal,),
        disposition=CohortRankDisposition.UNKNOWN,
        pressure_dimensions=(HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,),
    )
    slot = _public_cohort_slot(
        packet=packet,
        family=CohortDiscoveryFamily.PROJECT_CAPACITY,
        ranking=CohortRankingResult(
            ranked_members=(ranked,),
            exclusions=(),
            authorization_filtered_count=0,
        ),
    )

    assert slot is not None
    member = slot.members[0]
    assert member.rank == 1
    assert member.disposition is DevAnswerCohortDisposition.UNKNOWN
    assert member.inclusion_rationale is None
    assert packet_member.inclusion_rationale not in slot.model_dump_json()
    assert member.pressure_dimensions == [
        DevAnswerPressureDimension.COGNITIVE_WORKLOAD_PRESSURE
    ]
    projected = member.signals[0]
    assert projected.observed_states == [
        DevAnswerSourceRequirementState.AVAILABLE_STALE
    ]
    assert projected.data_semantics == "no_data"
    assert projected.freshness is FreshnessState.STALE
    assert projected.coverage == 0.4
    assert projected.denominator_present is False
    assert projected.attribution_present is False
    assert projected.dimension is DevAnswerPressureDimension.COGNITIVE_WORKLOAD_PRESSURE
    assert projected.state is DevAnswerPressureState.UNKNOWN
    assert projected.evidence_source_classes == [
        DevAnswerEvidenceSourceClass.WORK_GRAPH
    ]
    assert projected.limitation is None
    assert signal.limitation is not None
    assert signal.limitation not in slot.model_dump_json()
    assert projected.gap is DevAnswerEnrichmentGap.NO_DATA


def test_packet_exclusions_and_uncertain_completeness_are_disclosed_without_ids() -> (
    None
):
    payload = reference_packet("S03_shared_dependency_portfolio_risk")
    payload["organization_id"] = "org_projection_test"
    packet = AskDevInvestigationPacket.model_validate(payload)
    packet_member = packet.comparison_cohort.members[0]
    cohort = packet.comparison_cohort.model_copy(
        update={
            "completeness": CohortCompleteness.BEST_EFFORT_UNCERTAIN,
            "exclusions": (
                CohortExclusion(
                    subject_kind=InvestigationSubjectKind.PROJECT,
                    canonical_id="private_project_not_for_public_output",
                    reason=CohortExclusionReason.EXCLUDED_BY_QUESTION,
                    rationale="Not relevant to this comparison.",
                ),
            ),
        }
    )
    packet = packet.model_copy(update={"comparison_cohort": cohort})
    ranked = CohortRankedMember(
        candidate=CohortCandidate(
            canonical_id=packet_member.canonical_id,
            kind=GraphEntityKind.PROJECT,
            display_label=packet_member.display_label,
            basis_anchors=tuple((basis, ()) for basis in packet_member.inclusion_basis),
        ),
        signals=(
            CanonicalCohortSignal(
                signal_id="canonical_enrichment",
                source="canonical_enrichment",
                observed_states=(SourceRequirementState.AVAILABLE_UNKNOWN,),
                data_semantics="no_data",
                gap=EnrichmentGap.NO_DATA,
            ),
        ),
        disposition=CohortRankDisposition.UNKNOWN,
    )

    slot = _public_cohort_slot(
        packet=packet,
        family=CohortDiscoveryFamily.PROJECT_CAPACITY,
        ranking=CohortRankingResult(
            ranked_members=(ranked,),
            exclusions=(),
            authorization_filtered_count=0,
        ),
    )

    assert slot is not None
    assert slot.cohort_complete is False
    assert slot.warnings == [
        "1 graph-discovered candidate was excluded before canonical ranking.",
        "The discovered cohort is best-effort and may be incomplete.",
    ]
    assert "private_project_not_for_public_output" not in slot.model_dump_json()
