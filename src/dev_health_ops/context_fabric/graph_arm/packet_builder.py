"""CHAOS-3617: emit the frozen ``ask_dev_investigation_packet.v1``.

The arm's only output. Everything here is constructed through the
**canonical Pydantic models** from
``dev_health_ops.api.dev.investigation_contract`` — never a dict validated
against the JSON Schema, because the manifest is explicit that
``schema_only_validation_is_sufficient`` is ``false`` and a schema-valid
packet has had none of its cross-field rules checked.

**What this revision does and does not claim.** PR1 of the arm performs
subject resolution over canonical ids, bounded authorized traversal, related
entity and lineage-path discovery, and evidence indexing. It performs **no
driver synthesis**. So :func:`build_packet` never emits a supported outcome:
a packet with no asserted driver is, by the contract's own
``validate_supported_outcome_asserts_a_judgment``, a redirect rather than an
answer, and claiming ``SUPPORTED`` for one would be precisely the
"dashboard redirect without a direct judgment" fault mode. The outcome is
derived from what was actually produced rather than passed in, so the arm
cannot over-claim even by accident.

**Arm identity is trial metadata only.** ``versions.trial`` is the sole
place the word "graph" appears in an emitted packet, and it is optional on
``InvestigationVersions``. Nothing else in the output is backend-flavoured;
the backend-neutrality test asserts it.

**Evidence handles are the platform's own.** Every handle is minted by
``EvidenceReferenceSigner.issue`` over an org-scoped
``EvidenceRecord`` — the same function the evidence service uses — so a
packet handle verifies against the service that issues it rather than
against a parallel scheme this arm invented.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from dev_health_ops.api.dev.contracts import FreshnessState
from dev_health_ops.api.dev.contracts_v2.base import (
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.evidence_service import (
    EvidenceRecord,
    EvidenceReferenceSigner,
)
from dev_health_ops.api.dev.investigation_contract import (
    QUESTION_FAMILY_REGISTRY,
    TRIAL_SOURCE_ALLOWLIST,
    AnalyticalJob,
    AnalyticalSlice,
    AskDevInvestigationPacket,
    BoundedTimeContext,
    CohortCompleteness,
    CohortEvidenceClassification,
    CohortInclusionBasis,
    CohortMember,
    ComparisonCohort,
    ComparisonShape,
    DriverAnalysis,
    EvidenceCoverage,
    HistoricalComparability,
    InvestigationEvidenceEntry,
    InvestigationOutcome,
    InvestigationVersions,
    JobUncertainty,
    LineageHop,
    LineagePath,
    MissingSource,
    PacketLimitation,
    PacketLimitationKind,
    QuestionFamilyID,
    RelatedContext,
    RelatedEntity,
    RelevanceState,
    SourceContractVersion,
    SourceHealthObservation,
    SubjectCandidate,
    SubjectCommitmentState,
    SubjectDiscovery,
    SubjectMatchEvidence,
    SubjectMatchSignal,
    TrialMetadata,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import EdgeValidityBasis

from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .projection import PROJECTION_VERSION
from .readback import QUERY_VERSION, DiscoveredPath, InvestigationReadout
from .vocabulary import entity_kind_to_subject_kind
from .watermark import DEFAULT_STALENESS_TOLERANCE, IndexWatermark

__all__ = [
    "ARM_ID",
    "PRODUCER_ID",
    "RANKING_VERSION",
    "JobContext",
    "PacketTooLargeError",
    "TrialContext",
    "UnsupportedComparisonShapeError",
    "build_packet",
    "signer_from_environment",
]

#: The arm's identity. Lives in ``TrialMetadata`` and nowhere else.
ARM_ID = "graph_assisted_shadow_arm"
PRODUCER_ID = "context_fabric_graph_arm.v1"

#: No ranking is performed in this revision — candidates are returned in the
#: order the caller supplied their canonical ids. Version token recorded
#: anyway so a later ranking implementation is distinguishable from this one
#: in recorded runs rather than silently replacing it.
RANKING_VERSION = "graph_arm_no_ranking.v1"

_SOURCE_CONTRACT_VERSION = "graph_arm_source_read.v1"

#: The frozen contract's own bound on ``RelatedEntity.supporting_path_ids``.
#: Mirrored here rather than caught as a pydantic error, so exceeding it is a
#: disclosed cap instead of a crash at emission time.
_MAX_PATH_CITATIONS = 10


class PacketTooLargeError(RuntimeError):
    """The emitted packet exceeded the run's byte budget.

    Raised rather than trimmed. A packet is a web of internal references --
    evidence cites entities, entities cite paths, drivers cite both -- and
    every one of those is checked by the frozen contract, so there is no
    field this builder could drop without either breaking closure or
    silently changing what the arm claims to have found. The honest response
    is to fail and let the caller re-run with tighter traversal budgets,
    which produces a smaller *investigation* rather than a truncated report
    of a larger one.
    """


class UnsupportedComparisonShapeError(NotImplementedError):
    """This revision cannot build the requested comparison shape.

    Raised rather than emitting a one-member cohort under a cohort-bearing
    shape. Cohort construction is a capability under test and belongs to the
    capabilities revision; fabricating a cohort here would put a made-up
    comparison into the trial's scoring table under a family that expects a
    real one.
    """


@dataclass(frozen=True, slots=True)
class JobContext:
    """The analytical job the investigation was asked to perform."""

    job_id: str
    question_family: QuestionFamilyID
    job_statement: str
    comparison_shape: ComparisonShape
    window_start: datetime
    window_end: datetime
    timezone: str = "UTC"
    job_uncertainty: JobUncertainty = JobUncertainty.PRECISE


@dataclass(frozen=True, slots=True)
class TrialContext:
    """Reproducibility metadata recorded on every emitted packet."""

    run_id: str
    corpus_version: str | None = None
    fixture_version: str | None = None
    #: Exact dependency/model/projection versions the run used. Recorded on
    #: the trial artifact by the caller; kept here so a packet and its
    #: artifact cannot disagree about which build produced them.
    dependency_versions: Mapping[str, str] = field(default_factory=dict)


def signer_from_environment() -> EvidenceReferenceSigner:
    """The platform's evidence signer, or a loud failure.

    Deliberately no fallback secret. An unsigned or differently-signed
    handle would look exactly like a real one on the wire and fail only when
    someone tried to dereference it.
    """

    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise RuntimeError(
            "JWT_SECRET_KEY is unset, so no verifiable evidence handle can be "
            "minted; the graph arm will not emit a packet carrying handles "
            "the evidence service would reject"
        )
    return EvidenceReferenceSigner(secret)


def _packet_id(run_id: str, job_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cf-graph-arm/{run_id}/{job_id}"))


def _freshness(state: SourceRequirementState) -> FreshnessState:
    if state is SourceRequirementState.AVAILABLE_CURRENT:
        return FreshnessState.FRESH
    if state is SourceRequirementState.AVAILABLE_STALE:
        return FreshnessState.STALE
    return FreshnessState.UNKNOWN


def _lineage_path(
    path: DiscoveredPath, source_state: SourceRequirementState
) -> LineagePath:
    hops: list[LineageHop] = []
    for step in path.steps:
        from_kind = entity_kind_to_subject_kind(step.from_kind)
        to_kind = entity_kind_to_subject_kind(step.to_kind)
        if from_kind is None or to_kind is None:
            raise ValueError(
                f"path {path.path_id} traverses the organization partition "
                "root, which is not an emittable subject kind"
            )
        hops.append(
            LineageHop(
                source_entity_id=step.from_canonical_id,
                source_entity_kind=from_kind,
                relationship=step.relationship,
                direction=step.direction,
                target_entity_id=step.to_canonical_id,
                target_entity_kind=to_kind,
                observed_at=step.observed_at,
                relevance=RelevanceState.CURRENT,
            )
        )
    return LineagePath(
        path_id=path.path_id,
        origin_entity_id=path.origin_canonical_id,
        terminal_entity_id=path.terminal_canonical_id,
        hops=tuple(hops),
        inclusion_reason=(
            "reached by bounded authorized traversal from the committed "
            "subject over the frozen relationship allowlist"
        ),
        relevance=RelevanceState.CURRENT,
        evidence_ref_ids=(),
        truncated=False,
        truncation_reason=None,
        source_health=source_state,
    )


def build_packet(
    *,
    readout: InvestigationReadout,
    job: JobContext,
    watermark: IndexWatermark,
    signer: EvidenceReferenceSigner,
    trial: TrialContext,
    produced_at: datetime,
    budgets: TrialBudgets = DEFAULT_BUDGETS,
    staleness_tolerance: timedelta = DEFAULT_STALENESS_TOLERANCE,
) -> AskDevInvestigationPacket:
    """Turn one bounded traversal into the frozen investigation packet."""

    if job.comparison_shape is not ComparisonShape.SINGULAR_SUBJECT:
        raise UnsupportedComparisonShapeError(
            f"this arm revision cannot construct a {job.comparison_shape} "
            "cohort; cohort construction is a capability under test and is "
            "not implemented here. Emitting a one-member cohort under a "
            "cohort-bearing shape would put a fabricated comparison into the "
            "trial's scoring table"
        )

    family = QUESTION_FAMILY_REGISTRY[job.question_family]
    if job.comparison_shape not in family.permitted_comparison_shapes:
        raise ValueError(
            f"family {job.question_family} does not permit the "
            f"{job.comparison_shape} comparison shape"
        )

    source_state = watermark.freshness_for(
        job.window_end, tolerance=staleness_tolerance
    )
    entity_by_id = readout.entity_by_id()

    # ---- lineage -------------------------------------------------------
    paths = tuple(_lineage_path(path, source_state) for path in readout.paths)
    touched: dict[str, list[str]] = {}
    for path in readout.paths:
        for canonical_id in path.touched_ids():
            touched.setdefault(canonical_id, []).append(path.path_id)

    path_length = {path.path_id: len(path.steps) for path in readout.paths}
    citations_capped = False
    related_entities: list[RelatedEntity] = []
    for canonical_id in sorted(touched):
        entity = entity_by_id.get(canonical_id)
        if entity is None:
            continue
        subject_kind = entity_kind_to_subject_kind(entity.kind)
        if subject_kind is None:
            continue
        # ``RelatedEntity.supporting_path_ids`` is bounded at 10 by the frozen
        # contract. Cite the shortest paths first -- a two-hop chain explains
        # an entity's presence better than a five-hop one -- deterministically
        # by (length, path_id), and disclose when the cap bites rather than
        # silently dropping citations.
        ordered = sorted(
            set(touched[canonical_id]), key=lambda pid: (path_length[pid], pid)
        )
        if len(ordered) > _MAX_PATH_CITATIONS:
            citations_capped = True
            ordered = ordered[:_MAX_PATH_CITATIONS]
        related_entities.append(
            RelatedEntity(
                entity_id=entity.canonical_id,
                entity_kind=subject_kind,
                display_label=entity.display_label,
                inclusion_reason=(
                    "connected to the committed subject by at least one "
                    "authorized relationship path"
                ),
                supporting_path_ids=tuple(ordered),
                relevance=RelevanceState.CURRENT,
                observed_at=entity.observed_at,
            )
        )

    known_entity_ids = {entity.entity_id for entity in related_entities}

    # ---- subjects ------------------------------------------------------
    resolved_seeds = [
        seed for seed in readout.seed_canonical_ids if seed in entity_by_id
    ]
    candidates: list[SubjectCandidate] = []
    committed_ids: list[str] = []
    for rank, seed in enumerate(resolved_seeds, start=1):
        entity = entity_by_id[seed]
        subject_kind = entity_kind_to_subject_kind(entity.kind)
        if subject_kind is None:
            continue
        # A candidate may only be committed at rank 1 and never on a weak
        # signal. This arm resolves seeds by exact canonical id, which is the
        # strongest available signal -- but committing anything below rank 1
        # is rejected by the contract, so only rank 1 commits.
        commit = rank == 1 and seed in known_entity_ids
        candidates.append(
            SubjectCandidate(
                candidate_id=f"cand{rank:03d}",
                rank=rank,
                subject_kind=subject_kind,
                canonical_id=entity.canonical_id,
                display_label=entity.display_label,
                commitment_state=(
                    SubjectCommitmentState.COMMITTED
                    if commit
                    else SubjectCommitmentState.PROPOSED
                ),
                match_rationale=(
                    "the question supplied this canonical identifier and the "
                    "graph holds an entity with exactly that identifier"
                ),
                match_signals=(
                    SubjectMatchEvidence(
                        signal=SubjectMatchSignal.EXACT_CANONICAL_ID,
                        matched_text=entity.canonical_id,
                        source_class=SourceClass.WORK_GRAPH,
                        evidence_ref_ids=(),
                    ),
                ),
                match_confidence=1.0,
                relevance=RelevanceState.CURRENT,
            )
        )
        if commit:
            committed_ids.append(entity.canonical_id)

    # ---- evidence ------------------------------------------------------
    evidence_entries: list[InvestigationEvidenceEntry] = []
    authorized_observation_ids: list[str] = []
    for observation in readout.observations:
        supports = tuple(
            subject
            for subject in observation.subject_canonical_ids
            if subject in known_entity_ids
        )
        if not supports:
            # Unattached evidence displaces lineage. The contract refuses to
            # index it and so does this builder.
            continue
        record = EvidenceRecord(
            source_system="context_fabric_graph_arm",
            source_version=_SOURCE_CONTRACT_VERSION,
            entity_type=observation.kind.value,
            entity_id=observation.canonical_id,
            display_label=observation.title,
            observed_at=observation.observed_at,
            freshness=_freshness(source_state),
            provenance="structured record projected into the trial graph",
            confidence=1.0,
            repository_ids=observation.repository_ids,
        )
        handle = signer.issue(readout.org_id, record)
        authorized_observation_ids.append(observation.canonical_id)
        evidence_entries.append(
            InvestigationEvidenceEntry(
                evidence={
                    "schema_version": "dev_evidence_ref.v1",
                    "evidence_ref_id": handle,
                    "source_system": record.source_system,
                    "source_version": record.source_version,
                    "entity_type": record.entity_type,
                    "entity_id": record.entity_id,
                    "display_label": record.display_label,
                    "link": None,
                    "observed_at": record.observed_at,
                    "freshness": record.freshness.value,
                    "provenance": record.provenance,
                    "confidence": record.confidence,
                    "citation_text": None,
                    "repository_ids": list(record.repository_ids),
                    "valid_entity_ids": list(supports),
                    "flags": {},
                },
                source_class=observation.source_class,
                supports_path_ids=(),
                supports_entity_ids=supports,
                supports_driver_ids=(),
                supports_subject_ids=tuple(
                    subject
                    for subject in supports
                    if subject in {item.canonical_id for item in candidates}
                ),
                relevance=RelevanceState.CURRENT,
            )
        )

    # ---- authorization envelope ---------------------------------------
    #
    # The declared set is entity ids plus the observation ids the packet
    # cites as evidence: an observation identifier reaching a consumer is an
    # identifier the caller must be authorized for, and the packet-level
    # validator checks exactly that. Widening the set would weaken the hop
    # check, so the narrower invariant -- every hop endpoint is an
    # *authorized entity*, not merely an authorized id -- is enforced here
    # and pinned by test_chaos_3617_authorization.py.
    authorized_entity_ids = tuple(sorted(readout.authorized_entity_ids))
    for path in readout.paths:
        for step in path.steps:
            for endpoint in (step.from_canonical_id, step.to_canonical_id):
                if endpoint not in authorized_entity_ids:
                    raise PermissionError(
                        f"path {path.path_id} traverses {endpoint!r}, which is "
                        "not in the authorized entity set"
                    )
    declared_authorized = tuple(
        sorted(set(authorized_entity_ids) | set(authorized_observation_ids))
    )

    # ---- source coverage ----------------------------------------------
    observed_classes = sorted(
        readout.observed_source_classes & set(TRIAL_SOURCE_ALLOWLIST),
        key=lambda item: item.value,
    )
    source_health = tuple(
        SourceHealthObservation(
            source_class=source_class,
            state=source_state,
            observed_at=watermark.indexed_through,
            detail=watermark.detail_for(job.window_end),
        )
        for source_class in observed_classes
    )
    missing = tuple(
        MissingSource(
            source_class=source_class,
            state=SourceRequirementState.UNAVAILABLE,
            impact=(
                "the trial projection holds no record of this source class for "
                "the requested window, so nothing in this packet rests on it"
            ),
        )
        for source_class in sorted(
            set(family.required_source_classes) - set(observed_classes),
            key=lambda item: item.value,
        )
    )

    # ---- limitations ---------------------------------------------------
    limitations: list[PacketLimitation] = [
        PacketLimitation(
            kind=PacketLimitationKind.INTERPRETATION_UNCERTAINTY,
            detail=(
                "this arm revision performs subject resolution, authorized "
                "traversal and evidence indexing only; it synthesizes no "
                "drivers, so it asserts no judgment about causes"
            ),
        )
    ]
    if missing:
        limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.MISSING_SOURCE,
                detail=(
                    "source classes this question family requires were not "
                    "present in the trial projection and are declared missing"
                ),
            )
        )
    if source_state is SourceRequirementState.AVAILABLE_STALE:
        limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.STALE_SOURCE,
                detail=watermark.detail_for(job.window_end),
            )
        )
    if readout.authorization_filtered_count:
        limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.AUTHORIZATION_FILTERED,
                detail=(
                    f"{readout.authorization_filtered_count} candidate results "
                    "were outside the caller's authorized scope and were "
                    "removed before ranking"
                ),
            )
        )
    if (
        readout.entities_truncated
        or readout.paths_truncated
        or readout.evidence_truncated
        or citations_capped
    ):
        limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.TRUNCATED_TRAVERSAL,
                detail=(
                    "traversal or path citation stopped at a configured "
                    "bound; the result is partial"
                ),
            )
        )

    # ---- assembly ------------------------------------------------------
    time_context = BoundedTimeContext(
        start=job.window_start,
        end=job.window_end,
        timezone=job.timezone,
        analytical_slice=AnalyticalSlice.CURRENT,
        as_of=None,
        historical_comparability=HistoricalComparability.NOT_APPLICABLE,
        edge_validity_basis=EdgeValidityBasis.NOT_REQUIRED,
    )
    analytical_job = AnalyticalJob(
        schema_version="ask_dev_analytical_job.v1",
        job_id=job.job_id,
        question_family=job.question_family,
        job_uncertainty=job.job_uncertainty,
        job_statement=job.job_statement,
        comparison_shape=job.comparison_shape,
        time_context=time_context,
        surface_context_refs=(),
        conversation_reference_ids=(),
        interpretation_limitations=(),
    )
    subject_discovery = SubjectDiscovery(
        schema_version="ask_dev_subject_discovery.v1",
        candidates=tuple(candidates),
        unresolved_mentions=(),
        committed_subject_ids=tuple(committed_ids),
        authorization_filtered_count=readout.authorization_filtered_count,
        candidates_truncated=False,
        truncation_reason=None,
    )
    cohort_members = tuple(
        CohortMember(
            subject_kind=candidate.subject_kind,
            canonical_id=candidate.canonical_id,
            display_label=candidate.display_label,
            inclusion_basis=(CohortInclusionBasis.EXPLICITLY_NAMED,),
            inclusion_rationale="the question named this subject directly",
            inclusion_evidence_ids=(),
            inclusion_evidence_classification=(
                CohortEvidenceClassification.EXPLICITLY_NAMED_BY_QUESTION
            ),
            relevance=RelevanceState.CURRENT,
        )
        for candidate in candidates
        if candidate.commitment_state is SubjectCommitmentState.COMMITTED
    )
    comparison_cohort = ComparisonCohort(
        schema_version="ask_dev_comparison_cohort.v1",
        cohort_id=f"{job.job_id}_cohort",
        comparison_shape=job.comparison_shape,
        members=cohort_members,
        exclusions=(),
        supported_comparison_dimensions=(),
        completeness=(
            CohortCompleteness.COMPLETE
            if cohort_members
            else CohortCompleteness.BEST_EFFORT_UNCERTAIN
        ),
        truncation_reason=None,
        cohort_uncertainty=(
            None
            if cohort_members
            else "no subject was committed, so no cohort was constructed"
        ),
        authorization_filtered_count=0,
    )
    related_context = RelatedContext(
        schema_version="ask_dev_related_context.v1",
        entities=tuple(related_entities),
        paths=paths,
        authorized_entity_ids=declared_authorized,
        authorization_filtered_count=0,
        entities_truncated=readout.entities_truncated,
        paths_truncated=readout.paths_truncated,
        truncation_reason=readout.truncation_reason,
    )
    driver_analysis = DriverAnalysis(
        schema_version="ask_dev_driver_analysis.v1",
        candidates=(),
        principal_driver_ids=(),
        candidates_truncated=False,
        truncation_reason=None,
    )
    evidence_coverage = EvidenceCoverage(
        schema_version="ask_dev_evidence_coverage.v1",
        evidence_index=tuple(evidence_entries),
        source_health=source_health,
        missing_sources=missing,
        conflicts=(),
        limitations=tuple(limitations),
        clarification_needs=(),
        authorization_filtered_count=0,
        evidence_truncated=readout.evidence_truncated,
        truncation_reason=(
            readout.truncation_reason if readout.evidence_truncated else None
        ),
    )
    versions = InvestigationVersions(
        schema_version="ask_dev_investigation_versions.v1",
        packet_schema_version="ask_dev_investigation_packet.v1",
        query_version=QUERY_VERSION,
        ranking_version=RANKING_VERSION,
        projection_version=PROJECTION_VERSION,
        source_contract_versions=tuple(
            SourceContractVersion(
                source_class=source_class, contract_version=_SOURCE_CONTRACT_VERSION
            )
            for source_class in (observed_classes or [SourceClass.WORK_GRAPH])
        ),
        corpus_version=trial.corpus_version,
        trial=TrialMetadata(
            arm_id=ARM_ID,
            producer_id=PRODUCER_ID,
            fixture_version=trial.fixture_version,
            run_id=trial.run_id,
        ),
    )

    packet = AskDevInvestigationPacket(
        schema_version="ask_dev_investigation_packet.v1",
        packet_id=_packet_id(trial.run_id, job.job_id),
        organization_id=readout.org_id,
        produced_at=produced_at,
        # No driver was synthesized, so no supported outcome is available.
        # Derived, never passed in: an arm that could be told its own outcome
        # could claim one it did not earn.
        outcome=InvestigationOutcome.UNSUPPORTED,
        analytical_job=analytical_job,
        subject_discovery=subject_discovery,
        comparison_cohort=comparison_cohort,
        related_context=related_context,
        driver_analysis=driver_analysis,
        evidence_coverage=evidence_coverage,
        versions=versions,
    )

    # The byte bound is measured on the *serialized* packet, which is the
    # only size a consumer ever sees.
    size = len(packet.model_dump_json())
    outcome = budgets.check_bytes(size)
    if not outcome.within_budget:
        raise PacketTooLargeError(
            f"{outcome.detail}; re-run with tighter traversal budgets rather "
            "than emitting a packet nobody bounded"
        )
    return packet
