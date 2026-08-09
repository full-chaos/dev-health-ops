"""Project a completed native Ask Dev run into ``ask_dev_investigation_packet.v1``.

This is a **pure function over a finished run**. It performs no queries, no
model calls and no traversal of its own: everything it emits already exists
in the run's interpretation, resolution ledger, subject set and
``DevInvestigationResult``. That is deliberate and it is the honesty
property — an arm that could go and fetch more would no longer be measuring
what the current product assembles.

Two rules govern every decision here, and both are enforced rather than
documented:

**Nothing is claimed that the run did not establish.** Every relationship
this module emits is looked up in
:data:`~.capabilities.NATIVE_RELATIONSHIP_CAPABILITY` first; a native fact
with no honest contract counterpart becomes evidence, or becomes nothing,
and never becomes a lineage hop with a guessed relationship type.

**A gap is reported, never repaired.** Missing sources, absent staffing
denominators, unresolved mentions, truncation and the historical slice all
travel as first-class packet fields. The projection would rather return no
packet at all — see :class:`NativeProjectionGap` — than return a flattering
one.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from dev_health_ops.api.dev.contracts import DevEvidenceRef
from dev_health_ops.api.dev.contracts_v2.base import EntityKind, SourceClass
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.contracts_v2.result import DevInvestigationResult
from dev_health_ops.api.dev.contracts_v2.subject import (
    DevEntityRefV2,
    DevResolutionLedger,
    DevSubjectSet,
    ResolutionOutcome,
)
from dev_health_ops.api.dev.investigation_contract import (
    QUESTION_FAMILY_REGISTRY,
    AnalyticalJob,
    AnalyticalSlice,
    AskDevInvestigationPacket,
    AssertionBasis,
    BoundedTimeContext,
    ClarificationNeed,
    ClarificationNeedKind,
    CohortCompleteness,
    CohortEvidenceClassification,
    CohortInclusionBasis,
    CohortMember,
    ComparisonCohort,
    ComparisonDimension,
    ComparisonShape,
    ConfidenceQualifier,
    DriverAnalysis,
    DriverCandidate,
    DriverCategory,
    DriverRole,
    DriverStanding,
    EvidenceCoverage,
    HistoricalComparability,
    InvestigationEvidenceEntry,
    InvestigationOutcome,
    InvestigationSubjectKind,
    InvestigationVersions,
    JobUncertainty,
    MissingSource,
    PacketLimitation,
    PacketLimitationKind,
    PacketSection,
    QuestionFamilyID,
    RelatedContext,
    RelevanceState,
    SourceContractVersion,
    SourceHealthObservation,
    StaffingDenominatorState,
    StaffingQualification,
    SubjectCandidate,
    SubjectCommitmentState,
    SubjectDiscovery,
    SubjectMatchEvidence,
    SubjectMatchSignal,
    TrialMetadata,
    TruncationReason,
    UnresolvedMention,
    UnresolvedMentionReason,
)
from dev_health_ops.api.dev.question_interpreter import InterpretedQuestion

from .capabilities import (
    NATIVE_SUBJECT_KIND,
    NATIVE_UNOBSERVED_SOURCE_CLASSES,
    classify_question_family,
    comparison_shape_for,
)

__all__ = [
    "NATIVE_ARM_ID",
    "NATIVE_PROJECTION_VERSION",
    "NativeProjectionGap",
    "NativeProjectionGapReason",
    "NativeProjectionInput",
    "NativeProjectionOutcome",
    "project_native_investigation",
]

#: Trial metadata only — never product truth. ``TrialMetadata`` is an
#: optional field of ``InvestigationVersions`` precisely so arm identity
#: stays evaluation metadata (CHAOS-3615).
NATIVE_ARM_ID = "native"
NATIVE_PROJECTION_VERSION = "native_investigation_projection.v1"
_NATIVE_QUERY_VERSION = "ask_dev_native_queries.v1"
_NATIVE_RANKING_VERSION = "ask_dev_native_ranking.v1"

_NAMESPACE = uuid.UUID("6f5f3c68-2f6f-4f4a-9b0e-3618a1c0de11")

#: Sentinels only, so the dataclass can keep its keyword-defaulted shape.
#: ``__post_init__`` rejects any window that is not a real interval, so
#: leaving both at their defaults is a valid (if uninformative) 24-hour
#: window rather than a silently degenerate one.
_DEFAULT_WINDOW_START = datetime(1970, 1, 1, tzinfo=UTC)
_DEFAULT_WINDOW_END = datetime(1970, 1, 2, tzinfo=UTC)

#: ``SourceClass`` members that are not on the trial allowlist at all, so a
#: native observation carrying one is dropped rather than smuggled onto a
#: packet that ``validate_sources_are_allowlisted`` would reject.
_TRIAL_ALLOWLIST = frozenset(
    SourceClass(member.value)
    for member in SourceClass
    if member.value
    in {
        "status_change",
        "work_item",
        "work_graph",
        "pull_request",
        "code_change",
        "review",
        "ci_run",
        "test_report",
        "deployment",
        "incident",
        "operational_control",
        "source_health",
        "cognitive_load",
        "investment_allocation",
        "health_profile",
        "deficiency_inventory",
    }
)


#: The comparison dimension a trial source class genuinely measures. Only
#: the unambiguous ones appear: ``work_item`` could in principle back
#: throughput or cycle time, but only when the run asked the metric service
#: for that metric, and a cohort claiming a dimension no query produced
#: would support a comparison it cannot make. A source class absent from
#: this map contributes no dimension rather than a plausible one.
_DIMENSION_BY_SOURCE_CLASS: Mapping[SourceClass, ComparisonDimension] = {
    SourceClass.DEFICIENCY_INVENTORY: ComparisonDimension.OPEN_DEFICIENCY_COUNT,
    SourceClass.STATUS_CHANGE: ComparisonDimension.STATUS_DECLARATION_GAP,
    SourceClass.SOURCE_HEALTH: ComparisonDimension.DATA_COVERAGE,
    SourceClass.INCIDENT: ComparisonDimension.INCIDENT_LOAD,
    SourceClass.DEPLOYMENT: ComparisonDimension.DEPLOYMENT_FREQUENCY,
    SourceClass.PULL_REQUEST: ComparisonDimension.REVIEW_LOAD,
}


class NativeProjectionGapReason(StrEnum):
    """Why a native run produced no packet at all.

    A gap is a *measurement*, not an error. The corrected trial reports how
    often the current product cannot even express its own run in the shared
    contract, and that number is only meaningful if the projection refuses
    to force a packet when one of these holds.
    """

    #: No frozen question family both covers this analytical job and permits
    #: the shape the run actually has.
    NO_REPRESENTABLE_QUESTION_FAMILY = "no_representable_question_family"
    #: The run never reached the deterministic plan executor, so there is no
    #: investigation result to project.
    NO_PLAN_GOVERNED_RESULT = "no_plan_governed_result"
    #: The run terminated before any subject was resolved and carried no
    #: clarification material either, so there is nothing to report.
    NO_SUBJECT_MATERIAL = "no_subject_material"
    #: The run has a cohort-bearing shape but observed no source class that
    #: measures a comparison dimension, so the cohort would support no
    #: comparison at all.
    NO_SUPPORTED_COMPARISON_DIMENSION = "no_supported_comparison_dimension"


@dataclass(frozen=True, slots=True)
class NativeProjectionGap:
    """One reason, and the detail a trial report needs to act on it."""

    reason: NativeProjectionGapReason
    detail: str


@dataclass(frozen=True, slots=True)
class NativeProjectionInput:
    """Everything the projection is allowed to see.

    Deliberately a closed record of one finished run rather than a set of
    service handles: a projection that cannot call a service cannot quietly
    enrich the baseline beyond what the run established.
    """

    org_id: str
    run_id: str
    produced_at: datetime
    interpretation: InterpretedQuestion
    #: ``None`` when preflight terminated before building one.
    ledger: DevResolutionLedger | None
    subject_set: DevSubjectSet | None
    committed_subject: DevEntityRefV2 | None
    investigation_result: DevInvestigationResult | None
    #: The run's own resolved evidence refs, as the frame carries them.
    evidence: tuple[DevEvidenceRefV2 | DevEvidenceRef, ...] = ()
    #: Mentions preflight could not resolve, by mention id.
    unresolved_mention_texts: Mapping[str, str] | None = None
    #: Candidates authorization removed before ranking, if the run counted
    #: them. Never inferred: an unknown count is zero here and the packet
    #: says so, rather than a guess that would read as a disclosure.
    authorization_filtered_count: int = 0
    #: The run's own bounded window. Required, and required to be a real
    #: interval: a projection that invented one would be inventing the
    #: packet's bounded time context, which every temporal claim rests on.
    window_start: datetime = _DEFAULT_WINDOW_START
    window_end: datetime = _DEFAULT_WINDOW_END
    timezone_name: str = "UTC"

    def __post_init__(self) -> None:
        if self.window_end <= self.window_start:
            raise ValueError(
                "the projected window must be a real interval; a degenerate one "
                "cannot carry a bounded time context"
            )


@dataclass(frozen=True, slots=True)
class NativeProjectionOutcome:
    """A validated packet, or the reasons there is none."""

    packet: AskDevInvestigationPacket | None
    gaps: tuple[NativeProjectionGap, ...]

    def __post_init__(self) -> None:
        if (self.packet is None) == (not self.gaps):
            raise ValueError(
                "a projection outcome carries exactly one of a packet or a "
                "non-empty gap list; anything else lets an unprojectable run "
                "read as an empty one"
            )


def _mint(*parts: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, ":".join(parts)))


def _subject_kind(entity_kind: EntityKind) -> InvestigationSubjectKind:
    return NATIVE_SUBJECT_KIND[entity_kind]


class _Limitations:
    """Accumulates packet limitations, one per kind.

    ``EvidenceCoverage.validate_index_is_coherent`` requires limitations to
    be unique by kind, and several disclosure rules independently demand the
    same kind. Collecting them here means a caller cannot forget one and
    cannot duplicate one.
    """

    def __init__(self) -> None:
        self._by_kind: dict[PacketLimitationKind, str] = {}

    def add(self, kind: PacketLimitationKind, detail: str) -> None:
        self._by_kind.setdefault(kind, detail[:240])

    def __contains__(self, kind: object) -> bool:
        return kind in self._by_kind

    def as_tuple(self) -> tuple[PacketLimitation, ...]:
        return tuple(
            PacketLimitation(kind=kind, detail=detail)
            for kind, detail in sorted(
                self._by_kind.items(), key=lambda item: item[0].value
            )
        )


def _analytical_job(
    payload: NativeProjectionInput,
    *,
    family: QuestionFamilyID,
    shape: ComparisonShape,
    limitations: _Limitations,
) -> AnalyticalJob:
    """The interpreter's own output, restated in contract terms.

    ``job_uncertainty`` is read off the interpreter's confidence and
    clarification flag rather than asserted: the native interpreter already
    distinguishes a deterministic recognizer hit from a low-confidence
    fallback, and flattening that to ``precise`` would hide the one thing
    the trial wants to know about ambiguous questions.
    """

    intent = payload.interpretation.intent
    if intent.requires_clarification:
        uncertainty = JobUncertainty.AMBIGUOUS
    elif intent.confidence < 0.9:
        uncertainty = JobUncertainty.BROAD_WITH_UNCERTAINTY
    else:
        uncertainty = JobUncertainty.PRECISE

    job_limitations: list[PacketLimitation] = []
    if uncertainty is not JobUncertainty.PRECISE:
        # An uncertain job MUST declare a limitation
        # (AnalyticalJob.validate_uncertainty_is_disclosed).
        job_limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.INTERPRETATION_UNCERTAINTY,
                detail=(
                    f"the deterministic interpreter reported confidence "
                    f"{intent.confidence:.2f} for intent {intent.intent_id.value}"
                )[:240],
            )
        )

    return AnalyticalJob(
        schema_version="ask_dev_analytical_job.v1",
        job_id=_mint("job", payload.run_id),
        question_family=family,
        job_uncertainty=uncertainty,
        job_statement=QUESTION_FAMILY_REGISTRY[family].analytical_job_statement[:240],
        comparison_shape=shape,
        time_context=BoundedTimeContext(
            start=payload.window_start,
            end=payload.window_end,
            timezone=payload.timezone_name,
            # The native arm has no as-of traversal (CHAOS-3569), so every
            # projected run is the current slice. A historical claim here
            # would be a claim the run cannot support.
            analytical_slice=AnalyticalSlice.CURRENT,
            as_of=None,
            historical_comparability=HistoricalComparability.NOT_APPLICABLE,
            edge_validity_basis="not_required",
        ),
        interpretation_limitations=tuple(job_limitations),
    )


def _subject_discovery(
    payload: NativeProjectionInput, *, limitations: _Limitations
) -> tuple[SubjectDiscovery, tuple[DevEntityRefV2, ...]]:
    """Candidates exactly as the resolution ledger recorded them.

    The ledger's ``DevResolutionCandidate`` carries an entity ref and a
    reason and nothing else — no score, no per-candidate signal. So the
    packet's ``rank`` is declaration order (the only ranking the native path
    has) and ``match_signals`` is a single honest signal, never a richer
    story than the resolver actually told.
    """

    candidates: list[SubjectCandidate] = []
    unresolved: list[UnresolvedMention] = []
    seen: dict[str, DevEntityRefV2] = {}
    committed_ids: list[str] = []

    committed_id = (
        payload.committed_subject.entity_id if payload.committed_subject else None
    )

    if payload.ledger is not None:
        for entry in payload.ledger.entries:
            entry_candidates = list(entry.candidates)
            if entry.committed_entity_ref is not None and not entry_candidates:
                # An exact match commits without ever listing alternatives;
                # the committed ref is the whole candidate set.
                ref = entry.committed_entity_ref
                entry_candidates = []
                seen.setdefault(ref.entity_id, ref)
                candidates.append(
                    _candidate(
                        ref=ref,
                        rank=len(candidates) + 1,
                        committed=ref.entity_id == committed_id,
                        signal=SubjectMatchSignal.EXACT_DISPLAY_NAME,
                        rationale=(
                            "resolver returned an exact authorized match for this "
                            "mention"
                        ),
                    )
                )
            for candidate in entry_candidates:
                ref = candidate.entity_ref
                if ref.entity_id in seen:
                    continue
                seen[ref.entity_id] = ref
                candidates.append(
                    _candidate(
                        ref=ref,
                        rank=len(candidates) + 1,
                        committed=ref.entity_id == committed_id,
                        # The catalog carries no explicit alias column; a
                        # non-exact hit is a label match and nothing
                        # stronger, so FUZZY_LABEL is the honest signal even
                        # though a richer one would score better.
                        signal=SubjectMatchSignal.FUZZY_LABEL,
                        rationale=candidate.reason,
                    )
                )
            if entry.outcome in {
                ResolutionOutcome.NO_AUTHORIZED_MATCH,
                ResolutionOutcome.AMBIGUOUS_CANDIDATES,
            }:
                unresolved.append(
                    UnresolvedMention(
                        mention_id=entry.mention_id,
                        mention_text=_mention_text(payload, entry.mention_id),
                        # MULTIPLE_CANDIDATES must name at least two
                        # (validate_unresolved_mentions_reference_declared_
                        # candidates). An ambiguous entry that listed one
                        # near-match means no candidate cleared the commit
                        # bar, which is NO_CANDIDATE -- not a weaker form of
                        # "several to choose from".
                        reason=(
                            UnresolvedMentionReason.MULTIPLE_CANDIDATES
                            if len(entry.candidates) >= 2
                            else UnresolvedMentionReason.NO_CANDIDATE
                        ),
                        candidate_ids=tuple(
                            item.entity_ref.entity_id for item in entry.candidates
                        ),
                    )
                )

    if committed_id is not None:
        committed_ids.append(committed_id)
        if committed_id not in seen and payload.committed_subject is not None:
            seen[committed_id] = payload.committed_subject
            candidates.insert(
                0,
                _candidate(
                    ref=payload.committed_subject,
                    rank=1,
                    committed=True,
                    signal=SubjectMatchSignal.EXACT_CANONICAL_ID,
                    rationale="committed by subject preflight",
                ),
            )
            candidates = [
                _renumber(candidate, rank=index + 1)
                for index, candidate in enumerate(candidates)
            ]

    # A committed candidate must rank first
    # (SubjectDiscovery.validate_commitment_is_evidenced).
    if (
        committed_id is not None
        and candidates
        and candidates[0].canonical_id != (committed_id)
    ):
        ordered = [item for item in candidates if item.canonical_id == committed_id]
        ordered += [item for item in candidates if item.canonical_id != committed_id]
        candidates = [
            _renumber(candidate, rank=index + 1)
            for index, candidate in enumerate(ordered)
        ]

    if payload.authorization_filtered_count:
        limitations.add(
            PacketLimitationKind.AUTHORIZATION_FILTERED,
            "candidates were removed by the caller's authorization scope",
        )

    discovery = SubjectDiscovery(
        schema_version="ask_dev_subject_discovery.v1",
        candidates=tuple(candidates[:25]),
        unresolved_mentions=tuple(unresolved[:10]),
        committed_subject_ids=tuple(committed_ids),
        authorization_filtered_count=payload.authorization_filtered_count,
        candidates_truncated=len(candidates) > 25,
        truncation_reason=(
            TruncationReason.NODE_BUDGET if len(candidates) > 25 else None
        ),
    )
    if discovery.candidates_truncated:
        limitations.add(
            PacketLimitationKind.TRUNCATED_TRAVERSAL,
            "the candidate list exceeded the contract's 25-candidate bound",
        )
    return discovery, tuple(seen.values())


def _candidate(
    *,
    ref: DevEntityRefV2,
    rank: int,
    committed: bool,
    signal: SubjectMatchSignal,
    rationale: str,
) -> SubjectCandidate:
    return SubjectCandidate(
        candidate_id=ref.entity_id,
        rank=rank,
        subject_kind=_subject_kind(ref.entity_kind),
        canonical_id=ref.entity_id,
        display_label=ref.display_label,
        commitment_state=(
            SubjectCommitmentState.COMMITTED
            if committed
            else SubjectCommitmentState.PROPOSED
        ),
        match_rationale=rationale[:240],
        match_signals=(
            SubjectMatchEvidence(
                signal=signal,
                matched_text=ref.display_label,
                source_class=SourceClass.WORK_GRAPH,
            ),
        ),
        match_confidence=1.0 if committed else 0.5,
        relevance=RelevanceState.CURRENT,
    )


def _renumber(candidate: SubjectCandidate, *, rank: int) -> SubjectCandidate:
    return candidate.model_copy(update={"rank": rank})


def _mention_text(payload: NativeProjectionInput, mention_id: str) -> str:
    texts = payload.unresolved_mention_texts or {}
    recorded = texts.get(mention_id)
    if recorded:
        return recorded[:240]
    for mention in payload.interpretation.mentions:
        if mention.mention_id == mention_id:
            return mention.original_text_span[:240]
    return "unrecorded mention"


def _supported_dimensions(
    payload: NativeProjectionInput,
) -> tuple[ComparisonDimension, ...]:
    """Dimensions the run's own observations genuinely support."""

    if payload.investigation_result is None:
        return ()
    found = {
        _DIMENSION_BY_SOURCE_CLASS[observation.source_class]
        for observation in payload.investigation_result.observations
        if observation.source_class in _DIMENSION_BY_SOURCE_CLASS
    }
    return tuple(sorted(found, key=lambda item: item.value))


def _comparison_cohort(
    payload: NativeProjectionInput,
    *,
    shape: ComparisonShape,
    dimensions: tuple[ComparisonDimension, ...],
    limitations: _Limitations,
) -> tuple[ComparisonCohort, tuple[DevEntityRefV2, ...]]:
    """The subjects the question named — never a constructed cohort.

    ``DevSubjectSet`` records committed mentions. It has no inclusion basis,
    no exclusion list and no notion of a peer. So every member here is
    ``explicitly_named`` with an ``explicitly_named_by_question``
    classification, and ``exclusions`` is empty. Presenting this as a
    discovered cohort would claim cohort construction the native path does
    not perform.
    """

    refs: tuple[DevEntityRefV2, ...] = ()
    completeness = CohortCompleteness.COMPLETE
    uncertainty: str | None = None

    if payload.subject_set is not None:
        refs = tuple(payload.subject_set.committed_entity_refs)
        if not payload.subject_set.cohort_complete:
            completeness = CohortCompleteness.BEST_EFFORT_UNCERTAIN
            uncertainty = (
                "the subject set reports incomplete cohort resolution: "
                f"{len(payload.subject_set.unresolved_mention_ids)} unresolved and "
                f"{len(payload.subject_set.ambiguous_mention_ids)} ambiguous mentions"
            )[:240]
    elif payload.committed_subject is not None:
        refs = (payload.committed_subject,)

    if shape is ComparisonShape.SINGULAR_SUBJECT:
        refs = refs[:1]

    members = tuple(
        CohortMember(
            subject_kind=_subject_kind(ref.entity_kind),
            canonical_id=ref.entity_id,
            display_label=ref.display_label,
            inclusion_basis=(CohortInclusionBasis.EXPLICITLY_NAMED,),
            inclusion_rationale=(
                "named by the question and committed by subject preflight"
            ),
            inclusion_evidence_classification=(
                CohortEvidenceClassification.EXPLICITLY_NAMED_BY_QUESTION
            ),
            relevance=RelevanceState.CURRENT,
        )
        for ref in refs[:50]
    )

    if completeness is CohortCompleteness.BEST_EFFORT_UNCERTAIN:
        limitations.add(
            PacketLimitationKind.TRUNCATED_TRAVERSAL,
            "cohort resolution was incomplete",
        )

    cohort = ComparisonCohort(
        schema_version="ask_dev_comparison_cohort.v1",
        cohort_id=_mint("cohort", payload.run_id),
        comparison_shape=shape,
        members=members,
        exclusions=(),
        supported_comparison_dimensions=(
            () if shape is ComparisonShape.SINGULAR_SUBJECT else dimensions
        ),
        completeness=completeness,
        cohort_uncertainty=uncertainty,
        authorization_filtered_count=0,
    )
    return cohort, refs


def _evidence_entries(
    payload: NativeProjectionInput,
    *,
    subject_ids: Sequence[str],
) -> tuple[tuple[InvestigationEvidenceEntry, ...], frozenset[str]]:
    """Every evidence ref the run resolved, attached to the subjects it backs.

    ``InvestigationEvidenceEntry.validate_supports_something`` rejects an
    entry that supports nothing, which is the contract's own guard against
    high-volume irrelevant evidence displacing lineage. The native path has
    no per-evidence relevance ranking, so an entry that cannot be attached
    to a declared subject is dropped rather than attached to everything.
    """

    entries: list[InvestigationEvidenceEntry] = []
    handles: set[str] = set()
    subjects = list(subject_ids)
    if not subjects:
        return (), frozenset()

    for ref in payload.evidence[:200]:
        wire = (
            ref
            if isinstance(ref, DevEvidenceRefV2)
            else DevEvidenceRefV2.model_validate(ref.model_dump())
        )
        if wire.evidence_ref_id in handles:
            continue
        source_class = _evidence_source_class(wire)
        if source_class is None:
            continue
        supported = [
            subject_id for subject_id in subjects if subject_id == wire.entity_id
        ] or subjects[:1]
        handles.add(wire.evidence_ref_id)
        entries.append(
            InvestigationEvidenceEntry(
                evidence=wire,
                source_class=source_class,
                supports_subject_ids=tuple(supported[:20]),
                relevance=RelevanceState.CURRENT,
            )
        )
    return tuple(entries), frozenset(handles)


_EVIDENCE_SOURCE_CLASS: Mapping[str, SourceClass] = {
    "work_items": SourceClass.WORK_ITEM,
    "work_units": SourceClass.WORK_ITEM,
    "work_graph": SourceClass.WORK_GRAPH,
    "pull_requests": SourceClass.PULL_REQUEST,
    "ci": SourceClass.CI_RUN,
    "ci_runs": SourceClass.CI_RUN,
    "deployments": SourceClass.DEPLOYMENT,
    "incidents": SourceClass.INCIDENT,
}


def _evidence_source_class(ref: DevEvidenceRefV2) -> SourceClass | None:
    """The trial source class an evidence ref belongs to, or ``None``.

    ``None`` means "this ref's source system has no allowlisted class", and
    the ref is dropped. Guessing a class would put a source on the packet
    that ``validate_sources_are_allowlisted`` should have rejected, which is
    exactly the smuggling the allowlist exists to stop.
    """

    return _EVIDENCE_SOURCE_CLASS.get(ref.source_system)


def _evidence_coverage(
    payload: NativeProjectionInput,
    *,
    family: QuestionFamilyID,
    entries: tuple[InvestigationEvidenceEntry, ...],
    authorization_filtered_count: int,
    limitations: _Limitations,
    clarifications: tuple[ClarificationNeed, ...],
) -> EvidenceCoverage:
    """Observed source health, and every required source that is absent.

    A family's required source classes must be *accounted for* — observed or
    declared missing (``validate_question_family_obligations``). Declaring
    them missing is the honest half of that, and it is what makes the
    native arm's six unobserved classes visible in the trial rather than
    reading as observed-empty.
    """

    observed: dict[SourceClass, SourceHealthObservation] = {}
    if payload.investigation_result is not None:
        for observation in payload.investigation_result.observations:
            if observation.source_class not in _TRIAL_ALLOWLIST:
                continue
            observed.setdefault(
                observation.source_class,
                SourceHealthObservation(
                    source_class=observation.source_class,
                    state=observation.observed_state,
                    observed_at=observation.watermark,
                    detail=(observation.limitation or None),
                ),
            )

    required = QUESTION_FAMILY_REGISTRY[family].required_source_classes
    missing: list[MissingSource] = []
    for source_class in required:
        if source_class in observed:
            continue
        missing.append(
            MissingSource(
                source_class=source_class,
                state="unavailable",
                impact=_missing_impact(source_class)[:240],
            )
        )

    if missing:
        limitations.add(
            PacketLimitationKind.MISSING_SOURCE,
            "sources this question family requires were not observed by any "
            "registered plan step",
        )

    return EvidenceCoverage(
        schema_version="ask_dev_evidence_coverage.v1",
        evidence_index=entries,
        source_health=tuple(
            observed[key] for key in sorted(observed, key=lambda item: item.value)
        ),
        missing_sources=tuple(missing),
        conflicts=(),
        limitations=limitations.as_tuple(),
        clarification_needs=clarifications,
        # Filtering is not truncation. Evidence dropped because its entity
        # is outside the authorized set is an authorization disclosure; only
        # a budget bite is a truncation, and conflating the two would tell a
        # reader the wrong cause.
        authorization_filtered_count=authorization_filtered_count,
        evidence_truncated=len(payload.evidence) > 200,
        truncation_reason=(
            TruncationReason.EVIDENCE_BUDGET if len(payload.evidence) > 200 else None
        ),
    )


def _missing_impact(source_class: SourceClass) -> str:
    if source_class in NATIVE_UNOBSERVED_SOURCE_CLASSES:
        if source_class in {
            SourceClass.COGNITIVE_LOAD,
            SourceClass.INVESTMENT_ALLOCATION,
        }:
            return (
                "the measurement exists in the team workload service but no plan "
                "step declares a source requirement under this class, so it is not "
                "observable as a source"
            )
        return "no registered plan step mints content under this source class"
    return "the run did not observe this source"


def _versions(payload: NativeProjectionInput) -> InvestigationVersions:
    contract_versions: list[SourceContractVersion] = []
    seen: set[SourceClass] = set()
    if payload.investigation_result is not None:
        for observation in payload.investigation_result.observations:
            if (
                observation.source_class in seen
                or observation.source_class not in _TRIAL_ALLOWLIST
            ):
                continue
            seen.add(observation.source_class)
            contract_versions.append(
                SourceContractVersion(
                    source_class=observation.source_class,
                    contract_version=_NATIVE_QUERY_VERSION,
                )
            )
    if not contract_versions:
        contract_versions.append(
            SourceContractVersion(
                source_class=SourceClass.WORK_GRAPH,
                contract_version=_NATIVE_QUERY_VERSION,
            )
        )

    return InvestigationVersions(
        schema_version="ask_dev_investigation_versions.v1",
        packet_schema_version="ask_dev_investigation_packet.v1",
        query_version=_NATIVE_QUERY_VERSION,
        ranking_version=_NATIVE_RANKING_VERSION,
        projection_version=NATIVE_PROJECTION_VERSION,
        source_contract_versions=tuple(contract_versions[:25]),
        trial=TrialMetadata(
            arm_id=NATIVE_ARM_ID,
            producer_id=NATIVE_PROJECTION_VERSION,
            run_id=payload.run_id,
        ),
    )


def _driver_analysis(
    payload: NativeProjectionInput,
    *,
    subject_ids: Sequence[str],
    evidence_handles: frozenset[str],
    may_assert: bool,
    limitations: _Limitations,
) -> DriverAnalysis:
    """Drivers only where a native finding genuinely supports one.

    ``DeficiencyFinding`` carries both ``relationship_paths`` and
    ``evidence_ref_ids``, so it can support a driver. ``HealthRuleFinding``
    carries neither — only ``evidence_source_classes``, which are classes
    rather than handles — so a health finding becomes a ``candidate_only``
    observation and never a contributing or principal driver. That is the
    single most consequential honest limit of this arm, and it is enforced
    here rather than noted in a comment.
    """

    if payload.investigation_result is None or not subject_ids:
        return DriverAnalysis(
            schema_version="ask_dev_driver_analysis.v1",
            candidates=(),
            principal_driver_ids=(),
            candidates_truncated=False,
        )

    candidates: list[DriverCandidate] = []
    affected = tuple(subject_ids[:50])

    for observation in payload.investigation_result.observations:
        content = observation.content
        if content is None:
            continue
        for finding in content.deficiency_findings:
            supporting = tuple(
                handle
                for handle in finding.evidence_ref_ids
                if handle in evidence_handles
            )[:25]
            if not supporting:
                # Without an indexed handle the packet's own evidence-closure
                # check would reject the citation, and a driver asserted on
                # an unindexed handle is an unsupported attribution.
                continue
            candidates.append(
                DriverCandidate(
                    driver_id=_mint("driver", payload.run_id, finding.finding_id),
                    category=DriverCategory.OPERATIONAL_PRESSURE,
                    summary=finding.blast_radius[:240],
                    affected_subject_ids=affected,
                    role=DriverRole.DRIVER,
                    # Contributing at best, never principal: principal
                    # additionally requires a supporting relationship path,
                    # and a native deficiency finding's own paths do not
                    # survive into a contract LineagePath (see
                    # capabilities). And where the question family requires
                    # a related-context section this arm cannot fill, even
                    # contributing is out of reach -- a packet cannot assert
                    # a judgment on a question it could not fully
                    # investigate (validate_supported_outcome_asserts_a_
                    # judgment holds the other end of that rule).
                    standing=(
                        DriverStanding.CONTRIBUTING_DRIVER
                        if may_assert
                        else DriverStanding.CANDIDATE_ONLY
                    ),
                    assertion_basis=AssertionBasis.MEASURED,
                    confidence_qualifier=ConfidenceQualifier.QUALIFIED,
                    supporting_path_ids=(),
                    supporting_evidence_ids=supporting,
                    relevance=RelevanceState.CURRENT,
                )
            )
        for health in content.health_findings:
            candidates.append(
                DriverCandidate(
                    driver_id=_mint("health", payload.run_id, health.finding_id),
                    category=DriverCategory.DELIVERY_PRESSURE,
                    summary=(
                        f"{health.dimension.value} rule {health.rule_id} reported "
                        f"{health.state.value}"
                    )[:240],
                    affected_subject_ids=affected,
                    role=DriverRole.SYMPTOM,
                    standing=DriverStanding.CANDIDATE_ONLY,
                    assertion_basis=AssertionBasis.SOURCE_ASSERTED,
                    confidence_qualifier=ConfidenceQualifier.UNCERTAIN,
                    supporting_path_ids=(),
                    supporting_evidence_ids=(),
                    relevance=RelevanceState.CURRENT,
                )
            )

    truncated = len(candidates) > 50
    kept = candidates[:50]
    if truncated:
        limitations.add(
            PacketLimitationKind.TRUNCATED_TRAVERSAL,
            "the driver candidate list exceeded the contract's 50-candidate bound",
        )
    return DriverAnalysis(
        schema_version="ask_dev_driver_analysis.v1",
        candidates=tuple(kept),
        principal_driver_ids=(),
        candidates_truncated=truncated,
        truncation_reason=TruncationReason.NODE_BUDGET if truncated else None,
    )


def _staffing_qualification(family: QuestionFamilyID) -> StaffingQualification | None:
    """An absent denominator, stated, for the families that need one.

    The addendum is explicit that a missing denominator lowers confidence
    rather than refusing the question, and the contract enforces the other
    half: a capacity claim with no qualification is an unsupported claim.
    """

    policy = QUESTION_FAMILY_REGISTRY[family].staffing_denominator_policy
    if policy.value == "not_applicable":
        return None
    return StaffingQualification(
        denominator_state=StaffingDenominatorState.DENOMINATOR_ABSENT,
        qualification_note=(
            "no planned allocation or headcount denominator is observable: no plan "
            "step mints investment_allocation or cognitive_load content"
        ),
    )


def project_native_investigation(
    payload: NativeProjectionInput,
) -> NativeProjectionOutcome:
    """Project one finished native run, or report why it cannot be projected."""

    shape = comparison_shape_for(
        cardinality=payload.interpretation.intent.cardinality,
        has_unresolved_mentions=_has_unresolved(payload),
    )
    family = classify_question_family(
        intent_id=payload.interpretation.intent.intent_id, shape=shape
    )
    if family is None:
        return NativeProjectionOutcome(
            packet=None,
            gaps=(
                NativeProjectionGap(
                    reason=(NativeProjectionGapReason.NO_REPRESENTABLE_QUESTION_FAMILY),
                    detail=(
                        f"intent {payload.interpretation.intent.intent_id.value} with "
                        f"shape {shape.value} matches no frozen question family that "
                        "also permits that shape"
                    ),
                ),
            ),
        )

    limitations = _Limitations()
    job = _analytical_job(payload, family=family, shape=shape, limitations=limitations)
    discovery, candidate_refs = _subject_discovery(payload, limitations=limitations)
    dimensions = _supported_dimensions(payload)
    if shape is not ComparisonShape.SINGULAR_SUBJECT and not dimensions:
        return NativeProjectionOutcome(
            packet=None,
            gaps=(
                NativeProjectionGap(
                    reason=(
                        NativeProjectionGapReason.NO_SUPPORTED_COMPARISON_DIMENSION
                    ),
                    detail=(
                        f"a {shape.value} cohort needs at least one comparison "
                        "dimension, and no observed source class measures one"
                    ),
                ),
            ),
        )
    cohort, cohort_refs = _comparison_cohort(
        payload, shape=shape, dimensions=dimensions, limitations=limitations
    )

    if not discovery.candidates and not discovery.unresolved_mentions:
        return NativeProjectionOutcome(
            packet=None,
            gaps=(
                NativeProjectionGap(
                    reason=NativeProjectionGapReason.NO_SUBJECT_MATERIAL,
                    detail=(
                        "the run recorded neither a resolution candidate nor an "
                        "unresolved mention, so there is nothing to report"
                    ),
                ),
            ),
        )

    authorized = _authorized_entity_ids(candidate_refs, cohort_refs)
    subject_ids = [item.canonical_id for item in discovery.candidates]

    entries, handles = _evidence_entries(payload, subject_ids=subject_ids)
    required_sections = QUESTION_FAMILY_REGISTRY[family].required_packet_sections
    may_assert = PacketSection.RELATED_CONTEXT not in required_sections
    drivers = _driver_analysis(
        payload,
        subject_ids=subject_ids,
        evidence_handles=handles,
        may_assert=may_assert,
        limitations=limitations,
    )
    drivers = _qualify_staffing(drivers, family=family, limitations=limitations)

    entries, filtered_evidence = _restrict_evidence_to_authorized(
        entries, authorized=authorized
    )
    if filtered_evidence:
        limitations.add(
            PacketLimitationKind.AUTHORIZATION_FILTERED,
            "evidence naming entities outside the authorized set was dropped",
        )
    clarifications = _clarifications(payload, discovery=discovery, shape=shape)
    outcome = _outcome(
        family=family,
        drivers=drivers,
        entries=entries,
        clarifications=clarifications,
        # The native arm never populates related context: work-graph edges
        # lose both endpoint kinds before they reach the investigation
        # result, so no LineageHop can be built. Passed explicitly rather
        # than read off the empty section, so wiring lineage later is a
        # visible edit here rather than a silent change of outcome.
        has_related_context=False,
        limitations=limitations,
    )
    if outcome is InvestigationOutcome.UNSUPPORTED and not limitations.as_tuple():
        limitations.add(
            PacketLimitationKind.MISSING_SOURCE,
            "the run established no driver this contract can carry",
        )

    coverage = _evidence_coverage(
        payload,
        family=family,
        entries=entries,
        authorization_filtered_count=filtered_evidence,
        limitations=limitations,
        clarifications=clarifications,
    )

    related = RelatedContext(
        schema_version="ask_dev_related_context.v1",
        entities=(),
        paths=(),
        authorized_entity_ids=tuple(sorted(authorized)),
        authorization_filtered_count=0,
        entities_truncated=False,
        paths_truncated=False,
    )

    packet = AskDevInvestigationPacket(
        schema_version="ask_dev_investigation_packet.v1",
        packet_id=_mint("packet", payload.run_id),
        organization_id=payload.org_id,
        produced_at=payload.produced_at,
        outcome=outcome,
        analytical_job=job,
        subject_discovery=discovery,
        comparison_cohort=cohort,
        related_context=related,
        driver_analysis=drivers,
        evidence_coverage=coverage,
        versions=_versions(payload),
    )
    return NativeProjectionOutcome(packet=packet, gaps=())


def _has_unresolved(payload: NativeProjectionInput) -> bool:
    if payload.ledger is None:
        return False
    return any(
        entry.outcome
        in {
            ResolutionOutcome.NO_AUTHORIZED_MATCH,
            ResolutionOutcome.AMBIGUOUS_CANDIDATES,
        }
        for entry in payload.ledger.entries
    )


def _authorized_entity_ids(*groups: Iterable[DevEntityRefV2]) -> frozenset[str]:
    return frozenset(ref.entity_id for group in groups for ref in group)


def _restrict_evidence_to_authorized(
    entries: tuple[InvestigationEvidenceEntry, ...], *, authorized: frozenset[str]
) -> tuple[tuple[InvestigationEvidenceEntry, ...], int]:
    """Drop evidence whose entity is outside the authorized set.

    ``validate_every_entity_is_authorized`` reads every entry's
    ``evidence.entity_id``. Widening the authorized set to admit the
    evidence would invert the check into a rubber stamp, so the evidence is
    dropped instead.
    """

    kept = tuple(entry for entry in entries if entry.evidence.entity_id in authorized)
    return kept, len(entries) - len(kept)


def _clarifications(
    payload: NativeProjectionInput,
    *,
    discovery: SubjectDiscovery,
    shape: ComparisonShape,
) -> tuple[ClarificationNeed, ...]:
    if not discovery.unresolved_mentions:
        return ()
    return (
        ClarificationNeed(
            kind=ClarificationNeedKind.AMBIGUOUS_SUBJECT,
            prompt=(
                "the question named a subject that did not resolve to a single "
                "authorized entity"
            ),
            candidate_ids=tuple(
                candidate_id
                for mention in discovery.unresolved_mentions
                for candidate_id in mention.candidate_ids
            )[:10],
        ),
    )


def _qualify_staffing(
    drivers: DriverAnalysis,
    *,
    family: QuestionFamilyID,
    limitations: _Limitations,
) -> DriverAnalysis:
    qualification = _staffing_qualification(family)
    if qualification is None:
        return drivers
    updated = tuple(
        candidate.model_copy(update={"staffing_qualification": qualification})
        if candidate.category is DriverCategory.CAPACITY_OR_STAFFING
        else candidate
        for candidate in drivers.candidates
    )
    if any(
        candidate.category is DriverCategory.CAPACITY_OR_STAFFING
        for candidate in updated
    ):
        limitations.add(
            PacketLimitationKind.ABSENT_STAFFING_DENOMINATOR,
            "no allocation or headcount denominator is observable",
        )
    return drivers.model_copy(update={"candidates": updated})


def _outcome(
    *,
    family: QuestionFamilyID,
    drivers: DriverAnalysis,
    entries: tuple[InvestigationEvidenceEntry, ...],
    clarifications: tuple[ClarificationNeed, ...],
    has_related_context: bool,
    limitations: _Limitations,
) -> InvestigationOutcome:
    """The honest outcome, derived from what the packet actually carries.

    Never optimistic, and three separate floors have to clear.

    ``SUPPORTED_WITH_GAPS`` needs an asserted driver *and* a non-empty
    evidence index — the contract's structural form of "this is not a
    dashboard redirect". It ALSO needs every packet section the question
    family requires to be populated, and that is where the native arm
    stops: five of the ten frozen families require ``related_context``,
    and this arm has no projectable lineage to put there. Those families
    therefore report ``UNSUPPORTED`` with the reason stated, rather than a
    supported outcome the contract would reject anyway. Discovering that at
    validation time and quietly emitting lineage to satisfy it is precisely
    the failure this projection exists not to commit.
    """

    asserted = [
        candidate
        for candidate in drivers.candidates
        if candidate.standing
        in {DriverStanding.PRINCIPAL_DRIVER, DriverStanding.CONTRIBUTING_DRIVER}
    ]
    required_sections = QUESTION_FAMILY_REGISTRY[family].required_packet_sections
    if PacketSection.RELATED_CONTEXT in required_sections and not has_related_context:
        limitations.add(
            PacketLimitationKind.MISSING_SOURCE,
            f"family {family.value} requires related context, and no native "
            "relationship survives into a contract lineage path",
        )
        if clarifications:
            return InvestigationOutcome.NEEDS_CLARIFICATION
        return InvestigationOutcome.UNSUPPORTED
    if clarifications and not asserted:
        return InvestigationOutcome.NEEDS_CLARIFICATION
    if asserted and entries:
        return InvestigationOutcome.SUPPORTED_WITH_GAPS
    return InvestigationOutcome.UNSUPPORTED
