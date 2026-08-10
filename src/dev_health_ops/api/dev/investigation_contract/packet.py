"""``ask_dev_investigation_packet.v1`` — the frozen investigation contract.

CHAOS-3615 deliverables 1 and 12.

Every model here is a ``ContractModelV2``: ``extra="forbid"`` and
``frozen=True``, exactly like the Ask Dev v2 wire contracts, so an arm
cannot smuggle a backend-specific field through as an extra key and cannot
mutate a packet after it has been produced.

Two conventions run through the whole module and are worth stating once.

**No reassuring default.** Every disclosure field —
``authorization_filtered_count``, every ``*_truncated`` flag,
``completeness`` — is required with no default. The tempting defaults are
all the flattering ones (nothing was filtered, nothing was truncated), and a
producer that forgets the field would then look exactly like one that had
nothing to disclose. Omission is a validation error instead. Pinned by
``test_chaos_3615_fault_modes.py::test_disclosure_fields_have_no_reassuring_default``.

**Truncation and filtering are always paired with a reason.** A ``True``
truncation flag without a ``TruncationReason``, or a non-zero
authorization-filtered count without a matching limitation, is rejected —
partial results that do not say why they are partial are indistinguishable
from complete ones.

The evidence vocabulary is deliberately *not* new: handles are
``EvidenceHandle`` (``ev1_`` + 40 hex, minted and verified by
``evidence_service.EvidenceHandleService``) and indexed items embed a real
``DevEvidenceRefV2``. A parallel evidence-identity scheme would have made
packet evidence unverifiable against the service that issues it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from dev_health_ops.api.dev.contracts import (
    Label,
    OpaqueID,
    ShortText,
    TimezoneName,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    ContractModelV2,
    EvidenceHandle,
    PlatformVersionToken,
    ServerHandle,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2

from .allowlists import SLICE_BOUNDARIES, TRIAL_SOURCE_ALLOWLIST
from .question_families import QUESTION_FAMILY_REGISTRY, QuestionFamilyID
from .relationships import RELATIONSHIP_ALLOWLIST, RelationshipType
from .vocabulary import (
    ASSERTED_DRIVER_STANDINGS,
    COHORT_BEARING_SHAPES,
    CURRENTLY_RELEVANT_STATES,
    NOT_COMPARABLE_STATES,
    SUBJECT_CLARIFICATION_KINDS,
    SUPPORTED_OUTCOMES,
    UNCERTAIN_JOB_STATES,
    UNQUALIFIED_DENOMINATOR_STATES,
    WEAK_SUBJECT_MATCH_SIGNALS,
    AnalyticalSlice,
    AssertionBasis,
    ClarificationNeedKind,
    CohortCompleteness,
    CohortEvidenceClassification,
    CohortExclusionReason,
    CohortInclusionBasis,
    ComparisonDimension,
    ComparisonShape,
    ConfidenceQualifier,
    ConflictResolution,
    DriverCategory,
    DriverExclusionReason,
    DriverRole,
    DriverStanding,
    EdgeValidityBasis,
    HistoricalComparability,
    InvestigationOutcome,
    InvestigationSubjectKind,
    JobUncertainty,
    PacketLimitationKind,
    PacketSection,
    RelationshipDirection,
    RelevanceState,
    StaffingDenominatorState,
    SubjectCommitmentState,
    SubjectMatchSignal,
    SurfaceKind,
    TruncationReason,
    UnresolvedMentionReason,
)

__all__ = [
    "CONTRACT_CHANGELOG",
    "INVESTIGATION_CONTRACT_MODELS",
    "AnalyticalJob",
    "AskDevInvestigationPacket",
    "BoundedTimeContext",
    "ClarificationNeed",
    "CohortExclusion",
    "CohortMember",
    "ComparisonCohort",
    "ContractChangelogEntry",
    "DriverAnalysis",
    "DriverCandidate",
    "EvidenceCoverage",
    "InvestigationEvidenceEntry",
    "InvestigationVersions",
    "LineageHop",
    "LineagePath",
    "MissingSource",
    "PacketLimitation",
    "ProductionJobProvenance",
    "RelatedContext",
    "RelatedEntity",
    "SourceConflict",
    "SourceContractVersion",
    "SourceHealthObservation",
    "StaffingQualification",
    "SubjectCandidate",
    "SubjectDiscovery",
    "SubjectMatchEvidence",
    "SurfaceContextRef",
    "TrialMetadata",
    "UnresolvedMention",
]


def _path_entity_ids(path: LineagePath) -> set[str]:
    """Every entity a path touches, endpoints and intermediates alike."""

    touched: set[str] = {path.origin_entity_id, path.terminal_entity_id}
    for hop in path.hops:
        touched.add(hop.source_entity_id)
        touched.add(hop.target_entity_id)
    return touched


def _require_truncation_reason(
    truncated: bool, reason: TruncationReason | None, field: str
) -> None:
    """Raise unless a truncation flag and its reason agree.

    Shared by every section that can truncate. Both directions are errors: a
    flag without a reason hides *why* the result is partial; a reason
    without a flag means a consumer counting truncations would miss it.
    """

    if truncated and reason is None:
        raise ValueError(
            f"{field} is truncated but declares no truncation_reason; a "
            "partial result that does not say why is indistinguishable from "
            "a complete one"
        )
    if not truncated and reason is not None:
        raise ValueError(
            f"{field} declares a truncation_reason but is not marked truncated"
        )


# --------------------------------------------------------------------------
# Analytical job
# --------------------------------------------------------------------------


class PacketLimitation(ContractModelV2):
    """One safe, closed-vocabulary limitation the packet is disclosing."""

    kind: PacketLimitationKind
    detail: ShortText


class ClarificationNeed(ContractModelV2):
    """A question the investigation needs answered before it can proceed."""

    kind: ClarificationNeedKind
    prompt: ShortText
    candidate_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=10)


class SurfaceContextRef(ContractModelV2):
    """A safe reference to the surface or conversation the question came from.

    Deliberately three closed/opaque fields and no URL: see
    ``vocabulary.SurfaceKind``.
    """

    surface_kind: SurfaceKind
    surface_id: OpaqueID
    entity_kind: InvestigationSubjectKind | None = None
    entity_id: OpaqueID | None = None

    @model_validator(mode="after")
    def validate_entity_reference_is_complete(self) -> Self:
        if (self.entity_kind is None) != (self.entity_id is None):
            raise ValueError(
                "a surface entity reference needs both entity_kind and "
                "entity_id, or neither"
            )
        return self


class BoundedTimeContext(ContractModelV2):
    """The bounded time window, and which slice of history it addresses."""

    start: AwareDatetime
    end: AwareDatetime
    timezone: TimezoneName
    analytical_slice: AnalyticalSlice
    as_of: AwareDatetime | None = None
    historical_comparability: HistoricalComparability
    edge_validity_basis: EdgeValidityBasis

    @model_validator(mode="after")
    def validate_window_and_slice(self) -> Self:
        if self.end <= self.start:
            raise ValueError("time context end must be strictly after start")
        current = self.analytical_slice is AnalyticalSlice.CURRENT
        if current:
            if self.as_of is not None:
                raise ValueError("the current slice must not carry an as_of instant")
            if (
                self.historical_comparability
                is not HistoricalComparability.NOT_APPLICABLE
            ):
                raise ValueError(
                    "the current slice's only legal comparability state is "
                    "'not_applicable'"
                )
            if self.edge_validity_basis is not EdgeValidityBasis.NOT_REQUIRED:
                raise ValueError(
                    "the current slice reads the live projection, so its only "
                    "legal edge_validity_basis is 'not_required'"
                )
            return self
        if self.as_of is None:
            raise ValueError(
                f"the {self.analytical_slice} slice requires an as_of instant; "
                "without one an as-of traversal cannot be reconstructed"
            )
        if self.historical_comparability is HistoricalComparability.NOT_APPLICABLE:
            raise ValueError(
                f"the {self.analytical_slice} slice must state whether it is "
                "comparable; 'not_applicable' is reserved for the current slice"
            )
        return self

    @model_validator(mode="after")
    def validate_comparability_is_backed_by_edge_validity(self) -> Self:
        """A historical comparison must have actually had edge validity.

        Adversarial review round 1, finding M7. ``SLICE_BOUNDARIES`` declared
        that historical slices require edge validity, but nothing on the wire
        recorded whether the arm had it — so a packet could read the *live*
        projection, label the slice ``COMPARABLE``, and emit a confident and
        entirely false delta. The declaration was a comment; this makes it a
        constraint.

        The three rules are the contrapositives of each other: a slice that
        needs edge validity may not claim it was ``NOT_REQUIRED``; only
        ``OBSERVED_INTERVALS`` may back ``COMPARABLE``; and ``UNAVAILABLE`` —
        the CHAOS-3569 state — forces the matching not-comparable reason
        rather than a vaguer one.
        """

        boundary = SLICE_BOUNDARIES[self.analytical_slice]
        if not boundary.requires_edge_validity:
            return self
        if self.edge_validity_basis is EdgeValidityBasis.NOT_REQUIRED:
            raise ValueError(
                f"the {self.analytical_slice} slice requires edge validity, so "
                "'not_required' is not a legal edge_validity_basis for it"
            )
        comparable = self.historical_comparability is HistoricalComparability.COMPARABLE
        observed = self.edge_validity_basis is EdgeValidityBasis.OBSERVED_INTERVALS
        if comparable and not observed:
            raise ValueError(
                f"the {self.analytical_slice} slice claims COMPARABLE on an "
                f"edge_validity_basis of {self.edge_validity_basis}; without "
                "observed validity intervals the as-of state was not "
                "reconstructed and any delta is fabricated"
            )
        if self.edge_validity_basis is EdgeValidityBasis.UNAVAILABLE and (
            self.historical_comparability
            is not HistoricalComparability.NOT_COMPARABLE_MISSING_EDGE_VALIDITY
        ):
            raise ValueError(
                "edge validity is unavailable, so the comparability state must "
                "be 'not_comparable_missing_edge_validity' (the CHAOS-3569 "
                f"state), not {self.historical_comparability}"
            )
        return self


@dataclass(frozen=True, slots=True)
class ContractChangelogEntry:
    """One dated, cited reason this frozen contract's shape widened.

    Mirrors ``trials/chaos_3619/refusal_causes.py``'s ``LedgeredDivergence``
    discipline: a frozen contract does not get to simply change shape, it
    gets to widen with a citation. This is a different ledger for a
    different question -- that one explains why measured *scores* diverge
    from a pinned run; this one explains why the *schema itself* is not the
    schema it was on a given date -- but the discipline is the same: never
    edited or deleted once landed, always dated, always cited to the
    decision that authorized it.
    """

    ticket: str
    pull_request: str
    #: ISO date the change landed. Required for the same reason
    #: ``LedgeredDivergence.landed_on`` is: a changelog without dates cannot
    #: be read as a history, only as a list of excuses.
    landed_on: str
    schema: str
    from_version: str
    to_version: str
    rationale: str


#: APPEND ONLY. Never edit or delete a landed entry.
CONTRACT_CHANGELOG: tuple[ContractChangelogEntry, ...] = (
    ContractChangelogEntry(
        ticket="CHAOS-3660",
        pull_request="TBD",
        landed_on="2026-08-10",
        schema="ask_dev_analytical_job",
        from_version="v1",
        to_version="v1+v2 (both accepted)",
        rationale=(
            "CHAOS-3678 (the production bounded graph query service) needs "
            "AnalyticalJob to carry a production-shaped job identity with "
            "NO QuestionFamilyID and NO TrialContext -- production's "
            "QuestionIntentID (13 members) does not name-map onto the "
            "trial's QuestionFamilyID (10 members; several, like "
            "AMBIGUOUS_IDENTITY and CLARIFICATION_AND_NO_MATCH, have no "
            "production counterpart at all), so minting a QuestionFamilyID "
            "from a production question would be inventing trial vocabulary "
            "that was never earned -- exactly what the corrective plan's "
            "'trial construction is not production construction' rule "
            "forbids. Ruled on CHAOS-3660 (orchestrator, packet-constructor "
            "decision): widen AnalyticalJob with an optional "
            "question_family (unchanged trial shape, schema_version stays "
            "'.v1', byte-stable -- every packet build_packet has ever "
            "emitted keeps this exact shape) and a new optional "
            "production_job: ProductionJobProvenance (schema_version "
            "'.v2', intent_id as a validated string per the ruling's "
            "typing decision, no QuestionFamilyID import into this frozen "
            "contract). validate_provenance_matches_schema_version enforces "
            "exactly one shape per packet, tied to which schema_version "
            "literal it declares. test_chaos_3678_real_artifacts_still_"
            "validate.py proves every existing contracts/ask-dev-"
            "investigation/v1/examples/ fixture still validates unchanged "
            "under this widened schema."
        ),
    ),
)


class ProductionJobProvenance(ContractModelV2):
    """Production's own job identity: no trial family, no fixture concept.

    CHAOS-3660/CHAOS-3678. ``intent_id`` is a **validated string**, not the
    production ``QuestionIntentID`` enum imported directly — this frozen
    contract must never import production's vocabulary (see
    ``investigation_contract/__init__.py``'s own "deliberately its own
    tree" boundary, which exists precisely so this contract does not depend
    on anything outside the trial). Coupling the frozen contract to a
    vocabulary that changes on production's own schedule would let a
    production enum edit silently rewrite what a *trial* schema_version
    means — a frozen contract has no way to notice that happened. The
    string is validated against the real ``QuestionIntentID`` member set by
    the producer (``packet_builder.build_production_packet``), one layer up
    from here, where the import is legitimate because that module is
    production code, not the frozen contract itself.

    ``run_id`` is the identifier the calling ``GraphInvestigationRequest``
    already carried — production's analog of what ``TrialContext.run_id``
    is for a trial run, but this model carries no corpus/fixture concept at
    all: production has neither.
    """

    schema_version: Literal["ask_dev_production_job_provenance.v1"]
    intent_id: ShortText
    run_id: ServerHandle


class AnalyticalJob(ContractModelV2):
    """The normalized analytical job, with its uncertainty stated.

    ``job_uncertainty`` is what keeps this contract from requiring a
    pre-enumerated intent: an arm may declare a broad job it is uncertain
    about and still investigate, provided it says so. The
    ``question_family`` is a *family*, not an enumerated intent — ten of
    them cover the whole trial, and one of them is "clarification and safe
    no-match".

    **Provenance is exactly one of two shapes (CHAOS-3660/CHAOS-3678).**
    ``question_family`` (with ``schema_version`` ending ``.v1``) is the
    original, byte-stable trial shape — every packet ``build_packet`` has
    ever emitted keeps this exact shape, unchanged. ``production_job``
    (with ``schema_version`` ending ``.v2``) is production's shape, with no
    trial family and no fixture/corpus concept. See
    ``CONTRACT_CHANGELOG`` for the dated, cited record of why this widened
    rather than staying single-shape, and
    ``validate_provenance_matches_schema_version`` for the enforcement.
    """

    schema_version: Literal["ask_dev_analytical_job.v1", "ask_dev_analytical_job.v2"]
    job_id: OpaqueID
    question_family: QuestionFamilyID | None = None
    production_job: ProductionJobProvenance | None = None
    job_uncertainty: JobUncertainty
    job_statement: ShortText
    comparison_shape: ComparisonShape
    time_context: BoundedTimeContext
    surface_context_refs: tuple[SurfaceContextRef, ...] = Field(
        default_factory=tuple, max_length=10
    )
    conversation_reference_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=10
    )
    interpretation_limitations: tuple[PacketLimitation, ...] = Field(
        default_factory=tuple, max_length=25
    )

    @model_validator(mode="after")
    def validate_provenance_matches_schema_version(self) -> Self:
        """Exactly one provenance shape, tied to the schema version literal.

        Not just "exactly one of the two fields is set" — that alone would
        let a caller declare ``schema_version=".v1"`` while supplying
        ``production_job``, which is a packet claiming to be the byte-stable
        trial shape while actually carrying production's. Tying the check to
        the literal is what makes "trial fields never silently defaulted on
        production packets; production fields never on trial packets" (the
        CHAOS-3660 ruling's own words) an enforced invariant rather than a
        convention two constructors happen to follow today.
        """

        is_trial_schema = self.schema_version == "ask_dev_analytical_job.v1"
        has_family = self.question_family is not None
        has_production = self.production_job is not None
        if has_family == has_production:
            raise ValueError(
                "AnalyticalJob must declare exactly one of question_family "
                "(trial) or production_job (production) -- never both, "
                "never neither"
            )
        if is_trial_schema != has_family:
            raise ValueError(
                f"schema_version {self.schema_version!r} does not match its "
                "provenance: a '.v1' job must carry question_family and no "
                "production_job; a '.v2' job must carry production_job and "
                "no question_family"
            )
        return self

    @model_validator(mode="after")
    def validate_uncertainty_is_disclosed(self) -> Self:
        """An uncertain job must say what it is uncertain about.

        Without this, ``BROAD_WITH_UNCERTAINTY`` would be a free pass: an
        arm could label every job broad, investigate whatever it liked, and
        never be accountable for the interpretation it actually chose.
        """

        if (
            self.job_uncertainty in UNCERTAIN_JOB_STATES
            and not self.interpretation_limitations
        ):
            raise ValueError(
                f"a {self.job_uncertainty} analytical job must declare at "
                "least one interpretation limitation"
            )
        return self


# --------------------------------------------------------------------------
# Subject discovery
# --------------------------------------------------------------------------


class SubjectMatchEvidence(ContractModelV2):
    """What matched, and what backs the claim that it matched."""

    signal: SubjectMatchSignal
    matched_text: ShortText
    source_class: SourceClass
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=10
    )


class SubjectCandidate(ContractModelV2):
    """One candidate canonical subject and why it is a candidate.

    ``match_signals`` is ``min_length=1``: a ranked candidate with no stated
    signal is the shape in which a wrong-but-similarly-named project reaches
    rank 1 unexaminably.
    """

    candidate_id: OpaqueID
    rank: int = Field(ge=1, le=25)
    subject_kind: InvestigationSubjectKind
    canonical_id: OpaqueID
    display_label: Label
    commitment_state: SubjectCommitmentState
    match_rationale: ShortText
    match_signals: tuple[SubjectMatchEvidence, ...] = Field(min_length=1, max_length=10)
    match_confidence: float = Field(ge=0, le=1)
    relevance: RelevanceState


class UnresolvedMention(ContractModelV2):
    """A reference in the question that discovery could not resolve."""

    mention_id: OpaqueID
    mention_text: ShortText
    reason: UnresolvedMentionReason
    candidate_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=25)


class SubjectDiscovery(ContractModelV2):
    """Candidate subjects, what was committed, and what stayed unresolved.

    Commitment is *not* a prerequisite for the rest of the packet: a
    discovery section with zero committed subjects is legal and is exercised
    by a positive fixture, because the correction addendum forbids requiring
    exact subject commitment before authorized context discovery.
    """

    schema_version: Literal["ask_dev_subject_discovery.v1"]
    candidates: tuple[SubjectCandidate, ...] = Field(
        default_factory=tuple, max_length=25
    )
    unresolved_mentions: tuple[UnresolvedMention, ...] = Field(
        default_factory=tuple, max_length=10
    )
    committed_subject_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=25
    )
    authorization_filtered_count: int = Field(ge=0)
    candidates_truncated: bool
    truncation_reason: TruncationReason | None = None

    @model_validator(mode="after")
    def validate_candidate_ranking(self) -> Self:
        """Ranks are 1..n, unique, and in declaration order.

        Declaration order matters because top-1 and top-3 are scored
        dimensions: a producer that emitted ranks out of order would make
        "the rank-1 candidate" ambiguous between position and label.
        """

        expected = tuple(range(1, len(self.candidates) + 1))
        actual = tuple(candidate.rank for candidate in self.candidates)
        if actual != expected:
            raise ValueError(
                f"candidate ranks must be 1..n in declaration order; got {actual}"
            )
        candidate_ids = [candidate.candidate_id for candidate in self.candidates]
        if len(set(candidate_ids)) != len(candidate_ids):
            raise ValueError("candidate_id values must be unique")
        return self

    @model_validator(mode="after")
    def validate_commitment_is_evidenced(self) -> Self:
        """Committed subjects agree with their candidates, and are earned.

        Three rules, all aimed at the same fault shape — a wrong but
        similarly named subject silently becoming *the* subject:

        1. ``committed_subject_ids`` is exactly the set of candidates in the
           ``COMMITTED`` state, in both directions. A commitment recorded in
           one place and not the other is a producer bug that would show up
           downstream as a subject nobody can trace.
        2. A committed candidate's signals may not be *only* weak ones.
           Fuzzy label matching is exactly what returns "Nightfall" for
           "Nightfall Migration"; committing on it alone is the fault.
        3. A committed candidate must be ranked first. Committing to
           something the arm itself ranked below another candidate, with no
           clarification, is the same fault wearing a different hat.
        """

        committed = {
            candidate.canonical_id
            for candidate in self.candidates
            if candidate.commitment_state is SubjectCommitmentState.COMMITTED
        }
        declared = set(self.committed_subject_ids)
        if committed != declared:
            raise ValueError(
                "committed_subject_ids must be exactly the canonical ids of "
                f"COMMITTED candidates; committed={sorted(committed)}, "
                f"declared={sorted(declared)}"
            )
        if len(self.committed_subject_ids) != len(declared):
            raise ValueError("committed_subject_ids must not repeat a subject")
        for candidate in self.candidates:
            if candidate.commitment_state is not SubjectCommitmentState.COMMITTED:
                continue
            signals = {signal.signal for signal in candidate.match_signals}
            if signals <= WEAK_SUBJECT_MATCH_SIGNALS:
                raise ValueError(
                    f"candidate {candidate.candidate_id} is committed on weak "
                    f"signals alone ({sorted(str(item) for item in signals)}); "
                    "a fuzzy label match is what returns a similarly named "
                    "subject, so it cannot carry a commitment by itself"
                )
            if candidate.rank != 1:
                raise ValueError(
                    f"candidate {candidate.candidate_id} is committed at rank "
                    f"{candidate.rank}; committing to a candidate the "
                    "investigation itself ranked below another, without "
                    "clarification, is an unexaminable subject choice"
                )
        return self

    @model_validator(mode="after")
    def validate_unresolved_mentions_reference_declared_candidates(self) -> Self:
        known = {candidate.candidate_id for candidate in self.candidates}
        for mention in self.unresolved_mentions:
            dangling = sorted(set(mention.candidate_ids) - known)
            if dangling:
                raise ValueError(
                    f"unresolved mention {mention.mention_id} names candidate "
                    f"ids that were never declared: {dangling}"
                )
            if (
                mention.reason is UnresolvedMentionReason.MULTIPLE_CANDIDATES
                and len(mention.candidate_ids) < 2
            ):
                raise ValueError(
                    f"unresolved mention {mention.mention_id} claims multiple "
                    "candidates but names fewer than two"
                )
        return self

    @model_validator(mode="after")
    def validate_truncation_disclosure(self) -> Self:
        _require_truncation_reason(
            self.candidates_truncated, self.truncation_reason, "subject_discovery"
        )
        return self


# --------------------------------------------------------------------------
# Comparison cohort
# --------------------------------------------------------------------------


class CohortMember(ContractModelV2):
    """A subject in the comparison cohort, with its inclusion justified."""

    subject_kind: InvestigationSubjectKind
    canonical_id: OpaqueID
    display_label: Label
    inclusion_basis: tuple[CohortInclusionBasis, ...] = Field(
        min_length=1, max_length=5
    )
    inclusion_rationale: ShortText
    inclusion_evidence_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=10
    )
    inclusion_evidence_classification: CohortEvidenceClassification | None = None
    relevance: RelevanceState

    @model_validator(mode="after")
    def validate_inclusion_is_evidenced(self) -> Self:
        """Evidence handles XOR an explicit no-evidence classification.

        The same F10 pattern as ``DevMetricRefV2`` and ``DeficiencyFinding``.
        "Neither" is the fault shape: a cohort member with no evidence and
        no stated reason for having none is exactly how an unrelated project
        joins a comparison unnoticed. "Both" is rejected too, because a
        classification exists only to explain an *absence*.
        """

        has_evidence = bool(self.inclusion_evidence_ids)
        has_classification = self.inclusion_evidence_classification is not None
        if has_evidence and has_classification:
            raise ValueError(
                f"cohort member {self.canonical_id} carries evidence and an "
                "inclusion_evidence_classification; the classification exists "
                "only for the no-evidence case"
            )
        if not has_evidence and not has_classification:
            raise ValueError(
                f"cohort member {self.canonical_id} carries neither evidence "
                "nor an explicit no-evidence classification; an unjustified "
                "member is an unrelated member nobody can spot"
            )
        if len(set(self.inclusion_basis)) != len(self.inclusion_basis):
            raise ValueError(
                f"cohort member {self.canonical_id} repeats an inclusion basis"
            )
        return self


class CohortExclusion(ContractModelV2):
    """A subject considered for the cohort and deliberately left out."""

    subject_kind: InvestigationSubjectKind
    canonical_id: OpaqueID
    reason: CohortExclusionReason
    rationale: ShortText


class ComparisonCohort(ContractModelV2):
    """The comparison cohort, its exclusions, and how complete it is."""

    schema_version: Literal["ask_dev_comparison_cohort.v1"]
    cohort_id: OpaqueID
    comparison_shape: ComparisonShape
    members: tuple[CohortMember, ...] = Field(default_factory=tuple, max_length=50)
    exclusions: tuple[CohortExclusion, ...] = Field(
        default_factory=tuple, max_length=50
    )
    supported_comparison_dimensions: tuple[ComparisonDimension, ...] = Field(
        default_factory=tuple, max_length=12
    )
    completeness: CohortCompleteness
    truncation_reason: TruncationReason | None = None
    cohort_uncertainty: ShortText | None = None
    authorization_filtered_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_membership_is_coherent(self) -> Self:
        member_ids = [member.canonical_id for member in self.members]
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("cohort members must be unique by canonical_id")
        excluded_ids = [exclusion.canonical_id for exclusion in self.exclusions]
        if len(set(excluded_ids)) != len(excluded_ids):
            raise ValueError("cohort exclusions must be unique by canonical_id")
        both = sorted(set(member_ids) & set(excluded_ids))
        if both:
            raise ValueError(f"subjects are both included and excluded: {both}")
        if len(set(self.supported_comparison_dimensions)) != len(
            self.supported_comparison_dimensions
        ):
            raise ValueError("supported_comparison_dimensions repeats a dimension")
        return self

    @model_validator(mode="after")
    def validate_comparison_is_not_vacuous(self) -> Self:
        """A cohort that claims to compare must be able to.

        Two members and at least one declared dimension. Without the
        dimension the cohort is a list the reader has to compare themselves;
        without the second member there is nothing to compare at all. Both
        are well-formed shapes that would score as "comparison supported"
        while supporting no comparison — the wildcard/optional-field
        vacuity fault, in its cohort-shaped form.
        """

        if self.comparison_shape is ComparisonShape.SINGULAR_SUBJECT:
            if len(self.members) > 1:
                raise ValueError(
                    "a singular-subject shape cannot carry more than one cohort member"
                )
            return self
        if self.comparison_shape in COHORT_BEARING_SHAPES:
            if len(self.members) < 2:
                raise ValueError(
                    f"a {self.comparison_shape} cohort needs at least two "
                    f"members to compare; got {len(self.members)}"
                )
            if not self.supported_comparison_dimensions:
                raise ValueError(
                    f"a {self.comparison_shape} cohort declares no supported "
                    "comparison dimension, so it supports no comparison"
                )
        return self

    @model_validator(mode="after")
    def validate_completeness_disclosure(self) -> Self:
        _require_truncation_reason(
            self.completeness is CohortCompleteness.TRUNCATED,
            self.truncation_reason,
            "comparison_cohort",
        )
        uncertain = self.completeness is CohortCompleteness.BEST_EFFORT_UNCERTAIN
        if uncertain and self.cohort_uncertainty is None:
            raise ValueError("a best-effort cohort must say what it is uncertain about")
        if not uncertain and self.cohort_uncertainty is not None:
            raise ValueError(
                "cohort_uncertainty is only meaningful on a best-effort cohort"
            )
        return self


# --------------------------------------------------------------------------
# Related context and lineage
# --------------------------------------------------------------------------


class LineageHop(ContractModelV2):
    """One traversed edge.

    ``source_entity_id`` is always the entity traversal started from and
    ``target_entity_id`` the entity it arrived at; ``direction`` says whether
    that traversal followed the relationship's canonical orientation or ran
    against it. Separating traversal order from canonical orientation is
    what makes a reversed relationship detectable at all — with a single
    combined field, "team owns project" and "project owned by team" are the
    same bytes.
    """

    source_entity_id: OpaqueID
    source_entity_kind: InvestigationSubjectKind
    relationship: RelationshipType
    direction: RelationshipDirection
    target_entity_id: OpaqueID
    target_entity_kind: InvestigationSubjectKind
    observed_at: AwareDatetime | None = None
    relevance: RelevanceState

    @model_validator(mode="after")
    def validate_direction_matches_allowlist(self) -> Self:
        """The endpoint kinds must match the declared canonical orientation."""

        orientation = RELATIONSHIP_ALLOWLIST[self.relationship]
        if self.direction is RelationshipDirection.FORWARD:
            source_kind, target_kind = self.source_entity_kind, self.target_entity_kind
        else:
            source_kind, target_kind = self.target_entity_kind, self.source_entity_kind
        if not orientation.permits(source_kind, target_kind):
            raise ValueError(
                f"hop {self.source_entity_id} -[{self.relationship}/"
                f"{self.direction}]-> {self.target_entity_id} contradicts the "
                f"canonical orientation ({orientation.canonical_reading}); "
                f"{source_kind} -> {target_kind} is not a declared ordering"
            )
        if self.source_entity_id == self.target_entity_id:
            raise ValueError(
                f"hop on {self.source_entity_id} points at itself; a self-loop "
                "explains nothing and inflates path recall"
            )
        return self


class LineagePath(ContractModelV2):
    """A bounded chain of hops, and why it was included."""

    path_id: OpaqueID
    origin_entity_id: OpaqueID
    terminal_entity_id: OpaqueID
    hops: tuple[LineageHop, ...] = Field(min_length=1, max_length=6)
    inclusion_reason: ShortText
    relevance: RelevanceState
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )
    truncated: bool
    truncation_reason: TruncationReason | None = None
    source_health: SourceRequirementState

    @model_validator(mode="after")
    def validate_path_is_connected(self) -> Self:
        """Hops chain end-to-end and match the declared endpoints.

        A "path" whose hops do not actually join is two unrelated facts
        presented as a causal chain, which is how a plausible but false
        lineage gets built out of true edges.
        """

        first, last = self.hops[0], self.hops[-1]
        if first.source_entity_id != self.origin_entity_id:
            raise ValueError(
                f"path {self.path_id} declares origin {self.origin_entity_id} "
                f"but its first hop starts at {first.source_entity_id}"
            )
        if last.target_entity_id != self.terminal_entity_id:
            raise ValueError(
                f"path {self.path_id} declares terminal "
                f"{self.terminal_entity_id} but its last hop ends at "
                f"{last.target_entity_id}"
            )
        for index in range(len(self.hops) - 1):
            current, following = self.hops[index], self.hops[index + 1]
            if current.target_entity_id != following.source_entity_id:
                raise ValueError(
                    f"path {self.path_id} is not connected: hop {index} ends "
                    f"at {current.target_entity_id} but hop {index + 1} "
                    f"starts at {following.source_entity_id}"
                )
        return self

    @model_validator(mode="after")
    def validate_truncation_disclosure(self) -> Self:
        _require_truncation_reason(
            self.truncated, self.truncation_reason, f"path {self.path_id}"
        )
        return self


class RelatedEntity(ContractModelV2):
    """A canonical entity the investigation pulled in, and its justification.

    ``supporting_path_ids`` is ``min_length=1``: an entity nothing connects
    to the subject is not related context, it is noise that displaces the
    lineage actually asked for.
    """

    entity_id: OpaqueID
    entity_kind: InvestigationSubjectKind
    display_label: Label
    inclusion_reason: ShortText
    supporting_path_ids: tuple[OpaqueID, ...] = Field(min_length=1, max_length=10)
    relevance: RelevanceState
    observed_at: AwareDatetime | None = None


class RelatedContext(ContractModelV2):
    """Related entities, the paths that reach them, and the authorized set."""

    schema_version: Literal["ask_dev_related_context.v1"]
    entities: tuple[RelatedEntity, ...] = Field(default_factory=tuple, max_length=100)
    paths: tuple[LineagePath, ...] = Field(default_factory=tuple, max_length=100)
    authorized_entity_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=500
    )
    authorization_filtered_count: int = Field(ge=0)
    entities_truncated: bool
    paths_truncated: bool
    truncation_reason: TruncationReason | None = None

    @model_validator(mode="after")
    def validate_identifiers_are_unique(self) -> Self:
        entity_ids = [entity.entity_id for entity in self.entities]
        if len(set(entity_ids)) != len(entity_ids):
            raise ValueError("related entities must be unique by entity_id")
        path_ids = [path.path_id for path in self.paths]
        if len(set(path_ids)) != len(path_ids):
            raise ValueError("lineage paths must be unique by path_id")
        if len(set(self.authorized_entity_ids)) != len(self.authorized_entity_ids):
            raise ValueError("authorized_entity_ids repeats an entity")
        return self

    @model_validator(mode="after")
    def validate_entities_are_reachable(self) -> Self:
        """Cited paths must exist **and** must actually reach the entity.

        Adversarial review round 1, finding M5. The original check only
        rejected dangling path ids, so an entity could cite a real path that
        terminates somewhere else entirely — the Auth Gateway service citing
        the project-owns-team path — and the packet would present two
        unrelated true facts as a single line of reasoning. Existence is not
        attachment.
        """

        by_id = {path.path_id: path for path in self.paths}
        for entity in self.entities:
            dangling = sorted(set(entity.supporting_path_ids) - set(by_id))
            if dangling:
                raise ValueError(
                    f"related entity {entity.entity_id} cites paths that were "
                    f"never declared: {dangling}"
                )
            unreached = sorted(
                path_id
                for path_id in entity.supporting_path_ids
                if entity.entity_id not in _path_entity_ids(by_id[path_id])
            )
            if unreached:
                raise ValueError(
                    f"related entity {entity.entity_id} cites paths that never "
                    f"reach it: {unreached}; a path that does not touch the "
                    "entity is not why the entity is here"
                )
        return self

    @model_validator(mode="after")
    def validate_paths_stay_inside_authorized_set(self) -> Self:
        """No hop endpoint or related entity may sit outside the authorized set.

        Enforced on *every* endpoint of every hop, not only on the entities
        the packet returns as results. A path that merely routes through a
        restricted entity still discloses that the entity exists and that it
        connects two things the caller can see — which is the leak, even
        when its own record is never returned.

        **What this cannot check, stated plainly.** ``authorized_entity_ids``
        is declared by the producer, so this validator proves *internal
        consistency* — the packet did not traverse anything it did not also
        claim was authorized — and not that the claim is true. An arm that
        listed the whole organization as authorized would pass here. That
        residual is why ``ZERO_UNAUTHORIZED_RESULTS`` is scored by CHAOS-3616
        against a real authorization oracle as well as by this check, and
        why the honest description of this guard is "makes an unauthorized
        traversal impossible to hide", not "makes it impossible".
        """

        authorized = set(self.authorized_entity_ids)
        offenders: set[str] = set()
        for path in self.paths:
            for hop in path.hops:
                for entity_id in (hop.source_entity_id, hop.target_entity_id):
                    if entity_id not in authorized:
                        offenders.add(entity_id)
        for entity in self.entities:
            if entity.entity_id not in authorized:
                offenders.add(entity.entity_id)
        if offenders:
            raise ValueError(
                "related context crosses entities outside the authorized set: "
                f"{sorted(offenders)}"
            )
        return self

    @model_validator(mode="after")
    def validate_truncation_disclosure(self) -> Self:
        _require_truncation_reason(
            self.entities_truncated or self.paths_truncated,
            self.truncation_reason,
            "related_context",
        )
        return self


# --------------------------------------------------------------------------
# Driver candidates
# --------------------------------------------------------------------------


class StaffingQualification(ContractModelV2):
    """How much allocation evidence stands behind a capacity claim."""

    denominator_state: StaffingDenominatorState
    denominator_source_classes: tuple[SourceClass, ...] = Field(
        default_factory=tuple, max_length=10
    )
    qualification_note: ShortText

    @model_validator(mode="after")
    def validate_available_denominator_names_its_sources(self) -> Self:
        if (
            self.denominator_state
            is StaffingDenominatorState.ALLOCATION_EVIDENCE_AVAILABLE
            and not self.denominator_source_classes
        ):
            raise ValueError(
                "a staffing denominator claimed available must name the source "
                "classes it came from"
            )
        return self


class DriverCandidate(ContractModelV2):
    """A candidate driver, its standing, and everything that standing rests on."""

    driver_id: OpaqueID
    category: DriverCategory
    summary: ShortText
    affected_subject_ids: tuple[OpaqueID, ...] = Field(min_length=1, max_length=50)
    role: DriverRole
    standing: DriverStanding
    assertion_basis: AssertionBasis
    confidence_qualifier: ConfidenceQualifier
    supporting_path_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=20
    )
    supporting_evidence_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )
    conflicting_evidence_ids: tuple[EvidenceHandle, ...] = Field(
        default_factory=tuple, max_length=25
    )
    conflict_note: ShortText | None = None
    relevance: RelevanceState
    exclusion_reason: DriverExclusionReason | None = None
    staffing_qualification: StaffingQualification | None = None

    @model_validator(mode="after")
    def validate_principal_standing_is_earned(self) -> Self:
        """Principal standing requires a driver, a path, evidence and currency.

        This is the whole symptom-as-driver guard. "Cycle time is up" is a
        real observation and a legitimate ``SYMPTOM`` candidate; what it may
        not be is the principal driver, and what no candidate may be is
        principal without lineage explaining the mechanism, evidence
        supporting it, and current relevance. Contributing standing is held
        to the same requirements at a lower bar (path or evidence, not
        necessarily both) because a contributing driver is still an
        assertion the packet is making.
        """

        if self.standing is DriverStanding.PRINCIPAL_DRIVER:
            if self.role is not DriverRole.DRIVER:
                raise ValueError(
                    f"driver {self.driver_id} is promoted to principal while "
                    f"classified {self.role}; a symptom is not a driver"
                )
            if not self.supporting_path_ids:
                raise ValueError(
                    f"driver {self.driver_id} is principal with no supporting "
                    "relationship path; without lineage there is no mechanism, "
                    "only a correlation"
                )
            if not self.supporting_evidence_ids:
                raise ValueError(
                    f"driver {self.driver_id} is principal with no supporting evidence"
                )
            if self.relevance not in CURRENTLY_RELEVANT_STATES:
                raise ValueError(
                    f"driver {self.driver_id} is principal but its relevance "
                    f"is {self.relevance}; a historical cause is not a "
                    "principal current driver"
                )
        if self.standing is DriverStanding.CONTRIBUTING_DRIVER:
            if self.role is DriverRole.SYMPTOM:
                raise ValueError(
                    f"driver {self.driver_id} contributes as a driver while "
                    "classified a symptom"
                )
            if not self.supporting_path_ids and not self.supporting_evidence_ids:
                raise ValueError(
                    f"driver {self.driver_id} is a contributing driver with "
                    "neither a supporting path nor supporting evidence"
                )
        return self

    @model_validator(mode="after")
    def validate_exclusion_is_stated(self) -> Self:
        excluded = self.standing is DriverStanding.EXCLUDED
        if excluded and self.exclusion_reason is None:
            raise ValueError(
                f"driver {self.driver_id} is excluded without a reason; the "
                "reasons a candidate did not reach principal standing are part "
                "of the investigation result, not an implementation detail"
            )
        if not excluded and self.exclusion_reason is not None:
            raise ValueError(
                f"driver {self.driver_id} carries an exclusion_reason while "
                f"holding {self.standing} standing"
            )
        return self

    @model_validator(mode="after")
    def validate_assertion_basis_matches_support(self) -> Self:
        if self.assertion_basis is AssertionBasis.MEASURED:
            if not self.supporting_evidence_ids:
                raise ValueError(
                    f"driver {self.driver_id} claims a measured basis with no "
                    "evidence handle; canonical services mint evidence for "
                    "what they measure"
                )
        elif self.confidence_qualifier is ConfidenceQualifier.MEASURED_CERTAIN:
            raise ValueError(
                f"driver {self.driver_id} is {self.assertion_basis} but claims "
                "measured certainty; only a measured basis may be certain"
            )
        return self

    @model_validator(mode="after")
    def validate_conflicts_are_disclosed(self) -> Self:
        has_conflict = bool(self.conflicting_evidence_ids)
        if has_conflict and self.conflict_note is None:
            raise ValueError(
                f"driver {self.driver_id} carries conflicting evidence with no "
                "conflict_note"
            )
        if not has_conflict and self.conflict_note is not None:
            raise ValueError(
                f"driver {self.driver_id} declares a conflict_note with no "
                "conflicting evidence"
            )
        if has_conflict and self.confidence_qualifier is (
            ConfidenceQualifier.MEASURED_CERTAIN
        ):
            raise ValueError(
                f"driver {self.driver_id} presents conflicting evidence as "
                "measured certainty"
            )
        overlap = sorted(
            set(self.supporting_evidence_ids) & set(self.conflicting_evidence_ids)
        )
        if overlap:
            raise ValueError(
                f"driver {self.driver_id} lists the same evidence as both "
                f"supporting and conflicting: {overlap}"
            )
        return self

    @model_validator(mode="after")
    def validate_staffing_claims_are_qualified(self) -> Self:
        """Capacity claims must be qualified exactly as far as their evidence.

        Both halves of the correction addendum's staffing rule live here.
        A capacity/staffing driver must carry a
        :class:`StaffingQualification` at all — silence about the
        denominator is the commonest way an unsupported staffing claim
        reaches an answer. And when that denominator is partial or absent,
        the claim may not be presented as ``MEASURED_CERTAIN``.

        What this deliberately does **not** do is force such a claim to
        ``UNSUPPORTED``: ``QUALIFIED`` and ``UNCERTAIN`` are both legal, and
        ``test_missing_denominator_still_supports_a_qualified_capacity_claim``
        pins that a missing denominator reduces confidence rather than
        killing the answer.
        """

        if self.category is not DriverCategory.CAPACITY_OR_STAFFING:
            if self.staffing_qualification is not None:
                raise ValueError(
                    f"driver {self.driver_id} carries a staffing qualification "
                    f"while categorised {self.category}"
                )
            return self
        if self.staffing_qualification is None:
            raise ValueError(
                f"driver {self.driver_id} is a capacity/staffing driver with no "
                "staffing_qualification; a staffing claim that says nothing "
                "about its denominator is an unsupported claim"
            )
        weak = (
            self.staffing_qualification.denominator_state
            in UNQUALIFIED_DENOMINATOR_STATES
        )
        if weak and self.confidence_qualifier is ConfidenceQualifier.MEASURED_CERTAIN:
            raise ValueError(
                f"driver {self.driver_id} presents a staffing claim as certain "
                f"while its denominator is "
                f"{self.staffing_qualification.denominator_state}"
            )
        return self


class DriverAnalysis(ContractModelV2):
    """Every driver candidate, and which of them are principal."""

    schema_version: Literal["ask_dev_driver_analysis.v1"]
    candidates: tuple[DriverCandidate, ...] = Field(
        default_factory=tuple, max_length=50
    )
    principal_driver_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=10
    )
    candidates_truncated: bool
    truncation_reason: TruncationReason | None = None

    @model_validator(mode="after")
    def validate_principal_list_matches_standings(self) -> Self:
        driver_ids = [candidate.driver_id for candidate in self.candidates]
        if len(set(driver_ids)) != len(driver_ids):
            raise ValueError("driver candidates must be unique by driver_id")
        principal = {
            candidate.driver_id
            for candidate in self.candidates
            if candidate.standing is DriverStanding.PRINCIPAL_DRIVER
        }
        declared = set(self.principal_driver_ids)
        if principal != declared:
            raise ValueError(
                "principal_driver_ids must be exactly the candidates holding "
                f"principal standing; standings={sorted(principal)}, "
                f"declared={sorted(declared)}"
            )
        if len(self.principal_driver_ids) != len(declared):
            raise ValueError("principal_driver_ids must not repeat a driver")
        return self

    @model_validator(mode="after")
    def validate_truncation_disclosure(self) -> Self:
        _require_truncation_reason(
            self.candidates_truncated, self.truncation_reason, "driver_analysis"
        )
        return self


# --------------------------------------------------------------------------
# Evidence, coverage and limitations
# --------------------------------------------------------------------------


class InvestigationEvidenceEntry(ContractModelV2):
    """One evidence item, and what in this packet it actually supports."""

    evidence: DevEvidenceRefV2
    source_class: SourceClass
    supports_path_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=20
    )
    supports_entity_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=20
    )
    supports_driver_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=20
    )
    supports_subject_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=20
    )
    relevance: RelevanceState

    @model_validator(mode="after")
    def validate_supports_something(self) -> Self:
        """Indexed evidence must support something the packet includes.

        This is the displacement half of evidence closure. An arm with a
        large, cheap source (commits, comments) can otherwise flood the
        index with items attached to nothing, burying the lineage that was
        asked for under material that is individually true and collectively
        irrelevant.
        """

        if not (
            self.supports_path_ids
            or self.supports_entity_ids
            or self.supports_driver_ids
            or self.supports_subject_ids
        ):
            raise ValueError(
                f"evidence {self.evidence.evidence_ref_id} supports nothing in "
                "this packet; unattached evidence displaces lineage rather "
                "than adding to it"
            )
        return self


class SourceHealthObservation(ContractModelV2):
    """The observed state of one source class during the investigation."""

    source_class: SourceClass
    state: SourceRequirementState
    observed_at: AwareDatetime | None = None
    detail: ShortText | None = None


class MissingSource(ContractModelV2):
    """A source the investigation wanted and did not get."""

    source_class: SourceClass
    state: SourceRequirementState
    impact: ShortText


class SourceConflict(ContractModelV2):
    """Two or more evidence items that disagree."""

    conflict_id: OpaqueID
    evidence_ref_ids: tuple[EvidenceHandle, ...] = Field(min_length=2, max_length=10)
    description: ShortText
    resolution: ConflictResolution


class EvidenceCoverage(ContractModelV2):
    """The evidence index, what was missing, and what the packet cannot say."""

    schema_version: Literal["ask_dev_evidence_coverage.v1"]
    evidence_index: tuple[InvestigationEvidenceEntry, ...] = Field(
        default_factory=tuple, max_length=200
    )
    source_health: tuple[SourceHealthObservation, ...] = Field(
        default_factory=tuple, max_length=25
    )
    missing_sources: tuple[MissingSource, ...] = Field(
        default_factory=tuple, max_length=25
    )
    conflicts: tuple[SourceConflict, ...] = Field(default_factory=tuple, max_length=25)
    limitations: tuple[PacketLimitation, ...] = Field(
        default_factory=tuple, max_length=25
    )
    clarification_needs: tuple[ClarificationNeed, ...] = Field(
        default_factory=tuple, max_length=10
    )
    authorization_filtered_count: int = Field(ge=0)
    evidence_truncated: bool
    truncation_reason: TruncationReason | None = None

    @model_validator(mode="after")
    def validate_index_is_coherent(self) -> Self:
        handles = [entry.evidence.evidence_ref_id for entry in self.evidence_index]
        if len(set(handles)) != len(handles):
            raise ValueError("evidence_index repeats an evidence handle")
        health_classes = [item.source_class for item in self.source_health]
        if len(set(health_classes)) != len(health_classes):
            raise ValueError("source_health declares a source class twice")
        missing_classes = [item.source_class for item in self.missing_sources]
        if len(set(missing_classes)) != len(missing_classes):
            raise ValueError("missing_sources declares a source class twice")
        conflict_ids = [conflict.conflict_id for conflict in self.conflicts]
        if len(set(conflict_ids)) != len(conflict_ids):
            raise ValueError("conflicts must be unique by conflict_id")
        indexed = set(handles)
        for conflict in self.conflicts:
            dangling = sorted(set(conflict.evidence_ref_ids) - indexed)
            if dangling:
                raise ValueError(
                    f"conflict {conflict.conflict_id} cites evidence that is "
                    f"not in the index: {dangling}"
                )
        limitation_kinds = [limitation.kind for limitation in self.limitations]
        if len(set(limitation_kinds)) != len(limitation_kinds):
            raise ValueError(
                "limitations repeats a kind; one disclosure per kind keeps the "
                "list a real summary rather than a pile"
            )
        contradictory = sorted(
            str(item) for item in set(health_classes) & set(missing_classes)
        )
        if contradictory:
            # Adversarial review round 1, finding M6. Uniqueness was checked
            # *within* each list but never *between* them, so a producer could
            # report work_graph as available_current in source_health and
            # unavailable in missing_sources at the same time -- and then pick
            # whichever reading flattered its coverage score.
            raise ValueError(
                "these source classes are reported both as observed and as "
                f"missing: {contradictory}; a source cannot be simultaneously "
                "available and absent, and a consumer cannot know which "
                "coverage claim to score"
            )
        return self

    @model_validator(mode="after")
    def validate_disclosures_match_state(self) -> Self:
        _require_truncation_reason(
            self.evidence_truncated, self.truncation_reason, "evidence_coverage"
        )
        kinds = {limitation.kind for limitation in self.limitations}
        if self.missing_sources and PacketLimitationKind.MISSING_SOURCE not in kinds:
            raise ValueError(
                "missing sources are recorded but no MISSING_SOURCE limitation "
                "is disclosed"
            )
        unresolved = any(
            conflict.resolution is ConflictResolution.UNRESOLVED
            for conflict in self.conflicts
        )
        if unresolved and PacketLimitationKind.CONFLICTING_EVIDENCE not in kinds:
            raise ValueError(
                "an unresolved source conflict is recorded but no "
                "CONFLICTING_EVIDENCE limitation is disclosed"
            )
        return self


# --------------------------------------------------------------------------
# Versions and trial metadata
# --------------------------------------------------------------------------


class SourceContractVersion(ContractModelV2):
    """Which version of a source's own contract the investigation read."""

    source_class: SourceClass
    contract_version: PlatformVersionToken


class TrialMetadata(ContractModelV2):
    """Evaluation metadata — never product truth.

    Arm identity lives here and only here, and the field that holds it is
    optional on :class:`InvestigationVersions`. That is the structural
    statement of "arm identity should be evaluation metadata": a native
    packet is complete without it, no consumer may branch on it, and no
    field anywhere in this contract is mandatory only for one arm.
    """

    arm_id: OpaqueID
    producer_id: PlatformVersionToken
    fixture_version: PlatformVersionToken | None = None
    run_id: ServerHandle | None = None


class InvestigationVersions(ContractModelV2):
    """Every version an investigation result is reproducible against."""

    schema_version: Literal["ask_dev_investigation_versions.v1"]
    packet_schema_version: Literal["ask_dev_investigation_packet.v1"]
    query_version: PlatformVersionToken
    ranking_version: PlatformVersionToken
    projection_version: PlatformVersionToken
    source_contract_versions: tuple[SourceContractVersion, ...] = Field(
        min_length=1, max_length=25
    )
    corpus_version: PlatformVersionToken | None = None
    trial: TrialMetadata | None = None

    @model_validator(mode="after")
    def validate_source_contract_versions_are_unique(self) -> Self:
        classes = [item.source_class for item in self.source_contract_versions]
        if len(set(classes)) != len(classes):
            raise ValueError("source_contract_versions declares a source class twice")
        return self


# --------------------------------------------------------------------------
# The packet
# --------------------------------------------------------------------------


class AskDevInvestigationPacket(ContractModelV2):
    """A bounded, authorized, backend-neutral investigation result.

    Not a final user answer, not a dashboard response, not a graph-native
    query response, not an LLM reasoning trace, not arbitrary traversal
    output. It is the input the Ask Dev frame reasons over, and the artifact
    both trial arms are scored on.
    """

    schema_version: Literal["ask_dev_investigation_packet.v1"]
    packet_id: ServerHandle
    organization_id: OpaqueID
    produced_at: AwareDatetime
    outcome: InvestigationOutcome
    analytical_job: AnalyticalJob
    subject_discovery: SubjectDiscovery
    comparison_cohort: ComparisonCohort
    related_context: RelatedContext
    driver_analysis: DriverAnalysis
    evidence_coverage: EvidenceCoverage
    versions: InvestigationVersions

    @model_validator(mode="after")
    def validate_cohort_shape_matches_job(self) -> Self:
        if self.comparison_cohort.comparison_shape is not (
            self.analytical_job.comparison_shape
        ):
            raise ValueError(
                "the cohort's comparison_shape "
                f"({self.comparison_cohort.comparison_shape}) contradicts the "
                f"job's ({self.analytical_job.comparison_shape})"
            )
        return self

    @model_validator(mode="after")
    def validate_every_entity_is_authorized(self) -> Self:
        """The authorization envelope covers the **whole packet**, not one section.

        Adversarial review round 1, finding H2. ``RelatedContext``'s own guard
        checks the entities and hop endpoints it owns, and nothing else — so a
        cohort member, a subject candidate, a driver's affected subject or an
        indexed evidence item could name an entity outside the authorized set
        and the packet still validated. Every one of those is a label or an
        identifier reaching a consumer, which is the leak; scoping the check
        to lineage only protected the least likely route.

        Residual, unchanged and still worth repeating: the authorized set is
        producer-declared, so this proves the packet is internally consistent
        with its own claim, not that the claim is true. See
        ``RelatedContext.validate_paths_stay_inside_authorized_set``.
        """

        authorized = set(self.related_context.authorized_entity_ids)
        offenders: dict[str, set[str]] = {}

        def flag(where: str, entity_id: str) -> None:
            if entity_id not in authorized:
                offenders.setdefault(where, set()).add(entity_id)

        for candidate in self.subject_discovery.candidates:
            flag("subject_discovery.candidates", candidate.canonical_id)
        for member in self.comparison_cohort.members:
            flag("comparison_cohort.members", member.canonical_id)
        for exclusion in self.comparison_cohort.exclusions:
            flag("comparison_cohort.exclusions", exclusion.canonical_id)
        for driver in self.driver_analysis.candidates:
            for subject_id in driver.affected_subject_ids:
                flag("driver_analysis.affected_subject_ids", subject_id)
        for entry in self.evidence_coverage.evidence_index:
            flag("evidence_coverage.evidence_index", entry.evidence.entity_id)
        for ref in self.analytical_job.surface_context_refs:
            if ref.entity_id is not None:
                flag("analytical_job.surface_context_refs", ref.entity_id)

        if offenders:
            detail = "; ".join(
                f"{where}: {sorted(ids)}" for where, ids in sorted(offenders.items())
            )
            raise ValueError(
                "these entities appear in the packet but not in "
                f"related_context.authorized_entity_ids -- {detail}"
            )
        return self

    @model_validator(mode="after")
    def validate_sources_are_allowlisted(self) -> Self:
        """Every source class named anywhere must be on the trial allowlist.

        Adversarial review round 1, finding H3. ``TRIAL_SOURCE_ALLOWLIST``
        deliberately excludes ``SourceClass.TEMPORAL_CONTEXT`` — CHAOS-3567's
        inert stub — but the packet's ``source_class`` fields were typed as
        the full platform enum and no validator consulted the allowlist, so
        an arm could claim coverage from a source the trial does not score
        and that has no adapter behind it at all.

        Enforced rather than narrowed at the type level on purpose: the field
        type stays ``SourceClass`` so the packet keeps speaking the platform's
        vocabulary, and the *trial's* bound stays visible as a rule that can
        be relaxed for a later trial without a wire change.
        """

        allowed = set(TRIAL_SOURCE_ALLOWLIST)
        offenders: dict[str, set[str]] = {}

        def flag(where: str, source_class: SourceClass) -> None:
            if source_class not in allowed:
                offenders.setdefault(where, set()).add(str(source_class))

        for candidate in self.subject_discovery.candidates:
            for signal in candidate.match_signals:
                flag("subject_discovery.match_signals", signal.source_class)
        for driver in self.driver_analysis.candidates:
            if driver.staffing_qualification is not None:
                for (
                    source_class
                ) in driver.staffing_qualification.denominator_source_classes:
                    flag("driver_analysis.staffing_qualification", source_class)
        for entry in self.evidence_coverage.evidence_index:
            flag("evidence_coverage.evidence_index", entry.source_class)
        for observation in self.evidence_coverage.source_health:
            flag("evidence_coverage.source_health", observation.source_class)
        for missing in self.evidence_coverage.missing_sources:
            flag("evidence_coverage.missing_sources", missing.source_class)
        for version in self.versions.source_contract_versions:
            flag("versions.source_contract_versions", version.source_class)

        if offenders:
            detail = "; ".join(
                f"{where}: {sorted(names)}"
                for where, names in sorted(offenders.items())
            )
            raise ValueError(
                f"these source classes are not on the trial allowlist -- {detail}"
            )
        return self

    @model_validator(mode="after")
    def validate_question_family_obligations(self) -> Self:
        """The declared family's obligations bind the packet that claims it.

        Adversarial review round 1, finding H4. The family registry described
        what each family requires — permitted comparison shapes, source
        classes, populated sections — and nothing checked any of it, so an
        arm could label a discovered-cohort investigation
        ``project_status_drivers`` (the family with the loosest evidence
        requirements), skip the work, and still land in the per-family
        scoring table under a family it never actually answered. That
        corrupts exactly the report CHAOS-3616 exists to produce.

        Required sources are satisfied by being *accounted for*, not by being
        present: an arm that could not read deployments may declare that in
        ``missing_sources``. What it may not do is stay silent, which is the
        difference between a disclosed gap and an undisclosed one.

        CHAOS-3660/CHAOS-3678: a no-op for a production-shaped packet
        (``analytical_job.question_family is None``). The whole family
        registry -- ``QUESTION_FAMILY_REGISTRY``, permitted shapes, required
        source classes, required sections -- is trial-scoring machinery; a
        production packet does not claim a trial family and has nothing here
        to be held to. Production's own answer-quality obligations are a
        separate, not-yet-built mechanism (Phase 2, CHAOS-3669) and are
        deliberately not invented here as a side effect of widening this
        contract.
        """

        if self.analytical_job.question_family is None:
            return self

        family = QUESTION_FAMILY_REGISTRY[self.analytical_job.question_family]

        shape = self.analytical_job.comparison_shape
        if shape not in family.permitted_comparison_shapes:
            permitted = sorted(str(item) for item in family.permitted_comparison_shapes)
            raise ValueError(
                f"family {family.family_id} does not permit the "
                f"{shape} comparison shape (permitted: {permitted}); a packet "
                "cannot claim a family whose shape it does not answer"
            )

        accounted = (
            {entry.source_class for entry in self.evidence_coverage.evidence_index}
            | {item.source_class for item in self.evidence_coverage.source_health}
            | {item.source_class for item in self.evidence_coverage.missing_sources}
        )
        unaccounted = sorted(
            str(item) for item in set(family.required_source_classes) - accounted
        )
        if unaccounted:
            raise ValueError(
                f"family {family.family_id} requires source classes that this "
                f"packet neither observed nor declared missing: {unaccounted}"
            )

        populated = {
            PacketSection.ANALYTICAL_JOB: True,
            PacketSection.SUBJECT_DISCOVERY: bool(self.subject_discovery.candidates),
            PacketSection.COMPARISON_COHORT: bool(self.comparison_cohort.members),
            PacketSection.RELATED_CONTEXT: bool(self.related_context.entities),
            PacketSection.DRIVER_ANALYSIS: bool(self.driver_analysis.candidates),
            PacketSection.EVIDENCE_COVERAGE: bool(
                self.evidence_coverage.evidence_index
            ),
            PacketSection.VERSIONS: True,
        }
        # Only a packet that claims to have answered owes the full section
        # set. A clarification or no-match packet legitimately has no cohort
        # and no drivers -- requiring them would force an arm to invent both
        # rather than admit it could not resolve the subject.
        if self.outcome in SUPPORTED_OUTCOMES:
            empty = sorted(
                str(section)
                for section in family.required_packet_sections
                if not populated[section]
            )
            if empty:
                raise ValueError(
                    f"family {family.family_id} requires these packet sections "
                    f"to be populated, and they are empty: {empty}"
                )
        return self

    @model_validator(mode="after")
    def validate_no_unsafe_organization_widening(self) -> Self:
        """An unresolved reference may not become an organization-wide sweep.

        The named fault: the question mentioned a subject, resolution
        failed, and the arm answered about everyone instead of asking. The
        one legitimate way to hold an organization-wide shape with an
        unresolved mention outstanding is to be *asking* — outcome
        ``NEEDS_CLARIFICATION`` with a subject clarification recorded.
        """

        widened = (
            self.analytical_job.comparison_shape is ComparisonShape.ORGANIZATION_WIDE
        )
        if not widened or not self.subject_discovery.unresolved_mentions:
            return self
        asking = self.outcome is InvestigationOutcome.NEEDS_CLARIFICATION
        clarifies_subject = any(
            need.kind in SUBJECT_CLARIFICATION_KINDS
            for need in self.evidence_coverage.clarification_needs
        )
        if not (asking and clarifies_subject):
            mentions = sorted(
                mention.mention_id
                for mention in self.subject_discovery.unresolved_mentions
            )
            raise ValueError(
                "the investigation widened to organization scope with "
                f"unresolved subject references outstanding ({mentions}) "
                "without asking for clarification; widening after a failed "
                "resolution answers a question nobody asked"
            )
        return self

    @model_validator(mode="after")
    def validate_evidence_closure(self) -> Self:
        """Every referenced handle is indexed, and every index entry lands.

        The first half stops dangling citations. The second half — checked
        on the index's own ``supports_*`` fields — stops an evidence dump
        from claiming attachment to paths, entities, drivers or subjects the
        packet never declared.

        **What this cannot check.** Closure is checked *within the packet*.
        Whether a handle would actually verify against
        ``evidence_service.EvidenceHandleService`` is a runtime,
        org-scoped question a static contract cannot answer — the grammar
        pins the handle's shape, and dereferencing pins its validity. A
        packet that passes here can still cite a handle that no longer
        resolves.
        """

        indexed = {
            entry.evidence.evidence_ref_id
            for entry in self.evidence_coverage.evidence_index
        }
        referenced: set[str] = set()
        for candidate in self.subject_discovery.candidates:
            for signal in candidate.match_signals:
                referenced.update(signal.evidence_ref_ids)
        for member in self.comparison_cohort.members:
            referenced.update(member.inclusion_evidence_ids)
        for path in self.related_context.paths:
            referenced.update(path.evidence_ref_ids)
        for driver in self.driver_analysis.candidates:
            referenced.update(driver.supporting_evidence_ids)
            referenced.update(driver.conflicting_evidence_ids)
        dangling = sorted(referenced - indexed)
        if dangling:
            raise ValueError(
                "evidence handles are cited but absent from the evidence "
                f"index: {dangling}"
            )

        known_paths = {path.path_id for path in self.related_context.paths}
        known_entities = {entity.entity_id for entity in self.related_context.entities}
        known_drivers = {driver.driver_id for driver in self.driver_analysis.candidates}
        known_subjects = {
            candidate.canonical_id for candidate in self.subject_discovery.candidates
        } | {member.canonical_id for member in self.comparison_cohort.members}
        for entry in self.evidence_coverage.evidence_index:
            handle = entry.evidence.evidence_ref_id
            for label, cited, known in (
                ("paths", entry.supports_path_ids, known_paths),
                ("entities", entry.supports_entity_ids, known_entities),
                ("drivers", entry.supports_driver_ids, known_drivers),
                ("subjects", entry.supports_subject_ids, known_subjects),
            ):
                unknown = sorted(set(cited) - known)
                if unknown:
                    raise ValueError(
                        f"evidence {handle} claims to support {label} the "
                        f"packet never declared: {unknown}"
                    )
        return self

    @model_validator(mode="after")
    def validate_drivers_reference_declared_material(self) -> Self:
        known_paths = {path.path_id for path in self.related_context.paths}
        known_subjects = (
            {candidate.canonical_id for candidate in self.subject_discovery.candidates}
            | {member.canonical_id for member in self.comparison_cohort.members}
            | {entity.entity_id for entity in self.related_context.entities}
        )
        for driver in self.driver_analysis.candidates:
            dangling_paths = sorted(set(driver.supporting_path_ids) - known_paths)
            if dangling_paths:
                raise ValueError(
                    f"driver {driver.driver_id} cites paths that were never "
                    f"declared: {dangling_paths}"
                )
            dangling_subjects = sorted(
                set(driver.affected_subject_ids) - known_subjects
            )
            if dangling_subjects:
                raise ValueError(
                    f"driver {driver.driver_id} affects subjects the packet "
                    f"never declared: {dangling_subjects}"
                )
        return self

    @model_validator(mode="after")
    def validate_supported_outcome_asserts_a_judgment(self) -> Self:
        """A supported packet must assert something, not just point at data.

        The structural form of "open the dashboard" is a packet that is
        perfectly well formed, cites plenty of evidence, and asserts no
        driver at all — leaving the reader to do the analysis. A supported
        outcome therefore requires at least one principal or contributing
        driver, and each of those has already had to earn its standing with
        lineage and evidence.

        The non-supported outcomes are held to their own floors, so that
        ``NO_MATCH`` cannot become the silent, privileged default for a
        packet that simply found nothing to say.
        """

        asserted = [
            driver
            for driver in self.driver_analysis.candidates
            if driver.standing in ASSERTED_DRIVER_STANDINGS
        ]
        if self.outcome in SUPPORTED_OUTCOMES:
            if not asserted:
                raise ValueError(
                    f"outcome {self.outcome} asserts no principal or "
                    "contributing driver; a packet that only points at "
                    "evidence has redirected, not answered"
                )
            if not self.evidence_coverage.evidence_index:
                raise ValueError(
                    f"outcome {self.outcome} carries an empty evidence index"
                )
            return self
        if asserted:
            raise ValueError(
                f"outcome {self.outcome} asserts drivers "
                f"({sorted(driver.driver_id for driver in asserted)}); a "
                "non-supported investigation must not carry a judgment"
            )
        if self.outcome is InvestigationOutcome.NEEDS_CLARIFICATION:
            if not self.evidence_coverage.clarification_needs:
                raise ValueError(
                    "a needs-clarification outcome must record what it needs clarified"
                )
        if self.outcome is InvestigationOutcome.NO_MATCH:
            if self.subject_discovery.committed_subject_ids:
                raise ValueError("a no-match outcome cannot also commit to a subject")
            if not (
                self.evidence_coverage.clarification_needs
                or self.evidence_coverage.limitations
            ):
                raise ValueError(
                    "a no-match outcome must state a limitation or a "
                    "clarification need; an unexplained no-match is a silent "
                    "default wearing an outcome label"
                )
        if self.outcome is InvestigationOutcome.UNSUPPORTED:
            if not self.evidence_coverage.limitations:
                raise ValueError(
                    "an unsupported outcome must state why it is unsupported"
                )
        return self

    @model_validator(mode="after")
    def validate_partial_results_are_disclosed(self) -> Self:
        """Filtering and truncation anywhere must surface as a limitation.

        Each section already pairs its own flag with a reason; this is the
        packet-level statement, so a consumer reading only ``limitations``
        cannot mistake a filtered or truncated investigation for a complete
        one.
        """

        kinds = {limitation.kind for limitation in self.evidence_coverage.limitations}
        filtered = (
            self.subject_discovery.authorization_filtered_count
            + self.comparison_cohort.authorization_filtered_count
            + self.related_context.authorization_filtered_count
            + self.evidence_coverage.authorization_filtered_count
        )
        if filtered and PacketLimitationKind.AUTHORIZATION_FILTERED not in kinds:
            raise ValueError(
                f"{filtered} results were filtered for authorization but no "
                "AUTHORIZATION_FILTERED limitation is disclosed"
            )
        truncated = any(
            (
                self.subject_discovery.candidates_truncated,
                self.comparison_cohort.completeness is CohortCompleteness.TRUNCATED,
                self.related_context.entities_truncated,
                self.related_context.paths_truncated,
                self.driver_analysis.candidates_truncated,
                self.evidence_coverage.evidence_truncated,
                any(path.truncated for path in self.related_context.paths),
            )
        )
        if truncated and PacketLimitationKind.TRUNCATED_TRAVERSAL not in kinds:
            raise ValueError(
                "the investigation truncated results but no TRUNCATED_TRAVERSAL "
                "limitation is disclosed"
            )
        return self

    @model_validator(mode="after")
    def validate_historical_comparability_is_disclosed(self) -> Self:
        """A not-comparable historical slice must say so — and stays valid.

        CHAOS-3569 leaves historical edge validity unimplemented, so some
        as-of comparisons cannot be reconstructed. The corrective plan's
        ruling is that such rows are NOT COMPARABLE rather than failures:
        this validator therefore requires the *disclosure*, and pointedly
        does not require the outcome to be downgraded.
        """

        comparability = self.analytical_job.time_context.historical_comparability
        if comparability not in NOT_COMPARABLE_STATES:
            return self
        kinds = {limitation.kind for limitation in self.evidence_coverage.limitations}
        job_kinds = {
            limitation.kind
            for limitation in self.analytical_job.interpretation_limitations
        }
        if PacketLimitationKind.HISTORICAL_SLICE_NOT_COMPARABLE not in (
            kinds | job_kinds
        ):
            raise ValueError(
                f"the time context is {comparability} but no "
                "HISTORICAL_SLICE_NOT_COMPARABLE limitation is disclosed"
            )
        return self

    @model_validator(mode="after")
    def validate_staffing_absence_is_disclosed(self) -> Self:
        weak = [
            driver
            for driver in self.driver_analysis.candidates
            if driver.staffing_qualification is not None
            and driver.staffing_qualification.denominator_state
            in UNQUALIFIED_DENOMINATOR_STATES
        ]
        if not weak:
            return self
        kinds = {limitation.kind for limitation in self.evidence_coverage.limitations}
        if PacketLimitationKind.ABSENT_STAFFING_DENOMINATOR not in kinds:
            raise ValueError(
                "a staffing claim rests on a partial or absent allocation "
                "denominator but no ABSENT_STAFFING_DENOMINATOR limitation is "
                "disclosed"
            )
        return self


#: The exported contract registry: ``schema_version`` -> model.
#:
#: Deliberately **not** merged into ``CONTRACT_MODELS_V2``: that registry
#: backs ``contracts/ask-dev/v2``, which is reserved for wire contracts
#: served to real clients (``scripts/acceptance/corpus/receipt.py:6-7``).
#: This packet is an internal trial artifact and gets its own root,
#: ``contracts/ask-dev-investigation/v1``.
INVESTIGATION_CONTRACT_MODELS: dict[str, type[ContractModelV2]] = {
    "ask_dev_investigation_packet.v1": AskDevInvestigationPacket,
    "ask_dev_analytical_job.v1": AnalyticalJob,
    # CHAOS-3660/CHAOS-3678: production's AnalyticalJob shape. Same Python
    # class as ".v1" -- the two schema_version literals distinguish the
    # provenance shape a given instance carries (see
    # validate_provenance_matches_schema_version), not two different models.
    "ask_dev_analytical_job.v2": AnalyticalJob,
    "ask_dev_production_job_provenance.v1": ProductionJobProvenance,
    "ask_dev_subject_discovery.v1": SubjectDiscovery,
    "ask_dev_comparison_cohort.v1": ComparisonCohort,
    "ask_dev_related_context.v1": RelatedContext,
    "ask_dev_driver_analysis.v1": DriverAnalysis,
    "ask_dev_evidence_coverage.v1": EvidenceCoverage,
    "ask_dev_investigation_versions.v1": InvestigationVersions,
}
