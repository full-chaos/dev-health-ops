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
from collections.abc import Container, Mapping, Sequence
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
    CohortExclusion,
    CohortInclusionBasis,
    CohortMember,
    ComparisonCohort,
    ComparisonDimension,
    ComparisonShape,
    DriverAnalysis,
    DriverCandidate,
    DriverStanding,
    EvidenceCoverage,
    HistoricalComparability,
    InvestigationEvidenceEntry,
    InvestigationOutcome,
    InvestigationSubjectKind,
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
    TruncationReason,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import (
    ASSERTED_DRIVER_STANDINGS,
    EdgeValidityBasis,
)

from .backend import (
    SEMANTIC_MECHANISMS,
    DeterministicEmbedder,
    EmbeddingBackend,
    MatchMechanism,
    embedder_projection_suffix,
)
from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .cohort import CohortCandidate, CohortProposal
from .drivers import DriverFinding
from .projection import PROJECTION_VERSION
from .readback import QUERY_VERSION, DiscoveredPath, InvestigationReadout
from .vocabulary import GraphEntityKind, entity_kind_to_subject_kind
from .watermark import DEFAULT_STALENESS_TOLERANCE, IndexWatermark

__all__ = [
    "ARM_ID",
    "PRODUCER_ID",
    "RANKING_VERSION",
    "IncomparableCohortError",
    "JobContext",
    "PacketTooLargeError",
    "SubjectMatchFinding",
    "UnsupportedMatchMechanismError",
    "TrialContext",
    "UnsupportedComparisonShapeError",
    "build_packet",
    "derive_outcome",
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


#: The cohort-bearing shapes this arm revision can actually construct.
#:
#: Both are "peers of a committed subject", which is what
#: :func:`~.cohort.build_cohort` derives from the graph. The two absent
#: shapes are exhaustive claims -- every project in the portfolio, every team
#: in the organization -- and an arm that cannot prove its enumeration was
#: complete must not make them.
_COHORT_CAPABLE_SHAPES: frozenset[ComparisonShape] = frozenset(
    {ComparisonShape.DISCOVERED_COHORT, ComparisonShape.EXPLICIT_COHORT}
)


#: Signals that cannot be produced without semantics, whatever mechanism a
#: producer claims. ``CONVERSATIONAL_REFERENCE`` resolves "the other project
#: we discussed", which no exact or lexical lookup can do; asserting it from
#: a non-semantic run would be a capability claim with nothing behind it.
_INHERENTLY_SEMANTIC_SIGNALS: frozenset[SubjectMatchSignal] = frozenset(
    {SubjectMatchSignal.CONVERSATIONAL_REFERENCE}
)


@dataclass(frozen=True, slots=True)
class SubjectMatchFinding:
    """One reason a candidate subject matched, plus *how* it was produced.

    ``mechanism`` never reaches the wire — the frozen contract has no field
    for it and forbids extras. It exists so :func:`build_packet` can refuse
    to emit a semantic claim that the active embedder cannot support.
    """

    canonical_id: str
    signal: SubjectMatchSignal
    matched_text: str
    source_class: SourceClass
    mechanism: MatchMechanism


class UnsupportedMatchMechanismError(RuntimeError):
    """A semantic match was offered while the embedder carries no semantics.

    The failure this prevents is silent and would corrupt the trial rather
    than break it: nearest-neighbour search over
    :class:`~.backend.DeterministicEmbedder`'s hash vectors returns a
    confident, arbitrary ordering, and a packet that presented it as an
    alias or fuzzy-label match would score as a retrieval capability the arm
    does not have. Raising is the only outcome that cannot be mistaken for a
    result.
    """


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
    shape. Fabricating a cohort here would put a made-up comparison into the
    trial's scoring table under a family that expects a real one.

    Still raised for ``PORTFOLIO_WIDE`` and ``ORGANIZATION_WIDE``. Those are
    not "the same construction with a wider seed": a portfolio-wide shape
    asserts the cohort is *every* member of the portfolio, and this arm has
    no way to know its enumeration was complete — a silently partial sweep
    presented as portfolio-wide is a stronger false claim than refusing.
    """


class IncomparableCohortError(ValueError):
    """A cohort was built and cannot support a comparison.

    Deliberately distinct from :class:`UnsupportedComparisonShapeError`,
    which says the arm cannot do this kind of work at all. This one says the
    arm did the work and the world has no comparison here — fewer than two
    authorized peers, or no dimension their shared bases can speak to. One
    error for both would make a capability gap and an empty result score
    identically, and the trial exists to tell them apart.
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


def _check_match_mechanisms(
    matches: Sequence[SubjectMatchFinding], embedder: EmbeddingBackend
) -> None:
    """Refuse semantic claims the active embedder cannot support.

    Both directions are checked, because a producer can get this wrong from
    either end: a mechanism that needs semantics under a non-semantic
    embedder, and a signal that is *inherently* semantic however it claims to
    have been produced.
    """

    if embedder.semantic:
        return
    offending = [
        (match.canonical_id, match.signal.value, match.mechanism.value)
        for match in matches
        if match.mechanism in SEMANTIC_MECHANISMS
        or match.signal in _INHERENTLY_SEMANTIC_SIGNALS
    ]
    if offending:
        raise UnsupportedMatchMechanismError(
            f"these subject matches claim semantics the active embedder "
            f"({embedder.model_id}) does not have: {offending}. Similarity "
            "over non-semantic vectors is a confident arbitrary ordering, and "
            "a packet presenting it as a match would score as a retrieval "
            "capability this arm does not have"
        )


def _assert_support_is_closed(
    drivers: Sequence[DriverCandidate],
    evidence: Sequence[InvestigationEvidenceEntry],
    known_paths: Container[str],
    about: Mapping[str, frozenset[str]],
) -> None:
    """Every asserted driver's support must be its OWN, and in this packet.

    The contract's rule is satisfied by "some asserted driver exists and the
    evidence index is non-empty" — which is weaker than it reads. Adversarial
    review reproduced both gaps: swapping an honest driver's evidence for an
    indexed handle belonging to a DIFFERENT subject still produced a
    supported outcome, and so did emptying its paths while keeping one
    indexed handle. Neither driver had support; the packet had support
    somewhere, and the outcome could not tell the difference.

    So this arm holds itself to a stricter bar than the contract:

    * the driver must cite evidence, and every handle it cites must be in
      THIS packet's index — a citation the packet cannot resolve is not
      support, it is a reference;
    * the driver must cite a lineage path. The contract permits a
      contributing driver with evidence and no path; this arm does not,
      because "every asserted finding is path-born" is the property the
      whole graph claim rests on. Enforcing it only inside
      ``discover_drivers`` left the packet boundary open, which is exactly
      where a caller-assembled driver arrives.
    """

    by_handle = {entry.evidence.evidence_ref_id: entry for entry in evidence}
    for driver in drivers:
        if driver.standing not in ASSERTED_DRIVER_STANDINGS:
            continue
        # What this driver is ABOUT: the subject it affects and the cause
        # it names. Both matter -- a blocker's evidence is legitimately
        # about the BLOCKER, not about the thing being blocked, and a
        # rule that demanded the affected subject refused every honest
        # blocking driver in the corpus.
        subjects = set(driver.affected_subject_ids) | set(
            about.get(driver.driver_id, frozenset())
        )
        problems: list[str] = []
        if not driver.supporting_path_ids:
            problems.append("cites no lineage path")
        elif any(item not in known_paths for item in driver.supporting_path_ids):
            problems.append("cites a path this packet never declared")
        if not driver.supporting_evidence_ids:
            problems.append("cites no evidence")
        else:
            entries = [
                by_handle[handle]
                for handle in driver.supporting_evidence_ids
                if handle in by_handle
            ]
            if len(entries) != len(driver.supporting_evidence_ids):
                problems.append("cites evidence this packet never indexed")
            elif not any(
                subjects & set(entry.supports_entity_ids) for entry in entries
            ):
                problems.append(
                    "cites only evidence about other subjects, which is "
                    "support for something else"
                )
        if problems:
            raise ValueError(
                f"asserted driver {driver.driver_id} {'; '.join(problems)}. An "
                "asserted driver whose support is not in this packet is a "
                "judgment with nothing behind it"
            )


def derive_outcome(
    drivers: Sequence[DriverCandidate],
    evidence: Sequence[InvestigationEvidenceEntry],
    *,
    gaps: bool,
) -> InvestigationOutcome:
    """What this investigation concluded, derived from what it produced.

    Never passed in. An arm that could be *told* its own outcome could claim
    one it did not earn, and "supported" is precisely the claim the trial
    scores — so it must be a consequence of the packet's contents rather
    than an input beside them.

    The rule is the frozen contract's own: a supported outcome needs at least
    one driver with asserted standing *and* a non-empty evidence index. What
    makes that rule mean what it appears to mean is
    :func:`_assert_support_is_closed`, which runs first and refuses any
    asserted driver whose support is not actually in this packet. This
    revision synthesizes no drivers, so it reaches ``UNSUPPORTED`` every
    time — but it reaches it by evaluating the rule, not by asserting the
    answer. Written as a function precisely so both branches can be observed:
    a constant here would make "the arm never over-claims" untestable, which
    is the same thing as unproven.

    ``gaps`` is whether the run was degraded (stale, truncated or missing a
    required source). It only ever weakens a supported outcome; it can never
    promote one.
    """

    asserted = [
        driver for driver in drivers if driver.standing in ASSERTED_DRIVER_STANDINGS
    ]
    if not asserted or not evidence:
        return InvestigationOutcome.UNSUPPORTED
    if gaps:
        return InvestigationOutcome.SUPPORTED_WITH_GAPS
    return InvestigationOutcome.SUPPORTED


def _driver_candidate(
    finding: DriverFinding,
    handle_by_observation: Mapping[str, str],
    known_subject_ids: Container[str],
) -> DriverCandidate:
    """One structural finding, as the frozen contract's driver candidate.

    Evidence is translated from canonical observation ids to the handles
    this run minted, and an id with no handle **raises**.

    The first version dropped such ids silently. That branch turned out to be
    unreachable in every world under test — the guard-injection harness
    disabled it and every test still passed, reporting SURVIVED — which made
    it dead code wearing the appearance of a safety net. Raising is both
    reachable and correct: a finding citing evidence this packet does not
    carry is an internal inconsistency between discovery and emission, and
    the honest response is to fail rather than to quietly emit a driver with
    less support than it was built from.
    """

    def handles(ids: Sequence[str], role: str) -> tuple[str, ...]:
        missing = sorted(set(ids) - set(handle_by_observation))
        if missing:
            raise ValueError(
                f"driver {finding.driver_id} cites {role} evidence this "
                f"packet never indexed: {missing}. Discovery and emission "
                "disagree about what the run observed"
            )
        return tuple(handle_by_observation[item] for item in ids)

    supporting = handles(finding.evidence_ids, "supporting")
    conflicting = handles(finding.conflicting_evidence_ids, "conflicting")
    affected = tuple(
        item for item in (finding.subject_id,) if item in known_subject_ids
    ) or (finding.subject_id,)
    return DriverCandidate(
        driver_id=finding.driver_id,
        category=finding.category,
        # Arm-authored, and the compose guard knows it: this pair is the one
        # ``(constructor, field)`` entry on the collision list, because
        # ``EntityNode.summary`` two modules away must stay an empty literal.
        # Composed from canonical identifiers and a fixed clause only --
        # never from a source-supplied label.
        summary=f"{finding.summary_subject} {finding.summary_detail}",
        affected_subject_ids=affected,
        role=finding.role,
        standing=finding.standing,
        assertion_basis=finding.assertion_basis,
        confidence_qualifier=finding.confidence_qualifier,
        supporting_path_ids=finding.path_ids[:20],
        supporting_evidence_ids=supporting[:25],
        conflicting_evidence_ids=conflicting[:25],
        conflict_note=finding.conflict_detail if conflicting else None,
        relevance=RelevanceState.CURRENT,
        exclusion_reason=finding.exclusion_reason,
    )


def _packet_id(run_id: str, job_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cf-graph-arm/{run_id}/{job_id}"))


def _freshness(state: SourceRequirementState) -> FreshnessState:
    if state is SourceRequirementState.AVAILABLE_CURRENT:
        return FreshnessState.FRESH
    if state is SourceRequirementState.AVAILABLE_STALE:
        return FreshnessState.STALE
    return FreshnessState.UNKNOWN


def _inclusion_rationale(subject_id: str, member: CohortCandidate) -> str:
    """Why this peer is in the cohort, naming the anchor where there is one.

    "shares an owning team" is not a rationale a reader can check; "shares
    owning team ``team_atlas``" is. Where the edge asserted peerhood directly
    there is no anchor to name, and the rationale says so rather than
    rendering an empty list.
    """

    parts = [
        f"{basis.value} through {', '.join(anchors)}"
        if anchors
        else f"{basis.value} recorded directly"
        for basis, anchors in member.basis_anchors
    ]
    return f"shares {'; '.join(parts)} with {subject_id}"


def _cohort_subject_kind(
    canonical_id: str, kind: GraphEntityKind
) -> InvestigationSubjectKind:
    """The emittable subject kind for a cohort entry, or a loud refusal.

    ``ORGANIZATION`` is the partition root and has no wire subject kind. It
    can only reach here through a bug, and the honest response is to fail
    rather than to drop the member — a silently missing cohort member is a
    comparison quietly narrowed.
    """

    subject_kind = entity_kind_to_subject_kind(kind)
    if subject_kind is None:
        raise ValueError(
            f"cohort entry {canonical_id} has kind {kind.value}, which is not "
            "an emittable subject kind"
        )
    return subject_kind


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
    embedder: EmbeddingBackend | None = None,
    subject_matches: Sequence[SubjectMatchFinding] | None = None,
    cohort: CohortProposal | None = None,
    drivers: Sequence[DriverFinding] | None = None,
    drivers_truncated: bool = False,
    budgets: TrialBudgets = DEFAULT_BUDGETS,
    staleness_tolerance: timedelta = DEFAULT_STALENESS_TOLERANCE,
) -> AskDevInvestigationPacket:
    """Turn one bounded traversal into the frozen investigation packet.

    ``embedder`` is the embedder the run actually used. It decides two
    things: whether a semantic match may be emitted at all
    (:func:`_check_match_mechanisms`), and what the emitted
    ``projection_version`` says — because a store embedded with one model is
    not the same projection as a store embedded with another, and a version
    that called them the same would make two incomparable runs look
    comparable.

    ``subject_matches`` carries how each candidate was found. Defaulting to
    ``None`` reproduces this revision's only mechanism — exact canonical-id
    lookup — rather than inventing matches the arm did not make.

    ``cohort`` is a proposal from :func:`~.cohort.build_cohort`. It is
    *required* under a cohort-bearing shape and *refused* under a singular
    one: the builder never invents peers, and never quietly drops peers a
    caller went to the trouble of deriving.
    """

    active_embedder = embedder or DeterministicEmbedder()
    _check_match_mechanisms(subject_matches or (), active_embedder)

    if job.comparison_shape is ComparisonShape.SINGULAR_SUBJECT:
        if cohort is not None:
            raise ValueError(
                "a cohort proposal was supplied under a singular-subject "
                "shape; silently discarding it would hide a caller/job "
                "mismatch behind a packet that looks well-formed"
            )
    elif job.comparison_shape not in _COHORT_CAPABLE_SHAPES:
        raise UnsupportedComparisonShapeError(
            f"this arm revision cannot construct a {job.comparison_shape} "
            "cohort: that shape asserts an exhaustive enumeration this arm "
            "cannot prove it made, and a partial sweep presented as complete "
            "is a stronger false claim than refusing"
        )
    elif cohort is None:
        raise UnsupportedComparisonShapeError(
            f"a {job.comparison_shape} shape was requested with no cohort "
            "proposal; emitting a one-member cohort would put a fabricated "
            "comparison into the trial's scoring table"
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
    #: canonical observation id -> the handle this run minted for it.
    #: Drivers cite handles, never raw ids: the frozen contract checks
    #: that every cited handle is in the evidence index, and an id that
    #: never became a handle is evidence the packet does not actually
    #: carry.
    handle_by_observation: dict[str, str] = {}
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
        handle_by_observation[observation.canonical_id] = handle
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

    # ---- cohort --------------------------------------------------------
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
    cohort_exclusions: tuple[CohortExclusion, ...] = ()
    cohort_dimensions: tuple[ComparisonDimension, ...] = ()
    cohort_truncated = False
    cohort_authorization_filtered = 0
    if cohort is not None:
        outside = sorted(
            (
                {member.canonical_id for member in cohort.members}
                | {exclusion.canonical_id for exclusion in cohort.exclusions}
            )
            - set(readout.authorized_entity_ids)
        )
        if outside:
            # An *exclusion* leaks as surely as a member: naming a subject in
            # order to say it was left out still tells the caller it exists.
            # This catches a cohort built against a wider grant than the
            # traversal used, which is the shape a caller gets wrong.
            raise PermissionError(
                f"the cohort names entities outside the authorized set: {outside}"
            )
        if cohort.subject_id not in committed_ids:
            raise ValueError(
                f"the cohort was built around {cohort.subject_id!r}, which "
                "this packet did not commit to as a subject; a cohort of "
                "peers of some other subject is a comparison of the wrong "
                "thing"
            )
        cohort_members += tuple(
            CohortMember(
                subject_kind=_cohort_subject_kind(member.canonical_id, member.kind),
                canonical_id=member.canonical_id,
                display_label=member.display_label,
                inclusion_basis=member.bases,
                inclusion_rationale=_inclusion_rationale(cohort.subject_id, member),
                inclusion_evidence_ids=(),
                # The membership rests on registry edges the projection
                # already holds, not on a cited observation. Saying so is
                # what the contract's XOR rule is for: an unevidenced member
                # with no stated reason is an unrelated member nobody spots.
                inclusion_evidence_classification=(
                    CohortEvidenceClassification.CANONICAL_REGISTRY_MEMBERSHIP
                ),
                relevance=RelevanceState.CURRENT,
            )
            for member in cohort.members
        )
        cohort_exclusions = tuple(
            CohortExclusion(
                subject_kind=_cohort_subject_kind(
                    exclusion.canonical_id, exclusion.kind
                ),
                canonical_id=exclusion.canonical_id,
                reason=exclusion.reason,
                rationale=exclusion.rationale,
            )
            for exclusion in cohort.exclusions
        )
        cohort_dimensions = cohort.dimensions
        cohort_truncated = cohort.truncated
        cohort_authorization_filtered = cohort.authorization_filtered_count
        if len(cohort_members) < 2 or not cohort_dimensions:
            raise IncomparableCohortError(
                f"the cohort around {cohort.subject_id} holds "
                f"{len(cohort_members)} member(s) and "
                f"{len(cohort_dimensions)} comparison dimension(s); a "
                "comparison needs two subjects and something to compare them "
                "on, and emitting this would be a comparison in name only"
            )
        cohort_budget = budgets.check_cohort(len(cohort_members))
        if not cohort_budget.within_budget:
            raise PacketTooLargeError(
                f"{cohort_budget.detail}; re-run with a tighter cohort bound "
                "rather than emitting a cohort nobody bounded"
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
    filtered_total = (
        readout.authorization_filtered_count + cohort_authorization_filtered
    )
    if filtered_total:
        limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.AUTHORIZATION_FILTERED,
                detail=(
                    f"{filtered_total} candidate results were outside the "
                    "caller's authorized scope and were removed before ranking"
                ),
            )
        )
    # Every bound that can bite, in one condition. ``drivers_truncated`` was
    # the one omitted: it reached ``DriverAnalysis.candidates_truncated`` and
    # nothing else, so a driver bound produced a packet the frozen contract
    # refused outright -- "truncated results but no TRUNCATED_TRAVERSAL
    # limitation is disclosed" -- and a caller who dropped the flag to get
    # past that exception would have presented a capped candidate set as the
    # complete one. Adding a bound here without adding it to ``gaps`` below
    # would move the same defect rather than close it, so the two lists are
    # deliberately identical and a test asserts they stay so.
    truncation_bounds = (
        readout.entities_truncated,
        readout.paths_truncated,
        readout.evidence_truncated,
        citations_capped,
        cohort_truncated,
        drivers_truncated,
    )
    if any(truncation_bounds):
        limitations.append(
            PacketLimitation(
                kind=PacketLimitationKind.TRUNCATED_TRAVERSAL,
                detail=(
                    "traversal, path citation, cohort construction or driver "
                    "candidate discovery stopped at a configured bound; the "
                    "result is partial"
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
    comparison_cohort = ComparisonCohort(
        schema_version="ask_dev_comparison_cohort.v1",
        cohort_id=f"{job.job_id}_cohort",
        comparison_shape=job.comparison_shape,
        members=cohort_members,
        exclusions=cohort_exclusions,
        supported_comparison_dimensions=cohort_dimensions,
        completeness=(
            CohortCompleteness.TRUNCATED
            if cohort_truncated
            else CohortCompleteness.COMPLETE
            if cohort_members
            else CohortCompleteness.BEST_EFFORT_UNCERTAIN
        ),
        truncation_reason=(
            TruncationReason.COHORT_BUDGET if cohort_truncated else None
        ),
        cohort_uncertainty=(
            None
            if cohort_members
            else "no subject was committed, so no cohort was constructed"
        ),
        authorization_filtered_count=cohort_authorization_filtered,
    )
    related_context = RelatedContext(
        schema_version="ask_dev_related_context.v1",
        entities=tuple(related_entities),
        paths=paths,
        authorized_entity_ids=declared_authorized,
        authorization_filtered_count=0,
        entities_truncated=readout.entities_truncated,
        paths_truncated=readout.paths_truncated,
        # The per-flag reason, never the first-wins convenience property.
        # Fixing the readout alone MOVED this defect instead of killing it:
        # both sections went on reading `truncation_reason`, so once a path
        # bound and an evidence bound fired in the same read, related_context
        # and evidence_coverage both reported whichever came first.
        truncation_reason=(
            readout.entities_truncation_reason or readout.paths_truncation_reason
        ),
    )
    # Everything a driver may say it affects: the contract checks that a
    # driver's affected subjects were declared somewhere in the packet.
    known_subject_ids = (
        {candidate.canonical_id for candidate in candidates}
        | {member.canonical_id for member in cohort_members}
        | known_entity_ids
    )
    driver_candidates = tuple(
        _driver_candidate(finding, handle_by_observation, known_subject_ids)
        for finding in drivers or ()
    )
    # Evidence entries now name the drivers that cite them. Without this the
    # index and the drivers agree only in one direction: a driver could point
    # at an entry that never claimed to support it, which is how unrelated
    # evidence passed for a driver's own.
    cited_by: dict[str, list[str]] = {}
    for candidate in driver_candidates:
        for handle in candidate.supporting_evidence_ids:
            cited_by.setdefault(handle, []).append(candidate.driver_id)
    evidence_entries = [
        entry.model_copy(
            update={
                "supports_driver_ids": tuple(
                    sorted(cited_by.get(entry.evidence.evidence_ref_id, ()))
                )
            }
        )
        if entry.evidence.evidence_ref_id in cited_by
        else entry
        for entry in evidence_entries
    ]

    _assert_support_is_closed(
        driver_candidates,
        evidence_entries,
        {path.path_id for path in paths},
        {
            finding.driver_id: frozenset({finding.subject_id, finding.cause_id})
            for finding in drivers or ()
        },
    )

    driver_analysis = DriverAnalysis(
        schema_version="ask_dev_driver_analysis.v1",
        candidates=driver_candidates,
        principal_driver_ids=tuple(
            candidate.driver_id
            for candidate in driver_candidates
            if candidate.standing is DriverStanding.PRINCIPAL_DRIVER
        ),
        candidates_truncated=drivers_truncated,
        truncation_reason=(
            # No driver-specific reason exists in the frozen vocabulary;
            # the bound that bites is the candidate NODE budget, and
            # naming a reason the contract does not have would be worse
            # than naming the one that is actually true.
            TruncationReason.NODE_BUDGET if drivers_truncated else None
        ),
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
        truncation_reason=readout.evidence_truncation_reason,
    )
    versions = InvestigationVersions(
        schema_version="ask_dev_investigation_versions.v1",
        packet_schema_version="ask_dev_investigation_packet.v1",
        query_version=QUERY_VERSION,
        ranking_version=RANKING_VERSION,
        projection_version=(
            f"{PROJECTION_VERSION.removesuffix('.v1')}."
            f"{embedder_projection_suffix(active_embedder)}.v1"
        ),
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
        # Derived, never passed in: an arm that could be told its own outcome
        # could claim one it did not earn. This revision synthesizes no
        # drivers, so the rule below always lands on UNSUPPORTED -- but it
        # lands there by evaluating the contract's own standing rule against
        # what was produced, which is what makes "the arm cannot over-claim"
        # a checked property rather than a constant.
        outcome=derive_outcome(
            driver_analysis.candidates,
            evidence_coverage.evidence_index,
            # The SAME bounds the limitation above discloses, plus the two
            # degradations that are not truncations. A bound that discloses a
            # limitation but does not weaken the outcome would let a partial
            # investigation reach a fully SUPPORTED verdict while the
            # limitation nobody reads sits beside it.
            gaps=bool(
                missing
                or source_state is SourceRequirementState.AVAILABLE_STALE
                or any(truncation_bounds)
            ),
        ),
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
