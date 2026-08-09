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
from dataclasses import replace as dataclasses_replace
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
from .readback import (
    QUERY_VERSION,
    DiscoveredObservation,
    DiscoveredPath,
    InvestigationReadout,
    PathStep,
)
from .vocabulary import (
    SOURCE_EVIDENCE_ENTITY_ATTRIBUTE,
    SOURCE_EVIDENCE_HANDLE_ATTRIBUTE,
    SOURCE_EVIDENCE_ID_ATTRIBUTE,
    GraphEntityKind,
    entity_kind_to_subject_kind,
)
from .watermark import DEFAULT_STALENESS_TOLERANCE, IndexWatermark

__all__ = [
    "ARM_ID",
    "PRODUCER_ID",
    "RANKING_VERSION",
    "EmbedderProvenanceMismatchError",
    "IncomparableCohortError",
    "JobContext",
    "PacketTooLargeError",
    "SubjectMatchFinding",
    "AuthorizationWithheldEvidenceError",
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

#: ``LineagePath.evidence_ref_ids`` is bounded at 25 by the frozen contract.
#: Held here so the emitter stays inside it rather than discovering the bound
#: as a validation error on a dense path.
_MAX_PATH_EVIDENCE = 25


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


class AuthorizationWithheldEvidenceError(PermissionError):
    """A driver rests on evidence this caller may not be shown.

    Distinct from the inconsistency raise beside it, and the distinction is
    the whole point (fix round 2, verifier N1). Evidence removed because the
    record it names is about an entity outside the caller's grant is the
    authorization filter working; evidence nothing observed is discovery and
    emission disagreeing. Collapsing the two turned a narrower grant into a
    dead packet, and a caller reading the old message would have gone looking
    for a bug that was not there.

    A ``PermissionError`` rather than a ``ValueError`` so a caller can route
    it the way it routes every other authorization refusal in this arm.
    """


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


class EmbedderProvenanceMismatchError(RuntimeError):
    """The packet would name an embedder that did not write the store.

    ``versions.projection_version`` is what a consumer reads to decide
    whether two recorded runs are comparable, and it is derived from an
    argument. Refusing rather than correcting the stamp: the two possible
    intentions — "I meant to read the other partition" and "I meant to pass
    the other embedder" — are the caller's to resolve, and silently picking
    one would make a run's provenance depend on which the builder guessed.
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


def _check_embedder_provenance(
    readout: InvestigationReadout, embedder: EmbeddingBackend
) -> None:
    """The caller's embedder must be the one that wrote these vectors.

    ``embedder`` decides what ``versions.projection_version`` says, and a
    consumer reads that stamp to decide whether two runs are comparable. It
    is an *argument*, with no connection to the embedder ``GraphArmStore``
    was constructed with at write time — so a packet could be stamped
    ``…openai_text_embedding_3_small.v1`` over a partition of BLAKE2b hashes
    purely because the caller passed a different object. Adversarial review
    reproduced it.

    Where the partition attests to an embedder, disagreement is refused here.
    Where it attests to none — an in-memory readout, which has no vectors at
    all, or a partition written before the attestation existed — there is
    nothing to disagree with, and the stamp names the only embedder anyone
    has offered. What such a readout cannot do is support a *semantic claim*:
    see :func:`_check_match_mechanisms`.
    """

    attested = readout.embedder_model_id
    if attested is not None and attested != embedder.model_id:
        raise EmbedderProvenanceMismatchError(
            f"this readout's partition records {attested!r} as having produced "
            f"its vectors, but the packet would be stamped for "
            f"{embedder.model_id!r}. A projection version naming a model that "
            "did not embed the store is how two incomparable runs come to look "
            "comparable"
        )


def _check_match_mechanisms(
    matches: Sequence[SubjectMatchFinding],
    embedder: EmbeddingBackend,
    attested_embedder: str | None,
) -> None:
    """Refuse semantic claims nothing can show the vectors support.

    Both directions of the caller's claim are checked, because a producer can
    get it wrong from either end: a mechanism that needs semantics under a
    non-semantic embedder, and a signal that is *inherently* semantic however
    it claims to have been produced.

    Neither direction is enough on its own, which is the finding this guard
    was widened for. ``embedder.semantic`` is the passed object's self-report,
    and the question that matters is whether the vectors this readout was
    searched over were produced by something semantic. So a semantic claim
    needs BOTH: an embedder that says it carries semantics, and a partition
    that attests the vectors came from it. A readout with no attestation
    supports no semantic claim whatever the caller passes — including a
    perfectly usable :class:`~.backend.CloudEmbedder`, because "this embedder
    could have produced semantic vectors" is not "these vectors are
    semantic".
    """

    if embedder.semantic and attested_embedder is not None:
        return
    offending = [
        (match.canonical_id, match.signal.value, match.mechanism.value)
        for match in matches
        if match.mechanism in SEMANTIC_MECHANISMS
        or match.signal in _INHERENTLY_SEMANTIC_SIGNALS
    ]
    if offending:
        cause = (
            f"the active embedder ({embedder.model_id}) carries no semantics"
            if not embedder.semantic
            else (
                "nothing attests that this readout's vectors were produced by "
                f"{embedder.model_id}; the partition records no embedder, so "
                "the claim rests on the caller's word"
            )
        )
        raise UnsupportedMatchMechanismError(
            f"these subject matches claim semantics {cause}: {offending}. "
            "Similarity over non-semantic vectors is a confident arbitrary "
            "ordering, and a packet presenting it as a match would score as a "
            "retrieval capability this arm does not have"
        )


def _assert_support_is_closed(
    drivers: Sequence[DriverCandidate],
    evidence: Sequence[InvestigationEvidenceEntry],
    paths: Sequence[LineagePath],
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
      where a caller-assembled driver arrives;
    * CHAOS-3630: every path the driver cites must itself close to evidence.
      Until that ticket, ``LineagePath.evidence_ref_ids`` was the literal
      ``()``, so relationship-closure could not be checked at all -- drivers
      closed to evidence and the relationships they rested on did not, which
      is half of the provenance claim missing while the other half was
      enforced. A path with no evidence explains the mechanism only in the
      sense that the arm asserts it.

    Paths with no evidence are perfectly legitimate in general -- roughly half
    the corpus's edges carry no evidence slugs -- which is why this is scoped
    to paths an ASSERTED driver leans on. A path nobody builds a judgment from
    owes nobody a citation.
    """

    by_handle = {entry.evidence.evidence_ref_id: entry for entry in evidence}
    known_paths = {path.path_id for path in paths}
    evidence_free_paths = {path.path_id for path in paths if not path.evidence_ref_ids}
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
        elif all(item in evidence_free_paths for item in driver.supporting_path_ids):
            problems.append(
                "cites only lineage paths that close to no evidence, so the "
                "mechanism it names rests on the arm's assertion alone"
            )
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
    filtered_ids: Container[str],
    path_relevance: Mapping[str, RelevanceState],
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
            # CHAOS-3627 fix round 2, verifier N1. Two very different things
            # were reaching this one raise, and only one of them is an
            # internal inconsistency.
            #
            # An id the AUTHORIZATION FILTER removed -- because the record it
            # names is about an entity outside this caller's grant -- is the
            # arm working correctly on a partial grant. Raising there turned a
            # narrower grant into a dead packet: discovery legitimately saw a
            # record that emission legitimately may not present.
            #
            # An id NOTHING observed is still the inconsistency this guard
            # exists for, and still raises. The distinction is drawn from the
            # drop set the evidence pass already recorded; nothing new is
            # reconciled and no discovery-side change is implied.
            withheld = sorted(item for item in missing if item in filtered_ids)
            unobserved = sorted(item for item in missing if item not in filtered_ids)
            if unobserved:
                raise ValueError(
                    f"driver {finding.driver_id} cites {role} evidence this "
                    f"packet never indexed: {unobserved}. Discovery and "
                    "emission disagree about what the run observed"
                )
            raise AuthorizationWithheldEvidenceError(
                f"driver {finding.driver_id} rests on {role} evidence this "
                f"caller may not be shown: {withheld}. The records are about "
                "entities outside the grant, so the driver cannot be asserted "
                "to this caller -- which is the authorization filter working, "
                "not an inconsistency"
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
        # A driver is as current as the best route that explains it. Derived
        # rather than declared (CHAOS-3629): a driver whose every supporting
        # path closed months ago is a historical cause, and the contract's
        # own CURRENTLY_RELEVANT_STATES is what stops such a thing being
        # reported as the principal current driver.
        relevance=_strongest(
            [
                path_relevance[path_id]
                for path_id in finding.path_ids
                if path_id in path_relevance
            ]
        ),
        exclusion_reason=finding.exclusion_reason,
    )


@dataclass(slots=True)
class _EvidenceGroup:
    """One source record, and everything this packet projected it as.

    ``record`` is built from the observation the handle was issued FOR, so
    ``record.entity_id`` is the entity the *source record* is about. An
    observation that merely cites the record contributes its own subjects to
    :attr:`supports` and nothing else: what a number is about does not change
    what the record evidencing it is about, and the corpus oracle reads the
    two through different fields (``authorization.py:190`` against
    ``:276-277``).
    """

    observation_id: str
    #: The canonical id of the RECORD this group's handle was issued for.
    #: What the join compares against, so a citation that names a different
    #: record cannot union its subjects in (fix round 2).
    source_id: str
    record: EvidenceRecord
    source_class: SourceClass
    supports: list[str]

    def also_supports(self, subjects: Sequence[str]) -> None:
        """Union in another projection's subjects, order preserved.

        Append-if-unseen rather than ``sorted(set(...))``: the source record's
        own subjects stay first, so the entry still reads as "this record,
        which these other things also rest on".
        """

        for subject in subjects:
            if subject not in self.supports:
                self.supports.append(subject)


def _describes_its_own_evidence(observation: DiscoveredObservation) -> bool:
    """Whether this observation IS the record its handle was issued for.

    True for a source record, and for anything with no source-issued handle
    at all -- an observation the platform's signer will mint for describes
    itself by definition. False only for an observation that names a
    *different* record as the source of its evidence, which is how a
    canonical measurement points at the record the world says evidences it.
    """

    handle = observation.attributes.get(SOURCE_EVIDENCE_HANDLE_ATTRIBUTE)
    if handle is None:
        return True
    return (
        observation.attributes.get(SOURCE_EVIDENCE_ID_ATTRIBUTE)
        == observation.canonical_id
    )


def _evidence_record(
    observation: DiscoveredObservation,
    supports: Sequence[str],
    source_state: SourceRequirementState,
) -> EvidenceRecord:
    """The evidence ref for one observation.

    ``entity_id`` is **the entity the evidence is about**, never the
    observation's own identifier. That field is entity vocabulary: the frozen
    contract checks it against the declared authorized set
    (``packet.py:1431``) and the CHAOS-3616 oracle reads it as an entity
    sighting (``authorization.py:190``), so an observation slug there is a
    fabricated entity and forces the declared set to be widened to hide it.

    Where the source declares what its record is about
    (:data:`~.vocabulary.SOURCE_EVIDENCE_ENTITY_ATTRIBUTE`) that is the
    answer, full stop. CHAOS-3627's fix round: the previous rule -- the first
    subject of whichever observation the traversal happened to reach -- was
    right whenever the record itself was reached and wrong whenever only a
    citing observation was, which both reviewers measured at roughly a third
    of packets. A citing observation's subject is what the CITATION is about;
    it does not change what the RECORD is about.

    Only where no source declares it does the arm fall back to the first
    subject in the source's own declared order, with the full set carried in
    ``supports_entity_ids``. That fallback exists for sources with no record
    model at all, and the world's own ``WorldEvidence`` is the shape it
    approximates: exactly one ``entity_id`` and nothing else.
    """

    declared = observation.attributes.get(SOURCE_EVIDENCE_ENTITY_ATTRIBUTE)
    return EvidenceRecord(
        source_system="context_fabric_graph_arm",
        source_version=_SOURCE_CONTRACT_VERSION,
        entity_type=observation.kind.value,
        entity_id=declared if declared is not None else supports[0],
        display_label=observation.title,
        observed_at=observation.observed_at,
        freshness=_freshness(source_state),
        provenance="structured record projected into the trial graph",
        confidence=1.0,
        repository_ids=observation.repository_ids,
    )


def _mint_handle(
    signer: EvidenceReferenceSigner,
    org_id: str,
    observation: DiscoveredObservation,
    record: EvidenceRecord,
) -> str:
    """A handle for a record whose source issued none.

    Minted through the platform's own ``EvidenceReferenceSigner`` -- not a
    parallel scheme -- but over a record identified by its **canonical id**
    rather than by the entity it is about.

    That substitution is the arm-side fix for CHAOS-3633 and it is deliberate.
    ``EvidenceReferenceSigner._payload`` identifies a record by ``(org,
    source_system, source_version, entity_type, entity_id, repositories)``,
    and ``entity_id`` on the wire is the entity the evidence is *about*. Two
    distinct records of one kind about one entity therefore mint the SAME
    handle, and since the frozen contract refuses a repeated handle in the
    index, the second one killed the entire packet -- measured on a
    legitimate handle-less world (the arm's own fixtures hold exactly such a
    pair: ``dec_auth_1`` superseded by ``dec_auth_2``). A denial of service on
    valid input, manufactured by the arm's own mint.

    The platform fix belongs in ``evidence_service`` and is tracked as
    CHAOS-3633; this arm must not reach into it. So the arm signs over the
    record's identity, which is what the payload needed all along.

    **The consequence, stated rather than buried**: the emitted evidence ref
    no longer round-trips through ``signer.verify``, because the emitted
    ``entity_id`` is the entity while the signed one is the record. Verifying
    a minted handle means re-deriving it through this function -- which is
    what ``test_chaos_3617_packet_contract`` now does. The property that
    matters is unchanged: the handle comes from the evidence service's own
    signing function, over this organization's key, and no scheme this arm
    invented.
    """

    return signer.issue(
        org_id, dataclasses_replace(record, entity_id=observation.canonical_id)
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


#: Relevance from best to worst, for the two directions this module reduces
#: over. ``UNKNOWN`` is deliberately absent: it is not a rung on this ladder
#: but the answer when there is no ladder to stand on, and folding it in would
#: make "we could not tell" comparable with "we could, and it is historical".
_RELEVANCE_ORDER: tuple[RelevanceState, ...] = (
    RelevanceState.CURRENT,
    RelevanceState.RECENTLY_CURRENT,
    RelevanceState.HISTORICAL_ONLY,
)


def _step_relevance(
    step: PathStep, as_of: datetime, window_start: datetime
) -> RelevanceState:
    """What the readout's own validity data says about one traversed edge.

    CHAOS-3629. This was ``RelevanceState.CURRENT``, a literal, at all eight
    construction sites, so the corpus's planted expired dependency was
    reported as a live cause and CHAOS-3619's ``current_relevance`` dimension
    was scoring a constant.

    The four cases mirror ``world.WorldRelationship.relevance_at`` member for
    member, because the corpus is the spec for this vocabulary and a second
    definition of "current" is how two of them drift apart:

    * not yet observed at ``as_of`` -> ``UNKNOWN``. The arm cannot describe
      the relevance of something it had not learned;
    * in force at ``as_of`` -> ``CURRENT``;
    * closed, but not before the investigation window opened ->
      ``RECENTLY_CURRENT``. It stopped being true recently enough to matter
      to the question being asked;
    * closed before the window -> ``HISTORICAL_ONLY``.

    An edge with no interval at all reads ``CURRENT`` through
    ``is_current_at``, and that is deliberate rather than an oversight — see
    that method, and the corpus's ``true_at``, which agree: most providers
    assert no interval, and reading silence as "expired" would erase the
    majority of a real graph.
    """

    if step.observed_at > as_of:
        return RelevanceState.UNKNOWN
    if step.is_current_at(as_of):
        return RelevanceState.CURRENT
    if step.valid_to is not None and step.valid_to >= window_start:
        return RelevanceState.RECENTLY_CURRENT
    return RelevanceState.HISTORICAL_ONLY


def _weakest(states: Sequence[RelevanceState]) -> RelevanceState:
    """A chain is only as current as its weakest link.

    Used for a path over its hops: a route through an edge that closed two
    months ago is not a live route, however current the rest of it is. Any
    ``UNKNOWN`` makes the whole thing ``UNKNOWN`` — an unknown link means the
    chain's status is unknown, not that it is merely old.
    """

    if not states:
        return RelevanceState.UNKNOWN
    if RelevanceState.UNKNOWN in states:
        return RelevanceState.UNKNOWN
    return max(states, key=_RELEVANCE_ORDER.index)


def _strongest(states: Sequence[RelevanceState]) -> RelevanceState:
    """One live route is enough.

    Used for an entity over the paths that reach it, and for a driver over the
    paths it cites. The opposite reduction to :func:`_weakest`, and the
    difference is not stylistic: an entity connected by both a live and an
    expired route is currently related, while a route through both a live and
    an expired edge is not a live route.
    """

    known = [state for state in states if state is not RelevanceState.UNKNOWN]
    if not known:
        return RelevanceState.UNKNOWN
    return min(known, key=_RELEVANCE_ORDER.index)


def _lineage_path(
    path: DiscoveredPath,
    source_state: SourceRequirementState,
    as_of: datetime,
    window_start: datetime,
    handle_by_observation: Mapping[str, str],
) -> LineagePath:
    hops: list[LineageHop] = []
    hop_relevance: list[RelevanceState] = []
    evidence: list[str] = []
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
                relevance=_step_relevance(step, as_of, window_start),
            )
        )
        hop_relevance.append(hops[-1].relevance)
        # CHAOS-3630. The ids were on the step the whole time; this field was
        # the literal ``()``, so no relationship in any packet ever closed to
        # evidence while every driver did.
        #
        # Only handles this packet actually carries are cited. An observation
        # the traversal reached but did not index -- withdrawn (CHAOS-3628),
        # or attached to nothing it returned -- has no handle here, and citing
        # a reference the index cannot resolve is the fault
        # ``_assert_support_is_closed`` exists to prevent for drivers.
        for observation_id in step.observation_ids:
            handle = handle_by_observation.get(observation_id)
            if handle is not None and handle not in evidence:
                evidence.append(handle)
    return LineagePath(
        path_id=path.path_id,
        origin_entity_id=path.origin_canonical_id,
        terminal_entity_id=path.terminal_canonical_id,
        hops=tuple(hops),
        inclusion_reason=(
            "reached by bounded authorized traversal from the committed "
            "subject over the frozen relationship allowlist"
        ),
        relevance=_weakest(hop_relevance),
        evidence_ref_ids=tuple(evidence[:_MAX_PATH_EVIDENCE]),
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
    _check_embedder_provenance(readout, active_embedder)
    _check_match_mechanisms(
        subject_matches or (), active_embedder, readout.embedder_model_id
    )

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
    #
    # The emitted paths are built at the END of this function, not here.
    # CHAOS-3630 made a path's evidence references real, and they are handles
    # -- so a path cannot be emitted until the evidence index exists. What is
    # computed here is only what the rest of the assembly needs: which paths
    # touch which entity, and how each path's own relevance reads.
    path_relevance = {
        path.path_id: _weakest(
            [
                _step_relevance(step, job.window_end, job.window_start)
                for step in path.steps
            ]
        )
        for path in readout.paths
    }
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
                # One live route is enough: an entity reached by both a
                # current and an expired path IS currently related. The
                # opposite reduction to a path's own, and the difference is
                # the point -- see ``_strongest``.
                relevance=_strongest([path_relevance[path_id] for path_id in ordered]),
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
                # A candidate the traversal reached takes the relevance of
                # the best route to it; a seed no path touched has none to
                # take, and UNKNOWN is the contract's word for that.
                relevance=_strongest(
                    [path_relevance[path_id] for path_id in touched.get(seed, ())]
                ),
            )
        )
        if commit:
            committed_ids.append(entity.canonical_id)

    # ---- authorization envelope ---------------------------------------
    #
    # ``related_context.authorized_entity_ids`` is ENTITY vocabulary. The
    # frozen contract types it so (``packet.py:789``, ``:852``) and the
    # CHAOS-3616 oracle compares it against the principal's entity grant
    # (``authorization.py:254-255``), which makes an observation id in that
    # field a false authorization claim rather than a harmless superset --
    # and because ``validate_every_entity_is_authorized`` reads the SAME
    # field, every id added to it is one more thing the contract's own leak
    # check stops catching.
    #
    # This builder used to declare entity ids plus observation ids plus
    # measurement keys, to get the observation slug it was putting in
    # ``evidence.entity_id`` past that validator. CHAOS-3627 fixed the
    # vocabulary at the source, so the widening is gone and the two
    # invariants it was carrying are enforced here instead, as the arm's own
    # checks: every hop endpoint is an authorized entity, and every
    # observation that reaches the evidence index is about authorized
    # entities only.
    authorized_entity_ids = tuple(sorted(readout.authorized_entity_ids))
    authorized_lookup = set(authorized_entity_ids)
    for path in readout.paths:
        for step in path.steps:
            for endpoint in (step.from_canonical_id, step.to_canonical_id):
                if endpoint not in authorized_entity_ids:
                    raise PermissionError(
                        f"path {path.path_id} traverses {endpoint!r}, which is "
                        "not in the authorized entity set"
                    )
    declared_authorized = authorized_entity_ids

    # ---- evidence ------------------------------------------------------
    #
    # One entry per SOURCE RECORD, not one per observation.
    #
    # Where a source issued a handle the arm cites the handle it was issued
    # (:data:`~.vocabulary.SOURCE_EVIDENCE_HANDLE_ATTRIBUTE`); re-minting one
    # changes the identity of the evidence being presented, so a consumer --
    # and the corpus oracle -- can no longer join it to the record it came
    # from. The signer stays for sources that issue no handle of their own.
    #
    # Two observations may therefore carry the same handle: a canonical
    # measurement and the record the source says evidences it are one piece
    # of evidence projected twice. They become one entry, described by
    # whichever of them the handle was issued FOR, with the other's subjects
    # unioned into its support. The frozen contract also refuses a repeated
    # handle in the index (``packet.py:1241-1243``), but that is not why the
    # merge is right: the source's own identity is.
    evidence_groups: dict[str, _EvidenceGroup] = {}
    #: canonical observation id -> the handle this packet cites for it.
    #: Drivers cite handles, never raw ids: the frozen contract checks
    #: that every cited handle is in the evidence index, and an id that
    #: never became a handle is evidence the packet does not actually
    #: carry.
    handle_by_observation: dict[str, str] = {}
    candidate_ids = {item.canonical_id for item in candidates}

    indexable: list[tuple[DiscoveredObservation, tuple[str, ...]]] = []
    #: The source RECORDS whose entity is outside the caller's authorized
    #: set, by record id -- not the observations that cite them.
    #:
    #: A set, and the unit is the point (fix round 2, codex medium 2). An
    #: evidence entry represents a record, so counting per dropped
    #: OBSERVATION reported 2 for two citations of one unauthorized record
    #: while exactly one entry went missing. A disclosure whose unit differs
    #: from the thing it is disclosing about is a number a reader cannot use.
    unattributable: set[str] = set()
    #: The same drops, keyed by OBSERVATION id rather than by record id.
    #:
    #: Two sets because the two consumers ask different questions, and round 2
    #: shipped one set answering both -- which is the defect both reviewers
    #: blocked on. The COUNT discloses records, because an evidence entry
    #: represents a record. The DRIVER check resolves observation ids, because
    #: a driver cites observations. A single set silently answered the driver
    #: check in the wrong unit, so every CITING observation (source_evidence_id
    #: != canonical_id) fell through to "nothing observed this" and raised the
    #: inconsistency error where the authorization refusal was due.
    withheld_observation_ids: set[str] = set()
    for observation in readout.observations:
        # Named distinctly from the cohort guard's ``outside``: the
        # guard-injection harness anchors on source lines, and a second
        # ``if outside:`` in this module makes that anchor ambiguous, which
        # the harness reports as INVALID rather than silently mutating both.
        unauthorized_subjects = sorted(
            subject
            for subject in observation.subject_canonical_ids
            if subject not in authorized_lookup
        )
        if unauthorized_subjects:
            # Refuse, do not narrow. The intersection below would simply drop
            # these subjects, so a reader bug -- or a second reader -- could
            # hand this builder unauthorized material and get back a packet
            # that looked clean. This is the evidence twin of the hop check.
            raise PermissionError(
                f"observation {observation.canonical_id!r} is about "
                f"{unauthorized_subjects}, which is not in the authorized "
                "entity set"
            )
        supports = tuple(
            subject
            for subject in observation.subject_canonical_ids
            if subject in known_entity_ids
        )
        if not supports:
            # Unattached evidence displaces lineage. The contract refuses to
            # index it and so does this builder.
            continue
        # The entry will be described by the entity the SOURCE RECORD is
        # about, so that entity has to be one this caller may be told about.
        # Naming it otherwise would put an unauthorized id in a field the
        # contract checks against the declared set and the corpus oracle
        # reads as an entity sighting. Dropping the entry is the only honest
        # option left: the alternatives are naming the wrong entity, which is
        # the defect this fix round exists to remove, or naming a right one
        # the caller may not see.
        declared_entity = observation.attributes.get(SOURCE_EVIDENCE_ENTITY_ATTRIBUTE)
        if declared_entity is not None and declared_entity not in authorized_lookup:
            unattributable.add(
                observation.attributes.get(
                    SOURCE_EVIDENCE_ID_ATTRIBUTE, observation.canonical_id
                )
            )
            withheld_observation_ids.add(observation.canonical_id)
            continue
        indexable.append((observation, supports))

    # Two passes, because an observation is sorted by its own canonical id
    # and may be read before the record it cites. The first pass builds the
    # entries the source issued handles FOR; the second attaches everything
    # that merely cites one of them.
    for observation, supports in indexable:
        if not _describes_its_own_evidence(observation):
            continue
        record = _evidence_record(observation, supports, source_state)
        handle = observation.attributes.get(SOURCE_EVIDENCE_HANDLE_ATTRIBUTE)
        if handle is None:
            handle = _mint_handle(signer, readout.org_id, observation, record)
        if handle in evidence_groups:
            # Only reachable for a genuinely repeated SOURCE-issued handle
            # now: the arm's own mint discriminates by record identity (see
            # ``_mint_handle``). Refusing beats merging here because two
            # records the SOURCE gave one identity cannot be told apart by
            # anything downstream, and presenting them as one piece of
            # evidence would lose a record silently.
            raise ValueError(
                f"observation {observation.canonical_id!r} carries a source "
                f"evidence handle already issued for "
                f"{evidence_groups[handle].observation_id!r}. One handle names "
                "one record; merging two records under it would present them "
                "to a consumer as the same evidence. The platform mint cannot "
                "separate two same-kind records about one entity either -- "
                "that is CHAOS-3633, and this arm works around it rather than "
                "reproducing it"
            )
        evidence_groups[handle] = _EvidenceGroup(
            observation_id=observation.canonical_id,
            source_id=observation.attributes.get(
                SOURCE_EVIDENCE_ID_ATTRIBUTE, observation.canonical_id
            ),
            record=record,
            source_class=observation.source_class,
            supports=list(supports),
        )
        handle_by_observation[observation.canonical_id] = handle

    for observation, supports in indexable:
        if _describes_its_own_evidence(observation):
            continue
        handle = observation.attributes[SOURCE_EVIDENCE_HANDLE_ATTRIBUTE]
        group = evidence_groups.get(handle)
        if group is not None and (
            observation.attributes.get(SOURCE_EVIDENCE_ID_ATTRIBUTE) != group.source_id
        ):
            # CHAOS-3627 fix round 2, codex BLOCKING = verifier N2. The join
            # was keyed on the HANDLE alone, so any observation carrying an
            # existing handle joined that record's group and unioned its
            # subjects in -- whatever record it actually named. The bound
            # "extra subjects arrive only from observations naming THIS
            # record" was therefore pinned by what the fixtures happen to do,
            # not enforced where the union happens.
            #
            # Refused rather than dropped, matching the pairing posture
            # everywhere else in this file: a source that issued one handle
            # for two different records is telling the arm something
            # contradictory, and quietly believing half of it is how the
            # contradiction reaches a consumer.
            raise ValueError(
                f"observation {observation.canonical_id!r} cites handle "
                f"{handle} but names record "
                f"{observation.attributes.get(SOURCE_EVIDENCE_ID_ATTRIBUTE)!r}; "
                f"that handle was issued for {group.source_id!r}. One handle "
                "names one record, and merging a citation of a different "
                "record into it would widen that record's support with "
                "subjects it has nothing to do with"
            )
        if group is None:
            # The record this observation cites was not reached by this
            # traversal -- it can be about an entity no path arrived at. The
            # handle is still the one the source issued for the number being
            # cited, so it is still what this packet must present; what falls
            # back is only which entity the entry is about, to the subject the
            # citing observation does name. Seeding both ends of that link is
            # what ``test_chaos_3627_arm_vocabulary`` calls input symmetry.
            evidence_groups[handle] = _EvidenceGroup(
                observation_id=observation.canonical_id,
                source_id=observation.attributes.get(
                    SOURCE_EVIDENCE_ID_ATTRIBUTE, observation.canonical_id
                ),
                record=_evidence_record(observation, supports, source_state),
                source_class=observation.source_class,
                supports=list(supports),
            )
        else:
            group.also_supports(supports)
        handle_by_observation[observation.canonical_id] = handle

    evidence_entries: list[InvestigationEvidenceEntry] = []
    for handle, group in evidence_groups.items():
        entry_supports = tuple(group.supports)
        evidence_entries.append(
            InvestigationEvidenceEntry(
                evidence={
                    "schema_version": "dev_evidence_ref.v1",
                    "evidence_ref_id": handle,
                    "source_system": group.record.source_system,
                    "source_version": group.record.source_version,
                    "entity_type": group.record.entity_type,
                    "entity_id": group.record.entity_id,
                    "display_label": group.record.display_label,
                    "link": None,
                    "observed_at": group.record.observed_at,
                    "freshness": group.record.freshness.value,
                    "provenance": group.record.provenance,
                    "confidence": group.record.confidence,
                    "citation_text": None,
                    "repository_ids": list(group.record.repository_ids),
                    "valid_entity_ids": list(entry_supports),
                    "flags": {},
                },
                source_class=group.source_class,
                supports_path_ids=(),
                supports_entity_ids=entry_supports,
                supports_driver_ids=(),
                supports_subject_ids=tuple(
                    subject for subject in entry_supports if subject in candidate_ids
                ),
                # An observation carries no validity interval, so the arm has
                # no basis for a relevance claim about it. UNKNOWN is the
                # contract's word for exactly that; CURRENT would be an
                # assertion that the record is still pertinent, which nothing
                # in the readout supports. This is the honest answer until
                # observations carry validity, and it is deliberately not
                # derived from the entities the evidence supports -- what a
                # record is ABOUT does not date the record.
                relevance=RelevanceState.UNKNOWN,
            )
        )

    # ---- lineage paths -------------------------------------------------
    #
    # Emitted here, after the evidence index exists, because CHAOS-3630 made
    # a path's references real handles rather than the literal ``()`` and a
    # handle only exists once its record has been indexed. The ordering is
    # load-bearing, not incidental: built earlier, a path could only ever
    # cite nothing.
    paths = tuple(
        _lineage_path(
            path,
            source_state,
            job.window_end,
            job.window_start,
            handle_by_observation,
        )
        for path in readout.paths
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
            # The committed subject's own relevance, which the traversal
            # derived; the same member the subject candidate carries, because
            # this IS that subject and two different answers about one entity
            # in one packet would be the packet contradicting itself.
            relevance=_strongest(
                [
                    path_relevance[path_id]
                    for path_id in touched.get(candidate.canonical_id, ())
                ]
            ),
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
                # A peer is found through registry edges the cohort builder
                # walks, and ``CohortCandidate`` carries no validity interval
                # for them, so this packet has no basis for a relevance claim
                # about the membership. UNKNOWN says that. Reaching for the
                # peer's own path relevance would answer a different question
                # -- how current is the ENTITY, not how current is its
                # membership -- and quietly present one as the other.
                relevance=RelevanceState.UNKNOWN,
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
        readout.authorization_filtered_count
        + cohort_authorization_filtered
        + len(unattributable)
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
        _driver_candidate(
            finding,
            handle_by_observation,
            known_subject_ids,
            withheld_observation_ids,
            path_relevance,
        )
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
        paths,
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
        authorization_filtered_count=len(unattributable),
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
