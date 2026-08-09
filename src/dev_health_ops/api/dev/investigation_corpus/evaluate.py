"""Score one packet against one case's oracle, per evaluation dimension.

Three properties of this layer are deliberate.

**The canonical validator always runs.** ``contracts/ask-dev-investigation/v1/
manifest.json`` records ``validation_policy.schema_only_validation_is_
sufficient: false`` — the generated JSON Schemas are structural, and a packet
that passes them has not been checked for authorization scope, evidence
closure, driver standing, family obligations or historical comparability.
:func:`evaluate_case` therefore validates through
``AskDevInvestigationPacket.model_validate`` and refuses to score anything
that does not survive it. A packet that fails the contract is reported as
``CONTRACT_INVALID``, not as a pile of dimension failures: mixing the two
would let a malformed packet look like a subtly wrong one.

**Every dimension resolves to one of three verdicts, never to silence.**
``PASS``, ``FAIL`` or ``NOT_APPLICABLE`` — and ``NOT_APPLICABLE`` always
carries a reason. A dimension a case does not exercise must render as an
explicit blank in the coverage matrix, never as an implicit pass. That is the
same rule the case registry applies to skipped cases, at the level of cells
rather than rows.

**No aggregate score.** :class:`CaseEvaluation` has no total, no percentage
and no weighted composite. The frozen scoring registry types
``aggregate_prohibited`` as ``Literal[True]`` on every dimension; producing a
single number here would route around that in the one place it would
actually be read.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from ..investigation_contract import (
    SCORING_DIMENSION_REGISTRY,
    AskDevInvestigationPacket,
    ScoringDimensionID,
)
from ..investigation_contract.vocabulary import (
    ConfidenceQualifier,
    DriverStanding,
    RelevanceState,
    SubjectCommitmentState,
)
from . import world
from .authorization import AuthorizationAudit, audit_authorization
from .cases import CASE_REGISTRY, CorpusCase
from .oracles import CaseOracle, PathExpectation, oracle_for

__all__ = [
    "CaseEvaluation",
    "DimensionResult",
    "Verdict",
    "evaluate_case",
    "evaluate_payload",
]


class Verdict(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    #: The case does not exercise this dimension. Always accompanied by a
    #: reason; an unexplained N/A is indistinguishable from an untested one.
    NOT_APPLICABLE = "not_applicable"
    #: The packet did not survive the canonical validator, so nothing about
    #: its content was scored.
    CONTRACT_INVALID = "contract_invalid"


@dataclass(frozen=True)
class DimensionResult:
    dimension_id: ScoringDimensionID
    verdict: Verdict
    detail: str


@dataclass(frozen=True)
class CaseEvaluation:
    """One case's result. Per dimension, never aggregated."""

    case_id: str
    contract_valid: bool
    contract_error: str
    authorization: AuthorizationAudit | None
    results: tuple[DimensionResult, ...]

    def by_dimension(self) -> Mapping[ScoringDimensionID, DimensionResult]:
        return {result.dimension_id: result for result in self.results}

    def failures(self) -> tuple[DimensionResult, ...]:
        return tuple(
            result for result in self.results if result.verdict is Verdict.FAIL
        )


_D = ScoringDimensionID


# --------------------------------------------------------------------------
# Helpers over the packet
# --------------------------------------------------------------------------


def _committed_ids(packet: AskDevInvestigationPacket) -> set[str]:
    return set(packet.subject_discovery.committed_subject_ids)


def _candidate_ids_in_rank_order(packet: AskDevInvestigationPacket) -> list[str]:
    return [
        candidate.canonical_id
        for candidate in sorted(
            packet.subject_discovery.candidates, key=lambda item: item.rank
        )
    ]


def _offered_candidate_ids(packet: AskDevInvestigationPacket) -> set[str]:
    """Everything the packet puts in front of a user as a possible subject."""

    offered = {
        candidate.canonical_id for candidate in packet.subject_discovery.candidates
    }
    for mention in packet.subject_discovery.unresolved_mentions:
        by_candidate = {
            candidate.candidate_id: candidate.canonical_id
            for candidate in packet.subject_discovery.candidates
        }
        for candidate_id in mention.candidate_ids:
            resolved = by_candidate.get(candidate_id)
            if resolved is not None:
                offered.add(resolved)
    return offered


def _packet_paths(packet: AskDevInvestigationPacket) -> set[str]:
    """Every hop, as a canonical ``source -[rel]-> target`` key.

    Reverse-direction hops are normalized to their canonical reading so a
    path expectation matches however the arm chose to traverse it. Direction
    correctness is scored separately and deliberately: normalizing here and
    checking there keeps 'did you find the edge' and 'did you point it the
    right way' from collapsing into one number.
    """

    keys: set[str] = set()
    for path in packet.related_context.paths:
        for hop in path.hops:
            if hop.direction.value == "forward":
                source, target = hop.source_entity_id, hop.target_entity_id
            else:
                source, target = hop.target_entity_id, hop.source_entity_id
            keys.add(f"{source}-[{hop.relationship}]->{target}")
    return keys


def _reversed_hops(packet: AskDevInvestigationPacket) -> list[str]:
    """Hops whose canonical reading contradicts the world's own edge."""

    world_edges = {
        f"{edge.source_entity_id}-[{edge.relationship}]->{edge.target_entity_id}"
        for edge in world.WORLD_RELATIONSHIPS
        if not edge.is_false_claim
    }
    offenders: list[str] = []
    for path in packet.related_context.paths:
        for hop in path.hops:
            if hop.direction.value == "forward":
                source, target = hop.source_entity_id, hop.target_entity_id
            else:
                source, target = hop.target_entity_id, hop.source_entity_id
            key = f"{source}-[{hop.relationship}]->{target}"
            flipped = f"{target}-[{hop.relationship}]->{source}"
            if key not in world_edges and flipped in world_edges:
                offenders.append(key)
    return offenders


def _cited_handles(packet: AskDevInvestigationPacket) -> set[str]:
    handles = {
        entry.evidence.evidence_ref_id
        for entry in packet.evidence_coverage.evidence_index
    }
    for driver in packet.driver_analysis.candidates:
        handles.update(driver.supporting_evidence_ids)
    for path in packet.related_context.paths:
        handles.update(path.evidence_ref_ids)
    for member in packet.comparison_cohort.members:
        handles.update(member.inclusion_evidence_ids)
    for candidate in packet.subject_discovery.candidates:
        for signal in candidate.match_signals:
            handles.update(signal.evidence_ref_ids)
    return handles


def _asserted_driver_ids(packet: AskDevInvestigationPacket) -> set[str]:
    return {
        driver.driver_id
        for driver in packet.driver_analysis.candidates
        if driver.standing
        in {DriverStanding.PRINCIPAL_DRIVER, DriverStanding.CONTRIBUTING_DRIVER}
    }


def _path_present(packet: AskDevInvestigationPacket, expected: PathExpectation) -> bool:
    return expected.key in _packet_paths(packet)


# --------------------------------------------------------------------------
# Dimension scorers
# --------------------------------------------------------------------------

_Scorer = Callable[
    [CorpusCase, CaseOracle, AskDevInvestigationPacket, AuthorizationAudit],
    DimensionResult,
]


def _result(
    dimension_id: ScoringDimensionID, verdict: Verdict, detail: str
) -> DimensionResult:
    return DimensionResult(dimension_id, verdict, detail)


def _na(dimension_id: ScoringDimensionID, reason: str) -> DimensionResult:
    return _result(dimension_id, Verdict.NOT_APPLICABLE, reason)


def _score_subject_top_1(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if oracle.committed_subject_id is None:
        return _na(
            _D.SUBJECT_TOP_1,
            "the case has no single correct subject; it expects clarification "
            "or no match",
        )
    ranked = _candidate_ids_in_rank_order(packet)
    if not ranked:
        return _result(_D.SUBJECT_TOP_1, Verdict.FAIL, "no candidate was offered")
    ok = ranked[0] == oracle.committed_subject_id
    return _result(
        _D.SUBJECT_TOP_1,
        Verdict.PASS if ok else Verdict.FAIL,
        f"rank-1 was {ranked[0]}; expected {oracle.committed_subject_id}",
    )


def _score_subject_top_3(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if oracle.committed_subject_id is None:
        if not oracle.permitted_candidate_ids:
            return _na(
                _D.SUBJECT_TOP_3,
                "the case expects no candidate at all (no-match or refusal)",
            )
        ranked = set(_candidate_ids_in_rank_order(packet)[:3])
        ok = bool(ranked & set(oracle.permitted_candidate_ids))
        return _result(
            _D.SUBJECT_TOP_3,
            Verdict.PASS if ok else Verdict.FAIL,
            f"top-3 {sorted(ranked)} against permitted "
            f"{sorted(oracle.permitted_candidate_ids)}",
        )
    ranked = set(_candidate_ids_in_rank_order(packet)[:3])
    ok = oracle.committed_subject_id in ranked
    return _result(
        _D.SUBJECT_TOP_3,
        Verdict.PASS if ok else Verdict.FAIL,
        f"top-3 {sorted(ranked)}; expected {oracle.committed_subject_id}",
    )


def _score_clarification_candidate_precision(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    offered = _offered_candidate_ids(packet)
    permitted = set(oracle.permitted_candidate_ids)
    forbidden = set(oracle.forbidden_subject_ids)
    leaked = sorted(offered & forbidden)
    if leaked:
        return _result(
            _D.CLARIFICATION_CANDIDATE_PRECISION,
            Verdict.FAIL,
            f"offered forbidden candidates {leaked}",
        )
    if not permitted:
        ok = not offered
        return _result(
            _D.CLARIFICATION_CANDIDATE_PRECISION,
            Verdict.PASS if ok else Verdict.FAIL,
            "the case permits no candidate; "
            f"packet offered {sorted(offered) or 'none'}",
        )
    stray = sorted(offered - permitted)
    return _result(
        _D.CLARIFICATION_CANDIDATE_PRECISION,
        Verdict.PASS if not stray else Verdict.FAIL,
        f"offered outside the permitted set: {stray}"
        if stray
        else "no stray candidate",
    )


def _score_alias_resolution(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_match_signals:
        return _na(
            _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
            "the case does not reference its subject by alias, acronym, "
            "previous name or provider id",
        )
    committed = [
        candidate
        for candidate in packet.subject_discovery.candidates
        if candidate.commitment_state is SubjectCommitmentState.COMMITTED
    ]
    if not committed:
        return _result(
            _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
            Verdict.FAIL,
            "no subject was committed, so no match signal was recorded",
        )
    signals = {
        signal.signal for candidate in committed for signal in candidate.match_signals
    }
    missing = sorted(str(item) for item in set(oracle.required_match_signals) - signals)
    return _result(
        _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"missing required match signals {missing}"
        if missing
        else f"resolved via {sorted(str(item) for item in signals)}",
    )


def _score_conversational_reference(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if case.follows_case_id is None:
        return _na(
            _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
            "the case is not a conversational follow-up",
        )
    if oracle.committed_subject_id is None:
        asked = bool(packet.evidence_coverage.clarification_needs)
        return _result(
            _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
            Verdict.PASS if asked else Verdict.FAIL,
            "an ambiguous follow-up must ask rather than guess",
        )
    # Relabelled deliberately. Delegating to the rank-1 scorer and returning
    # its result verbatim would file the verdict under SUBJECT_TOP_1 and leave
    # this dimension with no result at all -- an empty cell that reads as a
    # clean sheet, which is the exact failure the verdict-totality test
    # exists to catch.
    resolved = _score_subject_top_1(case, oracle, packet, audit)
    return _result(
        _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
        resolved.verdict,
        f"resolved against the prior turn: {resolved.detail}",
    )


def _score_no_organization_widening(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    forbidden = set(oracle.forbidden_subject_ids) | set(oracle.forbidden_entity_ids)
    named = _offered_candidate_ids(packet) | {
        member.canonical_id for member in packet.comparison_cohort.members
    }
    leaked = sorted(named & forbidden)
    if leaked:
        return _result(
            _D.NO_UNSAFE_ORGANIZATION_WIDENING,
            Verdict.FAIL,
            f"the packet reached forbidden subjects {leaked}",
        )
    if oracle.permitted_candidate_ids:
        stray = sorted(
            named
            - set(oracle.permitted_candidate_ids)
            - set(oracle.required_cohort_ids)
        )
        if stray:
            return _result(
                _D.NO_UNSAFE_ORGANIZATION_WIDENING,
                Verdict.FAIL,
                f"the packet widened to {stray}",
            )
    return _result(
        _D.NO_UNSAFE_ORGANIZATION_WIDENING, Verdict.PASS, "scope stayed bounded"
    )


def _score_cohort_precision(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_cohort_ids and not oracle.forbidden_cohort_ids:
        return _na(_D.COHORT_PRECISION, "the case declares no cohort expectation")
    members = {member.canonical_id for member in packet.comparison_cohort.members}
    intruders = sorted(members & set(oracle.forbidden_cohort_ids))
    return _result(
        _D.COHORT_PRECISION,
        Verdict.PASS if not intruders else Verdict.FAIL,
        f"forbidden cohort members present: {intruders}"
        if intruders
        else "no forbidden member",
    )


def _score_cohort_recall(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_cohort_ids:
        return _na(_D.COHORT_RECALL, "the case requires no cohort member")
    members = {member.canonical_id for member in packet.comparison_cohort.members}
    missing = sorted(set(oracle.required_cohort_ids) - members)
    return _result(
        _D.COHORT_RECALL,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"missing required cohort members: {missing}" if missing else "cohort complete",
    )


def _score_cohort_inclusion_explainability(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not packet.comparison_cohort.members:
        return _na(_D.COHORT_INCLUSION_EXPLAINABILITY, "the packet carries no cohort")
    unexplained = [
        member.canonical_id
        for member in packet.comparison_cohort.members
        if not member.inclusion_rationale.strip()
    ]
    return _result(
        _D.COHORT_INCLUSION_EXPLAINABILITY,
        Verdict.PASS if not unexplained else Verdict.FAIL,
        f"members with no rationale: {sorted(unexplained)}"
        if unexplained
        else "every member states a basis and rationale",
    )


def _score_cohort_exclusion_explainability(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_exclusion_ids:
        return _na(
            _D.COHORT_EXCLUSION_EXPLAINABILITY,
            "the case names no subject the arm should have considered and dropped",
        )
    excluded = {
        exclusion.canonical_id for exclusion in packet.comparison_cohort.exclusions
    }
    missing = sorted(set(oracle.required_exclusion_ids) - excluded)
    return _result(
        _D.COHORT_EXCLUSION_EXPLAINABILITY,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"considered-and-dropped subjects never declared: {missing}"
        if missing
        else "every expected exclusion is stated",
    )


def _score_relevant_entity_recall(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_entity_ids:
        return _na(_D.RELEVANT_ENTITY_RECALL, "the case requires no related entity")
    # Scoped to ``related_context.entities`` because that is the field the
    # frozen registry names for this dimension. Counting cohort members and
    # committed subjects as well made the dimension unfailable on every
    # cohort-bearing case: the entity was always "present" somewhere, so a
    # packet that found none of the related context still scored full recall.
    present = {entity.entity_id for entity in packet.related_context.entities}
    missing = sorted(set(oracle.required_entity_ids) - present)
    return _result(
        _D.RELEVANT_ENTITY_RECALL,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"missing relevant entities: {missing}" if missing else "all present",
    )


def _score_relevant_relationship_recall(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_paths:
        return _na(
            _D.RELEVANT_RELATIONSHIP_RECALL, "the case requires no relationship path"
        )
    missing = sorted(
        expected.key
        for expected in oracle.required_paths
        if not _path_present(packet, expected)
    )
    return _result(
        _D.RELEVANT_RELATIONSHIP_RECALL,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"missing relationships: {missing}" if missing else "all present",
    )


def _score_lineage_path_precision(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    present = _packet_paths(packet)
    forbidden = sorted(
        expected.key for expected in oracle.forbidden_paths if expected.key in present
    )
    if forbidden:
        return _result(
            _D.LINEAGE_PATH_PRECISION,
            Verdict.FAIL,
            f"the packet traversed forbidden relationships: {forbidden}",
        )
    world_keys = {
        f"{edge.source_entity_id}-[{edge.relationship}]->{edge.target_entity_id}"
        for edge in world.WORLD_RELATIONSHIPS
        if not edge.is_false_claim
    }
    invented = sorted(present - world_keys)
    if invented:
        return _result(
            _D.LINEAGE_PATH_PRECISION,
            Verdict.FAIL,
            f"the packet emitted relationships the world does not contain: {invented}",
        )
    if not present:
        return _na(_D.LINEAGE_PATH_PRECISION, "the packet declares no lineage path")
    return _result(
        _D.LINEAGE_PATH_PRECISION,
        Verdict.PASS,
        "every emitted path exists in the world",
    )


def _score_lineage_direction(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not packet.related_context.paths:
        return _na(_D.LINEAGE_DIRECTION_CORRECTNESS, "the packet declares no path")
    offenders = _reversed_hops(packet)
    return _result(
        _D.LINEAGE_DIRECTION_CORRECTNESS,
        Verdict.PASS if not offenders else Verdict.FAIL,
        f"hops pointing the wrong way: {sorted(offenders)}"
        if offenders
        else "every hop matches the world's orientation",
    )


def _score_cross_source_association(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_source_classes:
        return _na(
            _D.CROSS_SOURCE_ASSOCIATION, "the case requires no particular source class"
        )
    observed = {entry.source_class for entry in packet.evidence_coverage.evidence_index}
    missing = sorted(
        str(item) for item in set(oracle.required_source_classes) - observed
    )
    return _result(
        _D.CROSS_SOURCE_ASSOCIATION,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"required source classes absent from the evidence index: {missing}"
        if missing
        else f"drew on {len(observed)} source classes",
    )


def _score_evidence_closure(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    """Closure against the *world*, not merely within the packet.

    The contract already proves internal closure. What it cannot do is notice
    that a perfectly closed packet cites a handle nobody ever issued, or one
    that was revoked — which is the half this scorer adds.
    """

    if audit.fabricated_evidence_handles:
        return _result(
            _D.EVIDENCE_CLOSURE,
            Verdict.FAIL,
            f"cited handles the world never minted: "
            f"{sorted(audit.fabricated_evidence_handles)}",
        )
    if audit.withdrawn_evidence_handles:
        return _result(
            _D.EVIDENCE_CLOSURE,
            Verdict.FAIL,
            f"cited withdrawn evidence: {sorted(audit.withdrawn_evidence_handles)}",
        )
    cited = _cited_handles(packet)
    required = {world.evidence_handle(slug) for slug in oracle.required_evidence_slugs}
    for driver in oracle.expected_principal_drivers:
        required.update(
            world.evidence_handle(slug) for slug in driver.supporting_evidence_slugs
        )
    forbidden = {world.evidence_handle(item.slug) for item in oracle.forbidden_evidence}
    leaked = sorted(cited & forbidden)
    if leaked:
        slugs = sorted(world.EVIDENCE_BY_HANDLE[handle].slug for handle in leaked)
        return _result(
            _D.EVIDENCE_CLOSURE, Verdict.FAIL, f"cited forbidden evidence: {slugs}"
        )
    if not required:
        return _na(
            _D.EVIDENCE_CLOSURE, "the case requires no specific evidence reference"
        )
    missing = sorted(
        world.EVIDENCE_BY_HANDLE[handle].slug for handle in required - cited
    )
    return _result(
        _D.EVIDENCE_CLOSURE,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"required evidence never cited: {missing}" if missing else "evidence complete",
    )


def _score_current_relevance(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_relevance:
        return _na(_D.CURRENT_RELEVANCE, "the case pins no entity's relevance state")
    observed: dict[str, RelevanceState] = {
        entity.entity_id: entity.relevance for entity in packet.related_context.entities
    }
    wrong: list[str] = []
    for entity_id, expected in oracle.required_relevance.items():
        actual = observed.get(entity_id)
        if actual is None:
            wrong.append(f"{entity_id}: absent, expected {expected}")
        elif actual is not expected:
            wrong.append(f"{entity_id}: {actual}, expected {expected}")
    return _result(
        _D.CURRENT_RELEVANCE,
        Verdict.PASS if not wrong else Verdict.FAIL,
        "; ".join(wrong) if wrong else "relevance states match the world",
    )


def _score_principal_driver_precision(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.expected_principal_drivers and not oracle.expected_non_drivers:
        return _na(_D.PRINCIPAL_DRIVER_PRECISION, "the case expects no driver judgment")
    promoted_non_drivers = [
        driver.driver_key
        for driver in oracle.expected_non_drivers
        if driver.standing is not DriverStanding.PRINCIPAL_DRIVER
        and _driver_reached_principal(packet, driver.supporting_evidence_slugs)
    ]
    return _result(
        _D.PRINCIPAL_DRIVER_PRECISION,
        Verdict.PASS if not promoted_non_drivers else Verdict.FAIL,
        f"non-drivers promoted to principal: {sorted(promoted_non_drivers)}"
        if promoted_non_drivers
        else "no non-driver reached principal standing",
    )


def _driver_reached_principal(
    packet: AskDevInvestigationPacket, evidence_slugs: tuple[str, ...]
) -> bool:
    """Whether a principal driver in the packet rests on exactly this evidence.

    Driver ids are the arm's to choose, so the oracle identifies a driver by
    the evidence it should rest on. A principal driver whose *entire* support
    is the evidence of an expected non-driver is that non-driver promoted.
    """

    if not evidence_slugs:
        return False
    handles = {world.evidence_handle(slug) for slug in evidence_slugs}
    for driver in packet.driver_analysis.candidates:
        if driver.standing is not DriverStanding.PRINCIPAL_DRIVER:
            continue
        support = set(driver.supporting_evidence_ids)
        if support and support <= handles:
            return True
    return False


def _score_principal_driver_recall(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.expected_principal_drivers:
        return _na(_D.PRINCIPAL_DRIVER_RECALL, "the case expects no principal driver")
    principal = [
        driver
        for driver in packet.driver_analysis.candidates
        if driver.standing is DriverStanding.PRINCIPAL_DRIVER
    ]
    supported: set[str] = set()
    for driver in principal:
        supported.update(driver.supporting_evidence_ids)
    missing: list[str] = []
    for expected in oracle.expected_principal_drivers:
        handles = {
            world.evidence_handle(slug) for slug in expected.supporting_evidence_slugs
        }
        if not (handles & supported):
            missing.append(expected.driver_key)
    return _result(
        _D.PRINCIPAL_DRIVER_RECALL,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"expected principal drivers not reached: {sorted(missing)}"
        if missing
        else f"{len(principal)} principal driver(s), all expected",
    )


def _score_symptom_versus_driver(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    symptoms = [
        driver
        for driver in oracle.expected_non_drivers
        if driver.role.value == "symptom"
    ]
    if not symptoms:
        return _na(
            _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
            "the case declares no candidate that must be classified a symptom",
        )
    wrong: list[str] = []
    for expected in symptoms:
        handles = {
            world.evidence_handle(slug) for slug in expected.supporting_evidence_slugs
        }
        for driver in packet.driver_analysis.candidates:
            support = set(driver.supporting_evidence_ids)
            if not support or not support <= handles:
                continue
            if driver.role.value != "symptom":
                wrong.append(f"{expected.driver_key} classified {driver.role}")
    return _result(
        _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
        Verdict.PASS if not wrong else Verdict.FAIL,
        "; ".join(wrong) if wrong else "symptoms classified as symptoms",
    )


def _score_unsupported_attribution(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    offenders: list[str] = []
    for driver in packet.driver_analysis.candidates:
        if driver.assertion_basis.value != "measured":
            continue
        for handle in driver.supporting_evidence_ids:
            record = world.EVIDENCE_BY_HANDLE.get(handle)
            if record is None:
                offenders.append(f"{driver.driver_id}: handle never minted")
            elif record.trust is world.TrustLevel.UNTRUSTED_CONTENT:
                offenders.append(
                    f"{driver.driver_id}: measured basis resting on untrusted "
                    f"content ({record.slug})"
                )
    if not packet.driver_analysis.candidates:
        return _na(_D.UNSUPPORTED_ATTRIBUTION_RATE, "the packet asserts no driver")
    return _result(
        _D.UNSUPPORTED_ATTRIBUTION_RATE,
        Verdict.PASS if not offenders else Verdict.FAIL,
        "; ".join(offenders)
        if offenders
        else "every measured claim rests on canonical evidence",
    )


def _score_comparative_judgment(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if len(oracle.required_cohort_ids) < 2:
        return _na(
            _D.COMPARATIVE_JUDGMENT_SUPPORT, "the case is not a comparison question"
        )
    dimensions = packet.comparison_cohort.supported_comparison_dimensions
    return _result(
        _D.COMPARATIVE_JUDGMENT_SUPPORT,
        Verdict.PASS if dimensions else Verdict.FAIL,
        f"declared comparison dimensions: {sorted(str(item) for item in dimensions)}"
        if dimensions
        else "a comparison cohort with no declared comparison dimension",
    )


def _score_answer_usefulness(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    expects_judgment = bool(oracle.expected_principal_drivers)
    asserted = _asserted_driver_ids(packet)
    if not expects_judgment:
        return _result(
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
            Verdict.PASS if not asserted else Verdict.FAIL,
            "the case expects no judgment; "
            f"packet asserted {sorted(asserted) or 'none'}",
        )
    return _result(
        _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        Verdict.PASS if asserted else Verdict.FAIL,
        f"asserted drivers: {sorted(asserted)}"
        if asserted
        else "no asserted driver; the packet points at data without judging it",
    )


def _score_useful_uncertainty(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if not oracle.required_limitation_kinds:
        return _na(
            _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
            "the case requires no particular limitation disclosure",
        )
    disclosed = {
        limitation.kind for limitation in packet.evidence_coverage.limitations
    } | {
        limitation.kind
        for limitation in packet.analytical_job.interpretation_limitations
    }
    missing = sorted(
        str(item) for item in set(oracle.required_limitation_kinds) - disclosed
    )
    return _result(
        _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
        Verdict.PASS if not missing else Verdict.FAIL,
        f"limitations that bit but were not disclosed: {missing}"
        if missing
        else "the limitations that bit are disclosed",
    )


def _score_zero_unauthorized(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    return _result(
        _D.ZERO_UNAUTHORIZED_RESULTS,
        Verdict.PASS if audit.is_clean else Verdict.FAIL,
        audit.summary(),
    )


def _score_zero_person_level_ranking(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    """Scored against the world, not against the packet's own vocabulary.

    The contract makes a person subject unrepresentable, so a packet cannot
    fail this structurally. What it *can* do is name an entity that is not a
    canonical subject at all — the shape a person-level answer would take
    here.
    """

    fabricated = sorted(item.entity_id for item in audit.fabricated_entities)
    if fabricated:
        return _result(
            _D.ZERO_PERSON_LEVEL_RANKING,
            Verdict.FAIL,
            f"the packet names entities the work graph does not contain: {fabricated}",
        )
    return _result(
        _D.ZERO_PERSON_LEVEL_RANKING,
        Verdict.PASS,
        "every named entity is a canonical non-person subject",
    )


def _score_zero_unsupported_staffing_certainty(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    if oracle.confidence_ceiling is ConfidenceQualifier.MEASURED_CERTAIN:
        return _na(
            _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
            "the case sets no confidence ceiling below measured certainty",
        )
    ceiling = oracle.confidence_ceiling
    order = {
        ConfidenceQualifier.UNSUPPORTED: 0,
        ConfidenceQualifier.UNCERTAIN: 1,
        ConfidenceQualifier.QUALIFIED: 2,
        ConfidenceQualifier.MEASURED_CERTAIN: 3,
    }
    offenders = [
        driver.driver_id
        for driver in packet.driver_analysis.candidates
        if order[driver.confidence_qualifier] > order[ceiling]
    ]
    return _result(
        _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
        Verdict.PASS if not offenders else Verdict.FAIL,
        f"claims above the {ceiling} ceiling: {sorted(offenders)}"
        if offenders
        else f"every claim is at or below {ceiling}",
    )


def _score_zero_graph_leakage(
    case: CorpusCase,
    oracle: CaseOracle,
    packet: AskDevInvestigationPacket,
    audit: AuthorizationAudit,
) -> DimensionResult:
    """Backend vocabulary anywhere in the serialized packet.

    The frozen contract enforces neutrality structurally, so this is a
    belt-and-braces scan of the *serialized* packet rather than of its
    schema: free-text fields are where a backend name would actually arrive.
    """

    banned = (
        "graphiti",
        "neo4j",
        "falkordb",
        "cypher",
        "match (",
        "gremlin",
        "group_id",
    )
    blob = packet.model_dump_json().casefold()
    hits = sorted({token for token in banned if token in blob})
    return _result(
        _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE,
        Verdict.PASS if not hits else Verdict.FAIL,
        f"backend vocabulary in the packet: {hits}"
        if hits
        else "no backend vocabulary",
    )


_SCORERS: Mapping[ScoringDimensionID, _Scorer] = {
    _D.SUBJECT_TOP_1: _score_subject_top_1,
    _D.SUBJECT_TOP_3: _score_subject_top_3,
    _D.CLARIFICATION_CANDIDATE_PRECISION: _score_clarification_candidate_precision,
    _D.ALIAS_ACRONYM_RENAME_RESOLUTION: _score_alias_resolution,
    _D.CONVERSATIONAL_REFERENCE_RESOLUTION: _score_conversational_reference,
    _D.NO_UNSAFE_ORGANIZATION_WIDENING: _score_no_organization_widening,
    _D.COHORT_PRECISION: _score_cohort_precision,
    _D.COHORT_RECALL: _score_cohort_recall,
    _D.COHORT_INCLUSION_EXPLAINABILITY: _score_cohort_inclusion_explainability,
    _D.COHORT_EXCLUSION_EXPLAINABILITY: _score_cohort_exclusion_explainability,
    _D.RELEVANT_ENTITY_RECALL: _score_relevant_entity_recall,
    _D.RELEVANT_RELATIONSHIP_RECALL: _score_relevant_relationship_recall,
    _D.LINEAGE_PATH_PRECISION: _score_lineage_path_precision,
    _D.LINEAGE_DIRECTION_CORRECTNESS: _score_lineage_direction,
    _D.CROSS_SOURCE_ASSOCIATION: _score_cross_source_association,
    _D.EVIDENCE_CLOSURE: _score_evidence_closure,
    _D.CURRENT_RELEVANCE: _score_current_relevance,
    _D.PRINCIPAL_DRIVER_PRECISION: _score_principal_driver_precision,
    _D.PRINCIPAL_DRIVER_RECALL: _score_principal_driver_recall,
    _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION: _score_symptom_versus_driver,
    _D.UNSUPPORTED_ATTRIBUTION_RATE: _score_unsupported_attribution,
    _D.COMPARATIVE_JUDGMENT_SUPPORT: _score_comparative_judgment,
    _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD: _score_answer_usefulness,
    _D.USEFUL_UNCERTAINTY_BEHAVIOUR: _score_useful_uncertainty,
    _D.ZERO_UNAUTHORIZED_RESULTS: _score_zero_unauthorized,
    _D.ZERO_PERSON_LEVEL_RANKING: _score_zero_person_level_ranking,
    _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY: _score_zero_unsupported_staffing_certainty,
    _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE: _score_zero_graph_leakage,
}

if set(_SCORERS) != set(SCORING_DIMENSION_REGISTRY):
    _missing = sorted(
        str(item) for item in set(SCORING_DIMENSION_REGISTRY) - set(_SCORERS)
    )
    _extra = sorted(
        str(item) for item in set(_SCORERS) - set(SCORING_DIMENSION_REGISTRY)
    )
    raise RuntimeError(
        "the evaluation layer does not cover the frozen scoring registry; "
        f"missing={_missing}, extra={_extra}. A dimension with no scorer would "
        "render as an empty cell that reads like a clean sheet."
    )


def evaluate_payload(case_id: str, payload: dict[str, Any]) -> CaseEvaluation:
    """Validate a packet payload canonically, then score it against its oracle.

    ``payload`` is a wire dict rather than a constructed model on purpose:
    the canonical validator is the thing under test, and handing it an
    already-constructed model would skip it.
    """

    case = CASE_REGISTRY[case_id]
    oracle = oracle_for(case_id)
    try:
        packet = AskDevInvestigationPacket.model_validate(payload)
    except ValidationError as error:
        return CaseEvaluation(
            case_id=case_id,
            contract_valid=False,
            contract_error=str(error),
            authorization=None,
            results=tuple(
                DimensionResult(
                    dimension_id,
                    Verdict.CONTRACT_INVALID,
                    "the packet did not survive the canonical validator, so "
                    "nothing about its content was scored",
                )
                for dimension_id in case.scoring_dimension_ids
            ),
        )
    return evaluate_case(case_id, packet, oracle=oracle)


def evaluate_case(
    case_id: str,
    packet: AskDevInvestigationPacket,
    *,
    oracle: CaseOracle | None = None,
) -> CaseEvaluation:
    """Score an already-validated packet. Prefer :func:`evaluate_payload`."""

    case = CASE_REGISTRY[case_id]
    resolved = oracle if oracle is not None else oracle_for(case_id)
    audit = audit_authorization(packet, case.principal_id, case_id=case_id)
    results = tuple(
        _SCORERS[dimension_id](case, resolved, packet, audit)
        for dimension_id in case.scoring_dimension_ids
    )
    return CaseEvaluation(
        case_id=case_id,
        contract_valid=True,
        contract_error="",
        authorization=audit,
        results=results,
    )
