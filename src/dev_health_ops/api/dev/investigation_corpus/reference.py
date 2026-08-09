"""A satisfiability witness: the packet a correct arm could emit per case.

**What this is, precisely.** For each authored case, :func:`reference_packet`
builds a contract-valid ``ask_dev_investigation_packet.v1`` payload that
satisfies that case's oracle. Its only job is to prove that the oracle is
*reachable* — that a correct implementation exists which passes every
dimension the case scores.

**What this is not.** It is not an arm, not a baseline, and not a second
implementation to compare against. It is derived from the oracle, so it can
never be evidence that the oracle is *right* — only that it is not
impossible. Saying that plainly matters: a reader who mistook this for an arm
would read a green run as a measurement of graph assistance, which it is not.

**Why it has to exist.** CHAOS-3612 records what happens without it. A corpus
whose expectations nobody has ever satisfied can require a citation no source
can supply, and the resulting failure reads as model quality rather than as a
corpus bug — "the most expensive kind of defect this trial can carry". The
witness turns that from a review question into an executed one, and
``test_chaos_3616_oracles.py`` runs every case through it.

**Why it is also the fault-mode substrate.** Because the witness is a packet
a correct arm could plausibly emit, mutating one field of it produces an
*arm-shaped* bad packet rather than a syntax error — which is the only kind
of negative input that proves an oracle catches the behaviour it names.
"""

from __future__ import annotations

import hashlib
from typing import Any

from ..investigation_contract.question_families import QUESTION_FAMILY_REGISTRY
from ..investigation_contract.vocabulary import (
    ComparisonShape,
    ConfidenceQualifier,
    DriverStanding,
    InvestigationOutcome,
    PacketLimitationKind,
    SubjectMatchSignal,
)
from . import world
from .cases import CASE_REGISTRY, AnswerDisposition, CorpusCase
from .oracles import CaseOracle, DriverExpectation, PathExpectation, oracle_for

__all__ = ["reference_packet"]

_ALLOWED_COMPARISON_DIMENSIONS = (
    "delivery_throughput",
    "review_load",
    "work_in_progress",
    "capacity_load_ratio",
)


def _packet_id(case_id: str) -> str:
    """A deterministic ``ServerHandle`` for a case. Pinned, never minted."""

    digest = hashlib.sha256(f"packet:{case_id}".encode()).hexdigest()
    return (
        f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"
    )


def _iso(value: object) -> str:
    return str(value).replace("+00:00", "Z")


def _evidence_ref(slug: str) -> dict[str, Any]:
    record = world.EVIDENCE_BY_SLUG[slug]
    entity = world.ENTITIES_BY_ID[record.entity_id]
    return {
        "schema_version": "dev_evidence_ref.v1",
        "evidence_ref_id": record.handle,
        "source_system": "dev_health_corpus",
        "source_version": world.CORPUS_VERSION,
        "entity_type": entity.kind.value,
        "entity_id": record.entity_id,
        "display_label": record.display_label,
        "link": {
            "internal_path": f"/work/{entity.kind.value}/{record.entity_id}",
            "source_url": None,
        },
        "observed_at": _iso(record.observed_at),
        "freshness": "fresh",
        "provenance": f"Corpus source record {record.slug}",
        "confidence": 1.0,
        "citation_text": record.citation_text,
        "repository_ids": [],
        "valid_entity_ids": [],
        "flags": {},
    }


def _expected_paths(oracle: CaseOracle) -> tuple[PathExpectation, ...]:
    seen: dict[str, PathExpectation] = {}
    for expectation in oracle.required_paths:
        seen.setdefault(expectation.key, expectation)
    for driver in oracle.expected_principal_drivers + oracle.expected_non_drivers:
        for expectation in driver.supporting_paths:
            seen.setdefault(expectation.key, expectation)
    return tuple(seen.values())


def _path_ids(paths: tuple[PathExpectation, ...]) -> dict[str, str]:
    return {
        expectation.key: f"path_{index + 1}" for index, expectation in enumerate(paths)
    }


def _required_slugs(oracle: CaseOracle) -> tuple[str, ...]:
    slugs: list[str] = list(oracle.required_evidence_slugs)
    for driver in oracle.expected_principal_drivers + oracle.expected_non_drivers:
        for slug in driver.supporting_evidence_slugs:
            if slug not in slugs:
                slugs.append(slug)
    return tuple(slugs)


def _cohort_members(case: CorpusCase, oracle: CaseOracle) -> list[str]:
    if case.comparison_shape is ComparisonShape.SINGULAR_SUBJECT:
        return [oracle.committed_subject_id] if oracle.committed_subject_id else []
    members = list(oracle.required_cohort_ids)
    if len(members) >= 2:
        return members
    forbidden = set(oracle.forbidden_cohort_ids) | set(oracle.forbidden_subject_ids)
    for candidate in oracle.permitted_candidate_ids:
        if len(members) >= 2:
            break
        if candidate in members or candidate in forbidden:
            continue
        members.append(candidate)
    return members


def _driver_payload(
    expectation: DriverExpectation,
    oracle: CaseOracle,
    path_ids: dict[str, str],
) -> dict[str, Any]:
    supporting = [
        path_ids[item.key]
        for item in expectation.supporting_paths
        if item.key in path_ids
    ]
    if not supporting and expectation.standing is DriverStanding.PRINCIPAL_DRIVER:
        supporting = [path_ids[item.key] for item in oracle.required_paths]
    handles = [
        world.evidence_handle(slug) for slug in expectation.supporting_evidence_slugs
    ]
    confidence = oracle.confidence_ceiling
    if confidence is ConfidenceQualifier.UNSUPPORTED:
        confidence = ConfidenceQualifier.UNCERTAIN
    basis = "measured" if handles else "inferred"
    if basis != "measured" and confidence is ConfidenceQualifier.MEASURED_CERTAIN:
        confidence = ConfidenceQualifier.QUALIFIED
    payload: dict[str, Any] = {
        "driver_id": expectation.driver_key,
        "category": expectation.category.value,
        "summary": expectation.rationale,
        "affected_subject_ids": list(expectation.affected_entity_ids),
        "role": expectation.role.value,
        "standing": expectation.standing.value,
        "assertion_basis": basis,
        "confidence_qualifier": confidence.value,
        "supporting_path_ids": supporting,
        "supporting_evidence_ids": handles,
        "conflicting_evidence_ids": [],
        "conflict_note": None,
        "relevance": expectation.relevance.value,
        "exclusion_reason": (
            "insufficient_measurement"
            if expectation.standing is DriverStanding.EXCLUDED
            else None
        ),
        "staffing_qualification": None,
    }
    if expectation.category.value == "capacity_or_staffing":
        weak = oracle.confidence_ceiling is not ConfidenceQualifier.MEASURED_CERTAIN
        has_allocation = any(
            world.EVIDENCE_BY_SLUG[slug].source_class
            is world.SourceClass.INVESTMENT_ALLOCATION
            for slug in expectation.supporting_evidence_slugs
        )
        if has_allocation:
            payload["staffing_qualification"] = {
                "denominator_state": "allocation_evidence_available",
                "denominator_source_classes": ["investment_allocation"],
                "qualification_note": (
                    "Assigned-FTE feed present for this project; the ratio is "
                    "measured against a real denominator."
                ),
            }
        else:
            payload["staffing_qualification"] = {
                "denominator_state": "denominator_absent",
                "denominator_source_classes": [],
                "qualification_note": (
                    "No allocation feed for this project. The demand-to-delivery "
                    "ratio is still measurable; the staffing label is not "
                    "certain."
                ),
            }
        if weak:
            payload["confidence_qualifier"] = ConfidenceQualifier.QUALIFIED.value
    return payload


def reference_packet(case_id: str) -> dict[str, Any]:
    """Build the witness packet for ``case_id`` as a wire dict.

    A dict rather than a model, deliberately: the canonical validator is what
    the evaluation layer runs, and handing it a constructed model would skip
    the very step the manifest's ``validation_policy`` insists on.
    """

    case = CASE_REGISTRY[case_id]
    oracle = oracle_for(case_id)
    paths = _expected_paths(oracle)
    path_ids = _path_ids(paths)
    slugs = _required_slugs(oracle)

    supported = bool(
        oracle.permitted_outcomes
        & {InvestigationOutcome.SUPPORTED, InvestigationOutcome.SUPPORTED_WITH_GAPS}
    )
    # Chosen deterministically from what the oracle permits, in the order a
    # correct arm would prefer. Alphabetical order over the permitted set
    # picks NEEDS_CLARIFICATION for a no-match case, and the contract's own
    # floor for that outcome then rejects the packet.
    if oracle.expected_answer is AnswerDisposition.CLARIFIED:
        outcome = InvestigationOutcome.NEEDS_CLARIFICATION
    elif oracle.expected_answer is AnswerDisposition.UNAVAILABLE:
        outcome = (
            InvestigationOutcome.NO_MATCH
            if InvestigationOutcome.NO_MATCH in oracle.permitted_outcomes
            else InvestigationOutcome.UNSUPPORTED
        )
    elif oracle.required_limitation_kinds and (
        InvestigationOutcome.SUPPORTED_WITH_GAPS in oracle.permitted_outcomes
    ):
        outcome = InvestigationOutcome.SUPPORTED_WITH_GAPS
    elif InvestigationOutcome.SUPPORTED in oracle.permitted_outcomes:
        outcome = InvestigationOutcome.SUPPORTED
    else:
        outcome = sorted(oracle.permitted_outcomes, key=lambda item: item.value)[0]
    if outcome not in oracle.permitted_outcomes:
        raise RuntimeError(
            f"the reference witness for {case_id} would emit {outcome}, which "
            "its own oracle does not permit"
        )

    # -- subject discovery -------------------------------------------------
    candidates: list[dict[str, Any]] = []
    ordered = list(oracle.permitted_candidate_ids)
    if oracle.committed_subject_id is not None:
        ordered = [oracle.committed_subject_id] + [
            item for item in ordered if item != oracle.committed_subject_id
        ]
    for index, entity_id in enumerate(ordered):
        entity = world.ENTITIES_BY_ID[entity_id]
        committed = entity_id == oracle.committed_subject_id
        clarifying = oracle.expected_answer is AnswerDisposition.CLARIFIED
        signals = list(oracle.required_match_signals) or [
            SubjectMatchSignal.EXACT_DISPLAY_NAME
        ]
        if not committed:
            signals = [SubjectMatchSignal.EXACT_DISPLAY_NAME]
        state = "proposed"
        if committed:
            state = "committed"
        elif clarifying:
            state = "ambiguous"
        candidates.append(
            {
                "candidate_id": f"cand_{index + 1}",
                "rank": index + 1,
                "subject_kind": entity.kind.value,
                "canonical_id": entity_id,
                "display_label": entity.display_label,
                "commitment_state": state,
                "match_rationale": (
                    f"Matched {entity.display_label} on "
                    f"{', '.join(signal.value for signal in signals)}."
                ),
                "match_signals": [
                    {
                        "signal": signal.value,
                        "matched_text": entity.display_label,
                        "source_class": "work_graph",
                        "evidence_ref_ids": [],
                    }
                    for signal in signals
                ],
                "match_confidence": 0.95 if committed else 0.4,
                "relevance": (
                    "recently_current"
                    if entity.state is world.EntityState.SUPERSEDED
                    else "current"
                ),
            }
        )

    unresolved: list[dict[str, Any]] = []
    if oracle.expected_answer is AnswerDisposition.CLARIFIED and len(candidates) >= 2:
        unresolved.append(
            {
                "mention_id": "mention_1",
                "mention_text": case.question,
                "reason": "multiple_candidates",
                "candidate_ids": [item["candidate_id"] for item in candidates],
            }
        )

    subject_discovery = {
        "schema_version": "ask_dev_subject_discovery.v1",
        "candidates": candidates,
        "unresolved_mentions": unresolved,
        "committed_subject_ids": (
            [oracle.committed_subject_id] if oracle.committed_subject_id else []
        ),
        "authorization_filtered_count": 0,
        "candidates_truncated": False,
        "truncation_reason": None,
    }

    # -- comparison cohort -------------------------------------------------
    member_ids = _cohort_members(case, oracle)
    members = [
        {
            "subject_kind": world.ENTITIES_BY_ID[entity_id].kind.value,
            "canonical_id": entity_id,
            "display_label": world.ENTITIES_BY_ID[entity_id].display_label,
            "inclusion_basis": ["comparable_delivery_profile"]
            if case.comparison_shape is not ComparisonShape.SINGULAR_SUBJECT
            else ["explicitly_named"],
            "inclusion_rationale": (
                "Included because the world's canonical registry places it in "
                "the comparison set this question asks about."
            ),
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "canonical_registry_membership"
            if case.comparison_shape is not ComparisonShape.SINGULAR_SUBJECT
            else "explicitly_named_by_question",
            "relevance": "current",
        }
        for entity_id in member_ids
    ]
    exclusions = [
        {
            "subject_kind": world.ENTITIES_BY_ID[entity_id].kind.value,
            "canonical_id": entity_id,
            "reason": "insufficient_evidence",
            "rationale": (
                "Considered and dropped: the corroborating evidence a member "
                "needs is absent or contradicted."
            ),
        }
        for entity_id in oracle.required_exclusion_ids
        if entity_id not in member_ids
    ]
    comparison_cohort = {
        "schema_version": "ask_dev_comparison_cohort.v1",
        "cohort_id": f"cohort_{case_id}",
        "comparison_shape": case.comparison_shape.value,
        "members": members,
        "exclusions": exclusions,
        "supported_comparison_dimensions": (
            list(_ALLOWED_COMPARISON_DIMENSIONS[:2])
            if case.comparison_shape is not ComparisonShape.SINGULAR_SUBJECT
            else []
        ),
        "completeness": "complete",
        "truncation_reason": None,
        "cohort_uncertainty": None,
        "authorization_filtered_count": 0,
    }

    # -- related context ---------------------------------------------------
    path_payloads: list[dict[str, Any]] = []
    for expectation in paths:
        source = world.ENTITIES_BY_ID[expectation.source_entity_id]
        target = world.ENTITIES_BY_ID[expectation.target_entity_id]
        path_payloads.append(
            {
                "path_id": path_ids[expectation.key],
                "origin_entity_id": expectation.source_entity_id,
                "terminal_entity_id": expectation.target_entity_id,
                "hops": [
                    {
                        "source_entity_id": expectation.source_entity_id,
                        "source_entity_kind": source.kind.value,
                        "relationship": expectation.relationship.value,
                        "direction": "forward",
                        "target_entity_id": expectation.target_entity_id,
                        "target_entity_kind": target.kind.value,
                        "observed_at": _iso(world.WINDOW_END),
                        "relevance": "current",
                    }
                ],
                "inclusion_reason": (
                    "Traversed because it is part of the mechanism this "
                    "question asks about."
                ),
                "relevance": "current",
                "evidence_ref_ids": [],
                "truncated": False,
                "truncation_reason": None,
                "source_health": "available_current",
            }
        )

    entity_payloads: list[dict[str, Any]] = []
    for entity_id in oracle.required_entity_ids:
        touching = [
            path_ids[expectation.key]
            for expectation in paths
            if entity_id in {expectation.source_entity_id, expectation.target_entity_id}
        ]
        entity = world.ENTITIES_BY_ID[entity_id]
        entity_payloads.append(
            {
                "entity_id": entity_id,
                "entity_kind": entity.kind.value,
                "display_label": entity.display_label,
                "inclusion_reason": (
                    "Reached by a traversed relationship that bears on the question."
                ),
                "supporting_path_ids": touching,
                "relevance": oracle.required_relevance.get(
                    entity_id,
                    world.RelevanceState.RECENTLY_CURRENT
                    if entity.state is world.EntityState.SUPERSEDED
                    else world.RelevanceState.CURRENT,
                ).value,
                "observed_at": _iso(world.WINDOW_END),
            }
        )

    used_entity_ids: set[str] = set()
    used_entity_ids.update(item["canonical_id"] for item in candidates)
    used_entity_ids.update(member_ids)
    used_entity_ids.update(item["canonical_id"] for item in exclusions)
    used_entity_ids.update(oracle.required_entity_ids)
    for expectation in paths:
        used_entity_ids.add(expectation.source_entity_id)
        used_entity_ids.add(expectation.target_entity_id)
    for slug in slugs:
        used_entity_ids.add(world.EVIDENCE_BY_SLUG[slug].entity_id)
    for driver in oracle.expected_principal_drivers + oracle.expected_non_drivers:
        used_entity_ids.update(driver.affected_entity_ids)

    related_context = {
        "schema_version": "ask_dev_related_context.v1",
        "entities": entity_payloads,
        "paths": path_payloads,
        "authorized_entity_ids": sorted(used_entity_ids),
        "authorization_filtered_count": 0,
        "entities_truncated": False,
        "paths_truncated": False,
        "truncation_reason": None,
    }

    # -- drivers -----------------------------------------------------------
    driver_payloads: list[dict[str, Any]] = []
    if supported:
        for driver_expectation in (
            oracle.expected_principal_drivers + oracle.expected_non_drivers
        ):
            driver_payloads.append(
                _driver_payload(driver_expectation, oracle, path_ids)
            )
    driver_analysis = {
        "schema_version": "ask_dev_driver_analysis.v1",
        "candidates": driver_payloads,
        "principal_driver_ids": [
            item["driver_id"]
            for item in driver_payloads
            if item["standing"] == "principal_driver"
        ],
        "candidates_truncated": False,
        "truncation_reason": None,
    }

    # -- evidence coverage -------------------------------------------------
    known_paths = set(path_ids.values())
    known_entities = {item["entity_id"] for item in entity_payloads}
    known_drivers = {item["driver_id"] for item in driver_payloads}
    known_subjects = {item["canonical_id"] for item in candidates} | set(member_ids)

    evidence_index: list[dict[str, Any]] = []
    for slug in slugs:
        record = world.EVIDENCE_BY_SLUG[slug]
        supports_drivers = [
            driver.driver_key
            for driver in oracle.expected_principal_drivers
            + oracle.expected_non_drivers
            if slug in driver.supporting_evidence_slugs
            and driver.driver_key in known_drivers
        ]
        supports_entities = (
            [record.entity_id] if record.entity_id in known_entities else []
        )
        supports_subjects = (
            [record.entity_id] if record.entity_id in known_subjects else []
        )
        supports_paths: list[str] = []
        if not (supports_drivers or supports_entities or supports_subjects):
            supports_paths = sorted(known_paths)[:1]
        evidence_index.append(
            {
                "evidence": _evidence_ref(slug),
                "source_class": record.source_class.value,
                "supports_path_ids": supports_paths,
                "supports_entity_ids": supports_entities,
                "supports_driver_ids": supports_drivers,
                "supports_subject_ids": supports_subjects,
                "relevance": "current",
            }
        )

    family_sources = {
        item.value
        for item in QUESTION_FAMILY_REGISTRY[
            case.question_family
        ].required_source_classes
    }
    indexed_sources = {item["source_class"] for item in evidence_index}
    limitation_kinds = {item.value for item in oracle.required_limitation_kinds}
    missing_sources: list[dict[str, Any]] = []
    source_health: list[dict[str, Any]] = []
    for source_class in sorted(family_sources - indexed_sources):
        if PacketLimitationKind.MISSING_SOURCE.value in limitation_kinds and (
            source_class == "investment_allocation"
        ):
            missing_sources.append(
                {
                    "source_class": source_class,
                    "state": "unconfigured",
                    "impact": (
                        "No allocation feed exists for this subject, so no "
                        "staffing denominator is available."
                    ),
                }
            )
            continue
        source_health.append(
            {
                "source_class": source_class,
                "state": "available_current",
                "observed_at": _iso(world.WINDOW_END),
                "detail": "Read and found to carry nothing bearing on this question.",
            }
        )

    limitations = [
        {
            "kind": kind,
            "detail": "Disclosed because it materially bounds this answer.",
        }
        for kind in sorted(limitation_kinds)
    ]
    if missing_sources and PacketLimitationKind.MISSING_SOURCE.value not in {
        item["kind"] for item in limitations
    }:
        limitations.append(
            {
                "kind": PacketLimitationKind.MISSING_SOURCE.value,
                "detail": "A source this family requires is unconfigured.",
            }
        )
    if any(
        item["staffing_qualification"] is not None
        and item["staffing_qualification"]["denominator_state"]
        != ("allocation_evidence_available")
        for item in driver_payloads
    ) and PacketLimitationKind.ABSENT_STAFFING_DENOMINATOR.value not in {
        item["kind"] for item in limitations
    }:
        limitations.append(
            {
                "kind": PacketLimitationKind.ABSENT_STAFFING_DENOMINATOR.value,
                "detail": "A capacity claim rests on a partial or absent denominator.",
            }
        )

    clarification_needs: list[dict[str, Any]] = []
    if oracle.expected_answer is AnswerDisposition.CLARIFIED:
        clarification_needs.append(
            {
                "kind": "ambiguous_subject",
                "prompt": "Which of these did you mean?",
                "candidate_ids": [item["candidate_id"] for item in candidates],
            }
        )
    if outcome is InvestigationOutcome.NO_MATCH and not limitations:
        limitations.append(
            {
                "kind": PacketLimitationKind.MISSING_SOURCE.value,
                "detail": (
                    "No canonical subject matches the reference in the "
                    "question, and the investigation did not widen."
                ),
            }
        )
    if outcome is InvestigationOutcome.UNSUPPORTED and not limitations:
        limitations.append(
            {
                "kind": PacketLimitationKind.MISSING_SOURCE.value,
                "detail": "The evidence required to answer this question is absent.",
            }
        )

    evidence_coverage = {
        "schema_version": "ask_dev_evidence_coverage.v1",
        "evidence_index": evidence_index,
        "source_health": source_health,
        "missing_sources": missing_sources,
        "conflicts": [],
        "limitations": limitations,
        "clarification_needs": clarification_needs,
        "authorization_filtered_count": 0,
        "evidence_truncated": False,
        "truncation_reason": None,
    }

    # -- analytical job ----------------------------------------------------
    uncertain = oracle.committed_subject_id is None
    interpretation_limitations = (
        [
            {
                "kind": PacketLimitationKind.INTERPRETATION_UNCERTAINTY.value,
                "detail": (
                    "The question names no single subject, so the analytical "
                    "job was interpreted broadly and the chosen reading is "
                    "stated rather than assumed."
                ),
            }
        ]
        if uncertain
        else []
    )
    analytical_job = {
        "schema_version": "ask_dev_analytical_job.v1",
        "job_id": f"job_{case_id}",
        "question_family": case.question_family.value,
        "job_uncertainty": "broad_with_uncertainty" if uncertain else "precise",
        "job_statement": case.title,
        "comparison_shape": case.comparison_shape.value,
        "time_context": {
            "start": _iso(world.WINDOW_START),
            "end": _iso(world.WINDOW_END),
            "timezone": "UTC",
            "analytical_slice": "current",
            "as_of": None,
            "historical_comparability": "not_applicable",
            "edge_validity_basis": "not_required",
        },
        "surface_context_refs": [],
        "conversation_reference_ids": (
            [f"conv_{case.follows_case_id}"] if case.follows_case_id else []
        ),
        "interpretation_limitations": interpretation_limitations,
    }

    source_contract_versions = sorted(
        {item["source_class"] for item in evidence_index}
        | {item["source_class"] for item in source_health}
        | {item["source_class"] for item in missing_sources}
    )
    if not source_contract_versions:
        source_contract_versions = ["work_graph"]
    versions = {
        "schema_version": "ask_dev_investigation_versions.v1",
        "packet_schema_version": "ask_dev_investigation_packet.v1",
        "query_version": "corpus_reference_query.v1",
        "ranking_version": "corpus_reference_ranking.v1",
        "projection_version": "corpus_reference_projection.v1",
        "source_contract_versions": [
            {"source_class": source_class, "contract_version": f"{source_class}.v1"}
            for source_class in source_contract_versions
        ],
        "corpus_version": world.CORPUS_VERSION,
        "trial": None,
    }

    return {
        "schema_version": "ask_dev_investigation_packet.v1",
        "packet_id": _packet_id(case_id),
        "organization_id": world.ORG_HELIO,
        "produced_at": _iso(world.TRIAL_NOW),
        "outcome": outcome.value,
        "analytical_job": analytical_job,
        "subject_discovery": subject_discovery,
        "comparison_cohort": comparison_cohort,
        "related_context": related_context,
        "driver_analysis": driver_analysis,
        "evidence_coverage": evidence_coverage,
        "versions": versions,
    }
