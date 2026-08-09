"""Fault injection proof: every CHAOS-3616 oracle expectation is load-bearing.

``tests/api/dev/test_chaos_3616_fault_modes.py`` proves each arm-shaped bad
packet is *caught*. That, on its own, is not proof that the named expectation
is what catches it — a mutation could trip an unrelated scorer and the test
would still be green with the expectation missing or wrong. This script
closes that gap the only way it can be closed: it **removes the expectation
and watches the bad packet be accepted.**

For each case it runs one subprocess that:

1. scores the arm-shaped payload against the pristine corpus and requires the
   named dimension to FAIL (baseline);
2. neutralizes exactly one thing the expectation rests on — the world's true
   authorization grant, the world's evidence mint, one field of one oracle;
3. scores the same payload again and requires that dimension to be
   **PASS or NOT_APPLICABLE**.

Note what is *not* neutralized: the scorer function. Stubbing a scorer to
return PASS would make step 3 tautological. What is removed instead is the
knowledge the scorer draws on, which is the thing that could actually be
absent in a corpus authored badly — a forbidden-cohort list nobody filled in,
an authorization grant that lets everything through, an evidence mint that
accepts any handle. An expectation whose removal changes nothing is not the
expectation the corpus claims it is, and the script fails.

Subprocess isolation is not decoration: step 2 mutates module-level corpus
state, and doing it inside the test process would corrupt every later test.

Run it directly::

    python scripts/verify_chaos_3616_oracle_guards.py

Exit code 0 means every named expectation was observed failing and then
observed not failing. Any other exit code names the case that did not behave
as claimed. The script also fails if the case table leaves a scoring
dimension neither injected nor explicitly excused — an uninjected dimension
would otherwise be an unmeasured claim of coverage, which is the whole
failure mode this corpus exists to avoid.
"""

from __future__ import annotations

import argparse
import copy
import dataclasses
import subprocess
import sys
from pathlib import Path
from typing import Any, NamedTuple

from dev_health_ops.api.dev.investigation_contract import (
    ALL_SCORING_DIMENSION_IDS,
    ScoringDimensionID,
)
from dev_health_ops.api.dev.investigation_corpus import (
    evaluate as evaluate_module,
)
from dev_health_ops.api.dev.investigation_corpus import (
    oracles as oracles_module,
)
from dev_health_ops.api.dev.investigation_corpus import world as world_module
from dev_health_ops.api.dev.investigation_corpus.evaluate import (
    Verdict,
    evaluate_payload,
)
from dev_health_ops.api.dev.investigation_corpus.reference import reference_packet

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

_D = ScoringDimensionID


class GuardCase(NamedTuple):
    """One dimension, the defect it must catch, and what makes it catch it."""

    dimension: ScoringDimensionID
    case_id: str
    mutation: str
    #: The oracle field to blank, or ``None`` for a world-level neutralizer.
    oracle_field: str | None
    #: A world-level neutralizer name, or ``None`` for an oracle field.
    world_neutralizer: str | None


# --------------------------------------------------------------------------
# Arm-shaped mutations
# --------------------------------------------------------------------------


def _mutate_wrong_subject(payload: dict[str, Any]) -> None:
    candidates = payload["subject_discovery"]["candidates"]
    first, second = candidates[0], candidates[1]
    first["canonical_id"], second["canonical_id"] = (
        second["canonical_id"],
        first["canonical_id"],
    )
    payload["subject_discovery"]["committed_subject_ids"] = [first["canonical_id"]]


def _mutate_only_the_decoy(payload: dict[str, Any]) -> None:
    """Drop the correct subject entirely and keep the near-miss.

    Swapping ranks is not enough for top-3: the right answer is still in the
    list, which is exactly what top-3 is supposed to tolerate. Only removing
    it can fail this dimension, and a case where the two dimensions fail
    together would not distinguish them.
    """

    candidates = payload["subject_discovery"]["candidates"]
    decoy = [
        candidate
        for candidate in candidates
        if candidate["canonical_id"] == world_module.PROJ_AUTH_HARDENING
    ]
    for index, candidate in enumerate(decoy):
        candidate["rank"] = index + 1
        candidate["commitment_state"] = "committed"
    payload["subject_discovery"]["candidates"] = decoy
    payload["subject_discovery"]["committed_subject_ids"] = [
        world_module.PROJ_AUTH_HARDENING
    ]


def _mutate_forbidden_but_real_path(payload: dict[str, Any]) -> None:
    """Traverse a relationship that exists in the world and is wrong here.

    The removed ratelimitd dependency. Distinguished from a *fabricated*
    path on purpose: the world-existence check cannot catch this one, so
    only ``forbidden_paths`` can, which is what makes the injection
    meaningful.
    """

    payload["related_context"]["paths"].append(
        {
            "path_id": "path_historical",
            "origin_entity_id": world_module.PROJ_PULSE,
            "terminal_entity_id": world_module.DEP_RATELIMITD,
            "hops": [
                {
                    "source_entity_id": world_module.PROJ_PULSE,
                    "source_entity_kind": "project",
                    "relationship": "depends_on",
                    "direction": "forward",
                    "target_entity_id": world_module.DEP_RATELIMITD,
                    "target_entity_kind": "dependency",
                    "observed_at": "2026-08-08T00:00:00Z",
                    "relevance": "current",
                }
            ],
            "inclusion_reason": "Pulse depends on ratelimitd.",
            "relevance": "current",
            "evidence_ref_ids": [],
            "truncated": False,
            "truncation_reason": None,
            "source_health": "available_current",
        }
    )
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {
            *payload["related_context"]["authorized_entity_ids"],
            world_module.DEP_RATELIMITD,
        }
    )


def _mutate_demote_every_driver(payload: dict[str, Any]) -> None:
    """A well-formed packet that asserts nothing: 'here are some links'."""

    for driver in payload["driver_analysis"]["candidates"]:
        driver["standing"] = "candidate_only"
        driver["exclusion_reason"] = None
    payload["driver_analysis"]["principal_driver_ids"] = []
    payload["outcome"] = "needs_clarification"
    payload["evidence_coverage"]["clarification_needs"] = [
        {
            "kind": "missing_comparison_basis",
            "prompt": "Open the team dashboard to compare these yourself.",
            "candidate_ids": [],
        }
    ]


def _mutate_false_inclusion_basis(payload: dict[str, Any]) -> None:
    """A member whose stated basis is a plausible sentence and untrue."""

    payload["comparison_cohort"]["members"].append(
        {
            "subject_kind": "project",
            "canonical_id": world_module.PROJ_MERIDIAN,
            "display_label": "Meridian Docs",
            "inclusion_basis": ["shared_dependency"],
            "inclusion_rationale": (
                "Shares the authcore dependency with the rest of the set."
            ),
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "canonical_registry_membership",
            "relevance": "current",
        }
    )
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {
            *payload["related_context"]["authorized_entity_ids"],
            world_module.PROJ_MERIDIAN,
        }
    )


def _mutate_unbacked_comparison_axis(payload: dict[str, Any]) -> None:
    """Compare on an axis the world has no numbers for."""

    payload["comparison_cohort"]["supported_comparison_dimensions"] = [
        "deployment_frequency"
    ]


def _mutate_fuzzy_signal_only(payload: dict[str, Any]) -> None:
    for candidate in payload["subject_discovery"]["candidates"]:
        if candidate["commitment_state"] != "committed":
            continue
        candidate["match_signals"] = [
            {
                "signal": "exact_display_name",
                "matched_text": candidate["display_label"],
                "source_class": "work_graph",
                "evidence_ref_ids": [],
            }
        ]


def _mutate_unauthorized_cohort_member(payload: dict[str, Any]) -> None:
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {*payload["related_context"]["authorized_entity_ids"], world_module.PROJ_QUARRY}
    )
    payload["comparison_cohort"]["members"].append(
        {
            "subject_kind": "project",
            "canonical_id": world_module.PROJ_QUARRY,
            "display_label": "Quarry Compliance",
            "inclusion_basis": ["same_portfolio"],
            "inclusion_rationale": "Swept in with the rest of the portfolio.",
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "canonical_registry_membership",
            "relevance": "current",
        }
    )


def _mutate_unrelated_cohort_member(payload: dict[str, Any]) -> None:
    payload["comparison_cohort"]["members"].append(
        {
            "subject_kind": "project",
            "canonical_id": world_module.PROJ_MERIDIAN,
            "display_label": "Meridian Docs",
            "inclusion_basis": ["shared_dependency"],
            "inclusion_rationale": "A planning note says it is behind authcore too.",
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "canonical_registry_membership",
            "relevance": "current",
        }
    )
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {
            *payload["related_context"]["authorized_entity_ids"],
            world_module.PROJ_MERIDIAN,
        }
    )


def _mutate_substitute_cohort_member(payload: dict[str, Any]) -> None:
    for member in payload["comparison_cohort"]["members"]:
        if member["canonical_id"] != world_module.PROJ_BEACON:
            continue
        member["canonical_id"] = world_module.PROJ_MERIDIAN
        member["display_label"] = "Meridian Docs"
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {
            *payload["related_context"]["authorized_entity_ids"],
            world_module.PROJ_MERIDIAN,
        }
    )


def _mutate_drop_exclusions(payload: dict[str, Any]) -> None:
    payload["comparison_cohort"]["exclusions"] = []


def _mutate_drop_related_entity(payload: dict[str, Any]) -> None:
    payload["related_context"]["entities"] = payload["related_context"]["entities"][:1]


def _mutate_drop_path(payload: dict[str, Any]) -> None:
    keep = payload["related_context"]["paths"][:1]
    kept_ids = {path["path_id"] for path in keep}
    payload["related_context"]["paths"] = keep
    payload["related_context"]["entities"] = [
        entity
        for entity in payload["related_context"]["entities"]
        if set(entity["supporting_path_ids"]) & kept_ids
    ]
    for entity in payload["related_context"]["entities"]:
        entity["supporting_path_ids"] = [
            path_id for path_id in entity["supporting_path_ids"] if path_id in kept_ids
        ]
    for driver in payload["driver_analysis"]["candidates"]:
        driver["supporting_path_ids"] = [
            path_id for path_id in driver["supporting_path_ids"] if path_id in kept_ids
        ]
        if not driver["supporting_path_ids"] and driver["standing"] == (
            "principal_driver"
        ):
            driver["standing"] = "contributing_driver"
    payload["driver_analysis"]["principal_driver_ids"] = [
        driver["driver_id"]
        for driver in payload["driver_analysis"]["candidates"]
        if driver["standing"] == "principal_driver"
    ]
    for entry in payload["evidence_coverage"]["evidence_index"]:
        entry["supports_path_ids"] = [
            path_id for path_id in entry["supports_path_ids"] if path_id in kept_ids
        ]
        entry["supports_entity_ids"] = [
            entity_id
            for entity_id in entry["supports_entity_ids"]
            if entity_id
            in {
                entity["entity_id"] for entity in payload["related_context"]["entities"]
            }
        ]
        if not (
            entry["supports_path_ids"]
            or entry["supports_entity_ids"]
            or entry["supports_driver_ids"]
            or entry["supports_subject_ids"]
        ):
            entry["supports_path_ids"] = sorted(kept_ids)[:1]


def _mutate_false_path(payload: dict[str, Any]) -> None:
    payload["related_context"]["paths"].append(
        {
            "path_id": "path_false",
            "origin_entity_id": world_module.PROJ_MERIDIAN,
            "terminal_entity_id": world_module.DEP_AUTHCORE,
            "hops": [
                {
                    "source_entity_id": world_module.PROJ_MERIDIAN,
                    "source_entity_kind": "project",
                    "relationship": "blocked_by",
                    "direction": "forward",
                    "target_entity_id": world_module.DEP_AUTHCORE,
                    "target_entity_kind": "dependency",
                    "observed_at": "2026-08-08T00:00:00Z",
                    "relevance": "current",
                }
            ],
            "inclusion_reason": "A planning note says Meridian is blocked too.",
            "relevance": "current",
            "evidence_ref_ids": [],
            "truncated": False,
            "truncation_reason": None,
            "source_health": "available_current",
        }
    )
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {
            *payload["related_context"]["authorized_entity_ids"],
            world_module.PROJ_MERIDIAN,
        }
    )


def _mutate_reverse_parent_of(payload: dict[str, Any]) -> None:
    for path in payload["related_context"]["paths"]:
        hop = path["hops"][0]
        if hop["relationship"] != "parent_of":
            continue
        hop["source_entity_id"], hop["target_entity_id"] = (
            hop["target_entity_id"],
            hop["source_entity_id"],
        )
        path["origin_entity_id"] = hop["source_entity_id"]
        path["terminal_entity_id"] = hop["target_entity_id"]


def _mutate_fabricated_handle(payload: dict[str, Any]) -> None:
    entry = copy.deepcopy(payload["evidence_coverage"]["evidence_index"][0])
    entry["evidence"]["evidence_ref_id"] = "ev1_" + "f0" * 20
    entry["supports_path_ids"] = [payload["related_context"]["paths"][0]["path_id"]]
    entry["supports_entity_ids"] = []
    entry["supports_driver_ids"] = []
    entry["supports_subject_ids"] = []
    payload["evidence_coverage"]["evidence_index"].append(entry)


def _mutate_drop_a_source_class(payload: dict[str, Any]) -> None:
    payload["evidence_coverage"]["evidence_index"] = [
        entry
        for entry in payload["evidence_coverage"]["evidence_index"]
        if entry["source_class"] != "investment_allocation"
    ]
    for driver in payload["driver_analysis"]["candidates"]:
        driver["supporting_evidence_ids"] = [
            handle
            for handle in driver["supporting_evidence_ids"]
            if handle != world_module.evidence_handle("ia_cinder_displaced")
        ]
    payload["evidence_coverage"]["source_health"].append(
        {
            "source_class": "investment_allocation",
            "state": "available_current",
            "observed_at": "2026-08-08T00:00:00Z",
            "detail": "Read but not used.",
        }
    )


def _mutate_wrong_relevance(payload: dict[str, Any]) -> None:
    for entity in payload["related_context"]["entities"]:
        entity["relevance"] = "historical_only"


def _mutate_promote_the_symptom(payload: dict[str, Any]) -> None:
    for driver in payload["driver_analysis"]["candidates"]:
        if driver["driver_id"] != "identity_ci_failures":
            continue
        driver["role"] = "driver"
        driver["standing"] = "principal_driver"
        driver["supporting_path_ids"] = payload["driver_analysis"]["candidates"][0][
            "supporting_path_ids"
        ]
    payload["driver_analysis"]["principal_driver_ids"] = [
        driver["driver_id"]
        for driver in payload["driver_analysis"]["candidates"]
        if driver["standing"] == "principal_driver"
    ]


def _mutate_relabel_the_symptom(payload: dict[str, Any]) -> None:
    for driver in payload["driver_analysis"]["candidates"]:
        if driver["driver_id"] == "identity_ci_failures":
            driver["role"] = "contextual_correlate"


def _mutate_drop_the_driver(payload: dict[str, Any]) -> None:
    payload["driver_analysis"]["candidates"] = []
    payload["driver_analysis"]["principal_driver_ids"] = []
    payload["outcome"] = "no_match"
    payload["subject_discovery"]["committed_subject_ids"] = []
    for candidate in payload["subject_discovery"]["candidates"]:
        candidate["commitment_state"] = "proposed"
    payload["evidence_coverage"]["evidence_index"] = []
    payload["evidence_coverage"]["source_health"] = [
        {
            "source_class": source_class,
            "state": "available_current",
            "observed_at": "2026-08-08T00:00:00Z",
            "detail": "Read; nothing outstanding.",
        }
        for source_class in (
            "work_item",
            "status_change",
            "pull_request",
            "review",
            "deployment",
        )
    ]
    payload["evidence_coverage"]["limitations"] = [
        {"kind": "missing_source", "detail": "Nothing outstanding was found."}
    ]


def _mutate_certain_staffing(payload: dict[str, Any]) -> None:
    for driver in payload["driver_analysis"]["candidates"]:
        driver["confidence_qualifier"] = "measured_certain"
        if driver.get("staffing_qualification"):
            driver["staffing_qualification"]["denominator_state"] = (
                "allocation_evidence_available"
            )
            driver["staffing_qualification"]["denominator_source_classes"] = [
                "investment_allocation"
            ]
    payload["evidence_coverage"]["limitations"] = [
        item
        for item in payload["evidence_coverage"]["limitations"]
        if item["kind"] != "absent_staffing_denominator"
    ]


def _mutate_drop_the_limitation(payload: dict[str, Any]) -> None:
    payload["evidence_coverage"]["limitations"] = [
        item
        for item in payload["evidence_coverage"]["limitations"]
        if item["kind"] != "stale_source"
    ]
    payload["outcome"] = "supported"


def _mutate_untrusted_measurement(payload: dict[str, Any]) -> None:
    handle = world_module.evidence_handle("doc_injected_runbook")
    driver = payload["driver_analysis"]["candidates"][0]
    driver["assertion_basis"] = "measured"
    driver["supporting_evidence_ids"] = [*driver["supporting_evidence_ids"], handle]
    entry = copy.deepcopy(payload["evidence_coverage"]["evidence_index"][0])
    entry["evidence"]["evidence_ref_id"] = handle
    entry["source_class"] = "work_item"
    entry["supports_driver_ids"] = [driver["driver_id"]]
    entry["supports_entity_ids"] = []
    entry["supports_subject_ids"] = []
    entry["supports_path_ids"] = []
    payload["evidence_coverage"]["evidence_index"].append(entry)


def _mutate_guess_instead_of_asking(payload: dict[str, Any]) -> None:
    payload["evidence_coverage"]["clarification_needs"] = []
    payload["outcome"] = "no_match"
    payload["evidence_coverage"]["limitations"] = [
        {"kind": "missing_source", "detail": "Could not resolve the reference."}
    ]


def _mutate_widen_after_no_match(payload: dict[str, Any]) -> None:
    payload["subject_discovery"]["candidates"] = [
        {
            "candidate_id": "cand_1",
            "rank": 1,
            "subject_kind": "project",
            "canonical_id": world_module.PROJ_IDENTITY_REWRITE,
            "display_label": "Identity Platform Rewrite",
            "commitment_state": "proposed",
            "match_rationale": "Nearest label match to the unresolved name.",
            "match_signals": [
                {
                    "signal": "fuzzy_label",
                    "matched_text": "Halcyon",
                    "source_class": "work_graph",
                    "evidence_ref_ids": [],
                }
            ],
            "match_confidence": 0.3,
            "relevance": "current",
        }
    ]
    payload["related_context"]["authorized_entity_ids"] = sorted(
        {
            *payload["related_context"]["authorized_entity_ids"],
            world_module.PROJ_IDENTITY_REWRITE,
        }
    )


MUTATIONS = {
    name: value
    for name, value in list(globals().items())
    if name.startswith("_mutate_")
}


# --------------------------------------------------------------------------
# The case table
# --------------------------------------------------------------------------

CASES: tuple[GuardCase, ...] = (
    GuardCase(
        _D.SUBJECT_TOP_1,
        "H03_the_auth_work",
        "_mutate_wrong_subject",
        "committed_subject_id",
        None,
    ),
    GuardCase(
        _D.SUBJECT_TOP_3,
        "H03_the_auth_work",
        "_mutate_only_the_decoy",
        "committed_subject_id",
        None,
    ),
    GuardCase(
        _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
        "H03_the_auth_work",
        "_mutate_fuzzy_signal_only",
        "required_match_signals",
        None,
    ),
    GuardCase(
        _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
        "H05_the_other_project_we_discussed",
        "_mutate_guess_instead_of_asking",
        None,
        "unfollow_the_case",
    ),
    GuardCase(
        _D.CLARIFICATION_CANDIDATE_PRECISION,
        "H08_no_match_must_not_widen",
        "_mutate_widen_after_no_match",
        None,
        "permit_every_candidate",
    ),
    GuardCase(
        _D.NO_UNSAFE_ORGANIZATION_WIDENING,
        "H08_no_match_must_not_widen",
        "_mutate_widen_after_no_match",
        "forbidden_subject_ids",
        None,
    ),
    GuardCase(
        _D.COHORT_PRECISION,
        "S03_shared_dependency_portfolio_risk",
        "_mutate_unrelated_cohort_member",
        "forbidden_cohort_ids",
        None,
    ),
    GuardCase(
        _D.COHORT_RECALL,
        "P01_demand_exceeds_capacity",
        "_mutate_substitute_cohort_member",
        "required_cohort_ids",
        None,
    ),
    GuardCase(
        _D.COHORT_EXCLUSION_EXPLAINABILITY,
        "T02_high_wip_without_struggle",
        "_mutate_drop_exclusions",
        "required_exclusion_ids",
        None,
    ),
    GuardCase(
        _D.RELEVANT_ENTITY_RECALL,
        "S03_shared_dependency_portfolio_risk",
        "_mutate_drop_related_entity",
        "required_entity_ids",
        None,
    ),
    GuardCase(
        _D.RELEVANT_RELATIONSHIP_RECALL,
        "S03_shared_dependency_portfolio_risk",
        "_mutate_drop_path",
        "required_paths",
        None,
    ),
    GuardCase(
        _D.LINEAGE_PATH_PRECISION,
        "S02_implementation_versus_release_readiness",
        "_mutate_forbidden_but_real_path",
        "forbidden_paths",
        None,
    ),
    GuardCase(
        _D.LINEAGE_DIRECTION_CORRECTNESS,
        "S01_declared_versus_child_completion",
        "_mutate_reverse_parent_of",
        None,
        "erase_world_relationships",
    ),
    GuardCase(
        _D.CROSS_SOURCE_ASSOCIATION,
        "T03_operational_displaces_feature",
        "_mutate_drop_a_source_class",
        "required_source_classes",
        None,
    ),
    GuardCase(
        _D.EVIDENCE_CLOSURE,
        "T01_clearly_struggling_team",
        "_mutate_fabricated_handle",
        None,
        "accept_any_handle",
    ),
    GuardCase(
        _D.CURRENT_RELEVANCE,
        "S02_implementation_versus_release_readiness",
        "_mutate_wrong_relevance",
        "required_relevance",
        None,
    ),
    GuardCase(
        _D.PRINCIPAL_DRIVER_PRECISION,
        "S04_symptom_versus_driver",
        "_mutate_promote_the_symptom",
        "expected_non_drivers",
        None,
    ),
    GuardCase(
        _D.PRINCIPAL_DRIVER_RECALL,
        "A04_prompt_injection_in_document",
        "_mutate_drop_the_driver",
        "expected_principal_drivers",
        None,
    ),
    GuardCase(
        _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
        "S04_symptom_versus_driver",
        "_mutate_relabel_the_symptom",
        "expected_non_drivers",
        None,
    ),
    GuardCase(
        _D.UNSUPPORTED_ATTRIBUTION_RATE,
        "A04_prompt_injection_in_document",
        "_mutate_untrusted_measurement",
        None,
        "trust_everything",
    ),
    GuardCase(
        _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        "T01_clearly_struggling_team",
        "_mutate_demote_every_driver",
        "expected_principal_drivers",
        None,
    ),
    GuardCase(
        _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
        "A08_stale_and_truncated_state",
        "_mutate_drop_the_limitation",
        "required_limitation_kinds",
        None,
    ),
    GuardCase(
        _D.ZERO_UNAUTHORIZED_RESULTS,
        "S03_shared_dependency_portfolio_risk",
        "_mutate_unauthorized_cohort_member",
        None,
        "grant_everything",
    ),
    GuardCase(
        _D.ZERO_PERSON_LEVEL_RANKING,
        "T01_clearly_struggling_team",
        "_mutate_fabricated_entity",
        None,
        "know_every_entity",
    ),
    GuardCase(
        _D.COHORT_INCLUSION_EXPLAINABILITY,
        "T01_clearly_struggling_team",
        "_mutate_false_inclusion_basis",
        None,
        "believe_every_stated_basis",
    ),
    GuardCase(
        _D.COMPARATIVE_JUDGMENT_SUPPORT,
        "P01_demand_exceeds_capacity",
        "_mutate_unbacked_comparison_axis",
        None,
        "believe_every_comparison",
    ),
    GuardCase(
        _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
        "P05_allocation_absent_still_supportable",
        "_mutate_certain_staffing",
        "confidence_ceiling",
        None,
    ),
)


def _mutate_fabricated_entity(payload: dict[str, Any]) -> None:
    """Name an entity the work graph does not contain.

    The shape a person-level answer would take here: the packet cannot
    represent a person subject, so the only way to smuggle one in is an
    identifier the canonical registry has never heard of.
    """

    payload["related_context"]["authorized_entity_ids"] = sorted(
        {*payload["related_context"]["authorized_entity_ids"], "person_a_developer"}
    )
    payload["comparison_cohort"]["members"].append(
        {
            "subject_kind": "team",
            "canonical_id": "person_a_developer",
            "display_label": "A developer",
            "inclusion_basis": ["peer_of_named_subject"],
            "inclusion_rationale": "The individual carrying the review queue.",
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "canonical_registry_membership",
            "relevance": "current",
        }
    )


MUTATIONS["_mutate_fabricated_entity"] = _mutate_fabricated_entity


#: Dimensions with no injection case, and why. Every entry is a scorer that
#: reads no oracle field and no world fact, so there is nothing to remove:
#: neutralizing it would mean stubbing the function, which proves nothing.
UNINJECTED: dict[ScoringDimensionID, str] = {
    _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE: (
        "scans the serialized packet for a fixed banned-token list. The list "
        "is the guard; removing it is deleting the scorer, not removing an "
        "expectation."
    ),
}


# --------------------------------------------------------------------------
# Neutralizers
# --------------------------------------------------------------------------


def _grant_everything() -> None:
    everything = frozenset(world_module.ENTITIES_BY_ID)
    for principal_id, principal in list(world_module.PRINCIPALS.items()):
        world_module.PRINCIPALS[principal_id] = dataclasses.replace(  # type: ignore[index]
            principal, visible_entity_ids=everything
        )


def _accept_any_handle() -> None:
    class _Permissive(dict[str, Any]):
        """Resolve any handle to *something*, the way a mint-less corpus would."""

        def get(self, key: Any, default: Any = None) -> Any:
            found = dict.get(self, key)
            if found is not None:
                return found
            return next(iter(self.values()))

    evaluate_module.world.EVIDENCE_BY_HANDLE = _Permissive(
        world_module.EVIDENCE_BY_HANDLE
    )


def _know_every_entity() -> None:
    """Make the fabricated identifier a real canonical entity.

    Patching ``__contains__`` is not enough: ``audit_authorization`` takes
    ``set(world.ENTITIES_BY_ID)``, which reads the keys. The neutralizer has
    to add the key and grant it, which is exactly the corpus knowledge whose
    absence the dimension depends on.
    """

    fabricated = "person_a_developer"
    world_module.ENTITIES_BY_ID[fabricated] = world_module.WorldEntity(  # type: ignore[index]
        fabricated,
        world_module.InvestigationSubjectKind.TEAM,
        "A developer",
        world_module.ORG_HELIO,
        world_module.WORLD_EPOCH,
    )
    _grant_everything()


def _permit_every_candidate() -> None:
    """Let the case permit whatever the packet happened to offer."""

    everything = tuple(world_module.ENTITIES_BY_ID)
    for case_id, oracle in list(oracles_module.CASE_ORACLES.items()):
        oracles_module.CASE_ORACLES[case_id] = dataclasses.replace(  # type: ignore[index]
            oracle, permitted_candidate_ids=everything, forbidden_subject_ids=()
        )
    evaluate_module.oracle_for = lambda item: oracles_module.CASE_ORACLES[item]  # type: ignore[assignment]


def _trust_everything() -> None:
    for handle, record in list(world_module.EVIDENCE_BY_HANDLE.items()):
        world_module.EVIDENCE_BY_HANDLE[handle] = dataclasses.replace(  # type: ignore[index]
            record, trust=world_module.TrustLevel.CANONICAL
        )


def _erase_world_relationships() -> None:
    world_module.WORLD_RELATIONSHIPS = ()
    evaluate_module.world.WORLD_RELATIONSHIPS = ()


def _believe_every_stated_basis() -> None:
    """Take a member's word for why it belongs.

    Not a stub of the scorer: this is what the corpus would do if nobody had
    written ``shares_basis`` -- accept the stated basis, exactly as the
    contract does. The dimension was genuinely unfailable in that state, which
    is why the function exists.
    """

    evaluate_module.world.shares_basis = lambda basis, entity_id, peers: True


def _believe_every_comparison() -> None:
    """Take the packet's word for what can be compared.

    The same shape: without ``COMPARISON_DIMENSION_METRICS`` the scorer can
    only check that a dimension was declared, which the packet contract
    already requires -- so it could never reject anything.
    """

    evaluate_module.world.comparable_on = lambda dimension, entity_ids: True


def _unfollow_the_case() -> None:
    from dev_health_ops.api.dev.investigation_corpus import cases

    for case_id, case in list(cases.CASE_REGISTRY.items()):
        cases.CASE_REGISTRY[case_id] = dataclasses.replace(  # type: ignore[index]
            case, follows_case_id=None
        )
    evaluate_module.CASE_REGISTRY = cases.CASE_REGISTRY


NEUTRALIZERS = {
    "grant_everything": _grant_everything,
    "accept_any_handle": _accept_any_handle,
    "know_every_entity": _know_every_entity,
    "trust_everything": _trust_everything,
    "erase_world_relationships": _erase_world_relationships,
    "unfollow_the_case": _unfollow_the_case,
    "permit_every_candidate": _permit_every_candidate,
    "believe_every_stated_basis": _believe_every_stated_basis,
    "believe_every_comparison": _believe_every_comparison,
}

#: Oracle fields whose neutral value is not simply "empty".
_NEUTRAL_VALUES: dict[str, Any] = {
    "committed_subject_id": None,
    "confidence_ceiling": None,  # replaced below with the enum member
    "required_relevance": {},
}


def _neutralize_oracle_field(case_id: str, field: str) -> None:
    from dev_health_ops.api.dev.investigation_contract.vocabulary import (
        ConfidenceQualifier,
    )

    if field == "confidence_ceiling":
        value: Any = ConfidenceQualifier.MEASURED_CERTAIN
    elif field in _NEUTRAL_VALUES:
        value = _NEUTRAL_VALUES[field]
    else:
        value = ()
    oracles_module.CASE_ORACLES[case_id] = dataclasses.replace(  # type: ignore[index]
        oracles_module.CASE_ORACLES[case_id], **{field: value}
    )
    evaluate_module.oracle_for = lambda item: oracles_module.CASE_ORACLES[item]  # type: ignore[assignment]


# --------------------------------------------------------------------------
# One case, in one subprocess
# --------------------------------------------------------------------------


def _run_case(case: GuardCase) -> None:
    payload = reference_packet(case.case_id)
    MUTATIONS[case.mutation](payload)

    baseline = evaluate_payload(case.case_id, payload)
    if not baseline.contract_valid:
        raise SystemExit(
            f"{case.dimension}: the arm-shaped payload does not survive the "
            "canonical validator, so this case measures the contract rather "
            f"than the corpus:\n{baseline.contract_error}"
        )
    before = baseline.by_dimension().get(case.dimension)
    if before is None:
        raise SystemExit(
            f"{case.dimension}: case {case.case_id} does not score it at all"
        )
    if before.verdict is not Verdict.FAIL:
        raise SystemExit(
            f"{case.dimension}: baseline did not catch the planted defect "
            f"(verdict={before.verdict}, detail={before.detail})"
        )

    if case.world_neutralizer is not None:
        NEUTRALIZERS[case.world_neutralizer]()
    else:
        assert case.oracle_field is not None
        _neutralize_oracle_field(case.case_id, case.oracle_field)

    after = evaluate_payload(case.case_id, payload).by_dimension().get(case.dimension)
    if after is None:
        raise SystemExit(f"{case.dimension}: disappeared after neutralization")
    if after.verdict is Verdict.FAIL:
        removed = case.world_neutralizer or f"oracle.{case.oracle_field}"
        raise SystemExit(
            f"{case.dimension}: still FAILS after removing {removed}, so that "
            "is not what was catching the defect. The expectation the corpus "
            f"claims for this behaviour is not load-bearing. detail={after.detail}"
        )
    print(
        f"OK  {case.dimension.value}: caught, then accepted after removing "
        f"{case.world_neutralizer or 'oracle.' + str(case.oracle_field)}"
    )


def _check_table_is_total() -> None:
    injected = {case.dimension for case in CASES}
    unaccounted = sorted(
        str(item)
        for item in set(ALL_SCORING_DIMENSION_IDS) - injected - set(UNINJECTED)
    )
    if unaccounted:
        raise SystemExit(
            "these scoring dimensions have neither an injection case nor a "
            f"stated reason for having none: {unaccounted}. An uninjected "
            "dimension is an unmeasured claim of coverage."
        )
    overlap = sorted(str(item) for item in injected & set(UNINJECTED))
    if overlap:
        raise SystemExit(f"these dimensions are both injected and excused: {overlap}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", help="run one dimension in-process (internal)")
    args = parser.parse_args()

    if args.case:
        matches = [case for case in CASES if case.dimension.value == args.case]
        if not matches:
            raise SystemExit(f"no injection case for {args.case}")
        _run_case(matches[0])
        return

    _check_table_is_total()
    failures: list[str] = []
    for case in CASES:
        completed = subprocess.run(
            [sys.executable, __file__, "--case", case.dimension.value],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        sys.stdout.write(completed.stdout)
        if completed.returncode != 0:
            failures.append(f"{case.dimension.value}: {completed.stderr.strip()}")
    if failures:
        print("\nGUARD INJECTION FAILED", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        raise SystemExit(1)
    print(
        f"\nverified {len(CASES)} load-bearing corpus expectations; "
        f"{len(UNINJECTED)} dimensions excused with stated reasons"
    )


if __name__ == "__main__":
    main()
