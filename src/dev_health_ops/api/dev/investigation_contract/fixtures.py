"""Golden positive and negative fixtures for the investigation packet.

CHAOS-3615 deliverables 3 and 4.

Every payload is a plain JSON-serializable ``dict``, never a constructed
model — the exporter and the test suite both validate them through
``INVESTIGATION_CONTRACT_MODELS[name].model_validate(payload)``, which is the
only way a fixture can prove the *wire* shape rather than the Python one.
Same convention as ``contract_fixtures_v2``.

The positive fixtures are all slices of **one coherent investigation**: "Why
is the Nightfall Migration still not finished?", answered against a team, a
service, a shared dependency and a review backlog. Deriving each section
fixture from the same scenario rather than authoring eight unrelated
examples means the section goldens cannot drift apart from the packet
golden — the packet embeds the very dicts the section fixtures export.

The negative fixtures are **arm-shaped**, not syntax errors. Each one is a
packet an implementation could plausibly emit — a symptom promoted to
principal driver, a cohort member with no stated basis, a lineage hop
pointing the wrong way — because a validator that only rejects malformed
JSON proves nothing about the behaviours this contract exists to prevent.
Every one of the eleven named fault modes appears here and is exercised by
``tests/api/dev/test_chaos_3615_fault_modes.py``.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

NOW = "2026-08-08T12:00:00Z"
WINDOW_START = "2026-07-09T00:00:00Z"
WINDOW_END = "2026-08-08T00:00:00Z"
AS_OF = "2026-05-08T00:00:00Z"

ORG = "org_fullchaos"
PACKET_ID = "6f1c2a80-3d4e-4b71-9c8a-2e5f7d90ab13"

SUBJECT = "proj_nightfall_migration"
DECOY = "proj_nightfall"
TEAM = "team_platform"
SERVICE = "svc_auth_gateway"
DEPENDENCY = "dep_authlib"

PATH_OWNERSHIP = "path_ownership"
PATH_DEPENDENCY = "path_dependency"

DRIVER_DEPENDENCY = "drv_dependency_stall"
DRIVER_REVIEW = "drv_review_backlog"
DRIVER_CYCLE_TIME = "drv_cycle_time_rising"

EV_SUBJECT = "ev1_" + "a1" * 20
EV_DEPENDENCY = "ev1_" + "b2" * 20
EV_REVIEW = "ev1_" + "c3" * 20
EV_CYCLE_TIME = "ev1_" + "d4" * 20

__all__ = [
    "negative_fixtures",
    "positive_fixtures",
    "positive_variant_fixtures",
]


# --------------------------------------------------------------------------
# Shared building blocks
# --------------------------------------------------------------------------


def _evidence_ref(
    handle: str,
    *,
    source_system: str,
    entity_type: str,
    entity_id: str,
    display_label: str,
    citation_text: str,
) -> dict[str, Any]:
    """A real ``dev_evidence_ref.v1`` body, reusing the existing vocabulary.

    Deliberately the platform's own evidence ref rather than a
    packet-specific one: a handle in this index has to be the same handle
    ``EvidenceHandleService.verify`` would accept, or packet evidence would
    be unverifiable against the service that issues it.
    """

    return {
        "schema_version": "dev_evidence_ref.v1",
        "evidence_ref_id": handle,
        "source_system": source_system,
        "source_version": "work_graph.v1",
        "entity_type": entity_type,
        "entity_id": entity_id,
        "display_label": display_label,
        "link": {
            "internal_path": f"/work/{entity_type}/{entity_id}",
            "source_url": None,
        },
        "observed_at": NOW,
        "freshness": "fresh",
        "provenance": "Canonical work graph projection",
        "confidence": 1.0,
        "citation_text": citation_text,
        "repository_ids": [],
        "valid_entity_ids": [],
        "flags": {},
    }


def _analytical_job() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_analytical_job.v1",
        "job_id": "job_nightfall_status",
        "question_family": "project_status_drivers",
        "job_uncertainty": "precise",
        "job_statement": (
            "Report the current delivery status of the Nightfall Migration "
            "project and identify its principal current drivers."
        ),
        "comparison_shape": "singular_subject",
        "time_context": {
            "start": WINDOW_START,
            "end": WINDOW_END,
            "timezone": "America/Los_Angeles",
            "analytical_slice": "current",
            "as_of": None,
            "historical_comparability": "not_applicable",
        },
        "surface_context_refs": [
            {
                "surface_kind": "project_page",
                "surface_id": "surface_project_overview",
                "entity_kind": "project",
                "entity_id": SUBJECT,
            }
        ],
        "conversation_reference_ids": ["conv_2026_08_08_a"],
        "interpretation_limitations": [],
    }


def _subject_discovery() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_subject_discovery.v1",
        "candidates": [
            {
                "candidate_id": "cand_1",
                "rank": 1,
                "subject_kind": "project",
                "canonical_id": SUBJECT,
                "display_label": "Nightfall Migration",
                "commitment_state": "committed",
                "match_rationale": (
                    "The question's phrase 'the Nightfall migration' matches "
                    "this project's registered display name exactly, and the "
                    "surface the question was asked from is this project's "
                    "overview page."
                ),
                "match_signals": [
                    {
                        "signal": "exact_display_name",
                        "matched_text": "Nightfall Migration",
                        "source_class": "work_graph",
                        "evidence_ref_ids": [EV_SUBJECT],
                    },
                    {
                        "signal": "surface_context_reference",
                        "matched_text": "surface_project_overview",
                        "source_class": "work_graph",
                        "evidence_ref_ids": [],
                    },
                ],
                "match_confidence": 0.97,
                "relevance": "current",
            },
            {
                "candidate_id": "cand_2",
                "rank": 2,
                "subject_kind": "project",
                "canonical_id": DECOY,
                "display_label": "Nightfall",
                "commitment_state": "rejected",
                "match_rationale": (
                    "A similarly named archived project. Retained as a ranked "
                    "candidate so the near-miss is visible rather than "
                    "silently discarded."
                ),
                "match_signals": [
                    {
                        "signal": "fuzzy_label",
                        "matched_text": "Nightfall",
                        "source_class": "work_graph",
                        "evidence_ref_ids": [],
                    }
                ],
                "match_confidence": 0.41,
                "relevance": "historical_only",
            },
        ],
        "unresolved_mentions": [],
        "committed_subject_ids": [SUBJECT],
        "authorization_filtered_count": 0,
        "candidates_truncated": False,
        "truncation_reason": None,
    }


def _comparison_cohort() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_comparison_cohort.v1",
        "cohort_id": "cohort_nightfall_singular",
        "comparison_shape": "singular_subject",
        "members": [
            {
                "subject_kind": "project",
                "canonical_id": SUBJECT,
                "display_label": "Nightfall Migration",
                "inclusion_basis": ["explicitly_named"],
                "inclusion_rationale": (
                    "The question names this project directly; it is the sole "
                    "subject rather than a member of a comparison set."
                ),
                "inclusion_evidence_ids": [],
                "inclusion_evidence_classification": "explicitly_named_by_question",
                "relevance": "current",
            }
        ],
        "exclusions": [
            {
                "subject_kind": "project",
                "canonical_id": DECOY,
                "reason": "ambiguous_identity",
                "rationale": (
                    "Archived project with a near-identical name; excluded so "
                    "the near-miss is recorded rather than silently dropped."
                ),
            }
        ],
        "supported_comparison_dimensions": [],
        "completeness": "complete",
        "truncation_reason": None,
        "cohort_uncertainty": None,
        "authorization_filtered_count": 0,
    }


def _related_context() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_related_context.v1",
        "entities": [
            {
                "entity_id": TEAM,
                "entity_kind": "team",
                "display_label": "Platform",
                "inclusion_reason": (
                    "Owns the project; the team whose review capacity gates "
                    "the remaining work."
                ),
                "supporting_path_ids": [PATH_OWNERSHIP],
                "relevance": "current",
                "observed_at": NOW,
            },
            {
                "entity_id": SERVICE,
                "entity_kind": "service",
                "display_label": "Auth Gateway",
                "inclusion_reason": (
                    "The project's remaining work all lands in this service."
                ),
                "supporting_path_ids": [PATH_DEPENDENCY],
                "relevance": "current",
                "observed_at": NOW,
            },
            {
                "entity_id": DEPENDENCY,
                "entity_kind": "dependency",
                "display_label": "authlib",
                "inclusion_reason": (
                    "Shared dependency the gateway is blocked behind; the "
                    "terminal node of the driver's lineage."
                ),
                "supporting_path_ids": [PATH_DEPENDENCY],
                "relevance": "current",
                "observed_at": NOW,
            },
        ],
        "paths": [
            {
                "path_id": PATH_OWNERSHIP,
                "origin_entity_id": SUBJECT,
                "terminal_entity_id": TEAM,
                "hops": [
                    {
                        "source_entity_id": SUBJECT,
                        "source_entity_kind": "project",
                        "relationship": "owned_by_team",
                        "direction": "forward",
                        "target_entity_id": TEAM,
                        "target_entity_kind": "team",
                        "observed_at": NOW,
                        "relevance": "current",
                    }
                ],
                "inclusion_reason": (
                    "Establishes which team's review capacity applies to the "
                    "project's outstanding changes."
                ),
                "relevance": "current",
                "evidence_ref_ids": [EV_REVIEW],
                "truncated": False,
                "truncation_reason": None,
                "source_health": "available_current",
            },
            {
                "path_id": PATH_DEPENDENCY,
                "origin_entity_id": SUBJECT,
                "terminal_entity_id": DEPENDENCY,
                "hops": [
                    {
                        "source_entity_id": SUBJECT,
                        "source_entity_kind": "project",
                        "relationship": "depends_on",
                        "direction": "forward",
                        "target_entity_id": SERVICE,
                        "target_entity_kind": "service",
                        "observed_at": NOW,
                        "relevance": "current",
                    },
                    {
                        "source_entity_id": SERVICE,
                        "source_entity_kind": "service",
                        "relationship": "depends_on",
                        "direction": "forward",
                        "target_entity_id": DEPENDENCY,
                        "target_entity_kind": "dependency",
                        "observed_at": NOW,
                        "relevance": "current",
                    },
                ],
                "inclusion_reason": (
                    "The two-hop chain from the project to the stalled shared "
                    "dependency; the mechanism behind the principal driver."
                ),
                "relevance": "current",
                "evidence_ref_ids": [EV_DEPENDENCY],
                "truncated": False,
                "truncation_reason": None,
                "source_health": "available_current",
            },
        ],
        "authorized_entity_ids": [SUBJECT, TEAM, SERVICE, DEPENDENCY],
        "authorization_filtered_count": 0,
        "entities_truncated": False,
        "paths_truncated": False,
        "truncation_reason": None,
    }


def _driver_analysis() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_driver_analysis.v1",
        "candidates": [
            {
                "driver_id": DRIVER_DEPENDENCY,
                "category": "dependency_pressure",
                "summary": (
                    "The migration's remaining work is blocked behind an "
                    "unreleased authlib change the gateway depends on."
                ),
                "affected_subject_ids": [SUBJECT, SERVICE],
                "role": "driver",
                "standing": "principal_driver",
                "assertion_basis": "measured",
                "confidence_qualifier": "measured_certain",
                "supporting_path_ids": [PATH_DEPENDENCY],
                "supporting_evidence_ids": [EV_DEPENDENCY],
                "conflicting_evidence_ids": [],
                "conflict_note": None,
                "relevance": "current",
                "exclusion_reason": None,
                "staffing_qualification": None,
            },
            {
                "driver_id": DRIVER_REVIEW,
                "category": "review_pressure",
                "summary": (
                    "The owning team's review queue has grown faster than it "
                    "is being drained, extending the tail of every change."
                ),
                "affected_subject_ids": [SUBJECT, TEAM],
                "role": "driver",
                "standing": "contributing_driver",
                "assertion_basis": "measured",
                "confidence_qualifier": "qualified",
                "supporting_path_ids": [PATH_OWNERSHIP],
                "supporting_evidence_ids": [EV_REVIEW],
                "conflicting_evidence_ids": [],
                "conflict_note": None,
                "relevance": "current",
                "exclusion_reason": None,
                "staffing_qualification": None,
            },
            {
                "driver_id": DRIVER_CYCLE_TIME,
                "category": "delivery_pressure",
                "summary": (
                    "Median cycle time on the project has risen over the window."
                ),
                "affected_subject_ids": [SUBJECT],
                "role": "symptom",
                "standing": "excluded",
                "assertion_basis": "measured",
                "confidence_qualifier": "qualified",
                "supporting_path_ids": [],
                "supporting_evidence_ids": [EV_CYCLE_TIME],
                "conflicting_evidence_ids": [],
                "conflict_note": None,
                "relevance": "current",
                "exclusion_reason": "symptom_of_another_candidate",
                "staffing_qualification": None,
            },
        ],
        "principal_driver_ids": [DRIVER_DEPENDENCY],
        "candidates_truncated": False,
        "truncation_reason": None,
    }


def _evidence_coverage() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_evidence_coverage.v1",
        "evidence_index": [
            {
                "evidence": _evidence_ref(
                    EV_SUBJECT,
                    source_system="work_graph",
                    entity_type="project",
                    entity_id=SUBJECT,
                    display_label="Nightfall Migration registration",
                    citation_text="Project registered under this display name.",
                ),
                "source_class": "work_graph",
                "supports_path_ids": [],
                "supports_entity_ids": [],
                "supports_driver_ids": [],
                "supports_subject_ids": [SUBJECT],
                "relevance": "current",
            },
            {
                "evidence": _evidence_ref(
                    EV_DEPENDENCY,
                    source_system="work_graph",
                    entity_type="dependency",
                    entity_id=DEPENDENCY,
                    display_label="authlib release status",
                    citation_text="Dependency has no released version carrying the change.",
                ),
                "source_class": "work_graph",
                "supports_path_ids": [PATH_DEPENDENCY],
                "supports_entity_ids": [SERVICE, DEPENDENCY],
                "supports_driver_ids": [DRIVER_DEPENDENCY],
                "supports_subject_ids": [],
                "relevance": "current",
            },
            {
                "evidence": _evidence_ref(
                    EV_REVIEW,
                    source_system="review",
                    entity_type="team",
                    entity_id=TEAM,
                    display_label="Platform review queue",
                    citation_text="Open review count exceeded the drain rate for the window.",
                ),
                "source_class": "review",
                "supports_path_ids": [PATH_OWNERSHIP],
                "supports_entity_ids": [TEAM],
                "supports_driver_ids": [DRIVER_REVIEW],
                "supports_subject_ids": [],
                "relevance": "current",
            },
            {
                "evidence": _evidence_ref(
                    EV_CYCLE_TIME,
                    source_system="work_item",
                    entity_type="project",
                    entity_id=SUBJECT,
                    display_label="Cycle time p50",
                    citation_text="Median cycle time rose over the window.",
                ),
                "source_class": "work_item",
                "supports_path_ids": [],
                "supports_entity_ids": [],
                "supports_driver_ids": [DRIVER_CYCLE_TIME],
                "supports_subject_ids": [],
                "relevance": "current",
            },
        ],
        "source_health": [
            {
                "source_class": "work_graph",
                "state": "available_current",
                "observed_at": NOW,
                "detail": None,
            },
            {
                "source_class": "review",
                "state": "available_current",
                "observed_at": NOW,
                "detail": None,
            },
            {
                "source_class": "deployment",
                "state": "available_stale",
                "observed_at": NOW,
                "detail": "Last deployment sync completed 31 hours ago.",
            },
        ],
        "missing_sources": [
            {
                "source_class": "investment_allocation",
                "state": "unconfigured",
                "impact": (
                    "No allocation denominator is available, so no staffing "
                    "claim is made about this project."
                ),
            }
        ],
        "conflicts": [],
        "limitations": [
            {
                "kind": "missing_source",
                "detail": (
                    "Investment allocation is unconfigured for this "
                    "organization; capacity statements are out of scope for "
                    "this packet."
                ),
            },
            {
                "kind": "stale_source",
                "detail": (
                    "Deployment evidence is 31 hours old; a release landing "
                    "since then would not be reflected."
                ),
            },
        ],
        "clarification_needs": [],
        "authorization_filtered_count": 0,
        "evidence_truncated": False,
        "truncation_reason": None,
    }


def _versions() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_investigation_versions.v1",
        "packet_schema_version": "ask_dev_investigation_packet.v1",
        "query_version": "ask_dev_investigation_queries.v1",
        "ranking_version": "ask_dev_investigation_ranking.v1",
        "projection_version": "work_graph_projection.v1",
        "source_contract_versions": [
            {"source_class": "work_graph", "contract_version": "work_graph.v1"},
            {"source_class": "review", "contract_version": "review_metrics.v1"},
            {"source_class": "work_item", "contract_version": "work_item.v1"},
        ],
        "corpus_version": None,
        "trial": None,
    }


def _packet() -> dict[str, Any]:
    return {
        "schema_version": "ask_dev_investigation_packet.v1",
        "packet_id": PACKET_ID,
        "organization_id": ORG,
        "produced_at": NOW,
        "outcome": "supported",
        "analytical_job": _analytical_job(),
        "subject_discovery": _subject_discovery(),
        "comparison_cohort": _comparison_cohort(),
        "related_context": _related_context(),
        "driver_analysis": _driver_analysis(),
        "evidence_coverage": _evidence_coverage(),
        "versions": _versions(),
    }


def positive_fixtures() -> dict[str, dict[str, Any]]:
    """One golden per registered contract, all slices of the same scenario."""

    packet = _packet()
    return {
        "ask_dev_investigation_packet.v1": packet,
        "ask_dev_analytical_job.v1": deepcopy(packet["analytical_job"]),
        "ask_dev_subject_discovery.v1": deepcopy(packet["subject_discovery"]),
        "ask_dev_comparison_cohort.v1": deepcopy(packet["comparison_cohort"]),
        "ask_dev_related_context.v1": deepcopy(packet["related_context"]),
        "ask_dev_driver_analysis.v1": deepcopy(packet["driver_analysis"]),
        "ask_dev_evidence_coverage.v1": deepcopy(packet["evidence_coverage"]),
        "ask_dev_investigation_versions.v1": deepcopy(packet["versions"]),
    }


# --------------------------------------------------------------------------
# Positive variants: the anti-drift rules, as packets that must VALIDATE
# --------------------------------------------------------------------------


def _discovery_without_commitment_packet() -> dict[str, Any]:
    """Context discovery with no committed subject at all.

    The correction addendum forbids requiring exact subject commitment
    before authorized candidate and context discovery. This variant is the
    positive control for that rule: a full related-context and driver
    section with every candidate merely ``PROPOSED``.
    """

    packet = _packet()
    for candidate in packet["subject_discovery"]["candidates"]:
        candidate["commitment_state"] = "proposed"
    packet["subject_discovery"]["committed_subject_ids"] = []
    packet["outcome"] = "supported_with_gaps"
    packet["evidence_coverage"]["limitations"].append(
        {
            "kind": "interpretation_uncertainty",
            "detail": (
                "The subject was not committed; findings are reported against "
                "the top-ranked candidate and remain provisional."
            ),
        }
    )
    packet["analytical_job"]["job_uncertainty"] = "broad_with_uncertainty"
    packet["analytical_job"]["interpretation_limitations"] = [
        {
            "kind": "interpretation_uncertainty",
            "detail": (
                "The question named a project ambiguously; the job was widened "
                "to 'status and drivers of the best-matching project'."
            ),
        }
    ]
    return packet


def _not_comparable_historical_packet() -> dict[str, Any]:
    """A historical slice CHAOS-3569 cannot reconstruct — still SUPPORTED.

    The corrective plan's ruling in fixture form: gapped historical rows are
    NOT COMPARABLE, not blockers. This packet declares
    ``not_comparable_missing_edge_validity``, discloses the limitation, and
    remains a valid, supported investigation.
    """

    packet = _packet()
    packet["analytical_job"]["time_context"] = {
        "start": AS_OF,
        "end": WINDOW_END,
        "timezone": "America/Los_Angeles",
        "analytical_slice": "current_vs_historical",
        "as_of": AS_OF,
        "historical_comparability": "not_comparable_missing_edge_validity",
    }
    packet["evidence_coverage"]["limitations"].append(
        {
            "kind": "historical_slice_not_comparable",
            "detail": (
                "Relationship edges carry no validity interval, so the as-of "
                "state cannot be reconstructed; the historical half of this "
                "comparison is reported NOT COMPARABLE rather than as no "
                "change."
            ),
        }
    )
    return packet


def _qualified_capacity_packet() -> dict[str, Any]:
    """A capacity claim with no allocation denominator — qualified, not killed.

    The mirror image of the staffing fault mode, and the reason it needs a
    positive control: a missing denominator must *reduce confidence*, not
    make capacity questions unsupported. This packet asserts a capacity
    driver at ``QUALIFIED`` confidence with ``denominator_absent`` and is
    valid.
    """

    packet = _packet()
    packet["analytical_job"]["question_family"] = "project_capacity"
    packet["driver_analysis"]["candidates"][1] = {
        "driver_id": DRIVER_REVIEW,
        "category": "capacity_or_staffing",
        "summary": (
            "The owning team appears capacity-constrained relative to the "
            "outstanding work, inferred from review queue growth and work in "
            "progress rather than from an allocation denominator."
        ),
        "affected_subject_ids": [SUBJECT, TEAM],
        "role": "driver",
        "standing": "contributing_driver",
        "assertion_basis": "inferred",
        "confidence_qualifier": "qualified",
        "supporting_path_ids": [PATH_OWNERSHIP],
        "supporting_evidence_ids": [EV_REVIEW],
        "conflicting_evidence_ids": [],
        "conflict_note": None,
        "relevance": "current",
        "exclusion_reason": None,
        "staffing_qualification": {
            "denominator_state": "denominator_absent",
            "denominator_source_classes": [],
            "qualification_note": (
                "No allocation or headcount denominator is configured. The "
                "capacity judgment is inferred from queue growth and WIP and "
                "is reported as qualified rather than withheld."
            ),
        },
    }
    packet["evidence_coverage"]["limitations"].append(
        {
            "kind": "absent_staffing_denominator",
            "detail": (
                "No allocation denominator exists for this organization; the "
                "capacity statement is qualified accordingly and is not a "
                "measured claim."
            ),
        }
    )
    return packet


def _needs_clarification_packet() -> dict[str, Any]:
    """The safe no-match/clarification shape, including the one legal widening.

    Organization scope with an unresolved mention is legal in exactly one
    case — when the packet is *asking* rather than answering. This variant
    is that case, and it is the positive control paired with
    ``organization_widening_after_unresolved_reference``.
    """

    packet = _packet()
    packet["outcome"] = "needs_clarification"
    packet["analytical_job"]["question_family"] = "clarification_and_no_match"
    packet["analytical_job"]["comparison_shape"] = "organization_wide"
    packet["analytical_job"]["job_uncertainty"] = "ambiguous"
    packet["analytical_job"]["job_statement"] = (
        "Report what is going sideways; no subject was named."
    )
    packet["analytical_job"]["interpretation_limitations"] = [
        {
            "kind": "interpretation_uncertainty",
            "detail": "The question named no subject and no time context.",
        }
    ]
    packet["subject_discovery"]["candidates"][0]["commitment_state"] = "ambiguous"
    packet["subject_discovery"]["committed_subject_ids"] = []
    packet["subject_discovery"]["unresolved_mentions"] = [
        {
            "mention_id": "mention_1",
            "mention_text": "the nightfall thing",
            "reason": "multiple_candidates",
            "candidate_ids": ["cand_1", "cand_2"],
        }
    ]
    packet["comparison_cohort"]["comparison_shape"] = "organization_wide"
    packet["comparison_cohort"]["members"] = [
        {
            "subject_kind": "project",
            "canonical_id": SUBJECT,
            "display_label": "Nightfall Migration",
            "inclusion_basis": ["explicitly_named"],
            "inclusion_rationale": "One of two candidates for the named reference.",
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "explicitly_named_by_question",
            "relevance": "current",
        },
        {
            "subject_kind": "project",
            "canonical_id": DECOY,
            "display_label": "Nightfall",
            "inclusion_basis": ["explicitly_named"],
            "inclusion_rationale": "The other candidate for the named reference.",
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": "explicitly_named_by_question",
            "relevance": "historical_only",
        },
    ]
    packet["comparison_cohort"]["exclusions"] = []
    packet["comparison_cohort"]["supported_comparison_dimensions"] = [
        "delivery_throughput"
    ]
    for candidate in packet["driver_analysis"]["candidates"]:
        candidate["standing"] = "candidate_only"
        candidate["exclusion_reason"] = None
    packet["driver_analysis"]["principal_driver_ids"] = []
    packet["evidence_coverage"]["clarification_needs"] = [
        {
            "kind": "ambiguous_subject",
            "prompt": (
                "Two projects match 'nightfall'. Did you mean the Nightfall "
                "Migration, or the archived Nightfall project?"
            ),
            "candidate_ids": ["cand_1", "cand_2"],
        }
    ]
    return packet


def _trial_metadata_packet() -> dict[str, Any]:
    """The same packet with trial metadata attached.

    Proves arm identity is *addable* without being required, and that no
    other field changes when it is present — the structural meaning of "arm
    identity is evaluation metadata, not product truth".
    """

    packet = _packet()
    packet["versions"]["trial"] = {
        "arm_id": "arm_under_test",
        "producer_id": "investigation_arm.v1",
        "fixture_version": "chaos_3616_corpus.v1",
        "run_id": "0f9d3c21-7b45-4c8e-9a10-5d6e7f801234",
    }
    packet["versions"]["corpus_version"] = "chaos_3616_corpus.v1"
    return packet


def positive_variant_fixtures() -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Extra packets that MUST validate — the anti-drift positive controls.

    Every named anti-drift rule that could be "enforced" by simply making
    the legitimate case impossible has a variant here. A contract that
    rejected the bad shape *and* the good one would pass every negative
    test while being useless.
    """

    return {
        "ask_dev_investigation_packet.v1": [
            ("discovery_without_commitment", _discovery_without_commitment_packet()),
            ("not_comparable_historical_slice", _not_comparable_historical_packet()),
            ("qualified_capacity_without_denominator", _qualified_capacity_packet()),
            ("needs_clarification_with_widening", _needs_clarification_packet()),
            ("trial_metadata_present", _trial_metadata_packet()),
        ]
    }


# --------------------------------------------------------------------------
# Negative fixtures: arm-shaped bad packets, one per named fault mode
# --------------------------------------------------------------------------


def _fault_commitment_on_fuzzy_label() -> dict[str, Any]:
    """FAULT: wrong but similarly named subject ranked and committed first."""

    discovery = _subject_discovery()
    top = discovery["candidates"][0]
    top["canonical_id"] = DECOY
    top["display_label"] = "Nightfall"
    top["match_signals"] = [
        {
            "signal": "fuzzy_label",
            "matched_text": "Nightfall",
            "source_class": "work_graph",
            "evidence_ref_ids": [],
        }
    ]
    discovery["candidates"][1]["canonical_id"] = SUBJECT
    discovery["candidates"][1]["display_label"] = "Nightfall Migration"
    discovery["committed_subject_ids"] = [DECOY]
    return discovery


def _fault_committed_below_rank_one() -> dict[str, Any]:
    """FAULT: the committed subject is not the one the arm ranked first."""

    discovery = _subject_discovery()
    discovery["candidates"][0]["commitment_state"] = "proposed"
    discovery["candidates"][1]["commitment_state"] = "committed"
    discovery["candidates"][1]["match_signals"] = [
        {
            "signal": "exact_display_name",
            "matched_text": "Nightfall",
            "source_class": "work_graph",
            "evidence_ref_ids": [],
        }
    ]
    discovery["committed_subject_ids"] = [DECOY]
    return discovery


def _fault_ranks_out_of_order() -> dict[str, Any]:
    discovery = _subject_discovery()
    discovery["candidates"][0]["rank"] = 2
    discovery["candidates"][1]["rank"] = 1
    return discovery


def _fault_missing_truncation_flag() -> dict[str, Any]:
    """FAULT: an absent required disclosure field, silently defaulting."""

    discovery = _subject_discovery()
    del discovery["candidates_truncated"]
    return discovery


def _fault_organization_widening() -> dict[str, Any]:
    """FAULT: org-wide sweep after an unresolved named reference."""

    packet = _needs_clarification_packet()
    packet["outcome"] = "supported"
    packet["evidence_coverage"]["clarification_needs"] = []
    packet["driver_analysis"]["candidates"][0]["standing"] = "principal_driver"
    packet["driver_analysis"]["principal_driver_ids"] = [DRIVER_DEPENDENCY]
    return packet


def _fault_evidence_supports_nothing() -> dict[str, Any]:
    """FAULT: high-volume evidence attached to nothing, displacing lineage."""

    coverage = _evidence_coverage()
    coverage["evidence_index"].append(
        {
            "evidence": _evidence_ref(
                "ev1_" + "e5" * 20,
                source_system="code_change",
                entity_type="repository",
                entity_id="repo_dev_health",
                display_label="Recent commits",
                citation_text="1,482 commits in the window.",
            ),
            "source_class": "code_change",
            "supports_path_ids": [],
            "supports_entity_ids": [],
            "supports_driver_ids": [],
            "supports_subject_ids": [],
            "relevance": "current",
        }
    )
    return coverage


def _fault_dangling_evidence_citation() -> dict[str, Any]:
    """FAULT: a driver cites evidence that is nowhere in the index."""

    packet = _packet()
    packet["driver_analysis"]["candidates"][0]["supporting_evidence_ids"] = [
        "ev1_" + "f6" * 20
    ]
    return packet


def _fault_symptom_as_principal_driver() -> dict[str, Any]:
    """FAULT: a symptom promoted to principal driver with no lineage."""

    analysis = _driver_analysis()
    symptom = analysis["candidates"][2]
    symptom["standing"] = "principal_driver"
    symptom["exclusion_reason"] = None
    analysis["principal_driver_ids"] = [DRIVER_DEPENDENCY, DRIVER_CYCLE_TIME]
    return analysis


def _fault_principal_driver_without_path() -> dict[str, Any]:
    """FAULT: a real driver promoted to principal with no supporting path."""

    analysis = _driver_analysis()
    analysis["candidates"][0]["supporting_path_ids"] = []
    return analysis


def _fault_historical_driver_as_principal() -> dict[str, Any]:
    analysis = _driver_analysis()
    analysis["candidates"][0]["relevance"] = "historical_only"
    return analysis


def _fault_certain_staffing_without_denominator() -> dict[str, Any]:
    """FAULT: a staffing claim presented as certain with no denominator."""

    analysis = _driver_analysis()
    analysis["candidates"][1] = {
        "driver_id": DRIVER_REVIEW,
        "category": "capacity_or_staffing",
        "summary": "The team is understaffed for the remaining work.",
        "affected_subject_ids": [SUBJECT, TEAM],
        "role": "driver",
        "standing": "contributing_driver",
        "assertion_basis": "measured",
        "confidence_qualifier": "measured_certain",
        "supporting_path_ids": [PATH_OWNERSHIP],
        "supporting_evidence_ids": [EV_REVIEW],
        "conflicting_evidence_ids": [],
        "conflict_note": None,
        "relevance": "current",
        "exclusion_reason": None,
        "staffing_qualification": {
            "denominator_state": "denominator_absent",
            "denominator_source_classes": [],
            "qualification_note": "No allocation data.",
        },
    }
    return analysis


def _fault_staffing_claim_without_qualification() -> dict[str, Any]:
    """FAULT: a capacity driver that says nothing about its denominator."""

    analysis = _driver_analysis()
    analysis["candidates"][1]["category"] = "capacity_or_staffing"
    analysis["candidates"][1]["staffing_qualification"] = None
    return analysis


def _fault_unrelated_cohort_member() -> dict[str, Any]:
    """FAULT: a cohort member with neither evidence nor a classification."""

    cohort = _comparison_cohort()
    cohort["comparison_shape"] = "discovered_cohort"
    cohort["supported_comparison_dimensions"] = ["delivery_throughput"]
    cohort["members"].append(
        {
            "subject_kind": "project",
            "canonical_id": "proj_unrelated_billing",
            "display_label": "Billing Revamp",
            "inclusion_basis": ["comparable_delivery_profile"],
            "inclusion_rationale": "Also a project.",
            "inclusion_evidence_ids": [],
            "inclusion_evidence_classification": None,
            "relevance": "current",
        }
    )
    return cohort


def _fault_cohort_without_comparison_dimensions() -> dict[str, Any]:
    """FAULT: a cohort claiming comparison while declaring no dimension."""

    cohort = _comparison_cohort()
    cohort["comparison_shape"] = "discovered_cohort"
    cohort["members"].append(
        {
            "subject_kind": "project",
            "canonical_id": "proj_atlas",
            "display_label": "Atlas",
            "inclusion_basis": ["shared_dependency"],
            "inclusion_rationale": "Depends on the same stalled dependency.",
            "inclusion_evidence_ids": [EV_DEPENDENCY],
            "inclusion_evidence_classification": None,
            "relevance": "current",
        }
    )
    cohort["supported_comparison_dimensions"] = []
    return cohort


def _fault_cohort_truncated_without_reason() -> dict[str, Any]:
    cohort = _comparison_cohort()
    cohort["completeness"] = "truncated"
    return cohort


def _fault_reversed_relationship() -> dict[str, Any]:
    """FAULT: 'project owns team' emitted as a forward ownership edge."""

    context = _related_context()
    hop = context["paths"][0]["hops"][0]
    hop["source_entity_id"] = TEAM
    hop["source_entity_kind"] = "team"
    hop["target_entity_id"] = SUBJECT
    hop["target_entity_kind"] = "project"
    context["paths"][0]["origin_entity_id"] = TEAM
    context["paths"][0]["terminal_entity_id"] = SUBJECT
    return context


def _fault_unauthorized_path_entity() -> dict[str, Any]:
    """FAULT: a lineage path routed through an entity outside the authorized set."""

    context = _related_context()
    context["authorized_entity_ids"] = [SUBJECT, TEAM, SERVICE]
    return context


def _fault_disconnected_path() -> dict[str, Any]:
    context = _related_context()
    context["paths"][1]["hops"][1]["source_entity_id"] = "svc_unrelated"
    context["paths"][1]["hops"][1]["source_entity_kind"] = "service"
    context["authorized_entity_ids"].append("svc_unrelated")
    return context


def _fault_dashboard_redirect_as_answer() -> dict[str, Any]:
    """FAULT: SUPPORTED with evidence and surfaces but no asserted driver."""

    packet = _packet()
    for candidate in packet["driver_analysis"]["candidates"]:
        candidate["standing"] = "candidate_only"
        candidate["exclusion_reason"] = None
    packet["driver_analysis"]["principal_driver_ids"] = []
    return packet


def _fault_no_match_without_explanation() -> dict[str, Any]:
    """FAULT: a silent no-match — the privileged default wearing a label."""

    packet = _packet()
    packet["outcome"] = "no_match"
    for candidate in packet["subject_discovery"]["candidates"]:
        candidate["commitment_state"] = "proposed"
    packet["subject_discovery"]["committed_subject_ids"] = []
    for candidate in packet["driver_analysis"]["candidates"]:
        candidate["standing"] = "candidate_only"
        candidate["exclusion_reason"] = None
    packet["driver_analysis"]["principal_driver_ids"] = []
    packet["evidence_coverage"]["limitations"] = []
    packet["evidence_coverage"]["missing_sources"] = []
    packet["evidence_coverage"]["clarification_needs"] = []
    return packet


def _fault_undisclosed_authorization_filtering() -> dict[str, Any]:
    packet = _packet()
    packet["related_context"]["authorization_filtered_count"] = 4
    return packet


def _fault_undisclosed_not_comparable_slice() -> dict[str, Any]:
    packet = _not_comparable_historical_packet()
    packet["evidence_coverage"]["limitations"] = [
        limitation
        for limitation in packet["evidence_coverage"]["limitations"]
        if limitation["kind"] != "historical_slice_not_comparable"
    ]
    return packet


def _fault_uncertain_job_without_limitations() -> dict[str, Any]:
    job = _analytical_job()
    job["job_uncertainty"] = "broad_with_uncertainty"
    job["interpretation_limitations"] = []
    return job


def _fault_current_slice_with_as_of() -> dict[str, Any]:
    job = _analytical_job()
    job["time_context"]["as_of"] = AS_OF
    return job


def _fault_historical_slice_without_as_of() -> dict[str, Any]:
    job = _analytical_job()
    job["time_context"]["analytical_slice"] = "historical"
    job["time_context"]["historical_comparability"] = "comparable"
    return job


def _fault_coverage_missing_source_undisclosed() -> dict[str, Any]:
    coverage = _evidence_coverage()
    coverage["limitations"] = [
        limitation
        for limitation in coverage["limitations"]
        if limitation["kind"] != "missing_source"
    ]
    return coverage


def _fault_coverage_conflict_not_indexed() -> dict[str, Any]:
    coverage = _evidence_coverage()
    coverage["conflicts"] = [
        {
            "conflict_id": "conflict_1",
            "evidence_ref_ids": ["ev1_" + "07" * 20, "ev1_" + "08" * 20],
            "description": "Two sources disagree on the release state.",
            "resolution": "unresolved",
        }
    ]
    coverage["limitations"].append(
        {"kind": "conflicting_evidence", "detail": "Release state disputed."}
    )
    return coverage


def _fault_versions_without_source_contracts() -> dict[str, Any]:
    versions = _versions()
    versions["source_contract_versions"] = []
    return versions


def _fault_versions_duplicate_source_contract() -> dict[str, Any]:
    versions = _versions()
    versions["source_contract_versions"].append(
        {"source_class": "work_graph", "contract_version": "work_graph.v2"}
    )
    return versions


def _fault_analysis_principal_list_mismatch() -> dict[str, Any]:
    analysis = _driver_analysis()
    analysis["principal_driver_ids"] = []
    return analysis


def _fault_context_entity_without_path() -> dict[str, Any]:
    context = _related_context()
    context["entities"][0]["supporting_path_ids"] = ["path_never_declared"]
    return context


def _fault_coverage_unknown_extra_field() -> dict[str, Any]:
    coverage = _evidence_coverage()
    coverage["graph_node_ids"] = ["4:8a1f:12"]
    return coverage


def negative_fixtures() -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Arm-shaped payloads that MUST fail validation, keyed by contract.

    Labels are the fault, not the mechanism, so a reader scanning the
    exported artifact tree sees what the contract refuses rather than which
    validator happens to refuse it.
    """

    return {
        "ask_dev_investigation_packet.v1": [
            (
                "organization_widening_after_unresolved_reference",
                _fault_organization_widening(),
            ),
            (
                "dashboard_redirect_without_direct_judgment",
                _fault_dashboard_redirect_as_answer(),
            ),
            (
                "evidence_citation_absent_from_index",
                _fault_dangling_evidence_citation(),
            ),
            (
                "no_match_without_limitation_or_clarification",
                _fault_no_match_without_explanation(),
            ),
            (
                "authorization_filtering_undisclosed",
                _fault_undisclosed_authorization_filtering(),
            ),
            (
                "not_comparable_slice_undisclosed",
                _fault_undisclosed_not_comparable_slice(),
            ),
        ],
        "ask_dev_analytical_job.v1": [
            (
                "uncertain_job_without_limitations",
                _fault_uncertain_job_without_limitations(),
            ),
            ("current_slice_carrying_as_of", _fault_current_slice_with_as_of()),
            ("historical_slice_without_as_of", _fault_historical_slice_without_as_of()),
        ],
        "ask_dev_subject_discovery.v1": [
            ("commitment_on_fuzzy_label_alone", _fault_commitment_on_fuzzy_label()),
            ("commitment_below_rank_one", _fault_committed_below_rank_one()),
            ("candidate_ranks_out_of_order", _fault_ranks_out_of_order()),
            ("absent_truncation_disclosure_field", _fault_missing_truncation_flag()),
        ],
        "ask_dev_comparison_cohort.v1": [
            (
                "unrelated_member_without_inclusion_evidence",
                _fault_unrelated_cohort_member(),
            ),
            (
                "comparison_claimed_without_dimensions",
                _fault_cohort_without_comparison_dimensions(),
            ),
            ("truncated_without_reason", _fault_cohort_truncated_without_reason()),
        ],
        "ask_dev_related_context.v1": [
            ("reversed_relationship_direction", _fault_reversed_relationship()),
            ("path_crosses_unauthorized_entity", _fault_unauthorized_path_entity()),
            ("disconnected_path_presented_as_chain", _fault_disconnected_path()),
            ("entity_citing_undeclared_path", _fault_context_entity_without_path()),
        ],
        "ask_dev_driver_analysis.v1": [
            (
                "symptom_promoted_to_principal_driver",
                _fault_symptom_as_principal_driver(),
            ),
            (
                "principal_driver_without_supporting_path",
                _fault_principal_driver_without_path(),
            ),
            (
                "historical_driver_promoted_to_principal",
                _fault_historical_driver_as_principal(),
            ),
            (
                "staffing_certainty_without_denominator",
                _fault_certain_staffing_without_denominator(),
            ),
            (
                "capacity_driver_without_staffing_qualification",
                _fault_staffing_claim_without_qualification(),
            ),
            (
                "principal_list_contradicts_standings",
                _fault_analysis_principal_list_mismatch(),
            ),
        ],
        "ask_dev_evidence_coverage.v1": [
            ("evidence_supporting_nothing", _fault_evidence_supports_nothing()),
            (
                "missing_source_undisclosed",
                _fault_coverage_missing_source_undisclosed(),
            ),
            (
                "conflict_citing_unindexed_evidence",
                _fault_coverage_conflict_not_indexed(),
            ),
            (
                "graph_native_field_smuggled_as_extra",
                _fault_coverage_unknown_extra_field(),
            ),
        ],
        "ask_dev_investigation_versions.v1": [
            ("no_source_contract_versions", _fault_versions_without_source_contracts()),
            (
                "duplicate_source_contract_version",
                _fault_versions_duplicate_source_contract(),
            ),
        ],
    }
