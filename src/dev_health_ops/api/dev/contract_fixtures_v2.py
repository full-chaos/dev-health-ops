"""Deterministic golden payloads for the Ask Dev Wave 3.1 v2 contracts.

Mirrors ``dev_health_ops.api.dev.contract_fixtures`` (v1): every function
here returns plain JSON-serializable dicts (not constructed Pydantic
instances), since ``export_contracts_v2`` and the test suite both validate
these payloads through ``CONTRACT_MODELS_V2[name].model_validate(payload)``.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .contracts_v2.validators import CANONICAL_NO_ANSWER_COPY

NOW = "2026-07-28T12:00:00Z"
START = "2026-06-28T00:00:00Z"
END = "2026-07-28T00:00:00Z"


def _time_range(start: str = START, end: str = END) -> dict[str, Any]:
    return {"start": start, "end": end, "timezone": "America/Los_Angeles"}


def _scope() -> dict[str, Any]:
    return {
        "schema_version": "dev_scope.v1",
        "organization_id": "org_fullchaos",
        "direct_scope": "repository",
        "repositories": ["repo_dev_health"],
        "entity_refs": [],
        "team_ids": [],
        "time_range": _time_range(),
        "comparison_range": None,
        "surface_context": None,
    }


def _entity_ref() -> dict[str, Any]:
    return {
        "entity_kind": "repository",
        "entity_id": "repo_dev_health",
        "display_label": "full-chaos/dev-health",
        "repository_id": None,
        "team_id": None,
    }


def _evidence() -> dict[str, Any]:
    return {
        "schema_version": "dev_evidence_ref.v1",
        "evidence_ref_id": "ev1_a2bc440cf82f6979884d6d486dacdc900744d04b",
        "source_system": "work_graph",
        "source_version": "work_graph.v1",
        "entity_type": "work_item",
        "entity_id": "item_01",
        "display_label": "Status snapshot",
        "link": {"internal_path": "/work/items/item_01", "source_url": None},
        "observed_at": NOW,
        "freshness": "fresh",
        "provenance": "Canonical work graph projection",
        "confidence": 1.0,
        "citation_text": "Repository status observed directly.",
        "repository_ids": ["repo_dev_health"],
        "valid_entity_ids": ["item_01"],
        "flags": {
            "stale": False,
            "unavailable": False,
            "redacted": False,
            "deleted": False,
            "uncertain": False,
            "conflicting": False,
            "untrusted_content": True,
        },
    }


def _clarification_candidate(
    *,
    entity_id: str,
    display_label: str,
    entity_kind: str = "repository",
) -> dict[str, Any]:
    """CHAOS-3325: one real, authorized ``DevResolutionCandidate``-shaped
    entry -- the same shape ``dev_resolution_ledger.v1``'s own candidates
    already use (see ``_resolution_entry``), reused here rather than a
    separate fixture vocabulary."""

    return {
        "entity_ref": {
            "entity_kind": entity_kind,
            "entity_id": entity_id,
            "display_label": display_label,
            "repository_id": None,
            "team_id": None,
        },
        "reason": "Multiple authorized entities match this name.",
    }


def _relationship_path() -> dict[str, Any]:
    return {
        "path_id": "path_01",
        "source_entity_id": "repo_dev_health",
        "relationship": "has_status",
        "target_entity_id": "status_snapshot_01",
        "provenance": "Canonical status snapshot",
        "confidence": 1.0,
        "observed_at": NOW,
        "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
    }


def _mention() -> dict[str, Any]:
    return {
        "schema_version": "dev_subject_mention.v1",
        "mention_id": "0f1a2b3c-0009-4a00-8000-000000000001",
        "mention_ordinal": 0,
        "original_text_span": "dev-health",
        "requested_entity_kind": "repository",
        "normalized_lookup_text": "dev-health",
    }


def _resolution_entry() -> dict[str, Any]:
    return {
        "entry_ordinal": 0,
        "mention_id": "0f1a2b3c-0009-4a00-8000-000000000001",
        "outcome": "exact_match",
        "committed_entity_ref": _entity_ref(),
        "candidates": [],
        "repository_attribution": "repo_dev_health",
        "team_attribution": None,
        "resolver_version": "resolver.v1",
        "query_version": "resolve_scope.v1",
        "resolved_at": NOW,
    }


def _ledger() -> dict[str, Any]:
    return {
        "schema_version": "dev_resolution_ledger.v1",
        "ledger_id": "0f1a2b3c-0008-4a00-8000-000000000001",
        "mention_ids": ["0f1a2b3c-0009-4a00-8000-000000000001"],
        "entries": [_resolution_entry()],
        "updated_at": NOW,
    }


def _subject_set() -> dict[str, Any]:
    return {
        "schema_version": "dev_subject_set.v1",
        "set_id": "0f1a2b3c-000a-4a00-8000-000000000001",
        "entity_kind": "repository",
        "committed_entity_refs": [_entity_ref()],
        "original_mention_count": 1,
        "unresolved_mention_ids": [],
        "ambiguous_mention_ids": [],
        "cohort_complete": True,
        "warnings": [],
        "fingerprint": "fp_01",
    }


def _source_requirement() -> dict[str, Any]:
    return {
        "schema_version": "dev_source_requirement.v1",
        "source_class": "status_change",
        "adapter_id": "status_change_service.v1",
        "requirement_level": "mandatory",
        "applicability_rule_id": None,
        "applicability_rule_version": None,
        "freshness_policy": "daily",
        "minimum_usable_facts": 1,
        "minimum_sample": None,
        "minimum_coverage": None,
        "allowed_relationship_paths": ["repository_to_work_unit"],
    }


def _plan() -> dict[str, Any]:
    return {
        "schema_version": "dev_investigation_plan.v1",
        "plan_id": "status.entity.v2",
        "plan_version": "status.entity.v2.1",
        "intent_id": "entity_status",
        "supported_subject_kinds": ["repository"],
        "supported_cardinalities": ["singular"],
        "mandatory_steps": ["status_snapshot"],
        "conditional_steps": [],
        "step_dependencies": [],
        "source_requirements": [_source_requirement()],
        "batch_strategy": "single",
        "per_step_timeout_seconds": 30,
        "max_rows_per_step": 100,
        "max_bytes_per_step": 65_536,
        "max_sample_per_step": None,
        "enrichment_allowed": False,
        "completion_rule_id": "actual_completion",
        "completion_rule_version": "actual_completion.v1",
    }


def _source_observation() -> dict[str, Any]:
    return {
        "schema_version": "dev_source_observation.v1",
        "observation_id": "0f1a2b3c-000c-4a00-8000-000000000001",
        "source_class": "status_change",
        "adapter_id": "status_change_service.v1",
        "requirement_level": "mandatory",
        "observed_state": "available_current",
        "data_semantics": "measured_zero",
        "watermark": NOW,
        "subject_coverage": 1.0,
        "usable_fact_count": 0,
        "sample_count": None,
        "relationship_paths": [_relationship_path()],
        "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
        "limitation": None,
        "observed_at": NOW,
        "query_version": "status_snapshot.v1",
    }


def _investigation_result() -> dict[str, Any]:
    return {
        "schema_version": "dev_investigation_result.v1",
        "result_id": "0f1a2b3c-000b-4a00-8000-000000000001",
        "plan_id": "status.entity.v2",
        "plan_version": "status.entity.v2.1",
        "run_id": "0f1a2b3c-0001-4a00-8000-000000000001",
        "subject_set_fingerprint": None,
        "subject_entity_id": "repo_dev_health",
        "observations": [_source_observation()],
        "completed_steps": ["status_snapshot"],
        "skipped_steps": [],
        "failed_steps": [],
        "relationship_closure_verified": True,
        "completed_at": NOW,
    }


def _fact() -> dict[str, Any]:
    return {
        "fact_id": "fact_01",
        "text": "Repository dev-health has one required child issue still open.",
        "kind": "observed",
        "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
        "relationship_path_ids": ["path_01"],
        "confidence": 1.0,
        "disclosures": [],
    }


def _section() -> dict[str, Any]:
    return {"section_id": "summary", "title": "Summary", "fact_ids": ["fact_01"]}


def _completion() -> dict[str, Any]:
    return {
        "numerator": 3,
        "denominator": 4,
        "rate": 0.75,
        "calculable": True,
        "rule_id": "actual_completion",
        "rule_version": "actual_completion.v1",
    }


def _readiness() -> dict[str, Any]:
    return {
        "state": "not_ready",
        "rule_id": "actual_completion",
        "rule_version": "actual_completion.v1",
        "translated_user_reasons": ["One required child issue is still open."],
        "blocking_fact_ids": ["fact_01"],
    }


def _frame_versions() -> dict[str, Any]:
    return {
        "interpreter_version": "intent_interpreter.v1",
        "plan_id": "status.entity.v2",
        "plan_version": "status.entity.v2.1",
        "tool_contract_version": "ask_dev_tools.v2",
        "metric_definition_version": "ask_dev_metrics.v1",
        "query_version": "ask_dev_queries.v1",
        "prompt_version": None,
        "rule_version": "actual_completion.v1",
    }


def _coverage() -> dict[str, Any]:
    return {
        "required_source_count": 1,
        "available_source_count": 1,
        "unavailable_required_sources": [],
        "stale_required_sources": [],
        "as_of": NOW,
    }


def _frame() -> dict[str, Any]:
    return {
        "schema_version": "dev_answer_frame.v1",
        "frame_id": "0f1a2b3c-0002-4a00-8000-000000000001",
        "run_id": "0f1a2b3c-0001-4a00-8000-000000000001",
        "generated_at": NOW,
        "public_outcome": "answered",
        "subject_ref": _entity_ref(),
        "subject_set_ref": None,
        "clarification_candidates": [],
        "direct_answer": (
            "Repository dev-health is on track; one required child issue remains open."
        ),
        "completion": _completion(),
        "readiness": _readiness(),
        "sections": [_section()],
        "facts": [_fact()],
        "metrics": [],
        "comparisons": [],
        "relationship_paths": [_relationship_path()],
        "health_profile_refs": [],
        "finding_refs": [],
        "deficiency_refs": [],
        "conflicts": [],
        "limitations": [],
        "source_observations": [_source_observation()],
        "coverage": _coverage(),
        "evidence": [_evidence()],
        "safe_follow_up_questions": ["What changed since last week?"],
        "versions": _frame_versions(),
    }


def _narrative() -> dict[str, Any]:
    return {
        "schema_version": "dev_narrative.v1",
        "narrative_id": "0f1a2b3c-0003-4a00-8000-000000000001",
        "run_id": "0f1a2b3c-0001-4a00-8000-000000000001",
        "frame_id": "0f1a2b3c-0002-4a00-8000-000000000001",
        "mode": "deterministic_fallback",
        "body": (
            "Repository dev-health is on track, with one required child issue "
            "still open."
        ),
        "referenced_fact_ids": ["fact_01"],
        "referenced_section_ids": ["summary"],
        "provider_metadata": None,
        "generated_at": NOW,
        "validation_warnings": [],
    }


def _answer() -> dict[str, Any]:
    return {
        "schema_version": "dev_answer.v2",
        "answer_id": "0f1a2b3c-0004-4a00-8000-000000000001",
        "conversation_id": "0f1a2b3c-0005-4a00-8000-000000000001",
        "run_id": "0f1a2b3c-0001-4a00-8000-000000000001",
        "generated_at": NOW,
        "public_outcome": "answered",
        "outcome_display_label": "Answered",
        "frame": _frame(),
        "narrative": _narrative(),
    }


def _intent() -> dict[str, Any]:
    return {
        "schema_version": "dev_question_intent.v1",
        "intent_id": "entity_status",
        "interpreter_version": "intent_interpreter.v1",
        "cardinality": "singular",
        "subject_kinds": ["repository"],
        "mention_ordinals": [0],
        "requested_dimensions": [],
        "requested_metric_ids": [],
        "comparison_mode": "none",
        "ranking_requested": False,
        "confidence": 0.95,
        "interpretation_reasons": ["Exact repository name matched."],
        "requires_clarification": False,
        "clarification_reason": None,
        "client_question_class_hint": None,
        "client_hint_deprecation_warning": None,
        "generated_at": NOW,
    }


def _message_request() -> dict[str, Any]:
    return {
        "schema_version": "dev_message_request.v2",
        "request_id": "web-req-2026-07-31-0001",
        "client_message_id": "web-msg-2026-07-31-0001",
        "conversation_id": "0f1a2b3c-0005-4a00-8000-000000000001",
        "idempotency_key": "idem_01",
        "retry_of_run_id": "0f1a2b3c-0001-4a00-8000-000000000002",
        "question": "How is repo_dev_health doing?",
        "scope": _scope(),
        "requested_metric_ids": [],
        "question_class_hint": None,
    }


def _metric_ref() -> dict[str, Any]:
    """A valid, standalone ``dev_metric_ref.v1`` (v1 type, reused by v2 frames)."""

    return {
        "schema_version": "dev_metric_ref.v1",
        "metric_ref_id": "metric_01",
        "metric_id": "cycle_time_p50_hours",
        "label": "Cycle time (p50)",
        "definition_version": "ask_dev_metrics.v1",
        "unit": "hours",
        "aggregation": "p50",
        "display_precision": 1,
        "resolved_scope": _scope(),
        "dimensions": [],
        "current_window": _time_range(),
        "comparison_window": None,
        "value": 12.5,
        "comparison_value": None,
        "series": [],
        "query_version": "ask_dev_queries.v1",
        "source_version": "work_graph.v1",
        "freshness": "fresh",
        "coverage": 1.0,
        "evidence_ref_ids": [],
    }


def _health_finding() -> dict[str, Any]:
    """A valid, standalone ``health_rule_finding.v1`` (CHAOS-3297 stack #3)."""

    return {
        "schema_version": "health_rule_finding.v1",
        "finding_id": "0f1a2b3c-000a-4a00-8000-000000000001",
        "rule_id": "health_rule.change_failure_rate.v1",
        "rule_version": "health_rule.change_failure_rate.v1.1",
        "dimension": "reliability_release",
        "subject_kind": "project",
        "subject_id": "repo_dev_health",
        "state": "at_risk",
        "fact_kind": "observed",
        "shadow_only": True,
        "evidence_source_classes": ["status_change"],
        "remediation_template": "Investigate recent deployment failures.",
        "calibration_state": "provisional",
        "evaluated_at": NOW,
        "suppressed_reason": None,
    }


def _deficiency_finding() -> dict[str, Any]:
    """A valid, standalone ``deficiency_finding.v1`` (CHAOS-3297 stack #3)."""

    return {
        "schema_version": "deficiency_finding.v1",
        "finding_id": "0f1a2b3c-000b-4a00-8000-000000000001",
        "category": "data_integration",
        "rule_id": "deficiency_rule.unconfigured_required_source.v1",
        "rule_version": "deficiency_rule.unconfigured_required_source.v1",
        "subject_kind": "project",
        "subject_id": "repo_dev_health",
        "severity": "at_risk",
        "fact_kind": "observed",
        "observed_state": "unconfigured",
        "data_semantics": "not_measured",
        "sample_count": None,
        "coverage": 0.0,
        "current_window_days": 30,
        "comparison_window_days": None,
        "relationship_paths": [],
        "evidence_ref_ids": [],
        "evidence_classification": "structural_absence",
        "blast_radius": "Required source is unconfigured for this repository.",
        "remediation": {
            "schema_version": "deficiency_remediation.v1",
            "remediation_template": "Configure the required source.",
            "verification_condition": "Resolves once re-evaluated healthy.",
        },
        "limitations": [],
        "evaluated_at": NOW,
    }


#: The canonical server copy a ``denied`` frame is allowed to render, taken
#: from the contract's own table so the fixtures cannot drift from it.
DENIED_CANONICAL_COPY = CANONICAL_NO_ANSWER_COPY["denied"]


def _denied_frame_base() -> dict[str, Any]:
    """A fully compliant, content-free ``denied`` frame.

    The base for the ``NO_ANSWER_OUTCOMES`` negative fixtures below: every
    field the no-answer allowlist classifies ``ABSENT`` starts empty/``None``
    here and ``direct_answer`` starts at the canonical server copy, so each
    negative case can populate exactly one field and stay attributable to
    the classification that rejects it.
    """

    frame = deepcopy(_frame())
    frame["public_outcome"] = "denied"
    frame["subject_ref"] = None
    frame["subject_set_ref"] = None
    frame["direct_answer"] = DENIED_CANONICAL_COPY
    frame["completion"] = None
    frame["readiness"] = None
    frame["sections"] = []
    frame["facts"] = []
    frame["metrics"] = []
    frame["comparisons"] = []
    frame["relationship_paths"] = []
    frame["health_profile_refs"] = []
    frame["finding_refs"] = []
    frame["deficiency_refs"] = []
    frame["conflicts"] = []
    frame["limitations"] = []
    frame["source_observations"] = []
    frame["evidence"] = []
    frame["safe_follow_up_questions"] = []
    frame["versions"] = None
    return frame


def _denied_answer_base() -> dict[str, Any]:
    """A fully compliant, content-free ``denied`` ``dev_answer.v2``."""

    answer = deepcopy(_answer())
    answer["public_outcome"] = "denied"
    answer["outcome_display_label"] = "Not permitted"
    answer["frame"] = _denied_frame_base()
    answer["narrative"] = None
    return answer


def _needs_clarification_frame_base() -> dict[str, Any]:
    """A fully compliant, content-free ``needs_clarification`` frame.

    Mirrors ``_denied_frame_base`` but for the one empty-content outcome the
    no-answer allowlist projection does not govern --
    ``needs_clarification`` is deliberately excluded from
    ``NO_ANSWER_OUTCOMES`` (see ``no_answer_policy``'s module docstring), so
    ``direct_answer``/``versions`` are unconstrained free text/provenance
    here rather than pinned to the canonical no-answer table.
    ``clarification_candidates`` starts empty -- CHAOS-3325: a legal
    ``needs_clarification`` payload in its own right (e.g. the question
    could not be interpreted at all, before any mention was ever resolved),
    never backfilled with an invented candidate.
    """

    frame = deepcopy(_frame())
    frame["public_outcome"] = "needs_clarification"
    frame["subject_ref"] = None
    frame["subject_set_ref"] = None
    frame["clarification_candidates"] = []
    frame["direct_answer"] = "Which repository did you mean?"
    frame["completion"] = None
    frame["readiness"] = None
    frame["sections"] = []
    frame["facts"] = []
    frame["metrics"] = []
    frame["comparisons"] = []
    frame["relationship_paths"] = []
    frame["health_profile_refs"] = []
    frame["finding_refs"] = []
    frame["deficiency_refs"] = []
    frame["conflicts"] = []
    frame["limitations"] = []
    frame["source_observations"] = []
    frame["evidence"] = []
    frame["safe_follow_up_questions"] = []
    return frame


def needs_clarification_frame_with_candidates() -> dict[str, Any]:
    """CHAOS-3325: a ``needs_clarification`` frame carrying real, authorized
    clarification candidates -- the positive counterpart to the zero-
    candidate base above. Two candidates, never one, to keep this fixture
    distinct from (and not degenerate with) the zero-candidate case."""

    frame = _needs_clarification_frame_base()
    frame["clarification_candidates"] = [
        _clarification_candidate(
            entity_id="repo_nightfall_public",
            display_label="full-chaos/nightfall-public",
        ),
        _clarification_candidate(
            entity_id="repo_nightfall_internal",
            display_label="full-chaos/nightfall-internal",
        ),
    ]
    return frame


def _no_answer_outcome_prohibited_field_cases() -> list[tuple[str, dict[str, Any]]]:
    """One negative fixture per field ``validate_no_answer_content_leaks`` forbids.

    Each case starts from ``_denied_frame_base()`` (fully compliant) and
    populates exactly one prohibited field, so the rejection is
    attributable to that one field, not to some other guard.
    """

    def case(label: str, mutator: Any) -> tuple[str, dict[str, Any]]:
        value = _denied_frame_base()
        mutator(value)
        return (label, value)

    return [
        case(
            "denied_with_completion",
            lambda v: v.__setitem__("completion", _completion()),
        ),
        case(
            "denied_with_readiness",
            lambda v: v.__setitem__(
                "readiness",
                {
                    "state": "ready",
                    "rule_id": "actual_completion",
                    "rule_version": "actual_completion.v1",
                    "translated_user_reasons": [],
                    "blocking_fact_ids": [],
                },
            ),
        ),
        case(
            "denied_with_metrics",
            lambda v: v.__setitem__("metrics", [_metric_ref()]),
        ),
        case(
            "denied_with_comparisons",
            lambda v: v.__setitem__(
                "comparisons",
                [
                    {
                        "label": "Cycle time",
                        "current_value": 12.0,
                        "comparison_value": 9.0,
                        "unit": "hours",
                    }
                ],
            ),
        ),
        case(
            "denied_with_relationship_paths",
            lambda v: v.__setitem__("relationship_paths", [_relationship_path()]),
        ),
        # CHAOS-3325: clarification_candidates is ABSENT on every true
        # no-answer outcome -- only needs_clarification may carry it.
        case(
            "denied_with_clarification_candidates",
            lambda v: v.__setitem__(
                "clarification_candidates",
                [
                    _clarification_candidate(
                        entity_id="repo_nightfall_public",
                        display_label="full-chaos/nightfall-public",
                    )
                ],
            ),
        ),
        case(
            "denied_with_evidence",
            lambda v: v.__setitem__("evidence", [_evidence()]),
        ),
        case(
            "denied_with_source_observations",
            lambda v: v.__setitem__("source_observations", [_source_observation()]),
        ),
        case(
            "denied_with_health_profile_refs",
            lambda v: v.__setitem__("health_profile_refs", ["health_profile_01"]),
        ),
        case(
            "denied_with_finding_refs",
            lambda v: v.__setitem__("finding_refs", ["finding_01"]),
        ),
        case(
            "denied_with_deficiency_refs",
            lambda v: v.__setitem__("deficiency_refs", ["deficiency_01"]),
        ),
        case(
            "denied_with_health_findings",
            lambda v: v.__setitem__("health_findings", [_health_finding()]),
        ),
        case(
            "denied_with_deficiency_findings",
            lambda v: v.__setitem__("deficiency_findings", [_deficiency_finding()]),
        ),
        case(
            "denied_with_subject_identity",
            lambda v: v.__setitem__("subject_ref", _entity_ref()),
        ),
        # Round-2 adversarial review: the free-form copy channels the
        # round-1 denylist never named.
        case(
            "denied_with_conflicts",
            lambda v: v.__setitem__(
                "conflicts",
                [
                    {
                        "summary": (
                            "Project Nightfall is marked private in one "
                            "provider and public in another."
                        ),
                        "evidence_ref_ids": [
                            "ev1_5e6770d32646fba023ff686db7decc73d792e808",
                            "ev1_65678803ca13ee1c70bd07e33adbb03fcc9144dc",
                        ],
                    }
                ],
            ),
        ),
        case(
            "denied_with_limitations",
            lambda v: v.__setitem__(
                "limitations",
                ["Project Nightfall's deployment source was stale at query time."],
            ),
        ),
        case(
            "denied_with_follow_up_questions",
            lambda v: v.__setitem__(
                "safe_follow_up_questions",
                ["How is Project Nightfall's security review tracking?"],
            ),
        ),
        case(
            "denied_with_producer_direct_answer",
            lambda v: v.__setitem__(
                "direct_answer",
                "Project Nightfall is 40% complete but you are not on its guild.",
            ),
        ),
        # Round-3 adversarial review: ``versions`` was an IDENTIFIER-shaped
        # channel that admitted ``plan_id="private/Nightfall"``. A no-answer
        # outcome now carries no provenance block at all.
        case(
            "denied_with_versions",
            lambda v: v.__setitem__("versions", _frame_versions()),
        ),
    ]


def _coverage_source_vocabulary_cases() -> list[tuple[str, dict[str, Any]]]:
    """Round-3: a subject-derived name in a coverage source list.

    Not part of the ``denied_with_*`` family, because the closure is at the
    *type* level and therefore holds for every outcome, not only no-answer
    ones: ``DevCoverageV2``'s source lists are the closed ``SourceClass``
    vocabulary. The no-answer projection additionally classifies them
    ``CLOSED_VOCABULARY`` so a later widening of the type cannot silently
    reopen the channel on a denial; that independent layer is exercised
    directly in ``test_contracts_v2`` (the type would otherwise mask it).
    """

    def case(label: str, field: str, value: str) -> tuple[str, dict[str, Any]]:
        frame = _frame()
        frame["coverage"][field] = [value]
        frame["coverage"]["required_source_count"] = 2
        return (label, frame)

    return [
        case(
            "coverage_unavailable_source_outside_vocabulary",
            "unavailable_required_sources",
            "private/Nightfall",
        ),
        case(
            "coverage_stale_source_outside_vocabulary",
            "stale_required_sources",
            "Nightfall-deployments",
        ),
    ]


def _stream_started() -> dict[str, Any]:
    return {
        "schema_version": "dev_stream_event.v2",
        "run_id": "0f1a2b3c-0001-4a00-8000-000000000001",
        "sequence": 0,
        "event": "run.started",
        "occurred_at": NOW,
        "progress": None,
        "resolution_ledger": None,
        "delta": None,
        "answer": None,
        "warning": None,
        "error": None,
        "terminal_kind": None,
    }


def _error() -> dict[str, Any]:
    return {
        "schema_version": "dev_error.v1",
        "request_id": "web-req-2026-07-31-0001",
        "code": "source_unavailable",
        "safe_message": "A required source is temporarily unavailable.",
        "retryable": True,
        "remediation": ["Retry after source health recovers."],
    }


#: Safe display labels per no-answer outcome, mirroring
#: ``contracts_v2.answer._OUTCOME_DISPLAY_LABELS`` (kept in step by an
#: import-time check in that module).
_NO_ANSWER_DISPLAY_LABELS = {
    "not_found": "Not found",
    "temporarily_unavailable": "Temporarily unavailable",
    "unsupported": "Not supported yet",
    "denied": "Not permitted",
    "failed": "Something went wrong",
}


def no_answer_answer_fixture(outcome: str) -> dict[str, Any]:
    """A valid, fully projected ``dev_answer.v2`` for one no-answer outcome.

    One builder for all five outcomes so a caller cannot accidentally
    construct a "compliant" no-answer payload that the allowlist projection
    would actually reject.
    """

    answer = _denied_answer_base()
    answer["public_outcome"] = outcome
    answer["outcome_display_label"] = _NO_ANSWER_DISPLAY_LABELS[outcome]
    answer["frame"]["public_outcome"] = outcome
    answer["frame"]["direct_answer"] = CANONICAL_NO_ANSWER_COPY[outcome]
    return answer


def positive_fixtures() -> dict[str, dict[str, Any]]:
    """Return one independent positive golden for every public v2 schema."""

    return {
        "dev_message_request.v2": _message_request(),
        "dev_question_intent.v1": _intent(),
        "dev_subject_mention.v1": _mention(),
        "dev_resolution_ledger.v1": _ledger(),
        "dev_subject_set.v1": _subject_set(),
        "dev_source_requirement.v1": _source_requirement(),
        "dev_investigation_plan.v1": _plan(),
        "dev_source_observation.v1": _source_observation(),
        "dev_investigation_result.v1": _investigation_result(),
        "dev_answer_frame.v1": _frame(),
        "dev_narrative.v1": _narrative(),
        "dev_answer.v2": _answer(),
        "dev_stream_event.v2": _stream_started(),
    }


def negative_fixtures() -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Return intentional failures; case labels explain the invariant exercised."""

    positives = positive_fixtures()

    def changed(schema: str, mutator: Any) -> dict[str, Any]:
        value = deepcopy(positives[schema])
        mutator(value)
        return value

    frame_internal_leakage = changed(
        "dev_answer_frame.v1",
        lambda value: value.__setitem__(
            "direct_answer",
            "Scope resolution failed with scope_forbidden for this repository.",
        ),
    )

    def _make_outcome_content_mismatch(value: dict[str, Any]) -> None:
        # Isolated to *only* trip validate_outcome_consistency's has_content
        # check (sections/facts non-empty for a no-content outcome).
        # `needs_clarification` rather than one of the five NO_ANSWER_OUTCOMES
        # deliberately: it is an empty-content outcome that the no-answer
        # allowlist projection does not govern, so this fixture stays
        # attributable to exactly one guardrail (see
        # test_disabling_one_frame_validator_flips_only_its_own_fixture).
        value["public_outcome"] = "needs_clarification"
        value["facts"][0]["evidence_ref_ids"] = []
        # F10 (CHAOS-3297 stack #3): a fact with neither evidence_ref_ids nor
        # a disclosure now fails validate_frame_grounding too, which would
        # make this fixture attributable to TWO guardrails instead of one.
        # A disclosure is this fact's explicit no-evidence classification,
        # keeping it isolated to validate_outcome_consistency's own check.
        value["facts"][0]["disclosures"] = ["untrusted_source"]
        value["facts"][0]["relationship_path_ids"] = []
        value["completion"] = None
        value["readiness"] = None
        value["metrics"] = []
        value["comparisons"] = []
        value["relationship_paths"] = []
        value["evidence"] = []
        value["source_observations"] = []
        value["health_profile_refs"] = []
        value["finding_refs"] = []
        value["deficiency_refs"] = []
        value["subject_ref"] = None

    frame_outcome_content_mismatch = changed(
        "dev_answer_frame.v1", _make_outcome_content_mismatch
    )
    frame_completion_without_denominator = changed(
        "dev_answer_frame.v1",
        lambda value: value["completion"].__setitem__("denominator", None),
    )
    frame_relationship_outside_frame = changed(
        "dev_answer_frame.v1",
        lambda value: value.__setitem__("relationship_paths", []),
    )
    # ``versions`` is optional on the model only so a no-answer outcome can
    # omit it; an outcome that carries content must still carry provenance.
    frame_answered_without_versions = changed(
        "dev_answer_frame.v1",
        lambda value: value.__setitem__("versions", None),
    )
    # CHAOS-3297 flags gap (ratified 2026-08-02): an 'answered' frame whose
    # only defect is a fact disclosure -- validate_outcome_consistency's new
    # clause, isolated from the pre-existing limitations/completion checks.
    frame_answered_with_disclosure = changed(
        "dev_answer_frame.v1",
        lambda value: value["facts"][0].__setitem__("disclosures", ["stale"]),
    )
    # F10 (CHAOS-3297 stack #3, ratified 2026-08-02): a fact with neither
    # evidence_ref_ids nor a disclosure -- isolated from every other frame
    # guardrail (the base positive fixture is otherwise a valid 'answered'
    # frame; only this one fact's evidence is cleared, with no disclosure
    # substituted).
    frame_fact_missing_grounding = changed(
        "dev_answer_frame.v1",
        lambda value: value["facts"][0].__setitem__("evidence_ref_ids", []),
    )
    # DevAnswerFact.validate_disclosures_canonical_order: declared out of
    # ascending FactDisclosure order.
    frame_disclosures_out_of_order = changed(
        "dev_answer_frame.v1",
        lambda value: value["facts"][0].__setitem__(
            "disclosures", ["conflicting", "stale"]
        ),
    )
    # Same validator: a duplicated disclosure is also not strictly ascending.
    frame_disclosures_duplicated = changed(
        "dev_answer_frame.v1",
        lambda value: value["facts"][0].__setitem__("disclosures", ["stale", "stale"]),
    )
    # CHAOS-3325: validate_outcome_consistency's new clause -- an 'answered'
    # frame whose only defect is a clarification candidate. Distinct from
    # denied_with_clarification_candidates (the ABSENT no-answer-policy
    # path): 'answered' is not one of NO_ANSWER_OUTCOMES, so that policy
    # never reaches it -- this clause is what does.
    frame_answered_with_clarification_candidates = changed(
        "dev_answer_frame.v1",
        lambda value: value.__setitem__(
            "clarification_candidates",
            [
                _clarification_candidate(
                    entity_id="repo_nightfall_public",
                    display_label="full-chaos/nightfall-public",
                )
            ],
        ),
    )
    # A candidate outside the closed EntityKind vocabulary -- the only
    # "unauthorized-shaped" candidate expressible at the wire-type level.
    # Real authorization is enforced by the builder using only
    # preflight-resolved entities (subject_preflight._entity_ref_v2), never
    # by a field on the wire shape itself.
    frame_clarification_candidate_unknown_entity_kind = changed(
        "dev_answer_frame.v1",
        lambda value: (
            value.__setitem__("public_outcome", "needs_clarification"),
            value.__setitem__("subject_ref", None),
            value.__setitem__("subject_set_ref", None),
            value.__setitem__("sections", []),
            value.__setitem__("facts", []),
            value.__setitem__("completion", None),
            value.__setitem__("readiness", None),
            value.__setitem__("metrics", []),
            value.__setitem__("comparisons", []),
            value.__setitem__("relationship_paths", []),
            value.__setitem__("evidence", []),
            value.__setitem__("source_observations", []),
            value.__setitem__("health_profile_refs", []),
            value.__setitem__("finding_refs", []),
            value.__setitem__("deficiency_refs", []),
            value.__setitem__(
                "clarification_candidates",
                [
                    _clarification_candidate(
                        entity_id="repo_nightfall_public",
                        display_label="full-chaos/nightfall-public",
                        entity_kind="malicious_kind",
                    )
                ],
            ),
        ),
    )
    # DevAnswerFrame.validate_frame_semantics: two candidates naming the same
    # authorized entity are not two distinct options.
    frame_clarification_candidates_duplicate_entity_id = changed(
        "dev_answer_frame.v1",
        lambda value: (
            value.__setitem__("public_outcome", "needs_clarification"),
            value.__setitem__("subject_ref", None),
            value.__setitem__("subject_set_ref", None),
            value.__setitem__("sections", []),
            value.__setitem__("facts", []),
            value.__setitem__("completion", None),
            value.__setitem__("readiness", None),
            value.__setitem__("metrics", []),
            value.__setitem__("comparisons", []),
            value.__setitem__("relationship_paths", []),
            value.__setitem__("evidence", []),
            value.__setitem__("source_observations", []),
            value.__setitem__("health_profile_refs", []),
            value.__setitem__("finding_refs", []),
            value.__setitem__("deficiency_refs", []),
            value.__setitem__(
                "clarification_candidates",
                [
                    _clarification_candidate(
                        entity_id="repo_nightfall_public",
                        display_label="full-chaos/nightfall-public",
                    ),
                    _clarification_candidate(
                        entity_id="repo_nightfall_public",
                        display_label="full-chaos/nightfall (duplicate)",
                    ),
                ],
            ),
        ),
    )
    answer_frame_run_id_mismatch = changed(
        "dev_answer.v2",
        lambda value: value["frame"].__setitem__(
            "run_id", "0f1a2b3c-0001-4a00-8000-0000000000ff"
        ),
    )
    answer_narrative_contradicts_number = changed(
        "dev_answer.v2",
        lambda value: value["narrative"].__setitem__(
            "body",
            "Repository dev-health is on track; the completion rate is 100%.",
        ),
    )
    answer_narrative_contradicts_readiness = changed(
        "dev_answer.v2",
        lambda value: value["narrative"].__setitem__(
            "body",
            "Repository dev-health is ready, with no open work remaining.",
        ),
    )
    answer_narrative_contradicts_subject = changed(
        "dev_answer.v2",
        lambda value: value["narrative"].__setitem__(
            "body",
            "Repository billing-service is on track, with one required child "
            "issue still open.",
        ),
    )
    answer_narrative_contradicts_recommendation = changed(
        "dev_answer.v2",
        lambda value: value["narrative"].__setitem__(
            "body",
            "Repository dev-health is on track. We recommend closing the "
            "remaining issue soon.",
        ),
    )

    # --- Round-2 adversarial-review counterexamples ------------------------
    # A no-answer outcome carries no narrative at all: the narrative is the
    # free-form channel that survived the round-1 structured-field scrub.
    denied_answer_with_narrative = _denied_answer_base()
    denied_answer_with_narrative["narrative"] = {
        **_narrative(),
        "referenced_fact_ids": [],
        "referenced_section_ids": [],
        "body": (
            "The project Nightfall exists but is restricted to another guild, "
            "so its status cannot be shown here."
        ),
    }

    # An unrelated comparison value of 100 no longer legitimizes a "100%
    # complete" claim against a frame whose completion block says 3/4.
    answer_narrative_unrelated_comparison_number = changed(
        "dev_answer.v2",
        lambda value: (
            value["frame"].__setitem__(
                "comparisons",
                [
                    {
                        "label": "Review throughput",
                        "current_value": 100.0,
                        "comparison_value": 82.0,
                        "unit": "count",
                    }
                ],
            ),
            value["narrative"].__setitem__(
                "body",
                "Repository dev-health is at 100% completion for the current window.",
            ),
        ),
    )

    # A different subject that merely shares a token with the committed one
    # ("billing-health" vs. "full-chaos/dev-health") no longer satisfies the
    # subject check, which used substring containment.
    answer_narrative_substring_subject = changed(
        "dev_answer.v2",
        lambda value: value["narrative"].__setitem__(
            "body",
            "Repository billing-health is on track, with one required child "
            "issue still open.",
        ),
    )

    # Recommendation prose must reference the specific recommendation fact;
    # the mere presence of one somewhere in the frame is not grounding.
    def _make_unbound_recommendation(value: dict[str, Any]) -> None:
        value["frame"]["facts"].append(
            {
                "fact_id": "fact_rec",
                "text": "Add a second reviewer to the release checklist.",
                "kind": "recommendation",
                "evidence_ref_ids": ["ev1_a2bc440cf82f6979884d6d486dacdc900744d04b"],
                "relationship_path_ids": [],
                "confidence": 0.8,
            }
        )
        value["frame"]["sections"][0]["fact_ids"].append("fact_rec")
        value["narrative"]["referenced_fact_ids"] = ["fact_01"]
        value["narrative"]["referenced_section_ids"] = []
        value["narrative"]["body"] = (
            "Repository dev-health is on track. We recommend freezing the "
            "release branch until the open child issue closes."
        )

    answer_narrative_unbound_recommendation = changed(
        "dev_answer.v2", _make_unbound_recommendation
    )

    # Round-3 adversarial review: the completion block's numerator and
    # denominator were unioned into one pool offered to every sentence, so a
    # 3/4 completion block legitimized "4 open security incidents" — a number
    # about something else entirely. Completion values are now admitted only
    # in a sentence that actually makes a completion claim, and this sentence
    # makes none.
    answer_narrative_completion_number_out_of_context = changed(
        "dev_answer.v2",
        lambda value: value["narrative"].__setitem__(
            "body",
            "Repository dev-health has 4 open security incidents.",
        ),
    )
    return {
        "dev_message_request.v2": [
            (
                "oversized_question",
                changed(
                    "dev_message_request.v2",
                    lambda value: value.__setitem__("question", "é" * 4_097),
                ),
            )
        ],
        "dev_question_intent.v1": [
            (
                "clarification_without_reason",
                changed(
                    "dev_question_intent.v1",
                    lambda value: value.__setitem__("requires_clarification", True),
                ),
            )
        ],
        "dev_subject_mention.v1": [
            (
                "ordinal_out_of_range",
                changed(
                    "dev_subject_mention.v1",
                    lambda value: value.__setitem__("mention_ordinal", 99),
                ),
            )
        ],
        "dev_resolution_ledger.v1": [
            (
                "erased_entry",
                changed(
                    "dev_resolution_ledger.v1",
                    lambda value: value["entries"][0].__setitem__("entry_ordinal", 1),
                ),
            )
        ],
        "dev_subject_set.v1": [
            (
                "omitted_without_warning",
                changed(
                    "dev_subject_set.v1",
                    lambda value: (
                        value.__setitem__(
                            "unresolved_mention_ids",
                            ["0f1a2b3c-0009-4a00-8000-000000000002"],
                        ),
                        value.__setitem__("cohort_complete", False),
                        value.__setitem__("original_mention_count", 2),
                    ),
                ),
            )
        ],
        "dev_source_requirement.v1": [
            (
                "conditional_without_rule",
                changed(
                    "dev_source_requirement.v1",
                    lambda value: value.__setitem__("requirement_level", "conditional"),
                ),
            )
        ],
        "dev_investigation_plan.v1": [
            (
                "step_in_both_buckets",
                changed(
                    "dev_investigation_plan.v1",
                    lambda value: value.__setitem__(
                        "conditional_steps", ["status_snapshot"]
                    ),
                ),
            )
        ],
        "dev_source_observation.v1": [
            (
                "measured_zero_on_unconfigured",
                changed(
                    "dev_source_observation.v1",
                    lambda value: (
                        value.__setitem__("observed_state", "unconfigured"),
                        value.__setitem__("limitation", "Provider not configured."),
                    ),
                ),
            )
        ],
        "dev_investigation_result.v1": [
            (
                "step_in_two_buckets",
                changed(
                    "dev_investigation_result.v1",
                    lambda value: value.__setitem__(
                        "failed_steps", ["status_snapshot"]
                    ),
                ),
            )
        ],
        "dev_answer_frame.v1": [
            ("internal_leakage", frame_internal_leakage),
            ("outcome_content_mismatch", frame_outcome_content_mismatch),
            ("completion_without_denominator", frame_completion_without_denominator),
            ("relationship_outside_frame", frame_relationship_outside_frame),
            ("answered_without_versions", frame_answered_without_versions),
            ("answered_with_disclosure", frame_answered_with_disclosure),
            ("fact_missing_grounding", frame_fact_missing_grounding),
            ("disclosures_out_of_order", frame_disclosures_out_of_order),
            ("disclosures_duplicated", frame_disclosures_duplicated),
            (
                "answered_with_clarification_candidates",
                frame_answered_with_clarification_candidates,
            ),
            (
                "clarification_candidate_unknown_entity_kind",
                frame_clarification_candidate_unknown_entity_kind,
            ),
            (
                "clarification_candidates_duplicate_entity_id",
                frame_clarification_candidates_duplicate_entity_id,
            ),
            *_no_answer_outcome_prohibited_field_cases(),
            *_coverage_source_vocabulary_cases(),
        ],
        "dev_narrative.v1": [
            (
                "provider_mode_without_metadata",
                changed(
                    "dev_narrative.v1",
                    lambda value: value.__setitem__("mode", "provider"),
                ),
            )
        ],
        "dev_answer.v2": [
            (
                "outcome_mismatch_with_frame",
                changed(
                    "dev_answer.v2",
                    lambda value: value.__setitem__("public_outcome", "not_found"),
                ),
            ),
            ("frame_run_id_mismatch", answer_frame_run_id_mismatch),
            ("narrative_contradicts_number", answer_narrative_contradicts_number),
            ("narrative_contradicts_readiness", answer_narrative_contradicts_readiness),
            ("narrative_contradicts_subject", answer_narrative_contradicts_subject),
            (
                "narrative_contradicts_recommendation",
                answer_narrative_contradicts_recommendation,
            ),
            ("denied_with_narrative", denied_answer_with_narrative),
            (
                "narrative_unrelated_comparison_number",
                answer_narrative_unrelated_comparison_number,
            ),
            ("narrative_substring_subject", answer_narrative_substring_subject),
            (
                "narrative_unbound_recommendation",
                answer_narrative_unbound_recommendation,
            ),
            (
                "narrative_completion_number_out_of_context",
                answer_narrative_completion_number_out_of_context,
            ),
        ],
        "dev_stream_event.v2": [
            (
                "answer_completed_without_payload",
                changed(
                    "dev_stream_event.v2",
                    lambda value: value.__setitem__("event", "answer.completed"),
                ),
            )
        ],
    }


def stream_fixtures() -> dict[str, list[dict[str, Any]]]:
    started = deepcopy(positive_fixtures()["dev_stream_event.v2"])
    terminal_error = {
        **deepcopy(started),
        "sequence": 1,
        "event": "error",
        "error": _error(),
    }
    done = {
        **deepcopy(started),
        "sequence": 2,
        "event": "done",
        "terminal_kind": "error",
    }
    duplicate_terminal = [
        deepcopy(started),
        deepcopy(terminal_error),
        {**deepcopy(terminal_error), "sequence": 2},
        {**deepcopy(done), "sequence": 3},
    ]

    # Codex adversarial review (CHAOS-3294): "run.started, done, error, done"
    # previously validated — a `done` before the real terminal result never
    # tripped the old check, which only looked at the *last* event.
    premature_done = {
        **deepcopy(started),
        "sequence": 1,
        "event": "done",
        "terminal_kind": "error",
    }
    premature_done_stream = [
        deepcopy(started),
        premature_done,
        {**deepcopy(terminal_error), "sequence": 2},
        {**deepcopy(done), "sequence": 3},
    ]

    # Round-2 adversarial review: a second `run.started` never tripped any
    # of the positional rules, which each covered one marker. The single
    # lifecycle invariant (`stream._validate_run_lifecycle`) rejects it.
    duplicate_start_stream = [
        deepcopy(started),
        {**deepcopy(started), "sequence": 1},
        {**deepcopy(terminal_error), "sequence": 2},
        {**deepcopy(done), "sequence": 3},
    ]

    duplicate_done_stream = [
        deepcopy(started),
        {**deepcopy(terminal_error), "sequence": 1},
        {**deepcopy(done), "sequence": 2},
        {**deepcopy(done), "sequence": 3},
    ]

    # Codex adversarial review (CHAOS-3294): two `resolution.updated` events
    # were validated independently; `validate_ledger_extends` was never
    # applied between them, so a later ledger could rewrite an earlier
    # entry's outcome/committed reference instead of only appending.
    ledger_v1 = _ledger()
    ledger_v2_rewritten = deepcopy(ledger_v1)
    ledger_v2_rewritten["entries"][0]["outcome"] = "no_authorized_match"
    ledger_v2_rewritten["entries"][0]["committed_entity_ref"] = None
    ledger_v2_rewritten["entries"][0]["repository_attribution"] = None
    resolution_v1 = {
        **deepcopy(started),
        "sequence": 1,
        "event": "resolution.updated",
        "resolution_ledger": ledger_v1,
    }
    resolution_v2_rewrite = {
        **deepcopy(started),
        "sequence": 2,
        "event": "resolution.updated",
        "resolution_ledger": ledger_v2_rewritten,
    }
    ledger_rewrite_stream = [
        deepcopy(started),
        resolution_v1,
        resolution_v2_rewrite,
        {**deepcopy(terminal_error), "sequence": 3},
        {**deepcopy(done), "sequence": 4},
    ]

    # Codex adversarial review (CHAOS-3294): an `answer.completed` event's
    # embedded answer could carry a different run_id than the event itself.
    mismatched_run_answer = deepcopy(positive_fixtures()["dev_answer.v2"])
    mismatched_run_answer["run_id"] = "0f1a2b3c-0001-4a00-8000-0000000000ff"
    mismatched_run_answer["frame"]["run_id"] = "0f1a2b3c-0001-4a00-8000-0000000000ff"
    if mismatched_run_answer["narrative"] is not None:
        mismatched_run_answer["narrative"]["run_id"] = (
            "0f1a2b3c-0001-4a00-8000-0000000000ff"
        )
    answer_completed_mismatch = {
        **deepcopy(started),
        "sequence": 1,
        "event": "answer.completed",
        "answer": mismatched_run_answer,
    }
    answer_run_id_mismatch_stream = [
        deepcopy(started),
        answer_completed_mismatch,
        {**deepcopy(done), "sequence": 2, "terminal_kind": "answer"},
    ]

    return {
        "valid": [started, terminal_error, done],
        "invalid_duplicate_terminal": duplicate_terminal,
        "invalid_missing_done": [deepcopy(started), deepcopy(terminal_error)],
        "invalid_out_of_order": [
            {**deepcopy(started), "sequence": 1},
            {**deepcopy(terminal_error), "sequence": 0},
            deepcopy(done),
        ],
        "invalid_premature_done": premature_done_stream,
        "invalid_duplicate_done": duplicate_done_stream,
        "invalid_duplicate_start": duplicate_start_stream,
        "invalid_ledger_rewrite": ledger_rewrite_stream,
        "invalid_answer_run_id_mismatch": answer_run_id_mismatch_stream,
    }


__all__ = [
    "needs_clarification_frame_with_candidates",
    "negative_fixtures",
    "no_answer_answer_fixture",
    "positive_fixtures",
    "stream_fixtures",
]
