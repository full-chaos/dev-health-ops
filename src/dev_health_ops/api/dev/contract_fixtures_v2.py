"""Deterministic golden payloads for the Ask Dev Wave 3.1 v2 contracts.

Mirrors ``dev_health_ops.api.dev.contract_fixtures`` (v1): every function
here returns plain JSON-serializable dicts (not constructed Pydantic
instances), since ``export_contracts_v2`` and the test suite both validate
these payloads through ``CONTRACT_MODELS_V2[name].model_validate(payload)``.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

NOW = "2026-07-28T12:00:00Z"
START = "2026-06-28T00:00:00Z"
END = "2026-07-28T00:00:00Z"
PREVIOUS_START = "2026-05-29T00:00:00Z"


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
        "evidence_ref_id": "ev_01",
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


def _relationship_path() -> dict[str, Any]:
    return {
        "path_id": "path_01",
        "source_entity_id": "repo_dev_health",
        "relationship": "has_status",
        "target_entity_id": "status_snapshot_01",
        "provenance": "Canonical status snapshot",
        "confidence": 1.0,
        "observed_at": NOW,
        "evidence_ref_ids": ["ev_01"],
    }


def _mention() -> dict[str, Any]:
    return {
        "schema_version": "dev_subject_mention.v1",
        "mention_id": "mention_01",
        "mention_ordinal": 0,
        "original_text_span": "dev-health",
        "requested_entity_kind": "repository",
        "normalized_lookup_text": "dev-health",
    }


def _resolution_entry() -> dict[str, Any]:
    return {
        "entry_ordinal": 0,
        "mention_id": "mention_01",
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
        "ledger_id": "ledger_01",
        "mention_ids": ["mention_01"],
        "entries": [_resolution_entry()],
        "updated_at": NOW,
    }


def _subject_set() -> dict[str, Any]:
    return {
        "schema_version": "dev_subject_set.v1",
        "set_id": "set_01",
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
        "observation_id": "obs_01",
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
        "evidence_ref_ids": ["ev_01"],
        "limitation": None,
        "observed_at": NOW,
        "query_version": "status_snapshot.v1",
    }


def _investigation_result() -> dict[str, Any]:
    return {
        "schema_version": "dev_investigation_result.v1",
        "result_id": "result_01",
        "plan_id": "status.entity.v2",
        "plan_version": "status.entity.v2.1",
        "run_id": "run_01",
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
        "evidence_ref_ids": ["ev_01"],
        "relationship_path_ids": ["path_01"],
        "confidence": 1.0,
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
        "frame_id": "frame_01",
        "run_id": "run_01",
        "generated_at": NOW,
        "public_outcome": "answered",
        "subject_ref": _entity_ref(),
        "subject_set_ref": None,
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
        "narrative_id": "narrative_01",
        "run_id": "run_01",
        "frame_id": "frame_01",
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
        "answer_id": "answer_01",
        "conversation_id": "conversation_01",
        "run_id": "run_01",
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
        "request_id": "request_01",
        "client_message_id": "message_01",
        "conversation_id": "conversation_01",
        "idempotency_key": "idem_01",
        "retry_of_run_id": "run_original_01",
        "question": "How is repo_dev_health doing?",
        "scope": _scope(),
        "requested_metric_ids": [],
        "question_class_hint": None,
    }


def _stream_started() -> dict[str, Any]:
    return {
        "schema_version": "dev_stream_event.v2",
        "run_id": "run_01",
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
        "request_id": "request_01",
        "code": "source_unavailable",
        "safe_message": "A required source is temporarily unavailable.",
        "retryable": True,
        "remediation": ["Retry after source health recovers."],
    }


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
    frame_outcome_content_mismatch = changed(
        "dev_answer_frame.v1",
        lambda value: value.__setitem__("public_outcome", "not_found"),
    )
    frame_completion_without_denominator = changed(
        "dev_answer_frame.v1",
        lambda value: value["completion"].__setitem__("denominator", None),
    )
    frame_relationship_outside_frame = changed(
        "dev_answer_frame.v1",
        lambda value: value.__setitem__("relationship_paths", []),
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
                        value.__setitem__("unresolved_mention_ids", ["mention_02"]),
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
            )
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
    return {
        "valid": [started, terminal_error, done],
        "invalid_duplicate_terminal": duplicate_terminal,
        "invalid_missing_done": [deepcopy(started), deepcopy(terminal_error)],
        "invalid_out_of_order": [
            {**deepcopy(started), "sequence": 1},
            {**deepcopy(terminal_error), "sequence": 0},
            deepcopy(done),
        ],
    }


__all__ = ["negative_fixtures", "positive_fixtures", "stream_fixtures"]
