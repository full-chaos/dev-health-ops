"""Deterministic golden payloads for the canonical Ask Dev v1 contracts."""

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
        "team_ids": ["team_platform"],
        "time_range": _time_range(),
        "comparison_range": _time_range(PREVIOUS_START, START),
        "surface_context": {
            "route_id": "diagnose_overview",
            "entity_refs": [],
            "filter_fingerprint": "filters_01",
        },
    }


def _scope_resolution() -> dict[str, Any]:
    scope = _scope()
    return {
        "schema_version": "dev_scope_resolution.v1",
        "requested_scope": deepcopy(scope),
        "resolved_scope": deepcopy(scope),
        "outcome": "exact",
        "authorized_repository_ids": ["repo_dev_health"],
        "authorized_entity_ids": [],
        "candidates": [],
        "fallbacks": [],
        "warnings": [],
        "resolved_at": NOW,
    }


def _evidence() -> dict[str, Any]:
    return {
        "schema_version": "dev_evidence_ref.v1",
        "evidence_ref_id": "ev_01",
        "source_system": "work_graph",
        "source_version": "work_graph.v1",
        "entity_type": "work_item",
        "entity_id": "item_01",
        "display_label": "Implement contract baseline",
        "link": {"internal_path": "/work/items/item_01", "source_url": None},
        "observed_at": NOW,
        "freshness": "fresh",
        "provenance": "Canonical work graph projection",
        "confidence": 1.0,
        "citation_text": "The contract implementation remains in progress.",
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


def _metric() -> dict[str, Any]:
    return {
        "schema_version": "dev_metric_ref.v1",
        "metric_ref_id": "metric_01",
        "metric_id": "items_completed",
        "label": "Items completed",
        "definition_version": "items_completed.v1",
        "unit": "items",
        "aggregation": "count",
        "display_precision": 0,
        "resolved_scope": _scope(),
        "dimensions": ["repository"],
        "current_window": _time_range(),
        "comparison_window": _time_range(PREVIOUS_START, START),
        "value": 12,
        "comparison_value": 10,
        "series": [],
        "query_version": "query_metric.v1",
        "source_version": "work_item_metrics_daily.v1",
        "freshness": "fresh",
        "coverage": 1.0,
        "evidence_ref_ids": ["ev_01"],
    }


def _claim() -> dict[str, Any]:
    return {
        "schema_version": "dev_claim.v1",
        "claim_id": "claim_01",
        "kind": "observed",
        "text": "Twelve work items completed in the selected period.",
        "confidence": 1.0,
        "evidence_ref_ids": ["ev_01"],
        "metric_ref_ids": ["metric_01"],
        "validity_scope": _scope(),
        "flags": {
            "stale": False,
            "uncertain": False,
            "conflicting": False,
            "untrusted_source": False,
        },
        "recommendation_rule_version": None,
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


def _answer() -> dict[str, Any]:
    return {
        "schema_version": "dev_answer.v1",
        "answer_id": "answer_01",
        "conversation_id": "conversation_01",
        "generated_at": NOW,
        "resolved_scope": _scope_resolution(),
        "as_of": NOW,
        "status": "complete",
        "direct_summary": "Twelve work items completed in the selected period.",
        "claims": [_claim()],
        "metrics": [_metric()],
        "evidence": [_evidence()],
        "conflicts": [],
        "coverage": {
            "required_source_count": 1,
            "available_source_count": 1,
            "unavailable_required_sources": [],
            "stale_required_sources": [],
            "as_of": NOW,
        },
        "warnings": [],
        "suggested_follow_up_questions": ["What changed from the previous period?"],
        "versions": {
            "prompt_version": "ask_dev_prompt.v1",
            "tool_contract_version": "ask_dev_tools.v1",
            "metric_definition_version": "ask_dev_metrics.v1",
            "query_version": "ask_dev_queries.v1",
        },
        "model": {
            "provider_source": "platform",
            "provider_family": "openai_compatible",
            "model_fingerprint": "model_certified_01",
        },
    }


def positive_fixtures() -> dict[str, dict[str, Any]]:
    """Return one independent positive golden for every public schema."""

    return {
        "dev_capabilities.v1": {
            "schema_version": "dev_capabilities.v1",
            "ask_dev": False,
            "byo_llm": True,
            "agent_context_runtime": False,
            "can_read": False,
            "can_manage": False,
        },
        "dev_conversation.v1": {
            "schema_version": "dev_conversation.v1",
            "conversation_id": "conversation_01",
            "title": "Repository delivery status",
            "current_scope": _scope(),
            "retention_days": 30,
            "state": "active",
            "message_count": 2,
            "latest_answer_id": "answer_01",
            "created_at": NOW,
            "updated_at": NOW,
            "expires_at": "2026-08-27T12:00:00Z",
        },
        "dev_conversation_summary.v1": {
            "schema_version": "dev_conversation_summary.v1",
            "conversation_id": "conversation_01",
            "title": "Repository delivery status",
            "direct_scope": "repository",
            "state": "active",
            "message_count": 2,
            "updated_at": NOW,
            "expires_at": "2026-08-27T12:00:00Z",
        },
        "dev_message_request.v1": {
            "schema_version": "dev_message_request.v1",
            "request_id": "request_01",
            "client_message_id": "message_01",
            "conversation_id": "conversation_01",
            "question": "How many items completed in this period?",
            "question_class": "registered_statistics",
            "scope": _scope(),
            "requested_metric_ids": ["items_completed"],
        },
        "dev_answer.v1": _answer(),
        "dev_claim.v1": _claim(),
        "dev_metric_ref.v1": _metric(),
        "dev_evidence_ref.v1": _evidence(),
        "dev_scope.v1": _scope(),
        "dev_scope_resolution.v1": _scope_resolution(),
        "dev_tool_request.v1": {
            "schema_version": "dev_tool_request.v1",
            "run_id": "run_01",
            "tool_call_id": "tool_call_01",
            "tool_id": "query_metric.v1",
            "scope": _scope(),
            "query": None,
            "metric_id": "items_completed",
            "evidence_ref_ids": [],
            "include_comparison": True,
            "limit": 12,
        },
        "dev_tool_result.v1": {
            "schema_version": "dev_tool_result.v1",
            "run_id": "run_01",
            "tool_call_id": "tool_call_01",
            "tool_id": "query_metric.v1",
            "status": "success",
            "scope_resolution": _scope_resolution(),
            "metrics": [_metric()],
            "evidence": [_evidence()],
            "status_facts": [],
            "graph_edges": [],
            "data_health": [],
            "warnings": [],
            "error": None,
            "serialized_bytes": 4_096,
        },
        "dev_feedback.v1": {
            "schema_version": "dev_feedback.v1",
            "feedback_id": "feedback_01",
            "answer_id": "answer_01",
            "rating": "up",
            "reasons": ["useful"],
            "comment": "The evidence answered my question.",
            "created_at": NOW,
        },
        "dev_stream_event.v1": {
            "schema_version": "dev_stream_event.v1",
            "run_id": "run_01",
            "sequence": 0,
            "event": "run.started",
            "occurred_at": NOW,
            "progress": None,
            "scope_resolution": None,
            "delta": None,
            "answer": None,
            "warning": None,
            "error": None,
            "terminal_kind": None,
        },
        "dev_error.v1": _error(),
    }


def negative_fixtures() -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Return intentional failures; case labels explain the invariant exercised."""

    positives = positive_fixtures()

    def changed(schema: str, mutator: Any) -> dict[str, Any]:
        value = deepcopy(positives[schema])
        mutator(value)
        return value

    answer_unknown_evidence = changed(
        "dev_answer.v1",
        lambda value: value["claims"][0].__setitem__(
            "evidence_ref_ids", ["ev_unknown"]
        ),
    )
    answer_unknown_metric = changed(
        "dev_answer.v1",
        lambda value: value["claims"][0].__setitem__(
            "metric_ref_ids", ["metric_unknown"]
        ),
    )
    answer_missing_versions = changed(
        "dev_answer.v1", lambda value: value.pop("versions")
    )
    answer_invalid_scope = changed(
        "dev_answer.v1",
        lambda value: value["resolved_scope"]["resolved_scope"].__setitem__(
            "repositories", []
        ),
    )
    answer_oversized = changed(
        "dev_answer.v1",
        lambda value: value.__setitem__(
            "evidence",
            [
                {**deepcopy(_evidence()), "evidence_ref_id": f"ev_{index:02d}"}
                for index in range(26)
            ],
        ),
    )
    answer_invalid_complete = changed(
        "dev_answer.v1",
        lambda value: value["coverage"].__setitem__(
            "stale_required_sources", ["work_graph"]
        ),
    )
    return {
        "dev_capabilities.v1": [
            (
                "unknown_gate",
                {
                    **deepcopy(positives["dev_capabilities.v1"]),
                    "provider_api_key": "secret",
                },
            )
        ],
        "dev_conversation.v1": [
            (
                "invalid_retention",
                changed(
                    "dev_conversation.v1",
                    lambda value: value.__setitem__("retention_days", 7),
                ),
            )
        ],
        "dev_conversation_summary.v1": [
            (
                "invalid_scope",
                changed(
                    "dev_conversation_summary.v1",
                    lambda value: value.__setitem__("direct_scope", "incident"),
                ),
            )
        ],
        "dev_message_request.v1": [
            (
                "oversized_question",
                changed(
                    "dev_message_request.v1",
                    lambda value: value.__setitem__("question", "é" * 4_097),
                ),
            )
        ],
        "dev_answer.v1": [
            ("unknown_evidence_id", answer_unknown_evidence),
            ("unknown_metric_id", answer_unknown_metric),
            ("missing_required_metadata", answer_missing_versions),
            ("invalid_scope", answer_invalid_scope),
            ("oversized_evidence", answer_oversized),
            ("invalid_complete_state", answer_invalid_complete),
        ],
        "dev_claim.v1": [
            (
                "ungrounded_observation",
                changed(
                    "dev_claim.v1",
                    lambda value: (
                        value.__setitem__("evidence_ref_ids", []),
                        value.__setitem__("metric_ref_ids", []),
                    ),
                ),
            )
        ],
        "dev_metric_ref.v1": [
            (
                "unregistered_metric",
                changed(
                    "dev_metric_ref.v1",
                    lambda value: value.__setitem__("metric_id", "invented_velocity"),
                ),
            )
        ],
        "dev_evidence_ref.v1": [
            (
                "invalid_confidence",
                changed(
                    "dev_evidence_ref.v1",
                    lambda value: value.__setitem__("confidence", 1.1),
                ),
            )
        ],
        "dev_scope.v1": [
            (
                "repository_scope_without_repository",
                changed(
                    "dev_scope.v1", lambda value: value.__setitem__("repositories", [])
                ),
            )
        ],
        "dev_scope_resolution.v1": [
            (
                "ambiguous_without_candidates",
                changed(
                    "dev_scope_resolution.v1",
                    lambda value: (
                        value.__setitem__("outcome", "ambiguous"),
                        value.__setitem__("resolved_scope", None),
                    ),
                ),
            )
        ],
        "dev_tool_request.v1": [
            (
                "unknown_tool",
                changed(
                    "dev_tool_request.v1",
                    lambda value: value.__setitem__("tool_id", "execute_sql.v1"),
                ),
            )
        ],
        "dev_tool_result.v1": [
            (
                "oversized_result",
                changed(
                    "dev_tool_result.v1",
                    lambda value: value.__setitem__("serialized_bytes", 65_537),
                ),
            )
        ],
        "dev_feedback.v1": [
            (
                "unsupported_reason",
                changed(
                    "dev_feedback.v1",
                    lambda value: value.__setitem__("reasons", ["train_model"]),
                ),
            )
        ],
        "dev_stream_event.v1": [
            (
                "private_reasoning_payload",
                {
                    **deepcopy(positives["dev_stream_event.v1"]),
                    "delta": "hidden reasoning",
                },
            )
        ],
        "dev_error.v1": [
            (
                "provider_exception_text",
                {
                    **deepcopy(positives["dev_error.v1"]),
                    "provider_exception": "raw database error",
                },
            )
        ],
    }


def stream_fixtures() -> dict[str, list[dict[str, Any]]]:
    started = deepcopy(positive_fixtures()["dev_stream_event.v1"])
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
