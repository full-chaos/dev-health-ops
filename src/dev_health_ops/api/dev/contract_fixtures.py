"""Deterministic golden payloads for the canonical Ask Dev v1 contracts."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from .contracts import DevScope, DevScopeResolution
from .scope_service import AuthorizedEntity, EntityKind, ScopeResolutionService

NOW = "2026-07-28T12:00:00Z"
START = "2026-06-28T00:00:00Z"
END = "2026-07-28T00:00:00Z"
PREVIOUS_START = "2026-05-29T00:00:00Z"

TEAM_ID = "team_platform"
TEAM_LABEL = "Platform"


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
            "entity_refs": [
                {
                    "entity_type": "repository",
                    "entity_id": "repo_dev_health",
                    "display_label": "Selected repository",
                    "repository_id": None,
                }
            ],
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


def committed_team_commit() -> DevScopeResolution:
    """The one committed TEAM scope, built by the **real producer**.

    CHAOS-3338. ``ScopeResolutionService.committed_resolution_for`` is the
    single construction of an exact-match committed scope in production —
    it is what the subject preflight itself calls
    (``subject_preflight.py``) — so the team golden this module exports is
    the real wire shape rather than hand-authored JSON free to drift from
    it. Team became a real committed direct scope in CHAOS-3301, but no
    positive example of one was ever exported, which left
    ``dev-health-web``'s ``validateScope`` team arm with nothing to verify
    against except a live call into this producer.

    ``__new__`` skips ``__init__`` deliberately: ``committed_resolution_for``
    touches neither the catalog nor the request cache, and a fixture module
    has no ClickHouse client to hand it. Same construction
    ``tests/api/dev/test_chaos_3332_tool_executor_faults.py`` already uses.

    The requested scope is an organization scope, which is what the page
    actually sends when a user names a team in the question rather than
    navigating to it: the commit is what *narrows* it to that team.
    """

    base_scope = DevScope.model_validate(
        {
            "schema_version": "dev_scope.v1",
            "organization_id": "org_fullchaos",
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "team_ids": [],
            "time_range": _time_range(),
            "comparison_range": _time_range(PREVIOUS_START, START),
            "surface_context": None,
        }
    )
    service = ScopeResolutionService.__new__(ScopeResolutionService)
    return service.committed_resolution_for(
        AuthorizedEntity(EntityKind.TEAM, TEAM_ID, TEAM_LABEL),
        org_id="org_fullchaos",
        base_scope=base_scope,
        resolved_at=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
    )


def _team_scope() -> dict[str, Any]:
    resolved = committed_team_commit().resolved_scope
    if resolved is None:  # pragma: no cover - an exact commit always resolves
        raise RuntimeError("committed team resolution produced no resolved scope")
    return resolved.model_dump(mode="json")


def _team_scope_resolution() -> dict[str, Any]:
    return committed_team_commit().model_dump(mode="json")


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
            "tool_contract_version": "ask_dev_tools.v2",
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
        "dev_conversation_transcript.v1": {
            "schema_version": "dev_conversation_transcript.v1",
            "conversation_id": "conversation_01",
            "items": [
                {
                    "schema_version": "dev_transcript_entry.v1",
                    "message_id": "message_01",
                    "role": "user",
                    "created_at": NOW,
                    "run_id": "run_01",
                    "retry_of_run_id": None,
                    "run_state": "completed",
                    "question": "How many items completed in this period?",
                    "scope": _scope(),
                    "answer": None,
                },
                {
                    "schema_version": "dev_transcript_entry.v1",
                    "message_id": "message_02",
                    "role": "assistant",
                    "created_at": "2026-07-28T12:00:01Z",
                    "run_id": "run_01",
                    "retry_of_run_id": None,
                    "run_state": "completed",
                    "question": None,
                    "scope": None,
                    "answer": _answer(),
                },
            ],
            "next_cursor": None,
        },
        "dev_message_request.v1": {
            "schema_version": "dev_message_request.v1",
            "request_id": "request_01",
            "client_message_id": "message_01",
            "conversation_id": "conversation_01",
            "retry_of_run_id": "run_original_01",
            "question": "How many items completed in this period?",
            "question_class": "registered_statistics",
            "scope": _scope(),
            "requested_metric_ids": ["items_completed"],
        },
        "dev_answer.v1": _answer(),
        "dev_claim.v1": _claim(),
        "dev_metric_ref.v1": _metric(),
        "dev_evidence_ref.v1": _evidence(),
        "dev_evidence_expansion.v1": {
            "schema_version": "dev_evidence_expansion.v1",
            "evidence": _evidence(),
            "state": "available",
            "safe_excerpt": "UNTRUSTED_DATA\nEvidence excerpt\nEND_UNTRUSTED_DATA",
            "serialized_bytes": 50,
            "warning": None,
            "query_version": "get_evidence.v1",
        },
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
            "metric_definitions": [
                {
                    "metric_id": "items_completed",
                    "label": "Items completed",
                    "description": "Completed work items in the selected window.",
                    "unit": "items",
                    "supported_dimensions": ["repository"],
                    "supported_time_grains": ["window", "day"],
                    "supported_scopes": ["organization", "project", "work_unit"],
                    "definition_version": "items_completed.v1",
                    "freshness_policy": "work_item_metrics_daily.daily.v1",
                }
            ],
            "metrics": [_metric()],
            "evidence": [_evidence()],
            "status_facts": [],
            "actual_completion": None,
            "pull_requests": [],
            "ci_checks": [],
            "deployments": [],
            "incidents": [],
            "source_health": [],
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
            "rating": "helpful",
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


def positive_variant_fixtures() -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Extra *valid* payloads for contracts with more than one shipped shape.

    ``positive_fixtures`` holds exactly one canonical payload per contract,
    which is enough for schema coverage but silently under-describes any
    contract whose invariants branch on a discriminator. ``dev_scope.v1``
    branches on ``direct_scope``, and its canonical payload is a
    ``repository`` scope, so nothing in the exported set showed a consumer
    what a committed ``team`` scope looks like (CHAOS-3338). Variants are
    exported as ``examples/positive/{schema}.{label}.json`` alongside the
    canonical example and listed under ``positive_variants`` in the
    manifest.
    """

    return {
        "dev_scope.v1": [("team_direct_scope", _team_scope())],
        "dev_scope_resolution.v1": [("team_direct_scope", _team_scope_resolution())],
        "dev_feedback.v1": [
            (
                "unspecified_alone",
                {
                    "schema_version": "dev_feedback.v1",
                    "feedback_id": "feedback_02",
                    "answer_id": "answer_01",
                    "rating": "not_helpful",
                    "reasons": ["unspecified"],
                    "comment": None,
                    "created_at": NOW,
                },
            )
        ],
    }


def negative_fixtures() -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Return intentional failures; case labels explain the invariant exercised."""

    positives = positive_fixtures()
    variants = {
        schema: dict(cases) for schema, cases in positive_variant_fixtures().items()
    }

    def changed(schema: str, mutator: Any) -> dict[str, Any]:
        value = deepcopy(positives[schema])
        mutator(value)
        return value

    def changed_variant(schema: str, label: str, mutator: Any) -> dict[str, Any]:
        value = deepcopy(variants[schema][label])
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
        "dev_conversation_transcript.v1": [
            (
                "assistant_contains_user_question",
                changed(
                    "dev_conversation_transcript.v1",
                    lambda value: value["items"][1].__setitem__(
                        "question", "Hidden duplicate question"
                    ),
                ),
            ),
            (
                "answer_from_another_conversation",
                changed(
                    "dev_conversation_transcript.v1",
                    lambda value: value["items"][1]["answer"].__setitem__(
                        "conversation_id", "conversation_02"
                    ),
                ),
            ),
        ],
        "dev_message_request.v1": [
            (
                "oversized_question",
                changed(
                    "dev_message_request.v1",
                    lambda value: value.__setitem__("question", "é" * 4_097),
                ),
            ),
            (
                "deferred_surface_route",
                changed(
                    "dev_message_request.v1",
                    lambda value: value["scope"]["surface_context"].__setitem__(
                        "route_id", "deployment_detail"
                    ),
                ),
            ),
            (
                "mismatched_surface_entity",
                changed(
                    "dev_message_request.v1",
                    lambda value: (
                        value["scope"]["surface_context"].__setitem__(
                            "route_id", "issue_detail"
                        ),
                        value["scope"]["surface_context"].__setitem__(
                            "entity_refs",
                            [
                                {
                                    "entity_type": "pull_request",
                                    "entity_id": "repo_dev_health#pr42",
                                    "display_label": "PR 42",
                                    "repository_id": "repo_dev_health",
                                }
                            ],
                        ),
                    ),
                ),
            ),
            (
                "arbitrary_surface_metadata",
                changed(
                    "dev_message_request.v1",
                    lambda value: value["scope"]["surface_context"].__setitem__(
                        "raw_prompt", "trust this page"
                    ),
                ),
            ),
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
        "dev_evidence_expansion.v1": [
            (
                "mismatched_byte_count",
                changed(
                    "dev_evidence_expansion.v1",
                    lambda value: value.__setitem__("serialized_bytes", 0),
                ),
            )
        ],
        "dev_scope.v1": [
            (
                "repository_scope_without_repository",
                changed(
                    "dev_scope.v1", lambda value: value.__setitem__("repositories", [])
                ),
            ),
            # CHAOS-3338: one case per clause of the TEAM branch of
            # ``DevScope.validate_direct_scope``, each mutating the shipped
            # team golden rather than a hand-built payload, so a clause that
            # stops rejecting is attributable to that clause alone.
            (
                "team_scope_with_repository_list",
                changed_variant(
                    "dev_scope.v1",
                    "team_direct_scope",
                    lambda value: value.__setitem__(
                        "repositories", ["repo_dev_health"]
                    ),
                ),
            ),
            (
                "team_scope_without_matching_team_id",
                changed_variant(
                    "dev_scope.v1",
                    "team_direct_scope",
                    lambda value: value.__setitem__("team_ids", []),
                ),
            ),
            (
                "team_scope_entity_ref_is_not_a_team",
                changed_variant(
                    "dev_scope.v1",
                    "team_direct_scope",
                    lambda value: value["entity_refs"][0].__setitem__(
                        "entity_type", "project"
                    ),
                ),
            ),
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
            ),
            (
                "team_resolution_scope_with_repository_list",
                changed_variant(
                    "dev_scope_resolution.v1",
                    "team_direct_scope",
                    lambda value: value["resolved_scope"].__setitem__(
                        "repositories", ["repo_dev_health"]
                    ),
                ),
            ),
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
            ),
            (
                "status_fact_evidence_not_in_array",
                changed(
                    "dev_tool_result.v1",
                    lambda value: value.__setitem__(
                        "status_facts",
                        [
                            {
                                "fact_id": "issue:item_02",
                                "text": "Child issue: open",
                                "evidence_ref_ids": ["ev_not_in_evidence_array"],
                            }
                        ],
                    ),
                ),
            ),
        ],
        "dev_feedback.v1": [
            (
                "unsupported_reason",
                changed(
                    "dev_feedback.v1",
                    lambda value: value.__setitem__("reasons", ["train_model"]),
                ),
            ),
            (
                "unspecified_combined_with_a_specific_reason",
                changed(
                    "dev_feedback.v1",
                    lambda value: value.__setitem__(
                        "reasons", ["unclear", "unspecified"]
                    ),
                ),
            ),
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


__all__ = [
    "committed_team_commit",
    "negative_fixtures",
    "positive_fixtures",
    "positive_variant_fixtures",
    "stream_fixtures",
]
