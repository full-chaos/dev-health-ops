"""Deterministic OpenAI Chat Completions service for Ask Dev acceptance only.

This module is not a product provider family. It is launched only by the
Compose acceptance profile and exercises the production OpenAI-compatible
adapter over real HTTP.

CHAOS-3219 Phase 1 Lane 1b: a request whose (normalized) question text
matches one of the active role's scripted cases (see ``provider_scripts.py``'s
module docstring -- routing is by a hash of the question text itself, never
a marker embedded in it, so nothing acceptance-specific ever enters the
persisted transcript) is routed through the per-role, per-case scripted
decision/fault engine in ``provider_scripts``. A request whose question
matches no scripted case is indistinguishable from an ordinary one and falls
through to the pre-existing default heuristic unchanged -- every branch
below this point that does not mention ``provider_scripts`` is the original,
pre-CHAOS-3219 heuristic, preserved byte-for-byte so every existing smoke
script and unit test against this server keeps passing unmodified. The one
exception is the retired ``[[case:`` marker, reserved defensively: its mere
presence anywhere in a question always fails loud, never falls through --
see ``provider_scripts.LEGACY_CASE_TAG_MARKER``.
"""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from dev_health_ops.llm.agent import provider_scripts
from dev_health_ops.llm.providers.openai_capabilities import is_wire_legal_tool_name

SCRIPTED_OPENAI_MODEL = "ask-dev-scripted-v1"
# Sanitized (wire-legal) counterparts of the canonical dotted registry tool
# ids this scripted server simulates a model choosing among. Must track
# whatever OpenAICompatibleAgentProvider.sanitize_tool_name actually
# produces -- kept as simple literals here (not by importing the sanitize
# function) so this scripted acceptance surface independently reflects what
# a real model would be offered and could choose, rather than silently
# following any future change to the sanitize implementation (CHAOS-3286).
_WIRE_READINESS_ECHO = "readiness_echo_v1"
_WIRE_QUERY_METRIC = "query_metric_v1"
_WIRE_SEARCH_EVIDENCE = "search_evidence_v1"
_WIRE_DATA_HEALTH = "data_health_v1"
_WIRE_LIST_METRICS = "list_metrics_v1"

# CHAOS-3262: the literal metric-catalog acceptance question. When this exact
# question is observed, the script deterministically drives a single
# list_metrics.v1 tool call followed by a final answer, independent of the
# completed-work acceptance flow below.
LIST_METRICS_QUESTION = "Which Ask Dev metrics are available?"


def _tool_results_from_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    messages = payload.get("messages") or []
    tool_messages = [item for item in messages if item.get("role") == "tool"]
    if tool_messages:
        results: list[dict[str, Any]] = []
        for message in tool_messages:
            try:
                value = json.loads(message["content"])
            except (KeyError, TypeError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                results.append(value)
        return results

    for message in reversed(messages):
        if message.get("role") != "user":
            continue
        try:
            user_payload = json.loads(message.get("content") or "")
            items = user_payload["tool_results"]["items"]
        except (KeyError, TypeError, json.JSONDecodeError):
            continue
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return []


def _question_from_messages(payload: dict[str, Any]) -> str | None:
    for message in payload.get("messages") or []:
        if message.get("role") != "user":
            continue
        try:
            user_payload = json.loads(message.get("content") or "")
            question = user_payload["question"]
        except (KeyError, TypeError, json.JSONDecodeError):
            continue
        if isinstance(question, str):
            return question
    return None


_REPOSITORY_IDENTITY = re.compile(r"\b[\w][\w.-]*/[\w][\w.-]*\b")


def _evidence_query_from_question(question: str | None) -> str:
    """Derive the scripted ``search_evidence.v1`` query from the question.

    CHAOS-3300: this used to be a single literal ("meridian/web-app")
    regardless of the question asked, which made the organization-wide
    (no committed subject) half of the unrelated-evidence attack
    unprovable through this fixture -- native evidence search executes the
    provider's bounded query verbatim (``EvidenceService.search`` ->
    ``ClickHouseEvidenceSource.search``'s ``positionCaseInsensitiveUTF8``
    substring match), so a single-repository literal can never surface a
    second repository's evidence no matter what the model was asked.

    When the question names a repository identity (an ``owner/name``
    token, e.g. "meridian/web-app"), use that identity verbatim -- this
    keeps the existing named-subject negative control's behavior
    byte-for-byte identical to before. Otherwise the question is
    organization-wide, so use a query that is not restricted to one
    repository: every name in ``fixtures/demo_identity.py``'s
    ``DEMO_REPO_NAMES`` begins with "meridian/", and ``work_item_id`` is
    one of the columns ``native_evidence``'s work-item search matches on
    (``_search_predicate(("work_item_id", "title", ...))``) while carrying
    the full repository name -- the unrelated-evidence smoke confirmed
    that live, observing entity ids of the form "meridian/web-app-*". A
    "meridian" substring therefore reaches every fixture repository where
    the old full-identity literal could only ever reach one.

    This is a fixture provider, not a product path: what it establishes is
    that the fixture no longer BLOCKS the organization-wide half of the
    attack. The product-side property (an org-wide scope admits evidence
    from a repository it never named) is proven separately at
    ``EvidenceService.search`` -- see
    ``test_organization_wide_search_admits_evidence_from_multiple_repositories``.
    """
    if question:
        match = _REPOSITORY_IDENTITY.search(question)
        if match:
            return match.group(0)
    return "meridian"


def _list_metrics_script(
    tool_results: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    """Deterministically drive one list_metrics.v1 call, then answer.

    Only every reached once the literal CHAOS-3262 reproduction question is
    observed; see ``LIST_METRICS_QUESTION``.
    """
    result_tool_ids = {str(result.get("tool_id") or "") for result in tool_results}
    if "list_metrics.v1" not in result_tool_ids:
        message: dict[str, Any] = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "scripted-call-list-metrics-v1",
                    "type": "function",
                    "function": {
                        "name": _WIRE_LIST_METRICS,
                        "arguments": json.dumps({"limit": 8}, separators=(",", ":")),
                    },
                }
            ],
        }
        return message, "tool_calls"

    list_metrics_result = next(
        result for result in tool_results if result.get("tool_id") == "list_metrics.v1"
    )
    definitions = list_metrics_result.get("metric_definitions") or []
    available = str(list_metrics_result.get("status") or "unavailable") in {
        "success",
        "partial",
    } and bool(definitions)
    now = datetime.now(UTC).isoformat()
    value = {
        "schema_version": "dev_answer.v1",
        "answer_id": "acceptance-list-metrics-answer",
        "conversation_id": "acceptance-conversation",
        "generated_at": now,
        "resolved_scope": {},
        "as_of": now,
        "status": "complete" if available else "degraded",
        "direct_summary": (
            (
                f"{len(definitions)} Ask Dev metrics are available in this scope: "
                + ", ".join(
                    str(
                        definition.get("definition_version")
                        or definition.get("metric_id")
                    )
                    for definition in definitions
                )
                + "."
            )
            if available
            else "The Ask Dev metric catalog could not be read."
        ),
        "claims": [],
        "metrics": [],
        "evidence": [],
        "conflicts": [],
        "coverage": {
            "required_source_count": 1,
            "available_source_count": 1 if available else 0,
            "unavailable_required_sources": [] if available else ["list_metrics.v1"],
            "stale_required_sources": [],
            "as_of": now,
        },
        "warnings": [],
        "suggested_follow_up_questions": [],
        "versions": {},
        "model": {},
    }
    message = {
        "role": "assistant",
        "content": json.dumps(
            {"kind": "final_answer", "value": value}, separators=(",", ":")
        ),
    }
    return message, "stop"


def _requested_tool_names_from_messages(payload: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for message in payload.get("messages") or []:
        if message.get("role") != "assistant":
            continue
        for call in message.get("tool_calls") or []:
            function = call.get("function") or {}
            name = function.get("name")
            if isinstance(name, str):
                names.add(name)
    return names


def _acceptance_answer(tool_results: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    available_tool_ids = {
        str(result.get("tool_id"))
        for result in tool_results
        if str(result.get("status") or "unavailable") in {"success", "partial"}
    }
    metrics = [
        metric
        for result in tool_results
        for metric in result.get("metrics") or []
        if isinstance(metric, dict)
    ]
    evidence = [
        item
        for result in tool_results
        for item in result.get("evidence") or []
        if isinstance(item, dict)
    ]
    required_tool_ids = {"query_metric.v1", "search_evidence.v1"}
    required_tool_ids.add("data_health.v1")
    health_results = [
        result for result in tool_results if result.get("tool_id") == "data_health.v1"
    ]
    health_measured = bool(
        health_results and any(result.get("data_health") for result in health_results)
    )
    grounded = (
        required_tool_ids.issubset(available_tool_ids)
        and bool(metrics)
        and bool(evidence)
        and health_measured
    )
    unavailable = sorted(required_tool_ids - available_tool_ids)
    health_items = [
        item
        for result in health_results
        for item in result.get("data_health") or []
        if isinstance(item, dict)
    ]
    stale = (
        ["data_health.v1"]
        if any(item.get("freshness") == "stale" for item in health_items)
        else []
    )
    completed_metric = next(
        (metric for metric in metrics if metric.get("metric_id") == "items_completed"),
        None,
    )
    repository_evidence = next(
        (
            item
            for item in evidence
            if "meridian/web-app" in str(item.get("entity_id") or "")
        ),
        None,
    )
    answer_grounded = False
    claims: list[dict[str, Any]] = []
    if grounded and completed_metric is not None and repository_evidence is not None:
        current = completed_metric.get("value")
        comparison = completed_metric.get("comparison_value")
        if isinstance(current, int | float) and isinstance(comparison, int | float):
            answer_grounded = True
            direction = (
                "increased"
                if current > comparison
                else "decreased"
                if current < comparison
                else "was unchanged"
            )
            direct_summary = (
                f"Completed work {direction} from {comparison:g} to {current:g} items "
                "in the selected time range."
            )
            claims = [
                {
                    "schema_version": "dev_claim.v1",
                    "claim_id": "acceptance-items-completed-change",
                    "kind": "observed",
                    "text": direct_summary,
                    "confidence": 1.0,
                    "evidence_ref_ids": [repository_evidence["evidence_ref_id"]],
                    "metric_ref_ids": [completed_metric["metric_ref_id"]],
                    "validity_scope": completed_metric["resolved_scope"],
                    "flags": {
                        "stale": False,
                        "uncertain": False,
                        "conflicting": False,
                        "untrusted_source": False,
                    },
                    "recommendation_rule_version": None,
                }
            ]
    if not answer_grounded:
        direct_summary = (
            "Completed-work change could not be established from the required "
            "metric, evidence, and provider-health sources."
        )
    return {
        "schema_version": "dev_answer.v1",
        "answer_id": "acceptance-answer",
        "conversation_id": "acceptance-conversation",
        "generated_at": now,
        "resolved_scope": {},
        "as_of": now,
        "status": "partial" if answer_grounded else "degraded",
        "direct_summary": direct_summary,
        "claims": claims,
        "metrics": metrics,
        "evidence": evidence,
        "conflicts": [],
        "coverage": {
            "required_source_count": len(required_tool_ids),
            "available_source_count": len(required_tool_ids & available_tool_ids),
            "unavailable_required_sources": unavailable,
            "stale_required_sources": stale,
            "as_of": now,
        },
        "warnings": ["Deterministic scripted acceptance response."]
        + (
            ["Provider health was measured through data_health.v1."]
            if health_measured
            else []
        ),
        "suggested_follow_up_questions": [],
        "versions": {},
        "model": {},
    }


class ScriptedOpenAIHandler(BaseHTTPRequestHandler):
    server: ScriptedOpenAIServer

    def do_GET(self) -> None:
        if self.path != "/healthz":
            self.send_error(404)
            return
        encoded = b'{"status":"ready","script":"ask-dev-scripted-v1"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        if self.headers.get("Authorization") != f"Bearer {self.server.api_key}":
            self.send_error(401)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
        except (TypeError, ValueError, json.JSONDecodeError):
            self.send_error(400)
            return
        if not isinstance(payload, dict):
            self.send_error(400)
            return
        self.server.requests.append(payload)
        tool_names = [item["function"]["name"] for item in payload.get("tools") or []]
        # Enforce OpenAI's real function-name wire constraint (CHAOS-3286): a
        # regression that reintroduces a dotted (or otherwise illegal) tool
        # name must fail scripted deterministic acceptance exactly as it
        # would fail against the real API, not pass silently.
        illegal = [name for name in tool_names if not is_wire_legal_tool_name(name)]
        if illegal:
            self._write_json(
                400,
                {
                    "error": {
                        "message": f"Invalid 'tools[].function.name': {illegal[0]!r}",
                        "type": "invalid_request_error",
                        "param": "tools[].function.name",
                        "code": "invalid_value",
                    }
                },
            )
            return
        tool_results = _tool_results_from_messages(payload)
        requested_tool_names = _requested_tool_names_from_messages(payload)
        result_tool_ids = {str(result.get("tool_id") or "") for result in tool_results}
        question = _question_from_messages(payload)

        # CHAOS-3219 Phase 1 Lane 1b: the retired [[case: marker is reserved
        # defensively -- ANY occurrence (well-formed, malformed, truncated,
        # duplicated) always fails loud, never falls through. A pure string
        # check: no file I/O, so it can never be skipped due to a missing
        # scripts directory the way question-hash routing below can be.
        if question is not None and provider_scripts.LEGACY_CASE_TAG_MARKER in question:
            self._write_unmapped_case_error(
                provider_scripts.UnmappedCaseError(
                    "legacy_case_tag_marker_present",
                    "questions may not contain the retired "
                    f"{provider_scripts.LEGACY_CASE_TAG_MARKER!r} marker; route "
                    "scripted cases by exact question text instead (see "
                    "provider-scripts/README.md)",
                )
            )
            return

        # Question-hash routing: a question whose normalized text matches no
        # scripted case (the routine outcome for every non-corpus question,
        # incl. every pre-CHAOS-3219 smoke/oracle/probe question) is
        # indistinguishable from an ordinary one and falls straight through
        # to the untagged heuristic below, completely unchanged.
        if question is not None:
            engine = provider_scripts.try_load_engine(provider_scripts.current_role())
            if engine is not None:
                try:
                    resolution = engine.resolve(question, round_index=len(tool_results))
                except provider_scripts.UnmappedCaseError as exc:
                    self._write_unmapped_case_error(exc)
                    return
                if resolution is not None and not isinstance(resolution, str):
                    self._serve_scripted_turn(payload, resolution, tool_names)
                    return
                # `resolution` is DELEGATE_DEFAULT or None (no scripted case
                # matched this question) -- fall through to the untagged
                # heuristic below, question text unchanged either way.

        if question == LIST_METRICS_QUESTION:
            list_metrics_message, list_metrics_finish_reason = _list_metrics_script(
                tool_results
            )
            self._send_completion(
                payload, list_metrics_message, list_metrics_finish_reason
            )
            return
        if not tool_results:
            arguments: dict[str, Any]
            if _WIRE_READINESS_ECHO in tool_names:
                tool_name = _WIRE_READINESS_ECHO
                arguments = {"nonce": "ready-v1"}
            elif _WIRE_QUERY_METRIC in tool_names:
                tool_name = _WIRE_QUERY_METRIC
                arguments = {
                    "metric_id": "items_completed",
                    "include_comparison": True,
                    "limit": 12,
                }
            else:
                tool_name = "status_snapshot_v1"
                arguments = {}
            message: dict[str, Any] = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "scripted-call-v1",
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "arguments": json.dumps(arguments, separators=(",", ":")),
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        elif (
            _WIRE_READINESS_ECHO not in tool_names
            and "query_metric.v1" in result_tool_ids
            and "search_evidence.v1" not in result_tool_ids
            and _WIRE_SEARCH_EVIDENCE in tool_names
        ):
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "scripted-call-search-evidence-v1",
                        "type": "function",
                        "function": {
                            "name": _WIRE_SEARCH_EVIDENCE,
                            "arguments": json.dumps(
                                {
                                    # See _evidence_query_from_question: a named
                                    # repository identity when the question names
                                    # one, otherwise an org-wide query that is not
                                    # restricted to a single repository.
                                    "query": _evidence_query_from_question(question),
                                    "limit": 25,
                                },
                                separators=(",", ":"),
                            ),
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        elif (
            _WIRE_READINESS_ECHO not in tool_names
            and "query_metric.v1" in result_tool_ids
            and "search_evidence.v1" in result_tool_ids
            and "data_health.v1" not in result_tool_ids
            and _WIRE_DATA_HEALTH in tool_names
        ):
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "scripted-call-data-health-v1",
                        "type": "function",
                        "function": {
                            "name": _WIRE_DATA_HEALTH,
                            "arguments": "{}",
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        else:
            value = (
                {"nonce": "ready-v1"}
                if _WIRE_READINESS_ECHO in requested_tool_names
                else _acceptance_answer(tool_results)
            )
            message = {
                "role": "assistant",
                "content": json.dumps(
                    {"kind": "final_answer", "value": value},
                    separators=(",", ":"),
                ),
            }
            finish_reason = "stop"
        self._send_completion(payload, message, finish_reason)

    def _write_json(
        self,
        status: int,
        payload: dict[str, Any],
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(encoded)

    def _write_unmapped_case_error(
        self, exc: provider_scripts.UnmappedCaseError
    ) -> None:
        """CHAOS-3219 Phase 1 Lane 1b requirement 4: an unmapped case tag
        must never accidentally pass as a generic canned answer. Always a
        distinct HTTP 422 with a ``type`` no legitimate wire-protocol error
        this server sends ever uses (``invalid_request_error`` is the
        wire-legal-tool-name 400 above; every scripted fault below uses
        ``scripted_provider_fault``), so a corpus runner or conformance test
        can tell "your case id/script is wrong" apart from both.
        """

        self._write_json(
            422,
            {
                "error": {
                    "type": "scripted_provider_unmapped_case",
                    "code": exc.code,
                    "message": exc.message,
                }
            },
        )

    def _serve_scripted_turn(
        self,
        payload: dict[str, Any],
        turn: provider_scripts.ScriptTurn,
        tool_names: list[str],
    ) -> None:
        provider_scripts.sleep_for_fault(turn.delay_ms)
        if turn.http_error is not None:
            extra_headers = (
                {"Retry-After": str(int(turn.http_error.retry_after_seconds))}
                if turn.http_error.retry_after_seconds is not None
                else None
            )
            self._write_json(
                turn.http_error.status,
                {
                    "error": {
                        "type": "scripted_provider_fault",
                        "code": turn.http_error.code,
                        "message": turn.http_error.message,
                    }
                },
                extra_headers=extra_headers,
            )
            return

        decision = turn.decision
        message: dict[str, Any]
        finish_reason: str
        if isinstance(decision, provider_scripts.ToolCallDecision):
            if decision.tool not in tool_names:
                # The script asked for a tool the client never offered on
                # this round -- a script/production drift, not a real
                # provider failure. Fail loud rather than send a tool_call
                # the client cannot possibly service.
                self._write_unmapped_case_error(
                    provider_scripts.UnmappedCaseError(
                        "scripted_tool_not_offered",
                        f"scripted decision requested tool {decision.tool!r}, "
                        f"which was not among the offered tools {tool_names!r}",
                    )
                )
                return
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "scripted-call-provider-scripts-v1",
                        "type": "function",
                        "function": {
                            "name": decision.tool,
                            "arguments": json.dumps(
                                dict(decision.arguments), separators=(",", ":")
                            ),
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        elif isinstance(decision, provider_scripts.FinalAnswerDecision):
            message = {
                "role": "assistant",
                "content": json.dumps(
                    {"kind": "final_answer", "value": dict(decision.value)},
                    separators=(",", ":"),
                ),
            }
            finish_reason = "stop"
        elif isinstance(decision, provider_scripts.DisambiguationDecision):
            message = {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "kind": "disambiguation",
                        "prompt": decision.prompt,
                        "candidates": list(decision.candidates),
                    },
                    separators=(",", ":"),
                ),
            }
            finish_reason = "stop"
        else:
            assert isinstance(decision, provider_scripts.RefusalDecision)  # noqa: S101
            message = {
                "role": "assistant",
                "content": json.dumps(
                    {
                        "kind": "refusal",
                        "code": decision.code,
                        "message": decision.message,
                    },
                    separators=(",", ":"),
                ),
            }
            finish_reason = "stop"
        self._send_completion(payload, message, finish_reason)

    def _send_completion(
        self,
        request: dict[str, Any],
        message: dict[str, Any],
        finish_reason: str,
    ) -> None:
        response = {
            "id": f"chatcmpl-{len(self.server.requests)}",
            "object": "chat.completion",
            "created": 1_785_283_200,
            "model": request.get("model", SCRIPTED_OPENAI_MODEL),
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": finish_reason,
                }
            ],
            "usage": {
                "prompt_tokens": 7,
                "completion_tokens": 5,
                "total_tokens": 12,
            },
        }
        self._write_json(200, response)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class ScriptedOpenAIServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        api_key: str,
        server_address: tuple[str, int] = ("127.0.0.1", 0),
    ) -> None:
        super().__init__(server_address, ScriptedOpenAIHandler)
        self.api_key = api_key
        self.requests: list[dict[str, Any]] = []


def main() -> None:
    if (
        os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"
        or os.getenv("ENVIRONMENT", "").strip().lower() != "acceptance"
    ):
        raise SystemExit(
            "scripted OpenAI service requires ASK_DEV_LIVE_ACCEPTANCE=1 "
            "and ENVIRONMENT=acceptance"
        )
    api_key = os.getenv("ASK_DEV_ACCEPTANCE_OPENAI_API_KEY", "")
    if not api_key:
        raise SystemExit("ASK_DEV_ACCEPTANCE_OPENAI_API_KEY is required")
    port_text = os.getenv("ASK_DEV_ACCEPTANCE_OPENAI_PORT", "8001")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise SystemExit("ASK_DEV_ACCEPTANCE_OPENAI_PORT must be an integer") from exc
    if not 1 <= port <= 65_535:
        raise SystemExit("ASK_DEV_ACCEPTANCE_OPENAI_PORT is outside 1..65535")
    server = ScriptedOpenAIServer(api_key, ("0.0.0.0", port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()


__all__ = [
    "LIST_METRICS_QUESTION",
    "SCRIPTED_OPENAI_MODEL",
    "ScriptedOpenAIHandler",
    "ScriptedOpenAIServer",
    "main",
]
