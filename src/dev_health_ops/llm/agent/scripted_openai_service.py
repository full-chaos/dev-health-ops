"""Deterministic OpenAI Chat Completions service for Ask Dev acceptance only.

This module is not a product provider family. It is launched only by the
Compose acceptance profile and exercises the production OpenAI-compatible
adapter over real HTTP.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

SCRIPTED_OPENAI_MODEL = "ask-dev-scripted-v1"


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
        tool_results = _tool_results_from_messages(payload)
        requested_tool_names = _requested_tool_names_from_messages(payload)
        result_tool_ids = {str(result.get("tool_id") or "") for result in tool_results}
        if not tool_results:
            arguments: dict[str, Any]
            if "readiness_echo" in tool_names:
                tool_name = "readiness_echo"
                arguments = {"nonce": "ready-v1"}
            elif "query_metric.v1" in tool_names:
                tool_name = "query_metric.v1"
                arguments = {
                    "metric_id": "items_completed",
                    "include_comparison": True,
                    "limit": 12,
                }
            else:
                tool_name = "status_snapshot.v1"
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
            "readiness_echo" not in tool_names
            and "query_metric.v1" in result_tool_ids
            and "search_evidence.v1" not in result_tool_ids
            and "search_evidence.v1" in tool_names
        ):
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "scripted-call-search-evidence-v1",
                        "type": "function",
                        "function": {
                            "name": "search_evidence.v1",
                            "arguments": json.dumps(
                                {
                                    # The acceptance corpus guarantees this repository
                                    # identity in native work-item IDs and titles. Native
                                    # evidence search deliberately executes the provider's
                                    # bounded query verbatim, so use the fixture identity
                                    # instead of an ungrounded natural-language phrase.
                                    "query": "meridian/web-app",
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
            "readiness_echo" not in tool_names
            and "query_metric.v1" in result_tool_ids
            and "search_evidence.v1" in result_tool_ids
            and "data_health.v1" not in result_tool_ids
            and "data_health.v1" in tool_names
        ):
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "scripted-call-data-health-v1",
                        "type": "function",
                        "function": {
                            "name": "data_health.v1",
                            "arguments": "{}",
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        else:
            value = (
                {"nonce": "ready-v1"}
                if "readiness_echo" in requested_tool_names
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
        encoded = json.dumps(response, separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

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
    "SCRIPTED_OPENAI_MODEL",
    "ScriptedOpenAIHandler",
    "ScriptedOpenAIServer",
    "main",
]
