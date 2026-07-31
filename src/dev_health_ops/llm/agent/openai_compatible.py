"""OpenAI-compatible adapter for Ask Dev's provider-neutral agent contract."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any, cast

from dev_health_ops.llm.providers._http import make_hardened_async_httpx_client
from dev_health_ops.llm.providers.openai_capabilities import (
    chat_completion_reasoning_effort,
    supports_parallel_tool_calls,
    supports_temperature,
)

from .contracts import (
    AgentDecisionResult,
    AgentDisambiguation,
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentProviderCapabilities,
    AgentRefusal,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
    CancellationSignal,
    StreamingMode,
    StructuredOutputMode,
    ToolDecisionMode,
)
from .errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)

READINESS_VERSION = "ask-dev-agent-v3"
"""Bumped v2 -> v3 for CHAOS-3254: the outbound wire contract changed (native
tool requests now send ``parallel_tool_calls``, gated by model family -- see
``supports_parallel_tool_calls``). A v2-certified endpoint has never been
asked to accept the new parameter, so it must be re-certified rather than
treated as still current. ``provider_fingerprint`` folds this constant in,
so bumping it invalidates every existing stored readiness record for every
provider instance (see ``AgentReadinessRecord.is_current``)."""
PLATFORM_PRICE_BOOK_VERSION = "openai-2026-07-29"

_DECISION_FIELDS = frozenset(
    {
        "kind",
        "tool_id",
        "arguments",
        "call_id",
        "value",
        "prompt",
        "candidates",
        "code",
        "message",
    }
)


class _SequentialToolContractViolation(ValueError):
    """A provider returned more than one native tool call in one decision.

    Ask Dev's runtime is a sequential bounded state machine: exactly one tool
    request per model decision. This is distinct from other malformed/invalid
    provider responses (``AgentProviderErrorCode.INVALID_RESPONSE``) because
    it must be classified as a stable provider/decision contract error rather
    than an opaque application ``internal_error`` (CHAOS-3254).
    """


_STRUCTURAL_SCHEMA_KEYS = frozenset(
    {
        "$defs",
        "$ref",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "anyOf",
        "enum",
        "const",
    }
)

# Server-owned conservative prices in microUSD per million tokens. Cached input
# is intentionally charged at the full input rate because the normalized usage
# contract does not expose cached-token detail.
# Source snapshot: https://developers.openai.com/api/docs/models/gpt-5-mini
_PLATFORM_MODEL_PRICES: dict[str, tuple[int, int]] = {
    "gpt-5-mini": (250_000, 2_000_000),
}


def _fingerprint(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()[:24]


def _estimated_cost_microusd(
    *, model: str, input_tokens: int, output_tokens: int
) -> int | None:
    canonical_model = next(
        (
            known
            for known in _PLATFORM_MODEL_PRICES
            if model == known or model.startswith(f"{known}-")
        ),
        None,
    )
    if canonical_model is None:
        return None
    input_rate, output_rate = _PLATFORM_MODEL_PRICES[canonical_model]
    numerator = input_tokens * input_rate + output_tokens * output_rate
    return (numerator + 999_999) // 1_000_000


class OpenAICompatibleAgentProvider:
    """Normalize native and JSON tool decisions from OpenAI-compatible endpoints."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str | None = None,
        client: Any | None = None,
        disclosure_key: str = "openai_compatible",
        context_window_tokens: int | None = None,
    ) -> None:
        self.model = model
        self.base_url = base_url or ""
        self._http_client: Any | None = None
        if client is None:
            from openai import AsyncOpenAI

            self._http_client = make_hardened_async_httpx_client()
            client = AsyncOpenAI(
                api_key=api_key,
                base_url=base_url or None,
                http_client=self._http_client,
            )
        self._client = client
        self._capabilities = AgentProviderCapabilities(
            structured_output=StructuredOutputMode.JSON_SCHEMA,
            tool_decisions=ToolDecisionMode.NATIVE,
            streaming=StreamingMode.BUFFERED,
            supports_cancellation=True,
            context_window_tokens=context_window_tokens,
            max_output_tokens=None,
            readiness_version=READINESS_VERSION,
            disclosure_key=disclosure_key,
        )

    @property
    def capabilities(self) -> AgentProviderCapabilities:
        return self._capabilities

    @property
    def provider_fingerprint(self) -> str:
        return _fingerprint("openai-compatible", self.base_url, READINESS_VERSION)

    @property
    def model_fingerprint(self) -> str:
        return _fingerprint(self.provider_fingerprint, self.model)

    async def decide(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[AgentToolDefinition],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: CancellationSignal | None = None,
    ) -> AgentDecisionResult:
        if signal is not None and signal.is_cancelled():
            raise AgentProviderError(
                AgentProviderErrorCode.CANCELLED,
                provider_dispatched=False,
            )
        started = time.monotonic()
        create_completion = cast(Any, self._client.chat.completions.create)
        allow_final_answer = not tools or any(
            message.role is AgentMessageRole.TOOL for message in messages
        )
        completion_kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [self._message_payload(item) for item in messages],
            "tools": [self._tool_payload(item) for item in tools] or None,
            "tool_choice": (
                ("auto" if allow_final_answer else "required") if tools else None
            ),
            "max_completion_tokens": max_output_tokens,
        }
        if tools and supports_parallel_tool_calls(self.model):
            # Ask Dev's runtime is a sequential one-decision-per-round state
            # machine (TRD 11-12, 20.4). Explicitly disabling native parallel
            # tool calls keeps a standards-compliant OpenAI-compatible model
            # from returning multiple tool calls in one response, which the
            # normalizer must otherwise reject (CHAOS-3254).
            completion_kwargs["parallel_tool_calls"] = False
        if supports_temperature(self.model):
            completion_kwargs["temperature"] = 0
        reasoning_effort = chat_completion_reasoning_effort(self.model)
        if reasoning_effort is not None:
            completion_kwargs["reasoning_effort"] = reasoning_effort
        if allow_final_answer:
            completion_kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "ask_dev_decision",
                    "strict": True,
                    "schema": self._decision_response_schema(
                        self._answer_draft_schema(response_schema),
                        tools,
                        allow_final_answer=True,
                    ),
                },
            }
        provider_task = asyncio.create_task(create_completion(**completion_kwargs))
        cancel_task = asyncio.create_task(signal.wait()) if signal is not None else None
        try:
            waiters = {provider_task}
            if cancel_task is not None:
                waiters.add(cancel_task)
            done, _ = await asyncio.wait(
                waiters, timeout=timeout_seconds, return_when=asyncio.FIRST_COMPLETED
            )
            if cancel_task is not None and cancel_task in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
            if provider_task not in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
            response = await provider_task
        except AgentProviderError:
            raise
        except Exception as exc:
            raise safe_agent_provider_error(exc) from None
        finally:
            if cancel_task is not None:
                cancel_task.cancel()
                await asyncio.gather(cancel_task, return_exceptions=True)

        try:
            decision = self._normalize_response(
                response, allowed_tool_ids=frozenset(item.tool_id for item in tools)
            )
        except _SequentialToolContractViolation:
            raise AgentProviderError(
                AgentProviderErrorCode.PROVIDER_CONTRACT_VIOLATION
            ) from None
        except (
            AttributeError,
            IndexError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE) from None
        usage = getattr(response, "usage", None)
        input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        prompt_details = getattr(usage, "prompt_tokens_details", None)
        cached_tokens = getattr(prompt_details, "cached_tokens", None)
        return AgentDecisionResult(
            decision=decision,
            usage=AgentUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_input_tokens=(
                    int(cached_tokens) if cached_tokens is not None else None
                ),
                estimated_cost_microusd=_estimated_cost_microusd(
                    model=self.model,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                ),
            ),
            latency_ms=max(0, round((time.monotonic() - started) * 1000)),
            provider_fingerprint=self.provider_fingerprint,
            model_fingerprint=self.model_fingerprint,
        )

    @staticmethod
    def _message_payload(message: AgentMessage) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "role": message.role.value,
            "content": message.content,
        }
        if message.role is AgentMessageRole.TOOL:
            if not message.tool_call_id:
                raise ValueError("tool messages require tool_call_id")
            payload["tool_call_id"] = message.tool_call_id
        if message.tool_request is not None:
            if message.role is not AgentMessageRole.ASSISTANT:
                raise ValueError("tool requests require an assistant message")
            payload["tool_calls"] = [
                {
                    "id": message.tool_request.call_id,
                    "type": "function",
                    "function": {
                        "name": message.tool_request.tool_id,
                        "arguments": json.dumps(
                            dict(message.tool_request.arguments),
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    },
                }
            ]
        return payload

    @staticmethod
    def _tool_payload(tool: AgentToolDefinition) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": tool.tool_id,
                "description": tool.description,
                "parameters": OpenAICompatibleAgentProvider._structural_schema(
                    tool.input_schema
                ),
                "strict": True,
            },
        }

    @staticmethod
    def _decision_response_schema(
        response_schema: Mapping[str, Any],
        tools: Sequence[AgentToolDefinition],
        *,
        allow_final_answer: bool = True,
    ) -> dict[str, Any]:
        """Describe every decision in OpenAI's supported root-object subset."""

        final_value_schema = OpenAICompatibleAgentProvider._structural_schema(
            response_schema
        )
        definitions: dict[str, Any] = {}
        OpenAICompatibleAgentProvider._merge_definitions(
            definitions, final_value_schema.pop("$defs", None)
        )
        argument_schemas: list[dict[str, Any]] = []
        for tool in tools:
            arguments_schema = OpenAICompatibleAgentProvider._structural_schema(
                tool.input_schema
            )
            OpenAICompatibleAgentProvider._merge_definitions(
                definitions, arguments_schema.pop("$defs", None)
            )
            argument_schemas.append(arguments_schema)

        nullable_arguments: dict[str, Any]
        if argument_schemas:
            nullable_arguments = {"anyOf": [*argument_schemas, {"type": "null"}]}
        else:
            nullable_arguments = {"type": "null"}

        schema: dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "required": sorted(_DECISION_FIELDS),
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": [
                        *(["tool_request"] if tools else []),
                        *(["final_answer"] if allow_final_answer else []),
                        "disambiguation",
                        "refusal",
                    ],
                },
                "tool_id": {
                    "type": ["string", "null"],
                    "enum": [*(tool.tool_id for tool in tools), None],
                },
                "arguments": nullable_arguments,
                "call_id": {
                    "anyOf": [
                        {"type": "string", "minLength": 1, "maxLength": 256},
                        {"type": "null"},
                    ]
                },
                "value": (
                    {"anyOf": [final_value_schema, {"type": "null"}]}
                    if allow_final_answer
                    else {"type": "null"}
                ),
                "prompt": {"type": ["string", "null"]},
                "candidates": {
                    "anyOf": [
                        {"type": "array", "items": {"type": "string"}},
                        {"type": "null"},
                    ]
                },
                "code": {"type": ["string", "null"]},
                "message": {"type": ["string", "null"]},
            },
        }
        if definitions:
            schema["$defs"] = definitions
        return OpenAICompatibleAgentProvider._structural_schema(schema)

    @staticmethod
    def _structural_schema(schema: Mapping[str, Any]) -> dict[str, Any]:
        """Project runtime-validated JSON Schema into provider grammar syntax."""

        def project(node: Any) -> Any:
            if isinstance(node, list):
                return [project(item) for item in node]
            if not isinstance(node, Mapping):
                return node
            result: dict[str, Any] = {}
            for key, value in node.items():
                if key not in _STRUCTURAL_SCHEMA_KEYS:
                    continue
                if key in {"$defs", "properties"} and isinstance(value, Mapping):
                    result[key] = {
                        str(name): project(definition)
                        for name, definition in value.items()
                    }
                else:
                    result[key] = project(value)
            properties = result.get("properties")
            if isinstance(properties, Mapping):
                result["additionalProperties"] = False
                result["required"] = sorted(str(name) for name in properties)
            return result

        return cast(dict[str, Any], project(schema))

    @staticmethod
    def _answer_draft_schema(schema: Mapping[str, Any]) -> Mapping[str, Any]:
        """Remove server-owned/defaulted DevAnswer fields from provider grammar."""

        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return schema
        draft_fields = {
            "status",
            "direct_summary",
        }
        if not draft_fields.issubset(properties):
            return schema

        selected_properties = {
            field: properties[field] for field in sorted(draft_fields)
        }
        definitions = schema.get("$defs")
        selected_definitions: dict[str, Any] = {}
        pending: list[Any] = list(selected_properties.values())
        while pending:
            node = pending.pop()
            if isinstance(node, Mapping):
                reference = node.get("$ref")
                if isinstance(reference, str) and reference.startswith("#/$defs/"):
                    name = reference.removeprefix("#/$defs/")
                    if (
                        name not in selected_definitions
                        and isinstance(definitions, Mapping)
                        and name in definitions
                    ):
                        definition = definitions[name]
                        selected_definitions[name] = definition
                        pending.append(definition)
                pending.extend(node.values())
            elif isinstance(node, list):
                pending.extend(node)

        draft: dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": selected_properties,
            "required": sorted(draft_fields),
        }
        if selected_definitions:
            draft["$defs"] = selected_definitions
        return draft

    @staticmethod
    def _merge_definitions(target: dict[str, Any], incoming: object | None) -> None:
        if incoming is None:
            return
        if not isinstance(incoming, Mapping):
            raise ValueError("JSON schema definitions must be an object")
        for name, definition in incoming.items():
            existing = target.get(str(name))
            if existing is not None and existing != definition:
                raise ValueError("conflicting JSON schema definitions")
            target[str(name)] = definition

    @staticmethod
    def _normalize_response(response: Any, *, allowed_tool_ids: frozenset[str]) -> Any:
        message = response.choices[0].message
        tool_calls = getattr(message, "tool_calls", None) or []
        if len(tool_calls) > 1:
            raise _SequentialToolContractViolation("only one tool decision is allowed")
        if tool_calls:
            call = tool_calls[0]
            tool_id = str(call.function.name)
            if tool_id not in allowed_tool_ids:
                raise ValueError("tool decision is not registered")
            arguments = json.loads(call.function.arguments)
            if not isinstance(arguments, dict):
                raise ValueError("tool arguments must be an object")
            call_id = str(call.id)
            if not call_id:
                raise ValueError("tool decision requires a call ID")
            return AgentToolRequest(
                tool_id=tool_id,
                arguments=arguments,
                call_id=call_id,
            )
        payload = json.loads(str(message.content or ""))
        if not isinstance(payload, dict):
            raise ValueError("agent decision must be an object")
        kind = payload.get("kind")
        if kind == "tool_request":
            return OpenAICompatibleAgentProvider._json_tool_request(
                payload, allowed_tool_ids=allowed_tool_ids
            )
        if kind == "final_answer":
            OpenAICompatibleAgentProvider._validate_envelope_fields(
                payload,
                compact_fields={"kind", "value"},
            )
            value = payload.get("value")
            if not isinstance(value, dict):
                raise ValueError("final answer must be an object")
            legacy_tool = OpenAICompatibleAgentProvider._legacy_json_tool_request(
                value, allowed_tool_ids=allowed_tool_ids
            )
            if legacy_tool is not None:
                return legacy_tool
            return AgentFinalAnswer(value=value)
        if kind == "disambiguation":
            OpenAICompatibleAgentProvider._validate_envelope_fields(
                payload,
                compact_fields={"kind", "prompt", "candidates"},
            )
            candidates = payload.get("candidates")
            if not isinstance(payload.get("prompt"), str) or not isinstance(
                candidates, list
            ):
                raise ValueError("invalid disambiguation decision")
            if not all(isinstance(item, str) for item in candidates):
                raise ValueError("invalid disambiguation candidates")
            return AgentDisambiguation(
                prompt=str(payload["prompt"]),
                candidates=tuple(str(item) for item in candidates),
            )
        if kind == "refusal":
            OpenAICompatibleAgentProvider._validate_envelope_fields(
                payload,
                compact_fields={"kind", "code", "message"},
            )
            if not isinstance(payload.get("code"), str) or not isinstance(
                payload.get("message"), str
            ):
                raise ValueError("invalid refusal decision")
            return AgentRefusal(
                code=str(payload["code"]), message=str(payload["message"])
            )
        raise ValueError("unknown agent decision")

    @staticmethod
    def _json_tool_request(
        payload: Mapping[str, Any], *, allowed_tool_ids: frozenset[str]
    ) -> AgentToolRequest:
        OpenAICompatibleAgentProvider._validate_envelope_fields(
            payload,
            compact_fields={"kind", "tool_id", "arguments", "call_id"},
        )
        tool_id = payload.get("tool_id")
        arguments = payload.get("arguments")
        call_id = payload.get("call_id")
        if not isinstance(tool_id, str) or tool_id not in allowed_tool_ids:
            raise ValueError("tool decision is not registered")
        if not isinstance(arguments, dict):
            raise ValueError("tool arguments must be an object")
        if not isinstance(call_id, str) or not call_id:
            raise ValueError("tool decision requires a call ID")
        return AgentToolRequest(tool_id, arguments, call_id)

    @staticmethod
    def _validate_envelope_fields(
        payload: Mapping[str, Any],
        *,
        compact_fields: set[str],
    ) -> None:
        fields = set(payload)
        if fields == compact_fields:
            return
        if fields != _DECISION_FIELDS:
            raise ValueError("invalid agent decision fields")

    @staticmethod
    def _legacy_json_tool_request(
        value: Mapping[str, Any], *, allowed_tool_ids: frozenset[str]
    ) -> AgentToolRequest | None:
        """Normalize the exact LM Studio envelope produced by the old bad schema."""

        if set(value) != {"tool_call"}:
            return None
        tool_call = value.get("tool_call")
        if not isinstance(tool_call, dict) or set(tool_call) != {"name", "args"}:
            raise ValueError("invalid JSON tool decision")
        tool_id = tool_call.get("name")
        arguments = tool_call.get("args")
        if not isinstance(tool_id, str) or tool_id not in allowed_tool_ids:
            raise ValueError("tool decision is not registered")
        if not isinstance(arguments, dict):
            raise ValueError("tool arguments must be an object")
        canonical = json.dumps(
            {"tool_id": tool_id, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        )
        call_id = "json-call-" + hashlib.sha256(canonical.encode()).hexdigest()[:16]
        return AgentToolRequest(tool_id, arguments, call_id)

    async def aclose(self) -> None:
        close = getattr(self._client, "close", None)
        if close is not None:
            await close()
        if self._http_client is not None:
            await self._http_client.aclose()
