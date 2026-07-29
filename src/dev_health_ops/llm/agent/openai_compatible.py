"""OpenAI-compatible adapter for Ask Dev's provider-neutral agent contract."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any, cast

from dev_health_ops.llm.providers._http import make_hardened_async_httpx_client

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

READINESS_VERSION = "ask-dev-agent-v1"


def _fingerprint(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()[:24]


class OpenAICompatibleAgentProvider:
    """Normalize a native tool-capable OpenAI-compatible endpoint."""

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
            raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
        started = time.monotonic()
        create_completion = cast(Any, self._client.chat.completions.create)
        provider_task = asyncio.create_task(
            create_completion(
                model=self.model,
                messages=[self._message_payload(item) for item in messages],
                tools=[self._tool_payload(item) for item in tools] or None,
                tool_choice="auto" if tools else None,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "ask_dev_decision",
                        "strict": True,
                        "schema": dict(response_schema),
                    },
                },
                max_completion_tokens=max_output_tokens,
            )
        )
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
            decision = self._normalize_response(response)
        except (
            AttributeError,
            IndexError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE) from None
        usage = getattr(response, "usage", None)
        return AgentDecisionResult(
            decision=decision,
            usage=AgentUsage(
                input_tokens=int(getattr(usage, "prompt_tokens", 0) or 0),
                output_tokens=int(getattr(usage, "completion_tokens", 0) or 0),
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
                "parameters": dict(tool.input_schema),
                "strict": True,
            },
        }

    @staticmethod
    def _normalize_response(response: Any) -> Any:
        message = response.choices[0].message
        tool_calls = getattr(message, "tool_calls", None) or []
        if len(tool_calls) > 1:
            raise ValueError("only one tool decision is allowed")
        if tool_calls:
            call = tool_calls[0]
            return AgentToolRequest(
                tool_id=str(call.function.name),
                arguments=json.loads(call.function.arguments),
                call_id=str(call.id),
            )
        payload = json.loads(str(message.content or ""))
        kind = payload.get("kind")
        if kind == "final_answer":
            value = payload.get("value")
            if not isinstance(value, dict):
                raise ValueError("final answer must be an object")
            return AgentFinalAnswer(value=value)
        if kind == "disambiguation":
            candidates = payload.get("candidates") or []
            return AgentDisambiguation(
                prompt=str(payload["prompt"]),
                candidates=tuple(str(item) for item in candidates),
            )
        if kind == "refusal":
            return AgentRefusal(
                code=str(payload["code"]), message=str(payload["message"])
            )
        raise ValueError("unknown agent decision")

    async def aclose(self) -> None:
        close = getattr(self._client, "close", None)
        if close is not None:
            await close()
        if self._http_client is not None:
            await self._http_client.aclose()
