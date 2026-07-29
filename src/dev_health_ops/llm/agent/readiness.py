"""Synthetic capability certification and safe persistence for Ask Dev."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Protocol

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.metrics.llm_token_usage import write_llm_token_usage
from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from dev_health_ops.models.settings import SettingCategory

from .contracts import (
    AgentFinalAnswer,
    AgentLLMProvider,
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
    CancellationSignal,
)
from .errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)

READINESS_SETTING_KEY = "ask_dev_agent_readiness"


class AgentReadinessOutcome(str, Enum):
    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class AgentReadinessRecord:
    fingerprint: str
    readiness_version: str
    checked_at: str
    outcome: AgentReadinessOutcome
    safe_error_code: str | None = None

    def is_current(self, *, fingerprint: str, readiness_version: str) -> bool:
        return (
            self.outcome is AgentReadinessOutcome.READY
            and self.fingerprint == fingerprint
            and self.readiness_version == readiness_version
        )


class AgentReadinessStore(Protocol):
    async def load(self) -> AgentReadinessRecord | None: ...

    async def save(self, record: AgentReadinessRecord) -> None: ...


class SettingsAgentReadinessStore:
    def __init__(self, settings: SettingsService):
        self._settings = settings

    async def load(self) -> AgentReadinessRecord | None:
        raw = await self._settings.get(
            READINESS_SETTING_KEY, category=SettingCategory.LLM.value
        )
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            return AgentReadinessRecord(
                fingerprint=str(payload["fingerprint"]),
                readiness_version=str(payload["readiness_version"]),
                checked_at=str(payload["checked_at"]),
                outcome=AgentReadinessOutcome(payload["outcome"]),
                safe_error_code=(
                    str(payload["safe_error_code"])
                    if payload.get("safe_error_code")
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    async def save(self, record: AgentReadinessRecord) -> None:
        payload = asdict(record)
        payload["outcome"] = record.outcome.value
        await self._settings.set(
            READINESS_SETTING_KEY,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            category=SettingCategory.LLM.value,
            description="Safe Ask Dev provider certification result",
        )


class AgentReadinessService:
    def __init__(
        self,
        store: AgentReadinessStore,
        *,
        usage_sink: BaseMetricsSink | None = None,
        org_id: str = "",
    ):
        self._store = store
        self._usage_sink = usage_sink
        self._org_id = org_id

    async def certify(
        self,
        provider: AgentLLMProvider,
        *,
        provider_name: str,
        model: str,
        fingerprint: str,
        timeout_seconds: float = 10,
        signal: CancellationSignal | None = None,
    ) -> AgentReadinessRecord:
        usage = AgentUsage()
        try:
            tool = AgentToolDefinition(
                tool_id="readiness_echo",
                description="Return the supplied nonce.",
                input_schema={
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["nonce"],
                    "properties": {"nonce": {"type": "string"}},
                },
            )
            response_schema: Mapping[str, object] = {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "value"],
                "properties": {
                    "kind": {"const": "final_answer"},
                    "value": {"type": "object"},
                },
            }
            first = await provider.decide(
                [
                    AgentMessage(
                        AgentMessageRole.USER,
                        "Call readiness_echo with nonce ready-v1.",
                    )
                ],
                [tool],
                response_schema,
                timeout_seconds,
                128,
                signal,
            )
            usage = _add_usage(usage, first.usage)
            if not isinstance(first.decision, AgentToolRequest):
                raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
            if first.decision.tool_id != tool.tool_id or first.decision.arguments != {
                "nonce": "ready-v1"
            }:
                raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
            second = await provider.decide(
                [
                    AgentMessage(
                        AgentMessageRole.USER,
                        "Call readiness_echo with nonce ready-v1.",
                    ),
                    AgentMessage(
                        AgentMessageRole.ASSISTANT,
                        "",
                        tool_request=first.decision,
                    ),
                    AgentMessage(
                        AgentMessageRole.TOOL,
                        '{"nonce":"ready-v1"}',
                        tool_call_id=first.decision.call_id,
                    ),
                ],
                [tool],
                response_schema,
                timeout_seconds,
                128,
                signal,
            )
            usage = _add_usage(usage, second.usage)
            if not isinstance(second.decision, AgentFinalAnswer):
                raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
            record = AgentReadinessRecord(
                fingerprint=fingerprint,
                readiness_version=provider.capabilities.readiness_version,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.READY,
            )
        except Exception as exc:
            safe = safe_agent_provider_error(exc)
            record = AgentReadinessRecord(
                fingerprint=fingerprint,
                readiness_version=provider.capabilities.readiness_version,
                checked_at=datetime.now(timezone.utc).isoformat(),
                outcome=AgentReadinessOutcome.FAILED,
                safe_error_code=safe.code.value,
            )
        await self._store.save(record)
        if self._usage_sink is not None:
            write_llm_token_usage(
                self._usage_sink,
                org_id=self._org_id,
                provider=provider_name,
                model=model,
                source="readiness",
                use_case="ask_dev",
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                calls=2,
            )
        return record


def _add_usage(left: AgentUsage, right: AgentUsage) -> AgentUsage:
    cost = None
    if (
        left.estimated_cost_microusd is not None
        or right.estimated_cost_microusd is not None
    ):
        cost = (left.estimated_cost_microusd or 0) + (
            right.estimated_cost_microusd or 0
        )
    return AgentUsage(
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        estimated_cost_microusd=cost,
    )
