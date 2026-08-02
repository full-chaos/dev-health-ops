"""Synthetic capability certification and safe persistence for Ask Dev."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Protocol

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
from .roles import RoleCertificationState

READINESS_SETTING_KEY = "ask_dev_agent_readiness"
READINESS_MAX_OUTPUT_TOKENS = 512
# Dotted, matching the shape of every real registry tool_id (e.g.
# "query_metric.v1"), so certification genuinely exercises the
# OpenAI-compatible adapter's wire-name sanitize/reverse-map round trip for a
# name-illegal tool_id -- a registry regression fails preflight here, not the
# first real user question (CHAOS-3286).
READINESS_ECHO_TOOL_ID = "readiness_echo.v1"

# Sentinel scope for the platform-owned (operator env-configured) provider's
# readiness record. Every real org's org_id is a non-empty UUID string
# (enforced at the admin-auth boundary by ``get_admin_org_id``), so "" can
# never collide with a real org's settings rows (CHAOS-3265). This store
# always explicitly writes/reads org_id="" -- it never relies on whatever the
# `settings` table's column default happens to resolve to. (Note: the
# SQLAlchemy model in models/settings.py declares
# ``server_default=""``, but the deployed migration
# (0001_initial_schema.py) actually creates the column with
# ``server_default="default"`` -- a pre-existing, unrelated drift between the
# ORM model and the live schema. Irrelevant here since org_id is always
# supplied explicitly, never omitted at insert time.)
#
# Belt-and-suspenders: the platform record ALSO uses a setting key distinct
# from the ordinary per-org ``READINESS_SETTING_KEY``. That way, even if some
# unrelated bug elsewhere ever wrote a stray row with an empty org_id under
# the *ordinary* key (a known failure mode in this codebase family), it would
# still be invisible here -- only the dedicated Platform Admin route ever
# reads or writes ``PLATFORM_READINESS_SETTING_KEY``.
PLATFORM_SETTINGS_ORG_ID = ""
PLATFORM_READINESS_SETTING_KEY = "platform_ask_dev_agent_readiness"

ReadinessState = Literal[
    "ready",
    "unsupported_model",
    "missing_credentials",
    "disabled",
    "degraded",
    "stale_readiness",
]


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
    def __init__(self, settings: SettingsService, *, key: str = READINESS_SETTING_KEY):
        self._settings = settings
        self._key = key

    async def load(self) -> AgentReadinessRecord | None:
        raw = await self._settings.get(self._key, category=SettingCategory.LLM.value)
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
            self._key,
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
        timeout_seconds: float = 30,
        signal: CancellationSignal | None = None,
    ) -> AgentReadinessRecord:
        usage = AgentUsage()
        try:
            tool = AgentToolDefinition(
                tool_id=READINESS_ECHO_TOOL_ID,
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
                "required": ["nonce"],
                "properties": {
                    "nonce": {
                        "type": "string",
                        "const": "ready-v1",
                    }
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
                READINESS_MAX_OUTPUT_TOKENS,
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
                    AgentMessage(
                        AgentMessageRole.USER,
                        (
                            "Return a final_answer now with value exactly "
                            '{"nonce":"ready-v1"}. Do not request another tool.'
                        ),
                    ),
                ],
                [],
                response_schema,
                timeout_seconds,
                READINESS_MAX_OUTPUT_TOKENS,
                signal,
            )
            usage = _add_usage(usage, second.usage)
            if not isinstance(second.decision, AgentFinalAnswer):
                raise AgentProviderError(AgentProviderErrorCode.INVALID_RESPONSE)
            if second.decision.value != {"nonce": "ready-v1"}:
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


def readiness_failure_state(safe_error_code: str | None) -> tuple[ReadinessState, str]:
    """Map a safe provider error code to an admin-facing readiness state.

    Shared by the org-admin Ask Dev router and the Platform Admin router so
    both project the exact same safe, non-identifying remediation text for a
    failed certification (CHAOS-3265).
    """

    if safe_error_code == "provider_not_configured":
        return (
            "missing_credentials",
            "Ask Dev could not authenticate with the configured model endpoint.",
        )
    if safe_error_code == "timeout":
        return "degraded", "The configured Ask Dev model timed out during readiness."
    if safe_error_code == "rate_limited":
        return "degraded", "The configured Ask Dev model rate limit was reached."
    if safe_error_code == "model_not_supported":
        return (
            "unsupported_model",
            "The configured Ask Dev model is unavailable to this provider account.",
        )
    if safe_error_code == "invalid_request":
        return (
            "unsupported_model",
            "The configured Ask Dev model rejected a required agent request capability.",
        )
    if safe_error_code == "invalid_response":
        return (
            "unsupported_model",
            "The configured model did not satisfy the Ask Dev agent capability contract.",
        )
    if safe_error_code == "provider_contract_violation":
        return (
            "unsupported_model",
            "The configured model returned multiple tool decisions in one turn, "
            "violating Ask Dev's required sequential tool-call contract.",
        )
    if safe_error_code == "output_exhausted":
        return (
            "unsupported_model",
            "The configured Ask Dev model exhausted its output/reasoning "
            "token budget before completing a valid response.",
        )
    if safe_error_code == "provider_unavailable":
        return "degraded", "The configured Ask Dev model endpoint is unavailable."
    return "degraded", "The configured Ask Dev model failed readiness."


def role_state_for_safe_error_code(
    safe_error_code: str | None,
) -> RoleCertificationState:
    """Map a safe provider error code to a per-role certification verdict.

    Reuses ``readiness_failure_state``'s existing safe_error_code ->
    admin-facing-state mapping rather than re-deriving the same
    classification twice (CHAOS-3285): ``"unsupported_model"`` is exactly
    the deterministic/structural case (including ``output_exhausted``,
    which maps there today) -- these become INCOMPATIBLE, since retrying
    the same request shape against the same provider will not resolve
    them. Every other failure state (missing credentials, disabled,
    degraded/transient) becomes FAILED -- an operator or transient-retry
    condition, not a structural one. ``None`` (no error) is COMPATIBLE.
    """

    if safe_error_code is None:
        return RoleCertificationState.COMPATIBLE
    state, _ = readiness_failure_state(safe_error_code)
    if state == "unsupported_model":
        return RoleCertificationState.INCOMPATIBLE
    return RoleCertificationState.FAILED


def _add_usage(left: AgentUsage, right: AgentUsage) -> AgentUsage:
    cost = None
    if (
        left.estimated_cost_microusd is not None
        or right.estimated_cost_microusd is not None
    ):
        cost = (left.estimated_cost_microusd or 0) + (
            right.estimated_cost_microusd or 0
        )
    reasoning_tokens = None
    if left.reasoning_tokens is not None or right.reasoning_tokens is not None:
        reasoning_tokens = (left.reasoning_tokens or 0) + (right.reasoning_tokens or 0)
    return AgentUsage(
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        estimated_cost_microusd=cost,
        reasoning_tokens=reasoning_tokens,
    )
