"""Fail-closed Ask Dev provider policy, separate from completion fallback."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from dev_health_ops.llm.credentials import LLMCredentials, validate_llm_base_url

from .errors import AgentProviderError, AgentProviderErrorCode

CERTIFIED_AGENT_PROVIDERS = frozenset({"openai", "scripted"})


class AgentProviderSource(str, Enum):
    BYO = "byo"
    PLATFORM = "platform"


class AgentFallbackPolicy(str, Enum):
    FAIL_CLOSED = "fail_closed"
    ALLOW_PLATFORM = "allow_platform"


@dataclass(frozen=True, slots=True)
class AgentProviderCandidate:
    provider: str
    model: str
    credentials: LLMCredentials = field(repr=False)
    source: AgentProviderSource = AgentProviderSource.PLATFORM
    readiness_current: bool = False

    @property
    def usable(self) -> bool:
        valid_url, _ = validate_llm_base_url(self.credentials.base_url)
        credentials_present = (
            bool(self.credentials.api_key) or self.provider == "scripted"
        )
        return (
            self.provider in CERTIFIED_AGENT_PROVIDERS
            and bool(self.model)
            and credentials_present
            and valid_url
            and self.readiness_current
        )


@dataclass(frozen=True, slots=True)
class AgentProviderPolicy:
    ask_dev_enabled: bool
    llm_globally_disabled: bool = False
    fallback: AgentFallbackPolicy = AgentFallbackPolicy.FAIL_CLOSED
    denied_providers: frozenset[str] = frozenset()
    denied_models: frozenset[str] = frozenset()


def resolve_agent_provider_selection(
    *,
    policy: AgentProviderPolicy,
    byo: AgentProviderCandidate | None,
    platform: AgentProviderCandidate | None,
) -> AgentProviderCandidate:
    """Resolve a certified candidate without implicit cross-source fallback."""
    if not policy.ask_dev_enabled or policy.llm_globally_disabled:
        raise AgentProviderError(AgentProviderErrorCode.DISABLED)

    def allowed(candidate: AgentProviderCandidate | None) -> bool:
        return bool(
            candidate
            and candidate.provider not in policy.denied_providers
            and candidate.model not in policy.denied_models
            and candidate.usable
        )

    if byo is not None:
        if allowed(byo):
            return byo
        if policy.fallback is not AgentFallbackPolicy.ALLOW_PLATFORM:
            code = (
                AgentProviderErrorCode.MODEL_NOT_SUPPORTED
                if byo.provider not in CERTIFIED_AGENT_PROVIDERS
                or byo.provider in policy.denied_providers
                or byo.model in policy.denied_models
                else AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED
            )
            raise AgentProviderError(code)

    if allowed(platform):
        return platform  # type: ignore[return-value]
    if platform is not None and (
        platform.provider not in CERTIFIED_AGENT_PROVIDERS
        or platform.provider in policy.denied_providers
        or platform.model in policy.denied_models
    ):
        raise AgentProviderError(AgentProviderErrorCode.MODEL_NOT_SUPPORTED)
    raise AgentProviderError(AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED)
