"""Source-bound Ask Dev provider policy, separate from completion fallback."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlsplit

from dev_health_ops.llm.credentials import LLMCredentials, validate_llm_base_url

from .errors import AgentProviderError, AgentProviderErrorCode

CERTIFIED_BYO_AGENT_PROVIDERS = frozenset({"openai"})
CERTIFIED_PLATFORM_AGENT_PROVIDERS = frozenset(
    {"openai", "local", "ollama", "lmstudio"}
)
CERTIFIED_AGENT_PROVIDERS = (
    CERTIFIED_BYO_AGENT_PROVIDERS | CERTIFIED_PLATFORM_AGENT_PROVIDERS
)
_LOCAL_PLATFORM_PROVIDERS = frozenset({"local", "ollama", "lmstudio"})


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
    def certified(self) -> bool:
        certified = (
            CERTIFIED_PLATFORM_AGENT_PROVIDERS
            if self.source is AgentProviderSource.PLATFORM
            else CERTIFIED_BYO_AGENT_PROVIDERS
        )
        return self.provider in certified

    @property
    def usable(self) -> bool:
        if self.source is AgentProviderSource.PLATFORM:
            valid_url = _operator_base_url_is_sane(self.credentials.base_url)
            credentials_present = bool(
                self.credentials.api_key
                or (
                    self.credentials.base_url
                    and self.provider in CERTIFIED_PLATFORM_AGENT_PROVIDERS
                )
            )
            if self.provider in _LOCAL_PLATFORM_PROVIDERS:
                credentials_present = bool(self.credentials.base_url)
        else:
            valid_url, _ = validate_llm_base_url(self.credentials.base_url)
            credentials_present = bool(self.credentials.api_key)
        return (
            self.certified
            and bool(self.model)
            and credentials_present
            and valid_url
            and self.readiness_current
        )


def _operator_base_url_is_sane(base_url: str | None) -> bool:
    if not base_url:
        return True
    if any(ord(char) <= 0x20 or ord(char) == 0x7F for char in base_url):
        return False
    try:
        parsed = urlsplit(base_url)
        host = parsed.hostname
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and parsed.username is None
        and parsed.password is None
        and host is not None
        and not parsed.query
        and not parsed.fragment
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
    """Resolve a certified candidate under the configured source fallback policy."""
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
                if not byo.certified
                or byo.provider in policy.denied_providers
                or byo.model in policy.denied_models
                else AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED
            )
            raise AgentProviderError(code)

    if allowed(platform):
        return platform  # type: ignore[return-value]
    if platform is not None and (
        not platform.certified
        or platform.provider in policy.denied_providers
        or platform.model in policy.denied_models
    ):
        raise AgentProviderError(AgentProviderErrorCode.MODEL_NOT_SUPPORTED)
    raise AgentProviderError(AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED)
