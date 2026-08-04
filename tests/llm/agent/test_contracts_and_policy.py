from __future__ import annotations

import inspect

import pytest

from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode
from dev_health_ops.llm.agent.policy import (
    AgentFallbackPolicy,
    AgentProviderCandidate,
    AgentProviderPolicy,
    AgentProviderSource,
    resolve_agent_provider_selection,
)
from dev_health_ops.llm.credentials import LLMCredentials
from dev_health_ops.llm.providers.base import LLMProvider


def candidate(
    *,
    provider: str = "openai",
    source: AgentProviderSource = AgentProviderSource.BYO,
    ready: bool = True,
    key: str = "secret",
) -> AgentProviderCandidate:
    return AgentProviderCandidate(
        provider=provider,
        model="agent-model",
        credentials=LLMCredentials(api_key=key),
        source=source,
        readiness_current=ready,
    )


def test_existing_completion_provider_contract_is_unchanged() -> None:
    assert list(inspect.signature(LLMProvider.complete).parameters) == [
        "self",
        "prompt",
    ]


def test_usable_certified_byo_wins() -> None:
    byo = candidate()
    platform = candidate(source=AgentProviderSource.PLATFORM)
    selected = resolve_agent_provider_selection(
        policy=AgentProviderPolicy(ask_dev_enabled=True),
        byo=byo,
        platform=platform,
    )
    assert selected is byo


def test_unsupported_byo_fails_closed_without_explicit_fallback() -> None:
    with pytest.raises(AgentProviderError) as caught:
        resolve_agent_provider_selection(
            policy=AgentProviderPolicy(ask_dev_enabled=True),
            byo=candidate(provider="anthropic"),
            platform=candidate(source=AgentProviderSource.PLATFORM),
        )
    assert caught.value.code is AgentProviderErrorCode.MODEL_NOT_SUPPORTED


def test_explicit_fallback_allows_certified_platform_provider() -> None:
    platform = candidate(source=AgentProviderSource.PLATFORM)
    selected = resolve_agent_provider_selection(
        policy=AgentProviderPolicy(
            ask_dev_enabled=True,
            fallback=AgentFallbackPolicy.ALLOW_PLATFORM,
        ),
        byo=candidate(provider="anthropic"),
        platform=platform,
    )
    assert selected is platform


@pytest.mark.parametrize("global_disabled", [False, True])
def test_disable_controls_are_fail_closed(global_disabled: bool) -> None:
    policy = AgentProviderPolicy(
        ask_dev_enabled=global_disabled,
        llm_globally_disabled=global_disabled,
    )
    with pytest.raises(AgentProviderError) as caught:
        resolve_agent_provider_selection(
            policy=policy,
            byo=None,
            platform=candidate(source=AgentProviderSource.PLATFORM),
        )
    assert caught.value.code is AgentProviderErrorCode.DISABLED


def test_uncertified_candidate_is_not_usable() -> None:
    with pytest.raises(AgentProviderError) as caught:
        resolve_agent_provider_selection(
            policy=AgentProviderPolicy(ask_dev_enabled=True),
            byo=candidate(ready=False),
            platform=None,
        )
    assert caught.value.code is AgentProviderErrorCode.PROVIDER_NOT_CONFIGURED


@pytest.mark.parametrize("provider", ["local", "ollama", "lmstudio"])
def test_platform_openai_compatible_alias_is_source_aware(provider: str) -> None:
    platform = AgentProviderCandidate(
        provider=provider,
        model="local-agent-model",
        credentials=LLMCredentials(base_url="http://host.docker.internal:1234/v1"),
        source=AgentProviderSource.PLATFORM,
        readiness_current=True,
    )
    assert platform.usable is True

    byo = AgentProviderCandidate(
        provider=provider,
        model="local-agent-model",
        credentials=LLMCredentials(
            api_key="org-key", base_url="https://models.example.com/v1"
        ),
        source=AgentProviderSource.BYO,
        readiness_current=True,
    )
    assert byo.certified is False
    assert byo.usable is False


@pytest.mark.parametrize(
    "base_url",
    [
        "http://user:secret@host.docker.internal:1234/v1",
        "http://host.docker.internal:1234/v1?token=secret",
        "file:///tmp/model.sock",
    ],
)
def test_platform_operator_url_rejects_credential_and_non_http_shapes(
    base_url: str,
) -> None:
    platform = AgentProviderCandidate(
        provider="local",
        model="local-agent-model",
        credentials=LLMCredentials(base_url=base_url),
        source=AgentProviderSource.PLATFORM,
        readiness_current=True,
    )
    assert platform.usable is False


@pytest.mark.parametrize(
    "base_url",
    [
        "http://127.0.0.1:1234/v1",
        "https://user:secret@models.example.com/v1",
        "file:///tmp/model.sock",
    ],
)
def test_byo_certified_candidate_rejects_unsafe_base_url(base_url: str) -> None:
    byo = AgentProviderCandidate(
        provider="openai",
        model="agent-model",
        credentials=LLMCredentials(api_key="org-key", base_url=base_url),
        source=AgentProviderSource.BYO,
        readiness_current=True,
    )

    assert byo.certified is True
    assert byo.usable is False


def test_internal_scripted_adapter_is_not_a_product_provider_family() -> None:
    with pytest.raises(AgentProviderError) as caught:
        resolve_agent_provider_selection(
            policy=AgentProviderPolicy(ask_dev_enabled=True),
            byo=None,
            platform=candidate(
                provider="scripted",
                source=AgentProviderSource.PLATFORM,
                key="",
            ),
        )
    assert caught.value.code is AgentProviderErrorCode.MODEL_NOT_SUPPORTED


# CHAOS-3358: the platform provider is operator-owned and assumed to work, so
# its stored certification is an operator-facing diagnostic and must not gate
# runtime selection. Every other conjunct of `usable` -- and BYO's strict
# readiness requirement -- is unchanged.


def test_platform_candidate_is_usable_without_a_current_certification() -> None:
    platform = candidate(source=AgentProviderSource.PLATFORM, ready=False)
    assert platform.usable is True

    selected = resolve_agent_provider_selection(
        policy=AgentProviderPolicy(ask_dev_enabled=True),
        byo=None,
        platform=platform,
    )
    assert selected is platform


def test_byo_candidate_still_requires_a_current_certification() -> None:
    assert candidate(source=AgentProviderSource.BYO, ready=False).usable is False
    assert candidate(source=AgentProviderSource.BYO, ready=True).usable is True


@pytest.mark.parametrize("provider", ["openai", "local", "ollama", "lmstudio"])
def test_platform_candidate_without_credentials_is_still_unusable(
    provider: str,
) -> None:
    """The gate that remains, observed failing: no api_key and no base_url."""

    platform = AgentProviderCandidate(
        provider=provider,
        model="agent-model",
        credentials=LLMCredentials(),
        source=AgentProviderSource.PLATFORM,
        readiness_current=False,
    )
    assert platform.usable is False


def test_platform_candidate_without_a_model_is_still_unusable() -> None:
    platform = AgentProviderCandidate(
        provider="openai",
        model="",
        credentials=LLMCredentials(api_key="operator-key"),
        source=AgentProviderSource.PLATFORM,
        readiness_current=False,
    )
    assert platform.usable is False


def test_platform_candidate_of_an_uncertified_family_is_still_unusable() -> None:
    platform = candidate(
        provider="anthropic", source=AgentProviderSource.PLATFORM, ready=False
    )
    assert platform.certified is False
    assert platform.usable is False

    with pytest.raises(AgentProviderError) as caught:
        resolve_agent_provider_selection(
            policy=AgentProviderPolicy(ask_dev_enabled=True),
            byo=None,
            platform=platform,
        )
    assert caught.value.code is AgentProviderErrorCode.MODEL_NOT_SUPPORTED


@pytest.mark.parametrize(
    "policy",
    [
        AgentProviderPolicy(
            ask_dev_enabled=True, denied_providers=frozenset({"openai"})
        ),
        AgentProviderPolicy(
            ask_dev_enabled=True, denied_models=frozenset({"agent-model"})
        ),
    ],
)
def test_denied_platform_provider_or_model_still_blocks(
    policy: AgentProviderPolicy,
) -> None:
    """Operator denylists outrank the platform's assumed-good status."""

    platform = candidate(source=AgentProviderSource.PLATFORM, ready=False)
    assert platform.usable is True

    with pytest.raises(AgentProviderError) as caught:
        resolve_agent_provider_selection(policy=policy, byo=None, platform=platform)
    assert caught.value.code is AgentProviderErrorCode.MODEL_NOT_SUPPORTED


def test_platform_unsafe_operator_base_url_is_still_unusable() -> None:
    platform = AgentProviderCandidate(
        provider="local",
        model="local-agent-model",
        credentials=LLMCredentials(
            base_url="http://user:secret@host.docker.internal:1234/v1"
        ),
        source=AgentProviderSource.PLATFORM,
        readiness_current=False,
    )
    assert platform.usable is False
