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
