from __future__ import annotations

from dev_health_ops.llm.providers.openai import (
    OpenAIProvider,
    OpenAIGPT5Provider,
    OpenAIGPTLegacyProvider,
    openai_provider_class_for,
)


def test_openai_provider_routing_logic():
    # GPT-5 family
    assert openai_provider_class_for("gpt-5-mini") is OpenAIGPT5Provider
    assert openai_provider_class_for("gpt-5") is OpenAIGPT5Provider
    assert openai_provider_class_for("gpt-6-stable") is OpenAIGPT5Provider

    # Legacy models
    assert openai_provider_class_for("gpt-4o") is OpenAIGPTLegacyProvider
    assert openai_provider_class_for("gpt-4o-mini") is OpenAIGPTLegacyProvider
    assert openai_provider_class_for("gpt-4-turbo") is OpenAIGPTLegacyProvider
    assert openai_provider_class_for("gpt-3.5-turbo") is OpenAIGPTLegacyProvider
    assert openai_provider_class_for("o1-preview") is OpenAIGPTLegacyProvider


def test_openai_facade_instantiation():
    # Verify facade picks the right impl
    p_gpt5 = OpenAIProvider(api_key="test", model="gpt-5-mini")
    assert isinstance(p_gpt5._impl, OpenAIGPT5Provider)

    p_legacy = OpenAIProvider(api_key="test", model="gpt-4o")
    assert isinstance(p_legacy._impl, OpenAIGPTLegacyProvider)
