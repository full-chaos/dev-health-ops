from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import patch

import pytest

import dev_health_ops.llm.providers as provider_factory
from dev_health_ops.llm import LLMAuthError, get_provider, is_llm_available
from dev_health_ops.llm.providers import resolve_model_name
from dev_health_ops.llm.providers.mock import MockProvider
from dev_health_ops.llm.providers.none import NoneProvider
from dev_health_ops.llm.providers.openai import OpenAIProvider


def test_auto_without_keys_raises_classified_error():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(LLMAuthError, match="--llm-provider mock"):
            get_provider("auto")


def test_explicit_openai_without_key_is_unavailable():
    with patch.dict(os.environ, {}, clear=True):
        assert is_llm_available("openai") is False
        with pytest.raises(LLMAuthError, match="OPENAI_API_KEY"):
            get_provider("openai")


def test_provider_specific_model_env_overrides_global_model():
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-test",
            "LLM_MODEL": "global-model",
            "LLM_MODEL_OPENAI": "openai-model",
        },
        clear=True,
    ):
        provider = get_provider("openai")
        assert isinstance(provider, OpenAIProvider)
        assert provider._impl.cfg.model == "openai-model"
        assert resolve_model_name("openai") == "openai-model"


def test_mock_provider_stamps_mock_model():
    provider = get_provider("mock", model="gpt-5-mini")
    assert isinstance(provider, MockProvider)
    result = asyncio.run(provider.complete("hello"))
    assert result.model == "mock"
    assert result.input_tokens is None
    assert result.output_tokens is None


def test_none_provider_is_not_available_and_not_mock():
    provider = get_provider("none", model="gpt-5-mini")
    assert isinstance(provider, NoneProvider)
    assert is_llm_available("none") is False
    result = asyncio.run(provider.complete("hello"))
    assert result.text == ""
    assert result.model == "none"


def test_resolved_provider_model_logged_once(caplog):
    provider_factory._LOGGED_PROVIDER_MODELS.clear()
    with patch.dict(
        os.environ,
        {"OPENAI_API_KEY": "sk-test", "LLM_MODEL_OPENAI": "openai-model"},
        clear=True,
    ):
        with caplog.at_level(logging.INFO, logger="dev_health_ops.llm.providers"):
            get_provider("openai")
            get_provider("openai")

    matching = [
        record
        for record in caplog.records
        if "Resolved LLM provider" in record.getMessage()
    ]
    assert len(matching) == 1
    assert "provider=openai model=openai-model" in matching[0].getMessage()
