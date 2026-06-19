from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import patch

import pytest

import dev_health_ops.llm.providers as provider_factory
from dev_health_ops.llm import LLMAuthError, LLMError, get_provider, is_llm_available
from dev_health_ops.llm.providers.local import LMStudioGPT5Provider, LocalProvider
from dev_health_ops.llm.providers.mock import MockProvider
from dev_health_ops.llm.providers.none import NoneProvider
from dev_health_ops.llm.providers.openai import OpenAIProvider


def test_auto_without_keys_raises_classified_error():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(LLMAuthError, match="--llm-provider mock"):
            get_provider("auto")


def test_auto_with_generic_env_key_but_no_provider_names_llm_provider():
    # A bare LLM_API_KEY cannot identify which provider API to call; fail loud
    # with guidance to set LLM_PROVIDER rather than a generic auto-detect error.
    with patch.dict(os.environ, {"LLM_API_KEY": "sk-generic"}, clear=True):
        with pytest.raises(LLMAuthError, match="LLM_PROVIDER"):
            get_provider("auto")


def test_auto_with_inline_key_but_no_provider_names_llm_provider():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(LLMAuthError, match="--llm-provider"):
            get_provider("auto", api_key="sk-inline")


def test_explicit_openai_without_key_is_unavailable():
    with patch.dict(os.environ, {}, clear=True):
        assert is_llm_available("openai") is False
        with pytest.raises(LLMAuthError, match="OPENAI_API_KEY"):
            get_provider("openai")


def test_openai_accepts_inline_credentials_over_env():
    with patch.dict(
        os.environ,
        {"OPENAI_API_KEY": "sk-env", "OPENAI_BASE_URL": "https://env.invalid/v1"},
        clear=True,
    ):
        provider = get_provider(
            "openai",
            model="gpt-4o-mini",
            api_key="sk-inline",
            base_url="https://inline.invalid/v1",
        )

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-inline"
    assert provider._impl.cfg.base_url == "https://inline.invalid/v1"


def test_generic_llm_env_credentials_are_available_for_explicit_provider():
    with patch.dict(
        os.environ,
        {"LLM_API_KEY": "sk-generic", "LLM_BASE_URL": "https://generic.invalid/v1"},
        clear=True,
    ):
        assert is_llm_available("openai") is True
        provider = get_provider("openai")

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-generic"
    assert provider._impl.cfg.base_url == "https://generic.invalid/v1"


def test_lmstudio_gpt5_accepts_inline_credentials_over_default():
    with patch.dict(os.environ, {}, clear=True):
        provider = get_provider(
            "lmstudio",
            model="openai/gpt-oss-20b",
            api_key="sk-inline-lmstudio",
            base_url="http://inline-lmstudio.invalid/v1",
        )

    assert isinstance(provider, LMStudioGPT5Provider)
    assert provider.cfg.api_key == "sk-inline-lmstudio"
    assert provider.cfg.base_url == "http://inline-lmstudio.invalid/v1"


def test_lmstudio_gpt5_uses_lmstudio_api_key_env_before_default():
    with patch.dict(
        os.environ,
        {
            "LLM_PROVIDER": "lmstudio",
            "LLM_API_KEY": "sk-global",
            "LMSTUDIO_API_KEY": "sk-env-lmstudio",
        },
        clear=True,
    ):
        provider = get_provider("auto", model="openai/gpt-oss-20b")

    assert isinstance(provider, LMStudioGPT5Provider)
    assert provider.cfg.api_key == "sk-env-lmstudio"


def test_lmstudio_gpt5_direct_constructor_prefers_lmstudio_api_key_env():
    with patch.dict(
        os.environ,
        {"LLM_API_KEY": "sk-global", "LMSTUDIO_API_KEY": "sk-env-lmstudio"},
        clear=True,
    ):
        provider = LMStudioGPT5Provider(model="openai/gpt-oss-20b")

    assert provider.cfg.api_key == "sk-env-lmstudio"


def test_lmstudio_gpt5_falls_back_to_dummy_key_when_unset():
    with patch.dict(os.environ, {}, clear=True):
        provider = get_provider("lmstudio", model="openai/gpt-oss-20b")

    assert isinstance(provider, LMStudioGPT5Provider)
    assert provider.cfg.api_key == "lm-studio"


def test_lmstudio_gpt5_validation_flag_off_skips_models_list():
    with patch("openai.OpenAI") as mock_openai_class:
        LMStudioGPT5Provider(
            model="openai/gpt-oss-20b",
            api_key="sk-inline-lmstudio",
            validate_model_on_startup=False,
        )

    mock_openai_class.assert_not_called()


def test_lmstudio_gpt5_validation_flag_on_passes_when_reachable():
    with patch("openai.OpenAI") as mock_openai_class:
        mock_client = mock_openai_class.return_value
        provider = LMStudioGPT5Provider(
            model="openai/gpt-oss-20b",
            api_key="sk-inline-lmstudio",
            validate_model_on_startup=True,
        )

    assert provider.cfg.validate_model_on_startup is True
    mock_openai_class.assert_called_once()
    _, client_kwargs = mock_openai_class.call_args
    assert client_kwargs["api_key"] == "sk-inline-lmstudio"
    assert client_kwargs["base_url"] == "http://localhost:1234/v1"
    assert client_kwargs["max_retries"] == 0
    assert client_kwargs["http_client"].follow_redirects is False
    assert client_kwargs["http_client"].trust_env is False
    mock_client.models.list.assert_called_once_with()
    mock_client.close.assert_called_once_with()


def test_lmstudio_gpt5_validation_auth_failure_raises_classified_llm_error():
    with patch("openai.OpenAI") as mock_openai_class:
        mock_client = mock_openai_class.return_value
        mock_client.models.list.side_effect = RuntimeError("401 unauthorized")

        with pytest.raises(LLMAuthError) as exc:
            LMStudioGPT5Provider(
                model="openai/gpt-oss-20b",
                api_key="sk-inline-lmstudio",
                validate_model_on_startup=True,
            )

    assert not isinstance(exc.value, RuntimeError)
    assert exc.value.provider == "lmstudio"
    assert exc.value.model == "openai/gpt-oss-20b"
    mock_client.close.assert_called_once_with()


def test_lmstudio_gpt5_validation_connection_failure_raises_classified_llm_error():
    with patch("openai.OpenAI") as mock_openai_class:
        mock_client = mock_openai_class.return_value
        mock_client.models.list.side_effect = RuntimeError("connection error")

        with pytest.raises(LLMError) as exc:
            LMStudioGPT5Provider(
                model="openai/gpt-oss-20b",
                api_key="sk-inline-lmstudio",
                validate_model_on_startup=True,
            )

    assert not isinstance(exc.value, RuntimeError)
    assert exc.value.provider == "lmstudio"
    assert exc.value.model == "openai/gpt-oss-20b"
    mock_client.close.assert_called_once_with()


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
        assert provider_factory.resolve_model_name("openai") == "openai-model"


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


def test_local_provider_failure_logs_redacted_url_and_error(caplog):
    class _Completions:
        async def create(self, **kwargs):
            raise RuntimeError("401 invalid_api_key sk-secret-token query_token=abc123")

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    provider = LocalProvider(
        base_url="https://user:pass@llm.example.test/v1?api_key=secret&token=abc",
        model="local-test",
    )
    provider._client = _Client()

    with caplog.at_level(logging.ERROR, logger="dev_health_ops.llm.providers.local"):
        with pytest.raises(LLMAuthError):
            asyncio.run(provider.complete("hello"))

    log_text = caplog.text
    assert "https://llm.example.test/v1" in log_text
    assert "LLMAuthError" in log_text
    assert "user:pass" not in log_text
    assert "api_key=secret" not in log_text
    assert "sk-secret-token" not in log_text
    assert "query_token=abc123" not in log_text
