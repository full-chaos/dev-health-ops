import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.llm import LLMRateLimitError
from dev_health_ops.llm.providers import get_provider
from dev_health_ops.llm.providers.qwen import (
    DEFAULT_DASHSCOPE_BASE_URL,
    QwenLMStudioProvider,
    QwenLocalProvider,
    QwenProvider,
)


class _RateLimitError(Exception):
    status_code = 429

    def __init__(self, retry_after: str = "0.25") -> None:
        super().__init__("429 too many requests")
        self.headers = {"Retry-After": retry_after}


def test_qwen_provider_registration():
    # Test explicit names
    with patch.dict(os.environ, {"QWEN_API_KEY": "test-key"}, clear=True):
        assert isinstance(get_provider("qwen"), QwenProvider)
    assert isinstance(get_provider("qwen-local"), QwenLocalProvider)
    assert isinstance(get_provider("qwen-lmstudio"), QwenLMStudioProvider)


def test_qwen_provider_config():
    # Test default config
    p = QwenProvider(api_key="test-key")
    assert p.api_key == "test-key"
    assert p.base_url == DEFAULT_DASHSCOPE_BASE_URL
    assert p.model == "qwen-plus"

    # Test override via env vars
    with patch.dict(
        os.environ,
        {
            "QWEN_API_KEY": "env-qwen-key",
            "DASHSCOPE_BASE_URL": "http://custom-dashscope/v1",
            "QWEN_MODEL": "qwen-max",
        },
    ):
        p = QwenProvider()
        assert p.api_key == "env-qwen-key"
        assert p.base_url == "http://custom-dashscope/v1"
        assert p.model == "qwen-max"


def test_qwen_api_key_precedence():
    # QWEN_API_KEY should take precedence over DASHSCOPE_API_KEY if both are set
    with patch.dict(
        os.environ, {"QWEN_API_KEY": "qwen-key", "DASHSCOPE_API_KEY": "dashscope-key"}
    ):
        p = QwenProvider()
        assert p.api_key == "qwen-key"

    # Should fallback to DASHSCOPE_API_KEY
    with patch.dict(os.environ, {"DASHSCOPE_API_KEY": "dashscope-key"}):
        if "QWEN_API_KEY" in os.environ:
            del os.environ["QWEN_API_KEY"]
        p = QwenProvider()
        assert p.api_key == "dashscope-key"


def test_qwen_auto_detection():
    # Test auto-detection via QWEN_API_KEY
    with patch.dict(os.environ, {"QWEN_API_KEY": "test-key"}, clear=True):
        p = get_provider("auto")
        assert isinstance(p, QwenProvider)

    # Test auto-detection via DASHSCOPE_API_KEY
    with patch.dict(os.environ, {"DASHSCOPE_API_KEY": "test-key"}, clear=True):
        p = get_provider("auto")
        assert isinstance(p, QwenProvider)


@pytest.mark.asyncio
async def test_qwen_provider_completion():
    # Mock AsyncOpenAI to verify base_url and model usage
    with patch("openai.AsyncOpenAI") as mock_openai_class:
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client

        # Setup mock response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Qwen response"
        mock_response.usage = type(
            "Usage", (), {"prompt_tokens": 8, "completion_tokens": 4}
        )()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        p = QwenProvider(api_key="sk-123")
        response = await p.complete("Hello")

        assert response.text == "Qwen response"
        assert response.model == "qwen-plus"
        assert response.input_tokens == 8
        assert response.output_tokens == 4
        # Verify client was initialized with correct base_url
        mock_openai_class.assert_called_once()
        _, client_kwargs = mock_openai_class.call_args
        assert client_kwargs["api_key"] == "sk-123"
        assert client_kwargs["base_url"] == DEFAULT_DASHSCOPE_BASE_URL
        assert client_kwargs["max_retries"] == 0
        assert client_kwargs["http_client"].follow_redirects is False
        assert client_kwargs["http_client"].trust_env is False
        # Verify completion was called with correct model
        mock_client.chat.completions.create.assert_called_once()
        args, kwargs = mock_client.chat.completions.create.call_args
        assert kwargs["model"] == "qwen-plus"


@pytest.mark.asyncio
async def test_qwen_retries_429_and_honors_retry_after():
    with (
        patch("openai.AsyncOpenAI") as mock_openai_class,
        patch(
            "dev_health_ops.llm.providers.local.asyncio.sleep", new_callable=AsyncMock
        ) as sleep,
    ):
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Qwen response"
        mock_response.usage = None
        mock_client.chat.completions.create = AsyncMock(
            side_effect=[_RateLimitError("0.5"), mock_response]
        )

        response = await QwenProvider(api_key="sk-123").complete("Hello")

        assert response.text == "Qwen response"
        assert mock_client.chat.completions.create.call_count == 2
        sleep.assert_awaited_once_with(0.5)


@pytest.mark.asyncio
async def test_qwen_gives_up_after_max_retries():
    with (
        patch("openai.AsyncOpenAI") as mock_openai_class,
        patch(
            "dev_health_ops.llm.providers.local.asyncio.sleep", new_callable=AsyncMock
        ),
    ):
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create = AsyncMock(
            side_effect=[_RateLimitError(), _RateLimitError()]
        )

        with pytest.raises(LLMRateLimitError):
            await QwenProvider(api_key="sk-123").complete("Hello")

        assert mock_client.chat.completions.create.call_count == 2
