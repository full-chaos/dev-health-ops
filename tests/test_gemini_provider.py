import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.llm import LLMRateLimitError
from dev_health_ops.llm.providers import get_provider
from dev_health_ops.llm.providers.gemini import DEFAULT_GEMINI_BASE_URL, GeminiProvider


class _RateLimitError(Exception):
    status_code = 429

    def __init__(self, retry_after: str = "0.25") -> None:
        super().__init__("429 too many requests")
        self.headers = {"Retry-After": retry_after}


def test_gemini_provider_registration():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}, clear=True):
        assert isinstance(get_provider("gemini"), GeminiProvider)


def test_gemini_provider_config():
    p = GeminiProvider(api_key="test-key")
    assert p.api_key == "test-key"
    assert p.base_url == DEFAULT_GEMINI_BASE_URL
    assert p.model == "gemini-3"

    with patch.dict(
        os.environ,
        {
            "GEMINI_API_KEY": "env-gemini-key",
            "GEMINI_BASE_URL": "http://custom-gemini/v1",
            "GEMINI_MODEL": "gemini-3-pro",
        },
    ):
        p = GeminiProvider()
        assert p.api_key == "env-gemini-key"
        assert p.base_url == "http://custom-gemini/v1"
        assert p.model == "gemini-3-pro"


def test_gemini_auto_detection():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}):
        if "LLM_PROVIDER" in os.environ:
            del os.environ["LLM_PROVIDER"]
        if "OPENAI_API_KEY" in os.environ:
            del os.environ["OPENAI_API_KEY"]
        if "ANTHROPIC_API_KEY" in os.environ:
            del os.environ["ANTHROPIC_API_KEY"]
        p = get_provider("auto")
        assert isinstance(p, GeminiProvider)


@pytest.mark.asyncio
async def test_gemini_provider_completion():
    with patch("openai.AsyncOpenAI") as mock_openai_class:
        mock_client = MagicMock()
        mock_openai_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Gemini response"
        mock_response.usage = type(
            "Usage", (), {"prompt_tokens": 5, "completion_tokens": 3}
        )()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        p = GeminiProvider(api_key="sk-123")
        response = await p.complete("Hello")

        assert response.text == "Gemini response"
        assert response.model == "gemini-3"
        assert response.input_tokens == 5
        assert response.output_tokens == 3
        mock_openai_class.assert_called_once()
        _, client_kwargs = mock_openai_class.call_args
        assert client_kwargs["api_key"] == "sk-123"
        assert client_kwargs["base_url"] == DEFAULT_GEMINI_BASE_URL
        assert client_kwargs["max_retries"] == 0
        assert client_kwargs["http_client"].follow_redirects is False
        assert client_kwargs["http_client"].trust_env is False
        mock_client.chat.completions.create.assert_called_once()
        _, kwargs = mock_client.chat.completions.create.call_args
        assert kwargs["model"] == "gemini-3"


@pytest.mark.asyncio
async def test_gemini_retries_429_and_honors_retry_after():
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
        mock_response.choices[0].message.content = "Gemini response"
        mock_response.usage = None
        mock_client.chat.completions.create = AsyncMock(
            side_effect=[_RateLimitError("0.75"), mock_response]
        )

        response = await GeminiProvider(api_key="sk-123").complete("Hello")

        assert response.text == "Gemini response"
        assert mock_client.chat.completions.create.call_count == 2
        sleep.assert_awaited_once_with(0.75)


@pytest.mark.asyncio
async def test_gemini_gives_up_after_max_retries():
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
            await GeminiProvider(api_key="sk-123").complete("Hello")

        assert mock_client.chat.completions.create.call_count == 2
