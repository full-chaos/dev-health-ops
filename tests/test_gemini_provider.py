import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.llm.providers import get_provider
from dev_health_ops.llm.providers.gemini import DEFAULT_GEMINI_BASE_URL, GeminiProvider


def test_gemini_provider_registration():
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
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        p = GeminiProvider(api_key="sk-123")
        response = await p.complete("Hello")

        assert response == "Gemini response"
        mock_openai_class.assert_called_once_with(
            api_key="sk-123", base_url=DEFAULT_GEMINI_BASE_URL
        )
        mock_client.chat.completions.create.assert_called_once()
        _, kwargs = mock_client.chat.completions.create.call_args
        assert kwargs["model"] == "gemini-3"
