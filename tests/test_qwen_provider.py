import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from dev_health_ops.llm.providers import get_provider
from dev_health_ops.llm.providers.qwen import (
    QwenProvider,
    QwenLocalProvider,
    QwenLMStudioProvider,
    DEFAULT_DASHSCOPE_BASE_URL,
)


def test_qwen_provider_registration():
    # Test explicit names
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
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        p = QwenProvider(api_key="sk-123")
        response = await p.complete("Hello")

        assert response == "Qwen response"
        # Verify client was initialized with correct base_url
        mock_openai_class.assert_called_once_with(
            api_key="sk-123", base_url=DEFAULT_DASHSCOPE_BASE_URL
        )
        # Verify completion was called with correct model
        mock_client.chat.completions.create.assert_called_once()
        args, kwargs = mock_client.chat.completions.create.call_args
        assert kwargs["model"] == "qwen-plus"
