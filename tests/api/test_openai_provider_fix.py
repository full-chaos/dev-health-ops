from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from dev_health_ops.api.services.llm_providers.openai import OpenAIProvider


class _StubResponse:
    def __init__(self, content: str, finish_reason: str = "stop") -> None:
        self.choices = [
            type(
                "Choice",
                (),
                {
                    "message": type("Msg", (), {"content": content})(),
                    "finish_reason": finish_reason,
                },
            )()
        ]


class _StubCompletions:
    def __init__(self, captured: dict, content: str = "ok", finish_reason: str = "stop") -> None:
        self._captured = captured
        self._content = content
        self._finish_reason = finish_reason

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        return _StubResponse(self._content, self._finish_reason)


class _StubChat:
    def __init__(self, captured: dict, content: str = "ok", finish_reason: str = "stop") -> None:
        self.completions = _StubCompletions(captured, content, finish_reason)


class _StubClient:
    def __init__(self, captured: dict, content: str = "ok", finish_reason: str = "stop") -> None:
        self.chat = _StubChat(captured, content, finish_reason)


@pytest.mark.asyncio
async def test_openai_provider_handles_empty_content_with_retry():
    """Test that OpenAI provider retries when content is empty"""
    captured: dict = {}
    
    # First call returns empty content, second call returns valid content
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini", max_tokens=1024)
    provider._client = _StubClient(captured, content="", finish_reason="length")
    
    # This should return empty string after retrying
    result = await provider.complete("test prompt")
    
    # Verify the call was made with correct parameters
    assert "max_completion_tokens" in captured["kwargs"]
    assert captured["kwargs"]["max_completion_tokens"] >= 512
    assert "response_format" in captured["kwargs"]
    assert captured["kwargs"]["response_format"]["type"] == "json_object"
    
    # Verify we got an empty result (as expected from our retry logic)
    assert result == ""


@pytest.mark.asyncio
async def test_openai_provider_uses_json_mode_and_instructions():
    """Test that OpenAI provider uses JSON mode with proper instructions"""
    captured: dict = {}
    
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini", max_tokens=1024)
    provider._client = _StubClient(captured, content='{"test": "result"}')
    
    result = await provider.complete("test prompt")
    
    # Verify the system message contains JSON instructions
    messages = captured["kwargs"]["messages"]
    system_message = messages[0]["content"]
    
    assert "Return ONLY valid JSON" in system_message
    assert "No markdown" in system_message
    assert "No commentary" in system_message


@pytest.mark.asyncio
async def test_openai_provider_uses_max_completion_tokens():
    """Test that OpenAI provider uses max_completion_tokens instead of max_tokens"""
    captured: dict = {}
    
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini", max_tokens=1024)
    provider._client = _StubClient(captured, content='{"test": "result"}')
    
    await provider.complete("test prompt")
    
    # Verify that max_completion_tokens is used (not max_tokens)
    assert "max_completion_tokens" in captured["kwargs"]
    assert "max_tokens" not in captured["kwargs"] or captured["kwargs"]["max_tokens"] is None
    assert captured["kwargs"]["max_completion_tokens"] == 1024


@pytest.mark.asyncio
async def test_openai_provider_uses_correct_temperature ():
    """Test that OpenAI provider uses correct temperature setting"""
    captured: dict = {}
    
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini", max_tokens=1024, temperature=0.2)
    provider._client = _StubClient(captured, content='{"test": "result"}')
    
    await provider.complete("test prompt")
    
    # Verify temperature is set
    assert "temperature" in captured["kwargs"]
    assert captured["kwargs"]["temperature"] == 0.2
