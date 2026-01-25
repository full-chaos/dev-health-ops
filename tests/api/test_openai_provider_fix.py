from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from dev_health_ops.llm.providers.openai import OpenAIProvider


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
    def __init__(
        self, captured: dict, content: str = "ok", finish_reason: str = "stop"
    ) -> None:
        self._captured = captured
        self._content = content
        self._finish_reason = finish_reason

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        return _StubResponse(self._content, self._finish_reason)


class _StubResponses:
    def __init__(
        self, captured: dict, content: str = "ok", finish_reason: str = "completed"
    ) -> None:
        self._captured = captured
        self._content = content
        self._finish_reason = finish_reason

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        # Map finish_reason to incomplete_details if needed
        resp = MagicMock()
        resp.output_text = self._content
        if self._finish_reason == "max_output_tokens":
            resp.incomplete_details = MagicMock(reason="max_output_tokens")
        else:
            resp.incomplete_details = None
        return resp


class _StubChat:
    def __init__(
        self, captured: dict, content: str = "ok", finish_reason: str = "stop"
    ) -> None:
        self.completions = _StubCompletions(captured, content, finish_reason)


class _StubClient:
    def __init__(
        self, captured: dict, content: str = "ok", finish_reason: str = "stop"
    ) -> None:
        self.chat = _StubChat(captured, content, finish_reason)
        # For GPT-5 tests, map "length" to "max_output_tokens"
        resp_reason = "max_output_tokens" if finish_reason == "length" else "completed"
        self.responses = _StubResponses(captured, content, resp_reason)


@pytest.mark.asyncio
async def test_openai_provider_handles_empty_content_with_retry():
    """Test that OpenAI provider retries when content is empty"""
    captured: dict = {}

    # First call returns empty content, second call returns valid content
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024
    )
    provider._impl._client = _StubClient(captured, content="", finish_reason="length")

    # This should return empty string after retrying
    result = await provider.complete("test prompt")

    # Verify the call was made with correct parameters
    assert "max_output_tokens" in captured["kwargs"]
    assert captured["kwargs"]["max_output_tokens"] >= 512
    assert "text" in captured["kwargs"]

    # Verify we got an empty result (as expected from our retry logic)
    assert result == ""


@pytest.mark.asyncio
async def test_openai_provider_uses_json_mode_and_instructions():
    """Test that OpenAI provider uses JSON mode with proper instructions"""
    captured: dict = {}

    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024
    )
    provider._impl._client = _StubClient(captured, content='{"test": "result"}')

    # Use a prompt that triggers the "schema" system message to verify all instructions
    prompt = 'Output schema: "subcategories", "evidence_quotes", "uncertainty"'
    await provider.complete(prompt)

    # Verify the system message contains JSON instructions
    # GPT-5 uses instructions and input
    instructions = captured["kwargs"]["instructions"]

    assert "Return ONLY valid JSON" in instructions
    assert "No markdown" in instructions
    assert "No commentary" in instructions


@pytest.mark.asyncio
async def test_openai_provider_uses_max_completion_tokens():
    """Test that OpenAI provider uses max_completion_tokens instead of max_tokens"""
    captured: dict = {}

    # For GPT-5 it uses max_output_tokens, but the facade accepts max_completion_tokens
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024
    )
    provider._impl._client = _StubClient(captured, content='{"test": "result"}')

    await provider.complete("test prompt")

    # Verify that max_output_tokens is used (for GPT-5).
    # Logic: max(1024 (clamped), 2048 (default for explanation)) = 2048
    assert "max_output_tokens" in captured["kwargs"]
    assert captured["kwargs"]["max_output_tokens"] == 4096


@pytest.mark.asyncio
async def test_openai_provider_uses_correct_temperature():
    """Test that OpenAI provider uses correct temperature setting"""
    captured: dict = {}

    # gpt-4o supports temperature
    provider = OpenAIProvider(
        api_key="test", model="gpt-4o", max_completion_tokens=1024, temperature=0.2
    )
    provider._impl._client = _StubClient(captured, content='{"test": "result"}')

    await provider.complete("test prompt")

    # Verify temperature is set
    assert "temperature" in captured["kwargs"]
    assert captured["kwargs"]["temperature"] == 0.2
