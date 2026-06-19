from __future__ import annotations

import pytest

from dev_health_ops.llm.providers.openai import OpenAIProvider


class _StubResponse:
    def __init__(self) -> None:
        self.output_text = '{"status": "ok"}'
        self.usage = type("Usage", (), {"input_tokens": 11, "output_tokens": 7})()


class _StubResponses:
    def __init__(self, captured: dict) -> None:
        self._captured = captured

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        return _StubResponse()


class _StubClient:
    def __init__(self, captured: dict) -> None:
        self.responses = _StubResponses(captured)


@pytest.mark.asyncio
async def test_openai_provider_uses_max_completion_tokens_for_gpt5():
    captured: dict = {}
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=123
    )
    provider._impl._client = _StubClient(captured)

    result = await provider.complete("hello")
    assert result.text == '{"status": "ok"}'
    assert result.model == "gpt-5-mini"
    assert result.input_tokens == 11
    assert result.output_tokens == 7
    # GPT-5 internal parameter is max_output_tokens
    assert "max_output_tokens" in captured["kwargs"]
    assert (
        captured["kwargs"]["max_output_tokens"] == 4096
    )  # max(clamped 2048 explanation default 4096)
    assert "max_tokens" not in captured["kwargs"]
    assert "temperature" not in captured["kwargs"]
