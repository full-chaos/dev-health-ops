from __future__ import annotations

import pytest

from dev_health_ops.api.services.llm_providers.openai import OpenAIProvider


class _StubResponse:
    def __init__(self) -> None:
        self.choices = [type("Choice", (), {"message": type("Msg", (), {"content": "ok"})()})()]


class _StubCompletions:
    def __init__(self, captured: dict) -> None:
        self._captured = captured

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        return _StubResponse()


class _StubChat:
    def __init__(self, captured: dict) -> None:
        self.completions = _StubCompletions(captured)


class _StubClient:
    def __init__(self, captured: dict) -> None:
        self.chat = _StubChat(captured)


@pytest.mark.asyncio
async def test_openai_provider_uses_max_completion_tokens_for_gpt5():
    captured: dict = {}
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini", max_tokens=123)
    provider._client = _StubClient(captured)

    result = await provider.complete("hello")
    assert result == "ok"
    assert "max_completion_tokens" in captured["kwargs"]
    assert captured["kwargs"]["max_completion_tokens"] == 123
    assert "max_tokens" not in captured["kwargs"]
    assert "temperature" not in captured["kwargs"]
