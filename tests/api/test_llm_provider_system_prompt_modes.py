from __future__ import annotations

import pytest

from dev_health_ops.api.services.llm_providers.openai import OpenAIProvider


class _StubResponse:
    def __init__(self, content: str) -> None:
        self.choices = [
            type(
                "Choice",
                (),
                {"message": type("Msg", (), {"content": content})()},
            )()
        ]


class _StubCompletions:
    def __init__(self, captured: dict) -> None:
        self._captured = captured

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        return _StubResponse("ok")


class _StubChat:
    def __init__(self, captured: dict) -> None:
        self.completions = _StubCompletions(captured)


class _StubClient:
    def __init__(self, captured: dict) -> None:
        self.chat = _StubChat(captured)


@pytest.mark.asyncio
async def test_openai_provider_uses_json_system_prompt_for_schema_prompts():
    captured: dict = {}
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini", max_tokens=50)
    provider._client = _StubClient(captured)

    prompt = """Output schema:
{
  "subcategories": { "feature_delivery.roadmap": 1.0 },
  "evidence_quotes": [{ "quote": "x", "source": "issue", "id": "jira:ABC-1" }],
  "uncertainty": "..."
}
"""
    await provider.complete(prompt)
    system = captured["kwargs"]["messages"][0]["content"]
    assert "JSON" in system

