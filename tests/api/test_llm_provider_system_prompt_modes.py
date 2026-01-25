from __future__ import annotations

from unittest.mock import MagicMock
import pytest

from dev_health_ops.llm.providers.openai import OpenAIProvider


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


class _StubResponses:
    def __init__(self, captured: dict) -> None:
        self._captured = captured

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        resp = MagicMock()
        resp.output_text = "{}"
        resp.incomplete_details = None
        return resp


class _StubClient:
    def __init__(self, captured: dict) -> None:
        self.responses = _StubResponses(captured)


@pytest.mark.asyncio
async def test_openai_provider_uses_json_system_prompt_for_schema_prompts():
    captured: dict = {}
    provider = OpenAIProvider(
        api_key="test", model="gpt-5-mini", max_completion_tokens=1024, temperature=0.2
    )
    provider._impl._client = _StubClient(captured)

    prompt = """Output schema:
{
  "subcategories": { "feature_delivery.roadmap": 1.0 },
  "evidence_quotes": [{ "quote": "x", "source": "issue", "id": "jira:ABC-1" }],
  "uncertainty": "..."
}
"""
    await provider.complete(prompt)
    system = captured["kwargs"]["instructions"]
    assert "JSON" in system
