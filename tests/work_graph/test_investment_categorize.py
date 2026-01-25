from __future__ import annotations

import asyncio

from dev_health_ops.work_graph.investment.categorize import categorize_text_bundle
from dev_health_ops.work_graph.investment.types import TextBundle


class StubProvider:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
        self.prompts = []

    async def complete(self, prompt: str) -> str:
        self.calls += 1
        self.prompts.append(prompt)
        return self.responses[self.calls - 1]


def _bundle() -> TextBundle:
    source_texts = {
        "issue": {"jira:ABC-1": "Fix login outage for auth service"},
        "pr": {},
        "commit": {},
    }
    return TextBundle(
        source_block="[issue] jira:ABC-1\nFix login outage for auth service",
        source_texts=source_texts,
        input_hash="hash",
        text_source_count=1,
        text_char_count=40,
    )


def test_retry_limit_and_fallback(monkeypatch):
    provider = StubProvider(["not json", "still not json"])
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.categorize.get_provider",
        lambda name, model=None: provider,
    )
    outcome = asyncio.run(categorize_text_bundle(_bundle(), llm_provider="mock"))
    assert provider.calls == 2
    assert outcome.status == "invalid_llm_output"
    assert outcome.subcategories.get("feature_delivery.roadmap") == 0.2


def test_repaired_status(monkeypatch):
    provider = StubProvider(
        [
            "not json",
            """{
              "subcategories": {
                "feature_delivery.roadmap": 1.0
              },
              "evidence_quotes": [
                { "quote": "Fix login outage", "source": "issue", "id": "jira:ABC-1" }
              ],
              "uncertainty": "Some uncertainty remains."
            }""",
        ]
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.categorize.get_provider",
        lambda name, model=None: provider,
    )
    outcome = asyncio.run(categorize_text_bundle(_bundle(), llm_provider="mock"))
    assert provider.calls == 2
    assert "Output schema" in provider.prompts[1]
    assert outcome.status == "repaired"
