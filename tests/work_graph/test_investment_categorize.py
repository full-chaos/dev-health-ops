from __future__ import annotations

import asyncio

from dev_health_ops.llm import CompletionResult
from dev_health_ops.work_graph.investment.categorize import categorize_text_bundle
from dev_health_ops.work_graph.investment.types import TextBundle


class StubProvider:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
        self.prompts = []

    async def complete(self, prompt: str) -> CompletionResult:
        self.calls += 1
        self.prompts.append(prompt)
        return CompletionResult(
            text=self.responses[self.calls - 1],
            input_tokens=10 * self.calls,
            output_tokens=5 * self.calls,
            model="stub-model",
        )

    async def complete_text(self, prompt: str) -> str:
        return (await self.complete(prompt)).text


def _bundle() -> TextBundle:
    source_texts = {
        "issue": {"jira:ABC-1": "Fix login outage for auth service"},
        "pr": {},
        "commit": {},
    }
    return TextBundle(
        source_block="[issue] E1\nFix login outage for auth service",
        source_texts=source_texts,
        handle_map={"E1": ("issue", "jira:ABC-1")},
        input_hash="hash",
        text_source_count=1,
        text_char_count=40,
    )


def _valid_repaired_response() -> str:
    return """{
      "subcategories": {
        "feature_delivery.customer": 0.0,
        "feature_delivery.roadmap": 0.6,
        "feature_delivery.enablement": 0.0,
        "operational.incident_response": 0.0,
        "operational.on_call": 0.0,
        "operational.support": 0.0,
        "maintenance.refactor": 0.0,
        "maintenance.upgrade": 0.0,
        "maintenance.debt": 0.0,
        "quality.testing": 0.0,
        "quality.bugfix": 0.4,
        "quality.reliability": 0.0,
        "risk.security": 0.0,
        "risk.compliance": 0.0,
        "risk.vulnerability": 0.0
      },
      "evidence_quotes": [
        { "quote": "Fix login outage", "source": "issue", "id": "E1" }
      ],
      "uncertainty": "Some uncertainty remains."
    }"""


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
    assert outcome.llm_calls == 2
    assert outcome.input_tokens == 30
    assert outcome.output_tokens == 15
    assert outcome.llm_model == "stub-model"


def test_repaired_status(monkeypatch):
    provider = StubProvider(
        [
            "not json",
            _valid_repaired_response(),
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
    assert outcome.warnings == []
    assert outcome.llm_calls == 2
    assert outcome.input_tokens == 30
    assert outcome.output_tokens == 15


def test_repair_prompt_includes_targeted_guidance_for_zero_mass_and_long_quote(
    monkeypatch,
):
    long_quote = "Fix login outage for auth service " * 10
    provider = StubProvider(
        [
            f"""{{
              "subcategories": {{
                "feature_delivery.customer": 0.0,
                "feature_delivery.roadmap": 0.0,
                "feature_delivery.enablement": 0.0,
                "operational.incident_response": 0.0,
                "operational.on_call": 0.0,
                "operational.support": 0.0,
                "maintenance.refactor": 0.0,
                "maintenance.upgrade": 0.0,
                "maintenance.debt": 0.0,
                "quality.testing": 0.0,
                "quality.bugfix": 0.0,
                "quality.reliability": 0.0,
                "risk.security": 0.0,
                "risk.compliance": 0.0,
                "risk.vulnerability": 0.0
              }},
              "evidence_quotes": [
                {{ "quote": "{long_quote}", "source": "issue", "id": "E1" }}
              ],
              "uncertainty": "Some uncertainty remains."
            }}""",
            _valid_repaired_response(),
        ]
    )
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.categorize.get_provider",
        lambda name, model=None: provider,
    )

    outcome = asyncio.run(categorize_text_bundle(_bundle(), llm_provider="mock"))

    assert outcome.status == "repaired"
    assert provider.calls == 2
    assert "evidence_quote_too_long:0" in provider.prompts[1]
    assert "probability_sum_out_of_range:0.0000" in provider.prompts[1]
    assert "replace the quote with a shorter exact substring" in provider.prompts[1]
    assert "all probabilities were zero" in provider.prompts[1]


def test_repair_fallback_reports_repaired_response_errors(monkeypatch):
    long_quote = "Fix login outage for auth service " * 10
    invalid_payload = f"""{{
      "subcategories": {{
        "feature_delivery.customer": 0.0,
        "feature_delivery.roadmap": 0.0,
        "feature_delivery.enablement": 0.0,
        "operational.incident_response": 0.0,
        "operational.on_call": 0.0,
        "operational.support": 0.0,
        "maintenance.refactor": 0.0,
        "maintenance.upgrade": 0.0,
        "maintenance.debt": 0.0,
        "quality.testing": 0.0,
        "quality.bugfix": 0.0,
        "quality.reliability": 0.0,
        "risk.security": 0.0,
        "risk.compliance": 0.0,
        "risk.vulnerability": 0.0
      }},
      "evidence_quotes": [
        {{ "quote": "{long_quote}", "source": "issue", "id": "E1" }}
      ],
      "uncertainty": "Some uncertainty remains."
    }}"""
    provider = StubProvider([invalid_payload, invalid_payload])
    monkeypatch.setattr(
        "dev_health_ops.work_graph.investment.categorize.get_provider",
        lambda name, model=None: provider,
    )

    outcome = asyncio.run(categorize_text_bundle(_bundle(), llm_provider="mock"))

    assert outcome.status == "invalid_llm_output"
    assert outcome.errors == [
        "probability_sum_out_of_range:0.0000",
        "evidence_quote_too_long:0",
    ]
