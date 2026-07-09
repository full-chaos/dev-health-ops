from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.llm.explainers.investment_mix_explainer import (
    build_prompt,
    load_prompt,
)
from dev_health_ops.llm.providers.batch import BatchItemRequest
from dev_health_ops.llm.providers.openai import (
    CATEGORIZATION_RESPONSE_FORMAT,
    RESPONSE_FORMAT_MARKER,
    OpenAIProvider,
    investment_mix_explanation_json_schema,
    is_investment_mix_explanation_prompt,
    is_json_schema_prompt,
)

_SAMPLE_PAYLOAD: dict[str, Any] = {
    "focus": {"theme": None, "subcategory": None},
    "total_effort": 1.0,
    "theme_distribution_top": [{"theme": "maintenance", "value": 0.4, "pct": 0.4}],
    "subcategory_distribution_top": [],
    "work_unit_count": 3,
    "work_unit_dominant_subcategory_counts_top": [],
    "evidence_quality_band_counts": {"high": 2, "moderate": 1},
    "evidence_quality_mean": 0.7,
    "evidence_quality_stddev": 0.1,
    "quality_drivers": [],
    "evidence_quote_samples": [],
}

_CATEGORIZATION_PROMPT = (
    f"{RESPONSE_FORMAT_MARKER}{CATEGORIZATION_RESPONSE_FORMAT}\n"
    """Output schema:
{
  "subcategories": {"feature_delivery.roadmap": 1.0},
  "evidence_quotes": [{"quote": "x", "source": "issue", "id": "jira:ABC-1"}],
  "uncertainty": "..."
}
"""
)


def _investment_mix_prompt() -> str:
    return build_prompt(base_prompt=load_prompt(), payload=_SAMPLE_PAYLOAD)


# -----------------------------------------------------------------------------
# Schema shape (strict Structured Outputs contract)
# -----------------------------------------------------------------------------


def _walk_object_schemas(schema: dict[str, Any]) -> list[dict[str, Any]]:
    """Collect every nested schema node that declares "properties"."""
    found: list[dict[str, Any]] = []
    if isinstance(schema, dict):
        if "properties" in schema:
            found.append(schema)
        for value in schema.get("properties", {}).values():
            found.extend(_walk_object_schemas(value))
        items = schema.get("items")
        if isinstance(items, dict):
            found.extend(_walk_object_schemas(items))
    return found


def test_investment_mix_explanation_schema_top_level_contract():
    schema = investment_mix_explanation_json_schema()

    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "summary",
        "top_findings",
        "confidence",
        "what_to_check_next",
        "anti_claims",
    }
    assert set(schema["properties"]) == set(schema["required"])


def test_investment_mix_explanation_schema_every_object_node_is_strict():
    schema = investment_mix_explanation_json_schema()
    for node in _walk_object_schemas(schema):
        assert node["additionalProperties"] is False
        # Structured Outputs strict mode requires every property to be listed
        # in "required" (nullability is expressed via type unions instead).
        assert set(node["required"]) == set(node["properties"])


def test_investment_mix_explanation_schema_finding_evidence_bounds():
    schema = investment_mix_explanation_json_schema()
    evidence_schema = schema["properties"]["top_findings"]["items"]["properties"][
        "evidence"
    ]

    assert set(evidence_schema["required"]) == {
        "theme",
        "subcategory",
        "share_pct",
        "delta_pct_points",
        "evidence_quality_mean",
        "evidence_quality_band",
    }
    assert evidence_schema["properties"]["subcategory"]["type"] == ["string", "null"]
    assert evidence_schema["properties"]["delta_pct_points"]["type"] == [
        "number",
        "null",
    ]
    assert evidence_schema["properties"]["evidence_quality_band"]["type"] == [
        "string",
        "null",
    ]


def test_investment_mix_explanation_schema_confidence_band_mix_enforces_fixed_bands():
    schema = investment_mix_explanation_json_schema()
    band_mix_schema = schema["properties"]["confidence"]["properties"]["band_mix"]

    assert set(band_mix_schema["required"]) == {
        "high",
        "moderate",
        "low",
        "very_low",
        "unknown",
    }
    assert band_mix_schema["additionalProperties"] is False
    for prop_schema in band_mix_schema["properties"].values():
        assert prop_schema["type"] == "integer"


def test_investment_mix_explanation_schema_confidence_level_enum():
    schema = investment_mix_explanation_json_schema()
    level_schema = schema["properties"]["confidence"]["properties"]["level"]

    assert level_schema["enum"] == ["high", "moderate", "low", "unknown"]


def test_investment_mix_explanation_schema_action_item_requires_all_fields():
    schema = investment_mix_explanation_json_schema()
    action_item_schema = schema["properties"]["what_to_check_next"]["items"]

    assert set(action_item_schema["required"]) == {"action", "why", "where"}
    for prop_schema in action_item_schema["properties"].values():
        assert prop_schema["type"] == "string"
        assert prop_schema["minLength"] == 1


# -----------------------------------------------------------------------------
# Prompt-detection heuristic
# -----------------------------------------------------------------------------


def test_is_investment_mix_explanation_prompt_true_for_real_prompt_and_payload():
    prompt = _investment_mix_prompt()

    assert is_investment_mix_explanation_prompt(prompt) is True
    # Must not collide with the categorization heuristic.
    assert is_json_schema_prompt(prompt) is False


def test_is_investment_mix_explanation_prompt_false_for_categorization_prompt():
    assert is_investment_mix_explanation_prompt(_CATEGORIZATION_PROMPT) is False
    assert is_json_schema_prompt(_CATEGORIZATION_PROMPT) is True


def test_is_investment_mix_explanation_prompt_false_for_generic_prompt():
    assert (
        is_investment_mix_explanation_prompt("Summarize this for me please.") is False
    )


def test_is_investment_mix_explanation_prompt_true_when_base_prompt_missing():
    prompt = build_prompt(base_prompt="", payload=_SAMPLE_PAYLOAD)
    assert is_investment_mix_explanation_prompt(prompt) is True


# -----------------------------------------------------------------------------
# Wiring into the GPT-5 Responses API path (interactive + batch)
# -----------------------------------------------------------------------------


class _StubResponses:
    def __init__(self, captured: dict) -> None:
        self._captured = captured

    async def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        resp = MagicMock()
        resp.output_text = '{"summary": "ok"}'
        resp.incomplete_details = None
        return resp


class _StubClient:
    def __init__(self, captured: dict) -> None:
        self.responses = _StubResponses(captured)


@pytest.mark.asyncio
async def test_gpt5_provider_uses_strict_schema_for_investment_mix_explanation():
    captured: dict = {}
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini")
    provider._impl._client = _StubClient(captured)

    await provider.complete(_investment_mix_prompt())

    text_format = captured["kwargs"]["text"]["format"]
    assert text_format["type"] == "json_schema"
    assert text_format["name"] == "investment_mix_explanation"
    assert text_format["strict"] is True
    assert text_format["schema"] == investment_mix_explanation_json_schema()
    assert captured["kwargs"]["text"]["verbosity"] == "low"


@pytest.mark.asyncio
async def test_gpt5_provider_uses_json_object_for_unstructured_explanation_prompt():
    captured: dict = {}
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini")
    provider._impl._client = _StubClient(captured)

    await provider.complete("Explain this precomputed data in plain prose.")

    assert captured["kwargs"]["text"]["format"] == {"type": "json_object"}


def test_batch_body_uses_strict_schema_for_investment_mix_explanation():
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini")
    body = provider._impl._batch_body(_investment_mix_prompt())

    text_format = body["text"]["format"]
    assert text_format["type"] == "json_schema"
    assert text_format["name"] == "investment_mix_explanation"
    assert text_format["strict"] is True
    assert text_format["schema"] == investment_mix_explanation_json_schema()
    assert body["text"]["verbosity"] == "low"


def test_batch_body_still_uses_categorization_schema_for_categorization_prompt():
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini")
    body = provider._impl._batch_body(_CATEGORIZATION_PROMPT)

    assert body["text"]["format"]["name"] == "categorization"


def test_batch_item_explicit_response_format_overrides_prompt_routing():
    provider = OpenAIProvider(api_key="test", model="gpt-5-mini")
    explicit_format = {
        "type": "json_schema",
        "name": "explicit_contract",
        "strict": True,
        "schema": {"type": "object", "properties": {}, "required": []},
    }
    line = provider._impl._batch_line(
        BatchItemRequest(
            custom_id="item-1",
            prompt="Generic prompt without a marker",
            response_format=explicit_format,
        )
    )

    assert json.loads(line)["body"]["text"]["format"] == explicit_format
