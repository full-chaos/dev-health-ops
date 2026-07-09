from __future__ import annotations

from dev_health_ops.llm.providers.openai import categorization_json_schema
from dev_health_ops.work_graph.investment.taxonomy import SUBCATEGORIES


def test_categorization_schema_enforces_quote_length_bounds():
    schema = categorization_json_schema()
    quote_schema = schema["properties"]["evidence_quotes"]["items"]["properties"][
        "quote"
    ]

    assert quote_schema["type"] == "string"
    assert quote_schema["minLength"] == 1
    assert quote_schema["maxLength"] == 280


def test_categorization_schema_enforces_non_empty_id():
    schema = categorization_json_schema()
    id_schema = schema["properties"]["evidence_quotes"]["items"]["properties"]["id"]

    assert id_schema["type"] == "string"
    assert id_schema["minLength"] == 1


def test_categorization_schema_matches_canonical_subcategories():
    schema = categorization_json_schema()
    subcategory_schema = schema["properties"]["subcategories"]

    assert set(subcategory_schema["required"]) == set(SUBCATEGORIES)
    assert set(subcategory_schema["properties"]) == set(SUBCATEGORIES)
