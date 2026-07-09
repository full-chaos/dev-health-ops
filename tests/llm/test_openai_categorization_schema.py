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


def test_categorization_schema_subcategory_values_allow_arbitrary_positive_weights():
    # Subcategory values are pre-normalization relative weights, not bounded
    # probabilities: no per-key "maximum" so a single subcategory can carry
    # weight > 1 before the caller normalizes the vector. Still non-negative.
    schema = categorization_json_schema()
    subcategory_schema = schema["properties"]["subcategories"]

    for value_schema in subcategory_schema["properties"].values():
        assert value_schema["type"] == "number"
        assert value_schema["minimum"] == 0
        assert "maximum" not in value_schema
