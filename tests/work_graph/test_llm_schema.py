from __future__ import annotations

from dev_health_ops.work_graph.investment.llm_schema import (
    parse_llm_json,
    validate_llm_payload,
)
from dev_health_ops.work_graph.investment.taxonomy import SUBCATEGORIES


def _source_texts():
    return {
        "issue": {"jira:ABC-1": "Fix login outage for auth service"},
        "pr": {"repo#pr1": "Add auth retry handling"},
        "commit": {"repo@abc": "Handle token refresh"},
    }


def test_rejects_unknown_keys_and_extra_keys():
    payload = {
        "subcategories": {"unknown.category": 1.0},
        "evidence_quotes": [
            {
                "quote": "Fix login outage",
                "source": "issue",
                "id": "jira:ABC-1",
                "extra": "x",
            }
        ],
        "uncertainty": "Limited evidence.",
        "extra": "nope",
    }
    result = validate_llm_payload(payload, _source_texts())
    assert not result.ok
    assert any("unexpected_top_level_keys" in err for err in result.errors)
    assert any("unknown_subcategory" in err for err in result.errors)
    assert any("evidence_quote_extra_keys" in err for err in result.errors)


def test_probabilities_normalize_to_one():
    subcategories = {key: 0.0 for key in SUBCATEGORIES}
    subcategories["feature_delivery.roadmap"] = 0.5
    subcategories["quality.bugfix"] = 0.5
    payload = {
        "subcategories": subcategories,
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "jira:ABC-1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts())
    assert result.ok
    total = sum(result.subcategories.values())
    assert abs(total - 1.0) < 1e-6


def test_evidence_quote_must_be_substring():
    payload = {
        "subcategories": {"feature_delivery.roadmap": 1.0},
        "evidence_quotes": [
            {"quote": "Not in text", "source": "issue", "id": "jira:ABC-1"}
        ],
        "uncertainty": "Limited evidence.",
    }
    result = validate_llm_payload(payload, _source_texts())
    assert not result.ok
    assert any("evidence_quote_not_substring" in err for err in result.errors)


def test_parse_llm_json_strict():
    payload, errors = parse_llm_json("{not json}")
    assert payload is None
    assert errors
