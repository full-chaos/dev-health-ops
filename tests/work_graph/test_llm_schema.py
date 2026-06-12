from __future__ import annotations

from dev_health_ops.work_graph.investment.llm_schema import (
    parse_llm_json,
    validate_llm_payload,
)
from dev_health_ops.work_graph.investment.taxonomy import SUBCATEGORIES


def _source_texts() -> dict[str, dict[str, str]]:
    return {
        "issue": {"jira:ABC-1": "Fix login outage for auth service"},
        "pr": {"repo#pr1": "Add auth retry handling"},
        "commit": {"repo@abc": "Handle\n token   refresh"},
    }


def _handle_map() -> dict[str, tuple[str, str]]:
    return {
        "E1": ("issue", "jira:ABC-1"),
        "E2": ("pr", "repo#pr1"),
        "E3": ("commit", "repo@abc"),
    }


def test_rejects_unknown_keys_and_extra_keys():
    payload: dict[str, object] = {
        "subcategories": {"unknown.category": 1.0},
        "evidence_quotes": [
            {
                "quote": "Fix login outage",
                "source": "issue",
                "id": "E1",
                "extra": "x",
            }
        ],
        "uncertainty": "Limited evidence.",
        "extra": "nope",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert any("unexpected_top_level_keys" in err for err in result.errors)
    assert any("unknown_subcategory" in err for err in result.errors)
    assert any("evidence_quote_extra_keys" in err for err in result.errors)


def test_probabilities_normalize_to_one():
    subcategories = {key: 0.0 for key in SUBCATEGORIES}
    subcategories["feature_delivery.roadmap"] = 0.5
    subcategories["quality.bugfix"] = 0.5
    payload: dict[str, object] = {
        "subcategories": subcategories,
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok
    total = sum(result.subcategories.values())
    assert abs(total - 1.0) < 1e-6


def test_evidence_quote_must_be_substring():
    payload: dict[str, object] = {
        "subcategories": {"feature_delivery.roadmap": 1.0},
        "evidence_quotes": [{"quote": "Not in text", "source": "issue", "id": "E1"}],
        "uncertainty": "Limited evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert any("evidence_quote_not_substring" in err for err in result.errors)


def test_evidence_handle_round_trips_to_real_source_id_despite_source_mismatch():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [{"quote": "Fix login outage", "source": "pr", "id": "E1"}],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok
    assert result.evidence_quotes[0].source_type == "issue"
    assert result.evidence_quotes[0].source_id == "jira:ABC-1"


def test_evidence_handle_not_in_map_is_unknown_source():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E99"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "evidence_quote_unknown_source:0" in result.errors


def test_probabilities_within_loose_band_are_renormalized_and_accepted():
    payload: dict[str, object] = {
        "subcategories": {
            "feature_delivery.roadmap": 0.55,
            "quality.bugfix": 0.40,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok
    assert abs(sum(result.subcategories.values()) - 1.0) < 1e-6
    assert result.subcategories["feature_delivery.roadmap"] == 0.55 / 0.95


def test_degenerate_probability_sum_still_rejected():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 0.5},
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert any("probability_sum_out_of_range:0.5000" in err for err in result.errors)


def test_evidence_quote_substring_check_normalizes_whitespace():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [
            {"quote": "Handle token refresh", "source": "commit", "id": "E3"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok


def test_parse_llm_json_strict():
    payload, errors = parse_llm_json("{not json}")
    assert payload is None
    assert errors
