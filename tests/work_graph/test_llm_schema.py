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
    assert result.warnings == []
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


def test_weights_close_to_one_are_renormalized_with_warning():
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
    assert result.warnings == ["weights_normalized:0.9500"]
    assert abs(sum(result.subcategories.values()) - 1.0) < 1e-6
    assert result.subcategories["feature_delivery.roadmap"] == 0.55 / 0.95


def test_relative_weights_of_any_positive_scale_are_normalized():
    payload: dict[str, object] = {
        "subcategories": {
            "feature_delivery.roadmap": 5,
            "quality.bugfix": 15,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok
    assert result.warnings == ["weights_normalized:20.0000"]
    assert abs(result.subcategories["feature_delivery.roadmap"] - 0.25) < 1e-9
    assert abs(result.subcategories["quality.bugfix"] - 0.75) < 1e-9


def test_low_weight_sum_previously_rejected_is_now_accepted_and_normalized():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 0.5},
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok
    assert result.warnings == ["weights_normalized:0.5000"]
    assert result.subcategories["quality.bugfix"] == 1.0


def test_all_zero_weights_are_rejected():
    payload: dict[str, object] = {
        "subcategories": {key: 0.0 for key in SUBCATEGORIES},
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "all_weights_zero" in result.errors


def test_missing_subcategories_are_rejected_as_all_weights_zero():
    payload: dict[str, object] = {
        "subcategories": {},
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "all_weights_zero" in result.errors


def test_negative_weight_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": -0.5,
            "feature_delivery.roadmap": 1.0,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "negative_weight:quality.bugfix" in result.errors


def test_nan_weight_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": float("nan"),
            "feature_delivery.roadmap": 1.0,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "non_finite_weight:quality.bugfix" in result.errors


def test_infinite_weight_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": float("inf"),
            "feature_delivery.roadmap": 1.0,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "non_finite_weight:quality.bugfix" in result.errors


def test_boolean_weight_value_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": True,
            "feature_delivery.roadmap": 1.0,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "invalid_weight:quality.bugfix" in result.errors


def test_string_weight_value_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": "0.5",
            "feature_delivery.roadmap": 1.0,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "invalid_weight:quality.bugfix" in result.errors


def test_null_weight_value_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": None,
            "feature_delivery.roadmap": 1.0,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "invalid_weight:quality.bugfix" in result.errors


def test_weight_sum_overflow_is_rejected():
    huge = 1.0e308
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": huge,
            "feature_delivery.roadmap": huge,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "weight_sum_not_finite" in result.errors


def test_integer_weight_overflow_is_rejected_without_raising():
    payload: dict[str, object] = {
        "subcategories": {
            "quality.bugfix": 10**10000,
            "feature_delivery.roadmap": 1,
        },
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "weight_overflow:quality.bugfix" in result.errors


def test_non_string_quote_fields_are_rejected():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [{"quote": 42, "source": "issue", "id": "E1"}],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "evidence_quote_invalid_type:0" in result.errors


def test_non_string_uncertainty_is_rejected():
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [
            {"quote": "Fix login outage", "source": "issue", "id": "E1"}
        ],
        "uncertainty": {"text": "not a string"},
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert not result.ok
    assert "uncertainty_invalid_type" in result.errors


def test_exact_quote_over_280_chars_is_rejected():
    long_quote = "Fix login outage for auth service " * 10
    source_texts = {
        "issue": {"jira:ABC-1": long_quote},
        "pr": _source_texts()["pr"],
        "commit": _source_texts()["commit"],
    }
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [{"quote": long_quote, "source": "issue", "id": "E1"}],
        "uncertainty": "Reasonable confidence based on evidence.",
    }

    assert len(long_quote) > 280
    result = validate_llm_payload(payload, source_texts, _handle_map())
    assert not result.ok
    assert any("evidence_quote_too_long:0" in err for err in result.errors)


def test_evidence_quote_substring_check_normalizes_whitespace():
    source_text = _source_texts()["commit"]["repo@abc"]
    payload: dict[str, object] = {
        "subcategories": {"quality.bugfix": 1.0},
        "evidence_quotes": [
            {"quote": "Handle token refresh", "source": "commit", "id": "E3"}
        ],
        "uncertainty": "Reasonable confidence based on evidence.",
    }
    result = validate_llm_payload(payload, _source_texts(), _handle_map())
    assert result.ok
    assert result.evidence_quotes[0].quote == "Handle\n token   refresh"
    assert result.evidence_quotes[0].quote in source_text


def test_parse_llm_json_strict():
    payload, errors = parse_llm_json("{not json}")
    assert payload is None
    assert errors
