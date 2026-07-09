import json

from dev_health_ops.llm.explainers.investment_mix_explainer import (
    parse_and_validate_response,
)


def test_parse_and_validate_response_rejects_dict_summary():
    raw_response = {
        "summary": {"statement": "This is a dictionary-based summary."},
        "top_findings": [
            {
                "finding": "Test finding",
                "evidence": {"theme": "feature_delivery", "share_pct": 50.0},
            }
        ],
        "confidence": {
            "level": "moderate",
            "quality_mean": 0.7,
            "quality_stddev": 0.1,
            "band_mix": {"high": 5},
            "drivers": ["test"],
        },
        "what_to_check_next": [],
        "anti_claims": [],
    }

    text = json.dumps(raw_response)
    result = parse_and_validate_response(text)

    assert result is None


def test_parse_and_validate_response_handles_string_summary():
    """Test that it still handles summary as a simple string."""
    raw_response = {
        "summary": "This summary appears string based.",
        "top_findings": [],
        "confidence": {
            "level": "low",
            "quality_mean": None,
            "quality_stddev": None,
            "band_mix": {
                "high": 0,
                "moderate": 0,
                "low": 0,
                "very_low": 0,
                "unknown": 0,
            },
            "drivers": [],
        },
        "what_to_check_next": [],
        "anti_claims": [],
    }

    text = json.dumps(raw_response)
    result = parse_and_validate_response(text)

    assert result is not None
    assert result["summary"] == "This summary appears string based."
