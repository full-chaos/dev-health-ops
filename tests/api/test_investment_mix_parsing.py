import json
from dev_health_ops.llm.explainers.investment_mix_explainer import (
    parse_and_validate_response,
)


def test_parse_and_validate_response_handles_dict_summary():
    """Test that it handles summary as a dictionary containing 'statement'."""
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

    assert result is not None
    assert result["summary"] == "This is a dictionary-based summary."


def test_parse_and_validate_response_handles_string_summary():
    """Test that it still handles summary as a simple string."""
    raw_response = {
        "summary": "This is a string-based summary.",
        "top_findings": [],
        "confidence": {"level": "low", "band_mix": {}, "drivers": []},
        "what_to_check_next": [],
        "anti_claims": [],
    }

    text = json.dumps(raw_response)
    result = parse_and_validate_response(text)

    assert result is not None
    assert result["summary"] == "This is a string-based summary."
