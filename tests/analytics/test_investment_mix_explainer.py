import json
from dev_health_ops.llm.explainers.investment_mix_explainer import (
    _extract_json_object,
    parse_and_validate_response,
)


def test_extract_json_object_basic():
    data = {"foo": "bar"}
    text = json.dumps(data)
    assert _extract_json_object(text) == data


def test_extract_json_object_with_markdown():
    data = {"foo": "bar"}
    text = f"""```json
{json.dumps(data)}
```"""
    assert _extract_json_object(text) == data

    text = f"""```
{json.dumps(data)}
```"""
    assert _extract_json_object(text) == data


def test_extract_json_object_with_preamble():
    data = {"foo": "bar"}
    text = f"""Here is the result:
{json.dumps(data)}
Hope it helps!"""
    assert _extract_json_object(text) == data


def test_extract_json_object_invalid():
    assert _extract_json_object("not json") is None
    assert _extract_json_object("{ invalid }") is None
    assert _extract_json_object("[]") is None  # must be a dict


def test_parse_and_validate_response_valid():
    payload = {
        "summary": "The distribution leans toward innovation themes (~40% of effort).",
        "top_findings": [
            {
                "finding": "Maintenance appears dominant in ~27% of effort.",
                "evidence": {
                    "theme": "maintenance",
                    "subcategory": "maintenance.refactor",
                    "share_pct": 27.0,
                    "evidence_quality_mean": 0.75,
                    "evidence_quality_band": "high",
                },
            }
        ],
        "confidence": {
            "level": "high",
            "quality_mean": 0.75,
            "quality_stddev": 0.1,
            "band_mix": {"high": 5, "moderate": 2},
            "drivers": [],
        },
        "what_to_check_next": [
            {
                "action": "Review refactor subcategory",
                "why": "High effort share",
                "where": "Subcategory panel",
            }
        ],
        "anti_claims": ["This does not measure productivity."],
    }
    text = json.dumps(payload)
    result = parse_and_validate_response(text)
    assert result is not None
    assert "leans toward innovation" in result["summary"]
    assert len(result["top_findings"]) == 1
    assert (
        result["top_findings"][0]["finding"]
        == "Maintenance appears dominant in ~27% of effort."
    )
    assert result["confidence"]["level"] == "high"
    assert result["status"] == "valid"


def test_parse_and_validate_response_forbidden_language():
    payload = {
        "summary": "This summary appears normal.",
        "top_findings": [
            {
                "finding": "It was determined that maintenance dominates.",  # "determined" is forbidden
                "evidence": {"theme": "maintenance", "share_pct": 30.0},
            }
        ],
        "confidence": {"level": "high"},
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_absolutely_forbidden():
    payload = {
        "summary": "This definitely shows a trend.",  # "definitely" is absolutely forbidden
        "top_findings": [],
        "confidence": {"level": "moderate"},
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_common_verbs():
    # "is" should be allowed (not in forbidden list)
    payload = {
        "summary": "The evidence is suggesting a trend in ~35% of effort.",
        "top_findings": [],
        "confidence": {"level": "moderate", "quality_mean": 0.6},
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    result = parse_and_validate_response(text)
    assert result is not None
    assert result["summary"] == "The evidence is suggesting a trend in ~35% of effort."


def test_parse_and_validate_response_missing_summary():
    payload = {
        "top_findings": [],
        "confidence": {"level": "low"},
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_fallback_confidence():
    payload = {
        "summary": "Effort appears spread across themes.",
        "top_findings": [],
        # confidence is missing, should use fallback
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    result = parse_and_validate_response(
        text,
        fallback_band_mix={"high": 3, "low": 2},
        fallback_drivers=["low_text_signal"],
        fallback_mean=0.55,
        fallback_stddev=0.2,
    )
    assert result is not None
    assert result["confidence"]["level"] == "unknown"
    assert result["confidence"]["band_mix"] == {"high": 3, "low": 2}
    assert result["confidence"]["drivers"] == ["low_text_signal"]
    assert result["confidence"]["quality_mean"] == 0.55
