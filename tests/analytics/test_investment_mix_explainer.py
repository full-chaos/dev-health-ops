import json
from analytics.investment_mix_explainer import _extract_json_object, parse_and_validate_response

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
    assert _extract_json_object("[]") is None # must be a dict

def test_parse_and_validate_response_valid():
    payload = {
        "summary": "The distribution leans toward innovation.",
        "dominant_themes": ["Theme A", "Theme B"],
        "key_drivers": ["Driver 1"],
        "operational_signals": ["Signal 1"],
        "confidence_note": "High confidence."
    }
    text = json.dumps(payload)
    result = parse_and_validate_response(text)
    assert result is not None
    assert result["summary"] == "The distribution leans toward innovation."
    assert result["dominant_themes"] == ["Theme A", "Theme B"]

def test_parse_and_validate_response_forbidden_language():
    payload = {
        "summary": "This is a summary.",
        "dominant_themes": ["Theme A"],
        "key_drivers": ["It was determined that..."], # " determined " is forbidden
        "operational_signals": [],
        "confidence_note": "Note."
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None

def test_parse_and_validate_response_common_verbs():
    # This test currently fails because "is" is forbidden
    payload = {
        "summary": "The evidence is suggesting a trend.",
        "dominant_themes": ["Theme A"],
        "key_drivers": [],
        "operational_signals": [],
        "confidence_note": "Note."
    }
    text = json.dumps(payload)
    # "is" is now allowed
    result = parse_and_validate_response(text)
    assert result is not None
    assert result["summary"] == "The evidence is suggesting a trend."

def test_parse_and_validate_response_missing_fields():
    payload = {
        "summary": "Summary only."
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None