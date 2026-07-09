import json

from dev_health_ops.llm.explainers.investment_mix_explainer import (
    PROMPT_VERSION,
    _extract_json_object,
    parse_and_validate_response,
    parse_investment_mix_response,
)


def _confidence() -> dict[str, object]:
    return {
        "level": "high",
        "quality_mean": 0.75,
        "quality_stddev": 0.1,
        "band_mix": {
            "high": 5,
            "moderate": 2,
            "low": 0,
            "very_low": 0,
            "unknown": 0,
        },
        "drivers": [],
    }


def _finding_evidence(**overrides: object) -> dict[str, object]:
    evidence: dict[str, object] = {
        "theme": "maintenance",
        "subcategory": "maintenance.refactor",
        "share_pct": 27.0,
        "delta_pct_points": None,
        "evidence_quality_mean": 0.75,
        "evidence_quality_band": "high",
    }
    evidence.update(overrides)
    return evidence


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
        "summary": "The distribution leans toward innovation themes.",
        "top_findings": [
            {
                "finding": "Maintenance appears dominant.",
                "evidence": _finding_evidence(),
            }
        ],
        "confidence": _confidence(),
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
    result = parse_and_validate_response(
        text,
        theme_shares_pct={"maintenance": 40.0},
        subcategory_shares_pct={"maintenance.refactor": 35.0},
        fallback_level="moderate",
        fallback_mean=0.6,
    )
    assert result is not None
    assert "leans toward innovation" in result["summary"]
    assert len(result["top_findings"]) == 1
    assert (
        result["top_findings"][0]["finding"]
        == "Maintenance appears dominant (~35% of effort)."
    )
    assert result["confidence"]["level"] == "moderate"
    assert result["top_findings"][0]["evidence"]["share_pct"] == 35.0
    assert result["top_findings"][0]["evidence"]["evidence_quality_mean"] == 0.6
    assert result["top_findings"][0]["evidence"]["evidence_quality_band"] is None
    assert result["status"] == "valid"


def test_parse_rejects_invented_taxonomy_and_non_finite_metadata():
    payload = {
        "summary": "The mix appears concentrated.",
        "top_findings": [
            {
                "finding": "Invented work appears dominant.",
                "evidence": _finding_evidence(theme="invented", share_pct=float("nan")),
            }
        ],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }

    result = parse_investment_mix_response(
        json.dumps(payload),
        theme_shares_pct={"maintenance": 40.0},
        subcategory_shares_pct={"maintenance.refactor": 40.0},
    )

    assert result.status == "invalid_llm_output"
    assert result.output is None


def test_parse_rejects_boolean_numeric_metadata():
    payload = {
        "summary": "The mix appears concentrated.",
        "top_findings": [
            {
                "finding": "Maintenance appears dominant.",
                "evidence": _finding_evidence(share_pct=True),
            }
        ],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }

    result = parse_investment_mix_response(
        json.dumps(payload),
        theme_shares_pct={"maintenance": 40.0},
        subcategory_shares_pct={"maintenance.refactor": 40.0},
    )

    assert result.status == "invalid_llm_output"


def test_parse_rejects_huge_integer_metadata_without_raising():
    payload = {
        "summary": "The mix appears concentrated.",
        "top_findings": [
            {
                "finding": "Maintenance appears dominant.",
                "evidence": _finding_evidence(share_pct=10**4000),
            }
        ],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }

    result = parse_investment_mix_response(
        json.dumps(payload),
        theme_shares_pct={"maintenance": 40.0},
        subcategory_shares_pct={"maintenance.refactor": 40.0},
    )

    assert result.status == "invalid_llm_output"


def test_parse_distinguishes_empty_object_from_invalid_json():
    assert parse_investment_mix_response("{}").status == "invalid_llm_output"
    assert parse_investment_mix_response("not json").status == "invalid_json"


def test_parse_enforces_schema_collection_and_string_bounds():
    payload = {
        "summary": "x" * 1001,
        "top_findings": [],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }
    assert (
        parse_investment_mix_response(json.dumps(payload)).status
        == "invalid_llm_output"
    )

    payload["summary"] = "The mix appears distributed."
    payload["anti_claims"] = ["No claim is made."] * 11
    assert (
        parse_investment_mix_response(json.dumps(payload)).status
        == "invalid_llm_output"
    )


def test_parse_and_validate_response_forbidden_language():
    payload = {
        "summary": "This summary appears normal.",
        "top_findings": [
            {
                "finding": "It was determined that maintenance dominates.",  # "determined" is forbidden
                "evidence": _finding_evidence(),
            }
        ],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    result = parse_investment_mix_response(
        text,
        theme_shares_pct={"maintenance": 30.0},
        subcategory_shares_pct={"maintenance.refactor": 30.0},
    )
    assert result.status == "forbidden_language"
    assert result.output is None


def test_parse_and_validate_response_absolutely_forbidden():
    payload = {
        "summary": "This definitely shows a trend.",  # "definitely" is absolutely forbidden
        "top_findings": [],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_rejects_is():
    payload = {
        "summary": "The evidence is suggesting a trend.",
        "top_findings": [],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_missing_summary():
    payload = {
        "top_findings": [],
        "confidence": {"level": "low"},
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_missing_confidence_is_invalid():
    payload = {
        "summary": "Effort appears spread across themes.",
        "top_findings": [],
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_missing_confidence_object_entirely():
    payload = {
        "summary": "Effort appears spread across themes.",
        "top_findings": [],
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_rejects_malformed_findings():
    payload = {
        "summary": "Effort appears concentrated in maintenance.",
        "top_findings": [
            {"finding": "Missing evidence entirely"},
            {"finding": "Evidence missing theme", "evidence": {"share_pct": 10.0}},
            {
                "finding": "Well-formed finding",
                "evidence": _finding_evidence(),
            },
        ],
        "confidence": _confidence(),
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert (
        parse_and_validate_response(
            text,
            theme_shares_pct={"maintenance": 40.0},
            subcategory_shares_pct={"maintenance.refactor": 40.0},
        )
        is None
    )


def test_parse_and_validate_response_rejects_invalid_confidence_types():
    payload = {
        "summary": "Effort appears spread across themes.",
        "top_findings": [],
        "confidence": {
            "level": "not-a-real-level",
            "quality_mean": "high",
            "quality_stddev": "low",
            "band_mix": "not-a-dict",
            "drivers": "not-a-list",
        },
        "what_to_check_next": [],
        "anti_claims": [],
    }
    text = json.dumps(payload)
    assert parse_and_validate_response(text) is None


def test_parse_and_validate_response_rejects_non_json_text():
    assert parse_and_validate_response("This is not JSON at all.") is None


def test_parse_and_validate_response_rejects_empty_string():
    assert parse_and_validate_response("") is None


def test_prompt_version_is_a_stable_low_cardinality_constant():
    # `PROMPT_VERSION` is emitted as a Prometheus `prompt_version` label by
    # work_graph/investment/llm_telemetry.py::record_explanation_parse — it
    # must be a short fixed string, never user/org-derived, so it can never
    # explode metric cardinality.
    assert isinstance(PROMPT_VERSION, str)
    assert PROMPT_VERSION
    assert len(PROMPT_VERSION) <= 64
    assert PROMPT_VERSION == "investment-mix-explain-v2"
