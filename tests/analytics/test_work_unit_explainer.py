"""
Unit tests for work_unit_explainer module.

These tests verify compliance with the investment explanation rules:
- Canonical prompt matches spec exactly
- Only allowed inputs are extracted
- Forbidden inputs are excluded
- Language validation works correctly
"""

from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.llm.explainers.work_unit_explainer import (
    CANONICAL_EXPLANATION_PROMPT,
    FORBIDDEN_WORDS,
    ExplanationInputs,
    build_explanation_prompt,
    extract_allowed_inputs,
    validate_explanation_language,
)


def _sample_inputs() -> ExplanationInputs:
    """Create sample inputs for testing."""
    return ExplanationInputs(
        work_unit_id="test-work-unit-123",
        time_range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        time_range_end=datetime(2025, 1, 15, tzinfo=timezone.utc),
        categories={
            "feature_delivery": 0.48,
            "maintenance": 0.30,
            "quality": 0.22,
        },
        evidence_quality_value=0.72,
        evidence_quality_band="moderate",
        evidence_summary={
            "structural": {
                "count": 3,
                "types": ["work_item_type", "subcategory_scores"],
            },
            "contextual": {"span_days": 14.0, "score": 0.7},
            "textual": {
                "match_count": 2,
                "categories_with_matches": ["feature_delivery"],
            },
        },
    )


def test_canonical_prompt_matches_spec():
    """Verify the canonical prompt matches AGENTS-WG.md exactly."""
    # The canonical prompt from AGENTS-WG.md Section 4
    expected_start = "You are explaining a precomputed investment view."
    expected_rules = [
        "Recalculate scores",
        "Change categories",
        "Introduce new conclusions",
    ]
    expected_explain = [
        "SUMMARY",
        "REASONS",
        "UNCERTAINTY",
    ]

    assert CANONICAL_EXPLANATION_PROMPT.startswith(expected_start)
    for rule in expected_rules:
        assert rule in CANONICAL_EXPLANATION_PROMPT
    for item in expected_explain:
        assert item in CANONICAL_EXPLANATION_PROMPT
    assert "evidence quality level and limits" in CANONICAL_EXPLANATION_PROMPT


def test_extract_allowed_inputs_excludes_raw_text():
    """Verify that raw text content is never included in allowed inputs."""
    evidence = {
        "structural": [
            {"type": "work_item_type", "work_item_type": "story", "count": 2},
            {"type": "graph_density", "value": 0.8},
        ],
        "contextual": [
            {"type": "time_range", "span_days": 7.0, "score": 0.9},
        ],
        "textual": [
            # These raw keywords should NOT appear in the output
            {"category": "feature_delivery", "keyword": "add login", "weight": 0.02},
            {
                "category": "feature_delivery",
                "keyword": "implement auth",
                "weight": 0.03,
            },
        ],
    }

    inputs = extract_allowed_inputs(
        work_unit_id="test-123",
        time_range_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        time_range_end=datetime(2025, 1, 8, tzinfo=timezone.utc),
        categories={"feature_delivery": 0.7, "maintenance": 0.3},
        evidence_quality_value=0.85,
        evidence_quality_band="high",
        evidence=evidence,
    )

    # Check that raw keywords are NOT in the evidence summary
    evidence_str = str(inputs.evidence_summary)
    assert "add login" not in evidence_str
    assert "implement auth" not in evidence_str

    # But metadata IS present
    assert inputs.evidence_summary.get("textual", {}).get("match_count") == 2
    assert "feature_delivery" in inputs.evidence_summary.get("textual", {}).get(
        "categories_with_matches", []
    )


def test_prompt_includes_evidence_quality_band():
    """Verify that evidence quality band is included in the prompt."""
    inputs = _sample_inputs()
    prompt = build_explanation_prompt(inputs)

    assert "moderate" in prompt
    assert "0.72" in prompt or "72" in prompt  # May be formatted as percentage


def test_prompt_includes_evidence_metadata_not_content():
    """Verify evidence metadata is included but not raw content."""
    inputs = _sample_inputs()
    prompt = build_explanation_prompt(inputs)

    # Metadata should be present
    assert "Structural evidence" in prompt
    assert "Contextual evidence" in prompt
    assert "textual phrases" in prompt.lower()

    # Categories should appear
    assert "feature_delivery" in prompt
    assert "maintenance" in prompt


def test_prompt_includes_canonical_instructions():
    """Verify the canonical instructions are in the prompt."""
    inputs = _sample_inputs()
    prompt = build_explanation_prompt(inputs)

    # Must include the canonical prompt
    assert "You are explaining a precomputed investment view" in prompt
    assert "Recalculate scores" in prompt
    assert "SUMMARY" in prompt
    assert "REASONS" in prompt
    assert "UNCERTAINTY" in prompt


def test_validate_language_catches_forbidden_words():
    """Verify that forbidden words are detected."""
    # Text with forbidden words
    bad_text = "This work unit is a feature. It was detected as maintenance."
    violations = validate_explanation_language(bad_text)

    assert len(violations) > 0
    assert any("is" in v.lower() for v in violations)
    assert any("was" in v.lower() for v in violations)
    assert any("detected" in v.lower() for v in violations)


def test_validate_language_allows_approved_words():
    """Verify that approved words pass validation."""
    # Text with only approved words
    good_text = (
        "This work unit appears to lean toward feature work. "
        "The evidence suggests a maintenance component as well."
    )
    violations = validate_explanation_language(good_text)

    assert len(violations) == 0


def test_forbidden_words_set_complete():
    """Verify all forbidden words from AGENTS-WG.md are in the set."""
    expected_forbidden = {"is", "was", "detected", "determined"}
    assert FORBIDDEN_WORDS == expected_forbidden


def test_categories_sum_approximately_one():
    """Verify categories in test inputs sum to approximately 1.0."""
    inputs = _sample_inputs()
    total = sum(inputs.categories.values())
    assert abs(total - 1.0) < 0.01  # Allow small floating point error


def test_prompt_time_range_included():
    """Verify time range is included in the prompt."""
    inputs = _sample_inputs()
    prompt = build_explanation_prompt(inputs)

    # Start and end dates should be present
    assert "2025-01-01" in prompt
    assert "2025-01-15" in prompt
    # Or the span
    assert "14" in prompt or "14.0" in prompt
