from __future__ import annotations

from analytics.work_units import (
    WorkUnitConfig,
    compute_confidence,
    compute_structural_scores,
    compute_text_agreement,
    compute_textual_modifiers,
    confidence_band,
)


def _minimal_config() -> WorkUnitConfig:
    return WorkUnitConfig(
        categories=["feature", "quality"],
        work_item_type_weights={"story": {"feature": 1.0}, "bug": {"quality": 1.0}},
        text_keywords={"feature": [{"keyword": "add", "weight": 0.2}]},
        text_source_weights={"issue_title": 1.0},
        text_max_modifier=0.15,
        confidence_weights={
            "provenance": 0.4,
            "temporal": 0.2,
            "density": 0.2,
            "text_agreement": 0.2,
        },
        temporal_window_days=30.0,
        temporal_fallback=0.5,
        text_agreement_fallback=0.5,
    )


def test_structural_scores_normalize():
    config = _minimal_config()
    scores, evidence = compute_structural_scores({"story": 1, "bug": 2}, config)
    assert round(scores["feature"], 3) == 0.333
    assert round(scores["quality"], 3) == 0.667
    assert any(item.get("type") == "structural_scores" for item in evidence)


def test_textual_modifiers_clamp():
    config = _minimal_config()
    modifiers, evidence = compute_textual_modifiers(
        {"issue_title": ["add module"]}, config
    )
    assert modifiers["feature"] == 0.15
    assert any(item.get("reason") == "clamped" for item in evidence)


def test_text_agreement_and_confidence_band():
    config = _minimal_config()
    structural = {"feature": 0.8, "quality": 0.2}
    modifiers = {"feature": 0.1, "quality": -0.05}
    agreement = compute_text_agreement(structural, modifiers, config)
    confidence = compute_confidence(
        provenance_score=0.9,
        temporal_score=0.8,
        density_score=0.7,
        text_agreement=agreement,
        config=config,
    )
    assert 0.0 <= agreement <= 1.0
    assert confidence_band(confidence) in {"high", "moderate", "low", "very_low"}
