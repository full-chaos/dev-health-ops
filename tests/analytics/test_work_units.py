from __future__ import annotations

from dev_health_ops.analytics.work_units import (
    compute_evidence_quality,
    compute_subcategory_scores,
    evidence_quality_band,
    merge_subcategory_vectors,
)


def test_subcategory_scores_normalize():
    scores, evidence = compute_subcategory_scores({"story": 1, "bug": 2})
    total = sum(scores.values())
    assert abs(total - 1.0) < 0.01
    assert any(item.get("type") == "subcategory_scores" for item in evidence)


def test_merge_subcategory_vectors_prefers_primary():
    primary = {
        "feature_delivery.customer": 0.8,
        "quality.bugfix": 0.2,
    }
    secondary = {
        "maintenance.refactor": 1.0,
    }
    merged = merge_subcategory_vectors(
        primary=primary,
        secondary=secondary,
        primary_weight=0.7,
    )
    assert merged["feature_delivery.customer"] > merged["maintenance.refactor"]
    total = sum(merged.values())
    assert abs(total - 1.0) < 0.01


def test_evidence_quality_band():
    value = compute_evidence_quality(
        text_source_count=3,
        metadata_present=True,
        density_score=0.9,
        provenance_score=0.9,
        temporal_score=0.8,
    )
    assert evidence_quality_band(value) in {"high", "moderate", "low", "very_low"}
