from __future__ import annotations

import pytest

from dev_health_ops.utils.normalization import (
    clamp,
    ensure_full_subcategory_vector,
    evidence_quality_band,
    normalize_scores,
    rollup_subcategories_to_themes,
    work_unit_id,
)


class TestClamp:
    def test_clamp_within_bounds(self):
        assert clamp(0.5) == 0.5

    def test_clamp_below_low(self):
        assert clamp(-0.5) == 0.0

    def test_clamp_above_high(self):
        assert clamp(1.5) == 1.0

    def test_clamp_custom_bounds(self):
        assert clamp(5.0, low=1.0, high=10.0) == 5.0
        assert clamp(0.5, low=1.0, high=10.0) == 1.0
        assert clamp(15.0, low=1.0, high=10.0) == 10.0


class TestNormalizeScores:
    def test_normalize_positive_scores(self):
        result = normalize_scores({"a": 2.0, "b": 3.0}, ["a", "b"])
        assert abs(result["a"] - 0.4) < 0.001
        assert abs(result["b"] - 0.6) < 0.001

    def test_normalize_zero_total(self):
        result = normalize_scores({"a": 0.0, "b": 0.0}, ["a", "b"])
        assert result["a"] == 0.5
        assert result["b"] == 0.5

    def test_normalize_missing_keys(self):
        result = normalize_scores({"a": 1.0}, ["a", "b", "c"])
        assert abs(sum(result.values()) - 1.0) < 0.001


class TestEvidenceQualityBand:
    def test_high_band(self):
        assert evidence_quality_band(0.9) == "high"
        assert evidence_quality_band(0.8) == "high"

    def test_moderate_band(self):
        assert evidence_quality_band(0.7) == "moderate"
        assert evidence_quality_band(0.6) == "moderate"

    def test_low_band(self):
        assert evidence_quality_band(0.5) == "low"
        assert evidence_quality_band(0.4) == "low"

    def test_very_low_band(self):
        assert evidence_quality_band(0.3) == "very_low"
        assert evidence_quality_band(0.0) == "very_low"


class TestWorkUnitId:
    def test_deterministic_id(self):
        nodes = [("pr", "123"), ("commit", "abc")]
        id1 = work_unit_id(nodes)
        id2 = work_unit_id(nodes)
        assert id1 == id2

    def test_order_independent(self):
        nodes1 = [("pr", "123"), ("commit", "abc")]
        nodes2 = [("commit", "abc"), ("pr", "123")]
        assert work_unit_id(nodes1) == work_unit_id(nodes2)

    def test_different_nodes_different_id(self):
        nodes1 = [("pr", "123")]
        nodes2 = [("pr", "456")]
        assert work_unit_id(nodes1) != work_unit_id(nodes2)

    def test_sha256_hex_length(self):
        nodes = [("pr", "123")]
        result = work_unit_id(nodes)
        assert len(result) == 64


class TestRollupSubcategoriesToThemes:
    def test_basic_rollup(self):
        subcategories = {
            "feature_delivery.customer": 0.5,
            "feature_delivery.roadmap": 0.5,
        }
        subcategory_to_theme = {
            "feature_delivery.customer": "feature_delivery",
            "feature_delivery.roadmap": "feature_delivery",
            "maintenance.debt": "maintenance",
        }
        themes = ["feature_delivery", "maintenance"]
        result = rollup_subcategories_to_themes(
            subcategories, subcategory_to_theme, themes
        )
        assert abs(result["feature_delivery"] - 1.0) < 0.001
        assert result["maintenance"] == 0.0

    def test_mixed_themes(self):
        subcategories = {
            "feature_delivery.customer": 0.6,
            "maintenance.debt": 0.4,
        }
        subcategory_to_theme = {
            "feature_delivery.customer": "feature_delivery",
            "maintenance.debt": "maintenance",
        }
        themes = ["feature_delivery", "maintenance"]
        result = rollup_subcategories_to_themes(
            subcategories, subcategory_to_theme, themes
        )
        assert abs(result["feature_delivery"] - 0.6) < 0.001
        assert abs(result["maintenance"] - 0.4) < 0.001

    def test_unknown_subcategory_ignored(self):
        subcategories = {"unknown.sub": 1.0}
        subcategory_to_theme = {"feature_delivery.customer": "feature_delivery"}
        themes = ["feature_delivery"]
        result = rollup_subcategories_to_themes(
            subcategories, subcategory_to_theme, themes
        )
        assert result["feature_delivery"] == 1.0


class TestEnsureFullSubcategoryVector:
    def test_fills_missing_keys(self):
        subcategories = {"a": 0.5, "b": 0.5}
        all_subcategories = ["a", "b", "c"]
        result = ensure_full_subcategory_vector(subcategories, all_subcategories)
        assert "c" in result
        assert abs(sum(result.values()) - 1.0) < 0.001

    def test_preserves_existing_ratios(self):
        subcategories = {"a": 0.6, "b": 0.4}
        all_subcategories = ["a", "b"]
        result = ensure_full_subcategory_vector(subcategories, all_subcategories)
        assert abs(result["a"] - 0.6) < 0.001
        assert abs(result["b"] - 0.4) < 0.001

    def test_empty_input(self):
        subcategories = {}
        all_subcategories = ["a", "b"]
        result = ensure_full_subcategory_vector(subcategories, all_subcategories)
        assert result["a"] == 0.5
        assert result["b"] == 0.5
