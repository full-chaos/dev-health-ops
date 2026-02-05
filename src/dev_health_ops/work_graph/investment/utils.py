"""Utility helpers for investment materialization."""

from __future__ import annotations

from typing import Dict

from dev_health_ops.utils.normalization import (
    clamp,
    ensure_full_subcategory_vector as _ensure_full_subcategory_vector,
    evidence_quality_band,
    rollup_subcategories_to_themes as _rollup_subcategories_to_themes,
    work_unit_id,
)
from dev_health_ops.work_graph.investment.taxonomy import (
    SUBCATEGORIES,
    SUBCATEGORY_TO_THEME,
    THEMES,
)

__all__ = [
    "clamp",
    "ensure_full_subcategory_vector",
    "evidence_quality_band",
    "rollup_subcategories_to_themes",
    "work_unit_id",
]


def rollup_subcategories_to_themes(
    subcategories: Dict[str, float],
) -> Dict[str, float]:
    return _rollup_subcategories_to_themes(
        subcategories, SUBCATEGORY_TO_THEME, sorted(THEMES)
    )


def ensure_full_subcategory_vector(
    subcategories: Dict[str, float],
) -> Dict[str, float]:
    return _ensure_full_subcategory_vector(subcategories, SUBCATEGORIES)
