"""Utility helpers for investment materialization."""

from __future__ import annotations

import hashlib
from typing import Dict, Iterable, Tuple

from dev_health_ops.work_graph.investment.taxonomy import (
    SUBCATEGORIES,
    THEMES,
    theme_of,
)


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def work_unit_id(nodes: Iterable[Tuple[str, str]]) -> str:
    tokens = sorted(f"{node_type}:{node_id}" for node_type, node_id in nodes)
    return _sha256_hex("|".join(tokens))


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def normalize_scores(scores: Dict[str, float], keys: Iterable[str]) -> Dict[str, float]:
    key_list = list(keys)
    total = sum(float(scores.get(key, 0.0) or 0.0) for key in key_list)
    if total <= 0.0:
        uniform = 1.0 / len(key_list) if key_list else 0.0
        return {key: uniform for key in key_list}
    return {key: float(scores.get(key, 0.0) or 0.0) / total for key in key_list}


def rollup_subcategories_to_themes(
    subcategories: Dict[str, float],
) -> Dict[str, float]:
    totals = {theme: 0.0 for theme in THEMES}
    for subcategory, value in subcategories.items():
        theme = theme_of(subcategory)
        if not theme:
            continue
        totals[theme] += float(value)
    return normalize_scores(totals, sorted(THEMES))


def ensure_full_subcategory_vector(
    subcategories: Dict[str, float],
) -> Dict[str, float]:
    normalized = normalize_scores(subcategories, sorted(SUBCATEGORIES))
    return {key: float(normalized.get(key, 0.0)) for key in sorted(SUBCATEGORIES)}


def evidence_quality_band(value: float) -> str:
    if value >= 0.8:
        return "high"
    if value >= 0.6:
        return "moderate"
    if value >= 0.4:
        return "low"
    return "very_low"
