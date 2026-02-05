from __future__ import annotations

import hashlib
from typing import Dict, Iterable, Tuple


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def normalize_scores(scores: Dict[str, float], keys: Iterable[str]) -> Dict[str, float]:
    key_list = list(keys)
    total = sum(float(scores.get(key, 0.0) or 0.0) for key in key_list)
    if total <= 0.0:
        uniform = 1.0 / len(key_list) if key_list else 0.0
        return {key: uniform for key in key_list}
    return {key: float(scores.get(key, 0.0) or 0.0) / total for key in key_list}


def evidence_quality_band(value: float) -> str:
    if value >= 0.8:
        return "high"
    if value >= 0.6:
        return "moderate"
    if value >= 0.4:
        return "low"
    return "very_low"


def work_unit_id(nodes: Iterable[Tuple[str, str]]) -> str:
    """Generate a stable ID for a work unit from its constituent nodes."""
    tokens = sorted(f"{node_type}:{node_id}" for node_type, node_id in nodes)
    return _sha256_hex("|".join(tokens))


def rollup_subcategories_to_themes(
    subcategories: Dict[str, float],
    subcategory_to_theme: Dict[str, str],
    themes: Iterable[str],
) -> Dict[str, float]:
    """Roll up subcategory probabilities to theme probabilities (deterministic)."""
    theme_list = list(themes)
    totals = {theme: 0.0 for theme in theme_list}
    for subcategory, value in subcategories.items():
        theme = subcategory_to_theme.get(subcategory)
        if theme and theme in totals:
            totals[theme] += float(value)
    return normalize_scores(totals, theme_list)


def ensure_full_subcategory_vector(
    subcategories: Dict[str, float],
    all_subcategories: Iterable[str],
) -> Dict[str, float]:
    """Ensure a subcategory vector has entries for all known subcategories."""
    subcat_list = sorted(all_subcategories)
    normalized = normalize_scores(subcategories, subcat_list)
    return {key: float(normalized.get(key, 0.0)) for key in subcat_list}
