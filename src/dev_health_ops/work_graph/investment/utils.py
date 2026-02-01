"""Utility helpers for investment materialization."""

from __future__ import annotations

import hashlib
from typing import Dict, Iterable, Tuple

from dev_health_ops.utils.normalization import (
    clamp,
    evidence_quality_band,
    normalize_scores,
)
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
