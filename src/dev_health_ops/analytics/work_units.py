from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from dev_health_ops.investment_taxonomy import SUBCATEGORIES, THEMES, theme_of

logger = logging.getLogger(__name__)

CANONICAL_INVESTMENT_THEMES: Tuple[str, ...] = tuple(sorted(THEMES))
CANONICAL_SUBCATEGORIES: Tuple[str, ...] = tuple(sorted(SUBCATEGORIES))

SUBCATEGORY_TO_THEME: Dict[str, str] = {
    subcategory: theme_of(subcategory) for subcategory in CANONICAL_SUBCATEGORIES
}

WORK_ITEM_TYPE_WEIGHTS: Dict[str, Dict[str, float]] = {
    "story": {"feature_delivery.roadmap": 1.0},
    "epic": {"feature_delivery.roadmap": 1.0},
    "feature": {"feature_delivery.customer": 1.0},
    "enhancement": {
        "feature_delivery.customer": 0.7,
        "feature_delivery.enablement": 0.3,
    },
    "task": {"feature_delivery.enablement": 0.6, "maintenance.debt": 0.4},
    "issue": {
        "feature_delivery.customer": 0.3,
        "maintenance.debt": 0.3,
        "quality.bugfix": 0.4,
    },
    "chore": {"maintenance.debt": 1.0},
    "refactor": {"maintenance.refactor": 0.8, "quality.reliability": 0.2},
    "bug": {"quality.bugfix": 0.8, "maintenance.debt": 0.2},
    "defect": {"quality.bugfix": 0.8, "maintenance.debt": 0.2},
    "incident": {"operational.incident_response": 1.0},
    "outage": {"operational.incident_response": 1.0},
    "support": {"operational.support": 1.0},
    "oncall": {"operational.on_call": 1.0},
    "reliability": {"quality.reliability": 0.7, "operational.on_call": 0.3},
    "security": {"risk.security": 1.0},
    "vulnerability": {"risk.vulnerability": 1.0},
    "compliance": {"risk.compliance": 1.0},
}

DEFAULT_TEXT_WEIGHT = 0.72
DEFAULT_METADATA_WEIGHT = 0.28
TEMPORAL_WINDOW_DAYS = 30.0
TEMPORAL_FALLBACK = 0.5

EVIDENCE_QUALITY_WEIGHTS = {
    "text_coverage": 0.45,
    "metadata_coverage": 0.2,
    "contextual_strength": 0.35,
}


@dataclass(frozen=True)
class WorkUnitConfig:
    text_weight: float = DEFAULT_TEXT_WEIGHT
    metadata_weight: float = DEFAULT_METADATA_WEIGHT
    temporal_window_days: float = TEMPORAL_WINDOW_DAYS
    temporal_fallback: float = TEMPORAL_FALLBACK


_CONFIG: Optional[WorkUnitConfig] = None


def _config() -> WorkUnitConfig:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = WorkUnitConfig()
    return _CONFIG


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def normalize_scores(
    scores: Dict[str, float], categories: Sequence[str]
) -> Dict[str, float]:
    total = sum(scores.get(cat, 0.0) for cat in categories)
    if total <= 0:
        uniform = 1.0 / len(categories) if categories else 0.0
        return {cat: uniform for cat in categories}
    return {cat: scores.get(cat, 0.0) / total for cat in categories}


def _normalize_work_item_type(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw else "unknown"


def compute_subcategory_scores(
    type_counts: Dict[str, int],
) -> Tuple[Dict[str, float], List[Dict[str, object]]]:
    scores = {cat: 0.0 for cat in CANONICAL_SUBCATEGORIES}
    evidence: List[Dict[str, object]] = []

    for work_type, count in type_counts.items():
        normalized_type = _normalize_work_item_type(work_type)
        weights = WORK_ITEM_TYPE_WEIGHTS.get(normalized_type)
        if not weights:
            weights = {
                cat: 1.0 / len(CANONICAL_SUBCATEGORIES)
                for cat in CANONICAL_SUBCATEGORIES
            }

        contribution: Dict[str, float] = {}
        for category, weight in (weights or {}).items():
            if category not in scores:
                continue
            weighted = float(weight) * float(count)
            scores[category] += weighted
            contribution[category] = weighted
        evidence.append(
            {
                "type": "work_item_type",
                "work_item_type": normalized_type,
                "count": int(count),
                "weights": weights,
                "contribution": contribution,
            }
        )

    normalized = normalize_scores(scores, CANONICAL_SUBCATEGORIES)
    evidence.append({"type": "subcategory_scores", "scores": normalized})
    return normalized, evidence


def merge_subcategory_vectors(
    *,
    primary: Optional[Dict[str, float]],
    secondary: Optional[Dict[str, float]],
    primary_weight: float,
) -> Dict[str, float]:
    if not primary and not secondary:
        return normalize_scores({}, CANONICAL_SUBCATEGORIES)
    if not secondary:
        return normalize_scores(primary or {}, CANONICAL_SUBCATEGORIES)
    if not primary:
        return normalize_scores(secondary or {}, CANONICAL_SUBCATEGORIES)

    weighted: Dict[str, float] = {}
    secondary_weight = 1.0 - primary_weight
    for category in CANONICAL_SUBCATEGORIES:
        weighted[category] = (
            float(primary.get(category, 0.0)) * primary_weight
            + float(secondary.get(category, 0.0)) * secondary_weight
        )

    return normalize_scores(weighted, CANONICAL_SUBCATEGORIES)


def rollup_subcategories_to_themes(
    subcategories: Dict[str, float],
) -> Dict[str, float]:
    totals = {theme: 0.0 for theme in CANONICAL_INVESTMENT_THEMES}
    for subcategory, value in subcategories.items():
        theme = SUBCATEGORY_TO_THEME.get(subcategory)
        if not theme:
            continue
        totals[theme] += float(value)
    return normalize_scores(totals, CANONICAL_INVESTMENT_THEMES)


def compute_evidence_quality(
    *,
    text_source_count: int,
    metadata_present: bool,
    density_score: float,
    provenance_score: float,
    temporal_score: float,
) -> float:
    text_coverage = _clamp(text_source_count / 3.0, 0.0, 1.0)
    metadata_coverage = 1.0 if metadata_present else 0.0
    contextual_strength = _clamp(
        (density_score + provenance_score + temporal_score) / 3.0
    )

    weights = EVIDENCE_QUALITY_WEIGHTS
    total_weight = sum(weights.values()) or 1.0
    value = (
        weights["text_coverage"] * text_coverage
        + weights["metadata_coverage"] * metadata_coverage
        + weights["contextual_strength"] * contextual_strength
    )
    return _clamp(value / total_weight)


def evidence_quality_band(value: float) -> str:
    if value >= 0.8:
        return "high"
    if value >= 0.6:
        return "moderate"
    if value >= 0.4:
        return "low"
    return "very_low"


def work_unit_id(nodes: Iterable[Tuple[str, str]]) -> str:
    tokens = sorted(f"{node_type}:{node_id}" for node_type, node_id in nodes)
    return _sha256_hex("|".join(tokens))
