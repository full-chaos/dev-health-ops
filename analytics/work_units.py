from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

logger = logging.getLogger(__name__)

DEFAULT_WORK_UNIT_CONFIG_PATH = Path("config/work_units.yaml")


@dataclass(frozen=True)
class WorkUnitConfig:
    categories: List[str]
    work_item_type_weights: Dict[str, Dict[str, float]]
    text_keywords: Dict[str, List[Dict[str, float]]]
    text_source_weights: Dict[str, float]
    text_max_modifier: float
    confidence_weights: Dict[str, float]
    temporal_window_days: float
    temporal_fallback: float
    text_agreement_fallback: float


def default_work_unit_config() -> WorkUnitConfig:
    categories = ["feature", "maintenance", "operational", "quality"]
    return WorkUnitConfig(
        categories=categories,
        work_item_type_weights={
            "story": {"feature": 1.0},
            "epic": {"feature": 1.0},
            "task": {"feature": 0.7, "maintenance": 0.3},
            "issue": {"feature": 0.5, "maintenance": 0.5},
            "chore": {"maintenance": 1.0},
            "bug": {"quality": 1.0},
            "incident": {"operational": 1.0},
            "unknown": {cat: 1.0 / len(categories) for cat in categories},
        },
        text_keywords={},
        text_source_weights={},
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


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_keywords(
    payload: Dict[str, List[Dict[str, float]]],
) -> Dict[str, List[Dict[str, float]]]:
    keywords: Dict[str, List[Dict[str, float]]] = {}
    for category, entries in (payload or {}).items():
        cleaned: List[Dict[str, float]] = []
        for entry in entries or []:
            if isinstance(entry, str):
                cleaned.append({"keyword": entry, "weight": 0.02})
                continue
            keyword = str(entry.get("keyword") or "").strip()
            if not keyword:
                continue
            try:
                weight = float(entry.get("weight", 0.0))
            except (TypeError, ValueError):
                weight = 0.0
            cleaned.append({"keyword": keyword, "weight": weight})
        if cleaned:
            keywords[str(category)] = cleaned
    return keywords


def load_work_unit_config(path: Optional[Path] = None) -> WorkUnitConfig:
    env_path = os.getenv("WORK_UNIT_CONFIG_PATH")
    if env_path:
        path = Path(env_path)
    path = path or DEFAULT_WORK_UNIT_CONFIG_PATH
    if not path.exists():
        logger.warning("Work unit config not found at %s, using defaults", path)
        return default_work_unit_config()
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    categories = [str(c) for c in (payload.get("categories") or []) if str(c).strip()]
    if not categories:
        categories = default_work_unit_config().categories

    structural = payload.get("structural") or {}
    work_item_type_weights = structural.get("work_item_type_weights") or {}

    textual = payload.get("textual") or {}
    text_max_modifier = float(textual.get("max_modifier", 0.15))
    text_source_weights = {
        str(k): float(v) for k, v in (textual.get("source_weights") or {}).items()
    }
    text_keywords = _normalize_keywords(textual.get("keywords") or {})

    confidence = payload.get("confidence") or {}
    confidence_weights = {
        str(k): float(v) for k, v in (confidence.get("weights") or {}).items()
    }

    return WorkUnitConfig(
        categories=categories,
        work_item_type_weights=work_item_type_weights,
        text_keywords=text_keywords,
        text_source_weights=text_source_weights,
        text_max_modifier=text_max_modifier,
        confidence_weights=confidence_weights,
        temporal_window_days=float(confidence.get("temporal_window_days", 30.0)),
        temporal_fallback=float(confidence.get("temporal_fallback", 0.5)),
        text_agreement_fallback=float(confidence.get("text_agreement_fallback", 0.5)),
    )


def normalize_scores(
    scores: Dict[str, float], categories: Sequence[str]
) -> Dict[str, float]:
    total = sum(scores.get(cat, 0.0) for cat in categories)
    if total <= 0:
        uniform = 1.0 / len(categories) if categories else 0.0
        return {cat: uniform for cat in categories}
    return {cat: scores.get(cat, 0.0) / total for cat in categories}


def compute_structural_scores(
    type_counts: Dict[str, int],
    config: WorkUnitConfig,
) -> Tuple[Dict[str, float], List[Dict[str, object]]]:
    scores = {cat: 0.0 for cat in config.categories}
    evidence: List[Dict[str, object]] = []

    for work_type, count in type_counts.items():
        if count <= 0:
            continue
        weights = config.work_item_type_weights.get(
            work_type, config.work_item_type_weights.get("unknown", {})
        )
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
                "work_item_type": work_type,
                "count": int(count),
                "weights": weights,
                "contribution": contribution,
            }
        )

    normalized = normalize_scores(scores, config.categories)
    evidence.append({"type": "structural_scores", "scores": normalized})
    return normalized, evidence


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def compute_textual_modifiers(
    texts_by_source: Dict[str, Sequence[str]],
    config: WorkUnitConfig,
) -> Tuple[Dict[str, float], List[Dict[str, object]]]:
    modifiers = {cat: 0.0 for cat in config.categories}
    evidence: List[Dict[str, object]] = []

    for source, texts in (texts_by_source or {}).items():
        source_weight = float(config.text_source_weights.get(source, 1.0))
        for text in texts or []:
            haystack = str(text or "").lower()
            if not haystack:
                continue
            for category, entries in (config.text_keywords or {}).items():
                if category not in modifiers:
                    continue
                for entry in entries or []:
                    keyword = str(entry.get("keyword") or "").strip()
                    if not keyword:
                        continue
                    if keyword.lower() not in haystack:
                        continue
                    weight = float(entry.get("weight", 0.0))
                    magnitude = weight * source_weight
                    modifiers[category] += magnitude
                    evidence.append(
                        {
                            "category": category,
                            "keyword": keyword,
                            "source": source,
                            "weight": weight,
                            "magnitude": magnitude,
                        }
                    )

    for category, value in list(modifiers.items()):
        clamped = _clamp(value, -config.text_max_modifier, config.text_max_modifier)
        if clamped != value:
            evidence.append(
                {
                    "category": category,
                    "reason": "clamped",
                    "raw": value,
                    "clamped": clamped,
                }
            )
            modifiers[category] = clamped

    return modifiers, evidence


def apply_textual_modifiers(
    structural_scores: Dict[str, float],
    modifiers: Dict[str, float],
    categories: Sequence[str],
) -> Dict[str, float]:
    combined = {}
    for category in categories:
        combined[category] = _clamp(
            structural_scores.get(category, 0.0) + modifiers.get(category, 0.0),
            0.0,
            1.0,
        )
    return normalize_scores(combined, categories)


def compute_text_agreement(
    structural_scores: Dict[str, float],
    modifiers: Dict[str, float],
    config: WorkUnitConfig,
) -> float:
    total_abs = sum(abs(modifiers.get(cat, 0.0)) for cat in config.categories)
    if total_abs <= 0:
        return config.text_agreement_fallback
    alignment = 0.0
    for category in config.categories:
        score = structural_scores.get(category, 0.0)
        mod = modifiers.get(category, 0.0)
        if mod >= 0:
            alignment += mod * score
        else:
            alignment += (-mod) * (1.0 - score)
    return _clamp(alignment / total_abs, 0.0, 1.0)


def compute_confidence(
    *,
    provenance_score: float,
    temporal_score: float,
    density_score: float,
    text_agreement: float,
    config: WorkUnitConfig,
) -> float:
    weights = config.confidence_weights or {}
    total_weight = sum(float(v) for v in weights.values())
    if total_weight <= 0:
        total_weight = 1.0
    value = (
        float(weights.get("provenance", 0.0)) * provenance_score
        + float(weights.get("temporal", 0.0)) * temporal_score
        + float(weights.get("density", 0.0)) * density_score
        + float(weights.get("text_agreement", 0.0)) * text_agreement
    )
    return _clamp(value / total_weight, 0.0, 1.0)


def confidence_band(value: float) -> str:
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
