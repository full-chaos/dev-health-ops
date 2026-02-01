from __future__ import annotations

from typing import Dict, Iterable


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
