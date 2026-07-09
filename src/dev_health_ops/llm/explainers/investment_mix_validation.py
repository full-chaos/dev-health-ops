"""Field validation for untrusted investment-mix explanation JSON."""

from __future__ import annotations

import math
import re
from typing import Any

from .investment_mix_types import ActionItem, Finding

BANDS = frozenset({"high", "moderate", "low", "very_low", "unknown"})
EVIDENCE_KEYS = {
    "theme",
    "subcategory",
    "share_pct",
    "delta_pct_points",
    "evidence_quality_mean",
    "evidence_quality_band",
}
TOP_LEVEL_KEYS = {
    "summary",
    "top_findings",
    "confidence",
    "what_to_check_next",
    "anti_claims",
}
FORBIDDEN_PATTERN = re.compile(
    r"\b(?:is|was|should|determined|detected|definitely|certainly|undoubtedly)\b|without\s+question",
    re.IGNORECASE,
)
NUMERIC_PATTERN = re.compile(r"\d")


def string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def contains_forbidden_language(text: str) -> bool:
    return FORBIDDEN_PATTERN.search(text) is not None


def finite_number(value: Any, *, minimum: float, maximum: float | None = None) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        numeric = float(value)
    except OverflowError:
        return False
    return (
        math.isfinite(numeric)
        and numeric >= minimum
        and (maximum is None or numeric <= maximum)
    )


def parse_finding(
    raw: Any,
    *,
    theme_shares_pct: dict[str, float],
    subcategory_shares_pct: dict[str, float],
    quality_mean: float | None,
    quality_band: str | None,
) -> Finding | None:
    if not isinstance(raw, dict) or set(raw) != {"finding", "evidence"}:
        return None
    finding = raw.get("finding")
    evidence = raw.get("evidence")
    if (
        not isinstance(finding, str)
        or not finding.strip()
        or len(finding) > 500
        or NUMERIC_PATTERN.search(finding)
    ):
        return None
    if not isinstance(evidence, dict) or set(evidence) != EVIDENCE_KEYS:
        return None
    theme = evidence.get("theme")
    subcategory = evidence.get("subcategory")
    if not isinstance(theme, str) or theme not in theme_shares_pct:
        return None
    if subcategory is not None and (
        not isinstance(subcategory, str)
        or subcategory not in subcategory_shares_pct
        or subcategory.split(".", 1)[0] != theme
    ):
        return None
    if not finite_number(evidence.get("share_pct"), minimum=0, maximum=100):
        return None
    delta = evidence.get("delta_pct_points")
    if delta is not None and not finite_number(delta, minimum=-100, maximum=100):
        return None
    raw_quality = evidence.get("evidence_quality_mean")
    if raw_quality is not None and not finite_number(raw_quality, minimum=0, maximum=1):
        return None
    raw_band = evidence.get("evidence_quality_band")
    if raw_band is not None and raw_band not in BANDS:
        return None
    share_pct = (
        subcategory_shares_pct[subcategory]
        if isinstance(subcategory, str)
        else theme_shares_pct[theme]
    )
    return {
        "finding": f"{finding.strip().rstrip('.')} (~{share_pct:.0f}% of effort).",
        "evidence": {
            "theme": theme,
            "subcategory": subcategory,
            "share_pct": share_pct,
            "delta_pct_points": None,
            "evidence_quality_mean": quality_mean,
            "evidence_quality_band": quality_band,
        },
    }


def valid_confidence(raw: Any) -> bool:
    expected = {"level", "quality_mean", "quality_stddev", "band_mix", "drivers"}
    if not isinstance(raw, dict) or set(raw) != expected:
        return False
    if raw.get("level") not in BANDS - {"very_low"}:
        return False
    mean = raw.get("quality_mean")
    stddev = raw.get("quality_stddev")
    if mean is not None and not finite_number(mean, minimum=0, maximum=1):
        return False
    if stddev is not None and not finite_number(stddev, minimum=0, maximum=1):
        return False
    band_mix = raw.get("band_mix")
    if not isinstance(band_mix, dict) or set(band_mix) != BANDS:
        return False
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in band_mix.values()
    ):
        return False
    drivers = raw.get("drivers")
    return (
        isinstance(drivers, list)
        and len(drivers) <= 10
        and all(
            isinstance(driver, str) and bool(driver.strip()) and len(driver) <= 120
            for driver in drivers
        )
    )


def parse_action(raw: Any) -> ActionItem | None:
    if not isinstance(raw, dict) or set(raw) != {"action", "why", "where"}:
        return None
    action = raw.get("action")
    why = raw.get("why")
    where = raw.get("where")
    if not isinstance(action, str) or not action.strip():
        return None
    if not isinstance(why, str) or not why.strip():
        return None
    if not isinstance(where, str) or not where.strip():
        return None
    if len(action) > 200 or len(why) > 300 or len(where) > 200:
        return None
    if any(NUMERIC_PATTERN.search(value) for value in (action, why, where)):
        return None
    return {"action": action.strip(), "why": why.strip(), "where": where.strip()}
