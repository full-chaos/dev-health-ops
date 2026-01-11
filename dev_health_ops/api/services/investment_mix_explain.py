from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from analytics.investment_mix_explainer import build_prompt, load_prompt, parse_and_validate_response
from investment_taxonomy import SUBCATEGORIES, THEMES, theme_of

from ..models.filters import MetricFilter
from ..models.schemas import InvestmentMixExplanation
from .investment import build_investment_response
from .llm_providers import get_provider
from .work_units import build_work_unit_investments

logger = logging.getLogger(__name__)


def _top_items(distribution: Dict[str, float], limit: int) -> List[Tuple[str, float]]:
    return sorted(
        [(k, float(v or 0.0)) for k, v in distribution.items() if float(v or 0.0) > 0.0],
        key=lambda item: item[1],
        reverse=True,
    )[: max(1, int(limit))]


def _dominant_subcategory(subcategories: Dict[str, float]) -> Optional[str]:
    best_key: Optional[str] = None
    best_value = 0.0
    for key, value in subcategories.items():
        v = float(value or 0.0)
        if v > best_value:
            best_value = v
            best_key = key
    return best_key


async def explain_investment_mix(
    *,
    db_url: str,
    filters: MetricFilter,
    theme: Optional[str] = None,
    subcategory: Optional[str] = None,
    llm_provider: str = "auto",
) -> InvestmentMixExplanation:
    if theme and theme not in THEMES:
        raise ValueError("Unknown theme")
    if subcategory and subcategory not in SUBCATEGORIES:
        raise ValueError("Unknown subcategory")
    if theme and subcategory and theme_of(subcategory) != theme:
        raise ValueError("Theme/subcategory mismatch")

    investment = await build_investment_response(db_url=db_url, filters=filters)
    theme_distribution = investment.theme_distribution
    subcategory_distribution = investment.subcategory_distribution

    if theme:
        subcategory_distribution = {
            key: value
            for key, value in subcategory_distribution.items()
            if key.startswith(f"{theme}.")
        }
    if subcategory:
        subcategory_distribution = {
            key: value for key, value in subcategory_distribution.items() if key == subcategory
        }

    units = await build_work_unit_investments(
        db_url=db_url,
        filters=filters,
        limit=200,
        include_text=True,
    )

    if theme:
        units = [
            unit for unit in units
            if float((unit.investment.themes or {}).get(theme, 0.0)) > 0.0
        ]
    if subcategory:
        units = [
            unit for unit in units
            if float((unit.investment.subcategories or {}).get(subcategory, 0.0)) > 0.0
        ]

    band_counts: Dict[str, int] = {}
    dominant_counts: Dict[str, int] = {}
    quotes_by_subcategory: Dict[str, List[str]] = {}

    for unit in units:
        band = str(unit.evidence_quality.band or "very_low")
        band_counts[band] = band_counts.get(band, 0) + 1

        dominant = _dominant_subcategory(unit.investment.subcategories or {})
        if dominant:
            dominant_counts[dominant] = dominant_counts.get(dominant, 0) + 1

        for entry in unit.evidence.textual or []:
            if not isinstance(entry, dict):
                continue
            quote = entry.get("quote")
            if not isinstance(quote, str) or not quote.strip():
                continue
            quotes_by_subcategory.setdefault(dominant or "unassigned", []).append(quote.strip())

    top_themes = _top_items(theme_distribution, 8)
    top_subcategories = _top_items(subcategory_distribution, 12)
    top_counts = _top_items({k: float(v) for k, v in dominant_counts.items()}, 10)

    sample_quotes: List[Dict[str, Any]] = []
    for subcat, _count in top_counts[:6]:
        quotes = quotes_by_subcategory.get(subcat, [])[:3]
        if quotes:
            sample_quotes.append({"subcategory": subcat, "quotes": quotes})

    total_effort = sum(float(v or 0.0) for v in theme_distribution.values())
    total_units = len(units)

    payload: Dict[str, Any] = {
        "focus": {"theme": theme, "subcategory": subcategory},
        "total_effort": total_effort,
        "theme_distribution_top": [
            {"theme": key, "value": value, "pct": (value / total_effort) if total_effort else 0.0}
            for key, value in top_themes
        ],
        "subcategory_distribution_top": [
            {"subcategory": key, "value": value, "pct": (value / total_effort) if total_effort else 0.0}
            for key, value in top_subcategories
        ],
        "work_unit_count": total_units,
        "work_unit_dominant_subcategory_counts_top": [
            {"subcategory": key, "count": int(value)} for key, value in top_counts
        ],
        "evidence_quality_band_counts": band_counts,
        "evidence_quote_samples": sample_quotes,
    }

    prompt_text = load_prompt()
    full_prompt = build_prompt(base_prompt=prompt_text, payload=payload)

    provider = get_provider(llm_provider)
    raw = await provider.complete(full_prompt)
    parsed = parse_and_validate_response(raw)
    if not parsed:
        logger.warning("Investment mix explanation parse/validation failed")
        return InvestmentMixExplanation(
            summary="This mix suggests effort leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.",
            dominant_themes=[key for key, _ in top_themes[:3]],
            key_drivers=["The distribution appears concentrated in the largest segments shown in the chart."],
            operational_signals=["Evidence quality bands indicate uncertainty varies across contributing work units."],
            confidence_note="AI-generated interpretation based on the data shown above; confidence appears bounded by the evidence quality mix.",
        )

    return InvestmentMixExplanation(**parsed)

