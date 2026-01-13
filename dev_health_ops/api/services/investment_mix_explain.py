from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from analytics.investment_mix_explainer import (
    build_prompt,
    load_prompt,
    parse_and_validate_response,
)
from investment_taxonomy import SUBCATEGORIES, THEMES, theme_of

from ..models.schemas import (
    InvestmentActionItem,
    InvestmentConfidence,
    InvestmentFinding,
    InvestmentFindingEvidence,
    InvestmentMixExplanation,
)
from .investment import build_investment_response
from .llm_providers import get_provider
from .work_units import build_work_unit_investments

logger = logging.getLogger(__name__)


def _top_items(distribution: Dict[str, float], limit: int) -> List[Tuple[str, float]]:
    return sorted(
        [
            (k, float(v or 0.0))
            for k, v in distribution.items()
            if float(v or 0.0) > 0.0
        ],
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


def _determine_confidence_level(
    quality_mean: Optional[float], quality_stddev: Optional[float]
) -> str:
    if quality_mean is None:
        return "unknown"
    if quality_mean >= 0.7 and (quality_stddev is None or quality_stddev < 0.15):
        return "high"
    if quality_mean >= 0.5:
        return "moderate"
    return "low"


async def explain_investment_mix(
    *,
    db_url: str,
    filters: Any,
    theme: Optional[str] = None,
    subcategory: Optional[str] = None,
    llm_provider: str = "auto",
    llm_model: Optional[str] = None,
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
            key: value
            for key, value in subcategory_distribution.items()
            if key == subcategory
        }

    units = await build_work_unit_investments(
        db_url=db_url,
        filters=filters,
        limit=200,
        include_text=True,
    )

    if theme:
        units = [
            unit
            for unit in units
            if float((unit.investment.themes or {}).get(theme, 0.0)) > 0.0
        ]
    if subcategory:
        units = [
            unit
            for unit in units
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
            quotes_by_subcategory.setdefault(dominant or "unassigned", []).append(
                quote.strip()
            )

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

    # Compute quality statistics from units
    quality_values = [
        float(u.evidence_quality.value)
        for u in units
        if u.evidence_quality.value is not None
    ]
    quality_mean = sum(quality_values) / len(quality_values) if quality_values else None
    quality_stddev = None
    if quality_values and len(quality_values) > 1:
        import math

        variance = sum((v - quality_mean) ** 2 for v in quality_values) / len(
            quality_values
        )
        quality_stddev = math.sqrt(variance)

    # Determine quality drivers based on band distribution
    quality_drivers: List[str] = []
    unknown_count = band_counts.get("unknown", 0)
    low_count = band_counts.get("low", 0) + band_counts.get("very_low", 0)
    total_band = sum(band_counts.values())
    if total_band > 0:
        if unknown_count / total_band > 0.3:
            quality_drivers.append("missing_evidence_metadata")
        if low_count / total_band > 0.5:
            quality_drivers.append("weak_cross_links")
    if quality_mean is not None and quality_mean < 0.4:
        quality_drivers.append("low_text_signal")
    if quality_stddev is not None and quality_stddev > 0.25:
        quality_drivers.append("high_uncertainty_spread")

    payload: Dict[str, Any] = {
        "focus": {"theme": theme, "subcategory": subcategory},
        "total_effort": total_effort,
        "theme_distribution_top": [
            {
                "theme": key,
                "value": value,
                "pct": (value / total_effort) if total_effort else 0.0,
            }
            for key, value in top_themes
        ],
        "subcategory_distribution_top": [
            {
                "subcategory": key,
                "value": value,
                "pct": (value / total_effort) if total_effort else 0.0,
            }
            for key, value in top_subcategories
        ],
        "work_unit_count": total_units,
        "work_unit_dominant_subcategory_counts_top": [
            {"subcategory": key, "count": int(value)} for key, value in top_counts
        ],
        "evidence_quality_band_counts": band_counts,
        "evidence_quality_mean": quality_mean,
        "evidence_quality_stddev": quality_stddev,
        "quality_drivers": quality_drivers,
        "evidence_quote_samples": sample_quotes,
    }

    prompt_text = load_prompt()
    full_prompt = build_prompt(base_prompt=prompt_text, payload=payload)

    provider = get_provider(llm_provider, model=llm_model)
    raw = await provider.complete(full_prompt)
    raw_len = len(raw) if raw is not None else 0
    if logger.isEnabledFor(logging.DEBUG):
        # Sanitize and truncate the LLM response before logging to avoid log injection.
        safe_preview = ""
        if raw:
            # Remove line breaks to keep the log entry on a single line.
            safe_preview = raw.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
            # Truncate to a reasonable length to avoid excessively large log entries.
            max_preview_len = 500
            if len(safe_preview) > max_preview_len:
                safe_preview = safe_preview[:max_preview_len] + "...[truncated]"
        logger.debug("Raw LLM response (%d chars, preview=%r)", raw_len, safe_preview)
    parsed = parse_and_validate_response(
        raw,
        fallback_band_mix=band_counts,
        fallback_drivers=quality_drivers,
        fallback_mean=quality_mean,
        fallback_stddev=quality_stddev,
    )

    if not parsed:
        logger.warning("Investment mix explanation parse/validation failed")
        # Build deterministic fallback
        confidence_level = _determine_confidence_level(quality_mean, quality_stddev)
        fallback_findings: List[InvestmentFinding] = []
        for key, value in top_themes[:2]:
            pct = (value / total_effort * 100) if total_effort else 0.0
            fallback_findings.append(
                InvestmentFinding(
                    finding=f"Effort appears concentrated in {key} (~{pct:.0f}% of total).",
                    evidence=InvestmentFindingEvidence(
                        theme=key,
                        subcategory=None,
                        share_pct=pct,
                        delta_pct_points=None,
                        evidence_quality_mean=quality_mean,
                        evidence_quality_band=confidence_level
                        if confidence_level != "unknown"
                        else None,
                    ),
                )
            )
        return InvestmentMixExplanation(
            summary="This mix suggests effort leans toward the leading themes shown, with subcategories providing the specific intent behind that allocation.",
            top_findings=fallback_findings,
            confidence=InvestmentConfidence(
                level=confidence_level,
                quality_mean=quality_mean,
                quality_stddev=quality_stddev,
                band_mix=band_counts,
                drivers=quality_drivers,
            ),
            what_to_check_next=[
                InvestmentActionItem(
                    action="Review the largest subcategories",
                    why="They drive the overall theme distribution",
                    where="Subcategory breakdown panel",
                )
            ],
            anti_claims=[
                "This does not measure individual productivity.",
                "This does not assign intent or correctness to any work.",
            ],
            status="invalid_llm_output",
        )

    return InvestmentMixExplanation(
        summary=parsed["summary"],
        top_findings=[
            InvestmentFinding(
                finding=f["finding"],
                evidence=InvestmentFindingEvidence(
                    theme=f["evidence"]["theme"],
                    subcategory=f["evidence"]["subcategory"],
                    share_pct=f["evidence"]["share_pct"],
                    delta_pct_points=f["evidence"]["delta_pct_points"],
                    evidence_quality_mean=f["evidence"]["evidence_quality_mean"],
                    evidence_quality_band=f["evidence"]["evidence_quality_band"],
                ),
            )
            for f in parsed["top_findings"]
        ],
        confidence=InvestmentConfidence(
            level=parsed["confidence"]["level"],
            quality_mean=parsed["confidence"]["quality_mean"],
            quality_stddev=parsed["confidence"]["quality_stddev"],
            band_mix=parsed["confidence"]["band_mix"],
            drivers=parsed["confidence"]["drivers"],
        ),
        what_to_check_next=[
            InvestmentActionItem(
                action=a["action"],
                why=a["why"],
                where=a["where"],
            )
            for a in parsed["what_to_check_next"]
        ],
        anti_claims=parsed["anti_claims"],
        status=parsed["status"],
    )
