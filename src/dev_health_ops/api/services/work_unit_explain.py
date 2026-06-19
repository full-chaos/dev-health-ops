"""
Work Unit Explanation Service.

Generates LLM-powered explanations for precomputed work unit investment views.

CRITICAL: This service follows the Investment View rules:
- LLMs explain results, they NEVER compute them
- Only allowed inputs passed to LLM
- Responses must use approved language
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache

from dev_health_ops.llm import (
    LLMProvider,
    get_provider,
    is_llm_available,
    resolve_provider_name,
)
from dev_health_ops.llm.explainers.work_unit_explainer import (
    build_explanation_prompt,
    extract_allowed_inputs,
    validate_explanation_language,
)
from dev_health_ops.metrics.llm_token_usage import write_llm_token_usage
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

from ..models.schemas import EvidenceQuality, WorkUnitExplanation, WorkUnitInvestment

logger = logging.getLogger(__name__)


@lru_cache(maxsize=256)
def _compiled_category_pattern(category: str) -> re.Pattern[str]:
    """Cache compiled regex for a category key.

    Category names come from a bounded taxonomy of investment themes, so the
    lru_cache cannot grow unbounded. Uses ``re.escape`` so that categories
    containing regex metacharacters (e.g. ``feature_delivery.customer``) are
    matched literally rather than re-interpreted as a pattern.
    """
    return re.compile(rf"{re.escape(category)}[^.]*\.", re.IGNORECASE)


async def explain_work_unit(
    investment: WorkUnitInvestment,
    llm_provider: str = "auto",
    llm_model: str | None = None,
    *,
    provider: LLMProvider | None = None,
    org_id: str | None = None,
    db_url: str | None = None,
) -> WorkUnitExplanation:
    """
    Generate an LLM explanation for a work unit's precomputed investment view.

    This function:
    1. Extracts only allowed inputs from the investment view
    2. Builds the canonical explanation prompt
    3. Calls the LLM provider
    4. Parses and validates the response
    5. Returns a structured WorkUnitExplanation

    Args:
        investment: The precomputed WorkUnitInvestment to explain
        llm_provider: Which LLM provider to use ("auto", "openai", "anthropic", "mock")
        llm_model: Optional model name to override provider default

    Returns:
        Structured WorkUnitExplanation with validated content
    """
    if _should_return_non_ai_explanation(
        llm_provider=llm_provider,
        provider=provider,
        org_id=org_id,
    ):
        return WorkUnitExplanation(
            work_unit_id=investment.work_unit_id,
            ai_generated=False,
            summary="",
            category_rationale={},
            evidence_highlights=[],
            uncertainty_disclosure="",
            evidence_quality_limits="",
        )

    # 1. Extract only allowed inputs
    evidence_quality_value = investment.evidence_quality.value
    if evidence_quality_value is None:
        evidence_quality_value = 0.0

    evidence_quality_band = investment.evidence_quality.band or "unknown"

    inputs = extract_allowed_inputs(
        work_unit_id=investment.work_unit_id,
        time_range_start=investment.time_range.start,
        time_range_end=investment.time_range.end,
        categories=investment.investment.themes,
        evidence_quality_value=evidence_quality_value,
        evidence_quality_band=evidence_quality_band,
        evidence={
            "structural": investment.evidence.structural,
            "contextual": investment.evidence.contextual,
            "textual": investment.evidence.textual,
        },
    )

    # 2. Build the canonical prompt
    prompt = build_explanation_prompt(inputs)
    logger.debug(
        "Generated explanation prompt for work_unit_id=%s", investment.work_unit_id
    )

    # 3. Call LLM provider
    resolved_provider = (
        provider
        if provider is not None
        else get_provider(llm_provider, model=llm_model, org_id=org_id)
    )
    completion = await resolved_provider.complete(prompt)
    raw_response = completion.text
    resolved_llm_provider = resolve_provider_name(llm_provider, org_id=org_id)
    if db_url:
        try:
            ch_sink = ClickHouseMetricsSink(db_url)
            try:
                write_llm_token_usage(
                    ch_sink,
                    org_id=org_id or "",
                    provider=resolved_llm_provider,
                    model=completion.model or llm_model,
                    source="work_unit_explain",
                    input_tokens=completion.input_tokens,
                    output_tokens=completion.output_tokens,
                )
            finally:
                ch_sink.close()
        except Exception:
            logger.debug("Token usage storage failed", exc_info=True)
    logger.debug(
        "Received LLM response for work_unit_id=%s, length=%d",
        investment.work_unit_id,
        len(raw_response),
    )

    # 4. Validate language compliance
    violations = validate_explanation_language(raw_response)
    if violations:
        logger.warning(
            "LLM response contains language violations for work_unit_id=%s: %s",
            investment.work_unit_id,
            violations,
        )
        # We log but don't reject - the violations are informational

    # 5. Parse and structure the response
    return _parse_llm_response(investment.work_unit_id, raw_response, investment)


def _should_return_non_ai_explanation(
    *,
    llm_provider: str,
    provider: LLMProvider | None,
    org_id: str | None,
) -> bool:
    if provider is not None:
        return provider.__class__.__name__ == "NoneProvider"
    try:
        if resolve_provider_name(llm_provider, org_id=org_id) == "none":
            return True
    except Exception:
        return not is_llm_available(llm_provider, org_id=org_id)
    return not is_llm_available(llm_provider, org_id=org_id)


def _parse_llm_response(
    work_unit_id: str,
    raw_response: str,
    investment: WorkUnitInvestment,
) -> WorkUnitExplanation:
    """
    Parse LLM response into structured WorkUnitExplanation.

    Extracts sections from the response and creates a structured output.
    Falls back to defaults if parsing fails.
    """
    # 1. Extract sections using the enforced headers
    summary = _extract_section(raw_response, "SUMMARY")
    reasons_text = _extract_section(raw_response, "REASONS")
    uncertainty_text = _extract_section(raw_response, "UNCERTAINTY")

    # 2. Refine summary (first paragraph if header extraction failed)
    if not summary:
        summary = _extract_section(raw_response, default=raw_response[:500])

    # 3. Categorize rationale from the overall text or REASONS section
    category_rationale = _extract_category_rationale(
        reasons_text or raw_response, investment.investment.themes
    )

    # 4. Extract specific evidence highlights from REASONS
    evidence_highlights = _extract_evidence_highlights(reasons_text or raw_response)

    # 5. Extract uncertainty disclosure
    uncertainty = uncertainty_text or _extract_uncertainty(
        raw_response, investment.evidence_quality.band or "unknown"
    )

    # 6. Extract evidence quality limits (usually part of UNCERTAINTY or bottom of text)
    evidence_quality_limits = _extract_evidence_quality_limits(
        uncertainty_text or raw_response, investment.evidence_quality
    )

    return WorkUnitExplanation(
        work_unit_id=work_unit_id,
        summary=summary,
        category_rationale=category_rationale,
        evidence_highlights=evidence_highlights,
        uncertainty_disclosure=uncertainty,
        evidence_quality_limits=evidence_quality_limits,
    )


def _extract_section(text: str, header: str | None = None, default: str = "") -> str:
    """Extract a section from the response by header, or return first paragraph."""
    if header:
        pattern = rf"\*\*{header}[:\*]*\*\*\s*(.*?)(?=\n\n|\*\*|$)"
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return default

    # Default: return first substantial paragraph
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    if paragraphs:
        return paragraphs[0]
    return default


def _extract_category_rationale(
    text: str, categories: dict[str, float]
) -> dict[str, str]:
    """Extract rationale for each category from the response."""
    rationale: dict[str, str] = {}

    # Try to find category analysis section
    analysis_section = _extract_section(text, "Category Analysis")

    for category in categories:
        # Look for mentions of the category using a cached compiled pattern.
        pattern = _compiled_category_pattern(category)
        matches = pattern.findall(text)
        if matches:
            rationale[category] = matches[0].strip()
        elif analysis_section:
            rationale[category] = "Category appears in overall analysis."
        else:
            rationale[category] = "Category leaning based on structural evidence."

    return rationale


def _extract_evidence_highlights(text: str) -> list[str]:
    """Extract list of important evidence from the response."""
    highlights: list[str] = []

    highlights_section = _extract_section(text, "Evidence Highlights")
    if highlights_section:
        bullets = re.findall(r"[-•]\s*(.+?)(?=\n|$)", highlights_section)
        highlights.extend(b.strip() for b in bullets if b.strip())

    if not highlights:
        if "structural" in text.lower():
            highlights.append("Structural evidence appears most significant")
        if "contextual" in text.lower():
            highlights.append("Contextual evidence provides corroboration")
        if "textual" in text.lower():
            highlights.append("Textual phrases align with the investment mix")

    return highlights or ["Structural evidence appears most significant"]


def _extract_uncertainty(text: str, evidence_quality_band: str) -> str:
    """Extract uncertainty disclosure from the response."""
    # Look for uncertainty section
    uncertainty = _extract_section(text, "Uncertainty Disclosure")
    if uncertainty:
        return uncertainty

    uncertainty = _extract_section(text, "Uncertainty")
    if uncertainty:
        return uncertainty

    # Default based on evidence quality band
    band_text = {
        "high": "With high evidence quality, uncertainty appears minimal but results should still be interpreted probabilistically.",
        "moderate": "Moderate evidence quality suggests meaningful uncertainty exists in the categorization.",
        "low": "Low evidence quality indicates significant uncertainty; these results should be treated as tentative.",
        "very_low": "Very low evidence quality indicates high uncertainty; categorization leans toward estimates only.",
    }
    return band_text.get(evidence_quality_band, band_text["moderate"])


def _extract_evidence_quality_limits(
    text: str,
    evidence_quality: EvidenceQuality,
) -> str:
    """Extract evidence quality limits statement from the response."""
    limits = _extract_section(text, "Evidence Quality Limits")
    if limits:
        return limits

    # Default statement
    return (
        f"With {evidence_quality.band} evidence quality ({evidence_quality.value:.0%}), "
        "these results should be interpreted as probabilistic indicators. "
        "The categorization suggests tendencies rather than definitive classifications."
    )
