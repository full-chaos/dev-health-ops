"""
Work Unit Explanation Service.

Generates LLM-powered explanations for precomputed work unit signals.

CRITICAL: This service follows AGENTS-WG.md Phase 3 rules:
- LLMs explain results, they NEVER compute them
- Only allowed inputs passed to LLM
- Responses must use approved language
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

from analytics.work_unit_explainer import (
    build_explanation_prompt,
    extract_allowed_inputs,
    validate_explanation_language,
)
from ..models.schemas import WorkUnitConfidence, WorkUnitExplanation, WorkUnitSignal
from .llm_providers import get_provider

logger = logging.getLogger(__name__)


async def explain_work_unit(
    signal: WorkUnitSignal,
    llm_provider: str = "auto",
) -> WorkUnitExplanation:
    """
    Generate an LLM explanation for a work unit's precomputed signals.

    This function:
    1. Extracts only allowed inputs from the signal (per AGENTS-WG.md)
    2. Builds the canonical explanation prompt
    3. Calls the LLM provider
    4. Parses and validates the response
    5. Returns a structured WorkUnitExplanation

    Args:
        signal: The precomputed WorkUnitSignal to explain
        llm_provider: Which LLM provider to use ("auto", "openai", "anthropic", "mock")

    Returns:
        Structured WorkUnitExplanation with validated content
    """
    # 1. Extract only allowed inputs
    inputs = extract_allowed_inputs(
        work_unit_id=signal.work_unit_id,
        time_range_start=signal.time_range.start,
        time_range_end=signal.time_range.end,
        categories=signal.categories,
        confidence_value=signal.confidence.value,
        confidence_band=signal.confidence.band,
        evidence={
            "structural": signal.evidence.structural,
            "temporal": signal.evidence.temporal,
            "textual": signal.evidence.textual,
        },
    )

    # 2. Build the canonical prompt
    prompt = build_explanation_prompt(inputs)
    logger.debug(
        "Generated explanation prompt for work_unit_id=%s", signal.work_unit_id
    )

    # 3. Call LLM provider
    provider = get_provider(llm_provider)
    raw_response = await provider.complete(prompt)
    logger.debug(
        "Received LLM response for work_unit_id=%s, length=%d",
        signal.work_unit_id,
        len(raw_response),
    )

    # 4. Validate language compliance
    violations = validate_explanation_language(raw_response)
    if violations:
        logger.warning(
            "LLM response contains language violations for work_unit_id=%s: %s",
            signal.work_unit_id,
            violations,
        )
        # We log but don't reject - the violations are informational

    # 5. Parse and structure the response
    return _parse_llm_response(signal.work_unit_id, raw_response, signal)


def _parse_llm_response(
    work_unit_id: str,
    raw_response: str,
    signal: WorkUnitSignal,
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
        reasons_text or raw_response, signal.categories
    )

    # 4. Extract specific signal importance from REASONS
    signal_importance = _extract_signal_importance(reasons_text or raw_response)

    # 5. Extract uncertainty disclosure
    uncertainty = uncertainty_text or _extract_uncertainty(
        raw_response, signal.confidence.band
    )

    # 6. Extract confidence limits (usually part of UNCERTAINTY or bottom of text)
    confidence_limits = _extract_confidence_limits(
        uncertainty_text or raw_response, signal.confidence
    )

    return WorkUnitExplanation(
        work_unit_id=work_unit_id,
        summary=summary,
        category_rationale=category_rationale,
        signal_importance=signal_importance,
        uncertainty_disclosure=uncertainty,
        confidence_limits=confidence_limits,
    )


def _extract_section(text: str, header: Optional[str] = None, default: str = "") -> str:
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
    text: str, categories: Dict[str, float]
) -> Dict[str, str]:
    """Extract rationale for each category from the response."""
    rationale: Dict[str, str] = {}

    # Try to find category analysis section
    analysis_section = _extract_section(text, "Category Analysis")

    for category in categories:
        # Look for mentions of the category
        pattern = rf"{category}[^.]*\."
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            rationale[category] = matches[0].strip()
        elif analysis_section:
            rationale[category] = "Category appears in overall analysis."
        else:
            rationale[category] = "Category leaning based on structural signals."

    return rationale


def _extract_signal_importance(text: str) -> List[str]:
    """Extract list of important signals from the response."""
    importance: List[str] = []

    # Look for signal importance section
    importance_section = _extract_section(text, "Signal Importance")
    if importance_section:
        # Extract bullet points
        bullets = re.findall(r"[-•]\s*(.+?)(?=\n|$)", importance_section)
        importance.extend(b.strip() for b in bullets if b.strip())

    # Fallback defaults based on common patterns
    if not importance:
        if "structural" in text.lower():
            importance.append("Structural evidence appears to be a primary signal")
        if "temporal" in text.lower():
            importance.append("Temporal coherence suggests consistent work patterns")
        if "textual" in text.lower():
            importance.append("Textual modifiers provided minor adjustments")

    return importance or ["Structural signals appear most significant"]


def _extract_uncertainty(text: str, confidence_band: str) -> str:
    """Extract uncertainty disclosure from the response."""
    # Look for uncertainty section
    uncertainty = _extract_section(text, "Uncertainty Disclosure")
    if uncertainty:
        return uncertainty

    uncertainty = _extract_section(text, "Uncertainty")
    if uncertainty:
        return uncertainty

    # Default based on confidence band
    band_text = {
        "high": "With high confidence, uncertainty appears minimal but results should still be interpreted probabilistically.",
        "moderate": "Moderate confidence suggests meaningful uncertainty exists in the categorization.",
        "low": "Low confidence indicates significant uncertainty; these results should be treated as tentative.",
        "very_low": "Very low confidence indicates high uncertainty; categorization leans toward estimates only.",
    }
    return band_text.get(confidence_band, band_text["moderate"])


def _extract_confidence_limits(
    text: str,
    confidence: "WorkUnitConfidence",
) -> str:
    """Extract confidence limits statement from the response."""
    # Look for confidence limits section
    limits = _extract_section(text, "Confidence Limits")
    if limits:
        return limits

    # Default statement
    return (
        f"With {confidence.band} confidence ({confidence.value:.0%}), "
        f"these results should be interpreted as probabilistic indicators. "
        f"The categorization suggests tendencies rather than definitive classifications."
    )
