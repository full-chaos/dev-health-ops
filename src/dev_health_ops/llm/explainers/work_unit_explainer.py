"""
Work Unit Explainer - builds LLM prompts for explaining precomputed investment views.

CRITICAL: This module follows Investment View rules:
- LLMs explain results, they NEVER compute them
- Only allowed inputs: investment vectors, evidence metadata,
  evidence quality band, time span
- FORBIDDEN inputs: raw events, raw text blobs, code diffs, heuristic formulas
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

# Canonical prompt from AGENTS-WG.md - USE VERBATIM
CANONICAL_EXPLANATION_PROMPT = """You are explaining a precomputed investment view.

You are not allowed to:
- Recalculate scores
- Change categories
- Introduce new conclusions
- Be conversational (no "Hello", "As an AI", or interactive follow-ups)

Explain the investment view in three distinct sections:

1. **SUMMARY**: Provide a high-level narrative (max 3 sentences) using probabilistic language (appears, leans, suggests) explaining why the work leans toward the primary categories.
2. **REASONS**: List the specific evidence (structural, contextual, textual) that contributed most to this interpretation.
3. **UNCERTAINTY**: Disclose where uncertainty exists based on the evidence quality and evidence mix.

Always include evidence quality level and limits."""

# Approved language per AGENTS-WG.md
APPROVED_WORDS = frozenset(["appears", "leans", "suggests"])

# Forbidden language per AGENTS-WG.md
FORBIDDEN_WORDS = frozenset(["is", "was", "detected", "determined"])


@dataclass(frozen=True)
class ExplanationInputs:
    """
    Allowed inputs for LLM explanation per AGENTS-WG.md Section 4.

    This dataclass contains ONLY the inputs that an LLM is allowed to see.
    Raw events, text blobs, code diffs, and heuristic formulas are forbidden.
    """

    work_unit_id: str
    time_range_start: datetime
    time_range_end: datetime
    categories: Dict[str, float]  # Investment category vectors
    evidence_quality_value: float
    evidence_quality_band: str  # high, moderate, low, very_low
    evidence_summary: Dict[str, Any] = field(default_factory=dict)


def extract_allowed_inputs(
    work_unit_id: str,
    time_range_start: datetime,
    time_range_end: datetime,
    categories: Dict[str, float],
    evidence_quality_value: float,
    evidence_quality_band: str,
    evidence: Optional[Dict[str, List[Any]]] = None,
) -> ExplanationInputs:
    """
    Extract only the inputs allowed per AGENTS-WG.md for LLM consumption.

    This function sanitizes evidence to include only metadata summaries,
    never raw text content.

    Args:
        work_unit_id: Unique identifier for the work unit
        time_range_start: Start of the work unit's time span
        time_range_end: End of the work unit's time span
        categories: Investment category vector (must sum to ~1.0)
        evidence_quality_value: Overall evidence quality score (0-1)
        evidence_quality_band: Evidence quality band (high/moderate/low/very_low)
        evidence: Optional evidence dict with structural/contextual/textual arrays

    Returns:
        ExplanationInputs with sanitized, allowed-only data
    """
    # Summarize evidence without exposing raw content
    evidence_summary = _summarize_evidence(evidence or {})

    return ExplanationInputs(
        work_unit_id=work_unit_id,
        time_range_start=time_range_start,
        time_range_end=time_range_end,
        categories=dict(categories),
        evidence_quality_value=evidence_quality_value,
        evidence_quality_band=evidence_quality_band,
        evidence_summary=evidence_summary,
    )


def _summarize_evidence(evidence: Dict[str, List[Any]]) -> Dict[str, Any]:
    """
    Summarize evidence arrays into metadata-only summaries.

    Raw text content is NEVER included, only counts and types.
    """
    summary: Dict[str, Any] = {}

    # Structural evidence - summarize types and values
    structural = evidence.get("structural", [])
    if structural:
        summary["structural"] = cast(
            Dict[str, Any],
            {
                "count": len(structural),
                "types": list(
                    {str(item.get("type", "unknown")) for item in structural}
                ),
            },
        )
        # Include graph density if present
        for item in structural:
            if item.get("type") == "graph_density":
                summary["structural"]["density"] = float(item.get("value", 0))
            if item.get("type") == "provenance":
                summary["structural"]["provenance_score"] = float(item.get("value", 0))

    # Contextual evidence - summarize time span and structural context
    contextual = evidence.get("contextual", [])
    if contextual:
        summary["contextual"] = cast(
            Dict[str, Any],
            {
                "count": len(contextual),
            },
        )
        for item in contextual:
            if item.get("type") == "time_range":
                summary["contextual"]["span_days"] = float(item.get("span_days", 0))
                summary["contextual"]["score"] = float(item.get("score", 0))

    # Textual evidence - count only, no raw content
    textual = evidence.get("textual", [])
    if textual:
        # Count only explicit text matches; avoid "unknown" categories.
        category_counts: Dict[str, int] = {}
        match_count = 0
        for item in textual:
            if not isinstance(item, dict):
                continue
            phrase = item.get("phrase") or item.get("keyword") or item.get("quote")
            category = item.get("subcategory") or item.get("category")
            if phrase is not None:
                match_count += 1
            if category:
                key = str(category)
                category_counts[key] = category_counts.get(key, 0) + 1
        if match_count or category_counts:
            summary["textual"] = {
                "match_count": match_count,
                "categories_with_matches": list(category_counts.keys()),
            }

    return summary


def build_explanation_prompt(inputs: ExplanationInputs) -> str:
    """
    Build the full LLM prompt from allowed inputs.

    Uses the canonical prompt from AGENTS-WG.md and appends structured
    work unit data for the LLM to explain.

    Args:
        inputs: Sanitized inputs extracted via extract_allowed_inputs()

    Returns:
        Complete prompt string for LLM
    """
    # Format investment vector
    categories_str = "\n".join(
        f"  - {cat}: {score:.2%}" for cat, score in sorted(inputs.categories.items())
    )

    # Format time range
    span_days = (
        inputs.time_range_end - inputs.time_range_start
    ).total_seconds() / 86400

    # Format evidence summary
    evidence_lines = []
    struct = inputs.evidence_summary.get("structural", {})
    if struct:
        evidence_lines.append(
            f"  - Structural evidence: {struct.get('count', 0)} items, "
            f"types: {', '.join(struct.get('types', []))}"
        )
        if "density" in struct:
            evidence_lines.append(f"    - Graph density: {struct['density']:.2f}")
        if "provenance_score" in struct:
            evidence_lines.append(
                f"    - Provenance score: {struct['provenance_score']:.2f}"
            )

    contextual = inputs.evidence_summary.get("contextual", {})
    if contextual:
        evidence_lines.append(
            f"  - Contextual evidence: span {contextual.get('span_days', 0):.1f} days, "
            f"score {contextual.get('score', 0):.2f}"
        )

    text = inputs.evidence_summary.get("textual", {})
    if text:
        evidence_lines.append(
            f"  - Textual phrases cited: {text.get('match_count', 0)} matches."
        )

    evidence_str = (
        "\n".join(evidence_lines) if evidence_lines else "  (no evidence details)"
    )

    # Build the data section
    data_section = f"""
---
WORK UNIT DATA (precomputed, do not recalculate):

Work Unit ID: {inputs.work_unit_id}
Time Range: {inputs.time_range_start.isoformat()} to {inputs.time_range_end.isoformat()}
Time Span: {span_days:.1f} days

Investment Vector:
{categories_str}

Evidence Quality: {inputs.evidence_quality_value:.2f} ({inputs.evidence_quality_band})

Evidence Summary:
{evidence_str}
---

Based on the above precomputed investment view, explain why this work leans toward certain categories.
Use probabilistic language (appears, leans, suggests). Never use definitive language (is, was, detected).
"""

    return CANONICAL_EXPLANATION_PROMPT + data_section


def validate_explanation_language(text: str) -> List[str]:
    """
    Validate that explanation text follows AGENTS-WG.md language rules.

    Returns list of violations found (empty if compliant).
    """
    violations = []
    words = set(text.lower().split())

    for forbidden in FORBIDDEN_WORDS:
        # Check for forbidden words appear as standalone words
        # (not as part of other words like "registered")
        if forbidden in words:
            violations.append(f"Forbidden word found: '{forbidden}'")

    return violations
