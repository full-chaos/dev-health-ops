"""
Mock LLM provider for testing and development.

Returns deterministic, compliant explanation and categorization text without external API calls.
"""

from __future__ import annotations

import json


class MockProvider:
    """
    Mock LLM provider that returns compliant explanation and categorization text.

    Used for testing and development when no real LLM API is available.
    The mock responses follow the investment view language rules exactly.
    """

    async def complete(self, prompt: str) -> str:
        """
        Generate a mock response that follows Investment model rules.

        The response uses only approved language (appears, leans, suggests)
        and never uses forbidden language (is, was, detected, determined).
        """
        if "Output schema" in prompt and "\"subcategories\"" in prompt:
            return self._mock_categorization(prompt)

        # Extract key info from prompt to make response contextual
        lines = prompt.split("\n")
        evidence_quality_band = "moderate"
        top_category = "feature_delivery.customer"
        top_score = 0.25

        for line in lines:
            if "Evidence Quality:" in line:
                if "(high)" in line:
                    evidence_quality_band = "high"
                elif "(moderate)" in line:
                    evidence_quality_band = "moderate"
                elif "(low)" in line:
                    evidence_quality_band = "low"
                elif "(very_low)" in line:
                    evidence_quality_band = "very_low"
            if "  - " in line and ":" in line and "%" in line:
                # Parse something like "  - feature_delivery.customer: 48.00%"
                try:
                    parts = line.strip().lstrip("- ").split(":")
                    category = parts[0].strip()
                    score_str = parts[1].strip().rstrip("%")
                    score = float(score_str) / 100
                    if score > top_score:
                        top_score = score
                        top_category = category
                except (ValueError, IndexError):
                    # Ignore malformed or unexpectedly formatted score lines in the mock provider
                    # and continue using the previously computed/default top_score and top_category.
                    pass

        # Build response using only approved language
        response = f"""**SUMMARY**: Based on the precomputed investment view, this work unit appears to lean toward {top_category} work.

**REASONS**: 
- Structural evidence appears to contribute most significantly to the categorization.
- Contextual evidence suggests the work occurred within a consistent timeframe.
- Textual phrases appear to align with the investment interpretation.

**UNCERTAINTY**: 
This analysis reflects {evidence_quality_band} evidence quality. The categorization leans toward {top_category} but may not fully capture the nuanced nature of the work. The evidence suggests a tendency rather than a definitive classification.

**Evidence Quality Limits**: With {evidence_quality_band} evidence quality ({top_score:.0%} for the top category), these results should be interpreted as probabilistic indicators. Lower-weight categories may still represent meaningful aspects of the work."""

        return response

    def _mock_categorization(self, prompt: str) -> str:
        current_source = "issue_title"
        phrase = "incremental improvement"
        for line in prompt.splitlines():
            line = line.strip()
            if line.startswith("[") and line.endswith("]"):
                current_source = line.strip("[]")
            if line.startswith("- ") and phrase == "incremental improvement":
                phrase = line[2:].strip()

        top_category = "feature_delivery.customer"
        lowered = phrase.lower()
        if any(token in lowered for token in ["incident", "outage", "on-call", "hotfix"]):
            top_category = "operational.incident_response"
        elif any(token in lowered for token in ["refactor", "cleanup", "chore", "upgrade"]):
            top_category = "maintenance.refactor"
        elif any(token in lowered for token in ["bug", "fix", "test", "reliability"]):
            top_category = "quality.bugfix"
        elif any(token in lowered for token in ["security", "vulnerability", "compliance"]):
            top_category = "risk.security"

        base = {cat: 1.0 / 15.0 for cat in [
            "feature_delivery.customer",
            "feature_delivery.roadmap",
            "feature_delivery.enablement",
            "operational.incident_response",
            "operational.on_call",
            "operational.support",
            "maintenance.refactor",
            "maintenance.upgrade",
            "maintenance.debt",
            "quality.testing",
            "quality.bugfix",
            "quality.reliability",
            "risk.security",
            "risk.compliance",
            "risk.vulnerability",
        ]}
        base[top_category] = 0.5
        remaining = 0.5 / 14.0
        for cat in base:
            if cat != top_category:
                base[cat] = remaining

        response = {
            "subcategories": base,
            "textual_evidence": [
                {
                    "phrase": phrase,
                    "source": current_source,
                    "subcategory": top_category,
                }
            ],
            "uncertainty": [
                {
                    "subcategory": top_category,
                    "statement": "Text evidence is limited; categorization suggests an initial interpretation.",
                }
            ],
        }
        return json.dumps(response)
