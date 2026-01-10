"""
Mock LLM provider for testing and development.

Returns deterministic, compliant explanation text without external API calls.
"""

from __future__ import annotations


class MockProvider:
    """
    Mock LLM provider that returns compliant explanation text.

    Used for testing and development when no real LLM API is available.
    The mock responses follow AGENTS-WG.md language rules exactly.
    """

    async def complete(self, prompt: str) -> str:
        """
        Generate a mock explanation that follows AGENTS-WG.md rules.

        The response uses only approved language (appears, leans, suggests)
        and never uses forbidden language (is, was, detected, determined).
        """
        # Extract key info from prompt to make response contextual
        lines = prompt.split("\n")
        confidence_band = "moderate"
        top_category = "feature"
        top_score = 0.25

        for line in lines:
            if "Overall Confidence:" in line:
                # Parse something like "Overall Confidence: 0.72 (moderate)"
                if "(high)" in line:
                    confidence_band = "high"
                elif "(moderate)" in line:
                    confidence_band = "moderate"
                elif "(low)" in line:
                    confidence_band = "low"
                elif "(very_low)" in line:
                    confidence_band = "very_low"
            if "  - " in line and ":" in line and "%" in line:
                # Parse something like "  - feature: 48.00%"
                try:
                    parts = line.strip().lstrip("- ").split(":")
                    category = parts[0].strip()
                    score_str = parts[1].strip().rstrip("%")
                    score = float(score_str) / 100
                    if score > top_score:
                        top_score = score
                        top_category = category
                except (ValueError, IndexError):
                    pass

        # Build response using only approved language
        response = f"""**SUMMARY**: Based on the precomputed signals, this work unit appears to lean toward {top_category} work.

**REASONS**: 
- Structural evidence appears to contribute most significantly to the categorization.
- Temporal coherence suggests the work occurred within a consistent timeframe.
- Textual modifiers were applied as minor adjustments but appear to align with structural signals.

**UNCERTAINTY**: 
This analysis reflects {confidence_band} overall confidence. The categorization leans toward {top_category} but may not fully capture the nuanced nature of the work. The signals suggest a tendency rather than a definitive classification.

**Confidence Limits**: With {confidence_band} confidence ({top_score:.0%} for the top category), these results should be interpreted as probabilistic indicators. Lower confidence categories may still represent meaningful aspects of the work."""

        return response
