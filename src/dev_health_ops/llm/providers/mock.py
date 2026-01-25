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
        if (
            "Output schema" in prompt
            and '"subcategories"' in prompt
            and '"evidence_quotes"' in prompt
        ) or "matching the schema" in prompt:
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
        summary = f"Based on the precomputed investment view, this work unit appears to lean toward {top_category} work."
        confidence_note = f"This analysis reflects {evidence_quality_band} evidence quality. The categorization leans toward {top_category} but may not fully capture the nuanced nature of the work."

        response_data = {
            "summary": summary,
            "dominant_themes": [top_category.split(".")[0]],
            "key_drivers": [
                "Structural evidence appears to contribute most significantly to the categorization.",
                "Textual phrases appear to align with the investment interpretation.",
            ],
            "operational_signals": [
                f"Evidence quality bands indicate {evidence_quality_band} uncertainty.",
                "Lower-weight categories may still represent meaningful aspects of the work.",
            ],
            "confidence_note": confidence_note,
        }
        return json.dumps(response_data)

    def _mock_categorization(self, prompt: str) -> str:
        # Extract the first available source block entry, which is formatted as:
        # [issue] <id>
        # <text...>
        source_type = "issue"
        source_id = ""
        source_text = ""
        lines = prompt.splitlines()
        for idx, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line.startswith("[") or "]" not in line:
                continue
            header, rest = line.split("]", 1)
            header = header.strip("[]").strip()
            rest = rest.strip()
            if header in {"issue", "pr", "commit"} and rest:
                source_type = header
                source_id = rest
                # take subsequent non-empty lines as text until next header or blank line
                for next_line in lines[idx + 1 :]:
                    next_line = next_line.rstrip("\n")
                    if next_line.strip().startswith("[") and "]" in next_line:
                        potential_header = next_line.split("]", 1)[0].strip("[").strip()
                        if potential_header in {"issue", "pr", "commit"}:
                            break
                    if next_line.strip() == "":
                        continue
                    # Found a non-empty line, use it as the source text/quote
                    source_text = next_line.strip()
                    break
                break

        phrase = source_text or "incremental improvement"

        top_category = "feature_delivery.customer"
        lowered = phrase.lower()
        if any(
            token in lowered for token in ["incident", "outage", "on-call", "hotfix"]
        ):
            top_category = "operational.incident_response"
        elif any(
            token in lowered for token in ["refactor", "cleanup", "chore", "upgrade"]
        ):
            top_category = "maintenance.refactor"
        elif any(token in lowered for token in ["bug", "fix", "test", "reliability"]):
            top_category = "quality.bugfix"
        elif any(
            token in lowered for token in ["security", "vulnerability", "compliance"]
        ):
            top_category = "risk.security"

        base = {
            cat: 1.0 / 15.0
            for cat in [
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
            ]
        }
        base[top_category] = 0.5
        remaining = 0.5 / 14.0
        for cat in base:
            if cat != top_category:
                base[cat] = remaining

        quote = phrase[: min(80, len(phrase))].strip()
        if not quote:
            quote = "incremental improvement"

        response = {
            "subcategories": base,
            "evidence_quotes": [
                {"quote": quote, "source": source_type, "id": source_id or "unknown"}
            ],
            "uncertainty": "Text evidence is limited; categorization suggests an initial interpretation.",
        }
        return json.dumps(response)

    async def aclose(self) -> None:
        pass
