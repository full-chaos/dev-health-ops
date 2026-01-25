"""LLM-backed categorization for investment subcategories."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, List

from dev_health_ops.llm import get_provider, LLMProvider
from dev_health_ops.work_graph.investment.llm_schema import (
    EvidenceQuote,
    LLMValidationResult,
    parse_llm_json,
    validate_llm_payload,
)
from dev_health_ops.work_graph.investment.taxonomy import SUBCATEGORIES
from dev_health_ops.work_graph.investment.types import TextBundle
from dev_health_ops.work_graph.investment.utils import ensure_full_subcategory_vector

logger = logging.getLogger(__name__)

CANONICAL_PROMPT = """You are categorizing work unit evidence into canonical investment subcategories.

Rules:
- Output JSON only. No markdown, no explanations.
- Use ONLY these subcategories as keys: {subcategories}
- Provide a probability distribution across all subcategories (values 0-1, sum to 1).
- Provide evidence_quotes as 1-10 items with exact substrings from the source text.
- evidence_quotes items must have: quote, source (issue|pr|commit), id.
- Provide uncertainty as a short string (1-280 chars).
- No extra keys.

Output schema:
{{
  "subcategories": {{
    "feature_delivery.customer": 0.0,
    "feature_delivery.roadmap": 0.0,
    "feature_delivery.enablement": 0.0,
    "operational.incident_response": 0.0,
    "operational.on_call": 0.0,
    "operational.support": 0.0,
    "maintenance.refactor": 0.0,
    "maintenance.upgrade": 0.0,
    "maintenance.debt": 0.0,
    "quality.testing": 0.0,
    "quality.bugfix": 0.0,
    "quality.reliability": 0.0,
    "risk.security": 0.0,
    "risk.compliance": 0.0,
    "risk.vulnerability": 0.0
  }},
  "evidence_quotes": [
    {{ "quote": "...", "source": "issue", "id": "..." }}
  ],
  "uncertainty": "..."
}}
"""

REPAIR_PROMPT = """Your previous response failed validation.

Errors:
{errors}

Return JSON only matching the schema and rules.
"""

FALLBACK_PRIOR = {
    "feature_delivery.roadmap": 0.2,
    "operational.on_call": 0.2,
    "maintenance.debt": 0.2,
    "quality.bugfix": 0.2,
    "risk.security": 0.2,
}


@dataclass(frozen=True)
class CategorizationOutcome:
    subcategories: Dict[str, float]
    evidence_quotes: List[EvidenceQuote]
    uncertainty: str
    status: str
    errors: List[str]


def _fallback_distribution() -> Dict[str, float]:
    return ensure_full_subcategory_vector(FALLBACK_PRIOR)


def fallback_outcome(reason: str) -> CategorizationOutcome:
    return CategorizationOutcome(
        subcategories=_fallback_distribution(),
        evidence_quotes=[],
        uncertainty="Insufficient validated evidence to assign a confident subcategory mix.",
        status=str(reason or "insufficient_evidence"),
        errors=[reason],
    )


async def _complete(
    prompt: str,
    provider_name: str,
    model: str | None = None,
    provider: LLMProvider | None = None,
) -> str:
    if provider:
        return await provider.complete(prompt)
    provider_instance = get_provider(provider_name, model=model)
    return await provider_instance.complete(prompt)


def _build_prompt(source_block: str) -> str:
    categories = ", ".join(sorted(SUBCATEGORIES))
    prompt = CANONICAL_PROMPT.format(subcategories=categories)
    if source_block:
        return f"{prompt}\n\nSource text (quotes must be exact substrings):\n{source_block}"
    return f"{prompt}\n\nSource text (quotes must be exact substrings):\n(EMPTY)"


def _build_repair_prompt(errors: List[str], source_block: str) -> str:
    errors_text = "\n".join(f"- {err}" for err in errors)
    repair = REPAIR_PROMPT.format(errors=errors_text)
    return f"{repair}\n\n{_build_prompt(source_block)}"


async def categorize_text_bundle(
    bundle: TextBundle,
    *,
    llm_provider: str,
    llm_model: str | None = None,
    provider: LLMProvider | None = None,
) -> CategorizationOutcome:
    prompt = _build_prompt(bundle.source_block)

    raw_response = await _complete(
        prompt, llm_provider, model=llm_model, provider=provider
    )
    payload, parse_errors = parse_llm_json(raw_response)
    if parse_errors:
        validation = LLMValidationResult(
            ok=False,
            errors=parse_errors,
            subcategories={},
            evidence_quotes=[],
            uncertainty="",
        )
    else:
        validation = validate_llm_payload(payload or {}, bundle.source_texts)

    if validation.ok:
        return CategorizationOutcome(
            subcategories=validation.subcategories,
            evidence_quotes=validation.evidence_quotes,
            uncertainty=validation.uncertainty,
            status="ok",
            errors=[],
        )

    repair_prompt = _build_repair_prompt(validation.errors, bundle.source_block)
    repaired_response = await _complete(
        repair_prompt, llm_provider, model=llm_model, provider=provider
    )
    payload, parse_errors = parse_llm_json(repaired_response)
    if parse_errors:
        validation = LLMValidationResult(
            ok=False,
            errors=parse_errors,
            subcategories={},
            evidence_quotes=[],
            uncertainty="",
        )
    else:
        validation = validate_llm_payload(payload or {}, bundle.source_texts)

    if validation.ok:
        return CategorizationOutcome(
            subcategories=validation.subcategories,
            evidence_quotes=validation.evidence_quotes,
            uncertainty=validation.uncertainty,
            status="repaired",
            errors=[],
        )

    logger.warning(
        "Investment categorization failed after repair: %s",
        json.dumps(validation.errors),
    )

    return CategorizationOutcome(
        subcategories=_fallback_distribution(),
        evidence_quotes=[],
        uncertainty="Insufficient validated evidence to assign a confident subcategory mix.",
        status="invalid_llm_output",
        errors=validation.errors,
    )
