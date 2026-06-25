"""LLM-backed categorization for investment subcategories."""

from __future__ import annotations

import json
import logging
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import Any

from dev_health_ops.llm import CompletionResult, LLMProvider, get_provider
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

TAXONOMY_VERSION = "investment-taxonomy-v1"
PROMPT_VERSION = "investment-categorization-v1"

CANONICAL_PROMPT = """You are categorizing work unit evidence into canonical investment subcategories.

Rules:
- Output JSON only. No markdown, no explanations.
- Use ONLY these subcategories as keys: {subcategories}
- Provide a probability distribution across all subcategories (values 0-1, sum to 1).
- Provide evidence_quotes as 1-10 items with exact substrings from the source text.
- evidence_quotes items must have: quote, source (issue|pr|commit), id.
- evidence_quotes id MUST be the bracketed handle shown before an evidence block (for example "E1"), copied exactly.
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
    {{ "quote": "...", "source": "issue", "id": "E1" }}
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
    subcategories: dict[str, float]
    evidence_quotes: list[EvidenceQuote]
    uncertainty: str
    status: str
    errors: list[str]
    warnings: list[str] = field(default_factory=list)
    llm_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    llm_model: str | None = None


def _token_count(value: int | None) -> int:
    return int(value or 0)


def _llm_call_span(
    *, provider_name: str, model: str | None
) -> AbstractContextManager[Any]:
    try:
        from opentelemetry import trace
    except ImportError:
        return nullcontext(None)
    tracer = trace.get_tracer(__name__)
    span = tracer.start_as_current_span("llm.complete")
    return span


def _fallback_distribution() -> dict[str, float]:
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
) -> CompletionResult:
    with _llm_call_span(provider_name=provider_name, model=model) as span:
        try:
            if provider:
                result = await provider.complete(prompt)
            else:
                provider_instance = get_provider(provider_name, model=model)
                result = await provider_instance.complete(prompt)
        except Exception as exc:
            if span is not None:
                span.set_attribute("llm.provider", provider_name)
                if model:
                    span.set_attribute("llm.model", model)
                span.set_attribute("llm.status", "error")
                span.record_exception(exc)
                try:
                    from opentelemetry.trace import Status, StatusCode

                    span.set_status(Status(StatusCode.ERROR, str(exc)[:200]))
                except ImportError:
                    # OpenTelemetry status API is optional; skip enrichment.
                    pass
            raise

        if span is not None:
            span.set_attribute("llm.provider", provider_name)
            span.set_attribute("llm.model", result.model or model or "")
            span.set_attribute("llm.input_tokens", _token_count(result.input_tokens))
            span.set_attribute("llm.output_tokens", _token_count(result.output_tokens))
            span.set_attribute("llm.status", "ok")
        return result


def _build_prompt(source_block: str) -> str:
    categories = ", ".join(sorted(SUBCATEGORIES))
    prompt = CANONICAL_PROMPT.format(subcategories=categories)
    if source_block:
        return f"{prompt}\n\nSource text (quotes must be exact substrings):\n{source_block}"
    return f"{prompt}\n\nSource text (quotes must be exact substrings):\n(EMPTY)"


def build_categorization_prompt(bundle: TextBundle) -> str:
    return _build_prompt(bundle.source_block)


def _build_repair_prompt(errors: list[str], source_block: str) -> str:
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

    raw_completion = await _complete(
        prompt, llm_provider, model=llm_model, provider=provider
    )
    input_tokens = _token_count(raw_completion.input_tokens)
    output_tokens = _token_count(raw_completion.output_tokens)
    llm_calls = 1
    resolved_model = raw_completion.model
    return await categorize_text_bundle_completion(
        bundle,
        raw_completion.text,
        llm_provider=llm_provider,
        llm_model=llm_model,
        provider=provider,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        llm_calls=llm_calls,
        resolved_model=resolved_model,
    )


async def categorize_text_bundle_completion(
    bundle: TextBundle,
    completion_text: str,
    *,
    llm_provider: str,
    llm_model: str | None = None,
    provider: LLMProvider | None = None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    llm_calls: int = 0,
    resolved_model: str | None = None,
) -> CategorizationOutcome:
    payload, parse_errors = parse_llm_json(completion_text)
    if parse_errors:
        validation = LLMValidationResult(
            ok=False,
            errors=parse_errors,
            subcategories={},
            evidence_quotes=[],
            uncertainty="",
        )
    else:
        validation = validate_llm_payload(
            payload or {}, bundle.source_texts, bundle.handle_map
        )

    if validation.ok:
        return CategorizationOutcome(
            subcategories=validation.subcategories,
            evidence_quotes=validation.evidence_quotes,
            uncertainty=validation.uncertainty,
            status="ok",
            errors=[],
            warnings=validation.warnings,
            llm_calls=llm_calls,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            llm_model=resolved_model,
        )

    repair_prompt = _build_repair_prompt(validation.errors, bundle.source_block)
    repaired_completion = await _complete(
        repair_prompt, llm_provider, model=llm_model, provider=provider
    )
    input_tokens += _token_count(repaired_completion.input_tokens)
    output_tokens += _token_count(repaired_completion.output_tokens)
    llm_calls += 1
    resolved_model = repaired_completion.model or resolved_model
    payload, parse_errors = parse_llm_json(repaired_completion.text)
    if parse_errors:
        validation = LLMValidationResult(
            ok=False,
            errors=parse_errors,
            subcategories={},
            evidence_quotes=[],
            uncertainty="",
        )
    else:
        validation = validate_llm_payload(
            payload or {}, bundle.source_texts, bundle.handle_map
        )

    if validation.ok:
        return CategorizationOutcome(
            subcategories=validation.subcategories,
            evidence_quotes=validation.evidence_quotes,
            uncertainty=validation.uncertainty,
            status="repaired",
            errors=[],
            warnings=validation.warnings,
            llm_calls=llm_calls,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            llm_model=resolved_model,
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
        warnings=validation.warnings,
        llm_calls=llm_calls,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        llm_model=resolved_model,
    )
