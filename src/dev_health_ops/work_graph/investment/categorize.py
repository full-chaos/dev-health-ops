"""LLM-backed categorization for investment subcategories."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from dev_health_ops.llm import CompletionResult, LLMProvider, get_provider
from dev_health_ops.work_graph.investment.categorization_prompts import (
    PROMPT_VERSION as PROMPT_VERSION,
)
from dev_health_ops.work_graph.investment.categorization_prompts import (
    TAXONOMY_VERSION as TAXONOMY_VERSION,
)
from dev_health_ops.work_graph.investment.categorization_prompts import (
    build_categorization_prompt as build_categorization_prompt,
)
from dev_health_ops.work_graph.investment.categorization_prompts import (
    build_prompt as _build_prompt,
)
from dev_health_ops.work_graph.investment.categorization_prompts import (
    build_repair_prompt as _build_repair_prompt,
)
from dev_health_ops.work_graph.investment.llm_schema import (
    EvidenceQuote,
    LLMValidationResult,
    parse_llm_json,
    validate_llm_payload,
)
from dev_health_ops.work_graph.investment.llm_telemetry import (
    PROMPT_KIND_CATEGORIZE,
    STAGE_INITIAL,
    STAGE_REPAIR,
    llm_call_metrics,
    record_categorization_outcome,
    record_validation,
)
from dev_health_ops.work_graph.investment.types import TextBundle
from dev_health_ops.work_graph.investment.utils import ensure_full_subcategory_vector

logger = logging.getLogger(__name__)

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


def _fallback_distribution() -> dict[str, float]:
    return ensure_full_subcategory_vector(FALLBACK_PRIOR)


def fallback_outcome(
    reason: str, *, provider: str = "unknown", model: str | None = None
) -> CategorizationOutcome:
    record_categorization_outcome(
        provider=provider, model=model, prompt_version=PROMPT_VERSION, status=reason
    )
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
    stage: str = STAGE_INITIAL,
) -> CompletionResult:
    with llm_call_metrics(
        provider=provider_name,
        model=model,
        stage=stage,
        prompt_kind=PROMPT_KIND_CATEGORIZE,
        prompt_version=PROMPT_VERSION,
    ) as call:
        provider_instance = provider or get_provider(provider_name, model=model)
        result = await provider_instance.complete(prompt)
        call.set_result(
            model=result.model,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            text=result.text,
        )
        return result


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
    record_validation(
        provider=llm_provider,
        model=resolved_model or llm_model,
        stage=STAGE_INITIAL,
        prompt_version=PROMPT_VERSION,
        errors=validation.errors,
    )

    if validation.ok:
        record_categorization_outcome(
            provider=llm_provider,
            model=resolved_model or llm_model,
            prompt_version=PROMPT_VERSION,
            status="ok",
        )
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

    repair_prompt = _build_repair_prompt(
        validation.errors, bundle.source_block, completion_text
    )
    repaired_completion = await _complete(
        repair_prompt,
        llm_provider,
        model=llm_model,
        provider=provider,
        stage=STAGE_REPAIR,
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
    record_validation(
        provider=llm_provider,
        model=resolved_model or llm_model,
        stage=STAGE_REPAIR,
        prompt_version=PROMPT_VERSION,
        errors=validation.errors,
    )

    if validation.ok:
        record_categorization_outcome(
            provider=llm_provider,
            model=resolved_model or llm_model,
            prompt_version=PROMPT_VERSION,
            status="repaired",
        )
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
    record_categorization_outcome(
        provider=llm_provider,
        model=resolved_model or llm_model,
        prompt_version=PROMPT_VERSION,
        status="invalid_llm_output",
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
