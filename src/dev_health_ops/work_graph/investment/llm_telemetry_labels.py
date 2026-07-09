"""Fixed-cardinality labels for investment LLM telemetry."""

from __future__ import annotations

from dev_health_ops.llm.errors import (
    LLMAuthError,
    LLMContextLengthError,
    LLMError,
    LLMOutputError,
    LLMRateLimitError,
    LLMServerError,
)

PROMPT_KIND_CATEGORIZE = "investment_categorize"
PROMPT_KIND_MIX_EXPLAIN = "investment_mix_explain"
STAGE_INITIAL = "initial"
STAGE_REPAIR = "repair"
STAGE_REQUEST = "request"

PROMPT_KINDS = frozenset({PROMPT_KIND_CATEGORIZE, PROMPT_KIND_MIX_EXPLAIN})
STAGES = frozenset({STAGE_INITIAL, STAGE_REPAIR, STAGE_REQUEST})
PROMPT_VERSIONS = frozenset(
    {"investment-categorization-v2", "investment-mix-explain-v2"}
)
PROVIDERS = frozenset(
    {
        "openai",
        "anthropic",
        "gemini",
        "qwen",
        "qwen-local",
        "qwen-lmstudio",
        "local",
        "ollama",
        "lmstudio",
        "mock",
        "none",
        "unknown",
    }
)
CATEGORIZATION_STATUSES = frozenset(
    {
        "ok",
        "repaired",
        "invalid_llm_output",
        "insufficient_evidence",
        "no_text_sources",
        "llm_task_failed",
    }
)
PARSE_STATUSES = frozenset(
    {"valid", "invalid_json", "invalid_llm_output", "forbidden_language"}
)
VALIDATION_ERROR_FAMILIES = frozenset(
    {
        "invalid_json",
        "payload_not_object",
        "missing_top_level_keys",
        "unexpected_top_level_keys",
        "subcategories_not_object",
        "unknown_subcategory",
        "invalid_weight",
        "non_finite_weight",
        "negative_weight",
        "weight_overflow",
        "weight_sum_not_finite",
        "all_weights_zero",
        "evidence_quotes_not_list",
        "evidence_quotes_count_out_of_range",
        "evidence_quote_not_object",
        "evidence_quote_missing_keys",
        "evidence_quote_extra_keys",
        "evidence_quote_invalid_type",
        "evidence_quote_empty",
        "evidence_quote_too_long",
        "evidence_quote_invalid_source",
        "evidence_quote_missing_id",
        "evidence_quote_unknown_source",
        "evidence_quote_not_substring",
        "uncertainty_invalid_type",
        "uncertainty_missing",
        "uncertainty_too_long",
    }
)


def bounded(value: str, allowed: frozenset[str], default: str = "other") -> str:
    return value if value in allowed else default


def provider_bucket(provider: str) -> str:
    return bounded(provider.strip().lower(), PROVIDERS)


def model_bucket(model: str | None) -> str:
    normalized = (model or "").strip().lower()
    if not normalized:
        return "unknown"
    if normalized.startswith("gpt-5-nano"):
        return "gpt-5-nano"
    if normalized.startswith("gpt-5-mini"):
        return "gpt-5-mini"
    if normalized.startswith(("gpt-5", "gpt-6", "openai/gpt-oss")):
        return "openai-reasoning-other"
    if normalized.startswith("gpt-4"):
        return "gpt-4"
    if normalized.startswith("claude"):
        return "claude"
    if normalized.startswith("gemini"):
        return "gemini"
    if normalized.startswith("qwen"):
        return "qwen"
    if normalized.startswith(("llama", "local-")):
        return "local"
    return "other"


def validation_error_family(raw_error: str) -> str:
    prefix = raw_error.split(":", 1)[0].strip()
    return bounded(prefix, VALIDATION_ERROR_FAMILIES)


def classify_llm_exception_family(exc: BaseException) -> str:
    if isinstance(exc, LLMAuthError):
        return "auth"
    if isinstance(exc, LLMRateLimitError):
        return "rate_limit"
    if isinstance(exc, LLMContextLengthError):
        return "context_length"
    if isinstance(exc, LLMServerError):
        return "server_error"
    if isinstance(exc, LLMOutputError):
        return "output_error"
    if isinstance(exc, LLMError):
        return "llm_error"
    return "unexpected_error"
