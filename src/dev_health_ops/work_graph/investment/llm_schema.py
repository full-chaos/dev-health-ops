"""Strict JSON schema validation for compute-time investment categorization."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field

from dev_health_ops.work_graph.investment.taxonomy import SUBCATEGORIES
from dev_health_ops.work_graph.investment.utils import ensure_full_subcategory_vector

ALLOWED_TOP_LEVEL_KEYS = {"subcategories", "evidence_quotes", "uncertainty"}
ALLOWED_QUOTE_KEYS = {"quote", "source", "id"}
ALLOWED_SOURCES = {"issue", "pr", "commit"}

# Subcategory values are relative weights, not fixed-sum probabilities: any
# finite, non-negative number on any consistent scale is accepted. The full
# vector is deterministically normalized to sum to 1 (see
# ensure_full_subcategory_vector). WEIGHT_NORMALIZATION_TOLERANCE only
# controls whether a `weights_normalized` audit warning is emitted; it never
# causes rejection.
WEIGHT_NORMALIZATION_TOLERANCE = 1e-6
MAX_UNCERTAINTY_LEN = 280
MAX_QUOTE_LEN = 280
MIN_QUOTES = 1
MAX_QUOTES = 10


@dataclass(frozen=True)
class EvidenceQuote:
    quote: str
    source_type: str
    source_id: str


@dataclass(frozen=True)
class LLMValidationResult:
    ok: bool
    errors: list[str]
    subcategories: dict[str, float]
    evidence_quotes: list[EvidenceQuote]
    uncertainty: str
    warnings: list[str] = field(default_factory=list)


def _recover_quote_span(quote: str, source_text: str) -> str | None:
    tokens = quote.split()
    if not tokens:
        return None
    pattern = r"\s+".join(re.escape(token) for token in tokens)
    match = re.search(pattern, source_text)
    if match is None:
        return None
    return match.group(0)


def parse_llm_json(raw_text: str) -> tuple[dict[str, object] | None, list[str]]:
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return None, [f"invalid_json: {exc}"]
    if not isinstance(payload, dict):
        return None, ["payload_not_object"]
    return payload, []


def validate_llm_payload(
    payload: dict[str, object],
    source_texts: dict[str, dict[str, str]],
    handle_map: dict[str, tuple[str, str]],
) -> LLMValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    keys = set(payload.keys())
    if keys != ALLOWED_TOP_LEVEL_KEYS:
        missing = ALLOWED_TOP_LEVEL_KEYS - keys
        extra = keys - ALLOWED_TOP_LEVEL_KEYS
        if missing:
            errors.append(f"missing_top_level_keys:{sorted(missing)}")
        if extra:
            errors.append(f"unexpected_top_level_keys:{sorted(extra)}")

    raw_subcategories = payload.get("subcategories")
    if not isinstance(raw_subcategories, dict):
        errors.append("subcategories_not_object")
        raw_subcategories = {}

    cleaned: dict[str, float] = {}
    for key, value in raw_subcategories.items():
        if key not in SUBCATEGORIES:
            errors.append(f"unknown_subcategory:{key}")
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"invalid_weight:{key}")
            continue
        try:
            numeric = float(value)
        except OverflowError:
            errors.append(f"weight_overflow:{key}")
            continue
        if not math.isfinite(numeric):
            errors.append(f"non_finite_weight:{key}")
            continue
        if numeric < 0.0:
            errors.append(f"negative_weight:{key}")
            continue
        cleaned[key] = numeric

    total = sum(cleaned.values())
    if not math.isfinite(total):
        errors.append("weight_sum_not_finite")
    elif total <= 0.0:
        errors.append("all_weights_zero")
    elif abs(total - 1.0) > WEIGHT_NORMALIZATION_TOLERANCE:
        warnings.append(f"weights_normalized:{total:.4f}")

    evidence_quotes_raw = payload.get("evidence_quotes")
    evidence_quotes: list[EvidenceQuote] = []
    if not isinstance(evidence_quotes_raw, list):
        errors.append("evidence_quotes_not_list")
        evidence_quotes_raw = []

    if isinstance(evidence_quotes_raw, list):
        if (
            len(evidence_quotes_raw) < MIN_QUOTES
            or len(evidence_quotes_raw) > MAX_QUOTES
        ):
            errors.append("evidence_quotes_count_out_of_range")
        for idx, entry in enumerate(evidence_quotes_raw):
            if not isinstance(entry, dict):
                errors.append(f"evidence_quote_not_object:{idx}")
                continue
            entry_keys = set(entry.keys())
            if entry_keys != ALLOWED_QUOTE_KEYS:
                missing = ALLOWED_QUOTE_KEYS - entry_keys
                extra = entry_keys - ALLOWED_QUOTE_KEYS
                if missing:
                    errors.append(
                        f"evidence_quote_missing_keys:{idx}:{sorted(missing)}"
                    )
                if extra:
                    errors.append(f"evidence_quote_extra_keys:{idx}:{sorted(extra)}")
                continue
            raw_quote = entry.get("quote")
            raw_source_type = entry.get("source")
            raw_source_id = entry.get("id")
            if (
                not isinstance(raw_quote, str)
                or not isinstance(raw_source_type, str)
                or not isinstance(raw_source_id, str)
            ):
                errors.append(f"evidence_quote_invalid_type:{idx}")
                continue
            quote = raw_quote.strip()
            source_type = raw_source_type.strip()
            source_id = raw_source_id.strip()
            if not quote:
                errors.append(f"evidence_quote_empty:{idx}")
                continue
            if len(quote) > MAX_QUOTE_LEN:
                errors.append(f"evidence_quote_too_long:{idx}")
                continue
            if source_type not in ALLOWED_SOURCES:
                errors.append(f"evidence_quote_invalid_source:{idx}:{source_type}")
                continue
            if not source_id:
                errors.append(f"evidence_quote_missing_id:{idx}")
                continue
            resolved = handle_map.get(source_id)
            if resolved is None:
                errors.append(f"evidence_quote_unknown_source:{idx}")
                continue
            real_source_type, real_source_id = resolved
            source_map = source_texts.get(real_source_type) or {}
            source_text = source_map.get(real_source_id, "")
            if not source_text:
                errors.append(f"evidence_quote_unknown_source:{idx}")
                continue
            recovered_quote = _recover_quote_span(quote, source_text)
            if recovered_quote is None:
                errors.append(f"evidence_quote_not_substring:{idx}")
                continue
            evidence_quotes.append(
                EvidenceQuote(
                    quote=recovered_quote,
                    source_type=real_source_type,
                    source_id=real_source_id,
                )
            )

    uncertainty_raw = payload.get("uncertainty")
    uncertainty = uncertainty_raw.strip() if isinstance(uncertainty_raw, str) else ""
    if not isinstance(uncertainty_raw, str):
        errors.append("uncertainty_invalid_type")
    elif not uncertainty:
        errors.append("uncertainty_missing")
    elif len(uncertainty) > MAX_UNCERTAINTY_LEN:
        errors.append("uncertainty_too_long")

    if errors:
        return LLMValidationResult(
            ok=False,
            errors=errors,
            subcategories={},
            evidence_quotes=[],
            uncertainty=uncertainty,
            warnings=warnings,
        )

    normalized = ensure_full_subcategory_vector(cleaned)
    return LLMValidationResult(
        ok=True,
        errors=[],
        subcategories=normalized,
        evidence_quotes=evidence_quotes,
        uncertainty=uncertainty,
        warnings=warnings,
    )
