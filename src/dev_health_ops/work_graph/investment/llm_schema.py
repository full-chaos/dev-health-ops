"""Strict JSON schema validation for compute-time investment categorization."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Dict, List, Optional

from dev_health_ops.work_graph.investment.taxonomy import SUBCATEGORIES
from dev_health_ops.work_graph.investment.utils import ensure_full_subcategory_vector

ALLOWED_TOP_LEVEL_KEYS = {"subcategories", "evidence_quotes", "uncertainty"}
ALLOWED_QUOTE_KEYS = {"quote", "source", "id"}
ALLOWED_SOURCES = {"issue", "pr", "commit"}

MIN_SUM = 0.98
MAX_SUM = 1.02
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
    errors: List[str]
    subcategories: Dict[str, float]
    evidence_quotes: List[EvidenceQuote]
    uncertainty: str


def parse_llm_json(raw_text: str) -> tuple[Optional[Dict[str, object]], List[str]]:
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return None, [f"invalid_json: {exc}"]
    if not isinstance(payload, dict):
        return None, ["payload_not_object"]
    return payload, []


def validate_llm_payload(
    payload: Dict[str, object],
    source_texts: Dict[str, Dict[str, str]],
) -> LLMValidationResult:
    errors: List[str] = []

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

    cleaned: Dict[str, float] = {}
    for key, value in raw_subcategories.items():
        if key not in SUBCATEGORIES:
            errors.append(f"unknown_subcategory:{key}")
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            errors.append(f"invalid_probability:{key}")
            continue
        if not math.isfinite(numeric) or numeric < 0.0 or numeric > 1.0:
            errors.append(f"probability_out_of_range:{key}")
            continue
        cleaned[key] = numeric

    total = sum(cleaned.values())
    if total < MIN_SUM or total > MAX_SUM:
        errors.append(f"probability_sum_out_of_range:{total:.4f}")

    evidence_quotes_raw = payload.get("evidence_quotes")
    evidence_quotes: List[EvidenceQuote] = []
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
            quote = str(entry.get("quote") or "").strip()
            source_type = str(entry.get("source") or "").strip()
            source_id = str(entry.get("id") or "").strip()
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
            source_map = source_texts.get(source_type) or {}
            source_text = source_map.get(source_id, "")
            if not source_text:
                errors.append(f"evidence_quote_unknown_source:{idx}")
                continue
            if quote not in source_text:
                errors.append(f"evidence_quote_not_substring:{idx}")
                continue
            evidence_quotes.append(
                EvidenceQuote(
                    quote=quote,
                    source_type=source_type,
                    source_id=source_id,
                )
            )

    uncertainty_raw = payload.get("uncertainty")
    uncertainty = str(uncertainty_raw or "").strip()
    if not uncertainty:
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
        )

    normalized = ensure_full_subcategory_vector(cleaned)
    return LLMValidationResult(
        ok=True,
        errors=[],
        subcategories=normalized,
        evidence_quotes=evidence_quotes,
        uncertainty=uncertainty,
    )
