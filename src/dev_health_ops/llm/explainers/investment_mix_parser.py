"""Strict parser for investment-mix explanation output."""

from __future__ import annotations

import logging
from typing import Literal

from dev_health_ops.llm.json_utils import extract_json_object

from .investment_mix_types import (
    ActionItem,
    Confidence,
    Finding,
    FindingEvidence,
    InvestmentMixExplainOutput,
    InvestmentMixParseResult,
    ParseStatus,
)
from .investment_mix_validation import (
    NUMERIC_PATTERN,
    TOP_LEVEL_KEYS,
    contains_forbidden_language,
    parse_action,
    parse_finding,
    string_list,
    valid_confidence,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ActionItem",
    "Confidence",
    "Finding",
    "FindingEvidence",
    "InvestmentMixExplainOutput",
    "InvestmentMixParseResult",
    "ParseStatus",
    "parse_investment_mix_response",
]


def parse_investment_mix_response(
    text: str,
    *,
    theme_shares_pct: dict[str, float] | None = None,
    subcategory_shares_pct: dict[str, float] | None = None,
    fallback_level: Literal["high", "moderate", "low", "unknown"] = "unknown",
    fallback_quality_band: str | None = None,
    fallback_band_mix: dict[str, int] | None = None,
    fallback_drivers: list[str] | None = None,
    fallback_mean: float | None = None,
    fallback_stddev: float | None = None,
) -> InvestmentMixParseResult:
    parsed = extract_json_object(text)
    if parsed is None:
        return InvestmentMixParseResult("invalid_json", None)
    if set(parsed) != TOP_LEVEL_KEYS:
        return InvestmentMixParseResult("invalid_llm_output", None)
    summary = parsed.get("summary")
    raw_findings = parsed.get("top_findings")
    raw_actions = parsed.get("what_to_check_next")
    raw_anti_claims = parsed.get("anti_claims")
    if (
        not isinstance(summary, str)
        or not summary.strip()
        or len(summary) > 1000
        or NUMERIC_PATTERN.search(summary)
    ):
        logger.warning("Missing or invalid 'summary' in LLM response")
        return InvestmentMixParseResult("invalid_llm_output", None)
    if (
        not isinstance(raw_findings, list)
        or len(raw_findings) > 10
        or not valid_confidence(parsed.get("confidence"))
    ):
        return InvestmentMixParseResult("invalid_llm_output", None)
    findings = [
        parse_finding(
            item,
            theme_shares_pct=theme_shares_pct or {},
            subcategory_shares_pct=subcategory_shares_pct or {},
            quality_mean=fallback_mean,
            quality_band=fallback_quality_band,
        )
        for item in raw_findings
    ]
    if (
        any(finding is None for finding in findings)
        or not isinstance(raw_actions, list)
        or len(raw_actions) > 10
    ):
        return InvestmentMixParseResult("invalid_llm_output", None)
    actions = [parse_action(item) for item in raw_actions]
    if isinstance(raw_anti_claims, list) and any(
        not isinstance(claim, str) or len(claim) > 300 for claim in raw_anti_claims
    ):
        return InvestmentMixParseResult("invalid_llm_output", None)
    anti_claims = string_list(raw_anti_claims)
    if (
        any(action is None for action in actions)
        or not isinstance(raw_anti_claims, list)
        or len(raw_anti_claims) > 10
        or any(
            len(claim) > 300 or NUMERIC_PATTERN.search(claim) for claim in anti_claims
        )
        or len(anti_claims) != len(raw_anti_claims)
    ):
        return InvestmentMixParseResult("invalid_llm_output", None)
    typed_findings = [finding for finding in findings if finding is not None]
    typed_actions = [action for action in actions if action is not None]
    output: InvestmentMixExplainOutput = {
        "summary": summary.strip(),
        "top_findings": typed_findings,
        "confidence": {
            "level": fallback_level,
            "quality_mean": fallback_mean,
            "quality_stddev": fallback_stddev,
            "band_mix": fallback_band_mix or {},
            "drivers": fallback_drivers or [],
        },
        "what_to_check_next": typed_actions,
        "anti_claims": anti_claims,
        "status": "valid",
    }
    narrative = [output["summary"], *[item["finding"] for item in typed_findings]]
    for action in typed_actions:
        narrative.extend([action["action"], action["why"], action["where"]])
    narrative.extend(anti_claims)
    if contains_forbidden_language(" ".join(narrative)):
        logger.warning("LLM response contains forbidden language")
        return InvestmentMixParseResult("forbidden_language", None)
    return InvestmentMixParseResult("valid", output)
