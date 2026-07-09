from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from dev_health_ops.llm.json_utils import extract_json_object as _extract_json_object
from dev_health_ops.llm.providers.openai import (
    INVESTMENT_MIX_RESPONSE_FORMAT,
    RESPONSE_FORMAT_MARKER,
)

from .investment_mix_parser import (
    ActionItem,
    Confidence,
    Finding,
    FindingEvidence,
    InvestmentMixExplainOutput,
    InvestmentMixParseResult,
    ParseStatus,
    parse_investment_mix_response,
)

PROMPT_PATH = (
    Path(__file__).parent.parent / "prompts" / "investment_mix_explain_prompt.txt"
)
PROMPT_VERSION = "investment-mix-explain-v2"

__all__ = [
    "PROMPT_VERSION",
    "ActionItem",
    "Confidence",
    "Finding",
    "FindingEvidence",
    "InvestmentMixExplainOutput",
    "InvestmentMixParseResult",
    "ParseStatus",
    "_extract_json_object",
    "build_prompt",
    "load_prompt",
    "parse_and_validate_response",
    "parse_investment_mix_response",
]


def load_prompt() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except OSError:
        return ""


def build_prompt(*, base_prompt: str, payload: dict[str, Any]) -> str:
    return (
        f"{RESPONSE_FORMAT_MARKER}{INVESTMENT_MIX_RESPONSE_FORMAT}\n"
        + base_prompt.rstrip()
        + "\n\n---\nPRECOMPUTED DATA (do not recalculate):\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n---\n\nOutput must be valid JSON."
    )


def parse_and_validate_response(
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
) -> InvestmentMixExplainOutput | None:
    return parse_investment_mix_response(
        text,
        theme_shares_pct=theme_shares_pct,
        subcategory_shares_pct=subcategory_shares_pct,
        fallback_level=fallback_level,
        fallback_quality_band=fallback_quality_band,
        fallback_band_mix=fallback_band_mix,
        fallback_drivers=fallback_drivers,
        fallback_mean=fallback_mean,
        fallback_stddev=fallback_stddev,
    ).output
