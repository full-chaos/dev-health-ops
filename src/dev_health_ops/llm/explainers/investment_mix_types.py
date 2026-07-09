"""Typed investment-mix explanation and parser results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypedDict


class FindingEvidence(TypedDict):
    theme: str
    subcategory: str | None
    share_pct: float
    delta_pct_points: float | None
    evidence_quality_mean: float | None
    evidence_quality_band: str | None


class Finding(TypedDict):
    finding: str
    evidence: FindingEvidence


class Confidence(TypedDict):
    level: Literal["high", "moderate", "low", "unknown"]
    quality_mean: float | None
    quality_stddev: float | None
    band_mix: dict[str, int]
    drivers: list[str]


class ActionItem(TypedDict):
    action: str
    why: str
    where: str


class InvestmentMixExplainOutput(TypedDict):
    summary: str
    top_findings: list[Finding]
    confidence: Confidence
    what_to_check_next: list[ActionItem]
    anti_claims: list[str]
    status: (
        Literal["valid", "invalid_json", "invalid_llm_output", "llm_unavailable"] | None
    )


ParseStatus = Literal[
    "valid", "invalid_json", "invalid_llm_output", "forbidden_language"
]


@dataclass(frozen=True, slots=True)
class InvestmentMixParseResult:
    status: ParseStatus
    output: InvestmentMixExplainOutput | None
