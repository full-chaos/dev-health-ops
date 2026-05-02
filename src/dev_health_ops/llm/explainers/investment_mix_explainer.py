from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Literal, TypedDict

from dev_health_ops.llm.json_utils import extract_json_object as _extract_json_object

PROMPT_PATH = (
    Path(__file__).parent.parent / "prompts" / "investment_mix_explain_prompt.txt"
)
logger = logging.getLogger(__name__)


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


_FORBIDDEN_WORDS = (" should ", " should.", " should,", " determined ", " detected ")
_ABSOLUTELY_FORBIDDEN = ("definitely", "certainly", "undoubtedly", "without question")


def load_prompt() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        return ""


def build_prompt(*, base_prompt: str, payload: dict[str, Any]) -> str:
    return (
        base_prompt.rstrip()
        + "\n\n---\nPRECOMPUTED DATA (do not recalculate):\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n---\n"
        + "\nOutput must be valid JSON."
    )


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def _contains_forbidden_language(text: str) -> bool:
    lowered = f" {text.lower()} "
    if any(token in lowered for token in _FORBIDDEN_WORDS):
        return True
    if any(word in lowered for word in _ABSOLUTELY_FORBIDDEN):
        return True
    return False


def _parse_finding(raw: Any) -> Finding | None:
    if not isinstance(raw, dict):
        return None
    finding_text = raw.get("finding")
    if not isinstance(finding_text, str) or not finding_text.strip():
        return None
    evidence_raw = raw.get("evidence")
    if not isinstance(evidence_raw, dict):
        return None
    theme = evidence_raw.get("theme")
    if not isinstance(theme, str) or not theme.strip():
        return None
    share_pct = evidence_raw.get("share_pct")
    if not isinstance(share_pct, (int, float)):
        share_pct = 0.0
    return {
        "finding": finding_text.strip(),
        "evidence": {
            "theme": theme.strip(),
            "subcategory": evidence_raw.get("subcategory")
            if isinstance(evidence_raw.get("subcategory"), str)
            else None,
            "share_pct": float(share_pct),
            "delta_pct_points": float(evidence_raw["delta_pct_points"])
            if isinstance(evidence_raw.get("delta_pct_points"), (int, float))
            else None,
            "evidence_quality_mean": float(evidence_raw["evidence_quality_mean"])
            if isinstance(evidence_raw.get("evidence_quality_mean"), (int, float))
            else None,
            "evidence_quality_band": evidence_raw.get("evidence_quality_band")
            if isinstance(evidence_raw.get("evidence_quality_band"), str)
            else None,
        },
    }


def _parse_action_item(raw: Any) -> ActionItem | None:
    if not isinstance(raw, dict):
        return None
    action = raw.get("action")
    why = raw.get("why")
    where = raw.get("where")
    if not isinstance(action, str) or not action.strip():
        return None
    if not isinstance(why, str) or not why.strip():
        return None
    if not isinstance(where, str) or not where.strip():
        return None
    action_text = action.strip()
    why_text = why.strip()
    where_text = where.strip()
    return {"action": action_text, "why": why_text, "where": where_text}


def _parse_band_mix(raw: Any, fallback: dict[str, int]) -> dict[str, int]:
    if not isinstance(raw, dict):
        return fallback
    parsed: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or isinstance(value, bool):
            continue
        if isinstance(value, int):
            parsed[key] = value
    return parsed or fallback


def _parse_confidence(
    raw: Any,
    fallback_band_mix: dict[str, int],
    fallback_drivers: list[str],
    fallback_mean: float | None,
    fallback_stddev: float | None,
) -> Confidence:
    if not isinstance(raw, dict):
        return {
            "level": "unknown",
            "quality_mean": fallback_mean,
            "quality_stddev": fallback_stddev,
            "band_mix": fallback_band_mix,
            "drivers": fallback_drivers,
        }
    raw_level = raw.get("level")
    level: Literal["high", "moderate", "low", "unknown"]
    if raw_level in ("high", "moderate", "low", "unknown"):
        level = raw_level
    else:
        level = "unknown"
    return {
        "level": level,
        "quality_mean": float(raw["quality_mean"])
        if isinstance(raw.get("quality_mean"), (int, float))
        else fallback_mean,
        "quality_stddev": float(raw["quality_stddev"])
        if isinstance(raw.get("quality_stddev"), (int, float))
        else fallback_stddev,
        "band_mix": _parse_band_mix(raw.get("band_mix"), fallback_band_mix),
        "drivers": _as_string_list(raw.get("drivers")) or fallback_drivers,
    }


def parse_and_validate_response(
    text: str,
    *,
    fallback_band_mix: dict[str, int] | None = None,
    fallback_drivers: list[str] | None = None,
    fallback_mean: float | None = None,
    fallback_stddev: float | None = None,
) -> InvestmentMixExplainOutput | None:
    parsed = _extract_json_object(text)
    if not parsed:
        return None

    summary = parsed.get("summary")
    if isinstance(summary, dict):
        summary = summary.get("statement")

    if not isinstance(summary, str) or not summary.strip():
        logger.warning("Missing or empty 'summary' in LLM response")
        return None

    # Parse findings
    top_findings: list[Finding] = []
    for raw_finding in parsed.get("top_findings") or []:
        finding = _parse_finding(raw_finding)
        if finding:
            top_findings.append(finding)

    # Parse confidence
    confidence = _parse_confidence(
        parsed.get("confidence"),
        fallback_band_mix or {},
        fallback_drivers or [],
        fallback_mean,
        fallback_stddev,
    )

    # Parse action items
    what_to_check_next: list[ActionItem] = []
    for raw_action in parsed.get("what_to_check_next") or []:
        action = _parse_action_item(raw_action)
        if action:
            what_to_check_next.append(action)

    # Parse anti-claims
    anti_claims = _as_string_list(parsed.get("anti_claims"))

    output: InvestmentMixExplainOutput = {
        "summary": summary.strip(),
        "top_findings": top_findings,
        "confidence": confidence,
        "what_to_check_next": what_to_check_next,
        "anti_claims": anti_claims,
        "status": "valid",
    }

    # Check for forbidden language
    all_text_parts = [output["summary"]]
    for f in top_findings:
        all_text_parts.append(f["finding"])
    for a in what_to_check_next:
        all_text_parts.extend([a["action"], a["why"], a["where"]])
    all_text_parts.extend(anti_claims)

    if _contains_forbidden_language(" ".join(all_text_parts)):
        logger.warning("LLM response contains forbidden language")
        return None

    return output
