from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TypedDict

PROMPT_PATH = (
    Path(__file__).parent.parent / "prompts" / "investment_mix_explain_prompt.txt"
)
logger = logging.getLogger(__name__)


class FindingEvidence(TypedDict):
    theme: str
    subcategory: Optional[str]
    share_pct: float
    delta_pct_points: Optional[float]
    evidence_quality_mean: Optional[float]
    evidence_quality_band: Optional[str]


class Finding(TypedDict):
    finding: str
    evidence: FindingEvidence


class Confidence(TypedDict):
    level: Literal["high", "moderate", "low", "unknown"]
    quality_mean: Optional[float]
    quality_stddev: Optional[float]
    band_mix: Dict[str, int]
    drivers: List[str]


class ActionItem(TypedDict):
    action: str
    why: str
    where: str


class InvestmentMixExplainOutput(TypedDict):
    summary: str
    top_findings: List[Finding]
    confidence: Confidence
    what_to_check_next: List[ActionItem]
    anti_claims: List[str]
    status: Optional[Literal["valid", "invalid_json", "invalid_llm_output"]]


_FORBIDDEN_WORDS = (" should ", " should.", " should,", " determined ", " detected ")
_ABSOLUTELY_FORBIDDEN = ("definitely", "certainly", "undoubtedly", "without question")


def load_prompt() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        return ""


def build_prompt(*, base_prompt: str, payload: Dict[str, Any]) -> str:
    return (
        base_prompt.rstrip()
        + "\n\n---\nPRECOMPUTED DATA (do not recalculate):\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n---\n"
        + "\nOutput must be valid JSON."
    )


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text or not text.strip():
        logger.warning("LLM response is empty or whitespace-only")
        return None

    candidate = text.strip()

    start = candidate.find("{")
    end = candidate.rfind("}")

    if start == -1 or end == -1 or end < start:
        safe_preview = text[:500].replace("\r", "\\r").replace("\n", "\\n")
        logger.warning(
            "Failed to find JSON object in LLM response. "
            "Preview of text (%d chars shown, total %d): %r",
            len(safe_preview),
            len(text),
            safe_preview,
        )
        return None

    json_str = candidate[start : end + 1]

    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError as e:
        safe_preview = json_str[:500].replace("\r", "\\r").replace("\n", "\\n")
        logger.warning(
            "JSON decode error in LLM response: %s. Text preview (%d chars shown, total %d): %r",
            e,
            len(safe_preview),
            len(json_str),
            safe_preview,
        )
        return None
    if not isinstance(parsed, dict):
        logger.warning("Parsed JSON is not a dictionary")
        return None
    return parsed


def _as_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
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


def _parse_finding(raw: Any) -> Optional[Finding]:
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


def _parse_action_item(raw: Any) -> Optional[ActionItem]:
    if not isinstance(raw, dict):
        return None
    action = raw.get("action")
    why = raw.get("why")
    where = raw.get("where")
    if not all(isinstance(x, str) and x.strip() for x in (action, why, where)):
        return None
    return {"action": action.strip(), "why": why.strip(), "where": where.strip()}


def _parse_confidence(
    raw: Any,
    fallback_band_mix: Dict[str, int],
    fallback_drivers: List[str],
    fallback_mean: Optional[float],
    fallback_stddev: Optional[float],
) -> Confidence:
    if not isinstance(raw, dict):
        return {
            "level": "unknown",
            "quality_mean": fallback_mean,
            "quality_stddev": fallback_stddev,
            "band_mix": fallback_band_mix,
            "drivers": fallback_drivers,
        }
    level = raw.get("level")
    if level not in ("high", "moderate", "low", "unknown"):
        level = "unknown"
    return {
        "level": level,
        "quality_mean": float(raw["quality_mean"])
        if isinstance(raw.get("quality_mean"), (int, float))
        else fallback_mean,
        "quality_stddev": float(raw["quality_stddev"])
        if isinstance(raw.get("quality_stddev"), (int, float))
        else fallback_stddev,
        "band_mix": raw.get("band_mix")
        if isinstance(raw.get("band_mix"), dict)
        else fallback_band_mix,
        "drivers": _as_string_list(raw.get("drivers")) or fallback_drivers,
    }


def parse_and_validate_response(
    text: str,
    *,
    fallback_band_mix: Optional[Dict[str, int]] = None,
    fallback_drivers: Optional[List[str]] = None,
    fallback_mean: Optional[float] = None,
    fallback_stddev: Optional[float] = None,
) -> Optional[InvestmentMixExplainOutput]:
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
    top_findings: List[Finding] = []
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
    what_to_check_next: List[ActionItem] = []
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
