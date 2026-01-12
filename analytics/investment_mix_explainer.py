from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict


PROMPT_PATH = Path(__file__).with_name("investment_mix_explain_prompt.txt")
logger = logging.getLogger(__name__)


class InvestmentMixExplainOutput(TypedDict):
    summary: str
    dominant_themes: List[str]
    key_drivers: List[str]
    operational_signals: List[str]
    confidence_note: str


_FORBIDDEN_WORDS = (" should ", " should.", " should,", " determined ", " detected ")


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
    )


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    candidate = text.strip()

    start = candidate.find("{")
    end = candidate.rfind("}")

    if start == -1 or end == -1 or end < start:
        logger.warning(f"Failed to find JSON object in LLM response. Text: {text[:500]}...")
        return None

    json_str = candidate[start : end + 1]

    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON decode error in LLM response: {e}. Text: {json_str[:500]}...")
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
    return any(token in lowered for token in _FORBIDDEN_WORDS)


def parse_and_validate_response(text: str) -> Optional[InvestmentMixExplainOutput]:
    parsed = _extract_json_object(text)
    if not parsed:
        return None

    summary = parsed.get("summary")
    confidence_note = parsed.get("confidence_note")
    if not isinstance(summary, str) or not summary.strip():
        logger.warning("Missing or empty 'summary' in LLM response")
        return None
    if not isinstance(confidence_note, str) or not confidence_note.strip():
        logger.warning("Missing or empty 'confidence_note' in LLM response")
        return None

    output: InvestmentMixExplainOutput = {
        "summary": summary.strip(),
        "dominant_themes": _as_string_list(parsed.get("dominant_themes")),
        "key_drivers": _as_string_list(parsed.get("key_drivers")),
        "operational_signals": _as_string_list(parsed.get("operational_signals")),
        "confidence_note": confidence_note.strip(),
    }

    all_text = " ".join(
        [
            output["summary"],
            output["confidence_note"],
            " ".join(output["dominant_themes"]),
            " ".join(output["key_drivers"]),
            " ".join(output["operational_signals"]),
        ]
    )
    if _contains_forbidden_language(all_text):
        logger.warning("LLM response contains forbidden language")
        return None

    return output

