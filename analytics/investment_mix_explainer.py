from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict


PROMPT_PATH = Path(__file__).with_name("investment_mix_explain_prompt.txt")


class InvestmentMixExplainOutput(TypedDict):
    summary: str
    dominant_themes: List[str]
    key_drivers: List[str]
    operational_signals: List[str]
    confidence_note: str


_FORBIDDEN_WORDS = (" should ", " should.", " should,", " is ", " was ", " determined ", " detected ")


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
    candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*```$", "", candidate)

    start = candidate.find("{")
    end = candidate.rfind("}")

    if start == -1 or end == -1 or end < start:
        return None

    json_str = candidate[start : end + 1]

    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
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
        return None
    if not isinstance(confidence_note, str) or not confidence_note.strip():
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
        return None

    return output

