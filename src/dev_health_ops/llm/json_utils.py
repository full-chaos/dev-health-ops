"""Shared JSON parsing helpers for LLM outputs.

LLM responses are often near-JSON but may contain surrounding prose or
whitespace. ``validate_json_or_empty`` is a strict gate used by
structured-output validators; ``extract_json_object`` is a recovery
helper that finds the first top-level ``{...}`` block and parses it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["validate_json_or_empty", "extract_json_object"]


def validate_json_or_empty(text: str) -> str:
    """Return a compact JSON string if ``text`` parses as JSON, else empty.

    Whitespace-only and empty inputs return ``""``. Invalid JSON also
    returns ``""`` (no logging — used in hot OpenAI validation path).
    """
    if not text or not text.strip():
        return ""
    try:
        obj = json.loads(text)
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""


def extract_json_object(text: str) -> dict[str, Any] | None:
    """Extract and parse the first top-level JSON object from ``text``.

    Logs a warning when extraction fails. Returns ``None`` when:
      - ``text`` is empty or whitespace
      - no balanced ``{...}`` block is found
      - the block is not valid JSON
      - the parsed value is not a dict (arrays, scalars return None)
    """
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
    except json.JSONDecodeError as exc:
        safe_preview = json_str[:500].replace("\r", "\\r").replace("\n", "\\n")
        logger.warning(
            "JSON decode error in LLM response: %s. "
            "Text preview (%d chars shown, total %d): %r",
            exc,
            len(safe_preview),
            len(json_str),
            safe_preview,
        )
        return None

    if not isinstance(parsed, dict):
        logger.warning("Parsed JSON is not a dictionary")
        return None
    return parsed
