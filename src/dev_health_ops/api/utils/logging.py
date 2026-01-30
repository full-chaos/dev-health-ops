"""Logging utilities for safe log output.

Provides sanitization functions to prevent log injection attacks
by removing control characters from user-controlled values.
"""

from __future__ import annotations

from typing import Any


def sanitize_for_log(value: Any, max_length: int = 1000) -> Any:
    """Sanitize a value for safe logging.

    Prevents log injection by removing CR/LF and control characters.
    Recursively sanitizes dicts, lists, tuples, and sets.
    Truncates strings longer than max_length.

    Args:
        value: The value to sanitize.
        max_length: Maximum string length before truncation.

    Returns:
        Sanitized value safe for logging.
    """

    def clean_string(text: str) -> str:
        cleaned = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
        cleaned = "".join(ch for ch in cleaned if ch >= " " and ch != "\x7f")
        if len(cleaned) > max_length:
            return cleaned[:max_length] + "...[truncated]"
        return cleaned

    if value is None:
        return ""

    if isinstance(value, str):
        return clean_string(value)

    if isinstance(value, dict):
        return {
            clean_string(str(k)): sanitize_for_log(v, max_length)
            for k, v in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [sanitize_for_log(elem, max_length) for elem in value]

    return clean_string(str(value))
