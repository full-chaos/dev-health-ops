"""Shared utilities for provider modules: env parsing, etc."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

__all__ = ["env_flag", "env_int"]

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}


def env_flag(name: str, default: bool) -> bool:
    """Read a boolean environment variable.

    Truthy values (case-insensitive, whitespace-trimmed): ``1``, ``true``,
    ``yes``, ``on``. Falsy: ``0``, ``false``, ``no``, ``off``. Any other
    value (or unset) returns ``default``.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    return default


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable, falling back to ``default``.

    Logs a warning when the variable is set but not parseable as an int.
    """
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s value %r; falling back to %d", name, raw, default)
        return default
