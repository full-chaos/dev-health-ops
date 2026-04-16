"""Shared utilities for provider modules: env parsing, etc."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

__all__ = ["EnvSpec", "env_flag", "env_int", "read_env_spec"]

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


@dataclass(frozen=True)
class EnvSpec:
    """Declarative specification of env vars for a client's ``from_env``.

    ``required``: mapping of ``field_name -> ENV_VAR_NAME``. Missing or
        empty values cause ``read_env_spec`` to raise ``ValueError`` with
        ``missing_error`` as the message.
    ``optional``: mapping of ``field_name -> (ENV_VAR_NAME, default)``.
        Unset env vars fall back to ``default`` (may be ``None``).
    ``missing_error``: human-readable error message used when any
        required var is missing. Include the env var names so the error
        is actionable.
    """

    required: dict[str, str] = field(default_factory=dict)
    optional: dict[str, tuple[str, object]] = field(default_factory=dict)
    missing_error: str = "Required environment variables missing"


def read_env_spec(spec: EnvSpec) -> dict[str, object]:
    """Read env vars as declared by ``spec``.

    Raises ``ValueError(spec.missing_error)`` if any required var is
    missing or empty. Returns a dict suitable for passing as kwargs to
    the auth dataclass / client constructor.
    """
    result: dict[str, object] = {}
    for key, env_name in spec.required.items():
        value = os.getenv(env_name) or ""
        if not value:
            raise ValueError(spec.missing_error)
        result[key] = value
    for key, (env_name, default) in spec.optional.items():
        value = os.getenv(env_name)
        result[key] = value if value else default
    return result
