"""`dev-hops push export` provider registry -- stub-only in v1 (CHAOS-2700
brief decision 14).

Provider export helpers (``push export github``, ``push export gitlab``)
are explicitly out of scope for v1 (epic plan). This registry exists so the
subcommand is a real, tested extension point rather than being entirely
absent from ``--help``: any name not registered here falls through to a
"not implemented" message and exit 1, without needing changes to ``cli.py``
once a real provider export lands.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

#: name -> handler(ns) -> exit code. Empty in v1 -- every `push export
#: <name>` currently falls through to the "not implemented" message.
EXPORT_PROVIDERS: dict[str, Callable[[Any], int]] = {}


def register_export_provider(
    name: str,
) -> Callable[[Callable[[Any], int]], Callable[[Any], int]]:
    def _decorator(fn: Callable[[Any], int]) -> Callable[[Any], int]:
        EXPORT_PROVIDERS[name] = fn
        return fn

    return _decorator


__all__ = ["EXPORT_PROVIDERS", "register_export_provider"]
