"""Shared case decoding for live GitHub work-item row oracle pairs."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any


def object_from_case(value: Any) -> Any:
    """Build the PyGithub-shaped object consumed by production normalizers."""
    if isinstance(value, dict):
        return SimpleNamespace(
            **{
                key: (
                    datetime.fromisoformat(item.replace("Z", "+00:00"))
                    if key.endswith("_at") and isinstance(item, str)
                    else object_from_case(item)
                )
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return [object_from_case(item) for item in value]
    return value
