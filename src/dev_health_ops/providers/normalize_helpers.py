"""Shared shape-agnostic helpers used by provider normalize modules.

These helpers accept raw API payloads that may be either ``dict``-shaped
(GraphQL / REST JSON) or object-shaped (PyGithub / python-gitlab / linear
mock-classes) and coerce them into predictable Python types.

Jira's ``_get_field`` descends into ``.fields`` specifically and is NOT
duplicated here — it stays in ``providers.jira.normalize``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

__all__ = [
    "as_dict",
    "as_str",
    "as_int",
    "as_node_list",
    "labels_from_nodes",
    "get_attr",
    "get_nested",
]


class _Named(Protocol):
    name: object


def as_dict(value: object) -> dict[str, object]:
    """Return ``value`` if it is a dict, else an empty dict."""
    return value if isinstance(value, dict) else {}


def as_str(value: object) -> str | None:
    """Coerce ``value`` to a string. ``None`` stays ``None``."""
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def as_int(value: object) -> int | None:
    """Coerce ``value`` to an int. Bool -> 0/1. Non-numeric returns ``None``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    return None


def as_node_list(value: object) -> list[dict[str, object]]:
    """Return ``value`` as a list of dicts (filtering non-dicts)."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def labels_from_nodes(
    nodes: Sequence[Mapping[str, object] | _Named] | None,
) -> list[str]:
    """Extract ``name`` strings from a sequence of dicts or objects."""
    labels: list[str] = []
    for node in nodes or []:
        name = (
            (node or {}).get("name")
            if isinstance(node, dict)
            else getattr(node, "name", None)
        )
        if name:
            labels.append(str(name))
    return labels


def get_attr(obj: Any, key: str) -> Any:
    """Single-level lookup: ``dict.get(key)`` or ``getattr(obj, key, None)``."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def get_nested(obj: Any, *keys: str) -> Any:
    """Walk nested dict/object keys, short-circuiting on ``None``.

    ``get_nested(issue, "assignee", "email")`` is equivalent to
    ``issue.get("assignee", {}).get("email")`` or ``issue.assignee.email``,
    whichever applies at each level.
    """
    for key in keys:
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            obj = getattr(obj, key, None)
    return obj
