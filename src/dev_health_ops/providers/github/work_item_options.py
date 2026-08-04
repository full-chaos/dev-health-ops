"""Canonical runtime controls for the GitHub work-item producer."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dev_health_ops.providers.utils import env_flag, env_int

GITHUB_WORK_ITEM_RUNTIME_DEFAULTS: dict[str, bool | int] = {
    "fetch_comments": True,
    "fetch_milestones": True,
    "comments_limit": 500,
}


def canonical_github_work_item_runtime_options(
    options: Mapping[str, Any] | None,
) -> dict[str, bool | int]:
    """Return explicit, validated controls without consulting process env.

    Planner-managed units persist this result in ``IntegrationDataset.options``.
    The same function is also applied at adapter execution for legacy/in-flight
    units created before the durable planner repair ran.
    """
    source = options or {}
    result = dict(GITHUB_WORK_ITEM_RUNTIME_DEFAULTS)
    for name in ("fetch_comments", "fetch_milestones"):
        value = source.get(name)
        if value is None:
            continue
        if not isinstance(value, bool):
            raise ValueError(f"GitHub work-items {name} must be a boolean")
        result[name] = value
    comments_limit = source.get("comments_limit")
    if comments_limit is not None:
        if (
            isinstance(comments_limit, bool)
            or not isinstance(comments_limit, int)
            or comments_limit < 0
        ):
            raise ValueError("GitHub work-items comments_limit must be non-negative")
        result["comments_limit"] = comments_limit
    return result


def snapshot_github_work_item_runtime_options(
    options: Mapping[str, Any] | None,
) -> dict[str, bool | int]:
    """Freeze documented legacy env fallbacks at a durable Python boundary.

    Explicit persisted values always win. Environment is consulted only for a
    missing value; once this result is stored, later repairs and PATCH requests
    preserve the snapshot rather than re-reading process-local state.
    """
    source = dict(options or {})
    if source.get("fetch_comments") is None:
        source["fetch_comments"] = env_flag("GITHUB_FETCH_COMMENTS", True)
    if source.get("fetch_milestones") is None:
        source["fetch_milestones"] = env_flag("GITHUB_FETCH_MILESTONES", True)
    if source.get("comments_limit") is None:
        source["comments_limit"] = env_int("GITHUB_COMMENTS_LIMIT", 500)
    return canonical_github_work_item_runtime_options(source)
