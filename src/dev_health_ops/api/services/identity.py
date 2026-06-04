"""Shared server-side identity resolution (Framework A7 / A8 / B7).

Resolves stable scope ids (``repo_id`` / ``team_id``) to human-readable
display names by querying the ``repos`` / ``teams`` analytics tables. This is
the same resolver contract the cockpit conclusion uses (CHAOS-2064) — extracted
here so Govern surfaces (Incident Correlation contributors, Change-Failure
associations, Coverage-by-Repository) can resolve identity server-side instead
of leaking ``#hex UNRESOLVED`` labels to the client.

Rules:
- Resolution happens server-side; ids are stable, display names are human.
- A bare UUID is never a valid display name (A8). Callers must apply a
  controlled fallback rather than surfacing the raw id.
- Best-effort: on any query failure an empty map is returned so callers fall
  back deterministically.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Literal

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from ..queries.client import query_dicts

logger = logging.getLogger(__name__)

ScopeKind = Literal["repo", "team"]

_BARE_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def looks_like_uuid(value: str | None) -> bool:
    """Return True when *value* is a bare UUID that must not appear as a label (A8)."""
    if not value:
        return False
    return bool(_BARE_UUID_RE.match(value.strip()))


async def resolve_scope_display_names(
    sink: BaseMetricsSink,
    *,
    org_id: str,
    scope: ScopeKind,
    ids: list[str],
) -> dict[str, str]:
    """Return ``{scope_id: display_name}`` for the given ids.

    Queries the ``repos`` (``repo`` slug) or ``teams`` (``name``) table. Only
    non-UUID display names are kept — a row that would resolve to a bare UUID is
    omitted so callers apply a controlled fallback rather than surfacing the raw
    id (A8 / B7). Best-effort: returns an empty dict on any failure.
    """
    unique_ids = sorted({str(i) for i in ids if i})
    if not unique_ids:
        return {}

    if scope == "repo":
        sql = """
            SELECT toString(id) AS scope_id, repo AS display_name
            FROM repos
            WHERE org_id = {org_id:String}
              AND toString(id) IN {scope_ids:Array(String)}
        """
        params: dict[str, Any] = {"org_id": org_id, "scope_ids": unique_ids}
    else:
        sql = """
            SELECT toString(id) AS scope_id, name AS display_name
            FROM teams
            WHERE org_id = {org_id:String}
              AND toString(id) IN {scope_ids:Array(String)}
        """
        params = {"org_id": org_id, "scope_ids": unique_ids}

    try:
        rows = await query_dicts(sink, sql, params)
    except Exception:
        logger.warning(
            "Could not resolve %s display names for ids=%s", scope, unique_ids
        )
        return {}

    resolved: dict[str, str] = {}
    for row in rows:
        scope_id = str(row.get("scope_id") or "")
        display_name = row.get("display_name")
        if not scope_id or not display_name:
            continue
        display_name = str(display_name).strip()
        # A8: a bare UUID is not a human label — omit so caller falls back.
        if not display_name or looks_like_uuid(display_name):
            continue
        resolved[scope_id] = display_name
    return resolved


def scope_kind_for_group_by(group_by: str) -> ScopeKind:
    """Map an explain/driver ``group_by`` column to a resolver scope kind."""
    return "team" if group_by == "team_id" else "repo"
