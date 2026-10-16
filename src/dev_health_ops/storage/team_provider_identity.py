"""Read the provider identity of stored teams (CHAOS-9432).

``teams`` is a ReplacingMergeTree(updated_at): the newest version of a team
wins, so ``provider_team_id``, ``provider_name`` and ``gone_at`` survive only if
every writer copies them onto the version it inserts. This is the one read the
Python writers use; it mirrors ``internal/teamprovider`` (Go): the first two are
the newest NON-EMPTY values over every stored version, ``gone_at`` is the newest
version's (``argMax`` skips a NULL argument, so a NULL is read through a zero
time).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

PROVIDER_IDENTITY_COLUMNS = ["provider_team_id", "provider_name", "gone_at"]

_QUERY = (
    "SELECT org_id, id, "
    "argMaxIf(provider_team_id, (updated_at, last_synced), provider_team_id != ''), "
    "argMaxIf(provider_name, (updated_at, last_synced), provider_name != ''), "
    "nullIf(argMax(ifNull(gone_at, toDateTime64(0, 6)), "
    "(updated_at, last_synced)), toDateTime64(0, 6)) "
    "FROM teams WHERE org_id IN {org_ids:Array(String)} "
    "AND id IN {ids:Array(String)} GROUP BY org_id, id"
)


def fetch_team_provider_identity(
    client: Any, pairs: Iterable[tuple[str, str]]
) -> dict[tuple[str, str], tuple[str, str, Any]]:
    """Return ``{(org_id, id): (provider_team_id, provider_name, gone_at)}``
    for the teams that already have a stored version; a team with none has no
    entry (callers write ``('', '', None)``)."""
    pairs = list(pairs)
    org_ids = sorted({str(org_id) for org_id, _ in pairs})
    ids = sorted({str(team_id) for _, team_id in pairs})
    if not ids:
        return {}
    result = client.query(_QUERY, parameters={"org_ids": org_ids, "ids": ids})
    return {
        (str(org_id), str(team_id)): (
            str(provider_team_id or ""),
            str(provider_name or ""),
            gone_at,
        )
        for org_id, team_id, provider_team_id, provider_name, gone_at in (
            result.result_rows or []
        )
    }
