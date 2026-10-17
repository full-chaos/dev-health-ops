"""Read the anchor of stored repositories (CHAOS-3383).

``repos`` is a ReplacingMergeTree(last_synced): the newest version of a
repository wins, so ``provider_repo_id`` (migration 134) and ``identity_path``
(migration 138) survive only if every writer copies them onto the version it
inserts. The Go writers carry them under their stored-version contracts; this
is the one read the Python writer uses. Each is the newest NON-EMPTY value over
every stored version of the key.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

REPO_ANCHOR_COLUMNS = ["provider_repo_id", "identity_path"]

_QUERY = (
    "SELECT org_id, toString(id), "
    "argMaxIf(provider_repo_id, last_synced, provider_repo_id != ''), "
    "argMaxIf(identity_path, last_synced, identity_path != '') "
    "FROM repos WHERE org_id IN {org_ids:Array(String)} "
    "AND toString(id) IN {ids:Array(String)} GROUP BY org_id, id"
)


def fetch_repo_anchor(
    client: Any, pairs: Iterable[tuple[str, str]]
) -> dict[tuple[str, str], tuple[str, str]]:
    """Return ``{(org_id, id): (provider_repo_id, identity_path)}`` for the
    repositories that already have a stored version; a repository with none
    has no entry (callers write ``('', '')``)."""
    pairs = list(pairs)
    org_ids = sorted({str(org_id) for org_id, _ in pairs})
    ids = sorted({str(repo_id) for _, repo_id in pairs})
    if not ids:
        return {}
    result = client.query(_QUERY, parameters={"org_ids": org_ids, "ids": ids})
    return {
        (str(org_id), str(repo_id)): (
            str(provider_repo_id or ""),
            str(identity_path or ""),
        )
        for org_id, repo_id, provider_repo_id, identity_path in (
            result.result_rows or []
        )
    }
