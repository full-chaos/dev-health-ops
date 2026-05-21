"""Bus Factor GraphQL resolver."""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from typing import Any, cast

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.knowledge import compute_bus_factor
from dev_health_ops.metrics.query_builder import OrgScopedQuery
from dev_health_ops.metrics.schemas import CommitStatRow

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.bus_factor import (
    BusFactorMaintainer,
    BusFactorRepoResult,
    BusFactorResult,
    BusFactorScopeInput,
)

logger = logging.getLogger(__name__)


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for Bus Factor resolver")
    return context.client


def _parse_repo_id(value: str | None) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except (TypeError, ValueError) as exc:
        logger.debug("Invalid repoId %r in Bus Factor scope: %s", value, exc)
        return None


def _author_identity(row: CommitStatRow) -> str:
    return row.get("author_email") or row.get("author_name") or "unknown"


def _churn(row: CommitStatRow) -> int:
    return (row.get("additions") or 0) + (row.get("deletions") or 0)


def _top_maintainers(
    rows: list[CommitStatRow], *, limit: int
) -> list[BusFactorMaintainer]:
    author_churn: dict[str, int] = defaultdict(int)
    total_churn = 0
    for row in rows:
        churn = _churn(row)
        if churn <= 0:
            continue
        author_churn[_author_identity(row)] += churn
        total_churn += churn

    if total_churn == 0:
        return []

    return [
        BusFactorMaintainer(author=author, share_percent=(churn / total_churn) * 100.0)
        for author, churn in sorted(
            author_churn.items(), key=lambda item: item[1], reverse=True
        )[:limit]
    ]


def _scope_rows(rows: list[CommitStatRow], scope_id: uuid.UUID) -> list[CommitStatRow]:
    return [cast(CommitStatRow, {**row, "repo_id": scope_id}) for row in rows]


def _bus_factor(repo_id: uuid.UUID, rows: list[CommitStatRow]) -> int:
    return compute_bus_factor(str(repo_id), rows, threshold_percent=0.8)


async def _load_commit_ownership_stats(
    client: Any,
    *,
    org_id: str,
    repo_id: uuid.UUID | None,
) -> tuple[list[CommitStatRow], dict[uuid.UUID, str]]:
    scope = OrgScopedQuery(org_id)
    params: dict[str, Any] = {}
    filters: list[str] = []
    if repo_id is not None:
        params["repo_id"] = str(repo_id)
        filters.append("gc.repo_id = {repo_id:UUID}")

    params = scope.inject(params)
    where_clause = f"AND {' AND '.join(filters)}" if filters else ""

    rows = await query_dicts(
        client,
        f"""
        SELECT
            gc.repo_id AS repo_id,
            coalesce(r.repo, toString(gc.repo_id)) AS repo_name,
            gcs.commit_hash AS commit_hash,
            gc.author_email AS author_email,
            gc.author_name AS author_name,
            gc.committer_when AS committer_when,
            gcs.file_path AS file_path,
            gcs.additions AS additions,
            gcs.deletions AS deletions,
            gcs.old_file_mode AS old_file_mode,
            gcs.new_file_mode AS new_file_mode
        FROM git_commit_stats AS gcs
        INNER JOIN git_commits AS gc
            ON gc.repo_id = gcs.repo_id
           AND gc.hash = gcs.commit_hash
           AND gc.org_id = gcs.org_id
        LEFT JOIN repos AS r
            ON r.id = gc.repo_id
           AND r.org_id = gc.org_id
        WHERE 1 = 1
          {scope.filter(alias="gc")}
          {scope.filter(alias="gcs")}
          {where_clause}
        """,
        params,
    )

    stats: list[CommitStatRow] = []
    repo_names: dict[uuid.UUID, str] = {}
    for row in rows:
        row_repo_id = row.get("repo_id")
        if row_repo_id is None:
            continue
        parsed_repo_id = uuid.UUID(str(row_repo_id))
        repo_names[parsed_repo_id] = str(row.get("repo_name") or parsed_repo_id)
        stats.append(
            {
                "repo_id": parsed_repo_id,
                "commit_hash": str(row.get("commit_hash") or ""),
                "author_email": row.get("author_email"),
                "author_name": row.get("author_name"),
                "committer_when": row["committer_when"],
                "file_path": row.get("file_path"),
                "additions": int(row.get("additions") or 0),
                "deletions": int(row.get("deletions") or 0),
                "old_file_mode": row.get("old_file_mode"),
                "new_file_mode": row.get("new_file_mode"),
            }
        )

    return stats, repo_names


async def resolve_bus_factor(
    context: GraphQLContext,
    org_id: str,
    scope: BusFactorScopeInput | None = None,
) -> BusFactorResult:
    """Resolve Bus Factor for an organization or a single repository."""

    authorized_org_id = require_org_id(context)
    if org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            org_id,
            authorized_org_id,
        )

    repo_id = _parse_repo_id(scope.repo_id if scope else None)
    stats, repo_names = await _load_commit_ownership_stats(
        _require_client(context), org_id=authorized_org_id, repo_id=repo_id
    )

    by_repo: dict[uuid.UUID, list[CommitStatRow]] = defaultdict(list)
    for row in stats:
        by_repo[row["repo_id"]].append(row)

    per_repo = [
        BusFactorRepoResult(
            repo_id=str(current_repo_id),
            repo_name=repo_names.get(current_repo_id, str(current_repo_id)),
            value=_bus_factor(current_repo_id, repo_rows),
            top_maintainers=_top_maintainers(repo_rows, limit=5),
            evidence_sample_count=len(repo_rows),
        )
        for current_repo_id, repo_rows in by_repo.items()
    ]
    per_repo.sort(key=lambda item: (item.value, item.repo_name))

    scope_repo_id = repo_id or uuid.uuid5(
        uuid.NAMESPACE_URL, f"dev-health:bus-factor:{authorized_org_id}:all"
    )
    scoped_rows = stats if repo_id else _scope_rows(stats, scope_repo_id)

    return BusFactorResult(
        scope_value=_bus_factor(scope_repo_id, scoped_rows),
        top_maintainers=_top_maintainers(stats, limit=5),
        per_repo=per_repo,
        evidence_sample_count=len(stats),
    )
