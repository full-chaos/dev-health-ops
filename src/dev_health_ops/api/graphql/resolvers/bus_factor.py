"""Bus Factor GraphQL resolver."""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from typing import Any, cast

from dev_health_ops.metrics.knowledge import compute_bus_factor
from dev_health_ops.metrics.loaders.ownership import OwnershipClickHouseLoader
from dev_health_ops.metrics.schemas import CommitStatRow

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.bus_factor import (
    BusFactor,
    BusFactorScope,
    BusFactorScopeInput,
    MaintainerShare,
    RepoBusFactor,
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


def _top_maintainers(rows: list[CommitStatRow], *, limit: int) -> list[MaintainerShare]:
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
        MaintainerShare(author=author, share_percent=(churn / total_churn) * 100.0)
        for author, churn in sorted(
            author_churn.items(), key=lambda item: item[1], reverse=True
        )[:limit]
    ]


def _scope_rows(rows: list[CommitStatRow], scope_id: uuid.UUID) -> list[CommitStatRow]:
    return [cast(CommitStatRow, {**row, "repo_id": scope_id}) for row in rows]


def _bus_factor(repo_id: uuid.UUID, rows: list[CommitStatRow]) -> int:
    return compute_bus_factor(str(repo_id), rows, threshold_percent=0.8)


async def resolve_bus_factor(
    context: GraphQLContext,
    org_id: str,
    scope: BusFactorScopeInput | None = None,
) -> BusFactor:
    """Resolve Bus Factor for an organization, team, or repository scope."""

    authorized_org_id = require_org_id(context)
    if org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            org_id,
            authorized_org_id,
        )

    repo_id = _parse_repo_id(scope.repo_id if scope else None)
    team_id = scope.team_id if scope else None
    loader = OwnershipClickHouseLoader(
        _require_client(context), org_id=authorized_org_id
    )
    window = await loader.load_commit_ownership_stats(repo_id=repo_id, team_id=team_id)

    by_repo: dict[uuid.UUID, list[CommitStatRow]] = defaultdict(list)
    for row in window.stats:
        by_repo[row["repo_id"]].append(row)

    repos = [
        RepoBusFactor(
            repo_id=str(current_repo_id),
            repo_name=window.repo_names.get(current_repo_id, str(current_repo_id)),
            value=_bus_factor(current_repo_id, repo_rows),
            top_maintainers=_top_maintainers(repo_rows, limit=3),
            evidence_sample_count=len(repo_rows),
        )
        for current_repo_id, repo_rows in by_repo.items()
    ]
    repos.sort(key=lambda item: (item.value, item.repo_name))

    scope_repo_id = repo_id or uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"dev-health:bus-factor:{authorized_org_id}:{team_id or 'all'}",
    )
    scoped_rows = window.stats if repo_id else _scope_rows(window.stats, scope_repo_id)

    return BusFactor(
        org_id=authorized_org_id,
        scope=BusFactorScope(
            repo_id=str(repo_id) if repo_id else None, team_id=team_id
        ),
        value=_bus_factor(scope_repo_id, scoped_rows),
        top_maintainers=_top_maintainers(window.stats, limit=5),
        repos=repos,
        evidence_sample_count=len(window.stats),
    )
