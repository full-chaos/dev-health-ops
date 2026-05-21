from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.bus_factor import resolve_bus_factor
from dev_health_ops.api.graphql.types.bus_factor import BusFactorScopeInput
from dev_health_ops.metrics.schemas import CommitStatRow

ORG_ID = "org-test"
REPO_A = UUID("11111111-1111-1111-1111-111111111111")
REPO_B = UUID("22222222-2222-2222-2222-222222222222")
NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)


def _ctx() -> GraphQLContext:
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/default")
    ctx.client = MagicMock()
    return ctx


def _row(repo_id: UUID, commit_hash: str, author: str, churn: int) -> CommitStatRow:
    return {
        "repo_id": repo_id,
        "commit_hash": commit_hash,
        "author_email": author,
        "author_name": author.split("@")[0],
        "committer_when": NOW,
        "file_path": f"src/{commit_hash}.py",
        "additions": churn,
        "deletions": 0,
        "old_file_mode": None,
        "new_file_mode": None,
    }


def _db_row(row: CommitStatRow, repo_name: str) -> dict[str, Any]:
    return {**row, "repo_name": repo_name}


def _patch_query(rows: list[dict[str, Any]]) -> Any:
    return patch(
        "dev_health_ops.api.graphql.resolvers.bus_factor.query_dicts",
        new_callable=AsyncMock,
        return_value=rows,
    )


@pytest.mark.asyncio
async def test_bus_factor_empty_state_returns_stable_contract():
    with _patch_query([]):
        result = await resolve_bus_factor(_ctx(), ORG_ID)

    assert result.scope_value == 0
    assert result.evidence_sample_count == 0
    assert result.top_maintainers == []
    assert result.per_repo == []


@pytest.mark.asyncio
async def test_bus_factor_populates_scope_and_repo_rollups():
    rows = [
        _db_row(_row(REPO_A, "a1", "maintainer-a@example.com", 80), "backend"),
        _db_row(_row(REPO_A, "a2", "maintainer-b@example.com", 20), "backend"),
        _db_row(_row(REPO_B, "b1", "maintainer-c@example.com", 90), "frontend"),
        _db_row(_row(REPO_B, "b2", "maintainer-a@example.com", 10), "frontend"),
    ]

    with _patch_query(rows):
        result = await resolve_bus_factor(_ctx(), ORG_ID)

    assert result.scope_value == 2
    assert result.evidence_sample_count == 4
    assert [share.author for share in result.top_maintainers[:2]] == [
        "maintainer-a@example.com",
        "maintainer-c@example.com",
    ]
    assert [repo.repo_name for repo in result.per_repo] == ["backend", "frontend"]
    assert [repo.value for repo in result.per_repo] == [1, 1]
    assert result.per_repo[0].top_maintainers[0].author == "maintainer-a@example.com"
    assert result.per_repo[1].top_maintainers[0].author == "maintainer-c@example.com"


@pytest.mark.asyncio
async def test_bus_factor_passes_repo_scope_to_loader():
    rows = [_db_row(_row(REPO_A, "a1", "maintainer-a@example.com", 100), "backend")]

    with _patch_query(rows) as query:
        result = await resolve_bus_factor(
            _ctx(), ORG_ID, BusFactorScopeInput(repo_id=str(REPO_A))
        )

    params = query.await_args.args[2]
    assert params["repo_id"] == str(REPO_A)
    assert result.scope_value == 1
