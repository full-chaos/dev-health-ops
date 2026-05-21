from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.bus_factor import resolve_bus_factor
from dev_health_ops.api.graphql.types.bus_factor import BusFactorScopeInput
from dev_health_ops.metrics.loaders.ownership import OwnershipWindow
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


def _patch_loader(window: OwnershipWindow) -> Any:
    return patch(
        "dev_health_ops.metrics.loaders.ownership."
        "OwnershipClickHouseLoader.load_commit_ownership_stats",
        new_callable=AsyncMock,
        return_value=window,
    )


@pytest.mark.asyncio
async def test_bus_factor_empty_state_returns_stable_contract():
    with _patch_loader(OwnershipWindow(stats=[], repo_names={})):
        result = await resolve_bus_factor(_ctx(), ORG_ID)

    assert result.org_id == ORG_ID
    assert result.value == 0
    assert result.evidence_sample_count == 0
    assert result.top_maintainers == []
    assert result.repos == []


@pytest.mark.asyncio
async def test_bus_factor_populates_scope_and_repo_rollups():
    window = OwnershipWindow(
        stats=[
            _row(REPO_A, "a1", "maintainer-a@example.com", 80),
            _row(REPO_A, "a2", "maintainer-b@example.com", 20),
            _row(REPO_B, "b1", "maintainer-c@example.com", 90),
            _row(REPO_B, "b2", "maintainer-a@example.com", 10),
        ],
        repo_names={REPO_A: "backend", REPO_B: "frontend"},
    )

    with _patch_loader(window):
        result = await resolve_bus_factor(_ctx(), ORG_ID)

    assert result.value == 2
    assert result.evidence_sample_count == 4
    assert [share.author for share in result.top_maintainers[:2]] == [
        "maintainer-a@example.com",
        "maintainer-c@example.com",
    ]
    assert [repo.repo_name for repo in result.repos] == ["backend", "frontend"]
    assert [repo.value for repo in result.repos] == [1, 1]
    assert result.repos[0].top_maintainers[0].author == "maintainer-a@example.com"
    assert result.repos[1].top_maintainers[0].author == "maintainer-c@example.com"


@pytest.mark.asyncio
async def test_bus_factor_passes_scope_to_loader():
    window = OwnershipWindow(
        stats=[_row(REPO_A, "a1", "maintainer-a@example.com", 100)],
        repo_names={REPO_A: "backend"},
    )

    with _patch_loader(window) as loader:
        result = await resolve_bus_factor(
            _ctx(), ORG_ID, BusFactorScopeInput(repo_id=str(REPO_A), team_id="team-a")
        )

    loader.assert_awaited_once_with(repo_id=REPO_A, team_id="team-a")
    assert result.scope.repo_id == str(REPO_A)
    assert result.scope.team_id == "team-a"
    assert result.value == 1
