from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    ScopeResolutionOutcome,
)
from dev_health_ops.api.dev.scope_service import ScopeResolveRequest, TimeRangeRequest
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    ClickHouseWorkGraphNeighborSource,
    GraphDirection,
    WorkGraphNeighborsRequest,
    WorkGraphNeighborsService,
    WorkGraphRawEdge,
    WorkGraphRootRef,
)

ORG = "00000000-0000-0000-0000-000000000001"
NOW = datetime(2026, 7, 28, tzinfo=UTC)


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG,
        direct_scope=DirectScope.ISSUE,
        repositories=["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue-a",
                display_label="Issue A",
                repository_id="repo-a",
            )
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=NOW,
            timezone="UTC",
        ),
    )


def _request(**changes: Any) -> WorkGraphNeighborsRequest:
    values: dict[str, Any] = {
        "scope_request": ScopeResolveRequest(
            time_range=TimeRangeRequest(
                start_date=date(2026, 7, 1), end_date=date(2026, 7, 28)
            )
        ),
        "root_refs": (WorkGraphRootRef("issue", "issue-a"),),
        "relationship_types": ("blocks",),
        "limit": 1,
    }
    values.update(changes)
    return WorkGraphNeighborsRequest(**values)


class AllowEntitlement:
    calls = 0

    async def require(self, org_id: str) -> None:
        assert org_id == ORG
        type(self).calls += 1


class ExactAuthorizer:
    async def resolve_contract(self, org_id, fingerprint, request):
        assert org_id == ORG
        assert fingerprint == "permissions-a"
        return SimpleNamespace(
            resolved_scope=_scope(),
            outcome=ScopeResolutionOutcome.EXACT,
            warnings=[],
        )


class FakeSource:
    async def fetch(self, **kwargs):
        assert kwargs["org_id"] == ORG
        assert kwargs["direction"] is GraphDirection.BOTH
        return (
            WorkGraphRawEdge(
                "edge-b",
                "issue",
                "issue-a",
                "issue",
                "issue-c",
                "blocks",
                "repo-a",
                "native",
                1,
                NOW,
                "work_graph_edges",
                "work-graph-edges.v1",
                NOW,
                "Issue A",
                "Issue C",
            ),
            WorkGraphRawEdge(
                "edge-a",
                "issue",
                "issue-a",
                "issue",
                "issue-b",
                "blocks",
                "repo-a",
                "native",
                1,
                NOW,
                "work_graph_edges",
                "work-graph-edges.v1",
                NOW,
                "Issue A",
                "Issue B",
            ),
        )


@pytest.mark.asyncio
async def test_neighbors_are_authorized_deterministic_bounded_and_truncated() -> None:
    AllowEntitlement.calls = 0
    service = WorkGraphNeighborsService(
        FakeSource(),
        AllowEntitlement(),
        ExactAuthorizer(),  # type: ignore[arg-type]
    )
    result = await service.neighbors(
        org_id=ORG,
        permission_fingerprint="permissions-a",
        request=_request(),
    )
    assert AllowEntitlement.calls == 1
    assert [edge.edge_id for edge in result.edges] == ["edge-a"]
    assert result.total_count == 2
    assert result.returned_count == 1
    assert result.truncated is True
    assert result.depth == 1
    assert result.warnings == ("result_truncated",)


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"depth": 2}, "depth is fixed"),
        ({"limit": 26}, "limit must be"),
        ({"relationship_types": ("invented",)}, "Unsupported"),
    ],
)
def test_request_rejects_model_controlled_traversal_and_unbounded_values(
    changes, match
) -> None:
    with pytest.raises(ValueError, match=match):
        _request(**changes)


@pytest.mark.asyncio
async def test_neighbors_reject_root_outside_authorized_scope() -> None:
    service = WorkGraphNeighborsService(
        FakeSource(),
        AllowEntitlement(),
        ExactAuthorizer(),  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match="outside the resolved scope"):
        await service.neighbors(
            org_id=ORG,
            permission_fingerprint="permissions-a",
            request=_request(root_refs=(WorkGraphRootRef("issue", "issue-other"),)),
        )


@pytest.mark.asyncio
async def test_clickhouse_reader_uses_tenant_scoped_parameterized_bounded_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    async def query(_client, sql: str, params: dict[str, Any]):
        calls.append((sql, params))
        assert "%(org_id)s" in sql
        assert params["org_id"] == ORG
        if "FROM work_graph_edges" in sql:
            return [
                {
                    "edge_id": "edge-a",
                    "source_type": "issue",
                    "source_id": "issue-a",
                    "target_type": "issue",
                    "target_id": "issue-b",
                    "relationship_type": "blocks",
                    "repository_id": "repo-a",
                    "provenance": "native",
                    "confidence": 1,
                    "observed_at": NOW,
                    "source_watermark": NOW,
                }
            ]
        if "FROM work_item_dependencies" in sql:
            return []
        return [
            {"work_item_id": "issue-a", "title": "Issue A"},
            {"work_item_id": "issue-b", "title": "Issue B"},
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.work_graph_neighbors_service.query_dicts", query
    )
    rows = await ClickHouseWorkGraphNeighborSource(object()).fetch(
        org_id=ORG,
        scope=_scope(),
        roots=(WorkGraphRootRef("issue", "issue-a"),),
        relationship_types=("blocks",),
        direction=GraphDirection.OUTGOING,
        limit=25,
    )
    assert len(rows) == 1
    assert rows[0].source_label == "Issue A"
    assert calls[0][1]["limit"] == 26
    assert calls[0][1]["root_pairs"] == [("issue", "issue-a")]
    assert calls[0][1]["repo_ids"] == ["repo-a"]
