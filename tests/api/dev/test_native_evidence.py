from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import FreshnessState
from dev_health_ops.api.dev.native_evidence import (
    ClickHouseEvidenceSource,
    SourceFreshnessPolicy,
    native_evidence_adapters,
)
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeResolution,
    ScopeResolutionOutcome,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


class FakeSink:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((query, params))
        return [
            {
                "entity_id": "issue-1",
                "display_label": "Release blocker",
                "excerpt": "Status: open",
                "provenance": "jira",
                "observed_at": NOW - timedelta(hours=1),
                "last_synced": NOW - timedelta(hours=2),
                "repository_id": "repo-a",
                "source_url": "https://jira.example/browse/ISSUE-1",
                "deleted": 0,
                "confidence": 1.0,
            }
        ]


def _resolution() -> ScopeResolution:
    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(AuthorizedEntity(EntityKind.REPOSITORY, "repo-a", "Repo A"),),
        team_filters=(),
        candidates=(),
        time_range=ResolvedTimeRange(
            timezone="UTC",
            utc_start=NOW - timedelta(days=30),
            utc_end=NOW,
            local_start=(NOW - timedelta(days=30)).isoformat(),
            local_end=NOW.isoformat(),
            comparison_utc_start=NOW - timedelta(days=60),
            comparison_utc_end=NOW - timedelta(days=30),
            comparison_local_start=(NOW - timedelta(days=60)).isoformat(),
            comparison_local_end=(NOW - timedelta(days=30)).isoformat(),
        ),
    )


def test_launch_native_evidence_class_registry_is_explicit() -> None:
    assert {item.source_system for item in native_evidence_adapters(object())} == {
        "work_units",
        "work_items",
        "pull_requests",
        "reviews",
        "commits",
        "ci_runs",
        "deployments",
        "incidents",
        "work_graph",
    }


@pytest.mark.asyncio
async def test_native_search_is_parameterized_tenant_scoped_and_source_capped() -> None:
    sink = FakeSink()
    source = ClickHouseEvidenceSource(
        sink,
        "work_items",
        policy=SourceFreshnessPolicy(
            "work_items", "work-items-sync.v1", timedelta(hours=48)
        ),
        now=NOW,
    )
    injection = "x') OR org_id != '' --"
    result = await source.search(
        org_id="org-a",
        scope=_resolution(),
        query=injection,
        limit=10_000,
    )

    query, params = sink.calls[0]
    assert injection not in query
    assert "org_id = {org_id:String}" in query
    assert "LIMIT {limit:UInt32}" in query
    assert params["org_id"] == "org-a"
    assert params["query"] == injection
    assert params["repository_ids"] == ["repo-a"]
    assert params["limit"] == 100
    assert result.records[0].freshness is FreshnessState.FRESH
    # A raw provider URL is data, not authority; no host is allowlisted here.
    assert result.records[0].authorized_link_hosts == ()


@pytest.mark.asyncio
async def test_native_expansion_uses_exact_entity_and_repository_parameters() -> None:
    sink = FakeSink()
    source = ClickHouseEvidenceSource(
        sink,
        "work_items",
        policy=SourceFreshnessPolicy(
            "work_items", "work-items-sync.v1", timedelta(hours=48)
        ),
        now=NOW,
    )
    from dev_health_ops.api.dev.contracts import DevEvidenceFlags, DevEvidenceRef

    ref = DevEvidenceRef(
        schema_version="dev_evidence_ref.v1",
        evidence_ref_id="ev1_0123456789012345678901234567890123456789",
        source_system="work_items",
        source_version="native.v1",
        entity_type="issue",
        entity_id="issue-1",
        display_label="Release blocker",
        observed_at=NOW,
        freshness=FreshnessState.FRESH,
        provenance="native",
        confidence=1,
        repository_ids=["repo-a"],
        valid_entity_ids=[],
        flags=DevEvidenceFlags(),
    )
    record = await source.expand(org_id="org-a", scope=_resolution(), evidence=ref)
    query, params = sink.calls[0]
    assert "work_item_id = {entity_id:String}" in query
    assert params == {
        "org_id": "org-a",
        "entity_id": "issue-1",
        "repository_ids": ["repo-a"],
        "scope_entity_id": "",
        "scope_pr_number": 0,
    }
    assert record is not None
