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
        # CHAOS-3368: declared-state facts expand through their own read of
        # the projects catalog, not the work_items adapter.
        "projects",
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


# ---------------------------------------------------------------------------
# CHAOS-3368: declared-state facts must expand through the "projects"
# adapter, not "work_items" (Codex adversarial review, HIGH, 2026-08-04).
# ---------------------------------------------------------------------------


class _PredicateEvaluatingSink:
    """Unlike ``FakeSink`` above (a canned-row stub used only to prove a
    query's own SQL text/params shape), this evaluates the real identity
    predicate against two disjoint row stores -- needed to prove ABSENCE,
    not merely to observe a query was issued. A canned-row fake cannot show
    that ``work_item_id = {entity_id:String}`` never matches a project id;
    only an evaluator that can also return NO rows can.
    """

    def __init__(self) -> None:
        self.work_items: dict[str, dict[str, Any]] = {
            "issue-1": {
                "entity_id": "issue-1",
                "display_label": "Release blocker",
                "excerpt": "Status: open",
                "provenance": "jira",
                "observed_at": NOW,
                "last_synced": NOW,
                "repository_id": "repo-a",
                "source_url": "",
                "deleted": 0,
                "confidence": 1.0,
            }
        }
        self.projects: dict[str, dict[str, Any]] = {
            "project_01": {
                "entity_id": "project_01",
                "display_label": "Project 01",
                "excerpt": "Declared state: started. Target date: 2026-09-01.",
                "provenance": "linear",
                "observed_at": NOW,
                "last_synced": NOW,
                "repository_id": "",
                "source_url": "",
                "deleted": 0,
                "confidence": 1.0,
            }
        }

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        entity_id = str(params.get("entity_id") or "")
        if "FROM work_items FINAL" in query:
            row = self.work_items.get(entity_id)
            return [row] if row else []
        if "FROM projects FINAL" in query:
            row = self.projects.get(entity_id)
            return [row] if row else []
        return []


def _project_evidence_ref(*, source_system: str) -> Any:
    from dev_health_ops.api.dev.contracts import DevEvidenceFlags, DevEvidenceRef

    return DevEvidenceRef(
        schema_version="dev_evidence_ref.v1",
        evidence_ref_id="ev1_0123456789012345678901234567890123456789",
        source_system=source_system,
        source_version="native.v1",
        entity_type="project",
        entity_id="project_01",
        display_label="Declared status",
        observed_at=NOW,
        freshness=FreshnessState.FRESH,
        provenance="native",
        confidence=1,
        repository_ids=[],
        valid_entity_ids=[],
        flags=DevEvidenceFlags(),
    )


@pytest.mark.asyncio
async def test_work_items_adapter_never_matches_a_project_identity() -> None:
    """CHAOS-3368 Codex HIGH (confirmed): pins WHY routing a declared-state
    fact's evidence through the "work_items" adapter (the pre-fix
    ``_STATUS_ENTITY_SOURCE_SYSTEM["project"] = "work_items"``) was a false
    pass. That adapter's ``expand_sql`` requires ``work_item_id =
    {entity_id:String}`` -- a project's own catalog id (this fixture:
    "project_01") is never a ``work_item_id``, so it can never match, and
    ``get_evidence.v1`` for such a fact would always come back NO_MATCHES
    (``expand()`` returns ``None``). A prior test asserted only
    ``fact.evidence_ref_ids`` truthy on the minted handle -- satisfied
    regardless of whether the handle actually expands to anything, i.e. a
    measurement that never happened, reading as coverage.
    """
    sink = _PredicateEvaluatingSink()
    work_items_adapter = ClickHouseEvidenceSource(
        sink,
        "work_items",
        policy=SourceFreshnessPolicy("work_items", "work-items-sync.v1", None),
        now=NOW,
    )

    record = await work_items_adapter.expand(
        org_id="org-a",
        scope=_resolution(),
        evidence=_project_evidence_ref(source_system="work_items"),
    )

    assert record is None, (
        "the 'work_items' adapter unexpectedly matched a project id -- if "
        "this now passes, the false-pass this test documents may no "
        "longer apply and the test itself needs revisiting"
    )


@pytest.mark.asyncio
async def test_projects_adapter_expands_a_declared_state_evidence_ref() -> None:
    """CHAOS-3368 Codex HIGH fix: the "projects" adapter (what
    ``_STATUS_ENTITY_SOURCE_SYSTEM["project"]`` now routes to) genuinely
    expands a declared-state fact's evidence, unlike "work_items" above.
    """
    sink = _PredicateEvaluatingSink()
    projects_adapter = ClickHouseEvidenceSource(
        sink,
        "projects",
        policy=SourceFreshnessPolicy("projects", "projects-sync.v1", None),
        now=NOW,
    )

    record = await projects_adapter.expand(
        org_id="org-a",
        scope=_resolution(),
        evidence=_project_evidence_ref(source_system="projects"),
    )

    assert record is not None
    assert record.entity_id == "project_01"
    assert record.entity_type == "project"
    assert "started" in (record.raw_excerpt or "")
    assert "2026-09-01" in (record.raw_excerpt or "")
