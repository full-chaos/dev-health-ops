"""CHAOS-3259: the production tool boundary must preserve status/evidence semantics.

These are real production-registry integration tests: they build the actual
``BoundedDevRuntime`` returned by ``build_production_runtime`` -- the real
``AskDevToolRegistry``, the real ``StatusChangeService`` (including its
deterministic ``actual-completion`` rule engine), and the real
``production_runtime.py`` tool adapters -- and only swap the ClickHouse-backed
leaves (``ClickHouseStatusChangeSource`` and ``ClickHouseAuthorizedEntityCatalog``)
for deterministic fakes, exactly the way ``test_production_runtime.py`` already
fakes ``resolve_production_provider``. Nothing in ``production_runtime.py``,
``status_change_service.py``, or the wire ``contracts.py`` validators is
mocked.
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev.contracts import (
    ClaimKind,
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DevToolRequest,
    DirectScope,
    EntityType,
    ToolID,
)
from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.api.dev.status_change_service import (
    ChangeCategory,
    ChangeWindow,
    CIFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    RawChangeSummary,
    RawStatusSnapshot,
    SourceReference,
    StatusFact,
)
from dev_health_ops.api.dev.tool_registry import ToolExecutionContext
from dev_health_ops.llm.agent.policy import AgentProviderSource

ORG_ID = "3d3a2b1e-3259-4c3e-9e6a-325934592591"
# Runtime-constructed per test-process start, never a literal, so it can't
# be mistaken for a checked-in credential by secret scanners.
EVIDENCE_SIGNING_FIXTURE_KEY = secrets.token_hex(32)
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
FRESH_OBSERVED = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
STALE_OBSERVED = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


class FakeProvider:
    """Minimal AgentLLMProvider stand-in; no tool test in this module calls it."""

    async def decide(self, **_values: Any) -> Any:
        raise AssertionError("provider calls are outside this projection test")

    async def aclose(self) -> None:
        return None


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ISSUE,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue_parent",
                display_label="Parent issue",
                repository_id="repo_dev_health",
            )
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 6, 30, tzinfo=UTC), end=NOW, timezone="UTC"
        ),
    )


class _FakeStatusChangeSource:
    """Stands in for ``ClickHouseStatusChangeSource``.

    Everything downstream of this -- ``StatusChangeService`` (the deterministic
    completion/conflict rule engine) and every ``production_runtime.py``
    adapter closure -- runs unmodified against real fixture data.
    """

    def __init__(self) -> None:
        self.declared = StatusFact(
            entity_type="issue",
            entity_id="issue_parent",
            display_label="Parent issue",
            status="done",
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:work_items",
            evidence_ref_ids=(),
        )
        self.child = StatusFact(
            entity_type="issue",
            entity_id="issue_child",
            display_label="Child issue",
            status="open",
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:work_items",
            evidence_ref_ids=(),
            required=True,
        )
        self.pull_request = PullRequestFact(
            entity_id="repo_dev_health#pr42",
            display_label="Pull request #42",
            state="merged",
            review_state="approved",
            changes_requested=0,
            merged=True,
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:pull_requests",
            evidence_ref_ids=(),
            required=True,
        )
        self.ci_failing = CIFact(
            entity_id="repo_dev_health#ci1001",
            display_label="Required acceptance suite",
            conclusion="failure",
            required=True,
            skipped_required_work=False,
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:ci_runs",
            evidence_ref_ids=(),
        )
        self.ci_skipped = CIFact(
            entity_id="repo_dev_health#ci1002",
            display_label="Required integration suite",
            conclusion="skipped",
            required=True,
            skipped_required_work=True,
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:ci_runs",
            evidence_ref_ids=(),
        )
        self.source_refs = (
            SourceReference(
                ref_id="ref:work_items",
                source_system="work_items",
                source_version="work-items.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
            SourceReference(
                ref_id="ref:pull_requests",
                source_system="pull_requests",
                source_version="pull-requests.v1",
                freshness=production_runtime.FreshnessState.STALE,
                watermark=STALE_OBSERVED,
                evidence_ref_ids=(),
            ),
            SourceReference(
                ref_id="ref:ci_runs",
                source_system="ci_runs",
                source_version="ci-runs.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
            SourceReference(
                ref_id="ref:deployments",
                source_system="deployments",
                source_version="deployments.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
        )

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        del org_id, scope, as_of, limit
        return RawStatusSnapshot(
            declared=self.declared,
            children=(self.child,),
            blockers=(),
            pull_requests=(self.pull_request,),
            ci=(self.ci_failing, self.ci_skipped),
            deployments=(),
            incidents=(
                IncidentFact(
                    entity_id="incident_01",
                    display_label="Resolved incident",
                    status="resolved",
                    active=False,
                    blocking=False,
                    observed_at=FRESH_OBSERVED,
                    source_ref_id="ref:incidents",
                    evidence_ref_ids=(),
                ),
            ),
            source_refs=self.source_refs,
            warnings=(),
        )

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary:
        del org_id, scope, current, comparison, limit
        return RawChangeSummary(changes=(), source_refs=self.source_refs)


class _FakeEntitlementAuthorizer:
    """Stands in for ``CanonicalAskDevEntitlementAuthorizer`` -- its real
    implementation needs a live Postgres session, which is out of scope for
    this adapter-boundary projection test.
    """

    def __init__(self, _session: Any) -> None:
        pass

    async def require(self, org_id: str) -> None:
        assert org_id == ORG_ID


class _FakeAuthorizedEntityCatalog:
    """Stands in for ``ClickHouseAuthorizedEntityCatalog`` for the follow-up
    ``get_evidence.v1`` call -- authorizes exactly the issue in ``_scope()``.
    """

    def __init__(self) -> None:
        self._entity = AuthorizedEntity(
            kind=EntityKind.ISSUE,
            canonical_id="issue_parent",
            label="Parent issue",
            repository_id="repo_dev_health",
        )

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str:
        del org_id, kinds
        return "watermark_01"

    async def exact(
        self, org_id: str, ref: Any, *, limit: int
    ) -> list[AuthorizedEntity]:
        del org_id, limit
        if ref.kind is EntityKind.ISSUE and ref.value == "issue_parent":
            return [self._entity]
        if ref.kind is EntityKind.PROJECT and ref.value == "project_01":
            return [
                AuthorizedEntity(
                    kind=EntityKind.PROJECT,
                    canonical_id="project_01",
                    label="Project 01",
                    repository_id="repo_dev_health",
                )
            ]
        return []

    async def search(
        self, org_id: str, query: str, kinds: tuple[EntityKind, ...], *, limit: int
    ) -> list[AuthorizedEntity]:
        del org_id, query, kinds, limit
        return []


async def _build_runtime(
    monkeypatch: pytest.MonkeyPatch, *, status_source: Any | None = None
) -> Any:
    async def resolve_provider(_session, *, org_id: str):
        assert org_id == ORG_ID
        return ProductionProviderResolution(
            provider=cast(Any, FakeProvider()),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )
    monkeypatch.setattr(
        production_runtime,
        "ClickHouseStatusChangeSource",
        lambda _clickhouse: status_source or _FakeStatusChangeSource(),
    )
    monkeypatch.setattr(
        production_runtime,
        "ClickHouseAuthorizedEntityCatalog",
        lambda _clickhouse: _FakeAuthorizedEntityCatalog(),
    )
    monkeypatch.setattr(
        production_runtime,
        "CanonicalAskDevEntitlementAuthorizer",
        _FakeEntitlementAuthorizer,
    )
    monkeypatch.setenv("JWT_SECRET_KEY", EVIDENCE_SIGNING_FIXTURE_KEY)
    return await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )


def _context(scope: DevScope) -> ToolExecutionContext:
    return ToolExecutionContext(
        org_id=scope.organization_id,
        user_id="user_01",
        permission_fingerprint="permissions_01",
        authorized_scope=scope,
        cancellation=asyncio.Event(),
        remaining_seconds=15,
    )


@pytest.mark.asyncio
async def test_status_snapshot_preserves_deterministic_completion_and_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3259 acceptance: completed parent + incomplete child + merged PR +
    failing/skipped acceptance + missing deployment + fresh/stale sources must
    all survive the production tool boundary, and every referenced evidence ID
    must be present -- and independently signer-verifiable -- in the same
    result.
    """

    runtime = await _build_runtime(monkeypatch)
    scope = _scope()
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.STATUS_SNAPSHOT,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    result = execution.result

    # -- overall result: partial, because the pull_requests source is stale --
    assert result.status == "partial"

    # -- declared + child status facts survive (guard: dropping either empties
    # status_facts or removes the entity_id from it) --
    fact_ids = {fact.fact_id for fact in result.status_facts}
    assert fact_ids == {"issue:issue_parent", "issue:issue_child"}
    for fact in result.status_facts:
        assert fact.evidence_ref_ids

    # -- PR/CI/deployment/incident facts survive with their typed fields
    # (guard: a regression to the old ``values = [declared, *children,
    # *blockers]`` line drops all four of these to empty) --
    assert len(result.pull_requests) == 1
    pr = result.pull_requests[0]
    assert pr.entity_id == "repo_dev_health#pr42"
    assert pr.merged is True
    assert pr.review_state == "approved"
    assert pr.evidence_ref_ids

    assert {fact.conclusion for fact in result.ci_checks} == {"failure", "skipped"}
    assert any(fact.skipped_required_work is True for fact in result.ci_checks)
    for fact in result.ci_checks:
        assert fact.evidence_ref_ids

    assert result.deployments == []  # no deployment evidence was returned
    assert len(result.incidents) == 1
    assert result.incidents[0].evidence_ref_ids

    # -- the deterministic actual-completion assessment survives, including
    # its conflict and required-child detail (guard: the old adapter never
    # projected ``result.actual`` at all) --
    assert result.actual_completion is not None
    assert result.actual_completion.state == "not_ready"
    assert result.actual_completion.rule_id == "actual-completion"
    assert "required_child_incomplete" in result.actual_completion.reason_codes
    assert "required_ci_not_passing" in result.actual_completion.reason_codes
    assert "required_ci_work_skipped" in result.actual_completion.reason_codes
    assert "required_release_evidence_missing" in result.actual_completion.reason_codes
    assert len(result.actual_completion.required_children) == 1
    assert result.actual_completion.required_children[0].fact_id == "issue:issue_child"
    assert len(result.actual_completion.conflicts) == 1
    conflict = result.actual_completion.conflicts[0]
    assert conflict.code == "declared_complete_conflicts_with_observed_work"
    assert conflict.severity == "blocking"
    assert conflict.evidence_ref_ids
    assert result.actual_completion.evidence_ref_ids

    # -- fresh/stale source health survives (guard: source freshness was
    # never surfaced at all before this fix) --
    freshness_by_source = {
        item.source_system: item.freshness for item in result.source_health
    }
    assert freshness_by_source["work_items"] == "fresh"
    assert freshness_by_source["pull_requests"] == "stale"
    assert freshness_by_source["ci_runs"] == "fresh"

    # -- every evidence ID referenced anywhere in this result is present in
    # its evidence array (the contract's own validator already enforces this
    # -- it could not have been constructed otherwise -- but assert the
    # positive shape explicitly too) --
    known_evidence_ids = {item.evidence_ref_id for item in result.evidence}
    assert known_evidence_ids  # non-empty: evidence was actually hydrated
    referenced = set()
    for fact in result.status_facts:
        referenced.update(fact.evidence_ref_ids)
    for fact in result.pull_requests:
        referenced.update(fact.evidence_ref_ids)
    for fact in result.ci_checks:
        referenced.update(fact.evidence_ref_ids)
    for fact in result.incidents:
        referenced.update(fact.evidence_ref_ids)
    referenced.update(result.actual_completion.evidence_ref_ids)
    assert referenced <= known_evidence_ids
    assert referenced  # the guard actually exercised the closure, not a no-op

    # -- every evidence ID minted here is independently signer-verifiable,
    # i.e. genuinely expandable through get_evidence.v1, not merely present
    # in the array (guard: a regression that forwards an un-signed opaque
    # domain ID would fail this even though the array-membership check above
    # still passes) --
    signer = EvidenceReferenceSigner(EVIDENCE_SIGNING_FIXTURE_KEY)
    for item in result.evidence:
        assert signer.verify(ORG_ID, item)

    await runtime.aclose()


@pytest.mark.asyncio
async def test_get_evidence_expands_status_snapshot_evidence_in_the_same_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The evidence a status_snapshot call mints must be expandable by a
    subsequent get_evidence.v1 call against the SAME registered runtime --
    the real ``EvidenceService``, real signer, and real (faked-catalog)
    authorization path, not just a request-local echo.
    """

    runtime = await _build_runtime(monkeypatch)
    scope = _scope()
    status_execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.STATUS_SNAPSHOT,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    evidence_ids = [item.evidence_ref_id for item in status_execution.result.evidence][
        :5
    ]
    assert evidence_ids

    get_execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_02",
            tool_id=ToolID.GET_EVIDENCE,
            scope=scope,
            evidence_ref_ids=evidence_ids,
            limit=10,
        ),
        _context(scope),
    )
    result = get_execution.result

    # The route must find and authorize every ID minted a moment ago: it must
    # never fall back to "evidence_reference_not_found" (the cache-miss path)
    # or an "unauthorized" outcome (the signature/authorization-mismatch
    # path -- get_evidence surfaces that as a per-item "not_found" warning
    # AND folds it into ``status_facts[i].text``, not into top-level
    # ``status``/``warnings`` alone, so both must be checked). Without a live
    # ClickHouse the raw content fetch itself degrades to "unavailable" for
    # entities the fake source can't resolve -- that is an honest,
    # non-crashing outcome, not a rejection, and is asserted separately below.
    assert result.status != "unavailable"
    assert "evidence_reference_not_found" not in result.warnings
    assert "some_evidence_references_were_not_found" not in result.warnings
    assert "not_found" not in result.warnings
    assert {item.evidence_ref_id for item in result.evidence} == set(evidence_ids)
    for fact in result.status_facts:
        assert fact.text != "not_found", (
            "evidence was rejected as unauthorized instead of expanded "
            "(or honestly reported unavailable/no_matches)"
        )

    await runtime.aclose()


def _project_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.PROJECT,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.PROJECT,
                entity_id="project_01",
                display_label="Project 01",
                repository_id="repo_dev_health",
            )
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 6, 30, tzinfo=UTC), end=NOW, timezone="UTC"
        ),
    )


def _real_native_evidence_query_dicts() -> Any:
    """A fake ``query_dicts`` that enforces the SAME ``scope_entity_id``
    predicate ``native_evidence.py``'s real expand SQL applies, instead of
    matching on ``entity_id`` alone. A prior version of this test's stub
    ignored that predicate and asserted a false "AVAILABLE" for a PR that
    real ClickHouse can never return under this scope (codex adversarial
    review, round 2, finding "linked-child expansion test bypasses
    production scope predicates").

    The child issue is modeled as belonging to ``project_01`` -- so a
    PROJECT-scoped call's ``work_items`` predicate
    (``project_id = scope_entity_id OR project_key = scope_entity_id``)
    genuinely passes, mirroring a real child-of-project relationship.
    """

    async def fake_query_dicts(_client: Any, sql: str, params: dict[str, Any]) -> Any:
        scope_entity_id = params.get("scope_entity_id", "")
        if "FROM work_items" in sql and params.get("entity_id") == "issue_child":
            if scope_entity_id not in ("", "issue_child", "project_01"):
                return []
            return [
                {
                    "entity_id": "issue_child",
                    "display_label": "Child issue",
                    "excerpt": "Status: open. Awaiting engineering review.",
                    "provenance": "native",
                    "observed_at": FRESH_OBSERVED,
                    "last_synced": FRESH_OBSERVED,
                    "repository_id": "repo_dev_health",
                    "source_url": "",
                    "deleted": 0,
                    "confidence": 1.0,
                }
            ]
        if (
            "FROM git_pull_requests" in sql
            and params.get("entity_id") == "repo_dev_health#pr42"
        ):
            # The pull_requests adapter's expand predicate only matches when
            # scope_entity_id IS the PR's own composite id -- never for a PR
            # merely linked to the direct-scope entity. No PROJECT- or
            # ISSUE-scoped status_snapshot call can ever satisfy this, so a
            # faithful fake must return no rows here too.
            if scope_entity_id != "repo_dev_health#pr42":
                return []
            return [
                {
                    "entity_id": "repo_dev_health#pr42",
                    "display_label": "Pull request #42",
                    "excerpt": "State: merged. Implements the fix.",
                    "provenance": "native",
                    "observed_at": FRESH_OBSERVED,
                    "last_synced": FRESH_OBSERVED,
                    "repository_id": "repo_dev_health",
                    "source_url": "",
                    "deleted": 0,
                    "confidence": 1.0,
                }
            ]
        return []

    return fake_query_dicts


@pytest.mark.asyncio
async def test_get_evidence_returns_real_content_for_linked_child_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evidence minted for a fact OTHER than the scope's own direct entity
    (the required child issue) must be truly expandable --
    ``EvidenceAvailability.AVAILABLE`` with a real excerpt -- not merely
    present in the evidence array. This is the exact failure mode
    CHAOS-3259's initial fix missed: binding ``valid_entity_ids`` to each
    fact's own ID (instead of the caller's authorized scope) made every
    non-direct-entity reference authorization-reject.

    The linked PULL REQUEST's evidence is asserted separately below as an
    honest NO_MATCHES, not AVAILABLE: the native ``pull_requests`` adapter's
    expand SQL only matches when the scope's OWN direct entity IS that PR,
    never for a PR merely linked to a different direct-scope entity. That is
    a pre-existing native-evidence-adapter limitation (search/expand scope
    predicates address "the direct entity itself", not "entities related to
    it") outside CHAOS-3259's four known adapter-boundary breaks; it does
    NOT regress anything CHAOS-3259 owns -- authorization now correctly
    succeeds (no more false "unauthorized"/"not_found"), and NO_MATCHES is
    the contract's own defined, honest outcome for it.
    """
    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_evidence.query_dicts",
        _real_native_evidence_query_dicts(),
    )

    runtime = await _build_runtime(monkeypatch)
    scope = _project_scope()
    status_execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.STATUS_SNAPSHOT,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    child_evidence_id = next(
        iter(
            fact.evidence_ref_ids[0]
            for fact in status_execution.result.status_facts
            if fact.fact_id == "issue:issue_child"
        )
    )
    pr_evidence_id = status_execution.result.pull_requests[0].evidence_ref_ids[0]
    assert child_evidence_id != pr_evidence_id

    get_execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_02",
            tool_id=ToolID.GET_EVIDENCE,
            scope=scope,
            evidence_ref_ids=[child_evidence_id, pr_evidence_id],
            limit=10,
        ),
        _context(scope),
    )
    result = get_execution.result
    texts = {fact.evidence_ref_ids[0]: fact.text for fact in result.status_facts}
    assert "Awaiting engineering review" in texts[child_evidence_id]
    # Honest NO_MATCHES, never "not_found"/"unauthorized" -- see docstring.
    assert texts[pr_evidence_id] in {"no_matches", "evidence_deleted_or_unavailable"}
    assert texts[pr_evidence_id] not in {"not_found", "unauthorized"}

    await runtime.aclose()


@pytest.mark.asyncio
async def test_work_graph_edges_preserve_provenance_and_mint_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The work_graph adapter must stop hardcoding ``evidence_ref_ids=[]`` and
    must preserve each edge's provenance/confidence/observed_at.
    """
    from dev_health_ops.api.dev import work_graph_neighbors_service as wgns

    edge_observed_at = FRESH_OBSERVED
    raw_edge = wgns.WorkGraphRawEdge(
        edge_id="edge_01",
        source_type="issue",
        source_id="issue_parent",
        target_type="pr",
        target_id="repo_dev_health#pr42",
        relationship_type="references",
        repository_id="repo_dev_health",
        provenance="persisted",
        confidence=0.9,
        observed_at=edge_observed_at,
        source_table="work_graph_edges",
        source_version="work-graph-edges.v1",
        source_watermark=edge_observed_at,
    )

    class _FakeGraphSource:
        async def fetch(self, **_kwargs: Any) -> tuple[Any, ...]:
            return (raw_edge,)

    # ClickHouseWorkGraphNeighborSource is captured at assembly time, so this
    # must be patched before building the runtime.
    monkeypatch.setattr(
        production_runtime,
        "ClickHouseWorkGraphNeighborSource",
        lambda _clickhouse: _FakeGraphSource(),
    )
    runtime = await _build_runtime(monkeypatch)

    scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ISSUE,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue_parent",
                display_label="Parent issue",
                repository_id="repo_dev_health",
            )
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 6, 30, tzinfo=UTC), end=NOW, timezone="UTC"
        ),
    )
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.WORK_GRAPH_NEIGHBORS,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    result = execution.result
    assert len(result.graph_edges) == 1
    edge = result.graph_edges[0]
    assert edge.provenance == "persisted"
    assert edge.confidence == pytest.approx(0.9)
    assert edge.observed_at == edge_observed_at
    assert edge.evidence_ref_ids
    known_evidence_ids = {item.evidence_ref_id for item in result.evidence}
    assert set(edge.evidence_ref_ids) <= known_evidence_ids

    await runtime.aclose()


def _change_scope() -> DevScope:
    current_end = NOW
    current_start = NOW - timedelta(days=30)
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.ISSUE,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue_parent",
                display_label="Parent issue",
                repository_id="repo_dev_health",
            )
        ],
        time_range=DevTimeRange(start=current_start, end=current_end, timezone="UTC"),
        comparison_range=DevTimeRange(
            start=current_start - timedelta(days=30),
            end=current_start,
            timezone="UTC",
        ),
    )


def _observed_change(
    *, change_id: str, before: str | None, after: str | None
) -> ObservedChange:
    # entity_id is the scope's OWN direct entity (issue_parent, per
    # _change_scope()) so a real work_items expand -- once mocked -- can
    # genuinely match it; two transitions of the scope's own issue is also
    # the realistic "what changed about this issue" shape for change_summary.
    return ObservedChange(
        change_id=change_id,
        category=ChangeCategory.STATUS,
        entity_type="issue",
        entity_id="issue_parent",
        display_label="Parent issue",
        before=before,
        after=after,
        observed_at=FRESH_OBSERVED,
        claim_kind=ClaimKind.OBSERVED,
        relationship_chain=(),
        metric_id=None,
        metric_value=None,
        metric_comparison_value=None,
        source_ref_ids=("ref:work_items",),
        evidence_ref_ids=(),
    )


class _FakeChangeOnlySource:
    """Two STATUS-category transitions on the SAME entity_id: a regression
    to keying minted evidence purely by entity_id would collapse both onto
    one evidence_ref_id (CHAOS-3259 codex finding: "different changes
    collapse onto the same evidence reference").
    """

    def __init__(self, changes: tuple[ObservedChange, ...]) -> None:
        self._changes = changes
        self._source_refs = (
            SourceReference(
                ref_id="ref:work_items",
                source_system="work_items",
                source_version="work-items.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
        )

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        del org_id, scope, as_of, limit
        return RawStatusSnapshot(declared=None, source_refs=self._source_refs)

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary:
        del org_id, scope, current, comparison, limit
        return RawChangeSummary(changes=self._changes, source_refs=self._source_refs)


@pytest.mark.asyncio
async def test_change_summary_does_not_collide_distinct_observations_of_one_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two distinct observed changes on one entity must mint distinct,
    independently-expandable evidence. A prior fix (round 1) achieved
    distinctness by folding change_id into the LOOKUP entity_id itself,
    which made every status-change reference unexpandable (codex round 2:
    "status-change collision fix makes every status reference
    unexpandable") -- the discriminator now lives in source_version, which
    native adapters never filter on, so distinctness and real expansion
    both hold.
    """

    async def fake_query_dicts(_client: Any, sql: str, params: dict[str, Any]) -> Any:
        if "FROM work_items" in sql and params.get("entity_id") == "issue_parent":
            if params.get("scope_entity_id", "") not in ("", "issue_parent"):
                return []
            return [
                {
                    "entity_id": "issue_parent",
                    "display_label": "Parent issue",
                    "excerpt": "Status: done.",
                    "provenance": "native",
                    "observed_at": FRESH_OBSERVED,
                    "last_synced": FRESH_OBSERVED,
                    "repository_id": "repo_dev_health",
                    "source_url": "",
                    "deleted": 0,
                    "confidence": 1.0,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_evidence.query_dicts", fake_query_dicts
    )

    changes = (
        _observed_change(change_id="transition-1", before="open", after="in_progress"),
        _observed_change(change_id="transition-2", before="in_progress", after="done"),
    )
    runtime = await _build_runtime(
        monkeypatch, status_source=_FakeChangeOnlySource(changes)
    )
    scope = _change_scope()
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.CHANGE_SUMMARY,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    result = execution.result
    assert len(result.status_facts) == 2
    evidence_ids = [fact.evidence_ref_ids[0] for fact in result.status_facts]
    assert len(set(evidence_ids)) == 2, (
        "two distinct observed changes on the same entity minted the same "
        "evidence_ref_id -- one change's evidence silently overwrote the "
        "other's in the shared evidence cache"
    )
    assert {item.evidence_ref_id for item in result.evidence} == set(evidence_ids)

    get_execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_02",
            tool_id=ToolID.GET_EVIDENCE,
            scope=scope,
            evidence_ref_ids=evidence_ids,
            limit=10,
        ),
        _context(scope),
    )
    get_result = get_execution.result
    texts = [fact.text for fact in get_result.status_facts]
    assert len(texts) == 2
    assert all("Status: done." in text for text in texts), (
        f"expected both distinct transition references to genuinely expand "
        f"to real content, got: {texts}"
    )

    await runtime.aclose()


class _FakeDenseStatusChangeSource:
    """A completed parent with 15 required incomplete children (each
    contributing the "required_child_incomplete" blocking reason) and 15
    already-merged, non-contributing pull requests: 31 unique addressable
    facts (1 declared + 15 children + 15 PRs), which mint more than the 25
    evidence entries ``DevToolResult.evidence`` allows. CHAOS-3259 codex
    finding: "per-category limits exceed the evidence contract before byte
    rejection" -- naive minting would raise a pydantic ValidationError
    instead of returning a bounded, warned result. The PRs are deliberately
    non-contributing (merged, reviewed, no changes requested) so truncation
    priority is actually exercised: round-2 codex finding "hash-order
    truncation removes verdict-driving facts" -- the 16 verdict-driving
    facts (declared + 15 required children) must all survive; only the 15
    incidental PRs should be the ones partially cut.
    """

    def __init__(self) -> None:
        self.declared = StatusFact(
            entity_type="issue",
            entity_id="issue_parent",
            display_label="Parent issue",
            status="done",
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:work_items",
            evidence_ref_ids=(),
        )
        self.children = tuple(
            StatusFact(
                entity_type="issue",
                entity_id=f"issue_child_{index:02d}",
                display_label=f"Child issue {index}",
                status="open",
                observed_at=FRESH_OBSERVED,
                source_ref_id="ref:work_items",
                evidence_ref_ids=(),
                required=True,
            )
            for index in range(15)
        )
        self.pull_requests = tuple(
            PullRequestFact(
                entity_id=f"repo_dev_health#pr{index}",
                display_label=f"Pull request #{index}",
                state="merged",
                review_state="approved",
                changes_requested=0,
                merged=True,
                observed_at=FRESH_OBSERVED,
                source_ref_id="ref:pull_requests",
                evidence_ref_ids=(),
                required=True,
            )
            for index in range(15)
        )
        self.source_refs = (
            SourceReference(
                ref_id="ref:work_items",
                source_system="work_items",
                source_version="work-items.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
            SourceReference(
                ref_id="ref:pull_requests",
                source_system="pull_requests",
                source_version="pull-requests.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
        )

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        del org_id, scope, as_of, limit
        return RawStatusSnapshot(
            declared=self.declared,
            children=self.children,
            pull_requests=self.pull_requests,
            source_refs=self.source_refs,
        )

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary:
        del org_id, scope, current, comparison, limit
        return RawChangeSummary(changes=(), source_refs=self.source_refs)


@pytest.mark.asyncio
async def test_status_snapshot_bounds_evidence_instead_of_failing_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = await _build_runtime(
        monkeypatch, status_source=_FakeDenseStatusChangeSource()
    )
    scope = _scope()
    # request.limit itself is capped at 25 by DevToolRequest; the dense
    # per-category source data above is what pushes total unique evidence
    # past the DevToolResult.evidence bound of 25, independent of limit.
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.STATUS_SNAPSHOT,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    result = execution.result
    assert len(result.evidence) <= 25
    assert "status_snapshot_evidence_result_truncated" in result.warnings

    known_evidence_ids = {item.evidence_ref_id for item in result.evidence}
    for fact in result.status_facts:
        assert fact.evidence_ref_ids
        assert set(fact.evidence_ref_ids) <= known_evidence_ids
    for fact in result.pull_requests:
        assert set(fact.evidence_ref_ids) <= known_evidence_ids

    # Priority preservation (codex round 2): the 16 verdict-driving facts --
    # the declared parent plus all 15 required, incomplete children -- must
    # ALL keep their evidence even though the result overall had to shed 6
    # of the 31 candidates. Only the 15 non-contributing (merged, approved)
    # PRs are eligible to be thinned, and since 16 priority + 25 budget
    # leaves room for 9, exactly 9 of them should survive.
    assert len(result.status_facts) == 16, (
        "a required, incomplete child lost its evidence and was dropped "
        "even though it drove the actual-completion verdict"
    )
    assert len(result.actual_completion.required_children) == 15
    for child in result.actual_completion.required_children:
        assert child.evidence_ref_ids, (
            f"required child {child.fact_id} lost its evidence to "
            f"truncation despite being verdict-driving priority evidence"
        )
    surviving_prs = [fact for fact in result.pull_requests if fact.evidence_ref_ids]
    assert len(surviving_prs) == 9

    # The conflict must never fall back to unrelated evidence: every ID it
    # cites has to come from a category that actually produced a blocking
    # reason code (here, "required_child_incomplete" -> status_facts), never
    # from the incidental, already-merged PRs.
    assert result.actual_completion.conflicts
    pr_evidence_ids = {
        evidence_id
        for fact in result.pull_requests
        for evidence_id in fact.evidence_ref_ids
    }
    for conflict in result.actual_completion.conflicts:
        assert conflict.evidence_ref_ids
        assert not (set(conflict.evidence_ref_ids) & pr_evidence_ids)

    await runtime.aclose()


class _FakeCiAcceptanceStatusChangeSource:
    """Two acceptance checks from the SAME CI run: coarsening their entity_id
    to the run-level locator (so expansion can resolve real content, since
    no native adapter indexes the finer check-level ID) would collide both
    checks onto one evidence_ref_id if the signed identity weren't
    separately discriminated (codex round 2: "CI acceptance checks in one
    run collapse onto one run-level reference").
    """

    def __init__(self) -> None:
        self.declared = StatusFact(
            entity_type="issue",
            entity_id="issue_parent",
            display_label="Parent issue",
            status="in_progress",
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:work_items",
            evidence_ref_ids=(),
        )
        self.ci_pass = CIFact(
            entity_id="repo_dev_health#ci2001#checkA",
            display_label="Lint check",
            conclusion="success",
            required=True,
            skipped_required_work=False,
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:ci_runs",
            evidence_ref_ids=(),
        )
        self.ci_fail = CIFact(
            entity_id="repo_dev_health#ci2001#checkB",
            display_label="Test suite",
            conclusion="failure",
            required=True,
            skipped_required_work=False,
            observed_at=FRESH_OBSERVED,
            source_ref_id="ref:ci_runs",
            evidence_ref_ids=(),
        )
        self.source_refs = (
            SourceReference(
                ref_id="ref:work_items",
                source_system="work_items",
                source_version="work-items.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
            SourceReference(
                ref_id="ref:ci_runs",
                source_system="ci_runs",
                source_version="ci-runs.v1",
                freshness=production_runtime.FreshnessState.FRESH,
                watermark=FRESH_OBSERVED,
                evidence_ref_ids=(),
            ),
        )

    async def status_snapshot(
        self, *, org_id: str, scope: DevScope, as_of: datetime, limit: int
    ) -> RawStatusSnapshot:
        del org_id, scope, as_of, limit
        return RawStatusSnapshot(
            declared=self.declared,
            ci=(self.ci_pass, self.ci_fail),
            source_refs=self.source_refs,
        )

    async def change_summary(
        self,
        *,
        org_id: str,
        scope: DevScope,
        current: ChangeWindow,
        comparison: ChangeWindow,
        limit: int,
    ) -> RawChangeSummary:
        del org_id, scope, current, comparison, limit
        return RawChangeSummary(changes=(), source_refs=self.source_refs)


@pytest.mark.asyncio
async def test_ci_acceptance_checks_on_one_run_do_not_collide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query_dicts(_client: Any, sql: str, params: dict[str, Any]) -> Any:
        if (
            "FROM ci_pipeline_runs" in sql
            and params.get("entity_id") == "repo_dev_health#ci2001"
        ):
            return [
                {
                    "entity_id": "repo_dev_health#ci2001",
                    "display_label": "CI run 2001",
                    "excerpt": "Status: mixed.",
                    "provenance": "native",
                    "observed_at": FRESH_OBSERVED,
                    "last_synced": FRESH_OBSERVED,
                    "repository_id": "repo_dev_health",
                    "source_url": "",
                    "deleted": 0,
                    "confidence": 1.0,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_evidence.query_dicts", fake_query_dicts
    )

    runtime = await _build_runtime(
        monkeypatch, status_source=_FakeCiAcceptanceStatusChangeSource()
    )
    scope = _scope()
    execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_01",
            tool_id=ToolID.STATUS_SNAPSHOT,
            scope=scope,
            limit=25,
        ),
        _context(scope),
    )
    result = execution.result
    assert {fact.conclusion for fact in result.ci_checks} == {"success", "failure"}
    evidence_ids = [fact.evidence_ref_ids[0] for fact in result.ci_checks]
    assert len(set(evidence_ids)) == 2, (
        "two distinct acceptance checks on the same run minted the same "
        "evidence_ref_id -- one check's evidence silently overwrote the "
        "other's"
    )

    get_execution = await runtime.registry.execute(
        DevToolRequest(
            schema_version="dev_tool_request.v1",
            run_id="run_01",
            tool_call_id="tool_call_02",
            tool_id=ToolID.GET_EVIDENCE,
            scope=scope,
            evidence_ref_ids=evidence_ids,
            limit=10,
        ),
        _context(scope),
    )
    texts = [fact.text for fact in get_execution.result.status_facts]
    assert len(texts) == 2
    assert all("Status: mixed." in text for text in texts)

    await runtime.aclose()
