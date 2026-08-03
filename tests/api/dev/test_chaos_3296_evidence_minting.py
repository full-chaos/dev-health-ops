"""Evidence-minting controls (CHAOS-3296): ``DevSourceObservation.content``.

CHAOS-3295 landed ``DevSourceContent`` as a typed slot every builtin step's
``StepOutcome`` can fill, but no step actually minted it -- every
``content`` field was structurally always ``None`` (``StepOutcome.content``
default, never overridden by any of the six core plans' step closures in
``investigation_plans/builtin_steps.py``). This is this issue's first
deliverable: populate it from the exact canonical-service results the steps
already fetch, with real signer-issued evidence (never a fabricated/forged
handle), reusing the same ``EvidenceReferenceSigner``/``_mint_evidence``
primitive ``production_runtime.py``'s proven v1 tool-call path already uses
-- never a second, parallel evidence-issuing mechanism.

Positive: a queried, non-empty canonical result mints exactly the matching
``DevSourceContent`` slot, every fact carries at least one real
``ev1_``-shaped evidence handle, and every other slot stays an empty tuple
(never omitted -- the CHAOS-3295 docstring's own invariant).
Negative: an unmeasured/empty/failed step never carries content, and a
metric with zero returned values mints no metric refs.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import (
    ClaimKind,
    DevEntityRef,
    DevScope,
    DirectScope,
    EntityType,
    FreshnessState,
    MetricID,
)
from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.investigation_plans import (
    StepRegistry,
    register_builtin_steps,
)
from dev_health_ops.api.dev.metrics.definitions import MetricDefinition
from dev_health_ops.api.dev.metrics.service import (
    MetricDataState,
    MetricQueryResult,
    MetricQueryValue,
    MetricSourceRef,
)
from dev_health_ops.api.dev.production_runtime import _mint_evidence
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    ChangeCategory,
    ChangeSummaryResult,
    ChangeWindow,
    CIFact,
    CompletionState,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    StatusFact,
    StatusResultState,
    StatusSnapshotResult,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    QUERY_VERSION as WORK_GRAPH_QUERY_VERSION,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    SCHEMA_VERSION as WORK_GRAPH_SCHEMA_VERSION,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    GraphDirection,
    WorkGraphNeighborEdge,
    WorkGraphNeighborsResult,
    WorkGraphResultState,
)
from tests._chaos_3295_plan_executor import (
    FakePlanExecutorRuntime,
    project_scope,
    step_context_for,
)

OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
_SIGNER = EvidenceReferenceSigner("chaos-3296-test-secret-0123456789abcdef")


def _evidence_handle_shaped(value: str) -> bool:
    return value.startswith("ev1_") and len(value) == 44


class ContentFakeRuntime(FakePlanExecutorRuntime):
    """Extends the shared 3295 fake with real signer-issued evidence.

    Reuses ``production_runtime._mint_evidence`` -- the exact primitive the
    proven v1 tool-call path issues evidence through -- rather than a second
    fabricated stub, so a collision or malformed-handle bug in the real
    minting code would show up here too.
    """

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.mint_evidence_calls = 0
        self.status_result: StatusSnapshotResult | None = None
        self.change_result: ChangeSummaryResult | None = None
        self.work_graph_result: WorkGraphNeighborsResult | None = None
        self.metric_results: list[MetricQueryResult] = []

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        self.status_snapshot_calls += 1
        if self._status_snapshot_fails:
            raise RuntimeError("status source unavailable")
        assert self.status_result is not None
        return self.status_result

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        self.change_summary_calls += 1
        assert self.change_result is not None
        return self.change_result

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        self.work_graph_calls += 1
        assert self.work_graph_result is not None
        return self.work_graph_result

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        self.query_metric_calls += 1
        for result in self.metric_results:
            if result.definition.metric_id.value == metric_id:
                return result
        raise AssertionError(f"no fixture metric result for {metric_id!r}")

    def mint_evidence(
        self,
        *,
        org_id,
        source_system,
        source_version,
        entity_type,
        entity_id,
        display_label,
        observed_at,
        freshness,
        confidence=1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ):
        self.mint_evidence_calls += 1
        ref = _mint_evidence(
            _SIGNER,
            org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness,
            confidence=confidence,
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )
        return ref.evidence_ref_id


def _registry(runtime: ContentFakeRuntime) -> StepRegistry:
    registry = StepRegistry()
    register_builtin_steps(registry, runtime)
    return registry


@pytest.mark.asyncio
async def test_status_snapshot_mints_content_across_every_matching_slot():
    runtime = ContentFakeRuntime()
    runtime.status_result = StatusSnapshotResult(
        contract_version="status_snapshot.v1",
        state=StatusResultState.COMPLETE,
        scope=project_scope(),
        as_of=OBSERVED_AT,
        declared=StatusFact(
            entity_type="project",
            entity_id="project-ask-dev",
            display_label="Ask Dev",
            status="in_progress",
            observed_at=OBSERVED_AT,
            source_ref_id="ref-declared",
            evidence_ref_ids=(),
        ),
        actual=ActualCompletion(
            state=CompletionState.NOT_READY,
            rule_id="actual-completion",
            rule_version="v4",
            reason_codes=("required_child_incomplete",),
            required_children=(
                StatusFact(
                    entity_type="issue",
                    entity_id="issue-1",
                    display_label="Issue One",
                    status="open",
                    observed_at=OBSERVED_AT,
                    source_ref_id="ref-child",
                    evidence_ref_ids=(),
                ),
            ),
            required_child_total=1,
            required_child_complete=0,
            display_truncated=False,
            conflicts=(),
            source_ref_ids=(),
            evidence_ref_ids=(),
        ),
        children=(),
        blockers=(),
        pull_requests=(
            PullRequestFact(
                entity_id="pr-1",
                display_label="Fix bug",
                state="open",
                review_state="approved",
                changes_requested=0,
                merged=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-pr",
                evidence_ref_ids=(),
                required=True,
            ),
        ),
        ci=(
            CIFact(
                entity_id="ci-1#check123",
                display_label="build",
                conclusion="success",
                required=True,
                skipped_required_work=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-ci",
                evidence_ref_ids=(),
            ),
        ),
        deployments=(
            DeploymentFact(
                entity_id="deploy-1",
                display_label="prod",
                status="succeeded",
                environment="production",
                required=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-deploy",
                evidence_ref_ids=(),
            ),
        ),
        incidents=(
            IncidentFact(
                entity_id="incident-1",
                display_label="Outage",
                status="resolved",
                active=False,
                blocking=False,
                observed_at=OBSERVED_AT,
                source_ref_id="ref-incident",
                evidence_ref_ids=(),
            ),
        ),
        source_refs=(),
        warnings=(),
    )
    registry = _registry(runtime)
    ctx = step_context_for()
    outcome = await registry.get("status.entity.v2", "status_snapshot").run(ctx)

    assert outcome.content is not None
    content = outcome.content
    assert (
        len(content.status_facts) == 1
    )  # declared only (children/blockers empty here)
    assert len(content.required_children) == 1
    assert len(content.pull_requests) == 1
    assert len(content.ci_checks) == 1
    assert len(content.deployments) == 1
    assert len(content.incidents) == 1
    # Every populated fact carries at least one real, signer-issued handle.
    for group in (
        content.status_facts,
        content.required_children,
        content.pull_requests,
        content.ci_checks,
        content.deployments,
        content.incidents,
    ):
        for fact in group:
            assert fact.evidence_ref_ids
            for evidence_id in fact.evidence_ref_ids:
                assert _evidence_handle_shaped(evidence_id)
    # Every non-matching slot stays an empty tuple, never omitted.
    assert content.graph_edges == ()
    assert content.observed_changes == ()
    assert content.metric_refs == ()
    assert runtime.mint_evidence_calls > 0


@pytest.mark.asyncio
async def test_change_summary_mints_observed_changes_not_status_facts():
    runtime = ContentFakeRuntime()
    runtime.change_result = ChangeSummaryResult(
        contract_version="change_summary.v1",
        state=StatusResultState.COMPLETE,
        current_window=ChangeWindow(OBSERVED_AT, OBSERVED_AT),
        comparison_window=ChangeWindow(OBSERVED_AT, OBSERVED_AT),
        changes=(
            ObservedChange(
                change_id="change-1",
                category=ChangeCategory.STATUS,
                entity_type="project",
                entity_id="project-ask-dev",
                display_label="Ask Dev",
                before="planned",
                after="in_progress",
                observed_at=OBSERVED_AT,
                claim_kind=ClaimKind.OBSERVED,
                relationship_chain=(),
                metric_id=None,
                metric_value=None,
                metric_comparison_value=None,
                source_ref_ids=(),
                evidence_ref_ids=(),
            ),
        ),
        source_refs=(),
        warnings=(),
    )
    scope = project_scope()
    scope = scope.model_copy(
        update={
            "comparison_range": scope.time_range,
        }
    )
    ctx = step_context_for(scope=scope)
    registry = _registry(runtime)
    outcome = await registry.get("change.observed.v1", "change_summary").run(ctx)

    assert outcome.content is not None
    assert len(outcome.content.observed_changes) == 1
    assert outcome.content.status_facts == ()
    change = outcome.content.observed_changes[0]
    assert change.evidence_ref_ids
    assert all(_evidence_handle_shaped(v) for v in change.evidence_ref_ids)


@pytest.mark.asyncio
async def test_work_graph_expansion_mints_graph_edges():
    runtime = ContentFakeRuntime()
    runtime.work_graph_result = WorkGraphNeighborsResult(
        schema_version=WORK_GRAPH_SCHEMA_VERSION,
        state=WorkGraphResultState.COMPLETE,
        nodes=(),
        edges=(
            WorkGraphNeighborEdge(
                edge_id="edge-1",
                source_type="issue",
                source_id="issue-1",
                target_type="pr",
                target_id="pr-1",
                relationship_type="references",
                direction=GraphDirection.BOTH,
                provenance="persisted",
                confidence=0.9,
                source_ref_id="ref-edge",
                observed_at=OBSERVED_AT,
            ),
        ),
        source_refs=(),
        warnings=(),
        total_count=1,
        returned_count=1,
        truncated=False,
        depth=1,
        query_version=WORK_GRAPH_QUERY_VERSION,
        watermark=None,
    )
    base = project_scope()
    scope = DevScope(
        schema_version="dev_scope.v1",
        organization_id=base.organization_id,
        direct_scope=DirectScope.ISSUE,
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.ISSUE,
                entity_id="issue-1",
                display_label="Issue One",
            )
        ],
        time_range=base.time_range,
    )
    ctx = step_context_for(scope=scope)
    registry = _registry(runtime)
    outcome = await registry.get("status.entity.v2", "work_graph_expansion").run(ctx)

    assert outcome.content is not None
    assert len(outcome.content.graph_edges) == 1
    edge = outcome.content.graph_edges[0]
    assert edge.source_entity_id == "issue-1"
    assert edge.target_entity_id == "pr-1"
    assert edge.evidence_ref_ids
    assert all(_evidence_handle_shaped(v) for v in edge.evidence_ref_ids)


@pytest.mark.asyncio
async def test_registered_metric_query_mints_metric_refs():
    definition = MetricDefinition(
        metric_id=MetricID.CYCLE_TIME_P50_HOURS,
        label="Cycle Time",
        owner="ask-dev",
        description="desc",
        definition_version="v1",
        source_table="work_items",
        source_version="v1",
        query_version="v1",
        unit="days",
        aggregation="avg",
        display_precision=1,
        null_semantics="no_data",
        zero_semantics="measured_zero",
        supported_scopes=(),
        supports_team_filter=False,
        supported_dimensions=(),
        min_range_days=1,
        max_range_days=90,
        supported_presets=(),
        supported_time_grains=("day",),
        comparison_rule="prior_period",
        freshness_policy="p.v1",
        expected_materialization="daily",
        upstream_sources=("work_items",),
        sensitivity="internal",
        entitlement="community",
    )
    metric_result = MetricQueryResult(
        definition=definition,
        state=MetricDataState.VALUE,
        freshness=FreshnessState.FRESH,
        values=(
            MetricQueryValue(
                dimensions=(),
                value=3.5,
                comparison_value=None,
                series=(),
            ),
        ),
        coverage=1.0,
        current_window_start=OBSERVED_AT,
        current_window_end=OBSERVED_AT,
        comparison_window_start=None,
        comparison_window_end=None,
        watermark=OBSERVED_AT,
        source_refs=(
            MetricSourceRef(
                ref_id="ref-metric",
                source_table="work_items",
                source_version="v1",
                watermark=OBSERVED_AT,
                query_version="v1",
            ),
        ),
    )
    runtime = ContentFakeRuntime(metric_definitions=(definition,))
    runtime.metric_results = [metric_result]
    ctx = step_context_for(requested_metric_ids=(MetricID.CYCLE_TIME_P50_HOURS.value,))
    registry = _registry(runtime)
    outcome = await registry.get("metric.comparison.v1", "registered_metric_query").run(
        ctx
    )

    assert outcome.content is not None
    assert len(outcome.content.metric_refs) == 1
    ref = outcome.content.metric_refs[0]
    assert ref.evidence_ref_ids
    assert all(_evidence_handle_shaped(v) for v in ref.evidence_ref_ids)


@pytest.mark.asyncio
async def test_a_failed_status_snapshot_never_carries_content():
    runtime = ContentFakeRuntime(status_snapshot_fails=True)
    registry = _registry(runtime)
    ctx = step_context_for()

    with pytest.raises(RuntimeError):
        await registry.get("status.entity.v2", "status_snapshot").run(ctx)
    assert runtime.mint_evidence_calls == 0
