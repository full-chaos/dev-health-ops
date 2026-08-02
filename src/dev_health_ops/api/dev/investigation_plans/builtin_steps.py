"""Step implementations for the six core plans (CHAOS-3295).

Every step here calls exactly one existing canonical service
(``StatusChangeService``, ``MetricQueryService``, ``WorkGraphNeighborsService``,
``DataHealthService``, ``EvidenceService``) through the narrow
:class:`PlanExecutorRuntime` port -- never a parallel query, never the
provider tool registry. Production wiring (``production_runtime.py``)
implements the port over the exact service instances
``_assemble_production_runtime`` already constructs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Protocol

from ..contracts import DevScope, MetricID
from ..contracts_v2.base import SourceClass, SourceRequirementState
from ..data_health_service import DataHealthResult
from ..metrics.definitions import MetricDefinition
from ..metrics.service import MetricQueryResult
from ..status_change_service import ChangeSummaryResult, StatusSnapshotResult
from ..work_graph_neighbors_service import WorkGraphNeighborsResult
from .state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
    data_health_state_to_requirement_state,
    metric_data_state_to_requirement_state,
    queried_semantics,
    status_result_state_to_requirement_state,
    work_graph_result_state_to_requirement_state,
)
from .steps import PlanStepDefinition, StepContext, StepOutcome, StepRegistry

__all__ = ["PlanExecutorRuntime", "register_builtin_steps"]

#: The exact warning token ``status_change_service.status_snapshot`` appends
#: (status_change_service.py) when ``assessment_source_limit_reached`` fires
#: -- MAX_STATUS_ASSESSMENT_ITEMS was hit on at least one raw fact category.
#: No dedicated field carries this signal, so it is recognized here by its
#: pinned warning string.
_ASSESSMENT_SOURCE_BOUND_WARNING = "status assessment source bound reached"


class PlanExecutorRuntime(Protocol):
    """The exact canonical-service surface builtin steps are allowed to call."""

    async def status_snapshot(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> StatusSnapshotResult: ...

    async def change_summary(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> ChangeSummaryResult: ...

    def list_metrics(self, scope: DevScope) -> Sequence[MetricDefinition]: ...

    async def query_metric(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        metric_id: str,
        scope: DevScope,
    ) -> MetricQueryResult: ...

    async def work_graph_neighbors(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> WorkGraphNeighborsResult: ...

    async def data_health(
        self, *, org_id: str, permission_fingerprint: str, scope: DevScope
    ) -> DataHealthResult:
        """Enumerate required source health for ``scope`` (trust.data.v1 core call)."""
        ...


def _work_graph_applicable(ctx: StepContext) -> bool:
    """Only entities the work graph actually indexes have neighbors at all.

    Mirrors ``production_runtime._work_graph_roots``'s own kind filter
    (issue/pull_request, aliased to "pr" -- a project/work_unit/team never
    resolves to a work-graph root) so the applicability predicate agrees
    with what the runtime adapter can actually find roots for.
    """

    return any(
        ref.entity_type.value in {"issue", "pull_request"}
        for ref in ctx.scope.entity_refs
    )


def _requested_metrics_applicable(ctx: StepContext) -> bool:
    return bool(ctx.requested_metric_ids)


def _usability_requested_applicable(ctx: StepContext) -> bool:
    # v1 scope: readiness/data-health enrichment always runs alongside the
    # metric catalog listing rather than depending on unmodeled per-request
    # "usability requested" signal; a future caller-supplied flag can narrow
    # this predicate without touching the registry or executor.
    del ctx
    return True


def _data_health_outcome(result: DataHealthResult) -> StepOutcome:
    if not result.sources:
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics="no_data",
            usable_fact_count=0,
        )
    # Worst-case (least available) source wins the aggregate observation --
    # a plan-level "is data trustworthy" answer cannot be better than its
    # weakest required source.
    _STATE_SEVERITY = {
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE: 0,
        SourceRequirementState.UNAVAILABLE: 1,
        SourceRequirementState.UNCONFIGURED: 2,
        SourceRequirementState.AVAILABLE_STALE: 3,
        SourceRequirementState.AVAILABLE_UNKNOWN: 4,
        SourceRequirementState.AVAILABLE_CURRENT: 5,
        SourceRequirementState.NOT_APPLICABLE: 6,
        SourceRequirementState.TRUNCATED: 3,
    }
    mapped = [data_health_state_to_requirement_state(s.state) for s in result.sources]
    worst = min(mapped, key=lambda state: _STATE_SEVERITY[state])
    usable = sum(1 for s in result.sources if s.state.value == "complete")
    watermark = max(
        (s.watermark for s in result.sources if s.watermark is not None), default=None
    )
    if worst in {
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNCONFIGURED,
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
    }:
        return StepOutcome(
            observed_state=worst,
            data_semantics="not_measured",
            usable_fact_count=0,
            watermark=watermark,
            limitation=f"required_source_{worst.value}",
        )
    return StepOutcome(
        observed_state=worst,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        watermark=watermark,
        subject_coverage=(
            sum(s.coverage for s in result.sources) / len(result.sources)
        ),
    )


def _status_mapped_outcome(
    state: SourceRequirementState,
    *,
    usable_fact_count: int,
    limitation: str,
    watermark=None,
    query_version: str = "unversioned",
) -> StepOutcome:
    """Build a ``StepOutcome`` from a state already mapped through this
    module's ``state_mapping`` functions.

    Codex finding (HIGH, 2026-08-01): ``StatusResultState.
    INSUFFICIENT_EVIDENCE`` maps to ``SourceRequirementState.UNAVAILABLE``, an
    unmeasured state -- reporting queried semantics for it (a positive
    ``usable_fact_count``, no ``limitation``) fails ``DevSourceObservation``'s
    own "a source that was not fully measured requires a bounded limitation"
    validator, which the orchestrator's outer exception handler then turns
    into a user-visible ``internal_error`` instead of a typed unavailable
    observation. Every caller that maps a canonical result state through
    ``state_mapping`` and then builds a ``StepOutcome`` must route through
    here rather than assuming the mapped state is always queryable.
    """

    if state in UNMEASURED_REQUIREMENT_STATES:
        return StepOutcome(
            observed_state=state,
            data_semantics="not_measured",
            usable_fact_count=0,
            limitation=limitation,
            watermark=watermark,
            query_version=query_version,
        )
    return StepOutcome(
        observed_state=state,
        data_semantics=queried_semantics(usable_fact_count),
        usable_fact_count=usable_fact_count,
        watermark=watermark,
        query_version=query_version,
    )


def register_builtin_steps(
    registry: StepRegistry, runtime: PlanExecutorRuntime
) -> None:
    """Populate ``registry`` with every step the six core plans declare."""

    async def status_snapshot_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.status_snapshot(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        # Recon finding (team-lead, 3297 grounding): MAX_STATUS_ASSESSMENT_ITEMS
        # firing (status_change_service.py's `assessment_source_limit_reached`)
        # has no dedicated SourceRequirementState field of its own -- if this
        # executor doesn't map it, a completion block downstream would treat a
        # truncated assessment as a complete one. TRUNCATED is an unmeasured
        # state (usable_fact_count must be 0), so this deliberately discards
        # the partial fact count in favor of disclosing the truncation.
        if _ASSESSMENT_SOURCE_BOUND_WARNING in result.warnings:
            return StepOutcome(
                observed_state=SourceRequirementState.TRUNCATED,
                data_semantics="not_measured",
                usable_fact_count=0,
                limitation="assessment_source_limit_reached",
            )
        usable = (
            (1 if result.declared else 0) + len(result.children) + len(result.blockers)
        )
        state = status_result_state_to_requirement_state(result.state)
        return _status_mapped_outcome(
            state,
            usable_fact_count=usable,
            limitation="status_snapshot_insufficient_evidence",
            watermark=max(
                (ref.watermark for ref in result.source_refs if ref.watermark),
                default=None,
            ),
            query_version="status-snapshot.v1",
        )

    async def change_summary_run(ctx: StepContext) -> StepOutcome:
        if ctx.scope.comparison_range is None:
            return StepOutcome(
                observed_state=SourceRequirementState.UNAVAILABLE,
                data_semantics="not_measured",
                usable_fact_count=0,
                limitation="comparison_window_unavailable",
            )
        result = await runtime.change_summary(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        usable = len(result.changes)
        return _status_mapped_outcome(
            status_result_state_to_requirement_state(result.state),
            usable_fact_count=usable,
            limitation="change_summary_insufficient_evidence",
            query_version="change-summary.v1",
        )

    async def required_source_health_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.data_health(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        return _data_health_outcome(result)

    async def work_graph_expansion_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.work_graph_neighbors(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        # Recon finding: `WorkGraphNeighborsResult.truncated` is the precise
        # bounds-exceeded signal (MAX_NEIGHBORS hit) -- distinct from the
        # coarser `state` enum, which conflates PARTIAL with "some edges
        # returned but not truncated" in principle. Truncation always wins.
        if result.truncated:
            return StepOutcome(
                observed_state=SourceRequirementState.TRUNCATED,
                data_semantics="not_measured",
                usable_fact_count=0,
                watermark=result.watermark,
                limitation="work_graph_result_truncated",
                query_version=result.query_version,
            )
        usable = len(result.edges)
        return StepOutcome(
            observed_state=work_graph_result_state_to_requirement_state(result.state),
            data_semantics=queried_semantics(usable),
            usable_fact_count=usable,
            watermark=result.watermark,
            query_version=result.query_version,
        )

    async def list_metrics_run(ctx: StepContext) -> StepOutcome:
        definitions = list(runtime.list_metrics(ctx.scope))
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics=queried_semantics(len(definitions)),
            usable_fact_count=len(definitions),
            query_version="list-metrics.v1",
        )

    async def readiness_data_health_run(ctx: StepContext) -> StepOutcome:
        result = await runtime.data_health(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
        )
        return _data_health_outcome(result)

    def registered_metrics_present_applicable(ctx: StepContext) -> bool:
        """Codex finding (MEDIUM, 2026-08-01): the plan declares this step
        conditional on ``registered_metrics_present.v1``, but registration
        previously hardcoded ``applicable=True`` -- an empty metric catalog
        then ran the step anyway and recorded UNAVAILABLE/
        ``all_requested_metrics_failed``, misreporting an absent optional
        source as an answer-completeness gap instead of skipping it as
        NOT_APPLICABLE. Applicability must be derived from the same catalog
        the step itself would query.
        """

        return bool(runtime.list_metrics(ctx.scope))

    async def registered_metric_deltas_run(ctx: StepContext) -> StepOutcome:
        definitions = list(runtime.list_metrics(ctx.scope))
        results = await asyncio.gather(
            *(
                runtime.query_metric(
                    org_id=ctx.org_id,
                    permission_fingerprint=ctx.permission_fingerprint,
                    metric_id=definition.metric_id.value,
                    scope=ctx.scope,
                )
                for definition in definitions
            ),
            return_exceptions=True,
        )
        return _combined_metric_outcome(results, query_version="query-metric.v1")

    async def registered_metric_query_run(ctx: StepContext) -> StepOutcome:
        metric_ids = ctx.requested_metric_ids or tuple(
            MetricID(definition.metric_id).value
            for definition in runtime.list_metrics(ctx.scope)
        )
        results = await asyncio.gather(
            *(
                runtime.query_metric(
                    org_id=ctx.org_id,
                    permission_fingerprint=ctx.permission_fingerprint,
                    metric_id=metric_id,
                    scope=ctx.scope,
                )
                for metric_id in metric_ids
            ),
            return_exceptions=True,
        )
        return _combined_metric_outcome(results, query_version="query-metric.v1")

    registry.register(
        PlanStepDefinition(
            step_id="status_snapshot",
            plan_id="status.entity.v2",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.status_snapshot.v1",
            requirement_level="mandatory",
            run=status_snapshot_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="required_source_health",
            plan_id="status.entity.v2",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            run=required_source_health_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="work_graph_expansion",
            plan_id="status.entity.v2",
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="work_graph_neighbors_service.neighbors.v1",
            requirement_level="conditional",
            run=work_graph_expansion_run,
            applicable=_work_graph_applicable,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="evidence_expansion",
            plan_id="status.entity.v2",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="evidence_service.search.v1",
            requirement_level="conditional",
            run=_evidence_expansion_run,
            applicable=lambda _ctx: False,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="status_snapshot",
            plan_id="work.remaining.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.status_snapshot.v1",
            requirement_level="mandatory",
            run=status_snapshot_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="work_graph_expansion",
            plan_id="work.remaining.v1",
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="work_graph_neighbors_service.neighbors.v1",
            requirement_level="conditional",
            run=work_graph_expansion_run,
            applicable=_work_graph_applicable,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="change_summary",
            plan_id="change.observed.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.change_summary.v1",
            requirement_level="mandatory",
            run=change_summary_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="required_source_health",
            plan_id="change.observed.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            run=required_source_health_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="registered_metric_deltas",
            plan_id="change.observed.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.query_metric.v1",
            requirement_level="conditional",
            run=registered_metric_deltas_run,
            applicable=registered_metrics_present_applicable,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="list_metrics",
            plan_id="statistics.registered.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.list_metrics.v1",
            requirement_level="mandatory",
            run=list_metrics_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="readiness_data_health",
            plan_id="statistics.registered.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="conditional",
            run=readiness_data_health_run,
            applicable=_usability_requested_applicable,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="registered_metric_query",
            plan_id="metric.comparison.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.query_metric.v1",
            requirement_level="mandatory",
            run=registered_metric_query_run,
        )
    )

    registry.register(
        PlanStepDefinition(
            step_id="required_source_health",
            plan_id="trust.data.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            run=required_source_health_run,
        )
    )


def _combined_metric_outcome(
    results: Sequence[MetricQueryResult | BaseException], *, query_version: str
) -> StepOutcome:
    """One combined observation across every requested metric (batched_fan_out)."""

    successful = [item for item in results if not isinstance(item, BaseException)]
    if not successful:
        return StepOutcome(
            observed_state=SourceRequirementState.UNAVAILABLE,
            data_semantics="not_measured",
            usable_fact_count=0,
            limitation="all_requested_metrics_failed",
            query_version=query_version,
        )
    mapped = [metric_data_state_to_requirement_state(item.state) for item in successful]
    _SEVERITY = {
        SourceRequirementState.UNAVAILABLE: 0,
        SourceRequirementState.UNCONFIGURED: 1,
        SourceRequirementState.AVAILABLE_STALE: 2,
        SourceRequirementState.AVAILABLE_UNKNOWN: 3,
        SourceRequirementState.AVAILABLE_CURRENT: 4,
    }
    worst = min(mapped, key=lambda state: _SEVERITY.get(state, 0))
    usable = sum(len(item.values) for item in successful)
    if worst in {
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNCONFIGURED,
    }:
        return StepOutcome(
            observed_state=worst,
            data_semantics="not_measured",
            usable_fact_count=0,
            limitation=f"required_metric_{worst.value}",
            query_version=query_version,
        )
    return StepOutcome(
        observed_state=worst,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        query_version=query_version,
    )


async def _evidence_expansion_run(ctx: StepContext) -> StepOutcome:
    """Placeholder for a future direct evidence-service wiring.

    Registered separately from :func:`register_builtin_steps`'s runtime
    closures because evidence expansion needs a query string this
    deterministic executor deliberately never has (``StepContext`` carries
    no raw question text, by design). v1 scope: report the step
    not-applicable-by-construction rather than call
    ``EvidenceService.search`` with a fabricated query -- prioritized
    evidence enrichment remains available to the model's bounded optional
    enrichment round when ``plan.enrichment_allowed`` is true.
    """

    del ctx
    return StepOutcome(
        observed_state=SourceRequirementState.NOT_APPLICABLE,
        data_semantics="not_measured",
        usable_fact_count=0,
        limitation="deterministic_query_text_unavailable",
    )
