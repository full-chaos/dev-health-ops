"""GraphQL adapters for the shared Ask Dev status/change services.

Rule and fixture versions are recorded in
``.github/ask-dev/status-change-rule-manifest.md``. GraphQL only projects the
application-service result; it never owns completion or change semantics.
"""

from __future__ import annotations

from typing import cast

import strawberry

from dev_health_ops.api.dev.metrics.clickhouse import ClickHouseMetricSource
from dev_health_ops.api.dev.metrics.service import MetricQueryService
from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource
from dev_health_ops.api.dev.status_change_service import (
    ActualCompletion,
    ChangeSummaryRequest,
    ChangeSummaryResult,
    CIFact,
    DeploymentFact,
    IncidentFact,
    ObservedChange,
    PullRequestFact,
    SourceReference,
    StatusChangeService,
    StatusConflict,
    StatusFact,
    StatusSnapshotRequest,
    StatusSnapshotResult,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.dev_metric import DevMetricID
from ..types.dev_status_change import (
    DevActualCompletion,
    DevChangeSummary,
    DevChangeSummaryInput,
    DevChangeWindow,
    DevCIStatusFact,
    DevCompletionState,
    DevDeploymentStatusFact,
    DevIncidentStatusFact,
    DevObservedChange,
    DevPullRequestStatusFact,
    DevStatusConflict,
    DevStatusFact,
    DevStatusScope,
    DevStatusScopeEntity,
    DevStatusSnapshot,
    DevStatusSnapshotInput,
    DevStatusSourceRef,
)
from . import dev_entitlement
from .dev_metric import resolve_dev_metric_scope
from .dev_scope import permission_fingerprint


def _status_change_service(context: GraphQLContext) -> StatusChangeService:
    injected = getattr(context, "dev_status_change_service", None)
    if injected is not None:
        return cast(StatusChangeService, injected)
    if context.client is None:
        raise RuntimeError("Database client not available for Ask Dev status")
    return StatusChangeService(
        ClickHouseStatusChangeSource(context.client),
        metric_service=MetricQueryService(ClickHouseMetricSource(context.client)),
    )


async def resolve_dev_status_snapshot(
    context: GraphQLContext, input: DevStatusSnapshotInput
) -> DevStatusSnapshot:
    org_id = require_org_id(context)
    fingerprint = permission_fingerprint(context)
    await dev_entitlement.require_ask_dev_entitlement(org_id)
    scope = await resolve_dev_metric_scope(context, input.scope)
    result = await _status_change_service(context).status_snapshot(
        org_id,
        fingerprint,
        StatusSnapshotRequest(
            scope=scope,
            as_of=input.as_of,
            max_items=input.max_items,
        ),
    )
    return _status_snapshot(result)


async def resolve_dev_change_summary(
    context: GraphQLContext, input: DevChangeSummaryInput
) -> DevChangeSummary:
    org_id = require_org_id(context)
    fingerprint = permission_fingerprint(context)
    await dev_entitlement.require_ask_dev_entitlement(org_id)
    scope = await resolve_dev_metric_scope(context, input.scope)
    result = await _status_change_service(context).change_summary(
        org_id,
        fingerprint,
        ChangeSummaryRequest(
            scope=scope,
            current_start=scope.time_range.start,
            current_end=scope.time_range.end,
            comparison_start=input.comparison_start,
            comparison_end=input.comparison_end,
            max_items=input.max_items,
        ),
    )
    return _change_summary(result)


def _status_snapshot(result: StatusSnapshotResult) -> DevStatusSnapshot:
    return DevStatusSnapshot(
        contract_version=result.contract_version,
        state=result.state.value,
        scope=_scope(result.scope),
        as_of=result.as_of,
        declared=_status_fact(result.declared) if result.declared else None,
        actual=_actual(result.actual),
        children=[_status_fact(item) for item in result.children],
        blockers=[_status_fact(item) for item in result.blockers],
        pull_requests=[_pull_request(item) for item in result.pull_requests],
        ci=[_ci(item) for item in result.ci],
        deployments=[_deployment(item) for item in result.deployments],
        incidents=[_incident(item) for item in result.incidents],
        source_refs=[_source_ref(item) for item in result.source_refs],
        warnings=list(result.warnings),
    )


def _change_summary(result: ChangeSummaryResult) -> DevChangeSummary:
    return DevChangeSummary(
        contract_version=result.contract_version,
        state=result.state.value,
        current_window=DevChangeWindow(
            start=result.current_window.start, end=result.current_window.end
        ),
        comparison_window=DevChangeWindow(
            start=result.comparison_window.start, end=result.comparison_window.end
        ),
        changes=[_change(item) for item in result.changes],
        source_refs=[_source_ref(item) for item in result.source_refs],
        warnings=list(result.warnings),
    )


def _scope(scope: object) -> DevStatusScope:
    time_range = getattr(scope, "time_range")
    comparison = getattr(scope, "comparison_range")
    return DevStatusScope(
        schema_version=getattr(scope, "schema_version"),
        organization_id=strawberry.ID(getattr(scope, "organization_id")),
        direct_scope=getattr(scope, "direct_scope").value,
        repository_ids=[
            strawberry.ID(value) for value in getattr(scope, "repositories")
        ],
        entities=[
            DevStatusScopeEntity(
                entity_type=item.entity_type.value,
                entity_id=strawberry.ID(item.entity_id),
                display_label=item.display_label,
                repository_id=strawberry.ID(item.repository_id)
                if item.repository_id
                else None,
            )
            for item in getattr(scope, "entity_refs")
        ],
        team_ids=[strawberry.ID(value) for value in getattr(scope, "team_ids")],
        current_start=time_range.start,
        current_end=time_range.end,
        comparison_start=comparison.start if comparison else None,
        comparison_end=comparison.end if comparison else None,
        timezone=time_range.timezone,
    )


def _ids(values: tuple[str, ...]) -> list[strawberry.ID]:
    return [strawberry.ID(value) for value in values]


def _status_fact(item: StatusFact) -> DevStatusFact:
    return DevStatusFact(
        entity_type=item.entity_type,
        entity_id=strawberry.ID(item.entity_id),
        display_label=item.display_label,
        status=item.status,
        observed_at=item.observed_at,
        source_ref_id=strawberry.ID(item.source_ref_id),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
        required=item.required,
    )


def _pull_request(item: PullRequestFact) -> DevPullRequestStatusFact:
    return DevPullRequestStatusFact(
        entity_id=strawberry.ID(item.entity_id),
        display_label=item.display_label,
        state=item.state,
        review_state=item.review_state,
        changes_requested=item.changes_requested,
        merged=item.merged,
        observed_at=item.observed_at,
        source_ref_id=strawberry.ID(item.source_ref_id),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
        required=item.required,
    )


def _ci(item: CIFact) -> DevCIStatusFact:
    return DevCIStatusFact(
        entity_id=strawberry.ID(item.entity_id),
        display_label=item.display_label,
        conclusion=item.conclusion,
        required=item.required,
        skipped_required_work=item.skipped_required_work,
        observed_at=item.observed_at,
        source_ref_id=strawberry.ID(item.source_ref_id),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
    )


def _deployment(item: DeploymentFact) -> DevDeploymentStatusFact:
    return DevDeploymentStatusFact(
        entity_id=strawberry.ID(item.entity_id),
        display_label=item.display_label,
        status=item.status,
        environment=item.environment,
        required=item.required,
        observed_at=item.observed_at,
        source_ref_id=strawberry.ID(item.source_ref_id),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
    )


def _incident(item: IncidentFact) -> DevIncidentStatusFact:
    return DevIncidentStatusFact(
        entity_id=strawberry.ID(item.entity_id),
        display_label=item.display_label,
        status=item.status,
        active=item.active,
        blocking=item.blocking,
        observed_at=item.observed_at,
        source_ref_id=strawberry.ID(item.source_ref_id),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
    )


def _conflict(item: StatusConflict) -> DevStatusConflict:
    return DevStatusConflict(
        code=item.code,
        message=item.message,
        severity=item.severity.value,
        source_ref_ids=_ids(item.source_ref_ids),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
    )


def _actual(item: ActualCompletion) -> DevActualCompletion:
    return DevActualCompletion(
        state=DevCompletionState(item.state.value),
        rule_id=item.rule_id,
        rule_version=item.rule_version,
        reason_codes=list(item.reason_codes),
        required_children=[_status_fact(value) for value in item.required_children],
        required_child_total=item.required_child_total,
        required_child_complete=item.required_child_complete,
        display_truncated=item.display_truncated,
        conflicts=[_conflict(value) for value in item.conflicts],
        source_ref_ids=_ids(item.source_ref_ids),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
    )


def _source_ref(item: SourceReference) -> DevStatusSourceRef:
    return DevStatusSourceRef(
        ref_id=strawberry.ID(item.ref_id),
        source_system=item.source_system,
        source_version=item.source_version,
        freshness=item.freshness.value,
        watermark=item.watermark,
        evidence_ref_ids=_ids(item.evidence_ref_ids),
    )


def _change(item: ObservedChange) -> DevObservedChange:
    return DevObservedChange(
        change_id=strawberry.ID(item.change_id),
        category=item.category.value,
        entity_type=item.entity_type,
        entity_id=strawberry.ID(item.entity_id),
        display_label=item.display_label,
        before=item.before,
        after=item.after,
        observed_at=item.observed_at,
        claim_kind=item.claim_kind.value,
        relationship_chain=list(item.relationship_chain),
        metric_id=DevMetricID(item.metric_id.value) if item.metric_id else None,
        metric_value=item.metric_value,
        metric_comparison_value=item.metric_comparison_value,
        source_ref_ids=_ids(item.source_ref_ids),
        evidence_ref_ids=_ids(item.evidence_ref_ids),
        confidence=item.confidence,
    )
