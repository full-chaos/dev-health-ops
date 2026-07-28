"""Typed GraphQL projections for Ask Dev status and observed-change services."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

import strawberry

from .dev_metric import DevMetricID, DevMetricScopeInput


@strawberry.input
class DevStatusSnapshotInput:
    scope: DevMetricScopeInput
    as_of: datetime | None = None
    max_items: int = 100


@strawberry.input
class DevChangeSummaryInput:
    scope: DevMetricScopeInput
    comparison_start: datetime
    comparison_end: datetime
    max_items: int = 100


@strawberry.enum
class DevCompletionState(Enum):
    READY = "ready"
    NOT_READY = "not_ready"
    INDETERMINATE = "indeterminate"


@strawberry.type
class DevStatusFact:
    entity_type: str
    entity_id: strawberry.ID
    display_label: str
    status: str
    observed_at: datetime
    source_ref_id: strawberry.ID
    evidence_ref_ids: list[strawberry.ID]
    required: bool


@strawberry.type
class DevPullRequestStatusFact:
    entity_id: strawberry.ID
    display_label: str
    state: str
    review_state: str | None
    changes_requested: int
    merged: bool
    observed_at: datetime
    source_ref_id: strawberry.ID
    evidence_ref_ids: list[strawberry.ID]
    required: bool


@strawberry.type
class DevCIStatusFact:
    entity_id: strawberry.ID
    display_label: str
    conclusion: str
    required: bool | None
    skipped_required_work: bool | None
    observed_at: datetime
    source_ref_id: strawberry.ID
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevDeploymentStatusFact:
    entity_id: strawberry.ID
    display_label: str
    status: str
    environment: str | None
    required: bool
    observed_at: datetime
    source_ref_id: strawberry.ID
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevIncidentStatusFact:
    entity_id: strawberry.ID
    display_label: str
    status: str
    active: bool
    blocking: bool
    observed_at: datetime
    source_ref_id: strawberry.ID
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevStatusConflict:
    code: str
    message: str
    severity: str
    source_ref_ids: list[strawberry.ID]
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevActualCompletion:
    state: DevCompletionState
    rule_id: str
    rule_version: str
    reason_codes: list[str]
    required_children: list[DevStatusFact]
    conflicts: list[DevStatusConflict]
    source_ref_ids: list[strawberry.ID]
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevStatusSourceRef:
    ref_id: strawberry.ID
    source_system: str
    source_version: str
    freshness: str
    watermark: datetime | None
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevStatusScopeEntity:
    entity_type: str
    entity_id: strawberry.ID
    display_label: str
    repository_id: strawberry.ID | None


@strawberry.type
class DevStatusScope:
    schema_version: str
    organization_id: strawberry.ID
    direct_scope: str
    repository_ids: list[strawberry.ID]
    entities: list[DevStatusScopeEntity]
    team_ids: list[strawberry.ID]
    current_start: datetime
    current_end: datetime
    comparison_start: datetime | None
    comparison_end: datetime | None
    timezone: str


@strawberry.type
class DevStatusSnapshot:
    contract_version: str
    state: str
    scope: DevStatusScope
    as_of: datetime
    declared: DevStatusFact | None
    actual: DevActualCompletion
    blockers: list[DevStatusFact]
    pull_requests: list[DevPullRequestStatusFact]
    ci: list[DevCIStatusFact]
    deployments: list[DevDeploymentStatusFact]
    incidents: list[DevIncidentStatusFact]
    source_refs: list[DevStatusSourceRef]
    warnings: list[str]


@strawberry.type
class DevChangeWindow:
    start: datetime
    end: datetime


@strawberry.type
class DevObservedChange:
    change_id: strawberry.ID
    category: str
    entity_type: str
    entity_id: strawberry.ID
    display_label: str
    before: str | None
    after: str | None
    observed_at: datetime
    claim_kind: str
    relationship_chain: list[str]
    metric_id: DevMetricID | None
    metric_value: float | None
    metric_comparison_value: float | None
    source_ref_ids: list[strawberry.ID]
    evidence_ref_ids: list[strawberry.ID]


@strawberry.type
class DevChangeSummary:
    contract_version: str
    state: str
    current_window: DevChangeWindow
    comparison_window: DevChangeWindow
    changes: list[DevObservedChange]
    source_refs: list[DevStatusSourceRef]
    warnings: list[str]
