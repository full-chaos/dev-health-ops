"""Pure worker-side normalization for external-ingest batches (CHAOS-2697).

Turns a parsed batch envelope's ``records`` into the :class:`NormalizedBatch`
shape ``sinks.write_batch()`` consumes, plus per-record rejections. Pure — no
I/O, no ClickHouse/Postgres calls — so it is unit-testable without services
and safe to re-run on redelivered stream entries.

Validation layering (master-spec CC6/CC17):

- Shape validation delegates to ``external_ingest.validate.validate_records``
  UNCHANGED (CC17: single owner, created complete by CHAOS-2691) so the
  ``POST /validate`` endpoint and this worker can never diverge on what a
  structurally valid record is.
- The kind x system matrix and the git-family instance-scope rule (CC6) are
  enforced HERE, as per-record rejections — ``validate.py`` deliberately
  excludes them (they need source context), and ``sinks.py`` only
  asserts-but-never-rejects (its ``record_outside_source_instance`` is a
  write-time warning). This module is the reject decision point.

The instance-scope comparison is case-INSENSITIVE (``casefold`` both sides),
deliberately looser than ``sinks._check_instance_scope``'s exact-match
warning: GitHub/GitLab full names and Jira/Linear keys are case-insensitive
identifiers on their providers (the same finding that made CHAOS-2695's
ownership matching case-insensitive), and ``derive_repo_uuid`` lower-cases
its seed — a case-variant identifier derives the SAME repo UUID, so
rejecting it would refuse data that cannot fork repo identity. The sink's
exact-match warning may still fire for such records; that is diagnostic
noise, not a contract violation.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ValidationError

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    EscalationPolicyV1,
    IncidentNoteV1,
    IncidentResponderV1,
    IncidentTimelineEventV1,
    OnCallAssignmentV1,
    OnCallScheduleV1,
    OperationalAlertV1,
    OperationalIncidentV1,
    OperationalRecordV1,
    OperationalServiceV1,
    OperationalTeamV1,
    OperationalUserV1,
    RecordEnvelope,
    ServiceRepositoryMappingV1,
)
from dev_health_ops.external_ingest.types import NormalizedBatch
from dev_health_ops.external_ingest.validate import validate_records
from dev_health_ops.models.operational import (
    EscalationPolicy,
    IncidentNote,
    IncidentResponder,
    IncidentTimelineEvent,
    OnCallAssignment,
    OnCallSchedule,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
    ServiceRepositoryMapping,
    canonical_operational_id,
)

logger = logging.getLogger(__name__)

_GIT_FAMILY_KINDS = frozenset(
    {"repository.v1", "pull_request.v1", "review.v1", "commit.v1"}
)
_WORK_ITEM_FAMILY_KINDS = frozenset(
    {"work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"}
)
_ORG_SCOPED_KINDS = frozenset({"team.v1", "identity.v1"})
_OPERATIONAL_KINDS = frozenset(
    {
        "operational_service.v1",
        "operational_incident.v1",
        "operational_alert.v1",
        "incident_timeline_event.v1",
        "incident_note.v1",
        "incident_responder.v1",
        "escalation_policy.v1",
        "on_call_schedule.v1",
        "on_call_assignment.v1",
        "operational_team.v1",
        "operational_user.v1",
        "service_repository_mapping.v1",
    }
)

#: Master-spec CC6 kind x system matrix. ``custom`` excludes the work_item
#: family in v1 (would widen the ``WorkItem.provider`` Literal vocabulary).
ALLOWED_KINDS_BY_SYSTEM: dict[str, frozenset[str]] = {
    "github": _GIT_FAMILY_KINDS
    | _WORK_ITEM_FAMILY_KINDS
    | _ORG_SCOPED_KINDS
    | _OPERATIONAL_KINDS,
    "gitlab": _GIT_FAMILY_KINDS
    | _WORK_ITEM_FAMILY_KINDS
    | _ORG_SCOPED_KINDS
    | _OPERATIONAL_KINDS,
    "jira": _WORK_ITEM_FAMILY_KINDS | _ORG_SCOPED_KINDS,
    "linear": _WORK_ITEM_FAMILY_KINDS | _ORG_SCOPED_KINDS,
    "custom": _GIT_FAMILY_KINDS | _ORG_SCOPED_KINDS | _OPERATIONAL_KINDS,
    "pagerduty": _OPERATIONAL_KINDS | _ORG_SCOPED_KINDS,
    "atlassian": _OPERATIONAL_KINDS | _ORG_SCOPED_KINDS,
}

#: CC6: systems whose git-family records must reference the batch's own
#: source.instance (jira/linear have no git-family kinds to scope).
_INSTANCE_SCOPED_SYSTEMS = frozenset({"github", "gitlab", "custom"})

#: kind -> (NormalizedBatch list attribute, record_index_by_kind key,
#: emit dicts?). Dict kinds are ``model_dump()``ed to the snake_case field
#: names ``sinks.py``'s row builders read; work-item-family kinds pass the
#: validated pydantic instances through (sinks duck-types attribute access).
_KIND_DISPATCH: dict[str, tuple[str, str, bool]] = {
    "repository.v1": ("repositories", "repository", True),
    "identity.v1": ("identities", "identity", True),
    "team.v1": ("teams", "team", True),
    "pull_request.v1": ("pull_requests", "pull_request", True),
    "review.v1": ("reviews", "review", True),
    "commit.v1": ("commits", "commit", True),
    "work_item.v1": ("work_items", "work_item", False),
    "work_item_transition.v1": ("work_item_transitions", "work_item_transition", False),
    "work_item_dependency.v1": (
        "work_item_dependencies",
        "work_item_dependency",
        False,
    ),
}

_OPERATIONAL_KIND_DISPATCH: dict[str, tuple[str, str]] = {
    "operational_service.v1": ("operational_services", "operational_service"),
    "operational_incident.v1": ("operational_incidents", "operational_incident"),
    "operational_alert.v1": ("operational_alerts", "operational_alert"),
    "incident_timeline_event.v1": (
        "incident_timeline_events",
        "incident_timeline_event",
    ),
    "incident_note.v1": ("incident_notes", "incident_note"),
    "incident_responder.v1": ("incident_responders", "incident_responder"),
    "escalation_policy.v1": ("escalation_policies", "escalation_policy"),
    "on_call_schedule.v1": ("on_call_schedules", "on_call_schedule"),
    "on_call_assignment.v1": ("on_call_assignments", "on_call_assignment"),
    "operational_team.v1": ("operational_teams", "operational_team"),
    "operational_user.v1": ("operational_users", "operational_user"),
    "service_repository_mapping.v1": (
        "service_repository_mappings",
        "service_repository_mapping",
    ),
}


@dataclass(frozen=True)
class RecordRejection:
    """One rejected record. Exactly one per rejected index — the status
    store's ``(ingestion_id, record_index)`` unique constraint allows a
    single persisted diagnostic per record, so multi-field shape failures
    collapse to the first error (``validate_records`` emits them in field
    order)."""

    index: int
    kind: str
    external_id: str | None
    code: str
    message: str
    path: str | None


@dataclass
class NormalizationResult:
    batch: NormalizedBatch
    rejections: list[RecordRejection] = field(default_factory=list)
    #: Accepted records per FULL kind name (e.g. ``{"pull_request.v1": 12}``)
    #: — the vocabulary both the batch row's ``record_counts`` column and
    #: CHAOS-2699's recompute planner expect (``plan_recompute`` intersects
    #: against ``.v1``-suffixed kind sets; the sink scope's bare-kind names
    #: would silently never trigger recompute).
    record_counts: dict[str, int] = field(default_factory=dict)
    items_received: int = 0

    @property
    def items_rejected(self) -> int:
        return len(self.rejections)

    @property
    def items_accepted(self) -> int:
        return self.items_received - len(self.rejections)


def _git_family_repo_identifier(kind: str, payload_model: BaseModel) -> str:
    """The repo identifier CC6 scopes to source.instance: ``repository.v1``
    carries it as its own ``external_id``; the other git-family kinds
    reference it as ``repository_external_id``."""
    if kind == "repository.v1":
        return str(getattr(payload_model, "external_id", "") or "")
    return str(getattr(payload_model, "repository_external_id", "") or "")


def _operational_reference_id(
    batch: NormalizedBatch, entity_family: str, external_id: str | None
) -> str | None:
    if external_id is None:
        return None
    return canonical_operational_id(
        batch.org_id,
        batch.source_system,
        batch.source_instance,
        entity_family,
        external_id,
    )


def _operational_common(
    payload: OperationalRecordV1, batch: NormalizedBatch, kind: str
) -> dict[str, Any]:
    return {
        "org_id": batch.org_id,
        "provider": batch.source_system,
        "provider_instance_id": batch.source_instance,
        "source_entity_type": f"external_push.{kind.rsplit('.', 1)[0]}",
        "external_id": payload.external_id,
        "source_version_at": payload.source_version_at,
        "source_id": batch.source_id,
        "source_url": payload.source_url,
        "source_event_at": payload.source_event_at,
        "source_event_id": payload.source_event_id,
        "raw_status": payload.raw_status,
        "raw_severity": payload.raw_severity,
        "raw_priority": payload.raw_priority,
        "normalized_status": payload.normalized_status,
        "normalized_severity": payload.normalized_severity,
        "normalized_priority": payload.normalized_priority,
        "relationship_provenance": payload.relationship_provenance,
        "relationship_confidence": payload.relationship_confidence,
    }


def _normalize_operational_record(
    kind: str, payload: OperationalRecordV1, batch: NormalizedBatch
) -> object:
    common = _operational_common(payload, batch, kind)
    match payload:
        case OperationalServiceV1():
            return OperationalService(
                **common,
                name=payload.name,
                description=payload.description,
                service_type=payload.service_type,
                owning_team_id=_operational_reference_id(
                    batch, "operational_team", payload.owning_team_external_id
                ),
                escalation_policy_id=_operational_reference_id(
                    batch,
                    "operational_escalation_policy",
                    payload.escalation_policy_external_id,
                ),
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case OperationalIncidentV1():
            return OperationalIncident(
                **common,
                service_id=_operational_reference_id(
                    batch, "operational_service", payload.service_external_id
                ),
                service_external_id=payload.service_external_id,
                escalation_policy_id=_operational_reference_id(
                    batch,
                    "operational_escalation_policy",
                    payload.escalation_policy_external_id,
                ),
                title=payload.title,
                description=payload.description,
                started_at=payload.started_at,
                resolved_at=payload.resolved_at,
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case OperationalAlertV1():
            return OperationalAlert(
                **common,
                service_id=_operational_reference_id(
                    batch, "operational_service", payload.service_external_id
                ),
                incident_id=_operational_reference_id(
                    batch, "operational_incident", payload.incident_external_id
                ),
                title=payload.title,
                description=payload.description,
                triggered_at=payload.triggered_at,
                acknowledged_at=payload.acknowledged_at,
                resolved_at=payload.resolved_at,
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case IncidentTimelineEventV1():
            return IncidentTimelineEvent(
                **common,
                incident_id=_operational_reference_id(
                    batch, "operational_incident", payload.incident_external_id
                )
                or "",
                event_type=payload.event_type,
                body=payload.body,
                actor_type=payload.actor_type,
                actor_id=_operational_reference_id(
                    batch, "operational_user", payload.actor_external_id
                ),
                occurred_at=payload.occurred_at,
            )
        case IncidentNoteV1():
            return IncidentNote(
                **common,
                incident_id=_operational_reference_id(
                    batch, "operational_incident", payload.incident_external_id
                )
                or "",
                body=payload.body,
                author_user_id=_operational_reference_id(
                    batch, "operational_user", payload.author_user_external_id
                ),
                created_at=payload.created_at,
            )
        case IncidentResponderV1():
            return IncidentResponder(
                **common,
                incident_id=_operational_reference_id(
                    batch, "operational_incident", payload.incident_external_id
                )
                or "",
                user_id=_operational_reference_id(
                    batch, "operational_user", payload.user_external_id
                ),
                responder_name=payload.responder_name,
                role=payload.role,
                responder_assignment_id=payload.responder_assignment_id,
                requested_at=payload.requested_at,
                assigned_at=payload.assigned_at,
                acknowledged_at=payload.acknowledged_at,
                completed_at=payload.completed_at,
            )
        case OnCallScheduleV1():
            return OnCallSchedule(
                **common,
                name=payload.name,
                description=payload.description,
                timezone=payload.timezone,
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case OperationalTeamV1():
            return OperationalTeam(
                **common,
                name=payload.name,
                description=payload.description,
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case EscalationPolicyV1():
            return EscalationPolicy(
                **common,
                name=payload.name,
                description=payload.description,
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case OnCallAssignmentV1():
            return OnCallAssignment(
                **common,
                schedule_id=_operational_reference_id(
                    batch, "operational_on_call_schedule", payload.schedule_external_id
                ),
                user_id=_operational_reference_id(
                    batch, "operational_user", payload.user_external_id
                ),
                escalation_policy_id=_operational_reference_id(
                    batch,
                    "operational_escalation_policy",
                    payload.escalation_policy_external_id,
                ),
                escalation_level=payload.escalation_level,
                starts_at=payload.starts_at,
                ends_at=payload.ends_at,
            )
        case OperationalUserV1():
            return OperationalUser(
                **common,
                display_name=payload.display_name,
                email=payload.email,
                is_deleted=payload.is_deleted,
                deleted_at=payload.deleted_at,
            )
        case ServiceRepositoryMappingV1():
            return ServiceRepositoryMapping(
                **common,
                service_id=_operational_reference_id(
                    batch, "operational_service", payload.service_external_id
                )
                or "",
                repo_full_name=payload.repo_full_name,
                repo_provider=payload.repo_provider,
                mapping_kind=payload.mapping_kind,
                rule_id=payload.rule_id,
                valid_from=payload.valid_from,
                valid_to=payload.valid_to,
                is_active=payload.is_active,
            )
    raise AssertionError(f"unsupported operational payload {type(payload)!r}")


def normalize_batch(
    *,
    org_id: str,
    source_id: uuid.UUID,
    source_system: str,
    source_instance: str,
    ingestion_id: uuid.UUID,
    records: list[RecordEnvelope],
) -> NormalizationResult:
    """Validate + normalize one batch's records. Never raises for record
    content — every per-record problem becomes a :class:`RecordRejection`;
    callers decide batch-level outcomes from the counts."""
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system=source_system,
        source_instance=source_instance,
        ingestion_id=ingestion_id,
    )
    result = NormalizationResult(batch=batch, items_received=len(records))

    # CC17: shape validation is validate.py's verdict, imported unchanged.
    # First error per index wins the (single) persisted rejection slot.
    shape_error_by_index: dict[int, tuple[str, str, str | None]] = {}
    for item in validate_records(records):
        shape_error_by_index.setdefault(
            item.index, (item.code, item.message, item.path)
        )

    allowed_kinds = ALLOWED_KINDS_BY_SYSTEM.get(source_system, frozenset())
    instance_folded = source_instance.casefold()

    for index, record in enumerate(records):
        shape_error = shape_error_by_index.get(index)
        if shape_error is not None:
            code, message, path = shape_error
            result.rejections.append(
                RecordRejection(
                    index=index,
                    kind=record.kind,
                    external_id=record.external_id,
                    code=code,
                    message=message,
                    path=path,
                )
            )
            continue

        if record.kind not in allowed_kinds:
            result.rejections.append(
                RecordRejection(
                    index=index,
                    kind=record.kind,
                    external_id=record.external_id,
                    code="unsupported_kind_for_system",
                    message=(
                        f"record kind {record.kind!r} is not accepted for "
                        f"source system {source_system!r} (master-spec CC6)"
                    ),
                    path=f"records[{index}].kind",
                )
            )
            continue

        model_cls = RECORD_KIND_MODELS[record.kind]
        try:
            payload_model = model_cls.model_validate(record.payload)
        except ValidationError:  # pragma: no cover - validate_records already
            # passed this exact payload against this exact model; reaching
            # here would mean the two diverged (a CC17 violation), so fail
            # the record loudly rather than crash the batch.
            logger.exception(
                "normalize_batch: model_validate diverged from validate_records "
                "for kind=%s index=%d",
                record.kind,
                index,
            )
            result.rejections.append(
                RecordRejection(
                    index=index,
                    kind=record.kind,
                    external_id=record.external_id,
                    code="invalid_field",
                    message="payload failed validation during normalization",
                    path=f"records[{index}].payload",
                )
            )
            continue

        if (
            record.kind in _GIT_FAMILY_KINDS
            and source_system in _INSTANCE_SCOPED_SYSTEMS
        ):
            identifier = _git_family_repo_identifier(record.kind, payload_model)
            if identifier.casefold() != instance_folded:
                result.rejections.append(
                    RecordRejection(
                        index=index,
                        kind=record.kind,
                        external_id=record.external_id,
                        code="record_outside_source_instance",
                        message=(
                            f"repository identifier {identifier!r} does not "
                            f"match this batch's source.instance "
                            f"{source_instance!r} (master-spec CC6)"
                        ),
                        path=f"records[{index}].payload",
                    )
                )
                continue

        operational_dispatch = _OPERATIONAL_KIND_DISPATCH.get(record.kind)
        if operational_dispatch is not None:
            assert isinstance(payload_model, OperationalRecordV1)
            attr, index_key = operational_dispatch
            target = getattr(batch, attr)
            target.append(
                _normalize_operational_record(record.kind, payload_model, batch)
            )
        else:
            attr, index_key, as_dict = _KIND_DISPATCH[record.kind]
            target = getattr(batch, attr)
            target.append(payload_model.model_dump() if as_dict else payload_model)
        batch.record_index_by_kind.setdefault(index_key, []).append(index)
        result.record_counts[record.kind] = result.record_counts.get(record.kind, 0) + 1

    return result


__all__ = [
    "ALLOWED_KINDS_BY_SYSTEM",
    "NormalizationResult",
    "RecordRejection",
    "normalize_batch",
]
