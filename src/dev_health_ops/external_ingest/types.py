"""Sink-write layer data contracts (CHAOS-2698).

``NormalizedBatch`` is the interface CHAOS-2697's worker normalization hands
to ``sinks.write_batch()``; ``SinkWriteResult``/``SinkWriteError``/
``AffectedScope`` are what this layer hands back to CHAOS-2694 (status/error
persistence) and CHAOS-2699 (bounded recompute). CHAOS-2699 does NOT import
these types — its planner takes primitives (org_id, repo_ids, etc.) directly;
CHAOS-2697 maps ``AffectedScope`` to those kwargs at the call site
(master-spec CC21).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

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
)

__all__ = [
    "NormalizedBatch",
    "SinkWriteError",
    "AffectedScope",
    "SinkWriteResult",
]


@dataclass
class NormalizedBatch:
    """Schema-validated, kind-normalized records ready for sink writes.

    ``repositories``/``pull_requests``/``reviews``/``commits``/``identities``/
    ``teams`` are plain dicts keyed by the same snake_case field names as
    their ``api/external_ingest/schemas.py`` pydantic model (``RepositoryV1``,
    ``PullRequestV1``, ``ReviewV1``, ``CommitV1``, ``IdentityV1``, ``TeamV1``).
    ``work_items``/``work_item_transitions``/``work_item_dependencies`` accept
    either the corresponding pydantic model instance (``WorkItemV1`` etc.) or
    an equivalent dict — ``sinks.py`` duck-types both via attribute-or-key
    lookup, so CHAOS-2697 may emit whichever shape is more convenient.
    """

    org_id: str
    source_id: uuid.UUID
    source_system: str  # "github" | "gitlab" | "jira" | "linear" | "custom"
    source_instance: (
        str  # e.g. "acme/api" (github/gitlab), "ABC" (jira), "CHAOS" (linear)
    )
    ingestion_id: uuid.UUID
    repositories: list[dict[str, Any]] = field(default_factory=list)
    identities: list[dict[str, Any]] = field(default_factory=list)
    teams: list[dict[str, Any]] = field(default_factory=list)
    work_items: list[Any] = field(default_factory=list)
    work_item_transitions: list[Any] = field(default_factory=list)
    work_item_dependencies: list[Any] = field(default_factory=list)
    pull_requests: list[dict[str, Any]] = field(default_factory=list)
    reviews: list[dict[str, Any]] = field(default_factory=list)
    commits: list[dict[str, Any]] = field(default_factory=list)
    operational_services: list[OperationalService] = field(default_factory=list)
    operational_incidents: list[OperationalIncident] = field(default_factory=list)
    operational_alerts: list[OperationalAlert] = field(default_factory=list)
    incident_timeline_events: list[IncidentTimelineEvent] = field(default_factory=list)
    incident_notes: list[IncidentNote] = field(default_factory=list)
    incident_responders: list[IncidentResponder] = field(default_factory=list)
    escalation_policies: list[EscalationPolicy] = field(default_factory=list)
    on_call_schedules: list[OnCallSchedule] = field(default_factory=list)
    on_call_assignments: list[OnCallAssignment] = field(default_factory=list)
    operational_teams: list[OperationalTeam] = field(default_factory=list)
    operational_users: list[OperationalUser] = field(default_factory=list)
    service_repository_mappings: list[ServiceRepositoryMapping] = field(
        default_factory=list
    )
    # Per-kind list of the record's original index in the accepted envelope's
    # `records` array, for error/warning correlation back to CHAOS-2694's
    # rejection diagnostics. Falls back to in-batch position when absent or
    # a kind's list is shorter than its record list.
    record_index_by_kind: dict[str, list[int]] = field(default_factory=dict)


@dataclass
class SinkWriteError:
    """A write-time failure or non-rejecting warning for one kind's write.

    ``record_index`` is the record's original position in the envelope's
    ``records`` array when known, else ``-1`` for batch-level (whole sink
    method call) failures — sink methods write a kind in one batched call, so
    a ClickHouse error there fails every record of that kind in this batch
    (brief-2698-sinks.md: batch-call granularity, not per-record).
    """

    record_index: int
    kind: str
    external_id: str | None
    code: str  # e.g. "clickhouse_insert_failed", "updated_at_clamped"
    message: str


@dataclass
class AffectedScope:
    """What this batch touched, for CHAOS-2699's bounded recompute planner."""

    org_id: str
    source_systems: set[str] = field(default_factory=set)
    source_instances: set[str] = field(default_factory=set)
    repo_ids: set[uuid.UUID] = field(default_factory=set)
    team_ids: set[str] = field(default_factory=set)
    work_item_ids: set[str] = field(default_factory=set)
    incident_ids: set[str] = field(default_factory=set)
    service_ids: set[str] = field(default_factory=set)
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    record_kinds: set[str] = field(default_factory=set)


@dataclass
class SinkWriteResult:
    """Machine-readable summary of one ``write_batch()`` call."""

    ingestion_id: uuid.UUID
    org_id: str
    counts_written: dict[str, int] = field(default_factory=dict)
    # Write-time failures only (validation already happened upstream).
    errors: list[SinkWriteError] = field(default_factory=list)
    # Non-rejecting diagnostics — currently only the D8/CC24 updatedAt clamp.
    # Additive vs. the interface brief's original sketch (counts/errors/
    # affected_scope only); CHAOS-2694 may ignore this field.
    warnings: list[SinkWriteError] = field(default_factory=list)
    affected_scope: AffectedScope = field(
        default_factory=lambda: AffectedScope(org_id="")
    )
