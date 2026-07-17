"""Pydantic v2 wire schemas for the external-ingest REST contract (CHAOS-2691).

Frozen wire contract for CHAOS-2690's customer-push ingestion epic — sibling
tickets (2692 schema registry, 2697 worker normalization, 2698 sinks, 2700
CLI) import these models rather than re-declaring field sets. See
``docs/architecture/external-ingest-rest-contract.md`` for the design
decisions behind the shapes below (D1-D13) and
``docs/architecture/adr-003-external-ingest-rest-boundary.md`` for the
REST-boundary/ownership-model rationale.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "external-ingest.v1"
MAX_RECORDS_DEFAULT = 1000
MAX_BODY_BYTES_DEFAULT = 10_000_000

_WORK_ITEM_STATUS = Literal[
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "canceled",
    "unknown",
]
_WORK_ITEM_TYPE = Literal["issue", "pr", "merge_request"]


class SourceDescriptor(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    type: Literal["customer_push"] = "customer_push"
    system: Literal[
        "github", "gitlab", "jira", "linear", "pagerduty", "atlassian", "custom"
    ]
    instance: str = Field(..., min_length=1, max_length=255)
    entity_family: str = Field(
        default="legacy", alias="entityFamily", min_length=1, max_length=255
    )
    producer: str | None = None
    producer_version: str | None = Field(default=None, alias="producerVersion")


class IngestWindow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    started_at: datetime = Field(..., alias="startedAt")
    ended_at: datetime = Field(..., alias="endedAt")

    @field_validator("ended_at")
    @classmethod
    def _ended_after_started(cls, v: datetime, info) -> datetime:
        started = info.data.get("started_at")
        if started and v < started:
            raise ValueError("window.endedAt must be >= window.startedAt")
        return v


class RecordEnvelope(BaseModel):
    """Generic wrapper: kind + externalId (for error correlation) + payload.

    ``payload`` is validated per-kind against ``RECORD_KIND_MODELS`` in
    ``router.py``/``validate.py``, not here — a discriminated union at this
    level would abort parsing the whole batch on one bad record (see
    docs/architecture/external-ingest-rest-contract.md D-payload-shape).
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    kind: str
    external_id: str = Field(..., alias="externalId", min_length=1, max_length=512)
    payload: dict


class BatchEnvelope(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    schema_version: str = Field(..., alias="schemaVersion")
    idempotency_key: str = Field(
        ..., alias="idempotencyKey", min_length=1, max_length=255
    )
    source: SourceDescriptor
    window: IngestWindow | None = None
    # min_length=1: empty batches are a 400 at parse time (master-spec CC3).
    records: list[RecordEnvelope] = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


class ValidationErrorItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    index: int
    kind: str
    code: str
    message: str
    path: str | None = None


class ValidationResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    valid: bool
    items_accepted: int = Field(..., alias="itemsAccepted")
    items_rejected: int = Field(..., alias="itemsRejected")
    errors: list[ValidationErrorItem] = Field(default_factory=list)


class BatchAcceptedResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    ingestion_id: str = Field(..., alias="ingestionId")
    status: Literal["accepted"] = "accepted"
    items_received: int = Field(..., alias="itemsReceived")
    stream: str


# ---------------------------------------------------------------------------
#
# All models: extra="forbid" — this is a versioned, external, customer-SDK
# contract, not an internal analytics event. A customer typo should be a
# loud validation error, not silently dropped data (deliberate deviation
# from the rest of the codebase's looser Pydantic configs).
# ---------------------------------------------------------------------------


class RepositoryV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    # externalId is the provider FULL NAME ("owner/repo" / "group/subgroup/
    # project"), NOT a URL (master-spec CC4, verified against
    # processors/github.py:1572 repo=repo_info.full_name and
    # processors/gitlab.py:1815 path_with_namespace) — becomes Repo.repo AND
    # the get_repo_uuid_from_repo() seed. Must equal source.instance for git
    # systems. For system="custom": seed = f"custom:{instance}:{externalId}".
    external_id: str = Field(..., alias="externalId", min_length=1, max_length=1024)
    source_system: Literal["github", "gitlab", "custom"] = Field(
        ..., alias="sourceSystem"
    )
    default_ref: str | None = Field(default=None, alias="defaultRef")
    tags: list[str] = Field(default_factory=list, max_length=50)
    settings: dict[str, str | int | float | bool] = Field(default_factory=dict)


class IdentityV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    canonical_id: str = Field(..., alias="canonicalId", min_length=1, max_length=255)
    display_name: str | None = Field(default=None, alias="displayName")
    email: str | None = None
    provider_identities: dict[str, list[str]] = Field(
        default_factory=dict, alias="providerIdentities"
    )
    team_ids: list[str] = Field(default_factory=list, alias="teamIds")
    is_active: bool = Field(default=True, alias="isActive")
    updated_at: datetime = Field(..., alias="updatedAt")


class TeamV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    id: str = Field(..., min_length=1, max_length=255)
    name: str
    description: str | None = None
    members: list[str] = Field(default_factory=list)
    project_keys: list[str] = Field(default_factory=list, alias="projectKeys")
    repo_patterns: list[str] = Field(default_factory=list, alias="repoPatterns")
    is_active: bool = Field(default=True, alias="isActive")
    updated_at: datetime = Field(..., alias="updatedAt")
    native_team_key: str | None = Field(default=None, alias="nativeTeamKey")
    parent_team_id: str | None = Field(default=None, alias="parentTeamId")


class WorkItemV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    # Provider-NATIVE key ("ABC-123", "CHAOS-123", issue/PR number) — master-
    # spec CC7. The namespaced work_item_id (jira:/linear:/gh:/ghpr:/
    # gitlab:#/gitlab:!) is derived server-side in external_ingest/ids.py
    # (CHAOS-2698); customers never send it.
    external_key: str = Field(..., alias="externalKey", min_length=1, max_length=512)
    provider: Literal["jira", "github", "gitlab", "linear"]
    title: str
    type: Literal[
        "story",
        "task",
        "bug",
        "epic",
        "pr",
        "merge_request",
        "issue",
        "incident",
        "chore",
        "unknown",
    ] = "unknown"
    status: _WORK_ITEM_STATUS
    status_raw: str | None = Field(default=None, alias="statusRaw")
    description: str | None = None
    repository_external_id: str | None = Field(
        default=None, alias="repositoryExternalId"
    )
    native_team_key: str | None = Field(default=None, alias="nativeTeamKey")
    project_key: str | None = Field(default=None, alias="projectKey")
    project_id: str | None = Field(default=None, alias="projectId")
    project_name: str | None = Field(default=None, alias="projectName")
    assignees: list[str] = Field(default_factory=list)
    reporter: str | None = None
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    started_at: datetime | None = Field(default=None, alias="startedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")
    closed_at: datetime | None = Field(default=None, alias="closedAt")
    labels: list[str] = Field(default_factory=list)
    story_points: float | None = Field(default=None, alias="storyPoints")
    sprint_id: str | None = Field(default=None, alias="sprintId")
    sprint_name: str | None = Field(default=None, alias="sprintName")
    parent_id: str | None = Field(default=None, alias="parentId")
    epic_id: str | None = Field(default=None, alias="epicId")
    url: str | None = None
    priority_raw: str | None = Field(default=None, alias="priorityRaw")
    service_class: str | None = Field(default=None, alias="serviceClass")
    due_at: datetime | None = Field(default=None, alias="dueAt")


class WorkItemTransitionV1(BaseModel):
    """Master-spec header item 2: takes ``externalKey`` (not the internal
    namespaced ``work_item_id``) + optional ``workItemType`` to disambiguate
    the issue vs. pr/merge_request namespace for github/gitlab.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    external_key: str = Field(..., alias="externalKey", min_length=1, max_length=512)
    provider: Literal["jira", "github", "gitlab", "linear"]
    work_item_type: _WORK_ITEM_TYPE | None = Field(default=None, alias="workItemType")
    occurred_at: datetime = Field(..., alias="occurredAt")
    from_status_raw: str | None = Field(default=None, alias="fromStatusRaw")
    to_status_raw: str | None = Field(default=None, alias="toStatusRaw")
    from_status: _WORK_ITEM_STATUS = Field(..., alias="fromStatus")
    to_status: _WORK_ITEM_STATUS = Field(..., alias="toStatus")
    actor: str | None = None


class WorkItemDependencyV1(BaseModel):
    """Master-spec header item 2: source/target take ``externalKey`` pairs
    (not internal namespaced work-item IDs) + optional per-side
    ``workItemType`` since source and target may be different namespaces
    (e.g. an issue blocked by a PR).
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    source_external_key: str = Field(
        ..., alias="sourceExternalKey", min_length=1, max_length=512
    )
    source_work_item_type: _WORK_ITEM_TYPE | None = Field(
        default=None, alias="sourceWorkItemType"
    )
    target_external_key: str = Field(
        ..., alias="targetExternalKey", min_length=1, max_length=512
    )
    target_work_item_type: _WORK_ITEM_TYPE | None = Field(
        default=None, alias="targetWorkItemType"
    )
    relationship_type: Literal[
        "blocks", "blocked_by", "relates_to", "duplicates", "parent_of", "child_of"
    ] = Field(..., alias="relationshipType")
    relationship_type_raw: str | None = Field(default=None, alias="relationshipTypeRaw")


class PullRequestV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    repository_external_id: str = Field(..., alias="repositoryExternalId")
    number: int = Field(..., ge=1)
    title: str | None = None
    body: str | None = None
    state: Literal["open", "closed", "merged"]
    author_name: str | None = Field(default=None, alias="authorName")
    author_email: str | None = Field(default=None, alias="authorEmail")
    created_at: datetime = Field(..., alias="createdAt")
    merged_at: datetime | None = Field(default=None, alias="mergedAt")
    closed_at: datetime | None = Field(default=None, alias="closedAt")
    head_branch: str | None = Field(default=None, alias="headBranch")
    base_branch: str | None = Field(default=None, alias="baseBranch")
    additions: int | None = Field(default=None, ge=0)
    deletions: int | None = Field(default=None, ge=0)
    changed_files: int | None = Field(default=None, alias="changedFiles", ge=0)
    first_review_at: datetime | None = Field(default=None, alias="firstReviewAt")
    first_comment_at: datetime | None = Field(default=None, alias="firstCommentAt")
    changes_requested_count: int | None = Field(
        default=0, alias="changesRequestedCount", ge=0
    )
    reviews_count: int | None = Field(default=0, alias="reviewsCount", ge=0)
    comments_count: int | None = Field(default=0, alias="commentsCount", ge=0)
    url: str | None = None


class ReviewV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    repository_external_id: str = Field(..., alias="repositoryExternalId")
    pull_request_number: int = Field(..., alias="pullRequestNumber", ge=1)
    review_id: str = Field(..., alias="reviewId", min_length=1)
    reviewer: str
    # Validated free-string allow-list, not the internal untyped raw
    # provider string (brief D12) — customer payloads are untrusted in a
    # way native sync providers are not.
    state: Literal["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"]
    submitted_at: datetime = Field(..., alias="submittedAt")


class CommitV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    repository_external_id: str = Field(..., alias="repositoryExternalId")
    hash: str = Field(..., min_length=7, max_length=64)
    message: str | None = None
    author_name: str | None = Field(default=None, alias="authorName")
    author_email: str | None = Field(default=None, alias="authorEmail")
    author_when: datetime = Field(..., alias="authorWhen")
    committer_name: str | None = Field(default=None, alias="committerName")
    committer_email: str | None = Field(default=None, alias="committerEmail")
    committer_when: datetime | None = Field(default=None, alias="committerWhen")
    parents: int = Field(default=1, ge=0)


_OPERATIONAL_STATUS = Literal[
    "active", "open", "acknowledged", "resolved", "closed", "suppressed"
]
_OPERATIONAL_SEVERITY = Literal["critical", "high", "medium", "low", "info"]
_OPERATIONAL_PRIORITY = Literal["critical", "high", "medium", "low"]


class OperationalRecordV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    external_id: str = Field(..., alias="externalId", min_length=1, max_length=512)
    source_version_at: datetime = Field(..., alias="sourceVersionAt")
    source_url: str | None = Field(default=None, alias="sourceUrl", max_length=2048)
    source_event_at: datetime | None = Field(default=None, alias="sourceEventAt")
    source_event_id: str | None = Field(
        default=None, alias="sourceEventId", max_length=512
    )
    raw_status: str | None = Field(default=None, alias="rawStatus", max_length=255)
    raw_severity: str | None = Field(default=None, alias="rawSeverity", max_length=255)
    raw_priority: str | None = Field(default=None, alias="rawPriority", max_length=255)
    normalized_status: _OPERATIONAL_STATUS | None = Field(
        default=None, alias="normalizedStatus"
    )
    normalized_severity: _OPERATIONAL_SEVERITY | None = Field(
        default=None, alias="normalizedSeverity"
    )
    normalized_priority: _OPERATIONAL_PRIORITY | None = Field(
        default=None, alias="normalizedPriority"
    )
    relationship_provenance: str | None = Field(
        default=None, alias="relationshipProvenance", max_length=255
    )
    relationship_confidence: float | None = Field(
        default=None, alias="relationshipConfidence", ge=0, le=1
    )


class OperationalServiceV1(OperationalRecordV1):
    name: str = Field(..., min_length=1, max_length=512)
    description: str | None = Field(default=None, max_length=20_000)
    service_type: str | None = Field(default=None, alias="serviceType", max_length=255)
    owning_team_external_id: str | None = Field(
        default=None, alias="owningTeamExternalId", max_length=512
    )
    escalation_policy_external_id: str | None = Field(
        default=None, alias="escalationPolicyExternalId", max_length=512
    )
    is_deleted: bool = Field(default=False, alias="isDeleted")
    deleted_at: datetime | None = Field(default=None, alias="deletedAt")


class OperationalIncidentV1(OperationalRecordV1):
    title: str = Field(..., min_length=1, max_length=1024)
    description: str | None = Field(default=None, max_length=20_000)
    service_external_id: str | None = Field(
        default=None, alias="serviceExternalId", max_length=512
    )
    escalation_policy_external_id: str | None = Field(
        default=None, alias="escalationPolicyExternalId", max_length=512
    )
    started_at: datetime | None = Field(default=None, alias="startedAt")
    resolved_at: datetime | None = Field(default=None, alias="resolvedAt")
    is_deleted: bool = Field(default=False, alias="isDeleted")
    deleted_at: datetime | None = Field(default=None, alias="deletedAt")


class OperationalAlertV1(OperationalRecordV1):
    title: str = Field(..., min_length=1, max_length=1024)
    description: str | None = Field(default=None, max_length=20_000)
    service_external_id: str | None = Field(
        default=None, alias="serviceExternalId", max_length=512
    )
    incident_external_id: str | None = Field(
        default=None, alias="incidentExternalId", max_length=512
    )
    triggered_at: datetime | None = Field(default=None, alias="triggeredAt")
    acknowledged_at: datetime | None = Field(default=None, alias="acknowledgedAt")
    resolved_at: datetime | None = Field(default=None, alias="resolvedAt")
    is_deleted: bool = Field(default=False, alias="isDeleted")
    deleted_at: datetime | None = Field(default=None, alias="deletedAt")


class IncidentTimelineEventV1(OperationalRecordV1):
    incident_external_id: str = Field(
        ..., alias="incidentExternalId", min_length=1, max_length=512
    )
    event_type: str = Field(..., alias="eventType", min_length=1, max_length=255)
    body: str | None = Field(default=None, max_length=20_000)
    actor_type: str | None = Field(default=None, alias="actorType", max_length=255)
    actor_external_id: str | None = Field(
        default=None, alias="actorExternalId", max_length=512
    )
    occurred_at: datetime | None = Field(default=None, alias="occurredAt")


class IncidentNoteV1(OperationalRecordV1):
    incident_external_id: str = Field(
        ..., alias="incidentExternalId", min_length=1, max_length=512
    )
    body: str = Field(..., min_length=1, max_length=20_000)
    author_user_external_id: str | None = Field(
        default=None, alias="authorUserExternalId", max_length=512
    )
    created_at: datetime | None = Field(default=None, alias="createdAt")


class IncidentResponderV1(OperationalRecordV1):
    incident_external_id: str = Field(
        ..., alias="incidentExternalId", min_length=1, max_length=512
    )
    user_external_id: str | None = Field(
        default=None, alias="userExternalId", max_length=512
    )
    responder_name: str | None = Field(
        default=None, alias="responderName", max_length=512
    )
    role: str | None = Field(default=None, max_length=255)
    responder_assignment_id: str | None = Field(
        default=None, alias="responderAssignmentId", max_length=512
    )
    requested_at: datetime | None = Field(default=None, alias="requestedAt")
    assigned_at: datetime | None = Field(default=None, alias="assignedAt")
    acknowledged_at: datetime | None = Field(default=None, alias="acknowledgedAt")
    completed_at: datetime | None = Field(default=None, alias="completedAt")


class EscalationPolicyV1(OperationalRecordV1):
    name: str = Field(..., min_length=1, max_length=512)
    description: str | None = Field(default=None, max_length=20_000)
    is_deleted: bool = Field(default=False, alias="isDeleted")
    deleted_at: datetime | None = Field(default=None, alias="deletedAt")


class OnCallScheduleV1(EscalationPolicyV1):
    timezone: str | None = Field(default=None, max_length=255)


class OnCallAssignmentV1(OperationalRecordV1):
    schedule_external_id: str | None = Field(
        default=None, alias="scheduleExternalId", max_length=512
    )
    user_external_id: str | None = Field(
        default=None, alias="userExternalId", max_length=512
    )
    escalation_policy_external_id: str | None = Field(
        default=None, alias="escalationPolicyExternalId", max_length=512
    )
    escalation_level: int | None = Field(
        default=None, alias="escalationLevel", ge=0, le=100
    )
    starts_at: datetime | None = Field(default=None, alias="startsAt")
    ends_at: datetime | None = Field(default=None, alias="endsAt")


class OperationalTeamV1(EscalationPolicyV1):
    pass


class OperationalUserV1(OperationalRecordV1):
    display_name: str = Field(..., alias="displayName", min_length=1, max_length=512)
    email: str | None = Field(default=None, max_length=512)
    is_deleted: bool = Field(default=False, alias="isDeleted")
    deleted_at: datetime | None = Field(default=None, alias="deletedAt")


class ServiceRepositoryMappingV1(OperationalRecordV1):
    service_external_id: str = Field(
        ..., alias="serviceExternalId", min_length=1, max_length=512
    )
    repo_full_name: str | None = Field(
        default=None, alias="repoFullName", max_length=1024
    )
    repo_provider: Literal["github", "gitlab", "custom"] | None = Field(
        default=None, alias="repoProvider"
    )
    mapping_kind: str | None = Field(default=None, alias="mappingKind", max_length=255)
    rule_id: str | None = Field(default=None, alias="ruleId", max_length=512)
    valid_from: datetime | None = Field(default=None, alias="validFrom")
    valid_to: datetime | None = Field(default=None, alias="validTo")
    is_active: bool = Field(default=True, alias="isActive")


RECORD_KIND_MODELS: dict[str, type[BaseModel]] = {
    "repository.v1": RepositoryV1,
    "identity.v1": IdentityV1,
    "team.v1": TeamV1,
    "work_item.v1": WorkItemV1,
    "work_item_transition.v1": WorkItemTransitionV1,
    "work_item_dependency.v1": WorkItemDependencyV1,
    "pull_request.v1": PullRequestV1,
    "review.v1": ReviewV1,
    "commit.v1": CommitV1,
    "operational_service.v1": OperationalServiceV1,
    "operational_incident.v1": OperationalIncidentV1,
    "operational_alert.v1": OperationalAlertV1,
    "incident_timeline_event.v1": IncidentTimelineEventV1,
    "incident_note.v1": IncidentNoteV1,
    "incident_responder.v1": IncidentResponderV1,
    "escalation_policy.v1": EscalationPolicyV1,
    "on_call_schedule.v1": OnCallScheduleV1,
    "on_call_assignment.v1": OnCallAssignmentV1,
    "operational_team.v1": OperationalTeamV1,
    "operational_user.v1": OperationalUserV1,
    "service_repository_mapping.v1": ServiceRepositoryMappingV1,
}


__all__ = [
    "SCHEMA_VERSION",
    "MAX_RECORDS_DEFAULT",
    "MAX_BODY_BYTES_DEFAULT",
    "SourceDescriptor",
    "IngestWindow",
    "RecordEnvelope",
    "BatchEnvelope",
    "ValidationErrorItem",
    "ValidationResponse",
    "BatchAcceptedResponse",
    "RepositoryV1",
    "IdentityV1",
    "TeamV1",
    "WorkItemV1",
    "WorkItemTransitionV1",
    "WorkItemDependencyV1",
    "PullRequestV1",
    "ReviewV1",
    "CommitV1",
    "OperationalRecordV1",
    "OperationalServiceV1",
    "OperationalIncidentV1",
    "OperationalAlertV1",
    "IncidentTimelineEventV1",
    "IncidentNoteV1",
    "IncidentResponderV1",
    "EscalationPolicyV1",
    "OnCallScheduleV1",
    "OnCallAssignmentV1",
    "OperationalTeamV1",
    "OperationalUserV1",
    "ServiceRepositoryMappingV1",
    "RECORD_KIND_MODELS",
]
