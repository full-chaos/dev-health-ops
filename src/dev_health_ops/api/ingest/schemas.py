from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Entity schemas (provider-agnostic, mirroring internal models)
# ---------------------------------------------------------------------------


class IngestCommit(BaseModel):
    hash: str
    message: str
    author_name: str
    author_email: str
    author_when: datetime  # UTC ISO-8601
    committer_name: str | None = None
    committer_email: str | None = None
    committer_when: datetime | None = None
    parents: int = 1


class IngestPullRequestReview(BaseModel):
    review_id: str
    reviewer: str
    state: str  # APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED
    submitted_at: datetime


class IngestPullRequest(BaseModel):
    number: int
    title: str
    body: str | None = None
    state: str  # open, closed, merged
    author_name: str
    author_email: str | None = None
    created_at: datetime
    merged_at: datetime | None = None
    closed_at: datetime | None = None
    head_branch: str | None = None
    base_branch: str | None = None
    additions: int | None = None
    deletions: int | None = None
    changed_files: int | None = None
    reviews: list[IngestPullRequestReview] = Field(default_factory=list)


class IngestWorkItem(BaseModel):
    work_item_id: str  # stable ID e.g. "jira:ABC-123"
    provider: Literal["jira", "github", "gitlab", "linear"]
    title: str
    type: Literal[
        "story", "task", "bug", "epic", "issue", "incident", "chore", "unknown"
    ] = "unknown"
    status: Literal[
        "backlog",
        "todo",
        "in_progress",
        "in_review",
        "blocked",
        "done",
        "canceled",
        "unknown",
    ] = "unknown"
    status_raw: str | None = None
    description: str | None = None
    project_key: str | None = None
    assignees: list[str] = Field(default_factory=list)
    reporter: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    labels: list[str] = Field(default_factory=list)
    story_points: float | None = None
    priority_raw: str | None = None
    url: str | None = None


class IngestTelemetrySignalBucket(BaseModel):
    signal_type: str  # e.g. friction.rage_click, error.unhandled, adoption.feature_used
    signal_count: int
    session_count: int
    unique_pseudonymous_count: int | None = None
    endpoint_group: str = ""
    environment: str
    repo_id: str = ""
    release_ref: str = ""
    bucket_start: datetime
    bucket_end: datetime
    is_sampled: bool = False
    schema_version: str = "1.0"
    dedupe_key: str


class IngestDeployment(BaseModel):
    deployment_id: str
    status: str
    environment: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    deployed_at: datetime | None = None
    pull_request_number: int | None = None
    release_ref: str | None = None
    release_ref_confidence: float | None = None


class IngestIncident(BaseModel):
    incident_id: str
    status: str
    started_at: datetime
    resolved_at: datetime | None = None


# ---------------------------------------------------------------------------
# Batch request schemas
# ---------------------------------------------------------------------------


class IngestBatchRequest(BaseModel):
    """Common base for all ingest requests."""

    org_id: str
    repo_url: str  # Used to derive deterministic repo_id for git-related entities


class IngestCommitsRequest(IngestBatchRequest):
    items: list[IngestCommit] = Field(..., min_length=1, max_length=1000)


class IngestPullRequestsRequest(IngestBatchRequest):
    items: list[IngestPullRequest] = Field(..., min_length=1, max_length=1000)


class IngestWorkItemsRequest(BaseModel):
    org_id: str
    items: list[IngestWorkItem] = Field(..., min_length=1, max_length=1000)
    # No repo_url — work items are project-scoped, not repo-scoped


class IngestTelemetryRequest(BaseModel):
    org_id: str
    items: list[IngestTelemetrySignalBucket] = Field(..., min_length=1, max_length=5000)


class IngestDeploymentsRequest(IngestBatchRequest):
    items: list[IngestDeployment] = Field(..., min_length=1, max_length=1000)


class IngestIncidentsRequest(IngestBatchRequest):
    items: list[IngestIncident] = Field(..., min_length=1, max_length=1000)


# ---------------------------------------------------------------------------
# Response schema
# ---------------------------------------------------------------------------


class IngestAcceptedResponse(BaseModel):
    ingestion_id: str  # UUID
    status: str = "accepted"
    items_received: int
    stream: str  # Redis stream name for transparency
