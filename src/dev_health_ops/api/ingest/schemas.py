from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

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
    committer_name: Optional[str] = None
    committer_email: Optional[str] = None
    committer_when: Optional[datetime] = None
    parents: int = 1


class IngestPullRequestReview(BaseModel):
    review_id: str
    reviewer: str
    state: str  # APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED
    submitted_at: datetime


class IngestPullRequest(BaseModel):
    number: int
    title: str
    body: Optional[str] = None
    state: str  # open, closed, merged
    author_name: str
    author_email: Optional[str] = None
    created_at: datetime
    merged_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    head_branch: Optional[str] = None
    base_branch: Optional[str] = None
    additions: Optional[int] = None
    deletions: Optional[int] = None
    changed_files: Optional[int] = None
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
    status_raw: Optional[str] = None
    description: Optional[str] = None
    project_key: Optional[str] = None
    assignees: list[str] = Field(default_factory=list)
    reporter: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    labels: list[str] = Field(default_factory=list)
    story_points: Optional[float] = None
    priority_raw: Optional[str] = None
    url: Optional[str] = None


class IngestDeployment(BaseModel):
    deployment_id: str
    status: str
    environment: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    deployed_at: Optional[datetime] = None
    pull_request_number: Optional[int] = None


class IngestIncident(BaseModel):
    incident_id: str
    status: str
    started_at: datetime
    resolved_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Batch request schemas
# ---------------------------------------------------------------------------


class IngestBatchRequest(BaseModel):
    """Common base for all ingest requests."""

    org_id: str = "default"
    repo_url: str  # Used to derive deterministic repo_id for git-related entities


class IngestCommitsRequest(IngestBatchRequest):
    items: list[IngestCommit] = Field(..., min_length=1, max_length=1000)


class IngestPullRequestsRequest(IngestBatchRequest):
    items: list[IngestPullRequest] = Field(..., min_length=1, max_length=1000)


class IngestWorkItemsRequest(BaseModel):
    org_id: str = "default"
    items: list[IngestWorkItem] = Field(..., min_length=1, max_length=1000)
    # No repo_url — work items are project-scoped, not repo-scoped


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
