from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Literal, Optional

WorkItemProvider = Literal["jira", "github", "gitlab", "linear"]

# Normalized status buckets used for cross-provider rollups.
WorkItemStatusCategory = Literal[
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "canceled",
    "unknown",
]

# Best-effort type buckets for issues/tickets/cards.
WorkItemType = Literal[
    "story",
    "task",
    "bug",
    "epic",
    "issue",
    "incident",
    "chore",
    "unknown",
]


@dataclass(frozen=True)
class WorkItem:
    """
    Normalized work item abstraction across Jira, GitHub, and GitLab.

    All timestamps are UTC.
    """

    work_item_id: (
        str  # stable string: jira:ABC-123, gh:owner/repo#123, gitlab:group/project#456
    )
    provider: WorkItemProvider
    title: str
    type: WorkItemType
    status: WorkItemStatusCategory
    status_raw: Optional[str]
    description: Optional[str] = None

    # Optional dimensions.
    repo_id: Optional[uuid.UUID] = None
    project_key: Optional[str] = None
    project_id: Optional[str] = None

    assignees: List[str] = field(
        default_factory=list
    )  # canonical identities when resolvable
    reporter: Optional[str] = None  # canonical identity when resolvable

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None

    labels: List[str] = field(default_factory=list)
    story_points: Optional[float] = None
    sprint_id: Optional[str] = None
    sprint_name: Optional[str] = None
    parent_id: Optional[str] = None
    epic_id: Optional[str] = None
    url: Optional[str] = None
    priority_raw: Optional[str] = None
    service_class: Optional[str] = None
    due_at: Optional[datetime] = None

    @property
    def work_scope_id(self) -> str:
        """
        Provider-native "project/work scope" identifier for grouping work tracking metrics.

        Notes:
        - Jira: uses `project_key` when present.
        - GitHub/GitLab: uses `project_id` (e.g. owner/repo, group/project) when present.
        - May be empty when the provider object is not scoped (e.g. GitHub draft project cards).
        """
        if self.provider == "jira" and self.project_key:
            return str(self.project_key)
        if self.project_id:
            return str(self.project_id)
        if self.project_key:
            return str(self.project_key)
        return ""


@dataclass(frozen=True)
class WorkItemStatusTransition:
    """
    A provider-normalized status transition event.

    `from_*` may be None for the first observed status.
    """

    work_item_id: str
    provider: WorkItemProvider
    occurred_at: datetime
    from_status_raw: Optional[str]
    to_status_raw: Optional[str]
    from_status: WorkItemStatusCategory
    to_status: WorkItemStatusCategory
    actor: Optional[str] = None  # canonical identity when resolvable


@dataclass(frozen=True)
class WorkItemDependency:
    source_work_item_id: str
    target_work_item_id: str
    relationship_type: str
    relationship_type_raw: str
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class WorkItemReopenEvent:
    work_item_id: str
    occurred_at: datetime
    from_status: WorkItemStatusCategory
    to_status: WorkItemStatusCategory
    from_status_raw: Optional[str]
    to_status_raw: Optional[str]
    actor: Optional[str]
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class WorkItemInteractionEvent:
    work_item_id: str
    provider: WorkItemProvider
    interaction_type: str
    occurred_at: datetime
    actor: Optional[str]
    body_length: int
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Sprint:
    provider: WorkItemProvider
    sprint_id: str
    name: Optional[str]
    state: Optional[str]
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    completed_at: Optional[datetime]
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Worklog:
    work_item_id: str
    provider: WorkItemProvider
    worklog_id: str
    author: Optional[str]
    started_at: datetime
    time_spent_seconds: int
    created_at: datetime
    updated_at: datetime
    last_synced: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
