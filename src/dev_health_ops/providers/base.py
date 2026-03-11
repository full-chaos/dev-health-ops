"""
Base provider contract for work item ingestion across Jira, GitHub, and GitLab.

This module defines the shared interface that all providers must implement,
along with capability flags and typed envelopes for consistent orchestration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dev_health_ops.models.work_items import (
        Sprint,
        WorkItem,
        WorkItemDependency,
        WorkItemInteractionEvent,
        WorkItemReopenEvent,
        WorkItemStatusTransition,
        Worklog,
    )


@dataclass(frozen=True)
class ProviderCapabilities:
    """
    Flags indicating which entity types a provider can ingest.

    Providers set these based on what their APIs support.
    """

    work_items: bool = True
    status_transitions: bool = True
    dependencies: bool = False
    interactions: bool = False
    sprints: bool = False
    reopen_events: bool = False
    priority: bool = False


@dataclass(frozen=True)
class IngestionWindow:
    """
    Time window for incremental ingestion.

    - updated_since: fetch items updated on or after this time
    - active_until: upper bound for the window (optional)
    """

    updated_since: datetime | None = None
    active_until: datetime | None = None


@dataclass(frozen=True)
class IngestionContext:
    """
    Context passed to provider.ingest() describing what to fetch.

    - window: time bounds for incremental sync
    - project_key: Jira project key (e.g. "ABC")
    - repo: GitHub/GitLab repo identifier (e.g. "owner/repo")
    - group: GitLab group path
    - limit: optional max items to fetch (for testing)
    """

    window: IngestionWindow
    project_key: str | None = None  # jira
    repo: str | None = None  # github/gitlab
    group: str | None = None  # gitlab
    limit: int | None = None


@dataclass
class ProviderBatch:
    """
    Typed envelope returned by Provider.ingest().

    Each list contains normalized model instances. Providers fill only the
    lists for capabilities they support; others remain empty.
    """

    work_items: list[WorkItem] = field(default_factory=list)
    status_transitions: list[WorkItemStatusTransition] = field(default_factory=list)
    dependencies: list[WorkItemDependency] = field(default_factory=list)
    interactions: list[WorkItemInteractionEvent] = field(default_factory=list)
    sprints: list[Sprint] = field(default_factory=list)
    reopen_events: list[WorkItemReopenEvent] = field(default_factory=list)
    worklogs: list[Worklog] = field(default_factory=list)


class Provider(ABC):
    """
    Abstract base class for work item providers.

    Subclasses must define:
    - name: unique provider identifier (e.g. "jira", "github", "gitlab")
    - capabilities: what entity types the provider can ingest
    - ingest(ctx): fetch and normalize entities within the given context
    """

    name: str
    capabilities: ProviderCapabilities

    @abstractmethod
    def ingest(self, ctx: IngestionContext) -> ProviderBatch:
        """
        Ingest work items and related entities within the given context.

        Returns a ProviderBatch with normalized entities. Only lists for
        supported capabilities will be populated.
        """

    def iter_ingest(self, ctx: IngestionContext) -> Iterable[ProviderBatch]:
        """
        Yield batches of ingested data.

        Default implementation wraps ingest() in a single-element iterable.
        Providers with large result sets should override to yield smaller
        batches for memory-bounded processing.
        """
        yield self.ingest(ctx)
