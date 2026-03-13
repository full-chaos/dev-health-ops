from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Protocol

from dev_health_ops.api.queries import client as queries_client_module
from dev_health_ops.fixtures import generator as fixtures_generator_module
from dev_health_ops.metrics.loaders.base import clickhouse_query_dicts
from dev_health_ops.metrics.schemas import (
    UserMetricsDailyRecord,
    WorkItemUserMetricsDailyRecord,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.base import IngestionContext, IngestionWindow
from dev_health_ops.providers.github import client as github_client_module
from dev_health_ops.providers.github import normalize as github_normalize_module
from dev_health_ops.providers.gitlab import client as gitlab_client_module
from dev_health_ops.providers.gitlab import normalize as gitlab_normalize_module
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.jira import client as jira_client_module
from dev_health_ops.providers.jira import normalize as jira_normalize_module
from dev_health_ops.providers.jira import provider as jira_provider_module
from dev_health_ops.providers.status_mapping import StatusMapping


class SyntheticGeneratorProtocol(Protocol):
    def generate_work_items(self, *, days: int) -> list[WorkItem]: ...

    def generate_work_item_transitions(
        self, items: list[WorkItem]
    ) -> list[WorkItemStatusTransition]: ...


class SyntheticGeneratorFactory(Protocol):
    def __call__(
        self, *, repo_id: Any, repo_name: str
    ) -> SyntheticGeneratorProtocol: ...


class JiraClientProtocol(Protocol):
    def iter_issues(
        self,
        *,
        jql: str,
        fields: Iterable[str] | None = None,
        expand_changelog: bool = True,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]: ...

    def iter_issue_comments(
        self,
        *,
        issue_id_or_key: str,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]: ...

    def get_sprint(self, *, sprint_id: str) -> dict[str, Any]: ...

    def close(self) -> None: ...


class JiraClientFactory(Protocol):
    def __call__(self) -> JiraClientProtocol: ...


class JiraProviderIngestProtocol(Protocol):
    def __call__(
        self,
        *,
        since: datetime,
        until: datetime | None,
        status_mapping: StatusMapping,
        identity: IdentityResolver,
        project_keys: Sequence[str] | None,
    ) -> tuple[
        list[WorkItem],
        list[WorkItemStatusTransition],
        list[WorkItemDependency],
        list[WorkItemReopenEvent],
        list[WorkItemInteractionEvent],
        list[Sprint],
    ]: ...


class GitHubClientProtocol(Protocol):
    def iter_issues(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        since: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]: ...

    def iter_issue_events(
        self, issue: Any, *, limit: int | None = None
    ) -> Iterable[Any]: ...

    def iter_project_v2_items(
        self,
        *,
        org_login: str,
        project_number: int,
        first: int = 50,
        max_items: int | None = None,
    ) -> Iterable[dict[str, Any]]: ...


class GitHubClientFactory(Protocol):
    def __call__(self, *, token: str) -> GitHubClientProtocol: ...


class GitLabClientProtocol(Protocol):
    def iter_project_issues(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
        updated_after: datetime | None = None,
    ) -> Iterable[Any]: ...


class GitLabClientFactory(Protocol):
    def __call__(self) -> GitLabClientProtocol: ...


@dataclass(frozen=True)
class MetricsDependencyRegistry:
    synthetic_generator_factory: SyntheticGeneratorFactory
    jira_client_factory: JiraClientFactory
    jira_build_jql: Callable[..., str]
    jira_issue_to_work_item: Callable[
        ..., tuple[WorkItem, list[WorkItemStatusTransition]]
    ]
    jira_extract_dependencies: Callable[..., list[WorkItemDependency]]
    jira_detect_reopen_events: Callable[..., list[WorkItemReopenEvent]]
    jira_comment_to_interaction: Callable[..., WorkItemInteractionEvent | None]
    jira_sprint_to_model: Callable[[Any], Sprint | None]
    jira_provider_ingest: JiraProviderIngestProtocol
    github_client_factory: GitHubClientFactory
    github_issue_to_work_item: Callable[
        ..., tuple[WorkItem, list[WorkItemStatusTransition]]
    ]
    github_project_item_to_work_item: Callable[
        ..., tuple[WorkItem | None, list[WorkItemStatusTransition]]
    ]
    gitlab_client_factory: GitLabClientFactory
    gitlab_issue_to_work_item: Callable[
        ..., tuple[WorkItem, list[WorkItemStatusTransition]]
    ]
    get_global_client: Callable[[str], Any]
    clickhouse_query_dicts: Callable[[Any, str, dict[str, Any]], list[dict[str, Any]]]
    user_metrics_daily_record: type[UserMetricsDailyRecord]
    work_item_user_metrics_daily_record: type[WorkItemUserMetricsDailyRecord]


def _default_make_synthetic_generator(
    *, repo_id: Any, repo_name: str
) -> SyntheticGeneratorProtocol:
    return fixtures_generator_module.SyntheticDataGenerator(
        repo_id=repo_id,
        repo_name=repo_name,
    )


def _default_make_jira_client() -> JiraClientProtocol:
    return jira_client_module.JiraClient.from_env()


def _default_jira_provider_ingest(
    *,
    since: datetime,
    until: datetime | None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    project_keys: Sequence[str] | None,
) -> tuple[
    list[WorkItem],
    list[WorkItemStatusTransition],
    list[WorkItemDependency],
    list[WorkItemReopenEvent],
    list[WorkItemInteractionEvent],
    list[Sprint],
]:
    ctx = IngestionContext(
        window=IngestionWindow(updated_since=since, active_until=until)
    )
    if project_keys:
        if len(project_keys) == 1:
            ctx = replace(ctx, project_key=project_keys[0])

    provider = jira_provider_module.JiraProvider(
        status_mapping=status_mapping,
        identity=identity,
    )
    batch_work_items: list[WorkItem] = []
    batch_status_transitions: list[WorkItemStatusTransition] = []
    batch_dependencies: list[WorkItemDependency] = []
    batch_reopen_events: list[WorkItemReopenEvent] = []
    batch_interactions: list[WorkItemInteractionEvent] = []
    batch_sprints: list[Sprint] = []
    for batch in provider.iter_ingest(ctx):
        batch_work_items.extend(batch.work_items)
        batch_status_transitions.extend(batch.status_transitions)
        batch_dependencies.extend(batch.dependencies)
        batch_reopen_events.extend(batch.reopen_events)
        batch_interactions.extend(batch.interactions)
        batch_sprints.extend(batch.sprints)
    return (
        batch_work_items,
        batch_status_transitions,
        batch_dependencies,
        batch_reopen_events,
        batch_interactions,
        batch_sprints,
    )


def _default_make_github_client(*, token: str) -> GitHubClientProtocol:
    return github_client_module.GitHubWorkClient(
        auth=github_client_module.GitHubAuth(token=token)
    )


def _default_make_gitlab_client() -> GitLabClientProtocol:
    return gitlab_client_module.GitLabWorkClient.from_env()


_registry = MetricsDependencyRegistry(
    synthetic_generator_factory=_default_make_synthetic_generator,
    jira_client_factory=_default_make_jira_client,
    jira_build_jql=jira_client_module.build_jira_jql,
    jira_issue_to_work_item=jira_normalize_module.jira_issue_to_work_item,
    jira_extract_dependencies=jira_normalize_module.extract_jira_issue_dependencies,
    jira_detect_reopen_events=jira_normalize_module.detect_reopen_events,
    jira_comment_to_interaction=jira_normalize_module.jira_comment_to_interaction_event,
    jira_sprint_to_model=jira_normalize_module.jira_sprint_payload_to_model,
    jira_provider_ingest=_default_jira_provider_ingest,
    github_client_factory=_default_make_github_client,
    github_issue_to_work_item=github_normalize_module.github_issue_to_work_item,
    github_project_item_to_work_item=github_normalize_module.github_project_v2_item_to_work_item,
    gitlab_client_factory=_default_make_gitlab_client,
    gitlab_issue_to_work_item=gitlab_normalize_module.gitlab_issue_to_work_item,
    get_global_client=queries_client_module.get_global_client,
    clickhouse_query_dicts=clickhouse_query_dicts,
    user_metrics_daily_record=UserMetricsDailyRecord,
    work_item_user_metrics_daily_record=WorkItemUserMetricsDailyRecord,
)


def get_metrics_dependencies() -> MetricsDependencyRegistry:
    return _registry


def register_metrics_dependencies(registry: MetricsDependencyRegistry) -> None:
    global _registry
    _registry = registry
