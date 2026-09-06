from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
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
    WorkItem,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.github import client as github_client_module
from dev_health_ops.providers.github import normalize as github_normalize_module
from dev_health_ops.providers.gitlab import client as gitlab_client_module
from dev_health_ops.providers.gitlab import normalize as gitlab_normalize_module


class SyntheticGeneratorProtocol(Protocol):
    def generate_work_items(self, *, days: int) -> list[WorkItem]: ...

    def generate_work_item_transitions(
        self, items: list[WorkItem]
    ) -> list[WorkItemStatusTransition]: ...


class SyntheticGeneratorFactory(Protocol):
    def __call__(
        self, *, repo_id: Any, repo_name: str
    ) -> SyntheticGeneratorProtocol: ...


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
    ) -> Iterable[Any]: ...


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

    def iter_project_merge_requests(
        self,
        *,
        project_id_or_path: str,
        state: str = "all",
        updated_after: datetime | None = None,
    ) -> Iterable[Any]: ...


class GitLabClientFactory(Protocol):
    def __call__(
        self, *, token: str, gitlab_url: str | None = None
    ) -> GitLabClientProtocol: ...


@dataclass(frozen=True)
class MetricsDependencyRegistry:
    synthetic_generator_factory: SyntheticGeneratorFactory
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


def _default_make_github_client(*, token: str) -> GitHubClientProtocol:
    return github_client_module.GitHubWorkClient(
        auth=github_client_module.GitHubAuth(token=token)
    )


def _default_make_gitlab_client(
    *, token: str, gitlab_url: str | None = None
) -> GitLabClientProtocol:
    return gitlab_client_module.GitLabWorkClient(
        auth=gitlab_client_module.GitLabAuth(
            token=token,
            base_url=gitlab_url or "https://gitlab.com",
        )
    )


_registry = MetricsDependencyRegistry(
    synthetic_generator_factory=_default_make_synthetic_generator,
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
