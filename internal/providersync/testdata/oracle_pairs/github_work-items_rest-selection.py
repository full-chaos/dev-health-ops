"""Live producer oracle for GitHub work-item controls and issue selection."""

from __future__ import annotations

import contextlib
import io
import pathlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TypeVar
from uuid import UUID

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports(real_client=True)

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.processors.dataset_adapters import _github_work_item_options
    from dev_health_ops.providers.base import (
        IngestionContext,
        IngestionWindow,
        WorkItemIngestionOptions,
    )
    from dev_health_ops.providers.github.client import (
        GitHubWorkClient,
        _GitHubCommentLike,
        _GitHubEventLike,
        _GitHubIssueBaseLike,
        _GitHubIssueLike,
        _GitHubMilestoneLike,
        _GitHubPullRequestLike,
    )
    from dev_health_ops.providers.github.provider import GitHubProvider
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping
    from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_ADAPTER_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/dataset_adapters.py"
_TItem = TypeVar("_TItem")


@dataclass(frozen=True)
class _OracleUser:
    login: str
    email: str | None = None
    name: str | None = None


@dataclass(frozen=True)
class _OracleIssue:
    number: int
    title: str
    state: str
    body: str
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    html_url: str
    url: str
    pull_request: object | None
    labels: tuple[object, ...]
    assignees: tuple[_OracleUser, ...]
    user: _OracleUser

    def get_comments(self) -> Iterable[_GitHubCommentLike]:
        return ()

    def get_events(self) -> Iterable[_GitHubEventLike]:
        return ()


def _issue(
    number: int, updated_at: datetime, *, pull_stub: bool = False
) -> _OracleIssue:
    return _OracleIssue(
        number=number,
        title=f"Issue {number}",
        state="open",
        body="",
        created_at=datetime(2026, 7, 2, tzinfo=timezone.utc),
        updated_at=updated_at,
        closed_at=None,
        html_url=f"https://github.com/acme/api/issues/{number}",
        url=f"https://api.github.com/repos/acme/api/issues/{number}",
        pull_request=object() if pull_stub else None,
        labels=(),
        assignees=(),
        user=_OracleUser(login="reporter"),
    )


class _Repo:
    def __init__(self, issues: list[_OracleIssue]) -> None:
        self.issues = issues

    def get_issues(self, *args: object, **kwargs: object) -> Iterable[Any]:
        return self.issues


class _OracleGitHubWorkClient(GitHubWorkClient):
    """Network-free client that preserves production issue selection."""

    def __init__(self) -> None:
        self.issue_calls = 0
        self.pr_calls = 0
        self.milestone_calls = 0
        self.comment_calls = 0
        self.event_limit = -1
        self.comment_limit = -1
        self.repo = _Repo(
            [
                _issue(1, datetime(2026, 7, 20, tzinfo=timezone.utc)),
                _issue(2, datetime(2026, 8, 1, tzinfo=timezone.utc)),
                _issue(
                    3,
                    datetime(2026, 7, 20, tzinfo=timezone.utc),
                    pull_stub=True,
                ),
            ]
        )

    def get_repo(self, **_kwargs: Any) -> _Repo:
        self.issue_calls += 1
        return self.repo

    def _call_github(self, operation: str, call: Callable[[], _TItem]) -> _TItem:
        return call()

    def _iter_github_items(
        self,
        source: Iterable[_TItem],
        *,
        operation: str,
        limit: int | None,
        skip: Callable[[_TItem], bool] | None = None,
        stop: Callable[[_TItem], bool] | None = None,
        scan_limit: int | None = None,
    ) -> Iterable[_TItem]:
        yield from self._iter_with_limit(
            source,
            operation=operation,
            limit=limit,
            skip=skip,
            stop=stop,
            scan_limit=scan_limit,
        )

    def iter_repo_milestones(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        limit: int | None = None,
    ) -> Iterable[_GitHubMilestoneLike]:
        self.milestone_calls += 1
        return ()

    def iter_issue_events(
        self,
        issue: _GitHubIssueLike | _GitHubPullRequestLike,
        *,
        limit: int | None = None,
    ) -> Iterable[_GitHubEventLike]:
        self.event_limit = -1 if limit is None else limit
        return ()

    def iter_issue_comments(
        self, issue: _GitHubIssueBaseLike, *, limit: int | None = None
    ) -> Iterable[_GitHubCommentLike]:
        self.comment_calls += 1
        self.comment_limit = -1 if limit is None else limit
        return ()

    def iter_pull_requests(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        sort: str = "updated",
        direction: str = "desc",
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
        scan_limit: int | None = None,
    ) -> Iterable[_GitHubPullRequestLike]:
        self.pr_calls += 1
        return ()


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    dataset_options = dict(case["dataset_options"])
    adapter_context = SyncTaskContext(
        unit_id="oracle-unit",
        sync_run_id="oracle-run",
        org_id="oracle-org",
        integration_id="oracle-integration",
        source_id="oracle-source",
        source_external_id="acme/api",
        provider="github",
        dataset_key="work-items",
        cost_class="network",
        mode="incremental",
        window_start=None,
        window_end=None,
        processor_flags={"sync_prs": bool(case["sync_prs"])},
        credential_id=None,
        decrypted_credentials={},
        db_url="postgresql://oracle.invalid/oracle",
        dataset_options=dataset_options,
    )
    controls = _github_work_item_options(adapter_context)
    client = _OracleGitHubWorkClient()
    context = IngestionContext(
        repo="acme/api",
        repo_id=UUID("616d4a76-b639-d421-808b-3cef6940d4b9"),
        window=IngestionWindow(
            updated_since=datetime(2026, 7, 1, tzinfo=timezone.utc),
            active_until=datetime(2026, 7, 31, tzinfo=timezone.utc),
        ),
        work_item_options=WorkItemIngestionOptions(**controls),
    )
    provider = GitHubProvider(
        status_mapping=load_status_mapping(),
        identity=load_identity_resolver(),
        client=client,
    )
    batch = provider._ingest_with_client(client=client, ctx=context)
    return {
        **controls,
        "selected_work_item_ids": [item.work_item_id for item in batch.work_items],
        "issue_calls": client.issue_calls,
        "pr_calls": client.pr_calls,
        "milestone_calls": client.milestone_calls,
        "comment_calls": client.comment_calls,
        "event_limit": client.event_limit,
        "comment_limit_observed": client.comment_limit,
    }


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items/rest-selection",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _ADAPTER_SOURCE.read_text(),
            "_github_work_item_options",
            (RETURN_LITERAL,),
        ),
    )
)
