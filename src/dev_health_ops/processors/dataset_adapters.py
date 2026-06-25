from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime, time, timezone
from typing import Any

from dev_health_ops.credentials.resolver import (
    github_credentials_from_mapping,
    gitlab_credentials_from_mapping,
    resolve_gitlab_url,
)
from dev_health_ops.storage import resolve_db_type, run_with_store
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.sync_bootstrap import ProviderRuntime, SyncTaskContext

_PROCESSOR_FLAG_NAMES = (
    "sync_git",
    "sync_prs",
    "sync_cicd",
    "sync_deployments",
    "sync_incidents",
    "sync_security",
    "sync_tests",
    "blame_only",
    "sync_commits",
    "sync_commit_stats",
    "sync_files",
    "sync_blame",
)

_CODE_DATASETS = frozenset(
    {
        DatasetKey.REPO_METADATA.value,
        DatasetKey.COMMITS.value,
        DatasetKey.COMMIT_STATS.value,
        DatasetKey.FILES.value,
        DatasetKey.BLAME.value,
        DatasetKey.PRS.value,
        DatasetKey.PR_REVIEWS.value,
        DatasetKey.PR_COMMENTS.value,
        DatasetKey.CICD.value,
        DatasetKey.TESTS.value,
        DatasetKey.DEPLOYMENTS.value,
        DatasetKey.INCIDENTS.value,
        DatasetKey.SECURITY.value,
    }
)

_WORK_ITEM_DATASETS = frozenset(
    {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }
)


async def _run_with_reused_or_new_store(
    context: SyncTaskContext,
    runtime: ProviderRuntime,
    handler: Callable[[Any], Awaitable[Any]],
) -> Any:
    if runtime.store is not None:
        if context.org_id:
            setattr(runtime.store, "org_id", context.org_id)
        return await handler(runtime.store)

    db_type = resolve_db_type(context.db_url, None)
    return await run_with_store(
        context.db_url,
        db_type,
        handler,
        org_id=context.org_id,
    )


def _credentials_mapping(context: SyncTaskContext) -> dict[str, Any]:
    credentials = context.decrypted_credentials or {}
    if isinstance(credentials, dict):
        return credentials
    return {}


def _explicit_flags(context: SyncTaskContext) -> dict[str, bool]:
    flags = {name: False for name in _PROCESSOR_FLAG_NAMES}
    flags.update(
        {
            str(name): bool(value)
            for name, value in (context.processor_flags or {}).items()
            if str(name) in flags
        }
    )
    return flags


def _github_repo_parts(source_external_id: str) -> tuple[str, str]:
    parts = (source_external_id or "").split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "GitHub dataset unit requires source_external_id in 'owner/repo' form"
        )
    return parts[0], parts[1]


def _github_credentials(context: SyncTaskContext) -> Any:
    credentials = context.decrypted_credentials
    if not isinstance(credentials, dict):
        return credentials
    resolved = github_credentials_from_mapping(credentials)
    if resolved is None:
        raise ValueError("Missing GitHub credentials for dataset unit")
    return resolved


def _gitlab_credentials(context: SyncTaskContext) -> tuple[str, str]:
    mapping = _credentials_mapping(context)
    resolved = gitlab_credentials_from_mapping(mapping)
    if resolved is None:
        raise ValueError("Missing GitLab token for dataset unit")
    gitlab_url = resolve_gitlab_url(mapping, resolved)
    return resolved.token, gitlab_url


def _gitlab_project_id(source_external_id: str) -> int:
    try:
        return int(source_external_id)
    except (TypeError, ValueError):
        pass
    raise ValueError(
        "GitLab dataset unit requires numeric source_external_id project id"
    )


def _window_backfill_days(context: SyncTaskContext) -> int:
    if context.window_start is None or context.window_end is None:
        return 1
    start_day = context.window_start.date()
    end_day = context.window_end.date()
    return max(1, (end_day - start_day).days + 1)


def _window_day(context: SyncTaskContext) -> date:
    window_end = context.window_end
    if window_end is not None:
        return window_end.date()
    window_start = context.window_start
    if window_start is not None:
        return window_start.date()
    return datetime.now(timezone.utc).date()


def _window_start_from_work_item_args(context: SyncTaskContext) -> datetime | None:
    if context.window_start is not None:
        return context.window_start
    day = _window_day(context)
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def _work_item_kwargs(context: SyncTaskContext) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "db_url": context.db_url,
        "day": _window_day(context),
        "backfill_days": _window_backfill_days(context),
        "provider": context.provider,
        "org_id": context.org_id,
        "credentials": _credentials_mapping(context) or None,
    }
    if context.provider in {"github", "gitlab", "linear"}:
        kwargs["repo_name"] = context.source_external_id
    if context.provider == "jira":
        kwargs["jira_project_keys"] = [context.source_external_id]
    if context.provider == "gitlab" and kwargs["credentials"]:
        token, gitlab_url = _gitlab_credentials(context)
        kwargs["credentials"] = {
            **kwargs["credentials"],
            "token": token,
            "gitlab_url": gitlab_url,
        }
    if context.provider == "github":
        flags = _explicit_flags(context)
        kwargs["include_issues"] = context.dataset_key in _WORK_ITEM_DATASETS
        # CHAOS-646: only ingest PRs as work items when the PRS dataset is also
        # enabled for this config. The planner stamps ``sync_prs`` on the github
        # work-items unit (False when PRs are not selected); None would let the
        # provider fall back to the GITHUB_INCLUDE_PRS env default (PRs ON).
        kwargs["include_pull_requests"] = flags["sync_prs"]
    return kwargs


def _run_github_dataset(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    from dev_health_ops.processors.github import process_github_repo

    owner, repo_name = _github_repo_parts(context.source_external_id)
    flags = _explicit_flags(context)
    token = _github_credentials(context)

    async def _handler(store: Any) -> None:
        await process_github_repo(
            store=store,
            owner=owner,
            repo_name=repo_name,
            token=token,
            fetch_blame=False,
            blame_only=flags["blame_only"],
            max_commits=None,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            sync_security=flags["sync_security"],
            sync_tests=flags["sync_tests"],
            backfill_missing=False,
            since=context.window_start,
            until=context.window_end,
            sync_commits=flags["sync_commits"],
            sync_commit_stats=flags["sync_commit_stats"],
            sync_files=flags["sync_files"],
            sync_blame=flags["sync_blame"],
        )

    run_async(_run_with_reused_or_new_store(context, runtime, _handler))
    return {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "owner": owner,
        "repo": repo_name,
        "flags": flags,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }


def _run_gitlab_dataset(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    from dev_health_ops.processors.gitlab import process_gitlab_project

    project_id = _gitlab_project_id(context.source_external_id)
    token, gitlab_url = _gitlab_credentials(context)
    flags = _explicit_flags(context)

    async def _handler(store: Any) -> None:
        await process_gitlab_project(
            store=store,
            project_id=project_id,
            token=token,
            gitlab_url=gitlab_url,
            fetch_blame=False,
            blame_only=flags["blame_only"],
            max_commits=None,
            sync_git=flags["sync_git"],
            sync_prs=flags["sync_prs"],
            sync_cicd=flags["sync_cicd"],
            sync_deployments=flags["sync_deployments"],
            sync_incidents=flags["sync_incidents"],
            sync_security=flags["sync_security"],
            sync_tests=flags["sync_tests"],
            backfill_missing=False,
            since=context.window_start,
            until=context.window_end,
            sync_commits=flags["sync_commits"],
            sync_commit_stats=flags["sync_commit_stats"],
            sync_files=flags["sync_files"],
            sync_blame=flags["sync_blame"],
        )

    run_async(_run_with_reused_or_new_store(context, runtime, _handler))
    return {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "project_id": project_id,
        "gitlab_url": gitlab_url,
        "flags": flags,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }


def _run_work_item_dataset(context: SyncTaskContext) -> dict[str, Any]:
    from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

    kwargs = _work_item_kwargs(context)
    run_work_items_sync_job(**kwargs)
    window_start = _window_start_from_work_item_args(context)
    return {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "work_items_synced": True,
        "day": kwargs["day"].isoformat(),
        "backfill_days": kwargs["backfill_days"],
        "window_start": window_start.isoformat() if window_start is not None else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }


def _run_feature_flags_dataset(context: SyncTaskContext) -> dict[str, Any]:
    from dev_health_ops.workers.feature_flag_sync import (
        _sync_gitlab_feature_flags,
        _sync_launchdarkly_feature_flags,
    )

    credentials = _credentials_mapping(context)
    sync_options: dict[str, Any] = {
        "project_id": context.source_external_id,
        "project_key": context.source_external_id,
    }
    if context.provider == "gitlab":
        result = _sync_gitlab_feature_flags(
            db_url=context.db_url,
            org_id=context.org_id,
            credentials=credentials,
            sync_options=sync_options,
        )
    elif context.provider == "launchdarkly":
        result = _sync_launchdarkly_feature_flags(
            db_url=context.db_url,
            org_id=context.org_id,
            credentials=credentials,
            sync_options=sync_options,
            since_dt=context.window_start,
        )
    else:
        raise ValueError(
            f"Unsupported feature-flags provider: provider={context.provider!r}"
        )

    return {
        "provider": context.provider,
        "dataset": context.dataset_key,
        "source": context.source_external_id,
        "feature_flags": result,
        "window_start": context.window_start.isoformat()
        if context.window_start is not None
        else None,
        "window_end": context.window_end.isoformat()
        if context.window_end is not None
        else None,
    }


def run_dataset_unit(
    context: SyncTaskContext, runtime: ProviderRuntime
) -> dict[str, Any]:
    provider = context.provider.lower()
    dataset_key = context.dataset_key

    if dataset_key in _CODE_DATASETS:
        if provider == "github":
            return _run_github_dataset(context, runtime)
        if provider == "gitlab":
            return _run_gitlab_dataset(context, runtime)

    if dataset_key in _WORK_ITEM_DATASETS and provider in {
        "github",
        "gitlab",
        "jira",
        "linear",
    }:
        return _run_work_item_dataset(context)

    if dataset_key == DatasetKey.FEATURE_FLAGS.value:
        return _run_feature_flags_dataset(context)

    raise ValueError(
        "Unsupported provider dataset unit: "
        f"provider={context.provider!r} dataset={context.dataset_key!r}"
    )
