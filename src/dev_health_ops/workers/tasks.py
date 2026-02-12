"""Celery task definitions for background job processing.

These tasks wrap the existing metrics jobs to enable async execution:
- run_daily_metrics: Compute and persist daily metrics
- run_complexity_job: Analyze code complexity
- run_work_items_sync: Sync work items from dev_health_ops.providers
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import fnmatch

from celery import chord, group

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


def _get_db_url() -> str:
    """Get data-store URL from environment.

    Prefers CLICKHOUSE_URI (the primary data store for sync/metrics),
    falling back to DATABASE_URI which may point to Postgres (admin DB).
    """
    return (
        os.getenv("CLICKHOUSE_URI")
        or os.getenv("DATABASE_URI")
        or os.getenv("DATABASE_URL")
        or ""
    )


def _merge_sync_flags(sync_targets: list[str]) -> dict[str, bool]:
    from dev_health_ops.processors.sync import _sync_flags_for_target

    merged_flags: dict[str, bool] = {}
    for target in sync_targets:
        flags = _sync_flags_for_target(target)
        for key, enabled in flags.items():
            if enabled:
                merged_flags[key] = True

    for key in (
        "sync_git",
        "sync_prs",
        "sync_cicd",
        "sync_deployments",
        "sync_incidents",
        "blame_only",
    ):
        merged_flags.setdefault(key, False)

    return merged_flags


def _extract_owner_repo(
    config_name: str, sync_options: dict[str, Any]
) -> tuple[str, str] | None:
    owner = sync_options.get("owner")
    repo_name = sync_options.get("repo")
    if owner and repo_name:
        return str(owner), str(repo_name)

    search = sync_options.get("search")
    if isinstance(search, str) and "/" in search:
        search_owner, search_repo = search.split("/", 1)
        repo_candidate = search_repo.replace("*", "").replace("?", "").strip()
        if search_owner and repo_candidate:
            return search_owner.strip(), repo_candidate

    if "/" in config_name:
        name_owner, name_repo = config_name.split("/", 1)
        if name_owner and name_repo:
            return name_owner.strip(), name_repo.strip()

    return None


def _decrypt_credential_sync(credential) -> dict[str, Any]:
    from dev_health_ops.api.services.settings import decrypt_value

    if credential.credentials_encrypted:
        return json.loads(decrypt_value(credential.credentials_encrypted))
    return {}


def _inject_provider_token(provider: str, token: str) -> None:
    env_var = {
        "github": "GITHUB_TOKEN",
        "gitlab": "GITLAB_TOKEN",
    }.get(provider.lower())
    if env_var and token:
        os.environ[env_var] = token


def _resolve_env_credentials(provider: str) -> dict[str, str]:
    from dev_health_ops.credentials.resolver import PROVIDER_ENV_VARS

    env_map = PROVIDER_ENV_VARS.get(provider.lower(), {})
    return {
        field_name: value
        for field_name, env_var in env_map.items()
        if (value := os.getenv(env_var))
    }


_GIT_TARGETS = {"git", "prs"}
_WORK_ITEM_TARGETS = {"work-items"}


def _dispatch_post_sync_tasks(
    *,
    provider: str,
    sync_targets: list[str],
    org_id: str,
) -> None:
    target_set = set(sync_targets)
    has_git = bool(target_set & _GIT_TARGETS)
    has_work_items = bool(target_set & _WORK_ITEM_TARGETS)
    dispatched: list[str] = []

    if has_git:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_daily_metrics",
            queue="metrics",
        )
        dispatched.append("run_daily_metrics")

        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_complexity_job",
            queue="metrics",
        )
        dispatched.append("run_complexity_job")

    if has_git and has_work_items:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_work_graph_build",
            queue="metrics",
        )
        dispatched.append("run_work_graph_build")

    if provider == "gitlab" and has_git:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_dora_metrics",
            queue="metrics",
        )
        dispatched.append("run_dora_metrics")

    if dispatched:
        logger.info(
            "Post-sync dispatch for config org_id=%s provider=%s targets=%s: %s",
            org_id,
            provider,
            sync_targets,
            dispatched,
        )


@celery_app.task(bind=True, max_retries=3, queue="sync")
def run_sync_config(
    self,
    config_id: str,
    org_id: str = "default",
    triggered_by: str = "manual",
) -> dict:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
    from dev_health_ops.models.settings import (
        IntegrationCredential,
        JobRun,
        JobRunStatus,
        JobStatus,
        ScheduledJob,
        SyncConfiguration,
    )
    from dev_health_ops.processors.github import process_github_repo
    from dev_health_ops.processors.gitlab import process_gitlab_project
    from dev_health_ops.storage import resolve_db_type, run_with_store
    from dev_health_ops.sync.watermarks import get_watermark, set_watermark

    config_uuid = uuid.UUID(config_id)
    db_url = _get_db_url()
    db_type = resolve_db_type(db_url, None)

    logger.info(
        "Starting sync config task: config_id=%s org_id=%s triggered_by=%s",
        config_id,
        org_id,
        triggered_by,
    )

    run_id: uuid.UUID | None = None
    job_id: uuid.UUID | None = None
    started_at = datetime.now(timezone.utc)
    provider = ""
    config_name = ""
    sync_targets: list[str] = []
    sync_options: dict[str, Any] = {}
    credentials: dict[str, Any] = {}

    try:
        with get_postgres_session_sync() as session:
            config = (
                session.query(SyncConfiguration)
                .filter(
                    SyncConfiguration.id == config_uuid,
                    SyncConfiguration.org_id == org_id,
                )
                .one_or_none()
            )
            if config is None:
                raise ValueError(f"Sync configuration not found: {config_id}")

            provider = (config.provider or "").lower()
            config_name = config.name
            sync_targets = list(config.sync_targets or [])
            sync_options = dict(config.sync_options or {})

            if config.credential_id:
                credential = (
                    session.query(IntegrationCredential)
                    .filter(
                        IntegrationCredential.id == config.credential_id,
                        IntegrationCredential.org_id == org_id,
                    )
                    .one_or_none()
                )
                if credential is None:
                    raise ValueError(
                        f"Credential not found for sync configuration: {config.credential_id}"
                    )

                credentials = _decrypt_credential_sync(credential)
            else:
                credentials = _resolve_env_credentials(provider)

            job = (
                session.query(ScheduledJob)
                .filter(
                    ScheduledJob.org_id == org_id,
                    ScheduledJob.sync_config_id == config_uuid,
                )
                .one_or_none()
            )
            if job is None:
                job = ScheduledJob(
                    name=f"sync-config-{config.id}",
                    job_type="sync",
                    schedule_cron="0 * * * *",
                    org_id=org_id,
                    job_config={
                        "provider": provider,
                        "sync_config_id": str(config.id),
                    },
                    sync_config_id=config.id,
                    status=JobStatus.ACTIVE.value,
                )
                session.add(job)
                session.flush()

            job_id = job.id

            run = JobRun(
                job_id=job.id,
                triggered_by=triggered_by,
                status=JobRunStatus.PENDING.value,
            )
            session.add(run)
            session.flush()
            run_id = run.id

            run.status = JobRunStatus.RUNNING.value
            run.started_at = started_at
            job.is_running = True
            job.last_run_at = started_at
            session.flush()

        result_payload: dict[str, Any] = {
            "provider": provider,
            "config_id": config_id,
            "sync_targets": sync_targets,
            "triggered_by": triggered_by,
        }

        since_dt: datetime | None = None
        full_resync = bool(sync_options.get("full_resync"))
        repo_id_for_watermark: str | None = None

        if provider == "github":
            _owr = _extract_owner_repo(
                config_name=config_name, sync_options=sync_options
            )
            if _owr:
                repo_id_for_watermark = f"{_owr[0]}/{_owr[1]}"
        elif provider == "gitlab":
            _pid = sync_options.get("project_id")
            if _pid is not None:
                repo_id_for_watermark = str(_pid)

        if repo_id_for_watermark and not full_resync:
            with get_postgres_session_sync() as session:
                watermarks = [
                    get_watermark(session, org_id, repo_id_for_watermark, t)
                    for t in sync_targets
                ]
                valid = [w for w in watermarks if w is not None]
                if valid and len(valid) == len(sync_targets):
                    since_dt = min(valid)

        if provider == "github":
            owner_repo = _extract_owner_repo(
                config_name=config_name, sync_options=sync_options
            )
            if owner_repo is None:
                raise ValueError(
                    "Missing GitHub owner/repo in sync options or config name"
                )

            owner, repo_name = owner_repo
            token = str(credentials.get("token") or "")
            if not token:
                raise ValueError("Missing GitHub token for sync configuration")

            merged_flags = _merge_sync_flags(sync_targets)

            async def _github_handler(store):
                await process_github_repo(
                    store=store,
                    owner=owner,
                    repo_name=repo_name,
                    token=token,
                    since=since_dt,
                    **merged_flags,
                )

            asyncio.run(run_with_store(db_url, db_type, _github_handler))
            result_payload.update(
                {
                    "owner": owner,
                    "repo": repo_name,
                    "flags": merged_flags,
                }
            )

        elif provider == "gitlab":
            project_id = sync_options.get("project_id")
            if project_id is None:
                raise ValueError("Missing GitLab project_id in sync options")

            token = str(credentials.get("token") or "")
            if not token:
                raise ValueError("Missing GitLab token for sync configuration")

            gitlab_url = str(sync_options.get("gitlab_url", "https://gitlab.com"))
            merged_flags = _merge_sync_flags(sync_targets)

            async def _gitlab_handler(store):
                await process_gitlab_project(
                    store=store,
                    project_id=int(project_id),
                    token=token,
                    gitlab_url=gitlab_url,
                    since=since_dt,
                    **merged_flags,
                )

            asyncio.run(run_with_store(db_url, db_type, _gitlab_handler))
            result_payload.update(
                {
                    "project_id": int(project_id),
                    "gitlab_url": gitlab_url,
                    "flags": merged_flags,
                }
            )

        elif provider == "jira":
            backfill_days = int(sync_options.get("backfill_days", 1))
            run_work_items_sync_job(
                db_url=db_url,
                day=date.today(),
                backfill_days=backfill_days,
                provider="jira",
            )
            result_payload["backfill_days"] = backfill_days

        if "work-items" in sync_targets and provider != "jira":
            token = str(credentials.get("token") or "")
            if token:
                _inject_provider_token(provider, token)
            backfill_days = int(sync_options.get("backfill_days", 1))
            run_work_items_sync_job(
                db_url=db_url,
                day=date.today(),
                backfill_days=backfill_days,
                provider=provider,
                repo_name=sync_options.get("repo"),
                search_pattern=sync_options.get("search"),
            )
            result_payload["work_items_synced"] = True

        completed_at = datetime.now(timezone.utc)
        duration_seconds = int((completed_at - started_at).total_seconds())

        with get_postgres_session_sync() as session:
            run = session.query(JobRun).filter(JobRun.id == run_id).one_or_none()
            job = (
                session.query(ScheduledJob)
                .filter(ScheduledJob.id == job_id)
                .one_or_none()
            )
            config = (
                session.query(SyncConfiguration)
                .filter(
                    SyncConfiguration.id == config_uuid,
                    SyncConfiguration.org_id == org_id,
                )
                .one_or_none()
            )

            if run:
                run.status = JobRunStatus.SUCCESS.value
                run.completed_at = completed_at
                run.duration_seconds = duration_seconds
                run.result = result_payload
                run.error = None

            if job:
                job.is_running = False
                job.last_run_status = JobRunStatus.SUCCESS.value
                job.last_run_duration_seconds = duration_seconds
                job.last_run_error = None
                job.run_count = int(job.run_count or 0) + 1

            if config:
                config.last_sync_at = completed_at
                config.last_sync_success = True
                config.last_sync_error = None
                config.last_sync_stats = result_payload

            session.flush()

            if repo_id_for_watermark:
                for t in sync_targets:
                    set_watermark(session, org_id, repo_id_for_watermark, t, started_at)
                session.flush()

        _dispatch_post_sync_tasks(
            provider=provider,
            sync_targets=sync_targets,
            org_id=org_id,
        )

        return {
            "status": "success",
            "job_run_id": str(run_id),
            "result": result_payload,
        }

    except Exception as exc:
        logger.exception(
            "Sync config task failed: config_id=%s org_id=%s error=%s",
            config_id,
            org_id,
            exc,
        )

        completed_at = datetime.now(timezone.utc)
        duration_seconds = int((completed_at - started_at).total_seconds())

        try:
            if run_id is not None:
                with get_postgres_session_sync() as session:
                    run = (
                        session.query(JobRun).filter(JobRun.id == run_id).one_or_none()
                    )
                    job = (
                        session.query(ScheduledJob)
                        .filter(ScheduledJob.id == job_id)
                        .one_or_none()
                    )
                    config = (
                        session.query(SyncConfiguration)
                        .filter(
                            SyncConfiguration.id == config_uuid,
                            SyncConfiguration.org_id == org_id,
                        )
                        .one_or_none()
                    )

                    if run:
                        run.status = JobRunStatus.FAILED.value
                        run.completed_at = completed_at
                        run.duration_seconds = duration_seconds
                        run.error = str(exc)

                    if job:
                        job.is_running = False
                        job.last_run_status = JobRunStatus.FAILED.value
                        job.last_run_duration_seconds = duration_seconds
                        job.last_run_error = str(exc)
                        job.run_count = int(job.run_count or 0) + 1
                        job.failure_count = int(job.failure_count or 0) + 1

                    if config:
                        config.last_sync_at = completed_at
                        config.last_sync_success = False
                        config.last_sync_error = str(exc)

                    session.flush()
        except Exception as update_error:
            logger.error("Failed updating job run failure state: %s", update_error)

        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


def _is_batch_eligible(config) -> bool:
    """Check if a SyncConfiguration should be dispatched as a batch.

    A config is batch-eligible when:
    - Provider is github or gitlab
    - sync_options contains a 'search' key with a wildcard pattern (e.g. "org/*")
    - OR sync_options contains 'discover: true'
    """
    provider = (config.provider or "").lower()
    if provider not in ("github", "gitlab"):
        return False

    sync_options = dict(config.sync_options or {})

    if sync_options.get("discover") is True:
        return True

    search = sync_options.get("search")
    if isinstance(search, str) and ("*" in search or "?" in search):
        return True

    return False


def _discover_repos_for_config(
    config, credentials: dict[str, Any]
) -> list[tuple[str, ...]]:
    """Resolve wildcard/org-level search patterns to concrete repo lists.

    For GitHub: Uses PyGithub to list org repos, filtered by pattern.
    For GitLab: Uses python-gitlab to list group/user projects.

    Returns:
        List of (owner, repo_name) tuples for GitHub,
        or (project_id,) tuples for GitLab.
    """
    provider = (config.provider or "").lower()
    sync_options = dict(config.sync_options or {})
    token = str(credentials.get("token") or "")

    if provider == "github":
        return _discover_github_repos(sync_options, token)
    elif provider == "gitlab":
        return _discover_gitlab_repos(sync_options, token)
    return []


def _discover_github_repos(
    sync_options: dict[str, Any], token: str
) -> list[tuple[str, str]]:
    """Discover GitHub repos matching the search pattern."""
    from github import Github

    search = sync_options.get("search", "")
    owner = sync_options.get("owner", "")

    if isinstance(search, str) and "/" in search:
        parts = search.split("/", 1)
        owner = parts[0]
        repo_pattern = parts[1]
    else:
        repo_pattern = "*"

    if not owner:
        return []

    g = Github(token)
    try:
        org = g.get_organization(owner)
        repos = org.get_repos()
    except Exception:
        try:
            user = g.get_user(owner)
            repos = user.get_repos()
        except Exception:
            return []

    result: list[tuple[str, str]] = []
    for repo in repos:
        if fnmatch.fnmatch(repo.name, repo_pattern):
            result.append((owner, repo.name))

    return result


def _discover_gitlab_repos(
    sync_options: dict[str, Any], token: str
) -> list[tuple[str,]]:
    """Discover GitLab projects matching the search pattern."""
    import gitlab as gitlab_lib

    gitlab_url = str(sync_options.get("gitlab_url", "https://gitlab.com"))
    search = sync_options.get("search", "")
    group_path = sync_options.get("group", "")

    if isinstance(search, str) and "/" in search:
        parts = search.split("/", 1)
        group_path = parts[0]
        project_pattern = parts[1]
    else:
        project_pattern = "*"

    if not group_path:
        return []

    gl = gitlab_lib.Gitlab(gitlab_url, private_token=token)
    try:
        grp = gl.groups.get(group_path)
        projects = grp.projects.list(all=True)
    except Exception:
        return []

    result: list[tuple[str,]] = []
    for project in projects:
        name = getattr(project, "name", "") or ""
        project_id = getattr(project, "id", None)
        if project_id is not None and fnmatch.fnmatch(name, project_pattern):
            result.append((str(project_id),))

    return result


def _get_batch_size(sync_options: dict[str, Any]) -> int:
    """Get batch size from sync_options or environment, default 5."""
    size = sync_options.get("batch_size")
    if size is not None:
        return int(size)
    env_size = os.getenv("SYNC_BATCH_SIZE")
    if env_size is not None:
        return int(env_size)
    return 5


@celery_app.task(bind=True, queue="sync")
def _batch_sync_callback(
    self,
    results: list,
    *,
    provider: str,
    sync_targets: list[str],
    org_id: str,
) -> dict:
    """Chord callback: dispatch post-sync tasks after all batch children complete."""
    logger.info(
        "Batch sync callback: %d child results, dispatching post-sync tasks",
        len(results) if results else 0,
    )
    _dispatch_post_sync_tasks(
        provider=provider,
        sync_targets=sync_targets,
        org_id=org_id,
    )
    return {
        "status": "post_sync_dispatched",
        "child_results": len(results) if results else 0,
    }


@celery_app.task(bind=True, queue="sync", rate_limit="5/m")
def dispatch_batch_sync(
    self,
    config_id: str,
    org_id: str = "default",
    triggered_by: str = "schedule",
) -> dict:
    """Fan out a batch-eligible SyncConfiguration into per-repo Celery tasks.

    Discovers all matching repos via the provider API, then dispatches
    individual run_sync_config tasks per repo using a chord so that
    post-sync tasks fire exactly once after all children complete.

    Args:
        config_id: UUID of the SyncConfiguration
        org_id: Organization scope
        triggered_by: What triggered this dispatch

    Returns:
        dict with status, total_repos, and batch_count
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import (
        IntegrationCredential,
        SyncConfiguration,
    )

    config_uuid = uuid.UUID(config_id)

    logger.info(
        "dispatch_batch_sync: config_id=%s org_id=%s triggered_by=%s",
        config_id,
        org_id,
        triggered_by,
    )

    try:
        with get_postgres_session_sync() as session:
            config = (
                session.query(SyncConfiguration)
                .filter(
                    SyncConfiguration.id == config_uuid,
                    SyncConfiguration.org_id == org_id,
                )
                .one_or_none()
            )
            if config is None:
                raise ValueError(f"Sync configuration not found: {config_id}")

            provider = (config.provider or "").lower()
            sync_targets = list(config.sync_targets or [])
            sync_options = dict(config.sync_options or {})
            config_name = config.name

            credentials: dict[str, Any] = {}
            if config.credential_id:
                credential = (
                    session.query(IntegrationCredential)
                    .filter(
                        IntegrationCredential.id == config.credential_id,
                        IntegrationCredential.org_id == org_id,
                    )
                    .one_or_none()
                )
                if credential is None:
                    raise ValueError(f"Credential not found: {config.credential_id}")
                credentials = _decrypt_credential_sync(credential)
            else:
                credentials = _resolve_env_credentials(provider)

        try:
            repos = _discover_repos_for_config(config, credentials)
        except Exception as disc_exc:
            logger.warning(
                "Discovery failed for config %s, falling back to single dispatch: %s",
                config_id,
                disc_exc,
            )
            run_sync_config.apply_async(
                kwargs={
                    "config_id": config_id,
                    "org_id": org_id,
                    "triggered_by": triggered_by,
                },
                queue="sync",
            )
            return {
                "status": "fallback_single",
                "reason": str(disc_exc),
                "total_repos": 0,
                "batch_count": 0,
            }

        if not repos:
            logger.info(
                "dispatch_batch_sync: no repos discovered for config %s",
                config_id,
            )
            return {"status": "no_repos", "total_repos": 0, "batch_count": 0}

        batch_size = _get_batch_size(sync_options)
        child_tasks = []

        for repo_tuple in repos:
            per_repo_options = dict(sync_options)
            per_repo_options.pop("discover", None)
            per_repo_options.pop("batch_size", None)

            if provider == "github":
                owner, repo_name = repo_tuple[0], repo_tuple[1]
                per_repo_options["owner"] = owner
                per_repo_options["repo"] = repo_name
                per_repo_options.pop("search", None)
            elif provider == "gitlab":
                project_id = repo_tuple[0]
                per_repo_options["project_id"] = int(project_id)
                per_repo_options.pop("search", None)
                per_repo_options.pop("group", None)

            child_tasks.append(
                _run_sync_for_repo.s(
                    config_id=config_id,
                    org_id=org_id,
                    triggered_by=triggered_by,
                    provider=provider,
                    sync_targets=sync_targets,
                    sync_options_override=per_repo_options,
                    credentials=credentials,
                    config_name=config_name,
                )
            )

        batches = [
            child_tasks[i : i + batch_size]
            for i in range(0, len(child_tasks), batch_size)
        ]
        total_batches = len(batches)

        all_tasks = [task for batch in batches for task in batch]
        chord(
            group(all_tasks),
            _batch_sync_callback.s(
                provider=provider,
                sync_targets=sync_targets,
                org_id=org_id,
            ),
        )()

        logger.info(
            "dispatch_batch_sync: dispatched %d tasks in %d batches for config %s",
            len(all_tasks),
            total_batches,
            config_id,
        )

        return {
            "status": "dispatched",
            "total_repos": len(repos),
            "batch_count": total_batches,
        }

    except Exception as exc:
        logger.exception(
            "dispatch_batch_sync failed: config_id=%s error=%s",
            config_id,
            exc,
        )
        return {"status": "error", "error": str(exc)}


@celery_app.task(bind=True, max_retries=3, queue="sync")
def _run_sync_for_repo(
    self,
    config_id: str,
    org_id: str,
    triggered_by: str,
    provider: str,
    sync_targets: list[str],
    sync_options_override: dict[str, Any],
    credentials: dict[str, Any],
    config_name: str,
) -> dict:
    """Execute sync for a single repo with overridden sync_options.

    This is the per-repo worker task dispatched by dispatch_batch_sync.
    It bypasses the DB config lookup and uses the provided options directly.
    """
    from dev_health_ops.metrics.job_work_items import run_work_items_sync_job
    from dev_health_ops.processors.github import process_github_repo
    from dev_health_ops.processors.gitlab import process_gitlab_project
    from dev_health_ops.storage import resolve_db_type, run_with_store

    db_url = _get_db_url()
    db_type = resolve_db_type(db_url, None)
    started_at = datetime.now(timezone.utc)

    logger.info(
        "Batch child sync: config=%s provider=%s options=%s",
        config_id,
        provider,
        {k: v for k, v in sync_options_override.items() if k != "token"},
    )

    try:
        result_payload: dict[str, Any] = {
            "provider": provider,
            "config_id": config_id,
            "sync_targets": sync_targets,
            "triggered_by": triggered_by,
        }

        if provider == "github":
            owner = str(sync_options_override.get("owner", ""))
            repo_name = str(sync_options_override.get("repo", ""))
            token = str(credentials.get("token") or "")

            if not owner or not repo_name or not token:
                raise ValueError(
                    f"Missing GitHub owner/repo/token for batch sync: "
                    f"owner={owner}, repo={repo_name}"
                )

            merged_flags = _merge_sync_flags(sync_targets)

            async def _github_handler(store):
                await process_github_repo(
                    store=store,
                    owner=owner,
                    repo_name=repo_name,
                    token=token,
                    **merged_flags,
                )

            asyncio.run(run_with_store(db_url, db_type, _github_handler))
            result_payload.update({"owner": owner, "repo": repo_name})

        elif provider == "gitlab":
            project_id = sync_options_override.get("project_id")
            token = str(credentials.get("token") or "")
            gitlab_url = str(
                sync_options_override.get("gitlab_url", "https://gitlab.com")
            )

            if project_id is None or not token:
                raise ValueError(
                    f"Missing GitLab project_id/token for batch sync: "
                    f"project_id={project_id}"
                )

            merged_flags = _merge_sync_flags(sync_targets)

            async def _gitlab_handler(store):
                await process_gitlab_project(
                    store=store,
                    project_id=int(project_id),
                    token=token,
                    gitlab_url=gitlab_url,
                    **merged_flags,
                )

            asyncio.run(run_with_store(db_url, db_type, _gitlab_handler))
            result_payload.update(
                {"project_id": int(project_id), "gitlab_url": gitlab_url}
            )

        if "work-items" in sync_targets:
            token = str(credentials.get("token") or "")
            if token:
                _inject_provider_token(provider, token)
            backfill_days = int(sync_options_override.get("backfill_days", 1))
            run_work_items_sync_job(
                db_url=db_url,
                day=date.today(),
                backfill_days=backfill_days,
                provider=provider,
                repo_name=sync_options_override.get("repo"),
                search_pattern=sync_options_override.get("search"),
            )
            result_payload["work_items_synced"] = True

        duration = int((datetime.now(timezone.utc) - started_at).total_seconds())
        return {
            "status": "success",
            "duration_seconds": duration,
            "result": result_payload,
        }

    except Exception as exc:
        logger.exception(
            "Batch child sync failed: config=%s provider=%s error=%s",
            config_id,
            provider,
            exc,
        )
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True)
def dispatch_scheduled_syncs(self) -> dict:
    """Check active sync configs and dispatch any that are due."""
    from croniter import croniter

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import (
        ScheduledJob,
        SyncConfiguration,
    )

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            configs = (
                session.query(SyncConfiguration)
                .filter(SyncConfiguration.is_active.is_(True))
                .all()
            )

            for config in configs:
                job = (
                    session.query(ScheduledJob)
                    .filter(
                        ScheduledJob.sync_config_id == config.id,
                        ScheduledJob.org_id == config.org_id,
                    )
                    .one_or_none()
                )

                if job and job.is_running:
                    skipped += 1
                    continue

                cron_expr = job.schedule_cron if job else "0 * * * *"
                last_sync = config.last_sync_at or config.created_at
                cron = croniter(cron_expr, last_sync)
                next_run = cron.get_next(datetime)

                if next_run <= now:
                    if _is_batch_eligible(config):
                        dispatch_batch_sync.apply_async(
                            kwargs={
                                "config_id": str(config.id),
                                "org_id": config.org_id,
                                "triggered_by": "schedule",
                            },
                            queue="sync",
                        )
                    else:
                        run_sync_config.apply_async(
                            kwargs={
                                "config_id": str(config.id),
                                "org_id": config.org_id,
                                "triggered_by": "schedule",
                            },
                            queue="sync",
                        )
                    dispatched.append(str(config.id))
                else:
                    skipped += 1

    except Exception:
        logger.exception("dispatch_scheduled_syncs failed")

    logger.info(
        "Scheduled sync dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}


@celery_app.task(bind=True)
def dispatch_scheduled_metrics(self) -> dict:
    """Check ScheduledJob entries with job_type='metrics' and dispatch any that are due."""
    from croniter import croniter

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import (
        JobStatus,
        ScheduledJob,
    )

    now = datetime.now(timezone.utc)
    dispatched: list[str] = []
    skipped = 0

    try:
        with get_postgres_session_sync() as session:
            jobs = (
                session.query(ScheduledJob)
                .filter(
                    ScheduledJob.job_type == "metrics",
                    ScheduledJob.status == JobStatus.ACTIVE.value,
                )
                .all()
            )

            for job in jobs:
                if job.is_running:
                    skipped += 1
                    continue

                cron_expr = job.schedule_cron or "0 1 * * *"
                last_run = job.last_run_at or job.created_at
                cron = croniter(cron_expr, last_run)
                next_run = cron.get_next(datetime)

                if next_run <= now:
                    job_config = job.job_config or {}
                    run_daily_metrics.apply_async(
                        kwargs={
                            "db_url": job_config.get("db_url"),
                            "day": job_config.get("day"),
                            "backfill_days": job_config.get("backfill_days", 1),
                            "repo_id": job_config.get("repo_id"),
                            "repo_name": job_config.get("repo_name"),
                            "sink": job_config.get("sink", "auto"),
                            "provider": job_config.get("provider", "auto"),
                        },
                        queue="metrics",
                    )
                    dispatched.append(str(job.id))
                else:
                    skipped += 1

    except Exception:
        logger.exception("dispatch_scheduled_metrics failed")

    logger.info(
        "Scheduled metrics dispatch: dispatched=%d skipped=%d",
        len(dispatched),
        skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped}


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_daily_metrics(
    self,
    db_url: Optional[str] = None,
    day: Optional[str] = None,
    backfill_days: int = 1,
    repo_id: Optional[str] = None,
    repo_name: Optional[str] = None,
    sink: str = "auto",
    provider: str = "auto",
) -> dict:
    """
    Compute and persist daily metrics asynchronously.

    Args:
        db_url: Database connection string (defaults to DATABASE_URI env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        repo_id: Optional repository UUID to filter
        repo_name: Optional repository name to filter
        sink: Sink type (auto|clickhouse|mongo|sqlite|postgres|both)
        provider: Work item provider (auto|all|jira|github|gitlab|none)

    Returns:
        dict with job status and summary
    """
    from dev_health_ops.metrics.job_daily import run_daily_metrics_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else date.today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting daily metrics task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        repo_name or str(parsed_repo_id) or "all",
    )

    try:
        # Run the async job in a new event loop
        asyncio.run(
            run_daily_metrics_job(
                db_url=db_url,
                day=target_day,
                backfill_days=backfill_days,
                repo_id=parsed_repo_id,
                repo_name=repo_name,
                sink=sink,
                provider=provider,
            )
        )
        # Invalidate GraphQL cache after successful metrics update
        _invalidate_metrics_cache(target_day.isoformat())

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
        }
    except Exception as exc:
        logger.exception("Daily metrics task failed: %s", exc)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True, queue="default")
def dispatch_daily_metrics_partitioned(
    self,
    db_url: Optional[str] = None,
    day: Optional[str] = None,
    backfill_days: int = 1,
    batch_size: int = 5,
    sink: str = "auto",
    provider: str = "auto",
    org_id: str = "default",
) -> dict:
    """Orchestrator: discover repos, partition into batches, fan out via chord.

    For each day in the backfill range, dispatches a chord of
    ``run_daily_metrics_batch`` tasks with a ``run_daily_metrics_finalize_task``
    callback.

    Args:
        db_url: Database connection string (defaults to env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        batch_size: Number of repos per batch task
        sink: Sink type (auto|clickhouse)
        provider: Work item provider (auto|all|jira|github|gitlab|none)
        org_id: Organization scope

    Returns:
        dict with dispatched count, batch_count, and days
    """
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else date.today()

    logger.info(
        "dispatch_daily_metrics_partitioned: day=%s backfill=%d batch_size=%d",
        target_day.isoformat(),
        backfill_days,
        batch_size,
    )

    try:
        ch_sink = ClickHouseMetricsSink(db_url)
        rows = ch_sink.client.query("SELECT id FROM repos").result_rows
        repo_ids = [str(row[0]) for row in rows]
    except Exception as exc:
        logger.exception("Failed to discover repos for partitioned dispatch: %s", exc)
        return {"status": "error", "error": str(exc)}

    if not repo_ids:
        logger.warning("No repos found — nothing to dispatch")
        return {"status": "no_repos", "dispatched": 0}

    batches = [
        repo_ids[i : i + batch_size] for i in range(0, len(repo_ids), batch_size)
    ]

    days_list = [target_day - timedelta(days=i) for i in range(backfill_days)]

    total_dispatched = 0
    for d in days_list:
        day_iso = d.isoformat()
        chord(
            [
                run_daily_metrics_batch.s(
                    repo_ids=[str(rid) for rid in batch],
                    day=day_iso,
                    db_url=db_url,
                    sink=sink,
                    provider=provider,
                    org_id=org_id,
                )
                for batch in batches
            ],
            run_daily_metrics_finalize_task.s(
                day=day_iso,
                db_url=db_url,
                sink=sink,
                org_id=org_id,
            ),
        )()
        total_dispatched += len(batches)

    logger.info(
        "dispatch_daily_metrics_partitioned: dispatched %d batches across %d days",
        total_dispatched,
        len(days_list),
    )

    return {
        "status": "dispatched",
        "repo_count": len(repo_ids),
        "batch_count": len(batches),
        "days": len(days_list),
        "total_dispatched": total_dispatched,
    }


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_daily_metrics_batch(
    self,
    repo_ids: list[str],
    day: str,
    db_url: Optional[str] = None,
    sink: str = "auto",
    provider: str = "auto",
    org_id: str = "default",
) -> dict:
    """Worker: compute daily metrics for a batch of repos (single day).

    Processes each repo independently so one failure does not kill the batch.
    Uses checkpoint CRUD to track progress and skip already-completed repos.

    Args:
        repo_ids: List of repository UUID strings
        day: Target day as ISO string
        db_url: Database connection string
        sink: Sink type
        provider: Work item provider
        org_id: Organization scope

    Returns:
        dict with per-repo results
    """
    from datetime import time

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.checkpoints import (
        is_completed,
        mark_completed,
        mark_failed,
        mark_running,
    )
    from dev_health_ops.metrics.job_daily import run_daily_metrics_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day)
    checkpoint_day = datetime.combine(target_day, time.min, tzinfo=timezone.utc)

    results: dict[str, Any] = {}

    for repo_id in repo_ids:
        repo_id_uuid = uuid.UUID(repo_id)
        try:
            with get_postgres_session_sync() as session:
                if is_completed(
                    session, org_id, repo_id_uuid, "daily_batch", checkpoint_day
                ):
                    logger.info(
                        "Skipping already-completed repo %s for day %s",
                        repo_id,
                        day,
                    )
                    results[repo_id] = {
                        "status": "skipped",
                        "reason": "already_completed",
                    }
                    continue

                checkpoint = mark_running(
                    session,
                    org_id,
                    repo_id_uuid,
                    "daily_batch",
                    checkpoint_day,
                    self.request.id,
                )
                checkpoint_id = checkpoint.id

            asyncio.run(
                run_daily_metrics_job(
                    db_url=db_url,
                    day=target_day,
                    backfill_days=1,
                    repo_id=repo_id_uuid,
                    skip_finalize=True,
                    sink=sink,
                    provider=provider,
                    org_id=org_id,
                )
            )

            with get_postgres_session_sync() as session:
                mark_completed(session, checkpoint_id)

            results[repo_id] = {"status": "success"}

        except Exception as exc:
            logger.exception(
                "run_daily_metrics_batch failed for repo %s day %s: %s",
                repo_id,
                day,
                exc,
            )
            try:
                with get_postgres_session_sync() as session:
                    mark_failed(session, checkpoint_id, str(exc))
            except Exception as mark_exc:
                logger.error("Failed to mark checkpoint as failed: %s", mark_exc)

            results[repo_id] = {"status": "failed", "error": str(exc)}

    return {
        "day": day,
        "repo_count": len(repo_ids),
        "results": results,
    }


@celery_app.task(bind=True, max_retries=2, queue="metrics")
def run_daily_metrics_finalize_task(
    self,
    batch_results: list,
    day: str,
    db_url: Optional[str] = None,
    sink: str = "auto",
    org_id: str = "default",
) -> dict:
    """Chord callback: finalize daily metrics after all batches complete.

    Runs the finalize step (rollups, aggregations) and invalidates caches.
    Named with ``_task`` suffix to avoid collision with
    ``job_daily.run_daily_metrics_finalize``.

    Args:
        batch_results: List of results from header tasks (chord callback arg)
        day: Target day as ISO string
        db_url: Database connection string
        sink: Sink type
        org_id: Organization scope

    Returns:
        dict with finalize status
    """
    from datetime import time

    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.metrics.checkpoints import (
        mark_completed,
        mark_failed,
        mark_running,
    )
    from dev_health_ops.metrics.job_daily import (
        run_daily_metrics_finalize as _run_finalize,
    )

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day)
    checkpoint_day = datetime.combine(target_day, time.min, tzinfo=timezone.utc)

    logger.info(
        "run_daily_metrics_finalize_task: day=%s batches=%d",
        day,
        len(batch_results) if batch_results else 0,
    )

    checkpoint_id = None
    try:
        with get_postgres_session_sync() as session:
            checkpoint = mark_running(
                session, org_id, None, "daily_finalize", checkpoint_day, self.request.id
            )
            checkpoint_id = checkpoint.id

        asyncio.run(
            _run_finalize(
                db_url=db_url,
                day=target_day,
                org_id=org_id,
                sink=sink,
            )
        )

        _invalidate_metrics_cache(day, org_id)

        if checkpoint_id is not None:
            with get_postgres_session_sync() as session:
                mark_completed(session, checkpoint_id)

        return {
            "status": "success",
            "day": day,
            "batches_received": len(batch_results) if batch_results else 0,
        }

    except Exception as exc:
        logger.exception(
            "run_daily_metrics_finalize_task failed for day %s: %s", day, exc
        )

        if checkpoint_id is not None:
            try:
                with get_postgres_session_sync() as session:
                    mark_failed(session, checkpoint_id, str(exc))
            except Exception as mark_exc:
                logger.error(
                    "Failed to mark finalize checkpoint as failed: %s", mark_exc
                )

        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


def _invalidate_metrics_cache(day: str, org_id: str = "default") -> None:
    """Invalidate GraphQL caches after metrics update."""
    try:
        from dev_health_ops.api.graphql.cache_invalidation import (
            invalidate_on_metrics_update,
        )
        from dev_health_ops.api.services.cache import create_cache

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_metrics_update(cache, org_id, day)
        logger.info("Invalidated %d cache entries after metrics update", count)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)


def _invalidate_sync_cache(sync_type: str, org_id: str = "default") -> None:
    """Invalidate GraphQL caches after data sync."""
    try:
        from dev_health_ops.api.graphql.cache_invalidation import (
            invalidate_on_sync_complete,
        )
        from dev_health_ops.api.services.cache import create_cache

        cache = create_cache(ttl_seconds=300)
        count = invalidate_on_sync_complete(cache, org_id, sync_type)
        logger.info("Invalidated %d cache entries after %s sync", count, sync_type)
    except Exception as e:
        logger.warning("Cache invalidation failed (non-fatal): %s", e)


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_complexity_job(
    self,
    db_url: Optional[str] = None,
    day: Optional[str] = None,
    backfill_days: int = 1,
    repo_id: Optional[str] = None,
    search_pattern: Optional[str] = None,
    language_globs: Optional[list[str]] = None,
    exclude_globs: Optional[list[str]] = None,
    max_files: Optional[int] = None,
) -> dict:
    """
    Compute code complexity metrics from ClickHouse git_files/git_blame.

    Analyzes file contents already synced to the database — no local
    repository checkout required.

    Args:
        db_url: ClickHouse connection string (defaults to CLICKHOUSE_URI env)
        day: Target day as ISO string (defaults to today)
        backfill_days: Number of days to backfill
        repo_id: Optional repository UUID to filter
        search_pattern: Repo name glob pattern (e.g. "org/*")
        language_globs: Include language globs (e.g. ["*.py", "*.ts"])
        exclude_globs: Exclude path globs (e.g. ["*/tests/*"])
        max_files: Limit number of files scanned per repo

    Returns:
        dict with job status and summary
    """
    from dev_health_ops.metrics.job_complexity_db import run_complexity_db_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else date.today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting complexity analysis task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        search_pattern or str(parsed_repo_id) or "all",
    )

    try:
        result = run_complexity_db_job(
            repo_id=parsed_repo_id,
            db_url=db_url,
            date=target_day,
            backfill_days=backfill_days,
            language_globs=language_globs,
            max_files=max_files,
            search_pattern=search_pattern,
            exclude_globs=exclude_globs,
        )
        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
            "exit_code": result,
        }
    except Exception as exc:
        logger.exception("Complexity analysis task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True, max_retries=3, queue="sync")
def run_work_items_sync(
    self,
    db_url: Optional[str] = None,
    provider: str = "auto",
    since_days: int = 30,
) -> dict:
    """
    Sync work items from external providers.

    Args:
        db_url: Database connection string
        provider: Provider to sync from (auto|jira|github|gitlab|all)
        since_days: Number of days to look back

    Returns:
        dict with sync status and counts
    """
    from datetime import datetime, timedelta, timezone

    db_url = db_url or _get_db_url()
    since = datetime.now(timezone.utc) - timedelta(days=since_days)

    logger.info(
        "Starting work items sync task: provider=%s since=%s",
        provider,
        since.isoformat(),
    )

    try:
        from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

        # run_work_items_sync_job is synchronous
        run_work_items_sync_job(
            db_url=db_url,
            day=since.date(),
            backfill_days=since_days,
            provider=provider,
        )

        # Invalidate GraphQL cache after successful sync
        _invalidate_sync_cache(provider)

        return {
            "status": "success",
            "provider": provider,
            "since_days": since_days,
        }
    except Exception as exc:
        logger.exception("Work items sync task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True, max_retries=2, queue="sync")
def sync_team_drift(self, org_id: str = "default") -> dict:

    from dev_health_ops.api.services.settings import (
        IntegrationCredentialsService,
        TeamDiscoveryService,
        TeamDriftSyncService,
    )
    from dev_health_ops.db import get_postgres_session

    async def _run():
        results = []
        async with get_postgres_session() as session:
            creds_svc = IntegrationCredentialsService(session, org_id)
            discovery_svc = TeamDiscoveryService(session, org_id)
            drift_svc = TeamDriftSyncService(session, org_id)

            for provider in ("github", "gitlab", "jira"):
                credential = await creds_svc.get(provider, "default")
                if credential is None:
                    continue
                decrypted = await creds_svc.get_decrypted_credentials(
                    provider, "default"
                )
                if decrypted is None:
                    continue

                config = credential.config or {}
                try:
                    if provider == "github":
                        token = decrypted.get("token")
                        org_name = config.get("org")
                        if not token or not org_name:
                            continue
                        teams = await discovery_svc.discover_github(
                            token=token,
                            org_name=org_name,
                        )
                    elif provider == "gitlab":
                        token = decrypted.get("token")
                        group_path = config.get("group")
                        url = config.get("url", "https://gitlab.com")
                        if not token or not group_path:
                            continue
                        teams = await discovery_svc.discover_gitlab(
                            token=token,
                            group_path=group_path,
                            url=url,
                        )
                    else:
                        email = decrypted.get("email")
                        api_token = decrypted.get("api_token") or decrypted.get("token")
                        jira_url = config.get("url") or decrypted.get("url")
                        if not email or not api_token or not jira_url:
                            continue
                        teams = await discovery_svc.discover_jira(
                            email=email,
                            api_token=api_token,
                            url=jira_url,
                        )

                    result = await drift_svc.run_drift_sync(provider, teams)
                    results.append(result)
                except Exception as exc:
                    logger.warning(
                        "Team drift sync failed for provider %s: %s",
                        provider,
                        exc,
                    )
                    results.append({"provider": provider, "error": str(exc)})

            await session.commit()
        return {"status": "success", "results": results}

    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.exception("sync_team_drift failed: %s", exc)
        raise self.retry(exc=exc, countdown=300)


@celery_app.task(bind=True, max_retries=2, queue="sync")
def reconcile_team_members(self, org_id: str = "default") -> dict:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import IdentityMapping

    team_members: dict[str, set[str]] = {}
    with get_postgres_session_sync() as session:
        mappings = (
            session.query(IdentityMapping)
            .filter(IdentityMapping.org_id == org_id)
            .all()
        )

        for mapping in mappings:
            canonical_id = str(mapping.canonical_id)
            for team_id in mapping.team_ids or []:
                if not team_id:
                    continue
                team_members.setdefault(str(team_id), set()).add(canonical_id)

    async def _run() -> dict:
        from dev_health_ops.models.teams import Team
        from dev_health_ops.storage.clickhouse import ClickHouseStore

        db_url = _get_db_url()
        if not db_url:
            raise ValueError(
                "Missing CLICKHOUSE_URI or DATABASE_URI for reconciliation"
            )

        async with ClickHouseStore(db_url) as store:
            teams = await store.get_all_teams()
            now = datetime.now(timezone.utc)
            updated_teams = [
                Team(
                    id=team.id,
                    team_uuid=uuid.UUID(str(team.team_uuid)),
                    name=team.name,
                    description=team.description,
                    members=sorted(team_members.get(team.id, set())),
                    updated_at=now,
                )
                for team in teams
            ]
            if updated_teams:
                await store.insert_teams(updated_teams)

            return {
                "status": "success",
                "teams_scanned": len(teams),
                "teams_updated": len(updated_teams),
                "mapped_teams": len(team_members),
            }

    try:
        return asyncio.run(_run())
    except Exception as exc:
        logger.exception("reconcile_team_members failed: %s", exc)
        raise self.retry(exc=exc, countdown=300)


@celery_app.task(bind=True, max_retries=2, queue="metrics")
def sync_teams_to_analytics(self, org_id: str = "default") -> dict:
    from dev_health_ops.providers.team_bridge import bridge_teams_to_clickhouse

    try:
        count = bridge_teams_to_clickhouse(org_id=org_id)
        return {"status": "success", "teams_synced": count}
    except Exception as exc:
        logger.exception("sync_teams_to_analytics failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_dora_metrics(
    self,
    db_url: Optional[str] = None,
    day: Optional[str] = None,
    backfill_days: int = 1,
    repo_id: Optional[str] = None,
    repo_name: Optional[str] = None,
    sink: str = "auto",
    metrics: Optional[str] = None,
    interval: str = "daily",
) -> dict:
    """
    Compute and persist DORA metrics asynchronously.

    Args:
            db_url: Database connection string (defaults to DATABASE_URI env)
            day: Target day as ISO string (defaults to today)
            backfill_days: Number of days to backfill
            repo_id: Optional repository UUID to filter
            repo_name: Optional repository name to filter
            sink: Sink type (auto|clickhouse|mongo|sqlite|postgres|both)
            metrics: Specific metrics to compute (optional)
            interval: Metric interval (daily|weekly|monthly)

    Returns:
            dict with job status and summary
    """
    from dev_health_ops.metrics.job_dora import run_dora_metrics_job

    db_url = db_url or _get_db_url()
    target_day = date.fromisoformat(day) if day else date.today()
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting DORA metrics task: day=%s backfill=%d repo=%s",
        target_day.isoformat(),
        backfill_days,
        repo_name or str(parsed_repo_id) or "all",
    )

    try:
        run_dora_metrics_job(
            db_url=db_url,
            day=target_day,
            backfill_days=backfill_days,
            repo_id=parsed_repo_id,
            repo_name=repo_name,
            sink=sink,
            metrics=metrics,
            interval=interval,
        )

        return {
            "status": "success",
            "day": target_day.isoformat(),
            "backfill_days": backfill_days,
        }
    except Exception as exc:
        logger.exception("DORA metrics task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True, max_retries=3, queue="metrics")
def run_work_graph_build(
    self,
    db_url: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    repo_id: Optional[str] = None,
    heuristic_window: int = 7,
    heuristic_confidence: float = 0.3,
) -> dict:
    """Build work graph from evidence.

    Args:
        db_url: Database connection string
        from_date: Start date (ISO format, defaults to 30 days ago)
        to_date: End date (ISO format, defaults to now)
        repo_id: Optional repository UUID to filter
        heuristic_window: Days window for heuristics
        heuristic_confidence: Confidence threshold for heuristics

    Returns:
        dict with build status and edge count
    """
    from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder

    db_url = db_url or _get_db_url()
    now = datetime.now(timezone.utc)

    # Parse dates
    if to_date:
        parsed_to = datetime.fromisoformat(to_date)
    else:
        parsed_to = now

    if from_date:
        parsed_from = datetime.fromisoformat(from_date)
    else:
        parsed_from = parsed_to - timedelta(days=30)

    # Parse repo_id
    parsed_repo_id = uuid.UUID(repo_id) if repo_id else None

    logger.info(
        "Starting work graph build task: from=%s to=%s repo=%s",
        parsed_from.isoformat(),
        parsed_to.isoformat(),
        repo_id or "all",
    )

    try:
        config = BuildConfig(
            dsn=db_url,
            from_date=parsed_from,
            to_date=parsed_to,
            repo_id=parsed_repo_id,
            heuristic_days_window=heuristic_window,
            heuristic_confidence=heuristic_confidence,
        )
        builder = WorkGraphBuilder(config)
        try:
            result = builder.build()
            return {"status": "success", "edges": result}
        finally:
            builder.close()
    except Exception as exc:
        logger.exception("Work graph build task failed: %s", exc)
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))


@celery_app.task(bind=True, max_retries=2, queue="metrics")
def run_investment_materialize(
    self,
    db_url: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    window_days: int = 30,
    repo_ids: Optional[list[str]] = None,
    team_ids: Optional[list[str]] = None,
    llm_provider: str = "auto",
    llm_model: Optional[str] = None,
    force: bool = False,
) -> dict:
    """Materialize investment distributions from work graph.

    Args:
        db_url: Database connection string
        from_date: Start date (ISO format)
        to_date: End date (ISO format)
        window_days: Days window for default date range
        repo_ids: Optional list of repository IDs to filter
        team_ids: Optional list of team IDs to filter
        llm_provider: LLM provider (auto|openai|anthropic)
        llm_model: Optional specific LLM model
        force: Force recomputation even if cached

    Returns:
        dict with materialization status and stats
    """
    from dev_health_ops.work_graph.investment.materialize import (
        MaterializeConfig,
        materialize_investments,
    )
    from datetime import time as dt_time

    db_url = db_url or _get_db_url()
    now = datetime.now(timezone.utc)

    # Parse to_date
    if to_date:
        parsed_to = datetime.combine(
            date.fromisoformat(to_date) + timedelta(days=1),
            dt_time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed_to = now

    # Parse from_date
    if from_date:
        parsed_from = datetime.combine(
            date.fromisoformat(from_date),
            dt_time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed_from = parsed_to - timedelta(days=window_days)

    logger.info(
        "Starting investment materialize task: from=%s to=%s repos=%s teams=%s",
        parsed_from.isoformat(),
        parsed_to.isoformat(),
        repo_ids or "all",
        team_ids or "all",
    )

    try:
        config = MaterializeConfig(
            dsn=db_url,
            from_ts=parsed_from,
            to_ts=parsed_to,
            repo_ids=repo_ids,
            llm_provider=llm_provider,
            persist_evidence_snippets=False,
            llm_model=llm_model,
            team_ids=team_ids,
            force=force,
        )
        stats = asyncio.run(materialize_investments(config))
        return {"status": "success", "stats": stats}
    except Exception as exc:
        logger.exception("Investment materialize task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


@celery_app.task(bind=True, max_retries=2, queue="metrics")
def run_capacity_forecast_job(
    self,
    db_url: Optional[str] = None,
    team_id: Optional[str] = None,
    work_scope_id: Optional[str] = None,
    target_items: Optional[int] = None,
    target_date: Optional[str] = None,
    history_days: int = 90,
    simulations: int = 10000,
    all_teams: bool = False,
) -> dict:
    """
    Run capacity forecasting job asynchronously.

    Args:
        db_url: Database connection string (defaults to DATABASE_URI env)
        team_id: Optional team UUID to forecast
        work_scope_id: Optional work scope UUID to forecast
        target_items: Optional target item count for forecast
        target_date: Optional target date as ISO string
        history_days: Number of historical days to analyze (default 90)
        simulations: Number of Monte Carlo simulations (default 10000)
        all_teams: If True, forecast for all teams

    Returns:
        dict with job status and forecast count
    """
    from dev_health_ops.metrics.job_capacity import run_capacity_forecast

    db_url = db_url or _get_db_url()
    parsed_target_date = date.fromisoformat(target_date) if target_date else None

    logger.info(
        "Starting capacity forecast task: team=%s scope=%s all_teams=%s",
        team_id,
        work_scope_id,
        all_teams,
    )

    try:
        results = asyncio.run(
            run_capacity_forecast(
                db_url=db_url,
                team_id=team_id,
                work_scope_id=work_scope_id,
                target_items=target_items,
                target_date=parsed_target_date,
                history_days=history_days,
                simulations=simulations,
                all_teams=all_teams,
                persist=True,
            )
        )

        return {"status": "success", "forecasts": len(results)}
    except Exception as exc:
        logger.exception("Capacity forecast task failed: %s", exc)
        raise self.retry(exc=exc, countdown=120 * (2**self.request.retries))


@celery_app.task(bind=True)
def health_check(self) -> dict:
    """Simple health check task to verify worker is running."""
    return {
        "status": "healthy",
        "worker_id": self.request.id,
    }


@celery_app.task(bind=True, max_retries=3, queue="webhooks")
def process_webhook_event(
    self,
    provider: str,
    event_type: str,
    delivery_id: Optional[str] = None,
    payload: Optional[dict] = None,
    org_id: Optional[str] = None,
    repo_name: Optional[str] = None,
) -> dict:
    """
    Process a webhook event asynchronously.

    This task handles the actual processing of webhook events after
    they've been received and validated by the webhook endpoints.

    Args:
        provider: Source provider (github, gitlab, jira)
        event_type: Canonical event type
        delivery_id: Provider's delivery ID for idempotency
        payload: Raw webhook payload
        org_id: Organization scope
        repo_name: Repository name (if applicable)

    Returns:
        dict with processing status and summary
    """
    from datetime import datetime, timezone

    logger.info(
        "Processing webhook event: provider=%s type=%s delivery=%s repo=%s",
        provider,
        event_type,
        delivery_id,
        repo_name,
    )

    try:
        if delivery_id:
            if _is_duplicate_delivery(provider, delivery_id):
                logger.info(
                    "Skipping duplicate webhook delivery: %s/%s",
                    provider,
                    delivery_id,
                )
                return {
                    "status": "skipped",
                    "reason": "duplicate_delivery",
                    "delivery_id": delivery_id,
                }
            _record_delivery(provider, delivery_id)

        if provider == "github":
            result = _process_github_event(event_type, payload, org_id, repo_name)
        elif provider == "gitlab":
            result = _process_gitlab_event(event_type, payload, org_id, repo_name)
        elif provider == "jira":
            result = _process_jira_event(event_type, payload, org_id)
        else:
            logger.warning("Unknown webhook provider: %s", provider)
            return {"status": "error", "reason": f"unknown_provider: {provider}"}

        _invalidate_sync_cache(provider, org_id or "default")

        return {
            "status": "success",
            "provider": provider,
            "event_type": event_type,
            "delivery_id": delivery_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            **result,
        }

    except Exception as exc:
        logger.exception(
            "Webhook processing failed: provider=%s type=%s error=%s",
            provider,
            event_type,
            exc,
        )
        raise self.retry(exc=exc, countdown=30 * (2**self.request.retries))


def _is_duplicate_delivery(provider: str, delivery_id: str) -> bool:
    """Check if we've already processed this delivery.

    Uses Redis for persistence across workers if available, otherwise
    falls back to a simple in-memory check.
    """
    cache_key = f"webhook_delivery:{provider}:{delivery_id}"
    try:
        from dev_health_ops.api.services.cache import create_cache

        # Use a short TTL for idempotency (e.g., 24 hours)
        cache = create_cache(ttl_seconds=86400)
        return cache.get(cache_key) is not None
    except Exception as e:
        logger.warning("Idempotency check failed (falling back to False): %s", e)
        return False


def _record_delivery(provider: str, delivery_id: str) -> None:
    """Record that we've processed this delivery.

    This prevents duplicate processing if the provider retries.
    """
    cache_key = f"webhook_delivery:{provider}:{delivery_id}"
    try:
        from dev_health_ops.api.services.cache import create_cache

        cache = create_cache(ttl_seconds=86400)
        cache.set(cache_key, "processed")
    except Exception as e:
        logger.warning("Failed to record webhook delivery: %s", e)


def _process_github_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
    repo_name: str | None,
) -> dict:
    """Process a GitHub webhook event."""
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    # Import specialized sync processors
    from dev_health_ops.processors.github import process_github_repo
    from dev_health_ops.storage import run_with_store, resolve_db_type

    db_url = _get_db_url()
    db_type = resolve_db_type(db_url, None)

    # Repository owner and name from payload if not provided
    repo_payload = payload.get("repository", {})
    owner = repo_payload.get("owner", {}).get("login")
    repo = repo_payload.get("name")

    if not (owner and repo):
        # Fallback to provided repo_name if possible
        if repo_name and "/" in repo_name:
            owner, repo = repo_name.split("/", 1)
        else:
            return {"processed": False, "reason": "missing_repo_info"}

    token = os.getenv("GITHUB_TOKEN") or ""
    if not token:
        return {"processed": False, "reason": "missing_github_token"}

    async def _sync_handler(store):
        if event_type == "push":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=True,
                sync_prs=False,
                sync_cicd=False,
            )
        elif event_type == "pull_request":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=True,
                sync_cicd=False,
            )
        elif event_type in ("issue_created", "issue_updated", "issue_closed"):
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=False,
                sync_incidents=True,
            )
        elif event_type == "deployment":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=False,
                sync_deployments=True,
            )
        elif event_type == "workflow_run":
            await process_github_repo(
                store=store,
                owner=owner,
                repo_name=repo,
                token=token,
                sync_git=False,
                sync_prs=False,
                sync_cicd=True,
            )

    # Execute sync
    try:
        asyncio.run(run_with_store(db_url, db_type, _sync_handler))
        return {"processed": True, "repo": f"{owner}/{repo}", "event": event_type}
    except Exception as e:
        logger.error("Failed to process GitHub webhook %s: %s", event_type, e)
        return {"processed": False, "error": str(e)}


def _process_gitlab_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
    repo_name: str | None,
) -> dict:
    """Process a GitLab webhook event."""
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    from dev_health_ops.processors.gitlab import process_gitlab_project
    from dev_health_ops.storage import run_with_store, resolve_db_type

    db_url = _get_db_url()
    db_type = resolve_db_type(db_url, None)

    # Project ID from payload
    project_payload = payload.get("project", {})
    project_id = project_payload.get("id")

    if not project_id:
        return {"processed": False, "reason": "missing_project_id"}

    token = os.getenv("GITLAB_TOKEN") or ""
    gitlab_url = os.getenv("GITLAB_URL", "https://gitlab.com")
    if not token:
        return {"processed": False, "reason": "missing_gitlab_token"}

    async def _sync_handler(store):
        if event_type == "push":
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=True,
                sync_prs=False,
                sync_cicd=False,
            )
        elif event_type == "merge_request":
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=False,
                sync_prs=True,
                sync_cicd=False,
            )
        elif event_type in ("issue_created", "issue_updated", "issue_closed"):
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=False,
                sync_prs=False,
                sync_incidents=True,
            )
        elif event_type == "pipeline":
            await process_gitlab_project(
                store=store,
                project_id=project_id,
                token=token,
                gitlab_url=gitlab_url,
                sync_git=False,
                sync_prs=False,
                sync_cicd=True,
            )

    try:
        asyncio.run(run_with_store(db_url, db_type, _sync_handler))
        return {"processed": True, "project_id": project_id, "event": event_type}
    except Exception as e:
        logger.error("Failed to process GitLab webhook %s: %s", event_type, e)
        return {"processed": False, "error": str(e)}


def _process_jira_event(
    event_type: str,
    payload: dict | None,
    org_id: str | None,
) -> dict:
    """Process a Jira webhook event."""
    if not payload:
        return {"processed": False, "reason": "empty_payload"}

    from dev_health_ops.metrics.job_work_items import run_work_items_sync_job

    try:
        # Jira sync doesn't have a single-issue sync yet, so we trigger a broad sync
        # for the provider. In a production system, we'd optimize this to sync only
        # the specific issue key.
        run_work_items_sync_job(
            db_url=_get_db_url(),
            day=date.today(),
            backfill_days=1,
            provider="jira",
        )
        return {"processed": True, "event": event_type}
    except Exception as e:
        logger.error("Failed to process Jira webhook %s: %s", event_type, e)
        return {"processed": False, "error": str(e)}
