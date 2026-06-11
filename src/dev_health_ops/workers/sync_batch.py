from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from celery import chord, group

from dev_health_ops.credentials.resolver import github_credentials_from_mapping
from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
from dev_health_ops.workers.sync_runtime import (
    _dispatch_post_sync_tasks,
    _TerminalSyncError,
    run_sync_config,
)
from dev_health_ops.workers.task_utils import (
    _decrypt_credential_sync,
    _extract_provider_token,
    _get_db_url,
    _inject_provider_token,
    _merge_sync_flags,
    _normalize_sync_targets,
    _resolve_env_credentials,
)

logger = logging.getLogger(__name__)


def _set_run_duration(run_record, completed_at: datetime) -> None:
    """Compute duration_seconds from started_at when available."""
    started = getattr(run_record, "started_at", None)
    if started is None:
        return
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    setattr(
        run_record,
        "duration_seconds",
        max(0, int((completed_at - started).total_seconds())),
    )


def _update_config_sync_status(
    session,
    config_id: str,
    org_id: str,
    *,
    completed_at: datetime,
    success: bool,
    error: str | None = None,
    stats: dict[str, Any] | None = None,
) -> None:
    """Record the terminal sync outcome on the SyncConfiguration."""
    from dev_health_ops.models.settings import SyncConfiguration

    config_record = (
        session.query(SyncConfiguration)
        .filter(
            SyncConfiguration.id == uuid.UUID(config_id),
            SyncConfiguration.org_id == org_id,
        )
        .one_or_none()
    )
    if config_record is None:
        return
    setattr(config_record, "last_sync_at", completed_at)
    setattr(config_record, "last_sync_success", success)
    setattr(config_record, "last_sync_error", error)
    if stats is not None:
        setattr(config_record, "last_sync_stats", stats)


def _mark_batch_run_running(run_id: str) -> None:
    """Transition the pending JobRun to RUNNING when batch dispatch starts."""
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    try:
        with get_postgres_session_sync() as session:
            run_record = (
                session.query(JobRun)
                .filter(JobRun.id == uuid.UUID(run_id))
                .one_or_none()
            )
            if run_record is not None:
                setattr(run_record, "status", JobRunStatus.RUNNING.value)
                setattr(run_record, "started_at", datetime.now(timezone.utc))
                session.flush()
    except Exception as exc:
        logger.error("Failed to mark batch run %s running: %s", run_id, exc)


def _mark_batch_run_complete(
    run_id: str | None,
    results: list,
    *,
    config_id: str | None = None,
    org_id: str | None = None,
) -> None:
    """Update the parent JobRun to SUCCESS after all batch children complete.

    Also stamps last_sync_* on the SyncConfiguration when config_id is given,
    so the config no longer shows "Never Synced" after a batch run.
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    try:
        with get_postgres_session_sync() as session:
            completed_at = datetime.now(timezone.utc)
            stats = {"child_results": len(results) if results else 0}
            if run_id is not None:
                run_record = (
                    session.query(JobRun)
                    .filter(JobRun.id == uuid.UUID(run_id))
                    .one_or_none()
                )
                if run_record is not None:
                    setattr(run_record, "status", JobRunStatus.SUCCESS.value)
                    setattr(run_record, "completed_at", completed_at)
                    setattr(run_record, "result", stats)
                    setattr(run_record, "error", None)
                    _set_run_duration(run_record, completed_at)
            if config_id is not None and org_id is not None:
                _update_config_sync_status(
                    session,
                    config_id,
                    org_id,
                    completed_at=completed_at,
                    success=True,
                    stats=stats,
                )
            session.flush()
    except Exception as exc:
        logger.error("Failed to mark batch run %s complete: %s", run_id, exc)


def _mark_batch_run_failed(
    run_id: str | None,
    error: str,
    *,
    config_id: str | None = None,
    org_id: str | None = None,
) -> None:
    """Update the parent JobRun to FAILED so it never sticks in PENDING/RUNNING."""
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    try:
        with get_postgres_session_sync() as session:
            completed_at = datetime.now(timezone.utc)
            if run_id is not None:
                run_record = (
                    session.query(JobRun)
                    .filter(JobRun.id == uuid.UUID(run_id))
                    .one_or_none()
                )
                if run_record is not None:
                    setattr(run_record, "status", JobRunStatus.FAILED.value)
                    setattr(run_record, "completed_at", completed_at)
                    setattr(run_record, "error", error)
                    _set_run_duration(run_record, completed_at)
            if config_id is not None and org_id is not None:
                _update_config_sync_status(
                    session,
                    config_id,
                    org_id,
                    completed_at=completed_at,
                    success=False,
                    error=error,
                )
            session.flush()
    except Exception as exc:
        logger.error("Failed to mark batch run %s failed: %s", run_id, exc)


def _is_batch_eligible(config) -> bool:
    """Check if a SyncConfiguration should be dispatched as a batch.

    A config is batch-eligible when:
    - Provider is github or gitlab
    - sync_options contains a 'search' key with a wildcard pattern (e.g. "org/*")
    - OR sync_options names an org/group without a concrete repo/project
    - OR sync_options contains 'discover: true'
    """
    provider = (config.provider or "").lower()
    if provider not in ("github", "gitlab"):
        return False

    sync_options = dict(config.sync_options or {})

    if sync_options.get("discover") is True:
        return True

    search = sync_options.get("search")
    if isinstance(search, str):
        if "*" in search or "?" in search:
            return True
        if search.strip() and "/" not in search:
            return True

    if provider == "github":
        owner = sync_options.get("owner")
        repo = sync_options.get("repo")
        if owner and not repo:
            return True

    if provider == "gitlab":
        group = sync_options.get("group")
        project = sync_options.get("project_id") or sync_options.get("repo")
        if group and not project:
            return True

    return False


def _get_batch_size(sync_options: dict[str, Any]) -> int:
    """Get batch size from sync_options or environment, default 5."""
    size = sync_options.get("batch_size")
    if size is not None:
        return int(size)
    env_size = os.getenv("SYNC_BATCH_SIZE")
    if env_size is not None:
        return int(env_size)
    return 5


@celery_app.task(
    bind=True, queue="sync", name="dev_health_ops.workers.tasks._batch_sync_callback"
)
def _batch_sync_callback(
    self,
    results: list,
    *,
    provider: str,
    sync_targets: list[str],
    org_id: str,
    run_id: str | None = None,
    config_id: str | None = None,
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
    if run_id is not None or config_id is not None:
        _mark_batch_run_complete(run_id, results, config_id=config_id, org_id=org_id)
    return {
        "status": "post_sync_dispatched",
        "child_results": len(results) if results else 0,
    }


@celery_app.task(
    bind=True,
    queue="sync",
    rate_limit="5/m",
    name="dev_health_ops.workers.tasks.dispatch_batch_sync",
)
def dispatch_batch_sync(
    self,
    config_id: str,
    org_id: str,
    triggered_by: str = "schedule",
    pending_run_id: str | None = None,
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
            if not organization_exists_sync(session, org_id):
                logger.info(
                    "Skipping batch sync dispatch for deleted org_id=%s", org_id
                )
                return {"status": "skipped", "reason": "organization_not_found"}

            config = (
                session.query(SyncConfiguration)
                .filter(
                    SyncConfiguration.id == config_uuid,
                    SyncConfiguration.org_id == org_id,
                )
                .one_or_none()
            )
            if config is None:
                raise _TerminalSyncError(f"Sync configuration not found: {config_id}")

            provider = (config.provider or "").lower()
            sync_targets = _normalize_sync_targets(
                provider, list(config.sync_targets or [])
            )
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
                    raise _TerminalSyncError(
                        f"Credential not found: {config.credential_id}"
                    )
                credentials = _decrypt_credential_sync(credential)
            else:
                credentials = _resolve_env_credentials(provider)

        # The pending JobRun was persisted at trigger time (CHAOS-2255); mark it
        # RUNNING so the UI does not report the sync as "Idle" (CHAOS-2267).
        if pending_run_id is not None:
            _mark_batch_run_running(pending_run_id)

        from dev_health_ops.discovery.repos import discover_repos_for_config

        try:
            repos = discover_repos_for_config(config, credentials)
        except Exception as disc_exc:
            logger.warning(
                "Discovery failed for config %s, falling back to single dispatch: %s",
                config_id,
                disc_exc,
            )
            getattr(run_sync_config, "apply_async")(
                kwargs={
                    "config_id": config_id,
                    "org_id": org_id,
                    "triggered_by": triggered_by,
                    "pending_run_id": pending_run_id,
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
            # Terminal: the chord (and its callback) is never dispatched, so
            # resolve the run here. Discovery succeeded with zero matches, which
            # mirrors run_sync_config's semantics of SUCCESS on an empty sync.
            _mark_batch_run_complete(
                pending_run_id, [], config_id=config_id, org_id=org_id
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
                if "work-items" in sync_targets:
                    per_repo_options["search"] = f"{owner}/{repo_name}"
                else:
                    per_repo_options.pop("search", None)
            elif provider == "gitlab":
                project_id = repo_tuple[0]
                per_repo_options["project_id"] = int(project_id)
                per_repo_options.pop("search", None)
                per_repo_options.pop("group", None)

            child_tasks.append(
                getattr(_run_sync_for_repo, "s")(
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
            getattr(_batch_sync_callback, "s")(
                provider=provider,
                sync_targets=sync_targets,
                org_id=org_id,
                run_id=pending_run_id,
                config_id=config_id,
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
        # Terminal: nothing downstream will resolve the run, so fail it here
        # instead of leaving it stuck PENDING/RUNNING (CHAOS-2267).
        _mark_batch_run_failed(
            pending_run_id, str(exc), config_id=config_id, org_id=org_id
        )
        return {"status": "error", "error": str(exc)}


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="sync",
    name="dev_health_ops.workers.tasks._run_sync_for_repo",
)
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
        sync_targets = _normalize_sync_targets(provider, list(sync_targets or []))
        result_payload: dict[str, Any] = {
            "provider": provider,
            "config_id": config_id,
            "sync_targets": sync_targets,
            "triggered_by": triggered_by,
        }

        code_sync_targets = [
            target for target in sync_targets if target != "work-items"
        ]

        if provider == "github" and code_sync_targets:
            owner = str(sync_options_override.get("owner", ""))
            repo_name = str(sync_options_override.get("repo", ""))
            github_credentials = github_credentials_from_mapping(credentials)

            if not owner or not repo_name or github_credentials is None:
                raise ValueError(
                    f"Missing GitHub owner/repo/credentials for batch sync: "
                    f"owner={owner}, repo={repo_name}"
                )

            merged_flags = _merge_sync_flags(code_sync_targets)

            async def _github_handler(store):
                await process_github_repo(
                    store=store,
                    owner=owner,
                    repo_name=repo_name,
                    token=github_credentials,
                    blame_only=merged_flags.get("blame_only", False),
                    sync_git=merged_flags.get("sync_git", False),
                    sync_prs=merged_flags.get("sync_prs", False),
                    sync_cicd=merged_flags.get("sync_cicd", False),
                    sync_deployments=merged_flags.get("sync_deployments", False),
                    sync_incidents=merged_flags.get("sync_incidents", False),
                )

            run_async(run_with_store(db_url, db_type, _github_handler, org_id=org_id))
            result_payload.update({"owner": owner, "repo": repo_name})

        elif provider == "gitlab" and code_sync_targets:
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

            gitlab_targets = [
                target for target in code_sync_targets if target != "work-items"
            ]
            merged_flags = _merge_sync_flags(gitlab_targets)

            async def _gitlab_handler(store):
                await process_gitlab_project(
                    store=store,
                    project_id=int(project_id),
                    token=token,
                    gitlab_url=gitlab_url,
                    blame_only=merged_flags.get("blame_only", False),
                    sync_git=merged_flags.get("sync_git", False),
                    sync_prs=merged_flags.get("sync_prs", False),
                    sync_cicd=merged_flags.get("sync_cicd", False),
                    sync_deployments=merged_flags.get("sync_deployments", False),
                    sync_incidents=merged_flags.get("sync_incidents", False),
                )

            run_async(run_with_store(db_url, db_type, _gitlab_handler, org_id=org_id))
            result_payload.update(
                {"project_id": int(project_id), "gitlab_url": gitlab_url}
            )

        elif provider not in {"github", "gitlab"} and "work-items" not in sync_targets:
            raise ValueError(
                f"Unsupported batch sync provider/targets: provider={provider} targets={sync_targets}"
            )

        if "work-items" in sync_targets:
            token = _extract_provider_token(provider, credentials)
            if token:
                _inject_provider_token(provider, token)
            backfill_days = int(sync_options_override.get("backfill_days", 1))
            chunk_index = int(sync_options_override.get("chunk_index", 0))
            chunk_since = sync_options_override.get("since")
            chunk_before = sync_options_override.get("before")
            logger.info(
                "backfill_chunk_start",
                extra={
                    "chunk": chunk_index,
                    "job_id": sync_options_override.get("backfill_job_id"),
                    "since": chunk_since,
                    "before": chunk_before,
                    "provider": provider,
                },
            )
            started_backfill = datetime.now(timezone.utc)
            run_work_items_sync_job(
                db_url=db_url,
                day=utc_today(),
                backfill_days=backfill_days,
                provider=provider,
                repo_name=sync_options_override.get("repo"),
                search_pattern=sync_options_override.get("search"),
                org_id=org_id,
            )
            duration_ms = int(
                (datetime.now(timezone.utc) - started_backfill).total_seconds() * 1000
            )
            logger.info(
                "backfill_chunk_complete",
                extra={
                    "chunk": chunk_index,
                    "job_id": sync_options_override.get("backfill_job_id"),
                    "since": chunk_since,
                    "before": chunk_before,
                    "duration_ms": duration_ms,
                    "provider": provider,
                },
            )
            result_payload["work_items_synced"] = True

        duration = int((datetime.now(timezone.utc) - started_at).total_seconds())
        return {
            "status": "success",
            "duration_seconds": duration,
            "result": result_payload,
        }

    except Exception as exc:
        logger.info(
            "backfill_chunk_failed",
            extra={
                "chunk": sync_options_override.get("chunk_index", 0),
                "job_id": sync_options_override.get("backfill_job_id"),
                "provider": provider,
                "error": str(exc),
            },
        )
        logger.exception(
            "Batch child sync failed: config=%s provider=%s error=%s",
            config_id,
            provider,
            exc,
        )
        if isinstance(exc, _TerminalSyncError):
            logger.error(
                "Batch child sync failed permanently (no retry): config=%s error=%s",
                config_id,
                exc,
            )
            raise exc
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
