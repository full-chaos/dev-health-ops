from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from celery import chord, group

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.sync_runtime import (
    _dispatch_post_sync_tasks,
    run_sync_config,
)
from dev_health_ops.workers.task_utils import (
    _decrypt_credential_sync,
    _extract_provider_token,
    _get_db_url,
    _inject_provider_token,
    _merge_sync_flags,
    _resolve_env_credentials,
)

logger = logging.getLogger(__name__)

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


def _get_batch_size(sync_options: dict[str, Any]) -> int:
    """Get batch size from sync_options or environment, default 5."""
    size = sync_options.get("batch_size")
    if size is not None:
        return int(size)
    env_size = os.getenv("SYNC_BATCH_SIZE")
    if env_size is not None:
        return int(env_size)
    return 5

@celery_app.task(bind=True, queue="sync", name="dev_health_ops.workers.tasks._batch_sync_callback")
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

@celery_app.task(bind=True, queue="sync", rate_limit="5/m", name="dev_health_ops.workers.tasks.dispatch_batch_sync")
def dispatch_batch_sync(
    self,
    config_id: str,
    org_id: str,
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

        from dev_health_ops.discovery.repos import discover_repos_for_config

        try:
            repos = discover_repos_for_config(config, credentials)
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

@celery_app.task(bind=True, max_retries=3, queue="sync", name="dev_health_ops.workers.tasks._run_sync_for_repo")
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

            run_async(run_with_store(db_url, db_type, _github_handler, org_id=org_id))
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

            run_async(run_with_store(db_url, db_type, _gitlab_handler, org_id=org_id))
            result_payload.update(
                {"project_id": int(project_id), "gitlab_url": gitlab_url}
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
        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
