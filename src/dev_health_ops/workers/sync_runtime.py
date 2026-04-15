from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import (
    _GIT_TARGETS,
    _WORK_ITEM_TARGETS,
    _decrypt_credential_sync,
    _extract_owner_repo,
    _extract_provider_token,
    _get_db_url,
    _inject_provider_token,
    _merge_sync_flags,
    _resolve_env_credentials,
)

logger = logging.getLogger(__name__)


def _sync_launchdarkly_feature_flags(
    *,
    db_url: str,
    org_id: str,
    credentials: dict[str, Any],
    sync_options: dict[str, Any],
    since_dt: datetime | None,
) -> dict[str, Any]:
    from dev_health_ops.connectors.launchdarkly import LaunchDarklyConnector
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.processors.launchdarkly import (
        normalize_audit_events,
        normalize_flags,
    )
    from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder
    from dev_health_ops.work_graph.ids import generate_feature_flag_id
    from dev_health_ops.work_graph.models import EdgeType, NodeType, Provenance

    api_key = str(credentials.get("api_key") or "")
    project_key = str(
        credentials.get("project_key") or sync_options.get("project_key") or ""
    )
    environment = str(
        credentials.get("environment") or sync_options.get("environment") or ""
    )
    if not api_key or not project_key:
        raise ValueError(
            "LaunchDarkly feature-flag sync requires api_key and project_key"
        )
    if not db_url.startswith("clickhouse://"):
        raise ValueError(
            "Feature-flag sync requires CLICKHOUSE_URI / ClickHouse analytics sink"
        )

    async def _run() -> dict[str, Any]:
        async with LaunchDarklyConnector(
            api_key=api_key, project_key=project_key
        ) as connector:
            raw_flags = await connector.get_flags(project_key)
            raw_events = await connector.get_audit_log(since=since_dt, limit=200)

        flags = normalize_flags(raw_flags, org_id)
        if environment:
            flags = [
                flag.__class__(
                    provider=flag.provider,
                    flag_key=flag.flag_key,
                    project_key=flag.project_key,
                    repo_id=flag.repo_id,
                    environment=environment,
                    flag_type=flag.flag_type,
                    created_at=flag.created_at,
                    archived_at=flag.archived_at,
                    last_synced=flag.last_synced,
                    org_id=flag.org_id,
                )
                for flag in flags
            ]
        events = normalize_audit_events(raw_events, org_id)
        if environment:
            events = [
                event.__class__(
                    event_type=event.event_type,
                    flag_key=event.flag_key,
                    environment=event.environment or environment,
                    repo_id=event.repo_id,
                    actor_type=event.actor_type,
                    prev_state=event.prev_state,
                    next_state=event.next_state,
                    event_ts=event.event_ts,
                    ingested_at=event.ingested_at,
                    source_event_id=event.source_event_id,
                    dedupe_key=event.dedupe_key,
                    org_id=event.org_id,
                )
                for event in events
            ]

        sink = ClickHouseMetricsSink(db_url)
        sink.org_id = org_id  # type: ignore[attr-defined]
        builder = WorkGraphBuilder(BuildConfig(dsn=db_url, org_id=org_id))
        try:
            sink.write_feature_flags(flags)
            sink.write_feature_flag_events(events)

            latest_events: dict[str, Any] = {}
            for event in events:
                existing = latest_events.get(event.flag_key)
                if existing is None or event.event_ts > existing.event_ts:
                    latest_events[event.flag_key] = event

            for flag in flags:
                builder.add_feature_flag_node(
                    flag_key=flag.flag_key,
                    provider=flag.provider,
                    project_key=flag.project_key or project_key,
                    repo_id=flag.repo_id,
                    event_ts=flag.created_at,
                )
                latest_event = latest_events.get(flag.flag_key)
                if latest_event is None:
                    continue
                flag_id = generate_feature_flag_id(
                    org_id,
                    flag.provider,
                    flag.project_key or project_key,
                    flag.flag_key,
                )
                builder.add_feature_flag_edge(
                    flag_id=flag_id,
                    target_type=NodeType.FEATURE_FLAG,
                    target_id=flag_id,
                    edge_type=EdgeType.CONFIG_CHANGED_BY,
                    confidence=1.0,
                    evidence=(
                        f"{latest_event.event_ts.isoformat()}"
                        f"|{latest_event.event_type}"
                        f"|{latest_event.next_state or ''}"
                    ),
                    provenance=Provenance.NATIVE,
                    provider=flag.provider,
                    event_ts=latest_event.event_ts,
                )
        finally:
            builder.close()
            sink.close()

        return {
            "flags_synced": len(flags),
            "events_synced": len(events),
            "project_key": project_key,
            "environment": environment or None,
        }

    return run_async(_run())


def _sync_gitlab_feature_flags(
    *,
    db_url: str,
    org_id: str,
    credentials: dict[str, Any],
    sync_options: dict[str, Any],
) -> dict[str, Any]:
    from dev_health_ops.connectors.gitlab import GitLabConnector
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
    from dev_health_ops.processors.gitlab_feature_flags import (
        normalize_gitlab_feature_flags,
        snapshot_gitlab_feature_flag_events,
    )
    from dev_health_ops.work_graph.builder import BuildConfig, WorkGraphBuilder
    from dev_health_ops.work_graph.ids import generate_feature_flag_id
    from dev_health_ops.work_graph.models import EdgeType, NodeType, Provenance

    token = str(credentials.get("token") or "")
    gitlab_url = str(
        credentials.get("url")
        or credentials.get("base_url")
        or sync_options.get("gitlab_url")
        or "https://gitlab.com"
    )
    project_id_or_path = (
        sync_options.get("project_id")
        or sync_options.get("repo")
        or sync_options.get("project_key")
    )
    if not token or not project_id_or_path:
        raise ValueError(
            "GitLab feature-flag sync requires token and project_id/project path"
        )
    if not db_url.startswith("clickhouse://"):
        raise ValueError(
            "Feature-flag sync requires CLICKHOUSE_URI / ClickHouse analytics sink"
        )

    connector = GitLabConnector(url=gitlab_url, private_token=token)
    raw_flags = connector.get_feature_flags(project_id_or_path)
    project_key = connector.get_project_name(project_id_or_path)
    repo_id = None

    flags = normalize_gitlab_feature_flags(
        raw_flags,
        project_key=project_key,
        org_id=org_id,
        repo_id=repo_id,
    )
    events = snapshot_gitlab_feature_flag_events(
        raw_flags,
        project_key=project_key,
        org_id=org_id,
        repo_id=repo_id,
    )

    sink = ClickHouseMetricsSink(db_url)
    sink.org_id = org_id  # type: ignore[attr-defined]
    builder = WorkGraphBuilder(BuildConfig(dsn=db_url, org_id=org_id))
    try:
        sink.write_feature_flags(flags)
        sink.write_feature_flag_events(events)

        latest_events: dict[str, Any] = {}
        for event in events:
            existing = latest_events.get(event.flag_key)
            if existing is None or event.event_ts > existing.event_ts:
                latest_events[event.flag_key] = event

        for flag in flags:
            builder.add_feature_flag_node(
                flag_key=flag.flag_key,
                provider=flag.provider,
                project_key=flag.project_key or project_key,
                repo_id=flag.repo_id,
                event_ts=flag.created_at,
            )
            latest_event = latest_events.get(flag.flag_key)
            if latest_event is None:
                continue
            flag_id = generate_feature_flag_id(
                org_id,
                flag.provider,
                flag.project_key or project_key,
                flag.flag_key,
            )
            builder.add_feature_flag_edge(
                flag_id=flag_id,
                target_type=NodeType.FEATURE_FLAG,
                target_id=flag_id,
                edge_type=EdgeType.CONFIG_CHANGED_BY,
                confidence=1.0,
                evidence=(
                    f"{latest_event.event_ts.isoformat()}"
                    f"|{latest_event.event_type}"
                    f"|{latest_event.next_state or ''}"
                ),
                provenance=Provenance.NATIVE,
                provider=flag.provider,
                event_ts=latest_event.event_ts,
            )
    finally:
        builder.close()
        sink.close()

    return {
        "flags_synced": len(flags),
        "events_synced": len(events),
        "project_key": project_key,
        "gitlab_url": gitlab_url,
    }


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
            kwargs={"org_id": org_id},
            queue="metrics",
        )
        dispatched.append("run_daily_metrics")

        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_complexity_job",
            kwargs={"org_id": org_id},
            queue="metrics",
        )
        dispatched.append("run_complexity_job")

    if has_git and has_work_items:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_work_graph_build",
            kwargs={"org_id": org_id},
            queue="metrics",
        )

    if provider == "gitlab" and has_git:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_dora_metrics",
            kwargs={"org_id": org_id},
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
    org_id: str,
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
                    ScheduledJob.job_type == "sync",
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
            _pid = sync_options.get("project_id") or sync_options.get("repo")
            if _pid is not None:
                repo_id_for_watermark = str(_pid)
        elif provider == "launchdarkly":
            project_key = sync_options.get("project_key") or credentials.get(
                "project_key"
            )
            environment = sync_options.get("environment") or credentials.get(
                "environment"
            )
            if project_key:
                repo_id_for_watermark = f"{project_key}:{environment or 'default'}"

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

            run_async(run_with_store(db_url, db_type, _github_handler, org_id=org_id))
            result_payload.update(
                {
                    "owner": owner,
                    "repo": repo_name,
                    "flags": merged_flags,
                }
            )

        elif provider == "gitlab":
            feature_flag_requested = "feature-flags" in sync_targets
            gitlab_targets = [
                target for target in sync_targets if target != "feature-flags"
            ]
            if feature_flag_requested:
                result_payload["feature_flags"] = _sync_gitlab_feature_flags(
                    db_url=db_url,
                    org_id=org_id,
                    credentials=credentials,
                    sync_options=sync_options,
                )

            if not gitlab_targets:
                pass  # feature-flags-only sync handled above
            else:
                project_id = sync_options.get("project_id")
                if project_id is None:
                    raise ValueError("Missing GitLab project_id in sync options")

                token = str(credentials.get("token") or "")
                if not token:
                    raise ValueError("Missing GitLab token for sync configuration")

                gitlab_url = str(sync_options.get("gitlab_url", "https://gitlab.com"))
                merged_flags = _merge_sync_flags(gitlab_targets)

                async def _gitlab_handler(store):
                    await process_gitlab_project(
                        store=store,
                        project_id=int(project_id),
                        token=token,
                        gitlab_url=gitlab_url,
                        since=since_dt,
                        **merged_flags,
                    )

                run_async(
                    run_with_store(db_url, db_type, _gitlab_handler, org_id=org_id)
                )
                result_payload.update(
                    {
                        "project_id": int(project_id),
                        "gitlab_url": gitlab_url,
                        "flags": merged_flags,
                    }
                )

        elif provider == "launchdarkly":
            if "feature-flags" not in sync_targets:
                raise ValueError(
                    "LaunchDarkly sync configurations currently support only the feature-flags target"
                )
            result_payload["feature_flags"] = _sync_launchdarkly_feature_flags(
                db_url=db_url,
                org_id=org_id,
                credentials=credentials,
                sync_options=sync_options,
                since_dt=since_dt,
            )

        elif provider == "jira":
            backfill_days = int(sync_options.get("backfill_days", 1))
            run_work_items_sync_job(
                db_url=db_url,
                day=utc_today(),
                backfill_days=backfill_days,
                provider="jira",
                org_id=org_id,
            )
            result_payload["backfill_days"] = backfill_days

        if "work-items" in sync_targets and provider != "jira":
            token = _extract_provider_token(provider, credentials)
            if token:
                _inject_provider_token(provider, token)
            backfill_days = int(sync_options.get("backfill_days", 1))
            run_work_items_sync_job(
                db_url=db_url,
                day=utc_today(),
                backfill_days=backfill_days,
                provider=provider,
                repo_name=sync_options.get("repo"),
                search_pattern=sync_options.get("search"),
                org_id=org_id,
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
