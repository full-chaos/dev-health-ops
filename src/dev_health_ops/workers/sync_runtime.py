from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from celery import chain

from dev_health_ops.credentials.resolver import (
    github_credentials_from_mapping,
    gitlab_credentials_from_mapping,
    resolve_gitlab_url,
)
from dev_health_ops.exceptions import (
    AuthenticationException,
    ConnectorException,
)
from dev_health_ops.utils.datetime import utc_today
from dev_health_ops.workers.async_runner import run_async
from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.org_guard import organization_exists_sync
from dev_health_ops.workers.task_utils import (
    _GIT_TARGETS,
    _WORK_ITEM_TARGETS,
    _as_dict,
    _as_int,
    _as_str,
    _as_str_list,
    _as_uuid,
    _credential_mapping,
    _extract_owner_repo,
    _get_db_url,
    _jira_query_options,
    _merge_sync_flags,
    _normalize_sync_targets,
    _resolve_env_credentials,
)
from dev_health_ops.workers.team_autoimport import run_team_autoimport

# DORA (deployment frequency, lead time, change-failure-rate, MTTR) is computed
# from synced deployments/CI/incidents in ClickHouse. These targets can be
# scheduled independently of git/prs (e.g. a deployments-only sync config), so a
# post-sync DORA recompute must fire on any of them, not only on git (CHAOS-2399
# — without this a deployments-only sync lagged DORA up to a day until the daily
# beat). Defined here next to its sole consumer (_dispatch_post_sync_tasks).
_DORA_TARGETS = {"deployments", "cicd", "incidents"}

logger = logging.getLogger(__name__)


class _TerminalSyncError(Exception):
    pass


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
        setattr(sink, "org_id", org_id)
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
    setattr(sink, "org_id", org_id)
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
    metrics_day: str | None = None,
    metrics_backfill_days: int | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    work_graph_from_date: str | None = None,
    work_graph_to_date: str | None = None,
) -> None:
    target_set = set(sync_targets)
    has_git = bool(target_set & _GIT_TARGETS)
    has_work_items = bool(target_set & _WORK_ITEM_TARGETS)
    has_dora = bool(target_set & _DORA_TARGETS)
    dispatched: list[str] = []

    daily_metrics_kwargs: dict[str, Any] = {"org_id": org_id}
    if metrics_day is not None:
        daily_metrics_kwargs["day"] = metrics_day
    if metrics_backfill_days is not None:
        daily_metrics_kwargs["backfill_days"] = metrics_backfill_days

    if has_git and not has_work_items:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_daily_metrics",
            kwargs=daily_metrics_kwargs,
            queue="metrics",
        )
        dispatched.append("run_daily_metrics")

    if has_work_items:
        dispatched.append("sync_teams_to_analytics")
        dispatched.append("run_daily_metrics")

    if has_git:
        celery_app.send_task(
            "dev_health_ops.workers.tasks.run_complexity_job",
            kwargs={"org_id": org_id},
            queue="metrics",
        )
        dispatched.append("run_complexity_job")

    # Work-graph build + investment materialization read org-wide persisted
    # data (git PRs/commits + work items) that accumulate across separate sync
    # configs, so they must fire after *either* kind of sync — not only when a
    # single config carries both. Gating on `has_git and has_work_items` left
    # work-items-only providers (Jira/Linear) and separately scheduled code vs
    # work-item syncs with a permanently empty /investment view (CHAOS-2374).
    if has_git or has_work_items:
        build_kwargs: dict[str, Any] = {"org_id": org_id}
        graph_from_date = work_graph_from_date or from_date
        if graph_from_date is not None:
            build_kwargs["from_date"] = graph_from_date
        graph_to_date = work_graph_to_date or to_date
        if graph_to_date is not None:
            build_kwargs["to_date"] = graph_to_date

        materialize_kwargs: dict[str, Any] = {"org_id": org_id}
        if from_date is not None:
            materialize_kwargs["from_date"] = from_date
        if to_date is not None:
            materialize_kwargs["to_date"] = to_date

        # Investment distributions depend on the freshly rebuilt work graph.
        # Enqueueing two independent tasks on the same queue only preserves
        # publish order, not completion order: with multiple metrics workers
        # materialization could race ahead of the build and read a stale/empty
        # graph (CHAOS-2374). Chain them so materialize only starts after the
        # build *succeeds*. The links are immutable (.si()) so a parent's return
        # value is not injected as a positional arg into the next step.
        #
        build_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.run_work_graph_build",
            kwargs=build_kwargs,
            queue="metrics",
            immutable=has_work_items,
        )
        materialize_sig = celery_app.signature(
            "dev_health_ops.workers.tasks.dispatch_investment_materialize_partitioned",
            kwargs=materialize_kwargs,
            queue="default",
            immutable=True,
        )
        if has_work_items:
            sync_teams_sig = celery_app.signature(
                "dev_health_ops.workers.tasks.sync_teams_to_analytics",
                kwargs={"org_id": org_id},
                queue="metrics",
            )
            daily_metrics_sig = celery_app.signature(
                "dev_health_ops.workers.tasks.run_daily_metrics",
                kwargs=daily_metrics_kwargs,
                queue="metrics",
                immutable=True,
            )
            chain(
                sync_teams_sig,
                daily_metrics_sig,
                build_sig,
                materialize_sig,
            ).apply_async()
        else:
            chain(build_sig, materialize_sig).apply_async()
        dispatched.append("run_work_graph_build")
        dispatched.append("dispatch_investment_materialize_partitioned")

    if has_git or has_dora:
        # CHAOS-2382: DORA is provider-agnostic — computed from synced
        # deployments/incidents in ClickHouse, which both GitHub and GitLab
        # populate. Dispatch for every git-syncing org, not just GitLab.
        # CHAOS-2399: also dispatch after a deployments/cicd/incidents sync.
        # Those targets can be scheduled without git (a deployments-only sync
        # config), and they are exactly the inputs DORA reads — so gating on
        # git alone left DORA stale until the daily beat after such a sync.
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


def _is_terminal_sync_error(exc: Exception) -> bool:
    return isinstance(exc, (_TerminalSyncError, ValueError))


def _run_team_autoimport_for_sync_config(
    *,
    provider: str,
    org_id: str,
    credentials: dict[str, Any],
    sync_options: dict[str, Any],
    sync_targets: list[str],
    config_id: str,
    triggered_by: str,
    analytics_db_url: str | None,
) -> dict[str, Any] | None:
    if not sync_options.get("auto_import_teams"):
        return None
    return run_team_autoimport(
        provider=provider,
        org_id=org_id,
        credentials=credentials,
        scope={
            "mode": "sync_config",
            "sync_config_id": config_id,
            "sync_targets": sync_targets,
            "sync_options": dict(sync_options),
            "triggered_by": triggered_by,
        },
        analytics_db_url=analytics_db_url,
    )


@celery_app.task(bind=True, max_retries=3, queue="sync")
def run_sync_config(
    self,
    config_id: str,
    org_id: str,
    triggered_by: str = "manual",
    pending_run_id: str | None = None,
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
    from dev_health_ops.sync.watermarks import (
        get_legacy_repo_watermark,
        set_legacy_repo_watermark,
    )

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
            if not organization_exists_sync(session, org_id):
                logger.info("Skipping sync config task for deleted org_id=%s", org_id)
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
            if not bool(config.is_active):
                if pending_run_id is not None:
                    run_record = (
                        session.query(JobRun)
                        .filter(JobRun.id == uuid.UUID(pending_run_id))
                        .one_or_none()
                    )
                    if run_record is not None:
                        run_record.status = JobRunStatus.CANCELLED.value
                        run_record.completed_at = datetime.now(timezone.utc)
                        run_record.error = "Sync configuration is paused"
                session.flush()
                return {"status": "skipped", "reason": "sync_config_inactive"}

            provider = _as_str(config.provider).lower()
            config_name = _as_str(config.name)
            sync_targets = _normalize_sync_targets(
                provider, _as_str_list(config.sync_targets)
            )
            sync_options = _as_dict(config.sync_options)

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
                        f"Credential not found for sync configuration: {config.credential_id}"
                    )

                # Merge non-sensitive credential.config (e.g. self-hosted
                # GitLab url) under the decrypted secrets (CHAOS-2282).
                credentials = _credential_mapping(credential)
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
                explicit_cron = sync_options.get("schedule_cron")
                job = ScheduledJob(
                    name=f"sync-config-{_as_uuid(config.id)}",
                    job_type="sync",
                    schedule_cron=str(explicit_cron or "0 * * * *"),
                    org_id=org_id,
                    provider=provider,
                    job_config={
                        "provider": provider,
                        "sync_config_id": str(_as_uuid(config.id)),
                    },
                    sync_config_id=_as_uuid(config.id),
                    tz=str(sync_options.get("timezone") or "UTC"),
                    # Manual-only configs keep the job row for JobRun anchoring
                    # but must stay PAUSED so the scheduler never picks them up
                    # (CHAOS-2297).
                    status=(
                        JobStatus.ACTIVE.value
                        if bool(config.is_active) and explicit_cron
                        else JobStatus.PAUSED.value
                    ),
                )
                session.add(job)
                session.flush()
            else:
                # Reconcile cron AND status with the config (the source of
                # truth) so out-of-band config changes can't leave the job
                # parked PAUSED or stale ACTIVE (CHAOS-2297).
                explicit_cron = sync_options.get("schedule_cron")
                setattr(job, "schedule_cron", str(explicit_cron or "0 * * * *"))
                setattr(
                    job,
                    "status",
                    JobStatus.ACTIVE.value
                    if bool(config.is_active) and explicit_cron
                    else JobStatus.PAUSED.value,
                )

            job_id = _as_uuid(job.id)

            run: JobRun | None = None
            if pending_run_id is not None:
                # Reuse the PENDING row created at trigger time.
                run = (
                    session.query(JobRun)
                    .filter(JobRun.id == uuid.UUID(pending_run_id))
                    .one_or_none()
                )
            if run is None:
                run = JobRun(
                    job_id=job_id,
                    triggered_by=triggered_by,
                    status=JobRunStatus.PENDING.value,
                )
                session.add(run)
                session.flush()
            run_id = _as_uuid(run.id)

            setattr(run, "status", JobRunStatus.RUNNING.value)
            setattr(run, "started_at", started_at)
            setattr(job, "is_running", True)
            setattr(job, "last_run_at", started_at)
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

        code_sync_targets = [
            target for target in sync_targets if target != "work-items"
        ]

        if provider == "github" and code_sync_targets:
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
                    get_legacy_repo_watermark(session, org_id, repo_id_for_watermark, t)
                    for t in sync_targets
                ]
                valid = [w for w in watermarks if w is not None]
                if valid and len(valid) == len(sync_targets):
                    since_dt = min(valid)

        if provider == "github" and code_sync_targets:
            owner_repo = _extract_owner_repo(
                config_name=config_name, sync_options=sync_options
            )
            if owner_repo is None:
                raise ValueError(
                    "Missing GitHub owner/repo in sync options or config name"
                )

            owner, repo_name = owner_repo
            github_credentials = github_credentials_from_mapping(credentials)
            if github_credentials is None:
                raise ValueError(
                    "Missing GitHub token or App credentials for sync configuration"
                )

            merged_flags = _merge_sync_flags(code_sync_targets)

            async def _github_handler(store):
                await process_github_repo(
                    store=store,
                    owner=owner,
                    repo_name=repo_name,
                    token=github_credentials,
                    since=since_dt,
                    blame_only=merged_flags.get("blame_only", False),
                    sync_git=merged_flags.get("sync_git", False),
                    sync_prs=merged_flags.get("sync_prs", False),
                    sync_cicd=merged_flags.get("sync_cicd", False),
                    sync_deployments=merged_flags.get("sync_deployments", False),
                    sync_incidents=merged_flags.get("sync_incidents", False),
                    sync_security=merged_flags.get("sync_security", False),
                    sync_tests=merged_flags.get("sync_tests", False),
                )

            run_async(run_with_store(db_url, db_type, _github_handler, org_id=org_id))
            result_payload.update(
                {
                    "owner": owner,
                    "repo": repo_name,
                    "flags": merged_flags,
                }
            )

        elif provider == "gitlab" and code_sync_targets:
            feature_flag_requested = "feature-flags" in code_sync_targets
            gitlab_targets = [
                target
                for target in code_sync_targets
                if target not in {"feature-flags", "work-items"}
            ]
            if feature_flag_requested:
                # Feature flags are optional enrichment. A 403 (feature
                # disabled or token lacks Developer access) or any other
                # connector/config failure here must NOT abort the core
                # git/MR sync — record an observable skip status and continue
                # (CHAOS: GitLab 403 graceful degrade).
                try:
                    result_payload["feature_flags"] = _sync_gitlab_feature_flags(
                        db_url=db_url,
                        org_id=org_id,
                        credentials=credentials,
                        sync_options=sync_options,
                    )
                except AuthenticationException as exc:
                    logger.warning(
                        "Skipping GitLab feature-flag sync for org %s: %s",
                        org_id,
                        exc,
                    )
                    result_payload["feature_flags"] = {
                        "status": "skipped",
                        "reason": "forbidden",
                        "detail": str(exc),
                    }
                except (ConnectorException, ValueError) as exc:
                    logger.warning(
                        "GitLab feature-flag sync failed for org %s: %s",
                        org_id,
                        exc,
                        exc_info=True,
                    )
                    result_payload["feature_flags"] = {
                        "status": "failed",
                        "reason": type(exc).__name__,
                        "detail": str(exc),
                    }

            if not gitlab_targets:
                pass  # feature-flags-only sync handled above
            else:
                project_id = sync_options.get("project_id")
                if project_id is None:
                    raise ValueError("Missing GitLab project_id in sync options")

                gitlab_credentials = gitlab_credentials_from_mapping(credentials)
                if gitlab_credentials is None:
                    raise ValueError("Missing GitLab token for sync configuration")

                token = gitlab_credentials.token
                gitlab_url = resolve_gitlab_url(sync_options, gitlab_credentials)
                merged_flags = _merge_sync_flags(gitlab_targets)

                async def _gitlab_handler(store):
                    await process_gitlab_project(
                        store=store,
                        project_id=int(project_id),
                        token=token,
                        gitlab_url=gitlab_url,
                        since=since_dt,
                        blame_only=merged_flags.get("blame_only", False),
                        sync_git=merged_flags.get("sync_git", False),
                        sync_prs=merged_flags.get("sync_prs", False),
                        sync_cicd=merged_flags.get("sync_cicd", False),
                        sync_deployments=merged_flags.get("sync_deployments", False),
                        sync_incidents=merged_flags.get("sync_incidents", False),
                        sync_security=merged_flags.get("sync_security", False),
                        sync_tests=merged_flags.get("sync_tests", False),
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
            jira_project_keys, jira_jql, jira_fetch_all = _jira_query_options(
                sync_options
            )
            run_work_items_sync_job(
                db_url=db_url,
                day=utc_today(),
                backfill_days=backfill_days,
                provider="jira",
                org_id=org_id,
                credentials=credentials or None,
                jira_project_keys=jira_project_keys,
                jira_jql=jira_jql,
                jira_fetch_all=jira_fetch_all,
            )
            result_payload["backfill_days"] = backfill_days

        elif provider not in {"github", "gitlab"} and "work-items" not in sync_targets:
            raise ValueError(
                f"Unsupported sync provider/targets: provider={provider} targets={sync_targets}"
            )

        if "work-items" in sync_targets and provider != "jira":
            backfill_days = int(sync_options.get("backfill_days", 1))
            work_items_credentials: dict[str, Any] | None = credentials or None
            if provider == "gitlab" and work_items_credentials:
                gl_creds = gitlab_credentials_from_mapping(work_items_credentials)
                if gl_creds is not None:
                    work_items_credentials = {
                        **work_items_credentials,
                        "gitlab_url": resolve_gitlab_url(sync_options, gl_creds),
                    }
            run_work_items_sync_job(
                db_url=db_url,
                day=utc_today(),
                backfill_days=backfill_days,
                provider=provider,
                repo_name=sync_options.get("repo"),
                search_pattern=sync_options.get("search"),
                org_id=org_id,
                credentials=work_items_credentials,
            )
            result_payload["work_items_synced"] = True

        completed_at = datetime.now(timezone.utc)
        duration_seconds = int((completed_at - started_at).total_seconds())

        team_autoimport = _run_team_autoimport_for_sync_config(
            provider=provider,
            org_id=org_id,
            credentials=credentials,
            sync_options=sync_options,
            sync_targets=sync_targets,
            config_id=config_id,
            triggered_by=triggered_by,
            analytics_db_url=db_url,
        )
        if team_autoimport is not None:
            result_payload["team_autoimport"] = team_autoimport

        with get_postgres_session_sync() as session:
            run_record = session.query(JobRun).filter(JobRun.id == run_id).one_or_none()
            job_record = (
                session.query(ScheduledJob)
                .filter(ScheduledJob.id == job_id)
                .one_or_none()
            )
            config_record = (
                session.query(SyncConfiguration)
                .filter(
                    SyncConfiguration.id == config_uuid,
                    SyncConfiguration.org_id == org_id,
                )
                .one_or_none()
            )

            if run_record:
                setattr(run_record, "status", JobRunStatus.SUCCESS.value)
                setattr(run_record, "completed_at", completed_at)
                setattr(run_record, "duration_seconds", duration_seconds)
                setattr(run_record, "result", result_payload)
                setattr(run_record, "error", None)

            if job_record:
                setattr(job_record, "is_running", False)
                setattr(job_record, "last_run_status", JobRunStatus.SUCCESS.value)
                setattr(job_record, "last_run_duration_seconds", duration_seconds)
                setattr(job_record, "last_run_error", None)
                setattr(job_record, "run_count", _as_int(job_record.run_count) + 1)

            if config_record:
                setattr(config_record, "last_sync_at", completed_at)
                setattr(config_record, "last_sync_success", True)
                setattr(config_record, "last_sync_error", None)
                setattr(config_record, "last_sync_stats", result_payload)

            session.flush()

            if repo_id_for_watermark:
                for t in sync_targets:
                    set_legacy_repo_watermark(
                        session, org_id, repo_id_for_watermark, t, started_at
                    )
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
                    run_record = (
                        session.query(JobRun).filter(JobRun.id == run_id).one_or_none()
                    )
                    job_record = (
                        session.query(ScheduledJob)
                        .filter(ScheduledJob.id == job_id)
                        .one_or_none()
                    )
                    config_record = (
                        session.query(SyncConfiguration)
                        .filter(
                            SyncConfiguration.id == config_uuid,
                            SyncConfiguration.org_id == org_id,
                        )
                        .one_or_none()
                    )

                    if run_record:
                        setattr(run_record, "status", JobRunStatus.FAILED.value)
                        setattr(run_record, "completed_at", completed_at)
                        setattr(run_record, "duration_seconds", duration_seconds)
                        setattr(run_record, "error", str(exc))

                    if job_record:
                        setattr(job_record, "is_running", False)
                        setattr(
                            job_record, "last_run_status", JobRunStatus.FAILED.value
                        )
                        setattr(
                            job_record,
                            "last_run_duration_seconds",
                            duration_seconds,
                        )
                        setattr(job_record, "last_run_error", str(exc))
                        setattr(
                            job_record, "run_count", _as_int(job_record.run_count) + 1
                        )
                        setattr(
                            job_record,
                            "failure_count",
                            _as_int(job_record.failure_count) + 1,
                        )

                    if config_record:
                        setattr(config_record, "last_sync_at", completed_at)
                        setattr(config_record, "last_sync_success", False)
                        setattr(config_record, "last_sync_error", str(exc))

                    session.flush()
        except Exception as update_error:
            logger.error("Failed updating job run failure state: %s", update_error)

        if _is_terminal_sync_error(exc):
            logger.error(
                "Sync config task failed terminally; not retrying: config_id=%s org_id=%s error=%s",
                config_id,
                org_id,
                exc,
            )
            raise

        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
