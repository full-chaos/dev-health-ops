from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Protocol, cast

from croniter import croniter as Croniter
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    JOB_RUN_STATUS_LABELS,
    BackfillRequest,
    JobRunResponse,
    SyncConfigBatchCreate,
    SyncConfigBatchResponse,
    SyncConfigCreate,
    SyncConfigResponse,
    SyncConfigUpdate,
)
from dev_health_ops.api.services.configuration import (
    IntegrationCredentialsService,
    SyncConfigurationService,
)
from dev_health_ops.api.services.licensing import TierLimitService
from dev_health_ops.models import SyncRun
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
)
from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    SyncConfiguration,
)
from dev_health_ops.sync.datasets import supported_datasets, supported_legacy_targets
from dev_health_ops.sync.planner import plan_sync_run
from dev_health_ops.sync.trigger_routing import (
    mark_sync_run_failed,
    plan_request_for_config,
    planner_request_for_config_if_routed,
)
from dev_health_ops.workers.queues import sync_queue_for_provider
from dev_health_ops.workers.sync_batch import _is_batch_eligible
from dev_health_ops.workers.sync_units import dispatch_sync_run

from .common import get_session

router = APIRouter()

logger = logging.getLogger(__name__)


def _mark_job_run_failed(sync_session, run_id: str, error: str) -> None:
    completed_at = datetime.now(timezone.utc)
    run = (
        sync_session.query(JobRun)
        .filter(JobRun.id == uuid.UUID(str(run_id)))
        .one_or_none()
    )
    if run is None:
        return
    run.status = JobRunStatus.FAILED.value
    run.completed_at = completed_at
    run.error = error
    started_at = getattr(run, "started_at", None)
    if started_at is not None:
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        run.duration_seconds = max(0, int((completed_at - started_at).total_seconds()))
    sync_session.flush()


def _ensure_pending_sync_job_run(
    sync_session, config, org_id: str, triggered_by: str
) -> str:
    """Find-or-create the ScheduledJob anchor and create a PENDING JobRun.

    Returns the new JobRun id as a string.  Used by both the regular
    /trigger handler and the legacy backfill path so the sync-activity list
    shows the run immediately.
    """
    import uuid as _uuid

    config_uuid = _uuid.UUID(str(config.id))
    job = (
        sync_session.query(ScheduledJob)
        .filter(
            ScheduledJob.org_id == org_id,
            ScheduledJob.sync_config_id == config_uuid,
            ScheduledJob.job_type == "sync",
        )
        .one_or_none()
    )
    if job is None:
        _sync_options = dict(config.sync_options or {})
        _provider = str(config.provider or "")
        _explicit_cron = _sync_options.get("schedule_cron")
        job = ScheduledJob(
            name=f"sync-config-{config_uuid}",
            job_type="sync",
            schedule_cron=str(_explicit_cron or "0 * * * *"),
            org_id=org_id,
            provider=_provider,
            job_config={
                "provider": _provider,
                "sync_config_id": str(config_uuid),
            },
            sync_config_id=config_uuid,
            tz=str(_sync_options.get("timezone") or "UTC"),
            # Manual-only configs keep the job row for JobRun anchoring
            # but must not be picked up by the scheduler (CHAOS-2297).
            status=(
                JobStatus.ACTIVE.value
                if bool(config.is_active) and _explicit_cron
                else JobStatus.PAUSED.value
            ),
        )
        sync_session.add(job)
        sync_session.flush()
    run = JobRun(
        job_id=_uuid.UUID(str(job.id)),
        triggered_by=triggered_by,
        status=JobRunStatus.PENDING.value,
    )
    sync_session.add(run)
    sync_session.flush()
    return str(run.id)


async def _is_planner_active(session: AsyncSession, org_id: str) -> bool:
    """Return True when the integration planner is active for this org.

    Reads the ``sync.migrated_trigger_routing_enabled`` Setting row written
    by the CHAOS-2516 migration helper.
    """
    from dev_health_ops.models.settings import Setting, SettingCategory
    from dev_health_ops.sync.config_migration import (
        MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
    )

    def _check(sync_session) -> bool:
        row = (
            sync_session.query(Setting)
            .filter(
                Setting.org_id == org_id,
                Setting.category == SettingCategory.SYNC.value,
                Setting.key == MIGRATED_TRIGGER_ROUTING_SETTING_KEY,
            )
            .one_or_none()
        )
        if row is None:
            return False
        return str(getattr(row, "value", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    return await session.run_sync(_check)


async def _active_repo_count_for_batch_limit(
    session: AsyncSession, org_id: str, provider: str
) -> int:
    provider_normalized = provider.lower()
    active_configs = await session.execute(
        select(SyncConfiguration).where(
            SyncConfiguration.org_id == org_id,
            SyncConfiguration.is_active.is_(True),
        )
    )
    active_config_rows = list(active_configs.scalars().all())
    parent_ids_with_children = {
        config.parent_id
        for config in active_config_rows
        if config.parent_id is not None
    }
    planner_parent_ids = [
        config.id
        for config in active_config_rows
        if str(config.provider or "").lower() == provider_normalized
        and config.parent_id is None
        and config.migrated_integration_id is not None
        and config.id not in parent_ids_with_children
    ]
    legacy_count = sum(
        1 for config in active_config_rows if config.id not in planner_parent_ids
    )
    if not planner_parent_ids:
        return legacy_count

    source_count = await session.scalar(
        select(func.count(IntegrationSource.id)).where(
            IntegrationSource.org_id == org_id,
            func.lower(IntegrationSource.provider) == provider_normalized,
            IntegrationSource.integration_id.in_(
                select(SyncConfiguration.migrated_integration_id).where(
                    SyncConfiguration.id.in_(planner_parent_ids)
                )
            ),
            IntegrationSource.is_enabled.is_(True),
        )
    )
    return legacy_count + int(source_count or 0)


class _MutableSyncConfiguration(Protocol):
    sync_targets: list[str]
    sync_options: dict[str, Any]
    is_active: bool


def _sync_config_to_response(
    config: object,
    children_count: int | None = None,
) -> SyncConfigResponse:
    return SyncConfigResponse.model_validate(
        {
            "id": str(getattr(config, "id")),
            "name": getattr(config, "name"),
            "provider": getattr(config, "provider"),
            "credential_id": (
                str(getattr(config, "credential_id"))
                if getattr(config, "credential_id") is not None
                else None
            ),
            "sync_targets": list(getattr(config, "sync_targets") or []),
            "sync_options": dict(getattr(config, "sync_options") or {}),
            "is_active": getattr(config, "is_active"),
            "parent_id": (
                str(getattr(config, "parent_id"))
                if getattr(config, "parent_id") is not None
                else None
            ),
            "children_count": children_count,
            "last_sync_at": getattr(config, "last_sync_at"),
            "last_sync_success": getattr(config, "last_sync_success"),
            "last_sync_error": getattr(config, "last_sync_error"),
            "created_at": getattr(config, "created_at"),
            "updated_at": getattr(config, "updated_at"),
        }
    )


def _job_run_response(run: object) -> JobRunResponse:
    status_value = int(getattr(run, "status"))
    return JobRunResponse.model_validate(
        {
            "id": str(getattr(run, "id")),
            "job_id": str(getattr(run, "job_id")),
            "status": JOB_RUN_STATUS_LABELS.get(status_value, "unknown"),
            "started_at": getattr(run, "started_at"),
            "completed_at": getattr(run, "completed_at"),
            "duration_seconds": getattr(run, "duration_seconds"),
            "result": getattr(run, "result"),
            "error": getattr(run, "error"),
            "triggered_by": getattr(run, "triggered_by"),
            "created_at": getattr(run, "created_at"),
        }
    )


def _backfill_job_response(job: object, run_counts: dict[str, Any] | None = None):
    from dev_health_ops.api.schemas.backfill import BackfillJobResponse

    run_counts = run_counts or {}
    total_chunks = int(run_counts.get("total_chunks", getattr(job, "total_chunks")))
    completed_chunks = int(
        run_counts.get("completed_chunks", getattr(job, "completed_chunks"))
    )
    progress_pct = (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0.0
    return BackfillJobResponse(
        id=str(getattr(job, "id")),
        sync_config_id=str(getattr(job, "sync_config_id")),
        status=str(run_counts.get("status", getattr(job, "status"))),
        since_date=getattr(job, "since_date"),
        before_date=getattr(job, "before_date"),
        total_chunks=total_chunks,
        completed_chunks=completed_chunks,
        failed_chunks=int(
            run_counts.get("failed_chunks", getattr(job, "failed_chunks"))
        ),
        progress_pct=progress_pct,
        error_message=run_counts.get("error_message", getattr(job, "error_message")),
        started_at=getattr(job, "started_at"),
        completed_at=run_counts.get("completed_at", getattr(job, "completed_at")),
        created_at=getattr(job, "created_at"),
    )


def _backfill_job_sync_run_id(job: object) -> str | None:
    task_id = str(getattr(job, "celery_task_id") or "")
    marker = "sync_run:"
    if marker not in task_id:
        return None
    return task_id.rsplit(marker, 1)[-1] or None


async def _backfill_job_run_counts(
    session: AsyncSession, job: object
) -> dict[str, Any] | None:
    sync_run_id = _backfill_job_sync_run_id(job)
    if sync_run_id is None:
        return None
    try:
        run_uuid = uuid.UUID(sync_run_id)
    except ValueError:
        return None
    stmt = select(SyncRun).where(
        SyncRun.id == run_uuid,
        SyncRun.org_id == str(getattr(job, "org_id")),
    )
    result = await session.execute(stmt)
    run = result.scalar_one_or_none()
    if run is None:
        return None
    return {
        "status": getattr(run, "status"),
        "total_chunks": int(getattr(run, "total_units")),
        "completed_chunks": int(getattr(run, "completed_units")),
        "failed_chunks": int(getattr(run, "failed_units")),
        "completed_at": getattr(run, "completed_at"),
        "error_message": getattr(run, "error"),
    }


# Canonical mapping of provider → supported sync targets.
# Jira/Linear only support work-items; Git/CI/CD come from code hosts.
PROVIDER_SYNC_TARGETS: dict[str, list[str]] = {
    provider: supported_legacy_targets(provider)
    for provider in ("github", "gitlab", "jira", "linear", "launchdarkly")
}

NON_REPO_SYNC_PROVIDERS = {"jira", "linear"}
DEFAULT_SYNC_CRON = "0 * * * *"
DEFAULT_GITLAB_URL = "https://gitlab.com"


def _sync_options_with_top_level_fields(
    sync_options: dict[str, Any] | None,
    *,
    schedule_cron: str | None = None,
    timezone: str | None = None,
    initial_sync_depth: int | None = None,
) -> dict[str, Any]:
    merged = dict(sync_options or {})
    if schedule_cron is not None:
        merged["schedule_cron"] = schedule_cron
    if timezone is not None:
        merged["timezone"] = timezone
    if initial_sync_depth is not None:
        merged["initial_sync_depth"] = initial_sync_depth
    return merged


def _config_has_explicit_schedule(config: object) -> bool:
    """True when the config carries a user-set schedule_cron (not manual-only)."""
    sync_options = dict(getattr(config, "sync_options") or {})
    return bool(sync_options.get("schedule_cron"))


def _schedule_cron_for_config(config: object) -> str:
    sync_options = dict(getattr(config, "sync_options") or {})
    return str(sync_options.get("schedule_cron") or DEFAULT_SYNC_CRON)


async def _upsert_scheduled_job(
    session: AsyncSession, config: object, org_id: str
) -> None:
    sync_config_id = getattr(config, "id")
    stmt = select(ScheduledJob).where(
        ScheduledJob.org_id == org_id,
        ScheduledJob.sync_config_id == sync_config_id,
        ScheduledJob.job_type == "sync",
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    sync_options = dict(getattr(config, "sync_options") or {})
    provider = str(getattr(config, "provider")).lower()
    tz = str(sync_options.get("timezone") or "UTC")
    job_config = {
        "provider": provider,
        "sync_config_id": str(sync_config_id),
    }
    # Manual-only configs (no explicit schedule_cron) must not auto-sync
    # (CHAOS-2297). Keep the job row (manual triggers anchor JobRuns to it)
    # but park it PAUSED; the stored cron is only a non-null placeholder.
    status = (
        JobStatus.ACTIVE.value
        if getattr(config, "is_active") and _config_has_explicit_schedule(config)
        else JobStatus.PAUSED.value
    )
    if job is None:
        session.add(
            ScheduledJob(
                name=f"sync-config-{sync_config_id}",
                job_type="sync",
                schedule_cron=_schedule_cron_for_config(config),
                org_id=org_id,
                provider=provider,
                job_config=job_config,
                sync_config_id=sync_config_id,
                tz=tz,
                status=status,
            )
        )
        return

    job.schedule_cron = _schedule_cron_for_config(config)
    job.provider = provider
    job.timezone = tz
    job.job_config = job_config
    job.status = status


def _planner_dataset_keys(provider: str, sync_targets: list[str]) -> list[str]:
    targets = {str(target) for target in sync_targets if target is not None}
    return [
        spec.dataset_key
        for spec in supported_datasets(provider)
        if targets.intersection(spec.legacy_targets)
    ]


def _planner_source_rows(
    payload: SyncConfigBatchCreate,
    parent_options: dict[str, Any],
    gitlab_projects: dict[str, tuple[int, str]],
    org_id: str,
    integration_id: uuid.UUID,
    config_id: uuid.UUID,
) -> list[IntegrationSource]:
    provider = payload.provider.lower()
    rows: list[IntegrationSource] = []
    owner = str(
        payload.sync_options.get("owner")
        or payload.sync_options.get("group")
        or payload.name
    )
    for repo_name in payload.repos:
        if provider == "gitlab":
            project_id, full_name = gitlab_projects[repo_name]
            source_type = "project"
            external_id = str(project_id)
            source_name = full_name.rsplit("/", 1)[-1] if full_name else str(project_id)
            metadata = {
                "path_with_namespace": full_name,
                "planner_managed_sync_config_id": str(config_id),
            }
        else:
            source_type = "repository" if provider == "github" else "source"
            full_name = f"{owner}/{repo_name}" if provider == "github" else repo_name
            external_id = full_name
            source_name = repo_name
            metadata = {
                "planner_managed_sync_config_id": str(config_id),
            }
            if provider == "github":
                metadata["owner"] = owner
        rows.append(
            IntegrationSource(
                org_id=org_id,
                integration_id=integration_id,
                provider=payload.provider,
                source_type=source_type,
                external_id=external_id,
                name=source_name,
                full_name=full_name,
                metadata_=metadata,
                is_enabled=True,
            )
        )
    return rows


@router.get("/sync-targets")
async def get_provider_sync_targets() -> dict[str, list[str]]:
    return PROVIDER_SYNC_TARGETS


@router.get(
    "/sync-configs",
    response_model=list[SyncConfigResponse],
    description=(
        "List sync configurations. "
        "Child sync configs (rows with a non-null ``parent_id`` or linked to a "
        "migrated integration via ``migrated_integration_id``/``migrated_source_id``) "
        "are **deprecated** and hidden by default when the "
        "``HIDE_MIGRATED_CHILD_CONFIGS`` feature flag is enabled. "
        "Pass ``?include_migrated=true`` to include them (support/rollback)."
    ),
)
async def list_sync_configs(
    active_only: bool = False,
    parent_only: bool = False,
    include_migrated: bool = Query(
        default=False,
        description=(
            "Include deprecated child/migrated sync configs in the response. "
            "Defaults to False when HIDE_MIGRATED_CHILD_CONFIGS is enabled. "
            "Set to true for support or rollback access."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[SyncConfigResponse]:
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=active_only)
    if parent_only:
        configs = [c for c in configs if c.parent_id is None]

    # HIDE_MIGRATED_CHILD_CONFIGS: when enabled, filter out deprecated child
    # configs from the default list response. A config is considered a
    # "migrated child" when any of the following are true:
    #   - parent_id is set (legacy child config), OR
    #   - migrated_source_id is set (linked to an integration-era source).
    # The parent SyncConfiguration gets migrated_integration_id set by the
    # migration and is the rollback anchor, so it is NOT hidden.
    # Callers may pass ?include_migrated=true to bypass this filter for
    # support or rollback access.
    _hide_migrated = os.getenv("HIDE_MIGRATED_CHILD_CONFIGS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if _hide_migrated and not include_migrated:
        configs = [
            c
            for c in configs
            if (
                getattr(c, "parent_id", None) is None
                and getattr(c, "migrated_source_id", None) is None
            )
        ]

    # Build children count map without lazy-loading relationships
    from sqlalchemy import func, select

    children_counts: dict[str, int] = {}
    parent_id_col = getattr(SyncConfiguration, "parent_id")
    sync_configuration_id_col = getattr(SyncConfiguration, "id")
    parent_ids = [
        getattr(config, "id")
        for config in configs
        if getattr(config, "parent_id") is None
    ]
    if parent_ids:
        stmt = (
            select(parent_id_col, func.count(sync_configuration_id_col))
            .where(parent_id_col.in_(parent_ids))
            .group_by(parent_id_col)
        )
        rows = (await session.execute(stmt)).all()
        children_counts = {str(pid): cnt for pid, cnt in rows}

    results = []
    for c in configs:
        cc = children_counts.get(str(getattr(c, "id")))
        results.append(_sync_config_to_response(c, children_count=cc))
    return results


def _gitlab_group_from_options(sync_options: dict[str, Any]) -> str:
    """Extract the GitLab group/namespace from batch sync options.

    The web form submits the namespace under ``owner`` (shared field with
    GitHub); API callers may use ``group`` directly.
    """
    group = sync_options.get("group") or sync_options.get("owner") or ""
    # Strip CR/LF so the user-provided group can't forge log lines when it is
    # interpolated into warnings (CodeQL: log injection).
    return str(group).replace("\r", "").replace("\n", "").strip()


async def _resolve_gitlab_batch_projects(
    session: AsyncSession,
    org_id: str,
    payload: SyncConfigBatchCreate,
) -> tuple[dict[str, tuple[int, str]], str]:
    """Resolve batch ``repos`` entries to GitLab ``(project_id, child_name)``.

    Returns the resolved mapping plus the effective ``gitlab_url``, resolved
    with the same precedence the credential resolver uses:
    ``sync_options.gitlab_url`` → decrypted credential ``url`` → credential
    ``config.url`` → ``https://gitlab.com``. The caller persists that URL into
    parent/child options so children sync against the same instance used for
    name resolution.

    Name entries are resolved by listing the group's projects through the
    stored credential; unknown or ambiguous names raise a 400. When a listing
    is available, numeric entries that match a listed project *name* are
    treated as names (a project literally named ``007`` must not be coerced
    to project id 7); numeric entries matching no listed name keep project-id
    semantics.
    """
    entries = list(dict.fromkeys(payload.repos))
    named = [r for r in entries if not r.strip().isdigit()]
    numeric = [r for r in entries if r.strip().isdigit()]
    group = _gitlab_group_from_options(payload.sync_options)

    resolved: dict[str, tuple[int, str]] = {}
    by_id: dict[int, str] = {}
    by_key: dict[str, list[tuple[int, str]]] = {}

    if named:
        if not payload.credential_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    "GitLab batch create requires credential_id to resolve "
                    "project names to project ids"
                ),
            )
        if not group:
            raise HTTPException(
                status_code=400,
                detail=(
                    "GitLab batch create requires a group (sync_options.group "
                    "or sync_options.owner) to resolve project names"
                ),
            )

    # Resolve the effective GitLab instance URL up front (even for all-numeric
    # batches) so self-hosted URLs stored on the credential are not lost.
    option_url = str(payload.sync_options.get("gitlab_url") or "").strip()
    gitlab_url = option_url or DEFAULT_GITLAB_URL
    token: str | None = None
    if payload.credential_id:
        creds_svc = IntegrationCredentialsService(session, org_id)
        decrypted, credential = await creds_svc.get_decrypted_credentials_by_id(
            payload.credential_id
        )
        if credential is None or decrypted is None:
            raise HTTPException(status_code=400, detail="Credential not found")
        token = decrypted.get("token")
        cred_config: dict[str, Any] = getattr(credential, "config") or {}
        gitlab_url = str(
            option_url
            or decrypted.get("url")
            or cred_config.get("url")
            or DEFAULT_GITLAB_URL
        )

    if named and not token:
        raise HTTPException(status_code=400, detail="GitLab credential missing token")

    # List the group's projects when name entries require it, and also
    # opportunistically when numeric entries could shadow project names.
    should_list = bool(named) or (bool(numeric) and bool(token) and bool(group))
    if should_list:
        from dev_health_ops.connectors.gitlab import GitLabConnector

        connector = GitLabConnector(url=gitlab_url, private_token=str(token))
        try:
            projects = connector.list_repositories(org_name=group)
        except Exception as exc:
            if named:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to list GitLab projects for group '{group}': {exc}",
                )
            # All-numeric batch: keep historical id semantics when the listing
            # is unavailable, but log so shadowed names are diagnosable.
            logger.warning(
                "GitLab batch create: failed to list projects for group %r to "
                "cross-check numeric entries; treating them as project ids: %s",
                group,
                exc,
            )
            projects = []

        for project in projects:
            entry = (int(project.id), project.full_name or project.name)
            by_id[entry[0]] = entry[1]
            for key in {project.name, project.full_name}:
                if key:
                    by_key.setdefault(key, []).append(entry)

        # Numeric entries matching a listed project name are names, not ids.
        named_like = named + [r for r in numeric if r in by_key]

        missing = [r for r in named if r not in by_key]
        ambiguous = [r for r in named_like if len(by_key.get(r, [])) > 1]
        if missing or ambiguous:
            parts = []
            if missing:
                parts.append(f"not found in group '{group}': {', '.join(missing)}")
            if ambiguous:
                parts.append(
                    "ambiguous (multiple projects share this name, use the "
                    f"full path): {', '.join(ambiguous)}"
                )
            raise HTTPException(
                status_code=400,
                detail=f"Could not resolve GitLab projects — {'; '.join(parts)}",
            )

        for repo in named_like:
            resolved[repo] = by_key[repo][0]

    for repo in entries:
        if repo in resolved:
            continue
        project_id = int(repo.strip())
        child_name = by_id.get(
            project_id, f"{group}/{project_id}" if group else str(project_id)
        )
        resolved[repo] = (project_id, child_name)

    return resolved, gitlab_url


@router.post(
    "/sync-configs/batch",
    response_model=SyncConfigBatchResponse,
    status_code=201,
    description=(
        "Create a parent sync config + one child per repo. "
        "**Deprecated (CHAOS-2520):** Child sync configs are deprecated in favour of "
        "the integration/source/dataset model introduced in CHAOS-2507. "
        "When the integration planner is active (``sync.migrated_trigger_routing_enabled`` "
        "setting is enabled for the org), this endpoint creates the parent config only "
        "and returns zero children. Legacy behaviour is preserved when the planner is "
        "inactive (rollback path)."
    ),
)
async def batch_create_sync_configs(
    payload: SyncConfigBatchCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigBatchResponse:
    """Create a parent sync config + one child per repo.

    Child ``sync_options`` are provider-shaped:

    - **github**: each child carries ``repo`` (plus ``owner`` inherited from
      the parent options); child name is ``{owner}/{repo}``.
    - **gitlab**: each child carries an integer ``project_id`` plus ``group``
      (and ``gitlab_url`` when present in the parent options), matching what
      the sync runtime expects (``workers/sync_runtime.py`` requires
      ``project_id``). ``payload.repos`` entries may be either numeric GitLab
      project ids or project names, which are resolved to ids by listing the
      group's projects via the stored credential. Name entries therefore
      require ``credential_id`` and a ``group``/``owner`` in ``sync_options``;
      unknown or ambiguous names are rejected with a 400. When a credential
      and group are available the listing also cross-checks numeric entries:
      a numeric entry matching a listed project *name* is resolved as a name
      (so a project literally named ``007`` is not coerced to project id 7),
      and only entries matching no listed name keep id semantics. Without a
      credential, an all-numeric ``repos`` list is the escape hatch: entries
      are used as project ids as-is, with no listing call. The effective
      ``gitlab_url`` (``sync_options.gitlab_url`` → credential ``url`` →
      ``https://gitlab.com``) is persisted into parent and child options when
      it is not the public default, so self-hosted children sync against the
      same instance used for resolution. Child name is the project's
      ``path_with_namespace`` when known.

    .. deprecated:: CHAOS-2520
        Child sync configs are deprecated. When the integration planner is
        active (``sync.migrated_trigger_routing_enabled`` setting), new
        integrations must use the integration/source/dataset model instead.
        This endpoint will create the parent config only (zero children) when
        the planner flag is enabled.
    """
    current_count = await _active_repo_count_for_batch_limit(
        session, org_id, payload.provider
    )
    new_count = len(payload.repos)

    def _check_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), current_count + new_count)

    allowed, reason = await session.run_sync(_check_limit)
    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=reason or f"Repo limit exceeded (adding {new_count} repos)",
        )

    provider = payload.provider.lower()

    parent_options = dict(payload.sync_options)
    parent_options.pop("repo", None)  # parent has no single repo
    if payload.schedule_cron is not None:
        parent_options["schedule_cron"] = payload.schedule_cron
    if payload.timezone is not None:
        parent_options["timezone"] = payload.timezone
    if payload.initial_sync_depth is not None:
        parent_options["initial_sync_depth"] = payload.initial_sync_depth

    # Resolve GitLab repos (and the effective instance URL) before creating
    # the parent so a self-hosted gitlab_url derived from the credential is
    # persisted into both parent and child options — otherwise children with
    # a valid project_id would later sync against the gitlab.com default.
    gitlab_projects: dict[str, tuple[int, str]] = {}
    if provider == "gitlab" and payload.repos:
        gitlab_projects, effective_gitlab_url = await _resolve_gitlab_batch_projects(
            session, org_id, payload
        )
        if effective_gitlab_url != DEFAULT_GITLAB_URL:
            parent_options["gitlab_url"] = effective_gitlab_url

    integration = Integration(
        org_id=org_id,
        provider=payload.provider,
        credential_id=uuid.UUID(payload.credential_id)
        if payload.credential_id
        else None,
        name=payload.name,
        config=parent_options,
        is_active=True,
        schedule_cron=payload.schedule_cron,
        timezone=payload.timezone,
    )
    session.add(integration)
    await session.flush()

    parent = SyncConfiguration(
        name=payload.name,
        provider=payload.provider,
        org_id=org_id,
        credential_id=uuid.UUID(payload.credential_id)
        if payload.credential_id
        else None,
        sync_targets=payload.sync_targets,
        sync_options=parent_options,
        is_active=True,
        migrated_integration_id=integration.id,
    )
    session.add(parent)
    await session.flush()

    source_rows = _planner_source_rows(
        payload,
        parent_options,
        gitlab_projects,
        org_id,
        integration.id,
        parent.id,
    )
    dataset_rows = [
        IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key=dataset_key,
            is_enabled=True,
            options={"legacy_targets": list(payload.sync_targets)},
        )
        for dataset_key in _planner_dataset_keys(payload.provider, payload.sync_targets)
    ]
    session.add_all([*source_rows, *dataset_rows])

    await _upsert_scheduled_job(session, parent, org_id)
    await session.flush()

    return SyncConfigBatchResponse(
        parent=_sync_config_to_response(parent, children_count=0),
        children=[],
        total_created=0,
    )


@router.post("/sync-configs", response_model=SyncConfigResponse, status_code=201)
async def create_sync_config(
    payload: SyncConfigCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)

    # Fix 1 (HIGH): Enforce repo limit before creating a new sync config.
    existing_configs = await svc.list_all(active_only=True)
    current_count = len(existing_configs)

    def _check_repo_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_repo_limit(uuid.UUID(org_id), current_count + 1)

    allowed, reason = await session.run_sync(_check_repo_limit)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason or "Repo limit exceeded")

    # Fix 5 (LOW): Validate initial_sync_depth against tier limits.
    sync_options = _sync_options_with_top_level_fields(
        payload.sync_options,
        schedule_cron=payload.schedule_cron,
        timezone=payload.timezone,
        initial_sync_depth=payload.initial_sync_depth,
    )

    initial_sync_depth = sync_options.get("initial_sync_depth")
    if initial_sync_depth is not None:

        def _check_backfill_depth(sync_session) -> tuple[bool, str | None]:
            tier_svc = TierLimitService(sync_session)
            return tier_svc.check_backfill_limit(
                uuid.UUID(org_id), int(initial_sync_depth)
            )

        depth_allowed, depth_reason = await session.run_sync(_check_backfill_depth)
        if not depth_allowed:
            raise HTTPException(
                status_code=403,
                detail=depth_reason or "initial_sync_depth exceeds tier limit",
            )

    # Fix 3 (MEDIUM) & Fix 4 (MEDIUM): Validate schedule_cron interval and gate
    # scheduled jobs behind the "scheduled_jobs" feature (Team+ only).
    schedule_cron = sync_options.get("schedule_cron")
    if schedule_cron:
        # Fix 4: Gate scheduled_jobs feature — Community tier cannot set schedules.
        async def _check_scheduled_jobs_feature(
            payload: SyncConfigCreate = payload,
            session: AsyncSession = session,
            org_id: str = org_id,
        ) -> None:
            from dev_health_ops.licensing.gating import _check_org_feature_async

            feature = "scheduled_jobs"
            if not await _check_org_feature_async(
                feature, {"session": session, "org_id": org_id}
            ):
                from dev_health_ops.licensing import has_feature

                if not has_feature(feature, log_denial=False):
                    raise HTTPException(
                        status_code=403,
                        detail="scheduled_jobs feature requires Team tier or higher",
                    )

        await _check_scheduled_jobs_feature()

        # Fix 3: Validate the cron interval against the tier's min_sync_interval_hours.
        try:
            itr = Croniter(schedule_cron)
            next1 = itr.get_next(float)
            next2 = itr.get_next(float)
            interval_hours = (next2 - next1) / 3600.0
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid cron expression: {exc}"
            )

        def _get_min_interval(sync_session) -> float | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")
            return float(val) if val is not None else None

        min_interval = await session.run_sync(_get_min_interval)
        if min_interval is not None and interval_hours < min_interval:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Sync interval {interval_hours:.2f}h is below the minimum "
                    f"{min_interval}h allowed for your tier"
                ),
            )

    config = await svc.create(
        name=payload.name,
        provider=payload.provider,
        sync_targets=payload.sync_targets,
        sync_options=sync_options,
        credential_id=payload.credential_id,
    )
    await _upsert_scheduled_job(session, config, org_id)
    await session.flush()
    return _sync_config_to_response(config)


@router.get("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def get_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    return _sync_config_to_response(config)


@router.patch("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def update_sync_config(
    config_id: str,
    payload: SyncConfigUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    # Fix 3 (MEDIUM) & Fix 4 (MEDIUM): Validate schedule_cron when updating sync_options.
    # PATCH semantics for schedule fields: an explicitly provided null clears the
    # stored value, while an omitted field leaves it untouched. Top-level fields
    # own these keys and override any (possibly stale) copies nested inside
    # payload.sync_options, so a stale client payload can never resurrect an old
    # schedule.
    provided_fields = payload.model_fields_set
    top_level_schedule_fields = {
        "schedule_cron": payload.schedule_cron,
        "timezone": payload.timezone,
        "initial_sync_depth": payload.initial_sync_depth,
    }
    sync_options = dict(payload.sync_options or {})
    cleared_keys: set[str] = set()
    for key, value in top_level_schedule_fields.items():
        if key not in provided_fields:
            continue
        if value is None:
            cleared_keys.add(key)
            sync_options.pop(key, None)
        else:
            sync_options[key] = value
    sync_options_provided = payload.sync_options is not None or bool(
        provided_fields & top_level_schedule_fields.keys()
    )

    schedule_cron = sync_options.get("schedule_cron")
    if schedule_cron:
        # Fix 4: Gate scheduled_jobs feature — Community tier cannot set schedules.
        from dev_health_ops.licensing.gating import _check_org_feature_async

        feature = "scheduled_jobs"
        if not await _check_org_feature_async(
            feature, {"session": session, "org_id": org_id}
        ):
            from dev_health_ops.licensing import has_feature

            if not has_feature(feature, log_denial=False):
                raise HTTPException(
                    status_code=403,
                    detail="scheduled_jobs feature requires Team tier or higher",
                )

        # Fix 3: Validate the cron interval against the tier's min_sync_interval_hours.
        try:
            itr = Croniter(schedule_cron)
            next1 = itr.get_next(float)
            next2 = itr.get_next(float)
            interval_hours = (next2 - next1) / 3600.0
        except Exception as exc:
            raise HTTPException(
                status_code=422, detail=f"Invalid cron expression: {exc}"
            )

        def _get_min_interval(sync_session) -> float | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "min_sync_interval_hours")
            return float(val) if val is not None else None

        min_interval = await session.run_sync(_get_min_interval)
        if min_interval is not None and interval_hours < min_interval:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Sync interval {interval_hours:.2f}h is below the minimum "
                    f"{min_interval}h allowed for your tier"
                ),
            )

    mutable_config = cast(_MutableSyncConfiguration, config)
    if payload.sync_targets is not None:
        mutable_config.sync_targets = payload.sync_targets
    if sync_options_provided:
        merged_options = {
            **dict(getattr(config, "sync_options") or {}),
            **sync_options,
        }
        for key in cleared_keys:
            merged_options.pop(key, None)
        mutable_config.sync_options = merged_options
    if payload.is_active is not None:
        mutable_config.is_active = payload.is_active
    await session.flush()
    updated = config

    await _upsert_scheduled_job(session, updated, org_id)

    # Cascade shared settings to children when updating a parent config
    if getattr(updated, "parent_id") is None:
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.parent_id == getattr(updated, "id")
        )
        result = await session.execute(stmt)
        children = result.scalars().all()
        for child in children:
            mutable_child = cast(_MutableSyncConfiguration, child)
            if payload.sync_targets is not None:
                mutable_child.sync_targets = payload.sync_targets
            if payload.is_active is not None:
                mutable_child.is_active = payload.is_active
            # Propagate schedule/timezone/depth from sync_options if provided
            if sync_options_provided:
                child_sync_options = dict(getattr(child, "sync_options") or {})
                child_changed = False
                for key in ("schedule_cron", "timezone", "initial_sync_depth"):
                    if key in cleared_keys:
                        if key in child_sync_options:
                            del child_sync_options[key]
                            child_changed = True
                    elif key in sync_options:
                        child_sync_options[key] = sync_options[key]
                        child_changed = True
                if child_changed:
                    mutable_child.sync_options = child_sync_options
        if children:
            for child in children:
                await _upsert_scheduled_job(session, child, org_id)
            await session.flush()

    return _sync_config_to_response(updated)


@router.delete("/sync-configs/{config_id}", status_code=204)
async def delete_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> None:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    await svc.delete(
        str(getattr(config, "name")), provider=str(getattr(config, "provider"))
    )


@router.post("/sync-configs/{config_id}/trigger", status_code=202)
async def trigger_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    if str(getattr(config, "org_id", "")) != org_id:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    if not bool(getattr(config, "is_active", False)):
        raise HTTPException(
            status_code=409,
            detail="Sync configuration is paused and cannot be triggered",
        )

    # Fix 6 (LOW-MEDIUM): Check work items count against tier limit before triggering.
    if "work-items" in (config.sync_targets or []):

        def _get_max_work_items(sync_session) -> int | None:
            tier_svc = TierLimitService(sync_session)
            val = tier_svc.get_limit(uuid.UUID(org_id), "max_work_items")
            return int(val) if val is not None else None

        max_work_items = await session.run_sync(_get_max_work_items)
        if max_work_items is not None:
            try:
                import os

                from dev_health_ops.api.queries.client import (
                    get_global_client,
                    query_dicts,
                )

                ch_uri = os.getenv("CLICKHOUSE_URI", "")
                if ch_uri:
                    client = await get_global_client(ch_uri)
                    rows = await query_dicts(
                        client,
                        "SELECT count() AS cnt FROM work_items WHERE org_id = %(org_id)s",
                        {"org_id": org_id},
                    )
                    current_count = int((rows[0].get("cnt") or 0) if rows else 0)
                    if current_count >= max_work_items:
                        raise HTTPException(
                            status_code=403,
                            detail=(
                                f"Work items limit exceeded: {current_count}/{max_work_items}. "
                                "Upgrade your tier to sync more work items."
                            ),
                        )
            except HTTPException:
                raise
            except Exception:
                # ClickHouse unavailable — allow the sync to proceed rather than block it.
                pass

    plan_request = await session.run_sync(
        lambda sync_session: planner_request_for_config_if_routed(
            sync_session, config, triggered_by="manual", mode="incremental"
        )
    )
    if plan_request is not None:
        try:
            plan = await session.run_sync(
                lambda sync_session: plan_sync_run(sync_session, plan_request)
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        await session.commit()
        try:
            getattr(dispatch_sync_run, "apply_async")(
                args=(plan.sync_run_id,), queue="sync"
            )
        except Exception as exc:
            await session.run_sync(
                lambda s: mark_sync_run_failed(
                    s, plan.sync_run_id, "dispatch enqueue failed"
                )
            )
            raise HTTPException(
                status_code=503, detail=f"Task queue unavailable: {exc}"
            )
        return {
            "status": "triggered",
            "config_id": str(config.id),
            "sync_run_id": plan.sync_run_id,
            "total_units": plan.total_units,
        }

    try:
        from dev_health_ops.workers.sync_tasks import (
            dispatch_batch_sync,
            run_sync_config,
        )

        # Create a PENDING JobRun synchronously so the UI shows status immediately.
        # Ensure the ScheduledJob row exists first (worker also does this, but we
        # need the job_id to create the JobRun).
        pending_run_id: str = await session.run_sync(
            lambda sync_session: _ensure_pending_sync_job_run(
                sync_session, config, org_id, "manual"
            )
        )
        await session.commit()

        is_batch = _is_batch_eligible(config)
        task = dispatch_batch_sync if is_batch else run_sync_config
        # Per-provider queue routing (CHAOS-2299): an explicit apply_async
        # queue overrides the task decorator's queue="sync" default.
        try:
            result = getattr(task, "apply_async")(
                kwargs={
                    "config_id": str(config.id),
                    "org_id": str(config.org_id),
                    "triggered_by": "manual",
                    "pending_run_id": pending_run_id,
                },
                queue=sync_queue_for_provider(str(config.provider or "")),
            )
        except Exception as e:
            error_message = f"dispatch enqueue failed: {e}"
            await session.run_sync(
                lambda sync_session: _mark_job_run_failed(
                    sync_session, pending_run_id, error_message
                )
            )
            await session.commit()
            raise HTTPException(status_code=503, detail=f"Task queue unavailable: {e}")
        return {
            "status": "triggered",
            "config_id": str(config.id),
            "task_id": result.id,
            "run_id": pending_run_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Task queue unavailable: {e}")


@router.post("/sync-configs/{config_id}/backfill", status_code=202)
async def trigger_sync_config_backfill(
    config_id: str,
    payload: BackfillRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    requested_days = (payload.before - payload.since).days

    def _check_backfill_limit(sync_session) -> tuple[bool, str | None]:
        tier_svc = TierLimitService(sync_session)
        return tier_svc.check_backfill_limit(uuid.UUID(org_id), requested_days)

    allowed, reason = await session.run_sync(_check_backfill_limit)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason or "Backfill not allowed")

    from dev_health_ops.backfill.chunker import chunk_date_range
    from dev_health_ops.models.backfill import BackfillJob as BackfillJobModel

    windows = chunk_date_range(since=payload.since, before=payload.before, chunk_days=7)
    fanout_env = os.getenv("SYNC_FANOUT_BACKFILL", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    planner_backfill_request = await session.run_sync(
        lambda sync_session: planner_request_for_config_if_routed(
            sync_session, config, triggered_by="backfill", mode="backfill"
        )
    )
    if planner_backfill_request is None and fanout_env:
        planner_backfill_request = plan_request_for_config(
            config, triggered_by="backfill", mode="backfill"
        )
    fanout_backfill = planner_backfill_request is not None
    backfill_job = BackfillJobModel(
        org_id=org_id,
        sync_config_id=uuid.UUID(config_id),
        status="pending",
        since_date=payload.since,
        before_date=payload.before,
        total_chunks=0 if fanout_backfill else len(windows),
    )
    session.add(backfill_job)
    await session.flush()
    backfill_job_id = str(backfill_job.id)

    try:
        from dev_health_ops.workers.sync_tasks import run_backfill

        # For the legacy path only: create a PENDING JobRun so the sync-activity
        # list shows the backfill immediately (CHAOS-2536).  The fan-out path
        # already produces a visible SyncRun, so we skip this there.
        pending_run_id: str | None = None
        if not fanout_backfill:
            pending_run_id = await session.run_sync(
                lambda sync_session: _ensure_pending_sync_job_run(
                    sync_session, config, org_id, "backfill"
                )
            )
            await session.commit()

        result = getattr(run_backfill, "delay")(
            sync_config_id=str(config.id),
            since=payload.since.isoformat(),
            before=payload.before.isoformat(),
            org_id=org_id,
            backfill_job_id=backfill_job_id,
            pending_run_id=pending_run_id,
        )
        backfill_job.celery_task_id = result.id
        await session.flush()
        return {
            "status": "accepted",
            "config_id": str(config.id),
            "task_id": result.id,
            "backfill_job_id": backfill_job_id,
            "mode": "fanout" if fanout_backfill else "legacy",
            "since": payload.since.isoformat(),
            "before": payload.before.isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Task queue unavailable: {e}")


@router.get("/sync-configs/{config_id}/jobs", response_model=list[JobRunResponse])
async def list_sync_config_jobs(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[JobRunResponse]:
    svc = SyncConfigurationService(session, org_id)
    existing = await svc.get_by_id(config_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    scheduled_job_id_col = getattr(ScheduledJob, "id")
    scheduled_job_org_id = getattr(ScheduledJob, "org_id")
    scheduled_job_sync_config_id = getattr(ScheduledJob, "sync_config_id")
    scheduled_job_type = getattr(ScheduledJob, "job_type")
    job_stmt = select(scheduled_job_id_col).where(
        scheduled_job_org_id == org_id,
        scheduled_job_sync_config_id == uuid.UUID(config_id),
        scheduled_job_type == "sync",
    )
    job_result = await session.execute(job_stmt)
    job_ids = list(job_result.scalars().all())

    if not job_ids:
        return []

    job_run_job_id = getattr(JobRun, "job_id")
    job_run_created_at = getattr(JobRun, "created_at")
    runs_stmt = (
        select(JobRun)
        .where(job_run_job_id.in_(job_ids))
        .order_by(job_run_created_at.desc())
        .limit(50)
    )
    runs_result = await session.execute(runs_stmt)
    runs = list(runs_result.scalars().all())

    return [_job_run_response(run) for run in runs]


@router.get("/backfill-jobs")
async def list_backfill_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.schemas.backfill import BackfillJobListResponse
    from dev_health_ops.api.services.backfill import BackfillJobService

    svc = BackfillJobService(session, org_id)
    jobs, total = await svc.list_jobs(limit=limit, offset=offset)
    items = []
    for job in jobs:
        run_counts = await _backfill_job_run_counts(session, job)
        items.append(_backfill_job_response(job, run_counts))
    return BackfillJobListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/backfill-jobs/{job_id}")
async def get_backfill_job(
    job_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.services.backfill import BackfillJobService

    svc = BackfillJobService(session, org_id)
    job = await svc.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    run_counts = await _backfill_job_run_counts(session, job)
    return _backfill_job_response(job, run_counts)
