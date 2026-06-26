"""CHAOS-2677: first-run setup status endpoint (contract C2).

``GET /api/v1/admin/setup/status`` powers the dashboard
"value-or-precise-blocker" surface. It is a read-only projection over the
semantic layer (integration credentials + sync configuration + planner job
runs) that distinguishes the four first-run states:

* **not-connected** — no active integration credential.
* **connected-no-config** — credentials exist but no sync config yet.
* **config-failed** — a sync config exists and its last/most-recent run failed.
* **sync-running** — a planner job run is pending or running.

The endpoint never starts work and never persists; it only reflects the
current state so the frontend can route the user to the precise next action.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas_flat import SetupStatusResponse
from dev_health_ops.api.services.configuration import (
    IntegrationCredentialsService,
    SyncConfigurationService,
)
from dev_health_ops.models.integrations import IntegrationSource
from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncConfiguration,
)

from .common import get_session

router = APIRouter()

_REPO_PROVIDERS = {"github", "gitlab"}

SyncStatus = Literal["none", "pending", "running", "partial", "complete", "failed"]
NextAction = Literal[
    "connect_integration",
    "select_repositories",
    "create_sync_config",
    "start_sync",
    "complete",
]


def _select_primary_config(
    configs: list[SyncConfiguration],
) -> SyncConfiguration | None:
    """Pick the most relevant parent (non-child) sync config.

    Prefer active configs, then the most recently created one, so the status
    surface tracks the config the operator most likely just set up.
    """
    parents = [c for c in configs if getattr(c, "parent_id") is None]
    if not parents:
        return None

    def _key(config: SyncConfiguration) -> tuple[int, str]:
        created = getattr(config, "created_at", None)
        return (1 if bool(getattr(config, "is_active")) else 0, str(created or ""))

    return sorted(parents, key=_key, reverse=True)[0]


async def _selected_repository_count(
    session: AsyncSession, org_id: str, integration_id: object
) -> int:
    if integration_id is None:
        return 0
    count = await session.scalar(
        select(func.count(IntegrationSource.id)).where(
            IntegrationSource.org_id == org_id,
            IntegrationSource.integration_id == integration_id,
            IntegrationSource.is_enabled.is_(True),
        )
    )
    return int(count or 0)


async def _latest_job_run(
    session: AsyncSession, org_id: str, config_id: object
) -> JobRun | None:
    stmt = (
        select(JobRun)
        .join(ScheduledJob, JobRun.job_id == ScheduledJob.id)
        .where(
            ScheduledJob.org_id == org_id,
            ScheduledJob.sync_config_id == config_id,
        )
        .order_by(JobRun.created_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalars().first()


@router.get("/setup/status", response_model=SetupStatusResponse)
async def get_setup_status(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> SetupStatusResponse:
    cred_svc = IntegrationCredentialsService(session, org_id)
    credentials = await cred_svc.list_all(active_only=True)
    providers = sorted({str(getattr(c, "provider")) for c in credentials})
    has_integration = bool(providers)

    sync_svc = SyncConfigurationService(session, org_id)
    configs = await sync_svc.list_all()
    primary = _select_primary_config(configs)
    has_sync_config = primary is not None

    sync_config_id: str | None = None
    sync_status: SyncStatus = "none"
    first_sync_started = False
    selected_repositories_count = 0
    last_sync_error: str | None = None
    can_start_sync = False
    has_repo_selection = True

    if primary is not None:
        sync_config_id = str(getattr(primary, "id"))
        provider = str(getattr(primary, "provider")).lower()
        sync_options = dict(getattr(primary, "sync_options") or {})
        selected_repositories_count = await _selected_repository_count(
            session, org_id, getattr(primary, "integration_id")
        )
        if provider in _REPO_PROVIDERS:
            has_repo_selection = selected_repositories_count > 0 or bool(
                sync_options.get("all_repos")
            )
        else:
            has_repo_selection = True

        latest = await _latest_job_run(session, org_id, getattr(primary, "id"))
        if latest is not None:
            first_sync_started = True
            status_int = int(getattr(latest, "status"))
            run_result = getattr(latest, "result")
            run_result = run_result if isinstance(run_result, dict) else {}
            sync_run_status = str(run_result.get("sync_run_status") or "")
            if status_int == JobRunStatus.PENDING.value:
                sync_status = "pending"
            elif status_int == JobRunStatus.RUNNING.value:
                sync_status = "running"
            elif status_int == JobRunStatus.SUCCESS.value:
                sync_status = "complete"
            elif status_int in (
                JobRunStatus.FAILED.value,
                JobRunStatus.CANCELLED.value,
            ):
                sync_status = "failed"
            else:
                sync_status = "running"
            if sync_run_status in ("partial_failed", "partial"):
                sync_status = "partial"
            if sync_status == "failed":
                last_sync_error = getattr(latest, "error") or getattr(
                    primary, "last_sync_error"
                )
        else:
            # No planner run yet — fall back to the config-level last-sync facts
            # so a sync recorded outside the job-run anchor still surfaces.
            if getattr(primary, "last_sync_success") is True:
                sync_status = "complete"
                first_sync_started = True
            elif (
                getattr(primary, "last_sync_error")
                or getattr(primary, "last_sync_success") is False
            ):
                sync_status = "failed"
                first_sync_started = True
                last_sync_error = getattr(primary, "last_sync_error")
            else:
                sync_status = "none"

        in_flight = sync_status in ("pending", "running")
        can_start_sync = (
            bool(getattr(primary, "is_active")) and not in_flight and has_repo_selection
        )

    next_action: NextAction
    blocker: str | None = None
    if not has_integration:
        next_action = "connect_integration"
        blocker = "No integration connected"
    elif not has_sync_config:
        next_action = (
            "select_repositories"
            if any(p in _REPO_PROVIDERS for p in providers)
            else "create_sync_config"
        )
        blocker = "No sync configuration"
    elif sync_status == "failed":
        next_action = "start_sync"
        blocker = last_sync_error or "Last sync failed"
    elif sync_status in ("pending", "running", "complete", "partial"):
        next_action = "complete"
    elif not has_repo_selection:
        next_action = "select_repositories"
        blocker = "No repositories selected"
    else:
        next_action = "start_sync"

    return SetupStatusResponse(
        has_integration=has_integration,
        providers=providers,
        has_sync_config=has_sync_config,
        sync_config_id=sync_config_id,
        first_sync_started=first_sync_started,
        sync_status=sync_status,
        selected_repositories_count=selected_repositories_count,
        last_sync_error=last_sync_error,
        can_start_sync=can_start_sync,
        next_action=next_action,
        blocker=blocker,
    )
