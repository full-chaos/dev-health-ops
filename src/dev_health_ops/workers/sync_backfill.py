from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import (
    _as_str_list,
    _credential_mapping,
    _get_db_url,
    _normalize_sync_targets,
)

logger = logging.getLogger(__name__)


def _mark_backfill_job_running(backfill_job_id: str, started_at: datetime) -> None:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.backfill import BackfillJob as BackfillJobModel

    with get_postgres_session_sync() as session:
        bf_job = (
            session.query(BackfillJobModel)
            .filter(BackfillJobModel.id == uuid.UUID(backfill_job_id))
            .one_or_none()
        )
        if bf_job:
            setattr(bf_job, "status", "running")
            setattr(bf_job, "started_at", started_at)
            session.flush()


def _update_backfill_job_counts(
    backfill_job_id: str,
    *,
    total_chunks: int | None = None,
    completed_chunks: int | None = None,
    failed_chunks: int | None = None,
    celery_task_id: str | None = None,
    status: str | None = None,
    completed_at: datetime | None = None,
    error_message: str | None = None,
) -> None:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.backfill import BackfillJob as BackfillJobModel

    with get_postgres_session_sync() as session:
        bf_job = (
            session.query(BackfillJobModel)
            .filter(BackfillJobModel.id == uuid.UUID(backfill_job_id))
            .one_or_none()
        )
        if not bf_job:
            return
        if total_chunks is not None:
            setattr(bf_job, "total_chunks", total_chunks)
        if completed_chunks is not None:
            setattr(bf_job, "completed_chunks", completed_chunks)
        if failed_chunks is not None:
            setattr(bf_job, "failed_chunks", failed_chunks)
        if celery_task_id is not None:
            setattr(bf_job, "celery_task_id", celery_task_id)
        if status is not None:
            setattr(bf_job, "status", status)
        if completed_at is not None:
            setattr(bf_job, "completed_at", completed_at)
        if error_message is not None:
            setattr(bf_job, "error_message", error_message)
        session.flush()


def _fanout_backfill_task_id(celery_task_id: str | None, sync_run_id: str) -> str:
    if celery_task_id:
        return f"{celery_task_id}|sync_run:{sync_run_id}"
    return f"sync_run:{sync_run_id}"


def _mark_sync_job_run_running(
    pending_run_id: str | None, started_at: datetime
) -> None:
    if pending_run_id is None:
        return
    try:
        from dev_health_ops.db import get_postgres_session_sync
        from dev_health_ops.models.settings import JobRun, JobRunStatus

        with get_postgres_session_sync() as session:
            run = (
                session.query(JobRun)
                .filter(JobRun.id == uuid.UUID(pending_run_id))
                .one_or_none()
            )
            if run:
                setattr(run, "status", JobRunStatus.RUNNING.value)
                setattr(run, "started_at", started_at)
                session.flush()
    except Exception:
        logger.debug("Failed to mark sync job run running: %s", pending_run_id)


def _mark_sync_job_run_success(
    pending_run_id: str | None,
    completed_at: datetime,
    result: dict[str, Any] | None = None,
) -> None:
    if pending_run_id is None:
        return
    try:
        from dev_health_ops.db import get_postgres_session_sync
        from dev_health_ops.models.settings import JobRun, JobRunStatus

        with get_postgres_session_sync() as session:
            run = (
                session.query(JobRun)
                .filter(JobRun.id == uuid.UUID(pending_run_id))
                .one_or_none()
            )
            if run:
                setattr(run, "status", JobRunStatus.SUCCESS.value)
                setattr(run, "completed_at", completed_at)
                if result is not None:
                    current = run.result if isinstance(run.result, dict) else {}
                    setattr(run, "result", {**current, **result})
                session.flush()
    except Exception:
        logger.debug("Failed to mark sync job run success: %s", pending_run_id)


def _mark_sync_job_run_failed(
    pending_run_id: str | None, error: str, completed_at: datetime
) -> None:
    if pending_run_id is None:
        return
    try:
        from dev_health_ops.db import get_postgres_session_sync
        from dev_health_ops.models.settings import JobRun, JobRunStatus

        with get_postgres_session_sync() as session:
            run = (
                session.query(JobRun)
                .filter(JobRun.id == uuid.UUID(pending_run_id))
                .one_or_none()
            )
            if run:
                setattr(run, "status", JobRunStatus.FAILED.value)
                setattr(run, "error", error)
                setattr(run, "completed_at", completed_at)
                session.flush()
    except Exception:
        logger.debug("Failed to mark sync job run failed: %s", pending_run_id)


def _mark_sync_job_run_cancelled(
    pending_run_id: str | None, error: str, completed_at: datetime
) -> None:
    if pending_run_id is None:
        return
    try:
        from dev_health_ops.db import get_postgres_session_sync
        from dev_health_ops.models.settings import JobRun, JobRunStatus

        with get_postgres_session_sync() as session:
            run = (
                session.query(JobRun)
                .filter(JobRun.id == uuid.UUID(pending_run_id))
                .one_or_none()
            )
            if run:
                setattr(run, "status", JobRunStatus.CANCELLED.value)
                setattr(run, "error", error)
                setattr(run, "completed_at", completed_at)
                session.flush()
    except Exception:
        logger.debug("Failed to mark sync job run cancelled: %s", pending_run_id)


@celery_app.task(
    bind=True,
    max_retries=3,
    queue="backfill",
    name="dev_health_ops.workers.tasks.run_backfill",
)
def run_backfill(
    self,
    sync_config_id: str,
    since: str,
    before: str,
    org_id: str,
    backfill_job_id: str | None = None,
    pending_run_id: str | None = None,
) -> dict:
    from dev_health_ops.backfill.runner import (
        run_backfill_for_config,
        run_backfill_via_planner,
    )
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import IntegrationCredential, SyncConfiguration
    from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed

    sync_config_uuid = uuid.UUID(sync_config_id)
    started_at = datetime.now(timezone.utc)

    try:
        provider = ""
        sync_targets: list[str] = []
        use_fanout = False
        planner_integration_id = ""
        planner_source_ids: tuple[str, ...] | None = None
        planner_dataset_keys: tuple[str, ...] | None = None
        with get_postgres_session_sync() as session:
            config = (
                session.query(SyncConfiguration)
                .filter(
                    SyncConfiguration.id == sync_config_uuid,
                    SyncConfiguration.org_id == org_id,
                )
                .one_or_none()
            )
            if config is None:
                raise ValueError(f"Sync configuration not found: {sync_config_id}")
            if not bool(config.is_active):
                completed_at = datetime.now(timezone.utc)
                if backfill_job_id:
                    _update_backfill_job_counts(
                        backfill_job_id,
                        status="cancelled",
                        completed_at=completed_at,
                        error_message="Sync configuration is paused",
                    )
                _mark_sync_job_run_cancelled(
                    pending_run_id, "Sync configuration is paused", completed_at
                )
                return {"status": "skipped", "reason": "sync_config_inactive"}

            provider = str(config.provider or "").strip().lower()
            sync_targets = _normalize_sync_targets(
                provider,
                _as_str_list(config.sync_targets),
            )

            credentials: dict[str, Any] | None = None
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
                credentials = _credential_mapping(credential)

            plan_req = planner_request_for_config_if_routed(
                session, config, triggered_by="backfill", mode="backfill"
            )
            if plan_req is not None:
                use_fanout = True
                planner_integration_id = plan_req.integration_id
                planner_source_ids = plan_req.source_ids
                planner_dataset_keys = plan_req.dataset_keys

        if backfill_job_id:
            _mark_backfill_job_running(backfill_job_id, started_at)
        _mark_sync_job_run_running(pending_run_id, started_at)

        since_date = date.fromisoformat(since)
        before_date = date.fromisoformat(before)

        if use_fanout:
            result_payload = run_backfill_via_planner(
                planner_integration_id,
                since_date,
                before_date,
                org_id=org_id,
                source_ids=planner_source_ids,
                dataset_keys=planner_dataset_keys,
                triggered_by="backfill",
            )
            if backfill_job_id:
                unit_count = int(result_payload.get("unit_count") or 0)
                status = "completed" if unit_count == 0 else "running"
                _update_backfill_job_counts(
                    backfill_job_id,
                    total_chunks=unit_count,
                    completed_chunks=0,
                    failed_chunks=0,
                    celery_task_id=_fanout_backfill_task_id(
                        getattr(getattr(self, "request", None), "id", None),
                        str(result_payload["sync_run_id"]),
                    ),
                    status=status,
                    completed_at=(
                        datetime.now(timezone.utc) if status == "completed" else None
                    ),
                )
            _mark_sync_job_run_success(
                pending_run_id,
                datetime.now(timezone.utc),
                {"sync_run_id": str(result_payload["sync_run_id"])},
            )
            return {
                "status": "success",
                "result": result_payload,
            }

        def _backfill_progress(
            chunk_idx: int, total: int, w_since: date, w_before: date
        ) -> None:
            if not backfill_job_id:
                return
            try:
                with get_postgres_session_sync() as session:
                    from dev_health_ops.models.backfill import (
                        BackfillJob as BackfillJobModel,
                    )

                    bf_job = (
                        session.query(BackfillJobModel)
                        .filter(BackfillJobModel.id == uuid.UUID(backfill_job_id))
                        .one_or_none()
                    )
                    if bf_job:
                        setattr(bf_job, "completed_chunks", chunk_idx)
                        session.flush()
            except Exception:
                logger.debug(
                    "Failed to update backfill progress for chunk %d/%d",
                    chunk_idx,
                    total,
                )

        result_payload = run_backfill_for_config(
            db_url=_get_db_url(),
            sync_config_id=sync_config_id,
            org_id=org_id,
            since=since_date,
            before=before_date,
            sink="clickhouse",
            chunk_days=7,
            progress_cb=_backfill_progress,
            credentials=credentials,
        )

        completed_at = datetime.now(timezone.utc)
        if backfill_job_id:
            try:
                with get_postgres_session_sync() as session:
                    from dev_health_ops.models.backfill import (
                        BackfillJob as BackfillJobModel,
                    )

                    bf_job = (
                        session.query(BackfillJobModel)
                        .filter(BackfillJobModel.id == uuid.UUID(backfill_job_id))
                        .one_or_none()
                    )
                    if bf_job:
                        setattr(bf_job, "status", "completed")
                        setattr(bf_job, "completed_at", completed_at)
                        session.flush()
            except Exception:
                logger.debug(
                    "Failed to mark backfill job completed: %s", backfill_job_id
                )
        _mark_sync_job_run_success(pending_run_id, completed_at)

        try:
            from dev_health_ops.workers.sync_runtime import _dispatch_post_sync_tasks

            _dispatch_post_sync_tasks(
                provider=provider,
                sync_targets=sync_targets,
                org_id=org_id,
                metrics_day=before_date.isoformat(),
                metrics_backfill_days=(before_date - since_date).days + 1,
                from_date=since_date.isoformat(),
                to_date=before_date.isoformat(),
                work_graph_from_date=datetime.combine(
                    since_date,
                    time.min,
                    tzinfo=timezone.utc,
                ).isoformat(),
                work_graph_to_date=datetime.combine(
                    before_date + timedelta(days=1),
                    time.min,
                    tzinfo=timezone.utc,
                ).isoformat(),
            )
        except Exception:
            logger.exception(
                "Failed to dispatch post-backfill tasks: sync_config_id=%s org_id=%s",
                sync_config_id,
                org_id,
            )

        return {
            "status": "success",
            "result": result_payload,
        }
    except Exception as exc:
        logger.exception(
            "Backfill task failed: sync_config_id=%s org_id=%s error=%s",
            sync_config_id,
            org_id,
            exc,
        )
        completed_at = datetime.now(timezone.utc)

        if backfill_job_id:
            try:
                with get_postgres_session_sync() as session:
                    from dev_health_ops.models.backfill import (
                        BackfillJob as BackfillJobModel,
                    )

                    bf_job = (
                        session.query(BackfillJobModel)
                        .filter(BackfillJobModel.id == uuid.UUID(backfill_job_id))
                        .one_or_none()
                    )
                    if bf_job:
                        setattr(bf_job, "status", "failed")
                        setattr(bf_job, "error_message", str(exc))
                        setattr(bf_job, "completed_at", completed_at)
                        session.flush()
            except Exception:
                logger.debug("Failed to mark backfill job failed: %s", backfill_job_id)
        _mark_sync_job_run_failed(pending_run_id, str(exc), completed_at)

        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
