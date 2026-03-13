from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone

from dev_health_ops.workers.celery_app import celery_app
from dev_health_ops.workers.task_utils import (
    _decrypt_credential_sync,
    _extract_provider_token,
    _get_db_url,
    _inject_provider_token,
    _resolve_env_credentials,
)

logger = logging.getLogger(__name__)


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
) -> dict:
    from dev_health_ops.backfill.runner import run_backfill_for_config
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import (
        IntegrationCredential,
        SyncConfiguration,
    )

    sync_config_uuid = uuid.UUID(sync_config_id)
    started_at = datetime.now(timezone.utc)

    try:
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

            provider = (config.provider or "").lower()
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

            token = _extract_provider_token(provider, credentials)
            if token:
                _inject_provider_token(provider, token)

        if backfill_job_id:
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
                    bf_job.status = "running"
                    bf_job.started_at = started_at
                    session.flush()

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
                        bf_job.completed_chunks = chunk_idx
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
            since=date.fromisoformat(since),
            before=date.fromisoformat(before),
            sink="clickhouse",
            chunk_days=7,
            progress_cb=_backfill_progress,
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
                        bf_job.status = "completed"
                        bf_job.completed_at = completed_at
                        session.flush()
            except Exception:
                logger.debug(
                    "Failed to mark backfill job completed: %s", backfill_job_id
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
                        bf_job.status = "failed"
                        bf_job.error_message = str(exc)
                        bf_job.completed_at = completed_at
                        session.flush()
            except Exception:
                logger.debug("Failed to mark backfill job failed: %s", backfill_job_id)

        raise self.retry(exc=exc, countdown=60 * (2**self.request.retries))
