from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast

from dev_health_ops.metrics.testops_schemas import JobRunRow, PipelineRunExtendedRow

from .base import SQLAlchemyStoreMixinProtocol


class TestOpsCICDMixin:
    async def insert_testops_pipeline_runs(
        self: SQLAlchemyStoreMixinProtocol,
        runs: list[PipelineRunExtendedRow],
    ) -> None:
        if not runs:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in runs:
            row = {
                "repo_id": item.get("repo_id"),
                "run_id": item.get("run_id"),
                "pipeline_name": item.get("pipeline_name"),
                "provider": item.get("provider", ""),
                "status": item.get("status"),
                "queued_at": item.get("queued_at"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "duration_seconds": item.get("duration_seconds"),
                "queue_seconds": item.get("queue_seconds"),
                "retry_count": item.get("retry_count", 0),
                "cancel_reason": item.get("cancel_reason"),
                "trigger_source": item.get("trigger_source"),
                "commit_hash": item.get("commit_hash"),
                "branch": item.get("branch"),
                "pr_number": item.get("pr_number"),
                "team_id": item.get("team_id"),
                "service_id": item.get("service_id"),
                "org_id": item.get("org_id", ""),
                "last_synced": item.get("last_synced") or synced_at_default,
            }
            rows.append(row)

        await self._upsert_many(
            cast(Any, self._ci_pipeline_runs_table),
            rows,
            conflict_columns=["repo_id", "run_id"],
            update_columns=[
                "pipeline_name",
                "provider",
                "status",
                "queued_at",
                "started_at",
                "finished_at",
                "duration_seconds",
                "queue_seconds",
                "retry_count",
                "cancel_reason",
                "trigger_source",
                "commit_hash",
                "branch",
                "pr_number",
                "team_id",
                "service_id",
                "org_id",
                "last_synced",
            ],
        )

    async def insert_testops_job_runs(
        self: SQLAlchemyStoreMixinProtocol,
        jobs: list[JobRunRow],
    ) -> None:
        if not jobs:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        for item in jobs:
            row = {
                "repo_id": item.get("repo_id"),
                "run_id": item.get("run_id"),
                "job_id": item.get("job_id"),
                "job_name": item.get("job_name"),
                "stage": item.get("stage"),
                "status": item.get("status"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "duration_seconds": item.get("duration_seconds"),
                "runner_type": item.get("runner_type"),
                "retry_attempt": item.get("retry_attempt", 0),
                "org_id": item.get("org_id", ""),
                "last_synced": item.get("last_synced") or synced_at_default,
            }
            rows.append(row)

        await self._upsert_many(
            cast(Any, self._ci_job_runs_table),
            rows,
            conflict_columns=["repo_id", "run_id", "job_id"],
            update_columns=[
                "job_name",
                "stage",
                "status",
                "started_at",
                "finished_at",
                "duration_seconds",
                "runner_type",
                "retry_attempt",
                "org_id",
                "last_synced",
            ],
        )


async def clickhouse_insert_testops_pipeline_runs(
    self: Any,
    runs: list[PipelineRunExtendedRow],
) -> None:
    if not runs:
        return
    synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
    rows: list[dict[str, Any]] = []
    for item in runs:
        rows.append(
            {
                "repo_id": self._normalize_uuid(item.get("repo_id")),
                "run_id": str(item.get("run_id")),
                "pipeline_name": item.get("pipeline_name"),
                "provider": item.get("provider", ""),
                "status": item.get("status"),
                "queued_at": self._normalize_datetime(item.get("queued_at")),
                "started_at": self._normalize_datetime(item.get("started_at")),
                "finished_at": self._normalize_datetime(item.get("finished_at")),
                "duration_seconds": item.get("duration_seconds"),
                "queue_seconds": item.get("queue_seconds"),
                "retry_count": item.get("retry_count", 0),
                "cancel_reason": item.get("cancel_reason"),
                "trigger_source": item.get("trigger_source"),
                "commit_hash": item.get("commit_hash"),
                "branch": item.get("branch"),
                "pr_number": item.get("pr_number"),
                "team_id": item.get("team_id"),
                "service_id": item.get("service_id"),
                "org_id": item.get("org_id", ""),
                "last_synced": self._normalize_datetime(
                    item.get("last_synced") or synced_at_default
                ),
            }
        )

    await self._insert_rows(
        "ci_pipeline_runs",
        [
            "repo_id",
            "run_id",
            "pipeline_name",
            "provider",
            "status",
            "queued_at",
            "started_at",
            "finished_at",
            "duration_seconds",
            "queue_seconds",
            "retry_count",
            "cancel_reason",
            "trigger_source",
            "commit_hash",
            "branch",
            "pr_number",
            "team_id",
            "service_id",
            "org_id",
            "last_synced",
        ],
        rows,
    )


async def clickhouse_insert_testops_job_runs(
    self: Any,
    jobs: list[JobRunRow],
) -> None:
    if not jobs:
        return
    synced_at_default = self._normalize_datetime(datetime.now(timezone.utc))
    rows: list[dict[str, Any]] = []
    for item in jobs:
        rows.append(
            {
                "repo_id": self._normalize_uuid(item.get("repo_id")),
                "run_id": str(item.get("run_id")),
                "job_id": str(item.get("job_id")),
                "job_name": str(item.get("job_name")),
                "stage": item.get("stage"),
                "status": item.get("status"),
                "started_at": self._normalize_datetime(item.get("started_at")),
                "finished_at": self._normalize_datetime(item.get("finished_at")),
                "duration_seconds": item.get("duration_seconds"),
                "runner_type": item.get("runner_type"),
                "retry_attempt": item.get("retry_attempt", 0),
                "org_id": item.get("org_id", ""),
                "last_synced": self._normalize_datetime(
                    item.get("last_synced") or synced_at_default
                ),
            }
        )

    await self._insert_rows(
        "ci_job_runs",
        [
            "repo_id",
            "run_id",
            "job_id",
            "job_name",
            "stage",
            "status",
            "started_at",
            "finished_at",
            "duration_seconds",
            "runner_type",
            "retry_attempt",
            "org_id",
            "last_synced",
        ],
        rows,
    )
