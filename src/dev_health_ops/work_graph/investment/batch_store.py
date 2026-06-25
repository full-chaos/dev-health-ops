from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dev_health_ops.models.investment_batch import (
    InvestmentBatchItem,
    InvestmentBatchItemStatus,
    InvestmentBatchJob,
    InvestmentBatchJobStatus,
)

TERMINAL_ITEM_STATUSES = frozenset(
    {
        InvestmentBatchItemStatus.VALIDATED.value,
        InvestmentBatchItemStatus.REPAIRED.value,
        InvestmentBatchItemStatus.FALLBACK.value,
        InvestmentBatchItemStatus.REUSED.value,
        InvestmentBatchItemStatus.FAILED.value,
    }
)


@dataclass(frozen=True)
class InvestmentBatchItemSpec:
    work_unit_id: str
    component_index: int
    custom_id: str
    input_hash: str


class InvestmentBatchStore:
    def __init__(self, session: Session, org_id: str) -> None:
        self.session = session
        self.org_id = org_id

    def create_job(
        self,
        *,
        provider: str,
        model: str,
        run_id: str,
        prompt_version: str,
        contract_version: str,
        items: list[InvestmentBatchItemSpec],
        deadline_at: datetime | None = None,
        local_correlation_id: str | None = None,
    ) -> InvestmentBatchJob:
        now = datetime.now(timezone.utc)
        job = InvestmentBatchJob(
            org_id=self.org_id,
            provider=provider,
            model=model,
            status=InvestmentBatchJobStatus.CREATED.value,
            local_correlation_id=local_correlation_id or uuid.uuid4().hex,
            run_id=run_id,
            prompt_version=prompt_version,
            contract_version=contract_version,
            total_items=len(items),
            deadline_at=deadline_at,
            created_at=now,
            updated_at=now,
        )
        self.session.add(job)
        self.session.flush()
        for item in items:
            self.session.add(
                InvestmentBatchItem(
                    org_id=self.org_id,
                    job_id=job.id,
                    work_unit_id=item.work_unit_id,
                    component_index=item.component_index,
                    custom_id=item.custom_id,
                    input_hash=item.input_hash,
                    provider=provider,
                    model=model,
                    prompt_version=prompt_version,
                    contract_version=contract_version,
                    status=InvestmentBatchItemStatus.PENDING.value,
                    created_at=now,
                    updated_at=now,
                )
            )
        self.session.flush()
        return job

    def get_job(self, job_id: str | uuid.UUID) -> InvestmentBatchJob | None:
        return self.session.scalar(
            select(InvestmentBatchJob).where(
                InvestmentBatchJob.org_id == self.org_id,
                InvestmentBatchJob.id == uuid.UUID(str(job_id)),
            )
        )

    def get_job_by_correlation(
        self, local_correlation_id: str
    ) -> InvestmentBatchJob | None:
        return self.session.scalar(
            select(InvestmentBatchJob).where(
                InvestmentBatchJob.org_id == self.org_id,
                InvestmentBatchJob.local_correlation_id == local_correlation_id,
            )
        )

    def get_item_by_custom_id(
        self, *, job_id: str | uuid.UUID, custom_id: str
    ) -> InvestmentBatchItem | None:
        return self.session.scalar(
            select(InvestmentBatchItem).where(
                InvestmentBatchItem.org_id == self.org_id,
                InvestmentBatchItem.job_id == uuid.UUID(str(job_id)),
                InvestmentBatchItem.custom_id == custom_id,
            )
        )

    def find_reusable_items(
        self,
        *,
        provider: str,
        model: str,
        prompt_version: str,
        keys: list[tuple[str, int, str]],
    ) -> dict[tuple[str, int, str], InvestmentBatchItem]:
        if not keys:
            return {}
        rows = self.session.scalars(
            select(InvestmentBatchItem).where(
                InvestmentBatchItem.org_id == self.org_id,
                InvestmentBatchItem.provider == provider,
                InvestmentBatchItem.model == model,
                InvestmentBatchItem.prompt_version == prompt_version,
                InvestmentBatchItem.status.in_(
                    [
                        InvestmentBatchItemStatus.VALIDATED.value,
                        InvestmentBatchItemStatus.REPAIRED.value,
                        InvestmentBatchItemStatus.REUSED.value,
                    ]
                ),
            )
        ).all()
        wanted = set(keys)
        reusable: dict[tuple[str, int, str], InvestmentBatchItem] = {}
        for row in rows:
            key = (row.work_unit_id, row.component_index, row.input_hash)
            if key in wanted:
                reusable[key] = row
        return reusable

    def transition_job(
        self,
        job: InvestmentBatchJob,
        status: InvestmentBatchJobStatus | str,
        *,
        provider_job_id: str | None = None,
        error: str | None = None,
        provider_metadata: dict[str, Any] | None = None,
    ) -> InvestmentBatchJob:
        job.status = (
            status.value if isinstance(status, InvestmentBatchJobStatus) else status
        )
        if provider_job_id is not None:
            job.provider_job_id = provider_job_id
        if error is not None:
            job.error = error
        if provider_metadata is not None:
            job.provider_metadata = provider_metadata
        now = datetime.now(timezone.utc)
        if job.status == InvestmentBatchJobStatus.SUBMITTED.value:
            job.submitted_at = now
        if job.status in {
            InvestmentBatchJobStatus.SUCCEEDED.value,
            InvestmentBatchJobStatus.FAILED.value,
            InvestmentBatchJobStatus.CANCELLED.value,
            InvestmentBatchJobStatus.EXPIRED.value,
        }:
            job.completed_at = now
        job.updated_at = now
        self.session.flush()
        return job

    def transition_item(
        self,
        item: InvestmentBatchItem,
        status: InvestmentBatchItemStatus | str,
        *,
        provider_response: dict[str, Any] | None = None,
        provider_error: dict[str, Any] | None = None,
        audit: dict[str, Any] | None = None,
    ) -> InvestmentBatchItem:
        item.status = (
            status.value if isinstance(status, InvestmentBatchItemStatus) else status
        )
        if provider_response is not None:
            item.provider_response = provider_response
        if provider_error is not None:
            item.provider_error = provider_error
        if audit is not None:
            item.audit = audit
        now = datetime.now(timezone.utc)
        if item.status == InvestmentBatchItemStatus.SUBMITTED.value:
            item.submitted_at = now
        if item.status in TERMINAL_ITEM_STATUSES:
            item.completed_at = now
        item.updated_at = now
        self.session.flush()
        return item

    def terminal_counts(self, job: InvestmentBatchJob) -> tuple[int, int]:
        items = list(job.items)
        completed = sum(1 for item in items if item.status in TERMINAL_ITEM_STATUSES)
        failed = sum(
            1
            for item in items
            if item.status
            in {
                InvestmentBatchItemStatus.PROVIDER_FAILED.value,
                InvestmentBatchItemStatus.FALLBACK.value,
                InvestmentBatchItemStatus.FAILED.value,
            }
        )
        job.completed_items = completed
        job.failed_items = failed
        job.updated_at = datetime.now(timezone.utc)
        self.session.flush()
        return completed, failed
