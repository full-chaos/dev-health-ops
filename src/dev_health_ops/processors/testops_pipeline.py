from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dev_health_ops.connectors.testops.base import (
    BasePipelineAdapter,
    PipelineSyncBatch,
)
from dev_health_ops.metrics.sinks.ingestion import IngestionSink


@dataclass(slots=True)
class PipelineIngestionResult:
    pipeline_runs: int
    job_runs: int
    last_synced_cursor: datetime | None


class TestOpsPipelineProcessor:
    def __init__(self, ingestion_sink: IngestionSink) -> None:
        self.ingestion_sink = ingestion_sink

    @staticmethod
    def _effective_since(
        since_date: datetime | None, last_synced: datetime | None
    ) -> datetime | None:
        return since_date or last_synced

    async def fetch_and_store(
        self,
        adapter: BasePipelineAdapter,
        *,
        since_date: datetime | None = None,
        until_date: datetime | None = None,
        last_synced: datetime | None = None,
        **adapter_kwargs: Any,
    ) -> PipelineIngestionResult:
        batch = await adapter.fetch_pipeline_data(
            since_date=self._effective_since(since_date, last_synced),
            until_date=until_date,
            last_synced=last_synced,
            **adapter_kwargs,
        )
        await self.persist(batch)
        return PipelineIngestionResult(
            pipeline_runs=len(batch.pipeline_runs),
            job_runs=len(batch.job_runs),
            last_synced_cursor=batch.last_synced_cursor,
        )

    async def persist(self, batch: PipelineSyncBatch) -> None:
        if batch.pipeline_runs:
            insert_pipeline_runs = getattr(
                self.ingestion_sink, "insert_testops_pipeline_runs"
            )
            await insert_pipeline_runs(batch.pipeline_runs)
        if batch.job_runs:
            insert_job_runs = getattr(self.ingestion_sink, "insert_testops_job_runs")
            await insert_job_runs(batch.job_runs)
