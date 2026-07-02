"""Unit tests for direct-SQL recompute-status persistence (CHAOS-2699, D11/D12).

sqlite-in-memory (file-backed, per fixture -- see rationale below), no live
Postgres -- mirrors ``tests/test_external_ingest_status.py``'s convention.
No new ``postgres`` pytest marker (epic-wide synthesizer reconciliation).

``record_recompute_dispatch()`` takes a sync ``Session`` (mirrors the
Celery-task caller); ``mark_recompute_pending()``/``get_recompute_jobs()``
take an ``AsyncSession`` (mirrors the FastAPI caller). Both flavors point
at file-backed sqlite databases (not ``:memory:``) so each test's fixture
is fully isolated without needing to share a single connection across the
sync/async boundary.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from dev_health_ops.external_ingest.recompute import (
    RecomputeDispatchResult,
    RecomputeJobRecord,
    RecomputeScope,
)
from dev_health_ops.external_ingest.recompute_status import (
    get_recompute_jobs,
    mark_recompute_pending,
    record_recompute_dispatch,
)
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestRecomputeJob,
)
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

_TABLES = tables_of(ExternalIngestBatch, ExternalIngestRecomputeJob)

ORG = "org-a"
SYSTEM = "github"
INSTANCE = "acme/api"

_SEED_SQL = text(
    """
    INSERT INTO external_ingest_batches (
        ingestion_id, org_id, idempotency_key, payload_hash, source_system,
        source_instance, producer, producer_version, schema_version,
        window_started_at, window_ended_at, status, attempts,
        items_received, items_accepted, items_rejected, record_counts,
        error_summary, created_at, updated_at, completed_at,
        recompute_status, recompute_scope, recompute_dispatched_at,
        recompute_completed_at, recompute_error
    ) VALUES (
        :ingestion_id, :org_id, :idempotency_key, :payload_hash, :source_system,
        :source_instance, NULL, NULL, 'external-ingest.v1',
        NULL, NULL, 'completed', 1,
        3, 3, 0, NULL,
        NULL, :now, :now, NULL,
        :recompute_status, NULL, NULL, NULL, NULL
    )
    """
)


def _scope(**overrides: Any) -> RecomputeScope:
    defaults: dict[str, Any] = dict(
        org_id=ORG,
        source_system=SYSTEM,
        source_instance=INSTANCE,
        repo_ids=frozenset({"repo-a"}),
        team_ids=frozenset(),
        record_kinds=frozenset({"pull_request.v1"}),
        ingestion_ids=frozenset({"ing-1"}),
        window_start=datetime(2026, 6, 25, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 26, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return RecomputeScope(**defaults)


@pytest.fixture
def sync_session_maker(tmp_path: Path):
    db_path = tmp_path / "recompute-status-sync.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine, tables=_TABLES)
    maker = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield maker
    finally:
        engine.dispose()


def _seed_batch_sync(
    session, *, ingestion_id: str, recompute_status: str = "not_applicable"
) -> None:
    session.execute(
        _SEED_SQL,
        {
            "ingestion_id": ingestion_id,
            "org_id": ORG,
            "idempotency_key": f"key-{ingestion_id}",
            "payload_hash": "hash",
            "source_system": SYSTEM,
            "source_instance": INSTANCE,
            "now": datetime.now(timezone.utc),
            "recompute_status": recompute_status,
        },
    )
    session.commit()


def test_record_recompute_dispatch_writes_status_scope_and_jobs(
    sync_session_maker,
) -> None:
    with sync_session_maker() as session:
        _seed_batch_sync(session, ingestion_id="ing-1")

        scope = _scope()
        result = RecomputeDispatchResult(
            status="dispatched",
            jobs=(
                RecomputeJobRecord(
                    task="dev_health_ops.workers.tasks.run_daily_metrics",
                    task_id="task-1",
                    queue="metrics",
                    repo_id="repo-a",
                ),
                RecomputeJobRecord(
                    task="dev_health_ops.workers.tasks.run_work_graph_build",
                    task_id="task-2",
                    queue="metrics",
                    repo_id="repo-a",
                ),
            ),
            capped_days=False,
            capped_repos=False,
        )
        record_recompute_dispatch(
            session, org_id=ORG, ingestion_ids=["ing-1"], scope=scope, result=result
        )

    with sync_session_maker() as session:
        row = (
            session.execute(
                text("SELECT * FROM external_ingest_batches WHERE ingestion_id = :id"),
                {"id": "ing-1"},
            )
            .mappings()
            .first()
        )
        assert row is not None
        assert row["recompute_status"] == "dispatched"
        assert row["recompute_dispatched_at"] is not None
        assert row["recompute_completed_at"] is not None
        assert row["recompute_error"] is None
        scope_json = json.loads(row["recompute_scope"])
        assert scope_json["repoIds"] == ["repo-a"]
        assert scope_json["cappedDays"] is False

        jobs = (
            session.execute(
                text(
                    "SELECT * FROM external_ingest_recompute_jobs WHERE org_id = :org"
                ),
                {"org": ORG},
            )
            .mappings()
            .all()
        )
        assert len(jobs) == 2
        assert {j["celery_task_id"] for j in jobs} == {"task-1", "task-2"}
        assert {j["repo_id"] for j in jobs} == {"repo-a"}


def test_record_recompute_dispatch_covers_all_coalesced_ingestion_ids(
    sync_session_maker,
) -> None:
    """Risk 5 in the brief: a flush's status write must cover every
    ingestion_id that fed into the coalesced scope, not just one."""
    with sync_session_maker() as session:
        _seed_batch_sync(session, ingestion_id="ing-1")
        _seed_batch_sync(session, ingestion_id="ing-2")

        scope = _scope(ingestion_ids=frozenset({"ing-1", "ing-2"}))
        result = RecomputeDispatchResult(
            status="dispatched", jobs=(), capped_days=False, capped_repos=False
        )
        record_recompute_dispatch(
            session,
            org_id=ORG,
            ingestion_ids=["ing-1", "ing-2"],
            scope=scope,
            result=result,
        )

    with sync_session_maker() as session:
        rows = (
            session.execute(
                text(
                    "SELECT ingestion_id, recompute_status FROM external_ingest_batches "
                    "ORDER BY ingestion_id"
                )
            )
            .mappings()
            .all()
        )
        assert len(rows) == 2
        assert all(r["recompute_status"] == "dispatched" for r in rows)


def test_record_recompute_dispatch_failed_status_no_dispatched_at(
    sync_session_maker,
) -> None:
    with sync_session_maker() as session:
        _seed_batch_sync(session, ingestion_id="ing-1")
        scope = _scope()
        result = RecomputeDispatchResult(
            status="failed",
            jobs=(),
            capped_days=False,
            capped_repos=False,
            error="signature boom",
        )
        record_recompute_dispatch(
            session, org_id=ORG, ingestion_ids=["ing-1"], scope=scope, result=result
        )

    with sync_session_maker() as session:
        row = (
            session.execute(
                text("SELECT * FROM external_ingest_batches WHERE ingestion_id = :id"),
                {"id": "ing-1"},
            )
            .mappings()
            .first()
        )
        assert row["recompute_status"] == "failed"
        assert row["recompute_dispatched_at"] is None
        assert row["recompute_completed_at"] is not None
        assert row["recompute_error"] == "signature boom"


def test_record_recompute_dispatch_no_jobs_no_job_rows(sync_session_maker) -> None:
    with sync_session_maker() as session:
        _seed_batch_sync(session, ingestion_id="ing-1")
        scope = _scope()
        result = RecomputeDispatchResult(
            status="skipped_no_scope", jobs=(), capped_days=False, capped_repos=False
        )
        record_recompute_dispatch(
            session, org_id=ORG, ingestion_ids=["ing-1"], scope=scope, result=result
        )

    with sync_session_maker() as session:
        count = session.execute(
            text("SELECT COUNT(*) FROM external_ingest_recompute_jobs")
        ).scalar_one()
        assert count == 0


def test_record_recompute_dispatch_persists_job_with_none_task_id(
    sync_session_maker,
) -> None:
    """Adversarial-review finding: a job whose Celery AsyncResult had no
    ``.parent`` carries ``task_id=None`` -- persisting it must not raise an
    IntegrityError (which would silently roll back the whole status write,
    even though the underlying Celery jobs were already dispatched)."""
    with sync_session_maker() as session:
        _seed_batch_sync(session, ingestion_id="ing-1")
        scope = _scope()
        result = RecomputeDispatchResult(
            status="dispatched",
            jobs=(
                RecomputeJobRecord(
                    task="dev_health_ops.workers.tasks.run_daily_metrics",
                    task_id=None,
                    queue="metrics",
                    repo_id="repo-a",
                ),
            ),
            capped_days=False,
            capped_repos=False,
        )
        record_recompute_dispatch(
            session, org_id=ORG, ingestion_ids=["ing-1"], scope=scope, result=result
        )

    with sync_session_maker() as session:
        row = (
            session.execute(
                text("SELECT * FROM external_ingest_batches WHERE ingestion_id = :id"),
                {"id": "ing-1"},
            )
            .mappings()
            .first()
        )
        assert row["recompute_status"] == "dispatched"

        job = (
            session.execute(
                text(
                    "SELECT * FROM external_ingest_recompute_jobs WHERE org_id = :org"
                ),
                {"org": ORG},
            )
            .mappings()
            .first()
        )
        assert job is not None
        assert job["celery_task_id"] is None


@pytest_asyncio.fixture
async def async_session_maker(tmp_path: Path):
    db_path = tmp_path / "recompute-status-async.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_batch_async(
    session: AsyncSession,
    *,
    ingestion_id: str,
    recompute_status: str = "not_applicable",
) -> None:
    await session.execute(
        _SEED_SQL,
        {
            "ingestion_id": ingestion_id,
            "org_id": ORG,
            "idempotency_key": f"key-{ingestion_id}",
            "payload_hash": "hash",
            "source_system": SYSTEM,
            "source_instance": INSTANCE,
            "now": datetime.now(timezone.utc),
            "recompute_status": recompute_status,
        },
    )
    await session.commit()


async def _insert_job(
    session: AsyncSession,
    *,
    task_name: str,
    task_id: str,
    repo_id: str | None,
    dispatched_at: datetime,
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO external_ingest_recompute_jobs (
                id, org_id, source_system, source_instance, celery_task_name,
                celery_task_id, queue, repo_id, status, dispatched_at
            ) VALUES (
                :id, :org_id, :source_system, :source_instance, :task_name,
                :task_id, 'metrics', :repo_id, 'dispatched', :dispatched_at
            )
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "org_id": ORG,
            "source_system": SYSTEM,
            "source_instance": INSTANCE,
            "task_name": task_name,
            "task_id": task_id,
            "repo_id": repo_id,
            "dispatched_at": dispatched_at,
        },
    )


@pytest.mark.asyncio
async def test_mark_recompute_pending_transitions_not_applicable(
    async_session_maker,
) -> None:
    async with async_session_maker() as session:
        await _seed_batch_async(session, ingestion_id="ing-1")
        await mark_recompute_pending(session, org_id=ORG, ingestion_id="ing-1")
        await session.commit()

        row = (
            (
                await session.execute(
                    text(
                        "SELECT recompute_status FROM external_ingest_batches "
                        "WHERE ingestion_id = :id"
                    ),
                    {"id": "ing-1"},
                )
            )
            .mappings()
            .first()
        )
        assert row["recompute_status"] == "pending"


@pytest.mark.asyncio
async def test_mark_recompute_pending_never_regresses_terminal_status(
    async_session_maker,
) -> None:
    async with async_session_maker() as session:
        await _seed_batch_async(
            session, ingestion_id="ing-1", recompute_status="dispatched"
        )
        await mark_recompute_pending(session, org_id=ORG, ingestion_id="ing-1")
        await session.commit()

        row = (
            (
                await session.execute(
                    text(
                        "SELECT recompute_status FROM external_ingest_batches "
                        "WHERE ingestion_id = :id"
                    ),
                    {"id": "ing-1"},
                )
            )
            .mappings()
            .first()
        )
        assert row["recompute_status"] == "dispatched"


@pytest.mark.asyncio
async def test_get_recompute_jobs_returns_only_matching_flush(
    async_session_maker,
) -> None:
    dispatched_at = datetime(2026, 6, 26, 0, 1, tzinfo=timezone.utc)
    other_dispatched_at = datetime(2026, 6, 25, 0, 1, tzinfo=timezone.utc)
    async with async_session_maker() as session:
        await _insert_job(
            session,
            task_name="dev_health_ops.workers.tasks.run_daily_metrics",
            task_id="task-daily",
            repo_id="repo-a",
            dispatched_at=dispatched_at,
        )
        await _insert_job(
            session,
            task_name="dev_health_ops.workers.tasks.run_work_graph_build",
            task_id="task-build",
            repo_id="repo-a",
            dispatched_at=dispatched_at,
        )
        await _insert_job(
            session,
            task_name="dev_health_ops.workers.tasks.run_daily_metrics",
            task_id="task-old",
            repo_id="repo-b",
            dispatched_at=other_dispatched_at,
        )
        await session.commit()

        jobs = await get_recompute_jobs(
            session,
            org_id=ORG,
            source_system=SYSTEM,
            source_instance=INSTANCE,
            dispatched_at=dispatched_at,
        )
    assert len(jobs) == 2
    assert {j.repo_id for j in jobs} == {"repo-a"}
    assert {j.task_id for j in jobs} == {"task-daily", "task-build"}


@pytest.mark.asyncio
async def test_get_recompute_jobs_none_dispatched_at_returns_empty(
    async_session_maker,
) -> None:
    async with async_session_maker() as session:
        jobs = await get_recompute_jobs(
            session,
            org_id=ORG,
            source_system=SYSTEM,
            source_instance=INSTANCE,
            dispatched_at=None,
        )
    assert jobs == []
