"""Unit tests for external_ingest.processor (CHAOS-2697).

Real aiosqlite-backed status/payload/source tables (the store SQL actually
runs — same approach as tests/api/test_external_ingest_router.py); only the
two out-of-process boundaries are faked: ``sinks.write_batch`` (ClickHouse)
and ``recompute.schedule_or_coalesce`` (Valkey/Celery).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION
from dev_health_ops.api.external_ingest.status import (
    create_batch,
    get_batch,
    list_rejections,
)
from dev_health_ops.external_ingest import processor as processor_mod
from dev_health_ops.external_ingest import recompute as recompute_mod
from dev_health_ops.external_ingest.errors import PermanentProcessingError
from dev_health_ops.external_ingest.payload_store import payload_exists, upsert_payload
from dev_health_ops.external_ingest.processor import (
    TransientSinkWriteError,
    mark_batch_failed,
    process_batch,
)
from dev_health_ops.external_ingest.types import (
    AffectedScope,
    SinkWriteError,
    SinkWriteResult,
)
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestBatchPayload,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.ingest_auth import IngestSource, IngestSourceMode
from tests._helpers import tables_of

ORG = "org-2697"
SYSTEM = "github"
INSTANCE = "acme/api"
REPO_UUID = uuid.uuid4()

_TABLES = tables_of(
    ExternalIngestBatch,
    ExternalIngestRejection,
    ExternalIngestBatchPayload,
    IngestSource,
)

VALID_REPO = {
    "kind": "repository.v1",
    "externalId": INSTANCE,
    "payload": {"externalId": INSTANCE, "sourceSystem": "github"},
}
VALID_COMMIT = {
    "kind": "commit.v1",
    "externalId": f"{INSTANCE}@abc1234",
    "payload": {
        "repositoryExternalId": INSTANCE,
        "hash": "abc1234",
        "authorWhen": "2026-07-01T00:00:00Z",
    },
}
INVALID_WORK_ITEM = {"kind": "work_item.v1", "externalId": "wi-bad", "payload": {}}
OUT_OF_INSTANCE_COMMIT = {
    "kind": "commit.v1",
    "externalId": "other/repo@def5678",
    "payload": {
        "repositoryExternalId": "other/repo",
        "hash": "def5678",
        "authorWhen": "2026-07-01T00:00:00Z",
    },
}


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'external-ingest-processor.db'}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def source_id(session_maker) -> uuid.UUID:
    source = IngestSource(
        org_id=ORG,
        system=SYSTEM,
        instance=INSTANCE,
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    async with session_maker() as session:
        session.add(source)
        await session.commit()
        return source.id


@pytest.fixture
def fake_sink(monkeypatch):
    """write_batch stand-in: succeeds, reporting one repo in scope."""

    async def _write(batch, *, clickhouse_dsn):
        return SinkWriteResult(
            ingestion_id=batch.ingestion_id,
            org_id=batch.org_id,
            counts_written={"repository": len(batch.repositories)},
            errors=[],
            warnings=[],
            affected_scope=AffectedScope(
                org_id=batch.org_id,
                source_systems={batch.source_system},
                source_instances={batch.source_instance},
                repo_ids={REPO_UUID},
            ),
        )

    mock = AsyncMock(side_effect=_write)
    monkeypatch.setattr(processor_mod, "write_batch", mock)
    return mock


@pytest.fixture
def fake_recompute(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(processor_mod, "schedule_or_coalesce", mock)
    return mock


@pytest.fixture(autouse=True)
def _clickhouse_uri(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://ch:ch@fake:8123/ci_test")


@pytest.fixture(autouse=True)
def _patched_session(monkeypatch, session_maker):
    """Mirror the real get_postgres_session semantics (commit on clean exit,
    rollback + re-raise on exception) against the aiosqlite engine."""

    @asynccontextmanager
    async def _fake_session():
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    monkeypatch.setattr(processor_mod, "get_postgres_session", _fake_session)


async def _seed_batch(
    session_maker,
    *,
    records: list[dict],
    status: str | None = None,
    items_received: int | None = None,
    envelope_instance: str = INSTANCE,
    source_instance: str = INSTANCE,
    with_payload: bool = True,
) -> str:
    envelope = {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": "key-2697",
        "source": {
            "type": "customer_push",
            "system": SYSTEM,
            "instance": envelope_instance,
        },
        "records": records,
    }
    ingestion_id = uuid.uuid4()
    async with session_maker() as session:
        await create_batch(
            session,
            ingestion_id=ingestion_id,
            org_id=ORG,
            idempotency_key="key-2697",
            payload_hash="hash-2697",
            source_system=SYSTEM,
            source_instance=source_instance,
            producer=None,
            producer_version=None,
            schema_version=SCHEMA_VERSION,
            window_started_at=None,
            window_ended_at=None,
            items_received=(
                items_received if items_received is not None else len(records)
            ),
        )
        if with_payload:
            await upsert_payload(
                session,
                ingestion_id=str(ingestion_id),
                org_id=ORG,
                schema_version=SCHEMA_VERSION,
                payload_bytes=json.dumps(envelope).encode(),
            )
        if status is not None:
            await session.execute(
                text(
                    "UPDATE external_ingest_batches SET status = :status "
                    "WHERE ingestion_id = :ingestion_id"
                ),
                {"status": status, "ingestion_id": str(ingestion_id)},
            )
        await session.commit()
    return str(ingestion_id)


async def _get_row(session_maker, ingestion_id: str):
    async with session_maker() as session:
        return await get_batch(
            session, org_id=ORG, ingestion_id=uuid.UUID(ingestion_id)
        )


def _process(ingestion_id: str, **overrides):
    kwargs = dict(
        ingestion_id=ingestion_id,
        org_id=ORG,
        source_system=SYSTEM,
        source_instance=INSTANCE,
        schema_version=SCHEMA_VERSION,
    )
    kwargs.update(overrides)
    return process_batch(**kwargs)


@pytest.mark.asyncio
class TestHappyPath:
    async def test_completes_batch_end_to_end(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO, VALID_COMMIT]
        )

        accepted = await _process(ingestion_id)

        assert accepted == 2
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None
        assert row.status == "completed"
        assert row.items_accepted == 2
        assert row.items_rejected == 0
        assert row.record_counts == {"repository.v1": 1, "commit.v1": 1}
        assert row.completed_at is not None
        fake_sink.assert_awaited_once()
        # CC9: payload row deleted on terminal status.
        async with session_maker() as session:
            assert not await payload_exists(
                session, ingestion_id=ingestion_id, org_id=ORG
            )

    async def test_source_id_stamped_from_registry_case_insensitively(
        self, session_maker, fake_sink, fake_recompute
    ):
        # Registered casing differs from the pointer's — CHAOS-2695 blocks
        # case-variant duplicates, so a lower() match is the same source.
        source = IngestSource(
            org_id=ORG,
            system=SYSTEM,
            instance="Acme/API",
            mode=IngestSourceMode.CUSTOMER_PUSH.value,
            enabled=True,
        )
        async with session_maker() as session:
            session.add(source)
            await session.commit()
            expected_source_id = source.id
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])

        await _process(ingestion_id)

        (batch_arg,), _kwargs = fake_sink.await_args
        assert batch_arg.source_id == expected_source_id
        assert batch_arg.org_id == ORG

    async def test_recompute_dispatched_once_with_planner_vocabulary(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO, VALID_COMMIT]
        )

        await _process(ingestion_id)

        fake_recompute.assert_called_once()
        kwargs = fake_recompute.call_args.kwargs
        # Kwargs must bind the REAL schedule_or_coalesce signature — the
        # celery-signature-contract rule: drift fails here, not silently.
        inspect.signature(recompute_mod.schedule_or_coalesce).bind(**kwargs)
        assert kwargs["org_id"] == ORG
        assert kwargs["ingestion_id"] == ingestion_id
        assert kwargs["repo_ids"] == {str(REPO_UUID)}
        # FULL kind names (planner's .v1 vocabulary), not sink bare names.
        assert kwargs["record_kinds"] == {"repository.v1", "commit.v1"}

    async def test_second_invocation_is_an_idempotent_skip(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])
        assert await _process(ingestion_id) == 1

        assert await _process(ingestion_id) == 0

        fake_sink.assert_awaited_once()  # not re-written on replay
        fake_recompute.assert_called_once()

    async def test_stream_unavailable_batch_is_processed(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        # A 503'd accept whose XADD actually landed: expected duplicate
        # pointer for a stream_unavailable row — process it, don't wedge.
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO], status="stream_unavailable"
        )

        accepted = await _process(ingestion_id)

        assert accepted == 1
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "completed"


@pytest.mark.asyncio
class TestRejections:
    async def test_partial_batch_persists_collapsed_rejections(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(
            session_maker,
            records=[VALID_COMMIT, INVALID_WORK_ITEM, OUT_OF_INSTANCE_COMMIT],
        )

        accepted = await _process(ingestion_id)

        assert accepted == 1
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None
        assert row.status == "partial"
        assert row.items_accepted == 1
        assert row.items_rejected == 2
        assert row.record_counts == {"commit.v1": 1}
        async with session_maker() as session:
            rejections, total_stored = await list_rejections(
                session, org_id=ORG, ingestion_id=uuid.UUID(ingestion_id)
            )
        assert total_stored == 2
        by_index = {r.record_index: r for r in rejections}
        assert set(by_index) == {1, 2}
        assert by_index[1].code == "missing_required_field"
        assert by_index[2].code == "record_outside_source_instance"

    async def test_all_rejected_becomes_failed_and_skips_sinks(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(session_maker, records=[INVALID_WORK_ITEM])

        accepted = await _process(ingestion_id)

        assert accepted == 0
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "failed"
        fake_sink.assert_not_awaited()
        fake_recompute.assert_not_called()
        async with session_maker() as session:
            assert not await payload_exists(
                session, ingestion_id=ingestion_id, org_id=ORG
            )


@pytest.mark.asyncio
class TestSinkRetryLadder:
    async def test_persistent_sink_errors_raise_transient_after_ladder(
        self, session_maker, source_id, fake_recompute, monkeypatch
    ):
        error = SinkWriteError(
            record_index=-1,
            kind="repository",
            external_id=None,
            code="clickhouse_insert_failed",
            message="connection refused",
        )

        async def _failing_write(batch, *, clickhouse_dsn):
            return SinkWriteResult(
                ingestion_id=batch.ingestion_id,
                org_id=batch.org_id,
                errors=[error],
                affected_scope=AffectedScope(org_id=batch.org_id),
            )

        write_mock = AsyncMock(side_effect=_failing_write)
        monkeypatch.setattr(processor_mod, "write_batch", write_mock)
        sleeps: list[float] = []

        async def _fake_sleep(delay):
            sleeps.append(delay)

        monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])

        with pytest.raises(TransientSinkWriteError):
            await _process(ingestion_id)

        assert write_mock.await_count == 4  # initial + one per backoff step
        assert sleeps == [2.0, 4.0, 8.0]
        row = await _get_row(session_maker, ingestion_id)
        # Transient: batch stays in-flight for the reclaim ladder; payload
        # survives so the retry can re-run the whole thing.
        assert row is not None and row.status == "processing"
        async with session_maker() as session:
            assert await payload_exists(session, ingestion_id=ingestion_id, org_id=ORG)
        fake_recompute.assert_not_called()

    async def test_recovery_mid_ladder_completes(
        self, session_maker, source_id, fake_recompute, monkeypatch
    ):
        error = SinkWriteError(
            record_index=-1,
            kind="repository",
            external_id=None,
            code="clickhouse_insert_failed",
            message="blip",
        )
        outcomes = iter([[error], [error], []])

        async def _flaky_write(batch, *, clickhouse_dsn):
            return SinkWriteResult(
                ingestion_id=batch.ingestion_id,
                org_id=batch.org_id,
                errors=next(outcomes),
                affected_scope=AffectedScope(org_id=batch.org_id),
            )

        write_mock = AsyncMock(side_effect=_flaky_write)
        monkeypatch.setattr(processor_mod, "write_batch", write_mock)
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])

        accepted = await _process(ingestion_id)

        assert accepted == 1
        assert write_mock.await_count == 3
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "completed"


@pytest.mark.asyncio
class TestPermanentFailures:
    async def test_schema_version_mismatch(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])

        with pytest.raises(PermanentProcessingError, match="schema version"):
            await _process(ingestion_id, schema_version="external-ingest.v2")

        row = await _get_row(session_maker, ingestion_id)
        # Raised before mark_processing: still retryable-accepted.
        assert row is not None and row.status == "accepted"

    async def test_unknown_source_system(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])
        with pytest.raises(PermanentProcessingError, match="source system"):
            await _process(ingestion_id, source_system="subversion")

    async def test_non_uuid_ingestion_id(self, source_id, fake_sink, fake_recompute):
        with pytest.raises(PermanentProcessingError, match="non-UUID"):
            await _process("not-a-uuid")

    async def test_missing_status_row(self, source_id, fake_sink, fake_recompute):
        with pytest.raises(PermanentProcessingError, match="no status row"):
            await _process(str(uuid.uuid4()))

    async def test_missing_payload(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO], with_payload=False
        )
        with pytest.raises(PermanentProcessingError, match="payload row"):
            await _process(ingestion_id)

    async def test_unregistered_source(self, session_maker, fake_sink, fake_recompute):
        # No IngestSource row seeded at all (deleted after accept).
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])
        with pytest.raises(
            PermanentProcessingError, match="no registered ingest source"
        ):
            await _process(ingestion_id)

    async def test_pointer_payload_source_disagreement(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO], envelope_instance="acme/other"
        )
        with pytest.raises(PermanentProcessingError, match="stored payload says"):
            await _process(ingestion_id)

    async def test_record_count_disagreement(
        self, session_maker, source_id, fake_sink, fake_recompute
    ):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO], items_received=3
        )
        with pytest.raises(PermanentProcessingError, match="items_received"):
            await _process(ingestion_id)


@pytest.mark.asyncio
class TestRecomputeBestEffort:
    async def test_dispatch_failure_never_fails_ingestion(
        self, session_maker, source_id, fake_sink, monkeypatch
    ):
        boom = MagicMock(side_effect=RuntimeError("valkey exploded"))
        monkeypatch.setattr(processor_mod, "schedule_or_coalesce", boom)
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])

        accepted = await _process(ingestion_id)

        assert accepted == 1
        boom.assert_called_once()
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "completed"


@pytest.mark.asyncio
class TestLostCas:
    async def test_yields_zero_when_processing_transition_not_won(
        self, session_maker, source_id, fake_sink, fake_recompute, monkeypatch
    ):
        monkeypatch.setattr(processor_mod, "mark_processing", AsyncMock())
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])

        accepted = await _process(ingestion_id)

        assert accepted == 0
        fake_sink.assert_not_awaited()
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "accepted"


@pytest.mark.asyncio
class TestMarkBatchFailed:
    async def test_forces_failed_and_deletes_payload(self, session_maker, source_id):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO], status="processing"
        )

        await mark_batch_failed(
            ingestion_id=ingestion_id, org_id=ORG, reason="max_deliveries_exceeded"
        )

        row = await _get_row(session_maker, ingestion_id)
        assert row is not None
        assert row.status == "failed"
        assert row.error_summary == {
            "system_failure": True,
            "reason": "max_deliveries_exceeded",
        }
        # Counter invariant (adversarial round 2): failed = zero accepted;
        # the whole batch counts as rejected for GET/list consumers.
        assert row.items_accepted == 0
        assert row.items_rejected == row.items_received == 1
        assert row.record_counts is None
        assert row.completed_at is not None
        async with session_maker() as session:
            assert not await payload_exists(
                session, ingestion_id=ingestion_id, org_id=ORG
            )

    async def test_accepted_batch_can_be_failed(self, session_maker, source_id):
        # Permanent failures raised before mark_processing leave 'accepted'.
        ingestion_id = await _seed_batch(session_maker, records=[VALID_REPO])
        await mark_batch_failed(
            ingestion_id=ingestion_id, org_id=ORG, reason="bad envelope"
        )
        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "failed"

    async def test_terminal_batch_is_left_alone(self, session_maker, source_id):
        ingestion_id = await _seed_batch(
            session_maker, records=[VALID_REPO], status="completed"
        )

        await mark_batch_failed(ingestion_id=ingestion_id, org_id=ORG, reason="late")

        row = await _get_row(session_maker, ingestion_id)
        assert row is not None and row.status == "completed"
        async with session_maker() as session:
            # Payload untouched on the no-op path.
            assert await payload_exists(session, ingestion_id=ingestion_id, org_id=ORG)

    async def test_non_uuid_ingestion_id_is_a_noop(self, source_id):
        await mark_batch_failed(ingestion_id="junk", org_id=ORG, reason="r")

    async def test_raises_on_store_failure(self, source_id, monkeypatch):
        # The RAISES contract (consumer ACK gate): a failed status write must
        # propagate so the entry stays un-ACKed for a later retry.
        @asynccontextmanager
        async def _broken_session():
            raise RuntimeError("pg down")
            yield  # pragma: no cover

        monkeypatch.setattr(processor_mod, "get_postgres_session", _broken_session)
        with pytest.raises(RuntimeError, match="pg down"):
            await mark_batch_failed(
                ingestion_id=str(uuid.uuid4()), org_id=ORG, reason="r"
            )
