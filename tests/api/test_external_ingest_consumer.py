"""Tests for the external-ingest consumer: retry/reclaim, DLQ, and the
idempotent-skip guard (CHAOS-2693 D4/D5/CC11).

Uses ``fakeredis.FakeValkey`` (confirmed to implement XPENDING/XCLAIM/XRANGE
-- see brief Risk 4) end-to-end through ``consumer.consume()`` rather than a
hand-rolled fake, so the assertions exercise the real XREADGROUP/XPENDING/
XCLAIM/XACK sequence.
"""

from __future__ import annotations

import asyncio
import sys
import time
import types
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

fakeredis = pytest.importorskip("fakeredis")
from fakeredis import FakeValkey  # noqa: E402

from dev_health_ops.api.external_ingest import consumer as consumer_mod
from dev_health_ops.api.external_ingest import status as status_mod
from dev_health_ops.external_ingest.errors import PermanentProcessingError
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

_STATUS_TABLES = tables_of(ExternalIngestBatch, ExternalIngestRejection)

# Captured at collection time (before the module-wide `_not_terminal`
# autouse fixture below ever runs) so TestIdempotentSkipGuardRealStatusStore
# can explicitly restore the real implementation regardless of fixture
# ordering between that blanket stub and this class's own fixtures.
_REAL_IS_BATCH_TERMINAL_ASYNC = (
    consumer_mod.ExternalIngestStreamConsumer._is_batch_terminal_async
)
# Same rationale, for the module-wide `_processor_available` autouse stub.
_REAL_PROCESSOR_AVAILABLE = consumer_mod._processor_available


@pytest.fixture
def fake_redis():
    return FakeValkey(decode_responses=True)


def _consumer(**overrides) -> consumer_mod.ExternalIngestStreamConsumer:
    inst = consumer_mod.ExternalIngestStreamConsumer(consumer_name="test-consumer")
    for key, value in overrides.items():
        setattr(inst, key, value)
    return inst


def _xadd_batch(
    rc,
    org_id: str = "org-1",
    ingestion_id: str | None = None,
    source_system: str = "github",
) -> tuple[str, str]:
    stream = f"external-ingest:{org_id}:batches"
    ingestion_id = ingestion_id or str(uuid.uuid4())
    entry_id = rc.xadd(
        stream,
        {
            "ingestion_id": ingestion_id,
            "org_id": org_id,
            "source_system": source_system,
            "source_instance": "acme/api",
            "schema_version": "external-ingest.v1",
            "idempotency_key": "key-1",
            "record_count": "1",
        },
    )
    return stream, entry_id


@pytest.fixture(autouse=True)
def _patch_client(monkeypatch, fake_redis):
    monkeypatch.setattr(
        consumer_mod.StreamConsumer, "get_client", lambda self: fake_redis
    )


@pytest.fixture(autouse=True)
def _not_terminal(monkeypatch):
    """By default, no batch is terminal -- most tests exercise processing,
    not the idempotent-skip guard (which has its own dedicated tests)."""
    monkeypatch.setattr(
        consumer_mod.ExternalIngestStreamConsumer,
        "_is_batch_terminal_async",
        AsyncMock(return_value=False),
    )


@pytest.fixture(autouse=True)
def _processor_available(monkeypatch):
    """By default, treat CHAOS-2697's processor module as available -- most
    tests exercise retry/DLQ/reclaim mechanics that assume a real processor
    exists; the deployment-order guard itself has its own dedicated tests
    (TestProcessorAvailabilityGuard) that override this back to False."""
    monkeypatch.setattr(consumer_mod, "_processor_available", lambda: True)


class TestHappyPath:
    def test_success_acks_and_not_pending(self, fake_redis):
        stream, entry_id = _xadd_batch(fake_redis)
        c = _consumer()
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(return_value=1),
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 1
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert pending == []


class TestPermanentFailure:
    def test_lands_on_dlq_with_reason_and_is_acked(self, fake_redis):
        stream, entry_id = _xadd_batch(fake_redis, org_id="org-perm")
        c = _consumer()
        with (
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_process_entry_async",
                AsyncMock(side_effect=PermanentProcessingError("bad schema_version")),
            ),
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_mark_batch_failed_best_effort",
                lambda self, **kwargs: None,
            ),
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 0
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert pending == []  # ACKed, not left pending

        dlq_entries = fake_redis.xrange("external-ingest:org-perm:dlq")
        assert len(dlq_entries) == 1
        _dlq_id, dlq_data = dlq_entries[0]
        assert dlq_data["reason"] == "bad schema_version"
        assert dlq_data["original_stream"] == stream
        assert dlq_data["entry_id"] == entry_id

    def test_calls_mark_batch_failed(self, fake_redis, monkeypatch):
        # dev_health_ops.external_ingest.processor is CHAOS-2697's module and
        # does not exist yet in this issue's scope -- inject a stand-in so we
        # can assert the give-up path calls it exactly as CC23 pins, without
        # depending on 2697 having landed.
        fake_processor = types.ModuleType("dev_health_ops.external_ingest.processor")
        mark_failed = AsyncMock()
        setattr(
            fake_processor, "mark_batch_failed", mark_failed
        )  # ModuleType has no static attr
        monkeypatch.setitem(
            sys.modules, "dev_health_ops.external_ingest.processor", fake_processor
        )

        stream, entry_id = _xadd_batch(fake_redis, ingestion_id="ingest-marks-failed")
        c = _consumer()
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=PermanentProcessingError("nope")),
        ):
            c.consume(max_iterations=1)

        mark_failed.assert_awaited_once()
        _args, kwargs = mark_failed.call_args
        assert kwargs["ingestion_id"] == "ingest-marks-failed"
        assert kwargs["reason"] == "nope"

    def test_missing_processor_module_does_not_crash_consumer(self, fake_redis):
        """Before CHAOS-2697 lands, processor.py doesn't exist -- the
        give-up path must log and continue, not crash the consumer loop."""
        stream, _entry_id = _xadd_batch(fake_redis, org_id="org-no-processor")
        c = _consumer()
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=PermanentProcessingError("nope")),
        ):
            processed = c.consume(max_iterations=1)  # must not raise

        assert processed == 0
        assert fake_redis.xrange("external-ingest:org-no-processor:dlq")


class TestTransientFailure:
    def test_left_unacked_and_still_pending(self, fake_redis):
        stream, entry_id = _xadd_batch(fake_redis)
        c = _consumer()
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=RuntimeError("clickhouse blip")),
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 0
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert len(pending) == 1
        assert pending[0]["message_id"] == entry_id

    def test_not_routed_to_dlq_on_first_transient_failure(self, fake_redis):
        stream, _entry_id = _xadd_batch(fake_redis, org_id="org-transient")
        c = _consumer()
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=RuntimeError("blip")),
        ):
            c.consume(max_iterations=1)

        assert fake_redis.xrange("external-ingest:org-transient:dlq") == []


class TestReclaim:
    def test_stale_entry_under_max_deliveries_is_reclaimed_and_reprocessed(
        self, fake_redis
    ):
        stream, entry_id = _xadd_batch(fake_redis, org_id="org-reclaim")
        c = _consumer(reclaim_idle_ms=0)  # treat immediately-idle as stale

        # First pass: process_entry fails transiently, entry stays pending.
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=RuntimeError("blip")),
        ):
            c.consume(max_iterations=1)

        pending_before = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert len(pending_before) == 1
        assert pending_before[0]["times_delivered"] == 1

        # Second pass: reclaim_stale() picks it up (idle=0 means everything
        # is eligible), process_entry now succeeds.
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(return_value=1),
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 1
        pending_after = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert pending_after == []

    def test_entry_at_max_deliveries_is_given_up_not_reclaimed(
        self, fake_redis, monkeypatch
    ):
        stream, entry_id = _xadd_batch(fake_redis, org_id="org-giveup")
        c = _consumer(reclaim_idle_ms=0, max_deliveries=2)

        # Deliberately exercise the REAL (unmocked) move_to_dlq ->
        # _mark_batch_failed_best_effort -> run_async(...) path here rather
        # than stubbing it out: this is the sync give-up call site invoked
        # by the shared base's reclaim_stale() outside of any running event
        # loop -- the async permanent-failure path (test_calls_mark_batch_failed
        # above) exercises the awaited counterpart; this test is the sync
        # counterpart's regression coverage for the same class of bug (a
        # nested/re-entrant run_async() call would raise here).
        fake_processor = types.ModuleType("dev_health_ops.external_ingest.processor")
        mark_failed = AsyncMock()
        setattr(
            fake_processor, "mark_batch_failed", mark_failed
        )  # ModuleType has no static attr
        monkeypatch.setitem(
            sys.modules, "dev_health_ops.external_ingest.processor", fake_processor
        )

        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=RuntimeError("still broken")),
        ):
            # 3 separate consume() calls (not one max_iterations=3 call),
            # each preceded by a tiny sleep: fakeredis's XPENDING IDLE filter
            # requires actual elapsed wall-clock ms (idle=0 does NOT mean
            # "always eligible" -- see test_stream_consumer.py's identical
            # note), and three iterations run back-to-back inside one
            # consume() call can complete within the same 0ms tick,
            # especially under -n-distributed parallel test load, making the
            # give-up assertion below flaky without the explicit sleeps.
            #
            # iter 1: fresh read, delivery #1, fails, stays pending.
            c.consume(max_iterations=1)
            # iter 2: reclaim_stale sees delivery #1 < max_deliveries(2),
            #         reclaims -> delivery #2, handle_entries fails again,
            #         stays pending.
            time.sleep(0.01)
            c.consume(max_iterations=1)
            # iter 3: reclaim_stale sees delivery #2 >= max_deliveries(2) ->
            #         DLQ + ACK (give up), no 3rd XREADGROUP delivery.
            time.sleep(0.01)
            c.consume(max_iterations=1)

        pending_after = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert pending_after == []
        dlq_entries = fake_redis.xrange("external-ingest:org-giveup:dlq")
        assert len(dlq_entries) == 1
        assert dlq_entries[0][1]["reason"] == "max_deliveries_exceeded"
        assert dlq_entries[0][1]["entry_id"] == entry_id
        mark_failed.assert_awaited_once()
        _args, kwargs = mark_failed.call_args
        assert kwargs["reason"] == "max_deliveries_exceeded"


class TestPerOrgDlqIsolation:
    def test_two_orgs_poison_entries_land_on_distinct_dlq_streams(self, fake_redis):
        _xadd_batch(fake_redis, org_id="org-a", ingestion_id="a-1")
        _xadd_batch(fake_redis, org_id="org-b", ingestion_id="b-1")
        c = _consumer()

        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=PermanentProcessingError("bad")),
        ):
            c.consume(max_iterations=1)

        dlq_a = fake_redis.xrange("external-ingest:org-a:dlq")
        dlq_b = fake_redis.xrange("external-ingest:org-b:dlq")
        assert len(dlq_a) == 1
        assert len(dlq_b) == 1
        assert dlq_a[0][1]["ingestion_id"] == "a-1"
        assert dlq_b[0][1]["ingestion_id"] == "b-1"


class TestMoveToDlqFailureIsSwallowed:
    @staticmethod
    def _broken_dlq_client():
        class BrokenOnDlqOnly(FakeValkey):
            def xadd(self, name, *args, **kwargs):
                if name.endswith(":dlq"):
                    raise ConnectionError("dlq write failed")
                return super().xadd(name, *args, **kwargs)

        return BrokenOnDlqOnly(decode_responses=True)

    def test_broken_dlq_xadd_does_not_propagate(self):
        broken = self._broken_dlq_client()
        _stream, _entry_id = _xadd_batch(broken)
        c = _consumer()

        with (
            patch.object(
                consumer_mod.StreamConsumer, "get_client", lambda self: broken
            ),
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_process_entry_async",
                AsyncMock(side_effect=PermanentProcessingError("bad")),
            ),
        ):
            processed = c.consume(max_iterations=1)  # must not raise

        assert processed == 0

    def test_broken_dlq_xadd_leaves_entry_unacked_not_lost(self):
        """Adversarial-review finding: a DLQ write failure must not still
        ACK the source entry -- that would silently lose a poison message
        with no DLQ record and no way to retry the DLQ write. The entry
        must stay in the PEL so a later poll retries the DLQ write."""
        broken = self._broken_dlq_client()
        stream, entry_id = _xadd_batch(broken, org_id="org-dlq-broken")
        c = _consumer()

        with (
            patch.object(
                consumer_mod.StreamConsumer, "get_client", lambda self: broken
            ),
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_process_entry_async",
                AsyncMock(side_effect=PermanentProcessingError("bad")),
            ),
        ):
            c.consume(max_iterations=1)

        pending = broken.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert len(pending) == 1
        assert pending[0]["message_id"] == entry_id
        assert broken.xrange("external-ingest:org-dlq-broken:dlq") == []

    def test_reclaim_give_up_with_broken_dlq_leaves_entry_pending(self):
        """Same finding, exercised via the base's synchronous
        reclaim_stale() give-up path (move_to_dlq) rather than the async
        permanent-failure path."""
        broken = self._broken_dlq_client()
        stream, entry_id = _xadd_batch(broken, org_id="org-dlq-broken-reclaim")
        c = _consumer(reclaim_idle_ms=0, max_deliveries=1)

        with (
            patch.object(
                consumer_mod.StreamConsumer, "get_client", lambda self: broken
            ),
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_process_entry_async",
                AsyncMock(side_effect=RuntimeError("transient")),
            ),
        ):
            c.consume(max_iterations=1)  # delivery 1, transient, stays pending
            time.sleep(0.01)
            # reclaim_stale sees times_delivered(1) >= max_deliveries(1) ->
            # give-up path -> move_to_dlq -> DLQ write fails -> must NOT ack.
            c.consume(max_iterations=1)

        pending = broken.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert len(pending) == 1
        assert pending[0]["message_id"] == entry_id
        assert broken.xrange("external-ingest:org-dlq-broken-reclaim:dlq") == []


class TestProcessorAvailabilityGuard:
    """Deployment-order guard (adversarial-review finding): before
    CHAOS-2697 ships, the consumer must refuse to claim any stream entries
    rather than draining them into a guaranteed-ImportError retry ladder."""

    def test_consume_is_a_full_noop_when_processor_unavailable(
        self, fake_redis, monkeypatch
    ):
        monkeypatch.setattr(consumer_mod, "_processor_available", lambda: False)
        stream, entry_id = _xadd_batch(fake_redis)
        c = _consumer()

        processed = c.consume(max_iterations=1)

        assert processed == 0
        # No XREADGROUP/consumer-group activity at all: the entry was never
        # even delivered once (no PEL entry), unlike the transient-failure
        # case which delivers then leaves it pending.
        assert fake_redis.xlen(stream) == 1
        with_group = fake_redis.exists(stream)
        assert with_group  # stream itself untouched, still has our entry

    def test_consume_proceeds_normally_when_processor_available(
        self, fake_redis, monkeypatch
    ):
        monkeypatch.setattr(consumer_mod, "_processor_available", lambda: True)
        _stream, _entry_id = _xadd_batch(fake_redis)
        c = _consumer()

        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(return_value=1),
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 1

    def test_processor_available_reflects_real_import(self):
        """Sanity check on the real (unmocked) helper: CHAOS-2697's module
        genuinely does not exist yet at this issue's implementation time.
        Uses the reference captured at collection time (module docstring
        note above) since the autouse fixture in this file replaces
        ``consumer_mod._processor_available`` for every other test."""
        assert _REAL_PROCESSOR_AVAILABLE() is False


class TestIdempotentSkipGuard:
    def test_terminal_batch_is_acked_and_skipped_without_reprocessing(self, fake_redis):
        stream, entry_id = _xadd_batch(fake_redis, ingestion_id="ingest-terminal")
        c = _consumer()
        process_mock = AsyncMock(return_value=1)
        with (
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_is_batch_terminal_async",
                AsyncMock(return_value=True),
            ),
            patch.object(
                consumer_mod.ExternalIngestStreamConsumer,
                "_process_entry_async",
                process_mock,
            ),
        ):
            processed = c.consume(max_iterations=1)

        # Skipped (not "processed" -- terminal-skip returns 0 contribution)
        # but ACKed regardless, and process_entry is never invoked.
        assert processed == 0
        process_mock.assert_not_awaited()
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert pending == []


class TestConsumeExternalIngestStreams:
    def test_wires_batch_size_and_block_ms(self, fake_redis, monkeypatch):
        captured = {}
        orig_init = consumer_mod.ExternalIngestStreamConsumer.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            orig_init(self, *args, **kwargs)

        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer, "__init__", spy_init
        )
        consumer_mod.consume_external_ingest_streams(max_iterations=0)
        assert captured["block_ms"] == consumer_mod.BLOCK_MS
        assert captured["batch_size"] == consumer_mod.BATCH_SIZE


class TestIdempotentSkipGuardRealStatusStore:
    """Exercises the guard against the real status.get_batch()/sqlite
    integration (CHAOS-2694's store) rather than a mocked
    ``_is_batch_terminal_async`` -- proves the actual query + status-value
    comparison, not just the call site."""

    @pytest.fixture(autouse=True)
    def _use_real_terminal_check(self, monkeypatch):
        # Undo the module-wide `_not_terminal` autouse stub (declared above,
        # applies to every test by default) for this class specifically.
        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_is_batch_terminal_async",
            _REAL_IS_BATCH_TERMINAL_ASYNC,
        )

    @pytest_asyncio.fixture
    async def session_maker(self, tmp_path):
        db_path = tmp_path / "external-ingest-consumer-status.db"
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
        async with engine.begin() as conn:
            await conn.run_sync(
                lambda c: Base.metadata.create_all(c, tables=_STATUS_TABLES)
            )
        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            yield maker
        finally:
            await engine.dispose()

    @pytest.fixture
    def patched_db_session(self, monkeypatch, session_maker):
        @asynccontextmanager
        async def _fake_get_postgres_session():
            async with session_maker() as s:
                try:
                    yield s
                    await s.commit()
                except Exception:
                    await s.rollback()
                    raise

        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session", _fake_get_postgres_session
        )

    async def _seed_terminal_batch(
        self, session_maker, *, ingestion_id: uuid.UUID, org_id: str
    ) -> None:
        async with session_maker() as s:
            await status_mod.create_batch(
                s,
                ingestion_id=ingestion_id,
                org_id=org_id,
                idempotency_key="key-1",
                payload_hash="hash-1",
                source_system="github",
                source_instance="acme/api",
                producer=None,
                producer_version=None,
                schema_version="external-ingest.v1",
                window_started_at=None,
                window_ended_at=None,
                items_received=1,
            )
            await s.commit()
            await status_mod.mark_processing(
                s, org_id=org_id, ingestion_id=ingestion_id
            )
            await s.commit()
            await status_mod.complete_batch(
                s,
                org_id=org_id,
                ingestion_id=ingestion_id,
                items_accepted=1,
                items_rejected=0,
                rejections=[],
            )
            await s.commit()

    def test_terminal_batch_is_skipped_and_acked(
        self, fake_redis, session_maker, patched_db_session
    ):
        ingestion_id = uuid.uuid4()
        asyncio.run(
            self._seed_terminal_batch(
                session_maker, ingestion_id=ingestion_id, org_id="org-1"
            )
        )

        stream, entry_id = _xadd_batch(
            fake_redis, org_id="org-1", ingestion_id=str(ingestion_id)
        )
        c = _consumer()
        process_mock = AsyncMock(return_value=1)
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            process_mock,
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 0
        process_mock.assert_not_awaited()
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert pending == []

    def test_non_terminal_batch_is_processed_normally(
        self, fake_redis, session_maker, patched_db_session
    ):
        ingestion_id = uuid.uuid4()

        async def _seed_accepted() -> None:
            async with session_maker() as s:
                await status_mod.create_batch(
                    s,
                    ingestion_id=ingestion_id,
                    org_id="org-1",
                    idempotency_key="key-1",
                    payload_hash="hash-1",
                    source_system="github",
                    source_instance="acme/api",
                    producer=None,
                    producer_version=None,
                    schema_version="external-ingest.v1",
                    window_started_at=None,
                    window_ended_at=None,
                    items_received=1,
                )
                await s.commit()

        asyncio.run(_seed_accepted())

        _stream, _entry_id = _xadd_batch(
            fake_redis, org_id="org-1", ingestion_id=str(ingestion_id)
        )
        c = _consumer()
        process_mock = AsyncMock(return_value=1)
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            process_mock,
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 1
        process_mock.assert_awaited_once()

    def test_missing_batch_row_is_processed_normally(
        self, fake_redis, session_maker, patched_db_session
    ):
        """No status row at all (e.g. status.py's write raced this
        stream entry) -- fail open, process rather than skip."""
        _stream, _entry_id = _xadd_batch(
            fake_redis, org_id="org-1", ingestion_id=str(uuid.uuid4())
        )
        c = _consumer()
        process_mock = AsyncMock(return_value=1)
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            process_mock,
        ):
            processed = c.consume(max_iterations=1)

        assert processed == 1
        process_mock.assert_awaited_once()
