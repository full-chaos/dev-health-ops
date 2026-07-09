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
from typing import Any
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
# Same rationale, for the module-wide `_resolve_mark_batch_failed` autouse
# stub (CHAOS-2697: the processor module exists now, so the real resolver
# would hand any give-up path the REAL Postgres-backed mark_batch_failed).
_REAL_RESOLVE_MARK_BATCH_FAILED = (
    consumer_mod.ExternalIngestStreamConsumer._resolve_mark_batch_failed
)


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


@pytest.fixture(autouse=True)
def _mark_batch_failed_unresolvable(monkeypatch):
    """CHAOS-2697's processor module exists now, so the real
    ``_resolve_mark_batch_failed()`` would resolve the REAL Postgres-backed
    ``mark_batch_failed`` inside any test whose give-up path runs -- reaching
    for a live database from a unit test. Default every test to the
    unresolvable branch (the pre-2697 behavior these mechanics tests were
    written against); the tests exercising the resolution contract itself
    restore ``_REAL_RESOLVE_MARK_BATCH_FAILED`` and install a fake module."""
    monkeypatch.setattr(
        consumer_mod.ExternalIngestStreamConsumer,
        "_resolve_mark_batch_failed",
        staticmethod(lambda: None),
    )


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
        # Restore the real resolver (the autouse stub defaults it to
        # unresolvable) and inject a stand-in module so we can assert the
        # give-up path resolves and calls mark_batch_failed exactly as CC23
        # pins -- without the real Postgres-backed implementation running.
        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_resolve_mark_batch_failed",
            staticmethod(_REAL_RESOLVE_MARK_BATCH_FAILED),
        )
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

    def test_mark_batch_failed_raising_leaves_entry_unacked(
        self, fake_redis, monkeypatch
    ):
        """Adversarial-review rounds 1-3 (CHAOS-2697): when the failed-status
        write raises, the entry must NOT be ACKed (the batch would strand
        non-terminal with the entry gone), the DLQ row IS written (DLQ-first
        ordering -- round 3: mark-first lost the row entirely when the XADD
        failed after a successful mark), and retry cycles must NOT
        accumulate duplicate DLQ rows (round 2: the per-entry marker
        dedups)."""
        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_resolve_mark_batch_failed",
            staticmethod(_REAL_RESOLVE_MARK_BATCH_FAILED),
        )
        fake_processor = types.ModuleType("dev_health_ops.external_ingest.processor")
        mark_failed = AsyncMock(side_effect=RuntimeError("pg down"))
        setattr(fake_processor, "mark_batch_failed", mark_failed)
        monkeypatch.setitem(
            sys.modules, "dev_health_ops.external_ingest.processor", fake_processor
        )

        stream, _entry_id = _xadd_batch(
            fake_redis, org_id="org-mark-raises", ingestion_id="ingest-mark-raises"
        )
        c = _consumer(reclaim_idle_ms=0)
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=PermanentProcessingError("nope")),
        ):
            c.consume(max_iterations=1)
            # Retry cycle (reclaim redelivers the still-pending entry; the
            # sleep is the fakeredis XPENDING-idle requirement documented in
            # TestReclaim): the mark fails again, and the marker must
            # suppress a duplicate DLQ row.
            time.sleep(0.01)
            c.consume(max_iterations=1)

        assert mark_failed.await_count == 2
        # DLQ-first ordering: the row IS written (round 3: mark-first lost it
        # when the XADD failed after the mark landed) -- exactly once across
        # both attempts (round 2: marker dedup)...
        dlq_entries = fake_redis.xrange("external-ingest:org-mark-raises:dlq")
        assert len(dlq_entries) == 1
        # ...and the source entry remains pending (NOT acked) for a retry.
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        assert len(pending) == 1

    def test_missing_processor_module_does_not_crash_consumer(
        self, fake_redis, monkeypatch
    ):
        """Rollback scenario: if a deploy removes CHAOS-2697's processor.py
        again, the give-up path must log and continue, not crash the consumer
        loop. Simulated by poisoning the sys.modules entry (a ``None`` value
        makes ``import`` raise ImportError) under the REAL resolver."""
        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_resolve_mark_batch_failed",
            staticmethod(_REAL_RESOLVE_MARK_BATCH_FAILED),
        )
        monkeypatch.setitem(
            sys.modules, "dev_health_ops.external_ingest.processor", None
        )
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

        # Second pass: reclaim_stale() picks it up, process_entry now
        # succeeds. The sleep is load-bearing: fakeredis's XPENDING IDLE
        # filter requires actual elapsed wall-clock ms (idle=0 does NOT mean
        # "always eligible" -- see the identical note in the give-up test
        # below); back-to-back consume() calls can land in the same 0ms tick
        # under -n-distributed parallel load, making reclaim find nothing.
        time.sleep(0.01)
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
        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_resolve_mark_batch_failed",
            staticmethod(_REAL_RESOLVE_MARK_BATCH_FAILED),
        )
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

    def test_give_up_with_failing_mark_batch_failed_leaves_entry_unacked(
        self, fake_redis, monkeypatch
    ):
        """CHAOS-2697 adversarial-review HIGH: the sync give-up path
        (``reclaim_stale() -> move_to_dlq``) must NOT ACK when
        ``mark_batch_failed`` raised -- ACKing would strand the batch in a
        non-terminal status with its only retry handle gone (a ``processing``
        row REPLAYs, never RETRYs). Rounds 2-3 refinement: the DLQ row IS
        written (DLQ-first; mark-first lost it on XADD failure) but exactly
        once across retry cycles (marker dedup -- outage retries previously
        flooded the capped DLQ). Sync twin of
        ``test_mark_batch_failed_raising_leaves_entry_unacked`` above."""
        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_resolve_mark_batch_failed",
            staticmethod(_REAL_RESOLVE_MARK_BATCH_FAILED),
        )
        fake_processor = types.ModuleType("dev_health_ops.external_ingest.processor")
        mark_failed = AsyncMock(side_effect=RuntimeError("pg down"))
        setattr(fake_processor, "mark_batch_failed", mark_failed)
        monkeypatch.setitem(
            sys.modules, "dev_health_ops.external_ingest.processor", fake_processor
        )

        stream, entry_id = _xadd_batch(fake_redis, org_id="org-giveup-markfail")
        c = _consumer(reclaim_idle_ms=0, max_deliveries=1)

        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_process_entry_async",
            AsyncMock(side_effect=RuntimeError("still broken")),
        ):
            # iter 1: fresh read, delivery #1, fails transiently, pending.
            c.consume(max_iterations=1)
            # iter 2: reclaim_stale sees delivery #1 >= max_deliveries(1) ->
            # give-up -> DLQ write SUCCEEDS but mark_batch_failed raises ->
            # move_to_dlq must return False -> entry must stay pending.
            time.sleep(0.01)
            c.consume(max_iterations=1)
            # iter 3: give-up retried -- the marker must dedup the DLQ row
            # while the mark keeps failing.
            time.sleep(0.01)
            c.consume(max_iterations=1)

        assert mark_failed.await_count == 2
        # DLQ-first: exactly one DLQ row across both give-up attempts...
        dlq_entries = fake_redis.xrange("external-ingest:org-giveup-markfail:dlq")
        assert len(dlq_entries) == 1
        pending = fake_redis.xpending_range(
            stream, consumer_mod.CONSUMER_GROUP, min="-", max="+", count=10
        )
        # ...and the source entry keeps its retry handle for the status write.
        assert len(pending) == 1
        assert pending[0]["message_id"] == entry_id

    def test_give_up_transient_field_reread_failure_does_not_ack(self, fake_redis):
        """CHAOS-2697 adversarial-review round-2 HIGH: a transient XRANGE
        failure while re-reading the entry's fields must not be treated as
        safe-to-ACK -- the batch would never be marked failed and its retry
        handle would vanish. move_to_dlq must return False (retry later)."""
        stream, entry_id = _xadd_batch(fake_redis, org_id="org-xrange-fail")
        c = _consumer()
        with patch.object(
            consumer_mod.ExternalIngestStreamConsumer,
            "_fetch_entry_fields",
            lambda self, rc, sk, eid: None,
        ):
            assert c.move_to_dlq(fake_redis, stream, entry_id, "gave up") is False
        assert fake_redis.xrange("external-ingest:org-xrange-fail:dlq") == []

    def test_give_up_trimmed_entry_writes_tombstone_and_acks(self, fake_redis):
        """An entry MAXLEN-trimmed while pending has no recoverable fields:
        nothing addressable to mark, so the give-up path writes a tombstone
        DLQ row and reports safe-to-ACK -- the possibly-stranded batch row is
        the CHAOS-2769 orphan reconciler's target, not a reason to retry a
        re-read that is authoritatively empty forever."""
        c = _consumer()
        stream = "external-ingest:org-trimmed:batches"
        # Entry ID that was never XADDed: XRANGE succeeds and returns [].
        assert c.move_to_dlq(fake_redis, stream, "0-1", "gave up") is True
        dlq_entries = fake_redis.xrange("external-ingest:org-trimmed:dlq")
        assert len(dlq_entries) == 1
        assert dlq_entries[0][1]["ingestion_id"] == ""
        assert dlq_entries[0][1]["org_id"] == "org-trimmed"


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

    def test_broken_dlq_xadd_never_marks_batch_failed(self, monkeypatch):
        """CHAOS-2697 adversarial round 3: with DLQ-first ordering, a failed
        DLQ XADD must leave the batch UNMARKED. Marking first would let the
        idempotent-skip guard ACK the redelivered entry (the batch is now
        terminal) before the DLQ write is ever retried -- silent DLQ loss.
        Once the DLQ write recovers, the mark proceeds and the entry may be
        ACKed."""

        class FlakyDlq(FakeValkey):
            fail_next = True

            def xadd(self, name, *args, **kwargs):
                if name.endswith(":dlq") and FlakyDlq.fail_next:
                    FlakyDlq.fail_next = False
                    raise ConnectionError("dlq blip")
                return super().xadd(name, *args, **kwargs)

        FlakyDlq.fail_next = True
        # Any: valkey's sync xrange is typed Awaitable-or-value; the untyped
        # fake fixtures elsewhere in this file get the same treatment.
        rc: Any = FlakyDlq(decode_responses=True)
        stream, entry_id = _xadd_batch(rc, org_id="org-dlq-flaky")

        monkeypatch.setattr(
            consumer_mod.ExternalIngestStreamConsumer,
            "_resolve_mark_batch_failed",
            staticmethod(_REAL_RESOLVE_MARK_BATCH_FAILED),
        )
        fake_processor = types.ModuleType("dev_health_ops.external_ingest.processor")
        mark_failed = AsyncMock()
        setattr(fake_processor, "mark_batch_failed", mark_failed)
        monkeypatch.setitem(
            sys.modules, "dev_health_ops.external_ingest.processor", fake_processor
        )
        c = _consumer()

        # Attempt 1: XADD fails -> not safe to ACK, and crucially the batch
        # was never marked (nothing for the terminal-skip guard to see).
        assert c.move_to_dlq(rc, stream, entry_id, "gave up") is False
        mark_failed.assert_not_awaited()
        assert rc.xrange("external-ingest:org-dlq-flaky:dlq") == []

        # Attempt 2 (retry): DLQ recovers -> row written -> mark runs ->
        # safe to ACK.
        assert c.move_to_dlq(rc, stream, entry_id, "gave up") is True
        mark_failed.assert_awaited_once()
        dlq_entries = rc.xrange("external-ingest:org-dlq-flaky:dlq")
        assert len(dlq_entries) == 1

    def test_marker_hit_rewrites_when_original_dlq_row_trimmed(self, fake_redis):
        """CHAOS-2697 adversarial round 4 HIGH: the dedup marker must not
        suppress a fresh DLQ write once the original row has been trimmed
        from the capped stream. Otherwise a mark/ACK after trimming leaves a
        failed batch with no surviving DLQ record -- silent DLQ loss under
        sustained pressure. On a marker hit the code verifies the stored
        stream-id is still present and, if trimmed, writes a fresh row."""
        stream, entry_id = _xadd_batch(fake_redis, org_id="org-dlq-trim")
        c = _consumer()
        dlq = "external-ingest:org-dlq-trim:dlq"

        assert c.move_to_dlq(fake_redis, stream, entry_id, "gave up") is True
        first = fake_redis.xrange(dlq)
        assert len(first) == 1
        marker = f"{dlq}:written:{entry_id}"
        # The marker stores the DLQ stream-id, not a bare flag.
        assert fake_redis.get(marker) == first[0][0]

        # Simulate the capped stream trimming the original row out.
        fake_redis.xdel(dlq, first[0][0])
        assert fake_redis.xrange(dlq) == []

        # Retry: marker still points at the now-gone id -> must write fresh.
        # The empty-then-non-empty transition proves a fresh XADD happened
        # (a marker-only short-circuit would leave the stream empty -- the
        # round-4 silent-loss bug). Not asserting id-inequality: fakeredis
        # resets a drained stream's auto-id to <ms>-0 and can collide within
        # one millisecond, unlike real Redis's monotonic last_id.
        assert c.move_to_dlq(fake_redis, stream, entry_id, "gave up") is True
        second = fake_redis.xrange(dlq)
        assert len(second) == 1
        assert fake_redis.get(marker) == second[0][0]

    def test_marker_hit_with_surviving_row_does_not_duplicate(self, fake_redis):
        """The dedup still holds when the original row survives (round-2
        property preserved by the round-4 change): a marker hit whose stored
        id is still present short-circuits without a second XADD."""
        stream, entry_id = _xadd_batch(fake_redis, org_id="org-dlq-keep")
        c = _consumer()
        dlq = "external-ingest:org-dlq-keep:dlq"

        assert c.move_to_dlq(fake_redis, stream, entry_id, "gave up") is True
        assert c.move_to_dlq(fake_redis, stream, entry_id, "gave up") is True
        assert len(fake_redis.xrange(dlq)) == 1


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
        exists now, so the deployment-order guard genuinely passes — this is
        the assertion that "merging processor.py arms the consumer". Uses the
        reference captured at collection time (module docstring note above)
        since the autouse fixture in this file replaces
        ``consumer_mod._processor_available`` for every other test."""
        assert _REAL_PROCESSOR_AVAILABLE() is True


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
