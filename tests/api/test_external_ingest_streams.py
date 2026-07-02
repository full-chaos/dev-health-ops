"""Tests for the external-ingest durable-stream producer (CHAOS-2693).

Covers D1 (per-org naming), D2 (pointer-only -- no payload on the stream),
D3 (fail-closed StreamUnavailableError), the fail-closed payload-durability
invariant (team-lead-authorized amendment, see PR), and the
reenqueue_batch() seam.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

fakeredis = pytest.importorskip("fakeredis")
from fakeredis import FakeValkey  # noqa: E402

from dev_health_ops.api.external_ingest import streams

# Captured at collection time (before the module-wide `_payload_durable`
# autouse fixture below ever runs) so TestPayloadDurabilityInvariant can
# restore the real implementation regardless of fixture ordering.
_REAL_REQUIRE_PAYLOAD_DURABLE = streams._require_payload_durable


@pytest.fixture
def fake_redis():
    return FakeValkey(decode_responses=True)


@pytest.fixture
def patched_client(monkeypatch, fake_redis):
    monkeypatch.setattr(streams, "get_redis_client", lambda: fake_redis)
    return fake_redis


@pytest.fixture(autouse=True)
def _payload_durable(monkeypatch):
    """Default every test to "the payload row already exists" -- most tests
    here exercise the Redis/XADD side; the fail-closed payload-durability
    check itself has its own dedicated test class below."""
    monkeypatch.setattr(
        streams, "_require_payload_durable", AsyncMock(return_value=None)
    )


class TestNaming:
    def test_stream_name_is_per_org_batches(self):
        assert streams.stream_name("org-1") == "external-ingest:org-1:batches"

    def test_dlq_name_is_per_org_dlq(self):
        assert streams.dlq_name("org-1") == "external-ingest:org-1:dlq"

    def test_two_orgs_get_distinct_streams(self):
        assert streams.stream_name("org-a") != streams.stream_name("org-b")
        assert streams.dlq_name("org-a") != streams.dlq_name("org-b")


class TestEnqueueBatch:
    def _kwargs(self, **overrides):
        base = dict(
            org_id="org-1",
            ingestion_id="ingest-1",
            source_system="github",
            source_instance="acme/api",
            schema_version="external-ingest.v1",
            idempotency_key="key-1",
            record_count=3,
            window_started_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            window_ended_at=datetime(2026, 7, 1, 1, tzinfo=timezone.utc),
        )
        base.update(overrides)
        return base

    @pytest.mark.asyncio
    async def test_writes_one_entry_with_pointer_fields(
        self, patched_client, fake_redis
    ):
        stream = await streams.enqueue_batch(**self._kwargs())

        assert stream == "external-ingest:org-1:batches"
        entries = fake_redis.xrange(stream)
        assert len(entries) == 1
        _entry_id, data = entries[0]
        assert data["ingestion_id"] == "ingest-1"
        assert data["org_id"] == "org-1"
        assert data["source_system"] == "github"
        assert data["source_instance"] == "acme/api"
        assert data["schema_version"] == "external-ingest.v1"
        assert data["idempotency_key"] == "key-1"
        assert data["record_count"] == "3"
        assert data["window_started_at"] == "2026-07-01T00:00:00+00:00"
        assert data["window_ended_at"] == "2026-07-01T01:00:00+00:00"
        assert "enqueued_at" in data

    @pytest.mark.asyncio
    async def test_never_writes_payload_to_the_stream(self, patched_client, fake_redis):
        """D2: the stream carries a pointer only -- the payload lives in
        Postgres, not on the entry (there's no ``payload``/``payload_json``
        kwarg on ``enqueue_batch`` at all anymore)."""
        stream = await streams.enqueue_batch(**self._kwargs())
        _entry_id, data = fake_redis.xrange(stream)[0]
        assert "payload" not in data
        assert "payload_json" not in data

    @pytest.mark.asyncio
    async def test_maxlen_approximate_passed_on_every_xadd(self, monkeypatch):
        calls = []

        class SpyClient:
            def xadd(self, stream, fields, maxlen=None, approximate=None):
                calls.append({"maxlen": maxlen, "approximate": approximate})

        monkeypatch.setattr(streams, "get_redis_client", lambda: SpyClient())
        await streams.enqueue_batch(**self._kwargs())

        assert calls == [{"maxlen": streams.STREAM_MAXLEN, "approximate": True}]

    @pytest.mark.asyncio
    async def test_raises_stream_unavailable_when_redis_url_unset(self, monkeypatch):
        monkeypatch.setattr(streams, "get_redis_client", lambda: None)
        with pytest.raises(streams.StreamUnavailableError):
            await streams.enqueue_batch(**self._kwargs())

    @pytest.mark.asyncio
    async def test_raises_stream_unavailable_on_xadd_failure(self, monkeypatch):
        class BrokenClient:
            def xadd(self, *args, **kwargs):
                raise ConnectionError("boom")

        monkeypatch.setattr(streams, "get_redis_client", lambda: BrokenClient())
        with pytest.raises(streams.StreamUnavailableError):
            await streams.enqueue_batch(**self._kwargs())

    @pytest.mark.asyncio
    async def test_xadd_failure_is_not_silently_swallowed(self, monkeypatch):
        """Regression test for D3/fail-closed: a raising xadd must propagate
        as StreamUnavailableError, never return a falsy sentinel."""
        calls = {"count": 0}

        class BrokenClient:
            def xadd(self, *args, **kwargs):
                calls["count"] += 1
                raise TimeoutError("no ack from valkey")

        monkeypatch.setattr(streams, "get_redis_client", lambda: BrokenClient())
        with pytest.raises(streams.StreamUnavailableError):
            await streams.enqueue_batch(**self._kwargs())
        assert calls["count"] == 1


class TestPayloadDurabilityInvariant:
    """team-lead-authorized amendment: enqueue_batch() must fail closed if
    the payload row is not (yet) durable -- this converts what was a
    deployment-sequencing hazard into a standing correctness guarantee.

    Restores the real ``_require_payload_durable`` (undoing the module-wide
    autouse bypass above) so these tests exercise the actual check.
    """

    @pytest.fixture(autouse=True)
    def _use_real_check(self, monkeypatch):
        monkeypatch.setattr(
            streams, "_require_payload_durable", _REAL_REQUIRE_PAYLOAD_DURABLE
        )

    def _kwargs(self, **overrides):
        base = dict(
            org_id="org-1",
            ingestion_id="ingest-1",
            source_system="github",
            source_instance="acme/api",
            schema_version="external-ingest.v1",
            idempotency_key="key-1",
            record_count=1,
        )
        base.update(overrides)
        return base

    class _FakeSessionCtx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *exc):
            return False

    class _FakeSessionCtxRaisesOnEnter:
        async def __aenter__(self):
            raise ConnectionError("postgres down")

        async def __aexit__(self, *exc):
            return False

    @pytest.mark.asyncio
    async def test_raises_when_payload_row_missing(self, monkeypatch, patched_client):
        async def _fake_payload_exists(session, *, ingestion_id, org_id):
            return False

        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session", self._FakeSessionCtx
        )
        monkeypatch.setattr(
            "dev_health_ops.external_ingest.payload_store.payload_exists",
            _fake_payload_exists,
        )

        with pytest.raises(streams.StreamUnavailableError, match="payload row missing"):
            await streams.enqueue_batch(**self._kwargs())

    @pytest.mark.asyncio
    async def test_does_not_write_to_redis_when_payload_missing(
        self, monkeypatch, fake_redis
    ):
        """The fail-closed check must run BEFORE the XADD -- a missing
        payload row must never produce a pointer on the stream at all."""

        async def _fake_payload_exists(session, *, ingestion_id, org_id):
            return False

        monkeypatch.setattr(streams, "get_redis_client", lambda: fake_redis)
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session", self._FakeSessionCtx
        )
        monkeypatch.setattr(
            "dev_health_ops.external_ingest.payload_store.payload_exists",
            _fake_payload_exists,
        )

        with pytest.raises(streams.StreamUnavailableError):
            await streams.enqueue_batch(**self._kwargs())

        assert fake_redis.xrange(streams.stream_name("org-1")) == []

    @pytest.mark.asyncio
    async def test_raises_when_durability_check_itself_errors(self, monkeypatch):
        """Cannot verify durability (e.g. Postgres down) -> fail closed too,
        same as "row absent": never enqueue a pointer we can't vouch for."""
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session",
            self._FakeSessionCtxRaisesOnEnter,
        )

        with pytest.raises(streams.StreamUnavailableError):
            await streams.enqueue_batch(**self._kwargs())

    @pytest.mark.asyncio
    async def test_proceeds_when_payload_row_present(
        self, monkeypatch, patched_client, fake_redis
    ):
        async def _fake_payload_exists(session, *, ingestion_id, org_id):
            return True

        monkeypatch.setattr(streams, "get_redis_client", lambda: patched_client)
        monkeypatch.setattr(
            "dev_health_ops.db.get_postgres_session", self._FakeSessionCtx
        )
        monkeypatch.setattr(
            "dev_health_ops.external_ingest.payload_store.payload_exists",
            _fake_payload_exists,
        )

        stream = await streams.enqueue_batch(**self._kwargs())
        assert fake_redis.xrange(stream)


class TestReenqueueBatch:
    @pytest.mark.asyncio
    async def test_delegates_to_enqueue_batch_same_ids(
        self, patched_client, fake_redis
    ):
        stream = await streams.reenqueue_batch(
            org_id="org-1",
            ingestion_id="ingest-1",
            source_system="github",
            source_instance="acme/api",
            schema_version="external-ingest.v1",
            idempotency_key="key-1",
            record_count=3,
        )
        assert stream == "external-ingest:org-1:batches"
        _entry_id, data = fake_redis.xrange(stream)[0]
        assert data["ingestion_id"] == "ingest-1"
        assert data["org_id"] == "org-1"

    @pytest.mark.asyncio
    async def test_propagates_stream_unavailable(self, monkeypatch):
        monkeypatch.setattr(streams, "get_redis_client", lambda: None)
        with pytest.raises(streams.StreamUnavailableError):
            await streams.reenqueue_batch(
                org_id="org-1",
                ingestion_id="ingest-1",
                source_system="github",
                source_instance="acme/api",
                schema_version="external-ingest.v1",
                idempotency_key="key-1",
                record_count=3,
            )
