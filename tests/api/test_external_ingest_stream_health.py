"""Tests for external-ingest stream liveness/lag observability (CHAOS-2693 D9)."""

from __future__ import annotations

import pytest

fakeredis = pytest.importorskip("fakeredis")
from fakeredis import FakeValkey  # noqa: E402

from dev_health_ops.api.external_ingest import stream_health
from dev_health_ops.api.external_ingest.streams import CONSUMER_GROUP


@pytest.fixture
def fake_redis():
    return FakeValkey(decode_responses=True)


@pytest.fixture(autouse=True)
def _patch_client(monkeypatch, fake_redis):
    monkeypatch.setattr(stream_health, "get_redis_client", lambda: fake_redis)


class TestReportStreamHealth:
    def test_empty_when_no_streams_exist(self):
        assert stream_health.report_stream_health() == {"streams": []}

    def test_returns_none_streams_when_redis_unavailable(self, monkeypatch):
        monkeypatch.setattr(stream_health, "get_redis_client", lambda: None)
        assert stream_health.report_stream_health() == {"streams": []}

    def test_reports_depth_and_pending_for_batches_stream(self, fake_redis):
        stream = "external-ingest:org-1:batches"
        fake_redis.xadd(stream, {"a": "1"})
        fake_redis.xadd(stream, {"a": "2"})
        fake_redis.xgroup_create(stream, CONSUMER_GROUP, id="0")
        fake_redis.xreadgroup(CONSUMER_GROUP, "c1", streams={stream: ">"}, count=10)

        result = stream_health.report_stream_health()

        assert len(result["streams"]) == 1
        stats = result["streams"][0]
        assert stats["stream"] == stream
        assert stats["is_dlq"] is False
        assert stats["depth"] == 2
        assert stats["pending"] == 2
        assert stats["oldest_pending_idle_ms"] is not None

    def test_reports_dlq_depth_without_pending_lookup(self, fake_redis):
        dlq = "external-ingest:org-1:dlq"
        fake_redis.xadd(dlq, {"reason": "poison"})

        result = stream_health.report_stream_health()

        assert len(result["streams"]) == 1
        stats = result["streams"][0]
        assert stats["stream"] == dlq
        assert stats["is_dlq"] is True
        assert stats["depth"] == 1
        assert stats["pending"] == 0
        assert stats["oldest_pending_idle_ms"] is None

    def test_warns_above_depth_threshold(self, fake_redis, caplog):
        stream = "external-ingest:org-1:batches"
        for i in range(stream_health.STREAM_DEPTH_WARNING_THRESHOLD + 1):
            fake_redis.xadd(stream, {"i": str(i)})

        with caplog.at_level("WARNING"):
            stream_health.report_stream_health()

        assert any(
            r.message == "external_ingest_stream_backlog" for r in caplog.records
        )

    def test_multiple_orgs_all_reported(self, fake_redis):
        fake_redis.xadd("external-ingest:org-a:batches", {"a": "1"})
        fake_redis.xadd("external-ingest:org-b:batches", {"a": "1"})

        result = stream_health.report_stream_health()

        streams = {s["stream"] for s in result["streams"]}
        assert streams == {
            "external-ingest:org-a:batches",
            "external-ingest:org-b:batches",
        }
