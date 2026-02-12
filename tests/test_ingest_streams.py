from __future__ import annotations

import json

import fakeredis
import pytest

from dev_health_ops.api.ingest.consumer import (
    CONSUMER_GROUP,
    _ensure_group,
    _move_to_dlq,
    _process_entries,
    consume_streams,
)
from dev_health_ops.api.ingest.streams import (
    get_redis_client,
    stream_name,
    write_to_stream,
)


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


class TestGetRedisClient:
    def test_returns_none_when_redis_url_not_set(self, monkeypatch):
        monkeypatch.delenv("REDIS_URL", raising=False)
        assert get_redis_client() is None

    def test_returns_client_when_redis_url_set(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
        client = get_redis_client()
        assert client is not None


class TestWriteToStream:
    def test_returns_false_when_client_is_none(self):
        assert write_to_stream(None, "test-stream", {"key": "val"}) is False

    def test_returns_true_and_writes(self, fake_redis):
        result = write_to_stream(fake_redis, "test-stream", {"key": "val"})
        assert result is True
        entries = fake_redis.xrange("test-stream")
        assert len(entries) == 1
        assert entries[0][1]["key"] == "val"

    def test_returns_false_on_exception(self):
        class BrokenRedis:
            def xadd(self, *args, **kwargs):
                raise ConnectionError("boom")

        assert write_to_stream(BrokenRedis(), "s", {"k": "v"}) is False


class TestStreamName:
    def test_formats_correctly(self):
        assert stream_name("acme", "commits") == "ingest:acme:commits"

    def test_formats_with_hyphens(self):
        assert stream_name("org-1", "pull-requests") == "ingest:org-1:pull-requests"


class TestEnsureGroup:
    def test_handles_already_exists(self, fake_redis):
        fake_redis.xadd("s", {"k": "v"})
        _ensure_group(fake_redis, "s")
        _ensure_group(fake_redis, "s")

    def test_creates_group_with_mkstream(self, fake_redis):
        _ensure_group(fake_redis, "new-stream")


class TestProcessEntries:
    def test_deserializes_valid_payloads(self):
        payload = json.dumps(
            {
                "org_id": "acme",
                "repo_url": "https://github.com/acme/app",
                "items": [
                    {"hash": "abc123", "message": "fix bug"},
                    {"hash": "def456", "message": "add feature"},
                ],
            }
        )
        entries = [
            ("1-0", {"ingestion_id": "ing-1", "payload": payload}),
        ]
        items = _process_entries(entries, "commits")
        assert len(items) == 2
        assert items[0]["hash"] == "abc123"
        assert items[0]["_org_id"] == "acme"
        assert items[0]["_repo_url"] == "https://github.com/acme/app"
        assert items[0]["_ingestion_id"] == "ing-1"
        assert items[1]["hash"] == "def456"

    def test_handles_malformed_json(self):
        entries = [
            ("1-0", {"ingestion_id": "ing-1", "payload": "not-json{{{"}),
        ]
        items = _process_entries(entries, "commits")
        assert items == []

    def test_handles_missing_payload_key(self):
        entries = [
            ("1-0", {"ingestion_id": "ing-1"}),
        ]
        items = _process_entries(entries, "commits")
        assert items == []

    def test_handles_empty_items(self):
        payload = json.dumps({"org_id": "x", "items": []})
        entries = [("1-0", {"ingestion_id": "i", "payload": payload})]
        items = _process_entries(entries, "commits")
        assert items == []


class TestMoveToDlq:
    def test_writes_to_dlq_stream(self, fake_redis):
        _move_to_dlq(fake_redis, "ingest:org:commits", "1-0", "commits")
        entries = fake_redis.xrange("ingest:dlq:commits")
        assert len(entries) == 1
        assert entries[0][1]["original_stream"] == "ingest:org:commits"
        assert entries[0][1]["entry_id"] == "1-0"
        assert "moved_at" in entries[0][1]


class TestConsumeStreams:
    def test_returns_zero_when_redis_unavailable(self, monkeypatch):
        monkeypatch.delenv("REDIS_URL", raising=False)
        assert consume_streams(max_iterations=1) == 0

    def test_processes_entries_and_acks(self, monkeypatch, fake_redis):
        skey = "ingest:default:commits"
        payload = json.dumps(
            {
                "org_id": "default",
                "repo_url": "https://github.com/org/repo",
                "items": [{"hash": "abc", "message": "test"}],
            }
        )
        fake_redis.xadd(skey, {"ingestion_id": "i1", "payload": payload})

        monkeypatch.setattr(
            "dev_health_ops.api.ingest.streams.get_redis_client",
            lambda: fake_redis,
        )

        processed = consume_streams(
            stream_patterns=[skey],
            max_iterations=1,
            consumer_name="test-consumer",
        )
        assert processed == 1

        pending = fake_redis.xpending(skey, CONSUMER_GROUP)
        assert pending["pending"] == 0

    def test_max_iterations_limits_loop(self, monkeypatch, fake_redis):
        monkeypatch.setattr(
            "dev_health_ops.api.ingest.streams.get_redis_client",
            lambda: fake_redis,
        )

        processed = consume_streams(
            stream_patterns=["ingest:default:commits"],
            max_iterations=3,
            consumer_name="test-consumer",
        )
        assert processed == 0

    def test_processes_multiple_streams(self, monkeypatch, fake_redis):
        """Verify consumer handles multiple stream patterns.

        fakeredis xreadgroup only returns entries from the first stream
        in a multi-stream call, so we test each stream individually
        to confirm the consumer correctly sets up groups and processes
        entries from any matching stream.
        """
        for entity in ("commits", "incidents"):
            skey = f"ingest:default:{entity}"
            payload = json.dumps(
                {
                    "org_id": "default",
                    "repo_url": "",
                    "items": [{"id": entity}],
                }
            )
            fake_redis.xadd(skey, {"ingestion_id": f"i-{entity}", "payload": payload})

        monkeypatch.setattr(
            "dev_health_ops.api.ingest.streams.get_redis_client",
            lambda: fake_redis,
        )
        monkeypatch.setattr("dev_health_ops.api.ingest.consumer.BLOCK_MS", 100)

        total = 0
        for entity in ("commits", "incidents"):
            processed = consume_streams(
                stream_patterns=[f"ingest:default:{entity}"],
                max_iterations=1,
                consumer_name="test-multi",
            )
            total += processed

        assert total == 2
