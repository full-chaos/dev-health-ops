from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from dev_health_ops.api.product_telemetry.consumer import (
    _ensure_group,
    consume_product_telemetry_streams,
)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[Any]], list[str]]] = []

    def insert(
        self, table: str, rows: list[list[Any]], column_names: list[str]
    ) -> None:
        self.inserts.append((table, rows, column_names))


class FakeSink:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, str]]],
        xgroup_error: Exception | None = None,
    ) -> None:
        self.entries = entries
        self.xgroup_error = xgroup_error
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []
        self.dlq: list[tuple[str, dict[str, str]]] = []

    def scan_iter(self, match: str, _type: str | None = None) -> list[str]:
        return ["product-telemetry:org_hash_123:events"]

    def xgroup_create(
        self, stream_key: str, group: str, id: str = "0", mkstream: bool = True
    ) -> None:
        if self.xgroup_error is not None:
            raise self.xgroup_error
        return None

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        entries = self.entries
        self.entries = []
        if not entries:
            return []
        return [("product-telemetry:org_hash_123:events", entries)]

    def xack(self, stream_key: str, group: str, *entry_ids: str) -> None:
        self.acked.append((stream_key, group, entry_ids))

    def xadd(self, stream_key: str, data: dict[str, str]) -> None:
        self.dlq.append((stream_key, data))


def _event(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": "chart_interacted",
        "schemaVersion": "2026-05-telemetry-v1",
        "eventId": "evt_123",
        "ts": "2026-05-25T12:00:00Z",
        "sessionId": "ses_123",
        "anonymousUserId": "anon_123",
        "orgIdHash": "org_hash_123",
        "routePattern": "/metrics",
        "payload": payload
        or {
            "chart": "quadrant",
            "action": "overlay_toggled",
            "surface": "metrics",
            "scope": "org",
        },
    }


def _entry(entry_id: str, event: dict[str, Any]) -> tuple[str, dict[str, str]]:
    return (
        entry_id,
        {
            "ingestion_id": "ing_123",
            "source": "dev-health-web",
            "org_id_hash": "org_hash_123",
            "events": json.dumps([event]),
        },
    )


def test_product_telemetry_consumer_persists_sanitized_payload_json(
    monkeypatch,
) -> None:
    redis = FakeRedis([_entry("1-0", _event())])
    clickhouse = FakeClickHouseClient()

    monkeypatch.setattr(
        "dev_health_ops.api._stream_consumer.get_consumer_redis_client", lambda: redis
    )
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.persist.create_sink",
        lambda: FakeSink(clickhouse),
    )

    processed = consume_product_telemetry_streams(max_iterations=1)

    assert processed == 1
    assert redis.acked == [
        (
            "product-telemetry:org_hash_123:events",
            "product-telemetry-consumers",
            ("1-0",),
        )
    ]
    assert redis.dlq == []
    table, rows, columns = clickhouse.inserts[0]
    assert table == "product_telemetry_events"
    row = dict(zip(columns, rows[0]))
    assert row["org_id_hash"] == "org_hash_123"
    assert row["event_id"] == "evt_123"
    assert row["payload_json"] == json.dumps(
        {
            "chart": "quadrant",
            "action": "overlay_toggled",
            "surface": "metrics",
            "scope": "org",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    assert isinstance(row["occurred_at"], datetime)


def test_product_telemetry_consumer_coerces_missing_org_hash_to_empty_string(
    monkeypatch,
) -> None:
    event = _event()
    event.pop("orgIdHash")
    redis = FakeRedis([_entry("1-0", event)])
    clickhouse = FakeClickHouseClient()

    monkeypatch.setattr(
        "dev_health_ops.api._stream_consumer.get_consumer_redis_client", lambda: redis
    )
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.persist.create_sink",
        lambda: FakeSink(clickhouse),
    )

    processed = consume_product_telemetry_streams(max_iterations=1)

    assert processed == 1
    _table, rows, columns = clickhouse.inserts[0]
    row = dict(zip(columns, rows[0]))
    assert row["org_id_hash"] == ""


def test_product_telemetry_consumer_rejects_blocked_payload_keys_to_dlq(
    monkeypatch,
) -> None:
    redis = FakeRedis([_entry("1-0", _event({"email": "ada@example.com", "ok": True}))])
    clickhouse = FakeClickHouseClient()

    monkeypatch.setattr(
        "dev_health_ops.api._stream_consumer.get_consumer_redis_client", lambda: redis
    )
    monkeypatch.setattr(
        "dev_health_ops.api.product_telemetry.persist.create_sink",
        lambda: FakeSink(clickhouse),
    )

    processed = consume_product_telemetry_streams(max_iterations=1)

    assert processed == 0
    assert clickhouse.inserts == []
    assert redis.acked == [
        (
            "product-telemetry:org_hash_123:events",
            "product-telemetry-consumers",
            ("1-0",),
        )
    ]
    assert redis.dlq
    dlq_stream, dlq_data = redis.dlq[0]
    assert dlq_stream == "product-telemetry:dlq"
    assert dlq_data["original_stream"] == "product-telemetry:org_hash_123:events"
    assert dlq_data["entry_id"] == "1-0"


def test_product_telemetry_consumer_ignores_existing_consumer_group() -> None:
    redis = FakeRedis(
        [], xgroup_error=Exception("BUSYGROUP Consumer Group name already exists")
    )

    _ensure_group(redis, "product-telemetry:org_hash_123:events")


def test_product_telemetry_consumer_raises_unexpected_consumer_group_error() -> None:
    redis = FakeRedis([], xgroup_error=Exception("connection refused"))

    try:
        _ensure_group(redis, "product-telemetry:org_hash_123:events")
    except Exception as exc:
        assert str(exc) == "connection refused"
    else:
        raise AssertionError("expected _ensure_group to re-raise unexpected errors")
