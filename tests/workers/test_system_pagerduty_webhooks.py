from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from dev_health_ops.workers import system_webhooks


class _RetrySentinel(Exception):
    pass


def _retry_sentinel(*, exc: BaseException, countdown: int) -> None:
    raise _RetrySentinel from exc


def _worker_context() -> SimpleNamespace:
    return SimpleNamespace(
        org_id="org-1",
        binding_id="binding-1",
        provider_instance_id="source-1",
        credential_id="credential-1",
    )


def _run(coroutine: Any) -> Any:
    async def await_coroutine() -> Any:
        return await coroutine

    return anyio.run(await_coroutine)


def test_pagerduty_worker_retries_when_configuration_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PAGERDUTY_API_TOKEN", raising=False)
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)

    with (
        patch.object(
            system_webhooks.process_pagerduty_webhook_event, "retry", _retry_sentinel
        ),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ),
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=_run,
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                binding_id="binding-1",
                stream_entry_id="1-0",
            )


def test_pagerduty_worker_deletes_stream_entry_only_after_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PAGERDUTY_API_TOKEN", raising=False)
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")

    class Redis:
        deleted: list[tuple[str, str]] = []
        requested: list[str] = []

        def xrange(
            self, stream: str, *_: object, **__: object
        ) -> list[tuple[str, dict[str, str]]]:
            self.requested.append(stream)
            return [
                (
                    "1-0",
                    {
                        "payload": json.dumps(
                            {
                                "event": {
                                    "id": "event-1",
                                    "event_type": "incident.triggered",
                                    "occurred_at": "2026-07-17T12:00:00Z",
                                    "data": {
                                        "id": "incident-1",
                                        "title": "Payments unavailable",
                                        "status": "triggered",
                                        "created_at": "2026-07-17T12:00:00Z",
                                    },
                                }
                            }
                        ),
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def xdel(self, stream: str, entry_id: str) -> None:
            self.deleted.append((stream, entry_id))

    redis = Redis()
    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.reconcile_pagerduty_webhook_with_locked_graph",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=_run,
        ) as run_async,
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ),
        patch(
            "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
            return_value=True,
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        result = task.run(
            binding_id="binding-1",
            stream_entry_id="1-0",
        )

    assert result["processed"] is True
    assert run_async.call_count == 2
    assert redis.requested == ["pagerduty-webhooks:binding-1"]
    assert redis.deleted == [("pagerduty-webhooks:binding-1", "1-0")]


def test_pagerduty_worker_feature_gate_prevents_binding_credential_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")

    class Redis:
        deleted: list[tuple[str, str]] = []

        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
                        "event_id": "event-1",
                        "payload": json.dumps(
                            {
                                "event": {
                                    "id": "event-1",
                                    "event_type": "incident.triggered",
                                    "occurred_at": "2026-07-17T12:00:00Z",
                                    "data": {
                                        "id": "incident-1",
                                        "title": "Payments unavailable",
                                        "status": "triggered",
                                        "created_at": "2026-07-17T12:00:00Z",
                                    },
                                }
                            }
                        ),
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def xadd(self, *_: object, **__: object) -> str:
            return "2-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            self.deleted.append((stream, entry_id))

    redis = Redis()
    with (
        patch("dev_health_ops.api.ingest.streams.get_redis_client", return_value=redis),
        patch(
            "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
            return_value=False,
        ),
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            side_effect=_run,
        ) as run_async,
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.load_pagerduty_webhook_auth",
            new=AsyncMock(),
        ) as load_token,
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(RuntimeError, match="persistence exhausted"):
            task.run(
                binding_id="binding-1",
                stream_entry_id="1-0",
            )

    run_async.assert_called_once()
    load_token.assert_not_awaited()
    assert redis.deleted == [("pagerduty-webhooks:binding-1", "1-0")]


def test_pagerduty_worker_moves_exhausted_entry_to_dlq_before_deleting_source() -> None:
    writes: list[tuple[str, dict[str, str]]] = []
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            writes.append((stream, fields))
            return "2-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    task = SimpleNamespace(
        max_retries=3,
        request=SimpleNamespace(id="task-1", retries=3),
    )

    with pytest.raises(RuntimeError, match="persistence exhausted"):
        system_webhooks._retry_or_dead_letter_pagerduty_webhook(
            task=task,
            redis_client=Redis(),
            stream_name="pagerduty-webhooks:binding-1",
            stream_entry_id="1-0",
            fields={"event_id": "event-1", "payload": "{}"},
            error=RuntimeError("ClickHouse unavailable"),
        )

    assert writes[0][0] == "pagerduty-webhooks:binding-1:dlq"
    assert writes[0][1]["event_id"] == "event-1"
    assert deleted == [("pagerduty-webhooks:binding-1", "1-0")]


def test_pagerduty_worker_retains_source_when_dlq_write_fails() -> None:
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xadd(self, *_: object, **__: object) -> str:
            raise RuntimeError("Redis unavailable")

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    task = SimpleNamespace(
        max_retries=3,
        request=SimpleNamespace(id="task-1", retries=3),
    )

    with pytest.raises(RuntimeError, match="Redis unavailable"):
        system_webhooks._retry_or_dead_letter_pagerduty_webhook(
            task=task,
            redis_client=Redis(),
            stream_name="pagerduty-webhooks:binding-1",
            stream_entry_id="1-0",
            fields={"event_id": "event-1", "payload": "{}"},
            error=RuntimeError("ClickHouse unavailable"),
        )

    assert deleted == []


def test_pagerduty_worker_dead_letters_exhausted_binding_resolution_failure() -> None:
    writes: list[tuple[str, dict[str, str]]] = []
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
                        "event_id": "event-1",
                        "payload": json.dumps(
                            {
                                "event": {
                                    "id": "event-1",
                                    "event_type": "incident.triggered",
                                    "occurred_at": "2026-07-17T12:00:00Z",
                                    "data": {},
                                }
                            }
                        ),
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            writes.append((stream, fields))
            return "2-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
    task.push_request(retries=task.max_retries, id="task-1")
    try:
        with (
            patch(
                "dev_health_ops.api.ingest.streams.get_redis_client",
                return_value=Redis(),
            ),
            patch("dev_health_ops.workers.system_webhooks.run_async", side_effect=_run),
            patch(
                "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
                new=AsyncMock(side_effect=RuntimeError("database unavailable")),
            ),
        ):
            with pytest.raises(RuntimeError, match="persistence exhausted"):
                task.run(binding_id="binding-1", stream_entry_id="1-0")
    finally:
        task.pop_request()

    assert writes[0][0] == "pagerduty-webhooks:binding-1:dlq"
    assert writes[0][1]["failure_type"] == "RuntimeError"
    assert deleted == [("pagerduty-webhooks:binding-1", "1-0")]


def test_pagerduty_worker_dead_letters_revoke_race_before_persistence() -> None:
    writes: list[tuple[str, dict[str, str]]] = []
    deleted: list[tuple[str, str]] = []

    class Redis:
        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
                        "event_id": "event-1",
                        "payload": json.dumps(
                            {
                                "event": {
                                    "id": "event-1",
                                    "event_type": "incident.triggered",
                                    "occurred_at": "2026-07-17T12:00:00Z",
                                    "data": {},
                                }
                            }
                        ),
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            writes.append((stream, fields))
            return "2-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
    task.push_request(retries=task.max_retries, id="task-1")
    original_context = _worker_context()
    try:
        with (
            patch(
                "dev_health_ops.api.ingest.streams.get_redis_client",
                return_value=Redis(),
            ),
            patch("dev_health_ops.workers.system_webhooks.run_async", side_effect=_run),
            patch(
                "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
                new=AsyncMock(return_value=original_context),
            ),
            patch(
                "dev_health_ops.providers.pagerduty.webhook_worker.reconcile_pagerduty_webhook_with_locked_graph",
                new=AsyncMock(
                    side_effect=RuntimeError(
                        "pagerduty webhook binding changed before persistence"
                    )
                ),
            ),
            patch(
                "dev_health_ops.workers.system_webhooks._canonical_incident_ingestion_allowed",
                return_value=True,
            ),
        ):
            with pytest.raises(RuntimeError, match="persistence exhausted"):
                task.run(binding_id="binding-1", stream_entry_id="1-0")
    finally:
        task.pop_request()

    assert writes[0][0] == "pagerduty-webhooks:binding-1:dlq"
    assert deleted == [("pagerduty-webhooks:binding-1", "1-0")]


def test_pagerduty_worker_reroutes_unparseable_entry_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PAGERDUTY_API_TOKEN", "token")
    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://example")

    class Redis:
        def xrange(self, *_: object, **__: object) -> list[tuple[str, dict[str, str]]]:
            return [
                (
                    "1-0",
                    {
                        "payload": "not-json",
                        "received_at": "2026-07-17T12:00:00+00:00",
                    },
                )
            ]

        def xadd(self, *_: object, **__: object) -> str:
            return "2-0"

        def xdel(self, *_: object, **__: object) -> None:
            return None

    with (
        patch(
            "dev_health_ops.api.ingest.streams.get_redis_client",
            return_value=Redis(),
        ),
        patch.object(
            system_webhooks.process_pagerduty_webhook_event, "retry", _retry_sentinel
        ),
        patch(
            "dev_health_ops.providers.pagerduty.webhook_worker.resolve_pagerduty_webhook_binding",
            new=AsyncMock(return_value=_worker_context()),
        ),
        patch("dev_health_ops.workers.system_webhooks.run_async", side_effect=_run),
        patch(
            "dev_health_ops.workers.system_webhooks.run_async",
            return_value=_worker_context(),
        ),
    ):
        task = cast(Any, system_webhooks.process_pagerduty_webhook_event)
        with pytest.raises(_RetrySentinel):
            task.run(
                binding_id="binding-1",
                stream_entry_id="1-0",
            )
