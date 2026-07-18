from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Generator
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app

SECRET = "pagerduty-webhook-secret"
OCCURRED_AT = "2026-07-17T12:00:00Z"


def _payload(
    *, event_id: str = "event-1", event_type: str = "incident.triggered"
) -> bytes:
    return json.dumps(
        {
            "event": {
                "id": event_id,
                "event_type": event_type,
                "occurred_at": OCCURRED_AT,
                "data": {
                    "id": "incident-1",
                    "title": "Payments unavailable",
                    "status": "triggered",
                    "created_at": OCCURRED_AT,
                    "updated_at": OCCURRED_AT,
                },
            },
        }
    ).encode()


def _signature(body: bytes) -> str:
    digest = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return f"v1={digest}"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient]:
    monkeypatch.setenv("PAGERDUTY_WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("PAGERDUTY_WEBHOOK_ORG_ID", "org-1")
    monkeypatch.setenv("PAGERDUTY_WEBHOOK_PROVIDER_INSTANCE_ID", "acme")
    with TestClient(app) as test_client:
        yield test_client


def test_accepts_replayed_event_and_enqueues_for_idempotent_persistence(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.api.webhooks import pagerduty

    writes: list[tuple[str, dict[str, str]]] = []
    dispatched: list[dict[str, str]] = []

    class Redis:
        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            writes.append((stream, fields))
            return "1-0"

    class Task:
        @staticmethod
        def delay(**kwargs: str) -> None:
            dispatched.append(kwargs)

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())
    monkeypatch.setattr(pagerduty, "process_pagerduty_webhook_event", Task())

    body = _payload()
    headers = {"x-pagerduty-signature": _signature(body)}

    response = client.post("/api/v1/webhooks/pagerduty", content=body, headers=headers)
    duplicate = client.post("/api/v1/webhooks/pagerduty", content=body, headers=headers)

    assert response.status_code == 202
    assert duplicate.status_code == 202
    assert response.json()["status"] == "accepted"
    assert duplicate.json()["status"] == "accepted"
    assert duplicate.json()["message"] == "Event accepted"
    assert len(writes) == 2
    assert writes[0][0] == "pagerduty-webhooks:org-1:acme"
    assert datetime.fromisoformat(
        writes[0][1]["occurred_at"]
    ) == datetime.fromisoformat(OCCURRED_AT.replace("Z", "+00:00"))
    assert len(dispatched) == 2


def test_dispatch_failure_does_not_suppress_a_replayed_event(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.api.webhooks import pagerduty

    writes: list[tuple[str, dict[str, str]]] = []

    class Redis:
        def xadd(self, stream: str, fields: dict[str, str], **_: object) -> str:
            writes.append((stream, fields))
            return f"{len(writes)}-0"

    class Task:
        attempts = 0

        @classmethod
        def delay(cls, **_: str) -> None:
            cls.attempts += 1
            if cls.attempts == 1:
                raise RuntimeError("broker unavailable")

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())
    monkeypatch.setattr(pagerduty, "process_pagerduty_webhook_event", Task())
    body = _payload()
    headers = {"x-pagerduty-signature": _signature(body)}

    failed = client.post("/api/v1/webhooks/pagerduty", content=body, headers=headers)
    replayed = client.post("/api/v1/webhooks/pagerduty", content=body, headers=headers)

    assert failed.status_code == 503
    assert replayed.status_code == 202
    assert len(writes) == 2


def test_rejects_an_invalid_signature(client: TestClient) -> None:
    response = client.post(
        "/api/v1/webhooks/pagerduty",
        content=_payload(),
        headers={"x-pagerduty-signature": "v1=not-a-signature"},
    )

    assert response.status_code == 401


@pytest.mark.parametrize("body", [b"{", b"x" * 1_048_577])
def test_rejects_malformed_or_oversized_payloads(
    client: TestClient, body: bytes
) -> None:
    response = client.post(
        "/api/v1/webhooks/pagerduty",
        content=body,
        headers={"x-pagerduty-signature": _signature(body)},
    )

    assert response.status_code in {400, 413}


def test_rejects_unknown_event_type(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.api.webhooks import pagerduty

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: None)
    body = _payload(event_type="incident.not-a-real-event")
    response = client.post(
        "/api/v1/webhooks/pagerduty",
        content=body,
        headers={"x-pagerduty-signature": _signature(body)},
    )

    assert response.status_code == 400


def test_configuration_and_test_event_paths(client: TestClient) -> None:
    body = _payload(event_id="test-event")
    headers = {"x-pagerduty-signature": _signature(body)}

    config = client.get("/api/v1/webhooks/pagerduty/configuration")
    validation = client.post(
        "/api/v1/webhooks/pagerduty/test-event", content=body, headers=headers
    )

    assert config.status_code == 200
    assert config.json() == {
        "configured": True,
        "org_id": "org-1",
        "provider_instance_id": "acme",
    }
    assert validation.status_code == 200
    assert validation.json()["event_id"] == "test-event"


def test_enqueue_does_not_cap_pending_stream_with_maxlen(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dev_health_ops.api.webhooks import pagerduty

    kwargs_seen: list[dict[str, object]] = []

    class Redis:
        def xadd(self, stream: str, fields: dict[str, str], **kwargs: object) -> str:
            kwargs_seen.append(kwargs)
            return "1-0"

    class Task:
        @staticmethod
        def delay(**_: str) -> None:
            return None

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())
    monkeypatch.setattr(pagerduty, "process_pagerduty_webhook_event", Task())
    body = _payload()
    headers = {"x-pagerduty-signature": _signature(body)}

    response = client.post("/api/v1/webhooks/pagerduty", content=body, headers=headers)

    # Pending entries must never be trimmed: no maxlen/approximate on ingest xadd.
    assert response.status_code == 202
    assert kwargs_seen == [{}]
