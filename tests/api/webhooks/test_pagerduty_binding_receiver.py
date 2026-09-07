from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Generator
from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from kombu.exceptions import KombuError
from valkey.exceptions import ValkeyError

from dev_health_ops.api.main import app
from dev_health_ops.api.webhooks import pagerduty

_BINDING_ID = "625d6f61-9507-4690-8e90-3e6c2fa73b6c"
_SECRET = "binding-scoped-signing-secret"


@dataclass(frozen=True, slots=True)
class _Binding:
    id: str = _BINDING_ID
    org_id: str = "5a23f94c-dac3-4542-9b95-149b2e5192e0"
    integration_source_id: str = "8c076977-37a0-43d4-a6e4-80c9e79891e0"
    credential_id: str = "ea999a45-91b1-4e6c-ae2b-5a6cf8aec2fb"
    provider_subscription_id: str = "subscription-1"
    status: str = "active"


_SUBSCRIPTION_ID = _Binding().provider_subscription_id


@dataclass(frozen=True, slots=True)
class _ResolvedBinding:
    binding: _Binding
    signing_secret: str = _SECRET


def _body(*, event_type: str = "incident.triggered") -> bytes:
    return json.dumps(
        {
            "event": {
                "id": "pagey-event-1",
                "event_type": event_type,
                "occurred_at": "2026-07-21T12:00:00Z",
                "data": {"id": "incident-1"},
            }
        }
    ).encode()


def _headers(
    body: bytes,
    *,
    secret: str = _SECRET,
    subscription_id: str = _Binding().provider_subscription_id,
) -> dict[str, str]:
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {
        "x-pagerduty-signature": f"v1={signature}",
        "x-webhook-subscription": subscription_id,
    }


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient]:
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(return_value=_ResolvedBinding(binding=_Binding())),
    )
    monkeypatch.setattr(
        pagerduty,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=True),
    )
    with TestClient(app) as test_client:
        yield test_client


def test_receiver_enqueues_a_verified_event_for_its_binding(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    writes: list[tuple[str, dict[str, str]]] = []
    claims: list[tuple[tuple[str, ...], dict[str, object]]] = []

    class Redis:
        def set(self, *args: str, **kwargs: object) -> bool:
            claims.append((args, kwargs))
            return True

        def xadd(self, stream: str, fields: dict[str, str], *_: object) -> str:
            writes.append((stream, fields))
            return "1-0"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())
    body = _body()

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 202
    assert writes[0][0] == f"pagerduty-webhooks:{_BINDING_ID}"
    assert writes[0][1]["binding_id"] == _BINDING_ID
    assert set(writes[0][1]) == {
        "binding_id",
        "event_id",
        "event_type",
        "occurred_at",
        "received_at",
        "raw_body_sha256",
        "payload",
    }
    assert writes[0][1]["raw_body_sha256"] == hashlib.sha256(body).hexdigest()
    # The stream write is the whole handoff. This used to also assert a Celery
    # `.delay(...)` alongside it; CHAOS-4105 deleted that task, so the entry
    # this write produces is consumed by the Go stream runner and by nothing
    # else. The fields asserted above are that consumer's entire input.
    body_hash = hashlib.sha256(body).hexdigest()
    replay_key = pagerduty._replay_key(
        _BINDING_ID,
        pagerduty._replay_identity(_SUBSCRIPTION_ID, "pagey-event-1", body),
    )
    assert claims == [
        (
            (
                replay_key,
                f"pending:{body_hash}",
            ),
            {"nx": True, "ex": pagerduty.PENDING_REPLAY_CLAIM_TTL_SECONDS},
        ),
        (
            (
                replay_key,
                f"accepted:{body_hash}",
            ),
            {"xx": True, "ex": pagerduty.REPLAY_RETENTION_SECONDS},
        ),
    ]


def test_receiver_accepts_the_official_no_timestamp_contract(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body()

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return True

        def xadd(self, *_: object) -> str:
            return "1-0"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 202


def test_receiver_rejects_invalid_signature_before_replay_or_json(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body()
    redis_used = False

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            nonlocal redis_used
            redis_used = True
            return True

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers={"x-webhook-subscription": _Binding().provider_subscription_id},
    )

    # Then
    assert response.status_code == 401
    assert redis_used is False


def test_receiver_rejects_event_types_outside_the_exact_allowlist(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body(event_type="incident.not-a-real-event")
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: None)

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 400


def test_receiver_accepts_an_identical_replay_without_enqueueing_or_dispatching(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body()
    queued = False

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return False

        def get(self, _: str) -> str:
            return f"accepted:{hashlib.sha256(body).hexdigest()}"

        def xadd(self, *_: object) -> str:
            nonlocal queued
            queued = True
            return "1-0"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 202
    assert queued is False


def test_receiver_acknowledges_pagey_ping_without_a_queue_write(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body(event_type="pagey.ping")
    redis_used = False

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            nonlocal redis_used
            redis_used = True
            return True

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 204
    assert redis_used is False


def test_receiver_feature_gate_blocks_verified_events_before_enqueueing(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body()
    monkeypatch.setattr(
        pagerduty,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: None)

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 403


def test_legacy_environment_route_cannot_enqueue(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: the unrouted legacy path. It must not reach the queue even when
    # every legacy environment variable is set. This used to also assert that
    # it dispatched no Celery task; CHAOS-4105 deleted that task, so reaching
    # the stream is now the only way a delivery can be processed at all, and
    # is therefore the whole property.
    body = _body()
    enqueued = False

    class Redis:
        def xadd(self, *_: object) -> str:
            nonlocal enqueued
            enqueued = True
            return "1-0"

    monkeypatch.setenv("PAGERDUTY_WEBHOOK_SECRET", _SECRET)
    monkeypatch.setenv("PAGERDUTY_WEBHOOK_ORG_ID", _Binding().org_id)
    monkeypatch.setenv(
        "PAGERDUTY_WEBHOOK_PROVIDER_INSTANCE_ID", _Binding().integration_source_id
    )
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        "/api/v1/webhooks/pagerduty",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 404
    assert enqueued is False


def test_receiver_rejects_a_cross_binding_subscription_mismatch(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a validly signed body, but a subscription header addressed to a
    # different PagerDuty subscription than the one persisted on this binding.
    body = _body()
    redis_used = False

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            nonlocal redis_used
            redis_used = True
            return True

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body, subscription_id="a-different-subscription"),
    )

    # Then: rejected before any queue interaction, same as an invalid signature.
    assert response.status_code == 401
    assert redis_used is False


def test_receiver_uses_the_route_uuid_before_comparing_the_subscription_header(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body()
    looked_up: list[object] = []

    async def load_by_id(binding_id: object) -> _ResolvedBinding:
        looked_up.append(binding_id)
        return _ResolvedBinding(binding=_Binding())

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return True

        def xadd(self, *_: object, **__: object) -> str:
            return "1-0"

    monkeypatch.setattr(pagerduty, "_load_receivable_binding", load_by_id)
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 202
    assert looked_up == [pagerduty.UUID(_BINDING_ID)]


def test_receiver_accepts_any_valid_signature_among_multiple_candidates(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: PagerDuty may send several comma-separated v1= candidates while
    # a signing secret rotation is in flight. Any one valid candidate must pass.
    body = _body()

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return True

        def xadd(self, *_: object) -> str:
            return "1-0"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    stale_signature = hmac.new(
        b"an-old-rotated-secret", body, hashlib.sha256
    ).hexdigest()
    current_signature = hmac.new(_SECRET.encode(), body, hashlib.sha256).hexdigest()

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers={
            "x-pagerduty-signature": f"v1={stale_signature},v1={current_signature}",
            "x-webhook-subscription": _SUBSCRIPTION_ID,
        },
    )

    # Then
    assert response.status_code == 202


def test_receiver_rejects_a_signature_signed_with_an_old_rotated_secret(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: the caller signs with a secret that predates rotation onto this
    # binding's current secret.
    body = _body()
    redis_used = False

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            nonlocal redis_used
            redis_used = True
            return True

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body, secret="an-old-rotated-secret"),
    )

    # Then
    assert response.status_code == 401
    assert redis_used is False


def test_receiver_rejects_a_guessed_binding_id(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a syntactically valid but non-existent binding UUID.
    body = _body()
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(
            side_effect=HTTPException(
                status_code=404, detail="Webhook binding not found"
            )
        ),
    )
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: None)

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 404


def test_receiver_rejects_a_revoked_binding_identically_to_an_unknown_one(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: a binding UUID that exists but has been revoked. The active-only
    # lookup surfaces this exactly like an unknown UUID -- the response must
    # never become an oracle for which binding IDs exist versus are revoked.
    body = _body()
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(
            side_effect=HTTPException(
                status_code=404, detail="Webhook binding not found"
            )
        ),
    )
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: None)

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 404


@pytest.mark.parametrize(
    "stream_error",
    [ValkeyError("stream unavailable"), KombuError("broker unavailable")],
)
def test_receiver_releases_the_replay_claim_when_the_stream_write_fails(
    stream_error: Exception, client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: the durable (non-expiring) replay claim succeeds but the
    # subsequent Redis stream write raises. Both exception types _enqueue_event
    # catches are exercised: KombuError used to be covered only through the
    # Celery dispatch that CHAOS-4105 deleted, and would otherwise have been
    # lost from coverage with it.
    body = _body()
    deleted: list[str] = []
    xdel_calls: list[tuple[str, str]] = []

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return True

        def xadd(self, *_: object) -> str:
            raise stream_error

        def xdel(self, stream: str, entry_id: str) -> int:
            xdel_calls.append((stream, entry_id))
            return 1

        def delete(self, key: str) -> int:
            deleted.append(key)
            return 1

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then: the claim is released (compensated) so a genuine redelivery is not
    # blocked forever by a durable marker that never expires on its own.
    assert response.status_code == 503
    assert deleted == [
        pagerduty._replay_key(
            _BINDING_ID,
            pagerduty._replay_identity(_SUBSCRIPTION_ID, "pagey-event-1", body),
        )
    ]
    # The stream write never landed, so there is nothing to compensate. This
    # pins the removal of _compensate_stream_write (CHAOS-4105): an XDEL here
    # would be the receiver deleting an entry it did not create -- and on the
    # success path, one the Go consumer may already hold pending.
    assert xdel_calls == []


def test_receiver_marks_a_candidate_ready_only_after_a_verified_pagey_ping(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    candidate = _ResolvedBinding(binding=_Binding(status="candidate"))
    ready_from_verified_ping = AsyncMock()
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(return_value=candidate),
    )
    monkeypatch.setattr(
        pagerduty,
        "_mark_candidate_ready_from_verified_ping",
        ready_from_verified_ping,
        raising=False,
    )
    body = _body(event_type="pagey.ping")

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 204
    ready_from_verified_ping.assert_awaited_once()


def test_receiver_does_not_mark_a_candidate_ready_for_an_unsigned_pagey_ping(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    candidate = _ResolvedBinding(binding=_Binding(status="candidate"))
    ready_from_verified_ping = AsyncMock()
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(return_value=candidate),
    )
    monkeypatch.setattr(
        pagerduty,
        "_mark_candidate_ready_from_verified_ping",
        ready_from_verified_ping,
    )
    body = _body(event_type="pagey.ping")

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers={"x-webhook-subscription": _SUBSCRIPTION_ID},
    )

    # Then
    assert response.status_code == 401
    ready_from_verified_ping.assert_not_awaited()


def test_receiver_does_not_mark_a_candidate_ready_when_the_feature_is_disabled(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    candidate = _ResolvedBinding(binding=_Binding(status="candidate"))
    ready_from_verified_ping = AsyncMock()
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(return_value=candidate),
    )
    monkeypatch.setattr(
        pagerduty,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        pagerduty,
        "_mark_candidate_ready_from_verified_ping",
        ready_from_verified_ping,
    )
    body = _body(event_type="pagey.ping")

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 403
    ready_from_verified_ping.assert_not_awaited()


def test_receiver_returns_retryable_response_during_a_provisional_delivery_claim(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    body = _body()

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return False

        def get(self, _: str) -> str:
            return f"pending:{hashlib.sha256(body).hexdigest()}"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 503


@pytest.mark.parametrize("binding_status", ["candidate", "ready"])
def test_receiver_rejects_a_fully_authenticated_non_ping_receivable_event(
    binding_status: str, client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given
    candidate = _ResolvedBinding(binding=_Binding(status=binding_status))
    monkeypatch.setattr(
        pagerduty,
        "_load_receivable_binding",
        AsyncMock(return_value=candidate),
    )
    body = _body()

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then
    assert response.status_code == 403


def test_receiver_writes_the_stream_entry_and_leaves_it_for_the_go_consumer(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given: this replaces the two PAGERDUTY_WEBHOOK_TRANSPORT tests that used
    # to live here -- one pinning that anything unset, empty or unrecognised
    # failed safe to a Celery dispatch, the other pinning that "river" left the
    # entry alone. CHAOS-4105 deleted the Celery consumer and the transport
    # switch that chose between the two runtimes, so there is nothing left to
    # select and no fail-safe direction to pin.
    #
    # What survives is the property both tests existed to protect: exactly one
    # consumer drains the entry. The receiver writes it and then leaves it
    # completely alone -- no second dispatch, and above all no delete, because
    # the Go consumer may already be holding it pending under its own receipt.
    writes: list[tuple[str, dict[str, str]]] = []
    deleted: list[tuple[str, str]] = []

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return True

        def xadd(self, stream: str, fields: dict[str, str], *_: object) -> str:
            writes.append((stream, fields))
            return "1-0"

        def xdel(self, stream: str, entry_id: str) -> None:
            deleted.append((stream, entry_id))

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())
    body = _body()

    # When
    response = client.post(
        f"/api/v1/webhooks/pagerduty/{_BINDING_ID}",
        content=body,
        headers=_headers(body),
    )

    # Then: the durable handoff is written, and left intact, for Go.
    assert response.status_code == 202
    assert writes[0][0] == f"pagerduty-webhooks:{_BINDING_ID}"
    assert writes[0][1]["binding_id"] == _BINDING_ID
    assert deleted == []
