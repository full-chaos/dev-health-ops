from __future__ import annotations

import hashlib

import pytest
from fastapi import HTTPException

from dev_health_ops.api.webhooks import pagerduty

_BINDING_ID = "625d6f61-9507-4690-8e90-3e6c2fa73b6c"
_SECOND_BINDING_ID = "41b14a1c-76e4-456a-98af-2b69ee607626"
_SUBSCRIPTION_ID = "pagerduty-subscription-1"
_SECOND_SUBSCRIPTION_ID = "pagerduty-subscription-2"
_REPLAY_IDENTITY_SEPARATOR = b"\x1f"


def test_replay_key_hashes_untrusted_event_id_within_canonical_binding_namespace() -> (
    None
):
    # Given
    malicious_event_id = "event:with:redis:separators"

    # When
    key = pagerduty._replay_key(_BINDING_ID, malicious_event_id)

    # Then
    assert key == (
        f"pagerduty-webhook-replay:{_BINDING_ID}:"
        f"{hashlib.sha256(malicious_event_id.encode()).hexdigest()}"
    )
    assert malicious_event_id not in key


def test_replay_key_keeps_identical_event_ids_isolated_per_binding() -> None:
    # Given
    event_id = "pagey-event-1"

    # When
    first_key = pagerduty._replay_key(_BINDING_ID, event_id)
    second_key = pagerduty._replay_key(_SECOND_BINDING_ID, event_id)

    # Then
    assert first_key != second_key


def test_replay_identity_uses_the_persisted_subscription_id() -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1"}}'

    # When
    first_identity = pagerduty._replay_identity(_SUBSCRIPTION_ID, "pagey-event-1", body)
    second_identity = pagerduty._replay_identity(
        _SECOND_SUBSCRIPTION_ID, "pagey-event-1", body
    )

    # Then
    assert first_identity != second_identity


def test_replay_identity_normalizes_blank_event_ids_to_the_raw_body_hash() -> None:
    # Given
    body = b'{"event":{"id":"   "}}'

    # When
    replay_identity = pagerduty._replay_identity(_SUBSCRIPTION_ID, "   ", body)

    # Then
    assert replay_identity.endswith(hashlib.sha256(body).hexdigest())


def test_replay_identity_falls_back_to_the_raw_body_hash_for_blank_event_id() -> None:
    # Given
    body = b'{"event":{"id":""}}'

    # When
    replay_identity = pagerduty._replay_identity(_SUBSCRIPTION_ID, "", body)

    # Then
    assert replay_identity.endswith(hashlib.sha256(body).hexdigest())


def test_claim_delivery_stores_body_hash_with_bounded_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1"}}'
    writes: list[tuple[tuple[str, ...], dict[str, object]]] = []

    class Redis:
        def set(self, *args: str, **kwargs: object) -> bool:
            writes.append((args, kwargs))
            return True

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    claim = pagerduty._claim_delivery(
        _BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body
    )

    # Then
    body_hash = hashlib.sha256(body).hexdigest()
    assert claim is pagerduty.ReplayClaimOutcome.CLAIMED
    assert writes == [
        (
            (
                f"pagerduty-webhook-replay:{_BINDING_ID}:"
                f"{hashlib.sha256(_SUBSCRIPTION_ID.encode() + _REPLAY_IDENTITY_SEPARATOR + b'pagey-event-1').hexdigest()}",
                f"pending:{body_hash}",
            ),
            {"nx": True, "ex": pagerduty.PENDING_REPLAY_CLAIM_TTL_SECONDS},
        )
    ]


def test_claim_delivery_returns_replay_when_an_accepted_body_hash_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1"}}'
    body_hash = hashlib.sha256(body).hexdigest()
    reads: list[str] = []

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return False

        def get(self, key: str) -> str:
            reads.append(key)
            return f"accepted:{body_hash}"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    claim = pagerduty._claim_delivery(
        _BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body
    )

    # Then
    assert claim is pagerduty.ReplayClaimOutcome.REPLAYED
    assert reads == [
        f"pagerduty-webhook-replay:{_BINDING_ID}:"
        f"{hashlib.sha256(_SUBSCRIPTION_ID.encode() + _REPLAY_IDENTITY_SEPARATOR + b'pagey-event-1').hexdigest()}"
    ]


def test_claim_delivery_returns_retryable_pending_when_first_dispatch_is_provisional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1"}}'
    body_hash = hashlib.sha256(body).hexdigest()

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return False

        def get(self, _: str) -> str:
            return f"pending:{body_hash}"

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    claim = pagerduty._claim_delivery(
        _BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body
    )

    # Then
    assert claim is pagerduty.ReplayClaimOutcome.PENDING


def test_claim_delivery_rejects_event_id_collision_with_different_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1","data":{"id":"new"}}}'

    class Redis:
        def set(self, *_: str, **__: object) -> bool:
            return False

        def get(self, _: str) -> str:
            return hashlib.sha256(b"different body").hexdigest()

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When / Then
    with pytest.raises(HTTPException) as error:
        pagerduty._claim_delivery(_BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body)

    assert error.value.status_code == 409


def test_claim_delivery_reclaims_an_expired_pending_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1"}}'

    class Redis:
        def __init__(self) -> None:
            self.claim_exists = True

        def set(self, *_: str, **__: object) -> bool:
            if self.claim_exists:
                return False
            self.claim_exists = True
            return True

        def get(self, _: str) -> str:
            return f"pending:{hashlib.sha256(body).hexdigest()}"

    redis = Redis()
    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: redis)

    # When
    pending = pagerduty._claim_delivery(
        _BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body
    )
    redis.claim_exists = False
    reclaimed = pagerduty._claim_delivery(
        _BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body
    )

    # Then
    assert pending is pagerduty.ReplayClaimOutcome.PENDING
    assert reclaimed is pagerduty.ReplayClaimOutcome.CLAIMED


def test_accept_replay_claim_promotes_pending_to_accepted_with_long_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    body = b'{"event":{"id":"pagey-event-1"}}'
    calls: list[tuple[tuple[str | int, ...], dict[str, object]]] = []

    class Redis:
        def eval(self, *args: str | int, **kwargs: object) -> int:
            calls.append((args, kwargs))
            return 1

    monkeypatch.setattr(pagerduty, "get_redis_client", lambda: Redis())

    # When
    pagerduty._accept_replay_claim(_BINDING_ID, _SUBSCRIPTION_ID, "pagey-event-1", body)

    # Then
    assert calls[0][0][-1] == str(pagerduty.REPLAY_RETENTION_SECONDS)


def test_replay_identity_keeps_subscription_namespaces_exactly_isolated() -> None:
    # Given / When
    first_key = pagerduty._replay_key(
        _BINDING_ID,
        pagerduty._replay_identity(_SUBSCRIPTION_ID, "pagey-event-1", b"first"),
    )
    second_key = pagerduty._replay_key(
        _BINDING_ID,
        pagerduty._replay_identity(_SECOND_SUBSCRIPTION_ID, "pagey-event-1", b"first"),
    )

    # Then
    assert first_key != second_key
