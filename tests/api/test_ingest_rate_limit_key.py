"""Per-token rate-limit keying for external-ingest endpoints (CHAOS-2691).

Regression coverage for an adversarial-review finding: while
EXTERNAL_INGEST_INSECURE_AUTH=1's interim auth is active, the bearer value
is not validated at all, so keying the limiter on it would let a caller
rotate arbitrary bearer strings to get a fresh bucket on every request. In
that mode the key must collapse to IP regardless of the bearer value.
"""

from __future__ import annotations

import hashlib

from fastapi import Request

from dev_health_ops.api.middleware.rate_limit import get_ingest_token_key


def _make_request(
    authorization: str | None = None, client_host: str = "203.0.113.5"
) -> Request:
    headers = []
    if authorization is not None:
        headers.append((b"authorization", authorization.encode()))
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/external-ingest/batches",
        "headers": headers,
        "client": (client_host, 12345),
    }
    return Request(scope)


def test_distinct_bearer_tokens_get_distinct_buckets_outside_interim_auth(monkeypatch):
    monkeypatch.delenv("EXTERNAL_INGEST_INSECURE_AUTH", raising=False)
    req_a = _make_request(authorization="Bearer token-a")
    req_b = _make_request(authorization="Bearer token-b")
    assert get_ingest_token_key(req_a) != get_ingest_token_key(req_b)


def test_same_bearer_token_same_bucket_outside_interim_auth(monkeypatch):
    monkeypatch.delenv("EXTERNAL_INGEST_INSECURE_AUTH", raising=False)
    assert get_ingest_token_key(
        _make_request(authorization="Bearer token-a")
    ) == get_ingest_token_key(_make_request(authorization="Bearer token-a"))


def test_raw_token_never_appears_in_key(monkeypatch):
    monkeypatch.delenv("EXTERNAL_INGEST_INSECURE_AUTH", raising=False)
    token = "super-secret-ingest-token"
    key = get_ingest_token_key(_make_request(authorization=f"Bearer {token}"))
    assert token not in key


def test_rotating_bearer_tokens_cannot_bypass_the_limit_in_interim_auth_mode(
    monkeypatch,
):
    monkeypatch.setenv("EXTERNAL_INGEST_INSECURE_AUTH", "1")
    req_a = _make_request(authorization="Bearer token-a", client_host="203.0.113.5")
    req_b = _make_request(
        authorization="Bearer token-b-rotated", client_host="203.0.113.5"
    )
    assert (
        get_ingest_token_key(req_a)
        == get_ingest_token_key(req_b)
        == "ingest-ip:203.0.113.5"
    )


def test_interim_auth_mode_still_distinguishes_by_ip(monkeypatch):
    monkeypatch.setenv("EXTERNAL_INGEST_INSECURE_AUTH", "1")
    req_a = _make_request(authorization="Bearer token-a", client_host="203.0.113.5")
    req_b = _make_request(authorization="Bearer token-a", client_host="198.51.100.9")
    assert get_ingest_token_key(req_a) != get_ingest_token_key(req_b)


def test_missing_bearer_falls_back_to_ip(monkeypatch):
    monkeypatch.delenv("EXTERNAL_INGEST_INSECURE_AUTH", raising=False)
    req = _make_request(authorization=None, client_host="203.0.113.5")
    assert get_ingest_token_key(req) == "ingest-ip:203.0.113.5"


def test_token_key_uses_sha256_digest_not_raw_prefix(monkeypatch):
    monkeypatch.delenv("EXTERNAL_INGEST_INSECURE_AUTH", raising=False)
    token = "dev-token"
    expected = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
    assert (
        get_ingest_token_key(_make_request(authorization=f"Bearer {token}"))
        == f"ingest-token:{expected}"
    )
