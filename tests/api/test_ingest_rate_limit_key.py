"""Per-token rate-limit keying for external-ingest endpoints (CHAOS-2691/2712).

CHAOS-2712 replaced raw-bearer-text hashing with keying on
``request.state.ingest_token_id`` -- set by ``require_ingest_scope``
(``api/external_ingest/auth.py``) only once a bearer has been resolved
against a real, DB-backed ``IngestToken`` row. Regression coverage for an
adversarial-review finding: an unauthenticated request (no auth dependency
ran, so no validated token id) must not be able to mint a fresh limiter
bucket per request by rotating an arbitrary/garbage ``Authorization: Bearer``
header -- it must collapse to the IP-based key regardless of what that
header contains.
"""

from __future__ import annotations

import hashlib

from fastapi import Request

from dev_health_ops.api.middleware.rate_limit import get_ingest_token_key


def _make_request(
    authorization: str | None = None,
    client_host: str = "203.0.113.5",
    ingest_token_id: str | None = None,
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
    request = Request(scope)
    if ingest_token_id is not None:
        request.state.ingest_token_id = ingest_token_id
    return request


def test_distinct_validated_token_ids_get_distinct_buckets():
    req_a = _make_request(ingest_token_id="token-id-a")
    req_b = _make_request(ingest_token_id="token-id-b")
    assert get_ingest_token_key(req_a) != get_ingest_token_key(req_b)


def test_same_validated_token_id_same_bucket():
    assert get_ingest_token_key(
        _make_request(ingest_token_id="token-id-a")
    ) == get_ingest_token_key(_make_request(ingest_token_id="token-id-a"))


def test_validated_token_id_never_appears_in_key():
    token_id = "11111111-2222-3333-4444-555555555555"
    key = get_ingest_token_key(_make_request(ingest_token_id=token_id))
    assert token_id not in key
    assert key.startswith("ingest-token:")


def test_token_key_uses_sha256_digest_of_token_id():
    token_id = "11111111-2222-3333-4444-555555555555"
    expected = hashlib.sha256(token_id.encode("utf-8")).hexdigest()[:16]
    assert get_ingest_token_key(_make_request(ingest_token_id=token_id)) == (
        f"ingest-token:{expected}"
    )


def test_no_validated_token_id_falls_back_to_ip_regardless_of_bearer_header():
    req = _make_request(authorization="Bearer some-unvalidated-string")
    assert get_ingest_token_key(req) == "ingest-ip:203.0.113.5"


def test_rotating_unvalidated_bearer_strings_cannot_mint_fresh_buckets():
    """Regression (adversarial review): without a preceding auth dependency
    (e.g. the public GET /schemas* endpoints), an attacker can send any
    Authorization header they like. It must never influence the limiter key
    -- only request.state.ingest_token_id (set post-validation) may."""
    same_ip_requests = [
        _make_request(authorization=f"Bearer rotated-{i}", client_host="203.0.113.5")
        for i in range(5)
    ]
    keys = {get_ingest_token_key(req) for req in same_ip_requests}
    assert keys == {"ingest-ip:203.0.113.5"}


def test_missing_bearer_and_missing_token_id_falls_back_to_ip():
    req = _make_request(authorization=None, client_host="203.0.113.5")
    assert get_ingest_token_key(req) == "ingest-ip:203.0.113.5"


def test_distinct_ips_without_validated_token_get_distinct_buckets():
    req_a = _make_request(client_host="203.0.113.5")
    req_b = _make_request(client_host="198.51.100.9")
    assert get_ingest_token_key(req_a) != get_ingest_token_key(req_b)
