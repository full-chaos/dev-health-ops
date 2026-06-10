"""Per-token rate-limit keying for POST /auth/validate (CHAOS-2232).

The web app calls /validate server-side, so every user shares the same TCP
peer (the web container). The key must come from the submitted token — as a
digest, never the raw value — with an IP fallback for tokenless requests.
"""

from __future__ import annotations

import hashlib
import json

from fastapi import Request

from dev_health_ops.api.middleware.rate_limit import get_validate_key


def _make_request(
    client_host: str = "172.20.0.11",
    body: bytes | None = None,
) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/auth/validate",
        "headers": [],
        "client": (client_host, 12345),
    }
    request = Request(scope)
    if body is not None:
        request._body = body
    return request


def test_token_present_keys_on_digest():
    token = "eyJhbGciOiJIUzI1NiJ9.payload.sig"
    req = _make_request(body=json.dumps({"token": token}).encode())
    expected = hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]
    assert get_validate_key(req) == f"validate-token:{expected}"


def test_raw_token_never_appears_in_key():
    token = "secret-token-value"
    req = _make_request(body=json.dumps({"token": token}).encode())
    assert token not in get_validate_key(req)


def test_distinct_tokens_get_distinct_buckets():
    req_a = _make_request(body=json.dumps({"token": "token-a"}).encode())
    req_b = _make_request(body=json.dumps({"token": "token-b"}).encode())
    assert get_validate_key(req_a) != get_validate_key(req_b)


def test_same_token_same_bucket_across_requests():
    body = json.dumps({"token": "token-a"}).encode()
    assert get_validate_key(_make_request(body=body)) == get_validate_key(
        _make_request(body=body)
    )


def test_missing_body_falls_back_to_ip():
    req = _make_request(client_host="172.20.0.11", body=None)
    assert get_validate_key(req) == "validate-ip:172.20.0.11"


def test_malformed_json_falls_back_to_ip():
    req = _make_request(client_host="172.20.0.11", body=b"not-json{")
    assert get_validate_key(req) == "validate-ip:172.20.0.11"


def test_non_string_token_falls_back_to_ip():
    req = _make_request(
        client_host="172.20.0.11", body=json.dumps({"token": 123}).encode()
    )
    assert get_validate_key(req) == "validate-ip:172.20.0.11"


def test_empty_token_falls_back_to_ip():
    req = _make_request(
        client_host="172.20.0.11", body=json.dumps({"token": ""}).encode()
    )
    assert get_validate_key(req) == "validate-ip:172.20.0.11"


def test_ip_fallback_respects_trusted_proxy(monkeypatch):
    """The fallback path reuses get_forwarded_ip's trust rules."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/auth/validate",
        "headers": [(b"x-forwarded-for", b"203.0.113.1")],
        "client": ("10.0.0.1", 12345),
    }
    assert get_validate_key(Request(scope)) == "validate-ip:203.0.113.1"
