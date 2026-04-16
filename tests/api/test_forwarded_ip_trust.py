"""X-Forwarded-For trust boundary tests (CHAOS security sprint)."""
from __future__ import annotations

import pytest
from fastapi import Request

from dev_health_ops.api.middleware.rate_limit import get_forwarded_ip


def _make_request(
    client_host: str,
    xff: str | None = None,
) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if xff is not None:
        headers.append((b"x-forwarded-for", xff.encode("latin-1")))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": headers,
        "client": (client_host, 12345),
    }
    return Request(scope)


def test_xff_ignored_from_untrusted_peer(monkeypatch):
    """XFF from a random internet peer must NOT be honoured."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    req = _make_request(client_host="1.2.3.4", xff="203.0.113.1")
    assert get_forwarded_ip(req) == "1.2.3.4"


def test_xff_honoured_from_trusted_peer(monkeypatch):
    """XFF from a listed trusted proxy must be used as the real client."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1,10.0.0.2")
    req = _make_request(client_host="10.0.0.2", xff="203.0.113.1")
    assert get_forwarded_ip(req) == "203.0.113.1"


def test_xff_missing_returns_peer(monkeypatch):
    """No XFF: peer IP is returned regardless of trust."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    req = _make_request(client_host="10.0.0.1", xff=None)
    assert get_forwarded_ip(req) == "10.0.0.1"


def test_trusted_proxies_unset_disables_xff(monkeypatch):
    """Unset/empty TRUSTED_PROXIES must fail-closed: never trust XFF."""
    monkeypatch.delenv("TRUSTED_PROXIES", raising=False)
    req = _make_request(client_host="10.0.0.1", xff="203.0.113.1")
    assert get_forwarded_ip(req) == "10.0.0.1"


def test_xff_takes_first_entry(monkeypatch):
    """When trusted, the leftmost XFF entry is the original client."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    req = _make_request(
        client_host="10.0.0.1", xff="203.0.113.1, 10.0.0.1"
    )
    assert get_forwarded_ip(req) == "203.0.113.1"
