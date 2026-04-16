"""Tests for narrowed exception handling in _extract_unverified_org_and_subject."""

from __future__ import annotations

import logging

from dev_health_ops.api.auth.router import _extract_unverified_org_and_subject


def test_returns_none_tuple_for_malformed_token(caplog):
    """A malformed token must yield (None, None) AND emit a debug log."""
    caplog.set_level(logging.DEBUG, logger="dev_health_ops.api.auth.router")
    org, sub = _extract_unverified_org_and_subject("not.a.token")
    assert (org, sub) == (None, None)
    assert any("unverified claims" in rec.message.lower() for rec in caplog.records)


def test_returns_none_tuple_for_empty_token():
    org, sub = _extract_unverified_org_and_subject("")
    assert (org, sub) == (None, None)


def test_returns_tuple_for_valid_unsigned_token():
    """A syntactically valid JWT (even if signature invalid) yields claims."""
    import base64
    import json

    def _b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64(
        json.dumps(
            {"sub": "user-1", "org_id": "00000000-0000-0000-0000-000000000001"}
        ).encode()
    )
    signature = _b64(b"\x00" * 32)
    token = f"{header}.{payload}.{signature}"

    org, sub = _extract_unverified_org_and_subject(token)
    assert sub == "user-1"
    assert org is not None
    assert str(org) == "00000000-0000-0000-0000-000000000001"
