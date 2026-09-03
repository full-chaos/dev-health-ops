"""Tests for the REST forwarder that mounts POST /api/v1/investment/explain
onto query-api's Go handler (CHAOS-4977 step 5b) --
:func:`maybe_dispatch_investment_explain_to_go`. Covers the four cases
team-lead named explicitly: switch off, switch on, fallback (every
distinct reason), and streaming (chunks pass through the returned
StreamingResponse individually, not merged into one buffered read).

Mirrors test_go_api_dispatcher.py's conventions: a bare Starlette Request
built by hand (no TestClient needed for the dispatch decision itself),
module-attribute monkeypatching for every external seam.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse

from dev_health_ops.api.graphql import investment_explain_dispatcher as dispatcher_mod
from dev_health_ops.api.graphql.investment_explain_dispatcher import (
    maybe_dispatch_investment_explain_to_go,
)
from dev_health_ops.api.services.auth import AuthenticatedUser

pytestmark = pytest.mark.asyncio


def _make_request(body: bytes = b"{}") -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/investment/explain",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }
    state = {"sent": False}

    async def receive() -> dict[str, Any]:
        if state["sent"]:
            return {"type": "http.disconnect"}
        state["sent"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def _sample_user() -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id="22222222-2222-4222-8222-222222222222",
        email="dev@example.com",
        org_id="11111111-1111-4111-8111-111111111111",
        role="admin",
        is_superuser=False,
    )


@pytest.fixture(autouse=True)
def _configure(monkeypatch: pytest.MonkeyPatch) -> None:
    # Every test starts from the safe default (unset == off); each test
    # turns on what it needs.
    monkeypatch.delenv("GO_API_INVESTMENT_EXPLAIN_ENABLED", raising=False)
    monkeypatch.delenv("GO_API_QUERY_API_URL", raising=False)

    @asynccontextmanager
    async def _fake_session() -> AsyncIterator[None]:
        yield None

    monkeypatch.setattr(dispatcher_mod, "get_postgres_session", _fake_session)


def _enable(
    monkeypatch: pytest.MonkeyPatch, *, target_url: str = "http://query-api.test:8090"
) -> None:
    monkeypatch.setenv("GO_API_INVESTMENT_EXPLAIN_ENABLED", "true")
    monkeypatch.setenv("GO_API_QUERY_API_URL", target_url)


def _patch_envelope_inputs_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    from dev_health_ops.api.services import licensing as licensing_mod

    async def _fake_tier(session, org_uuid):
        return "team"

    async def _fake_features(session, org_uuid):
        return ["feature-a"]

    monkeypatch.setattr(licensing_mod, "resolve_org_tier_async", _fake_tier)
    monkeypatch.setattr(
        licensing_mod, "resolve_licensed_features_async", _fake_features
    )
    monkeypatch.setattr(
        dispatcher_mod,
        "issue_effective_principal_envelope",
        lambda *a, **k: "fake-envelope",
    )


class _FakeUpstream:
    def __init__(
        self,
        status_code: int,
        chunks: list[bytes],
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self.headers = headers or {"content-type": "application/json"}
        self._chunks = chunks
        self.closed = False

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _FakeClient:
    def __init__(self, send_impl):
        self._send_impl = send_impl
        self.closed = False
        self.sent_kwargs: dict[str, Any] = {}

    def build_request(
        self, method, url, *, content=None, params=None, headers=None, timeout=None
    ):
        return {
            "method": method,
            "url": url,
            "content": content,
            "params": params,
            "headers": headers,
            "timeout": timeout,
        }

    async def send(self, request, *, stream: bool = False):
        self.sent_kwargs = {"request": request, "stream": stream}
        return await self._send_impl(request)

    async def aclose(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# off
# ---------------------------------------------------------------------------


async def test_disabled_by_default_falls_back_to_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


async def test_explicit_false_falls_back_to_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GO_API_INVESTMENT_EXPLAIN_ENABLED", "false")
    monkeypatch.setenv("GO_API_QUERY_API_URL", "http://query-api.test:8090")
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


# ---------------------------------------------------------------------------
# fallback (every reason short of a live 200)
# ---------------------------------------------------------------------------


async def test_enabled_but_no_target_url_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GO_API_INVESTMENT_EXPLAIN_ENABLED", "true")
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


async def test_envelope_inputs_missing_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable(monkeypatch)
    from dev_health_ops.api.services import licensing as licensing_mod

    async def _raise(session, org_uuid):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(licensing_mod, "resolve_org_tier_async", _raise)
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


async def test_envelope_signing_error_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable(monkeypatch)
    _patch_envelope_inputs_ok(monkeypatch)

    def _raise(*a, **k):
        raise RuntimeError("signing key missing")

    monkeypatch.setattr(dispatcher_mod, "issue_effective_principal_envelope", _raise)
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


async def test_go_timeout_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    _patch_envelope_inputs_ok(monkeypatch)

    async def _send(request):
        raise httpx.ConnectTimeout("timed out")

    monkeypatch.setattr(
        dispatcher_mod, "_build_http_client", lambda: _FakeClient(_send)
    )
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


async def test_go_connection_error_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    _patch_envelope_inputs_ok(monkeypatch)

    async def _send(request):
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(
        dispatcher_mod, "_build_http_client", lambda: _FakeClient(_send)
    )
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None


async def test_go_non_200_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    _patch_envelope_inputs_ok(monkeypatch)

    async def _send(request):
        return _FakeUpstream(503, [b"unavailable"])

    fake_client = _FakeClient(_send)
    monkeypatch.setattr(dispatcher_mod, "_build_http_client", lambda: fake_client)
    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=False,
    )
    assert result is None
    # a non-200 status must not leak an open connection back to the pool
    assert fake_client.closed


# ---------------------------------------------------------------------------
# streaming (the successful path)
# ---------------------------------------------------------------------------


async def test_success_streams_chunks_through_unbuffered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable(monkeypatch)
    _patch_envelope_inputs_ok(monkeypatch)

    expected_chunks = [b" ", b" ", b'{"summary": "ok"}']

    async def _send(request):
        assert request["headers"]["Authorization"] == "Bearer fake-envelope"
        return _FakeUpstream(200, expected_chunks)

    fake_client = _FakeClient(_send)
    monkeypatch.setattr(dispatcher_mod, "_build_http_client", lambda: fake_client)

    result = await maybe_dispatch_investment_explain_to_go(
        _make_request(),
        current_user=_sample_user(),
        llm_provider="auto",
        force_refresh=True,
    )

    assert isinstance(result, StreamingResponse)
    assert result.status_code == 200
    assert result.media_type == "application/json"
    # the request was made with stream=True -- a buffered client.post(...)
    # would defeat the whole point of this dispatcher (see module doc).
    assert fake_client.sent_kwargs["stream"] is True

    collected = [chunk async for chunk in result.body_iterator]
    assert collected == expected_chunks

    assert result.background is not None
    await result.background()
    assert fake_client.closed
