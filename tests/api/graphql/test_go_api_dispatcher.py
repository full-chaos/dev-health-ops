"""Tests for the Python edge dispatcher (CHAOS-4697).

Exercises :class:`GoApiDispatchRouter`'s dispatch DECISION directly
(``_maybe_dispatch_to_go``) with the registry lookup, envelope issuer, and
outbound HTTP call monkeypatched -- this is the fail-closed table from the
CHAOS-4697 brief, proven branch by branch. ``test_go_api_dispatcher_live_registry.py``-shaped
coverage (a real inserted-and-removed routing row against a real registry)
lives in :func:`test_dispatch_reads_a_real_inserted_routing_row_and_falls_back_after_removal`
below, opt-in against ``DEV_HEALTH_POSTGRES_TEST_URI`` -- the acceptance bar
CHAOS-4697's brief sets ("prove it with a row you insert and remove").
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request
from starlette.responses import Response
from strawberry.fastapi import GraphQLRouter
from strawberry.types.unset import UNSET

from dev_health_ops.api.graphql import go_api_dispatcher
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.go_api_dispatcher import GoApiDispatchRouter
from dev_health_ops.api.graphql.principal_envelope import EnvelopeSigningKeyError
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.types import LicenseTier

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_request(
    method: str,
    *,
    query_string: bytes = b"",
    body: bytes = b"",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    scope = {
        "type": "http",
        "method": method,
        "path": "/graphql",
        "query_string": query_string,
        "headers": headers or [],
    }
    state = {"sent": False}

    async def receive() -> dict[str, Any]:
        if state["sent"]:
            return {"type": "http.disconnect"}
        state["sent"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def _post_request(query: str, variables: dict[str, Any] | None = None) -> Request:
    payload: dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    body = json.dumps(payload).encode()
    return _make_request(
        "POST", body=body, headers=[(b"content-type", b"application/json")]
    )


def _get_request(query: str, variables: dict[str, Any] | None = None) -> Request:
    qs = f"query={query}"
    if variables is not None:
        qs += f"&variables={json.dumps(variables)}"
    return _make_request("GET", query_string=qs.encode())


def _context(*, user: AuthenticatedUser | None, tier=None, licensed_features=None):
    return GraphQLContext(
        org_id="11111111-1111-4111-8111-111111111111",
        db_url="clickhouse://localhost:8123/default",
        user=user,
        tier=tier,
        licensed_features=licensed_features,
    )


def _sample_user() -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id="22222222-2222-4222-8222-222222222222",
        email="dev@example.com",
        org_id="11111111-1111-4111-8111-111111111111",
        role="admin",
        is_superuser=False,
    )


TEST_OPERATION = "testOperation"
TEST_QUERY = "query Test { thing { id } }"


def _assert_go_failure(
    result: Response | None, *, reason: str, status: int | None = None
) -> dict[str, Any]:
    """A Go-plane failure is answered by the edge as a typed GraphQL error --
    never None, which would hand the request to a Python resolver that has no
    body for a routed operation."""
    assert result is not None
    assert result.status_code == 200
    body = json.loads(bytes(result.body))
    assert body["data"] is None
    (error,) = body["errors"]
    ext = error["extensions"]
    assert ext["code"] == reason
    assert ext["operation"] == TEST_OPERATION
    assert ext["plane"] == "go"
    assert isinstance(ext["elapsedMs"], int) and ext["elapsedMs"] >= 0
    assert ext.get("status") == status
    assert TEST_OPERATION in error["message"] and reason in error["message"]
    assert "no Python implementation" not in error["message"]
    return body


@pytest.fixture
def router() -> GoApiDispatchRouter[GraphQLContext, None]:
    from dev_health_ops.api.graphql.schema import schema

    return GoApiDispatchRouter[GraphQLContext, None](schema=schema, path="")


@pytest.fixture(autouse=True)
def _configure_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GO_API_QUERY_API_URL", "http://query-api.test:8090")
    # Every test's query digests to something -- point the catalog lookup
    # at a fixed test operation regardless of the actual digest, so tests
    # don't depend on go_api_operations.json's real contents.
    monkeypatch.setattr(
        go_api_dispatcher, "operation_for_digest", lambda digest: TEST_OPERATION
    )

    # No real Postgres by default -- individual tests monkeypatch
    # lookup_routing_state directly; get_postgres_session must still be
    # awaitable without a real engine.
    @asynccontextmanager
    async def _fake_session() -> AsyncIterator[None]:
        yield None

    monkeypatch.setattr(go_api_dispatcher, "get_postgres_session", _fake_session)


def _mock_transport(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture
def routing_row_mode(monkeypatch: pytest.MonkeyPatch):
    def _set(mode: str | None):
        async def _lookup(session, **kwargs):
            if mode is None:
                return None
            return SimpleNamespace(mode=mode)

        monkeypatch.setattr(go_api_dispatcher, "lookup_routing_state", _lookup)

    return _set


@pytest.fixture
def valid_envelope_inputs(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        go_api_dispatcher,
        "issue_effective_principal_envelope",
        lambda user, *, tier, licensed_features, **kw: "fake.envelope.jwt",
    )


# ---------------------------------------------------------------------------
# _extract_operation / _build_outbound_body
# ---------------------------------------------------------------------------


async def test_extract_operation_post_reads_json_body():
    request = _post_request(TEST_QUERY, {"a": 1})
    result = await go_api_dispatcher._extract_operation(request)
    assert result == (TEST_QUERY, {"a": 1}, None)


async def test_extract_operation_get_reads_query_params():
    request = _get_request(TEST_QUERY, {"a": 1})
    result = await go_api_dispatcher._extract_operation(request)
    assert result == (TEST_QUERY, {"a": 1}, None)


async def test_extract_operation_post_malformed_json_returns_none():
    request = _make_request("POST", body=b"not json")
    assert await go_api_dispatcher._extract_operation(request) is None


async def test_extract_operation_post_missing_query_field_returns_none():
    request = _make_request("POST", body=json.dumps({"variables": {}}).encode())
    assert await go_api_dispatcher._extract_operation(request) is None


async def test_extract_operation_get_missing_query_param_returns_none():
    request = _make_request("GET", query_string=b"")
    assert await go_api_dispatcher._extract_operation(request) is None


async def test_build_outbound_body_post_is_byte_for_byte_original():
    """The core verbatim-forwarding guarantee: for an original POST, the
    outbound body must be EXACTLY the bytes the client sent -- no
    re-parse, no re-dump, not even key reordering."""
    original_bytes = b'{"query":"query Test { thing }","variables":{"z":1,"a":2}}'
    request = _make_request("POST", body=original_bytes)
    await request.body()  # simulate _extract_operation having read it already
    outbound = await go_api_dispatcher._build_outbound_body(
        request, "query Test { thing }", {"z": 1, "a": 2}, None
    )
    assert outbound == original_bytes


async def test_build_outbound_body_get_constructs_post_body():
    """CHAOS-4706: a GET has no body to forward verbatim -- the dispatcher
    constructs one, and the query TEXT bytes inside it must be exactly
    what was extracted (no reformatting)."""
    request = _make_request("GET", query_string=b"query=" + TEST_QUERY.encode())
    outbound = await go_api_dispatcher._build_outbound_body(
        request, TEST_QUERY, {"x": 1}, "Test"
    )
    decoded = json.loads(outbound)
    assert decoded["query"] == TEST_QUERY
    assert decoded["variables"] == {"x": 1}
    assert decoded["operationName"] == "Test"


@pytest.mark.parametrize("raw", ["inf", "Infinity", "nan", "-inf"])
async def test_dispatch_timeout_seconds_rejects_non_finite_values(
    monkeypatch: pytest.MonkeyPatch, raw: str
):
    """Codex round 2 (P2, EXECUTED): float("inf") does not raise
    ValueError and `inf > 0` is True, so GO_API_DISPATCH_TIMEOUT_SECONDS=inf
    used to disable the fallback timeout entirely -- a stalled query-api
    would then hang every dispatched request forever, uncounted. Only a
    genuinely finite, positive value may be used."""
    monkeypatch.setenv("GO_API_DISPATCH_TIMEOUT_SECONDS", raw)
    assert (
        go_api_dispatcher._dispatch_timeout_seconds()
        == go_api_dispatcher._DEFAULT_DISPATCH_TIMEOUT_SECONDS
    )


async def test_dispatch_timeout_seconds_accepts_finite_positive_value(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GO_API_DISPATCH_TIMEOUT_SECONDS", "2.5")
    assert go_api_dispatcher._dispatch_timeout_seconds() == 2.5


# ---------------------------------------------------------------------------
# The fail-closed table (CHAOS-4697 brief), branch by branch
# ---------------------------------------------------------------------------


async def test_no_query_api_configured_returns_none_immediately(
    router: GoApiDispatchRouter, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("GO_API_QUERY_API_URL", raising=False)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


async def test_document_not_in_catalog_returns_none(
    router: GoApiDispatchRouter, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(go_api_dispatcher, "operation_for_digest", lambda digest: None)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


async def test_query_text_with_unpaired_surrogate_falls_back_instead_of_raising(
    router: GoApiDispatchRouter,
):
    """Codex round 1 (P2, EXECUTED): a client-supplied `\\uD800`-shaped JSON
    escape decodes to a valid Python str containing a lone surrogate --
    json.loads accepts it, but str.encode("utf-8") (inside document_digest)
    raises UnicodeEncodeError. That must never escape as an unhandled
    exception (turning an otherwise-fine request into a 500 instead of
    Python's normal response, uncounted) -- it must fall back cleanly."""
    body = json.dumps({"query": "\ud800"}).encode("utf-8")
    request = _make_request("POST", body=body)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(request, context)

    assert result is None


async def test_digest_computation_failure_logs_consistent_fallback_line(
    router: GoApiDispatchRouter,
    caplog: pytest.LogCaptureFixture,
):
    """CHAOS-4710 codex round 1 (P2, EXECUTED): this branch used to
    increment GO_API_DISPATCH_FALLBACK_TOTAL directly and return, bypassing
    _fallback()'s new consistent plane-decision INFO line -- the one
    fallback reason reachable BEFORE an operation/digest is known was
    exactly the one left half-instrumented by the original fix. Must now
    log go_api_dispatch.fallback (operation=unknown, plane=python,
    reason=digest_computation_error) same as every other fallback reason."""
    body = json.dumps({"query": "\ud800"}).encode("utf-8")
    request = _make_request("POST", body=body)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    with caplog.at_level(logging.INFO, logger=go_api_dispatcher.__name__):
        result = await router._maybe_dispatch_to_go(request, context)

    assert result is None
    fallback = [
        r.getMessage()
        for r in caplog.records
        if "go_api_dispatch.fallback" in r.getMessage()
    ]
    assert len(fallback) == 1, f"want exactly one fallback log line, got {fallback}"
    assert "operation=unknown" in fallback[0]
    assert "plane=python" in fallback[0]
    assert "reason=digest_computation_error" in fallback[0]


async def test_registry_lookup_raises_falls_back_to_python(
    router: GoApiDispatchRouter, monkeypatch: pytest.MonkeyPatch
):
    async def _raise(session, **kwargs):
        raise RuntimeError("db unreachable")

    monkeypatch.setattr(go_api_dispatcher, "lookup_routing_state", _raise)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


async def test_no_routing_row_falls_back_to_python(
    router: GoApiDispatchRouter, routing_row_mode
):
    routing_row_mode(None)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


@pytest.mark.parametrize("mode", ["python", "disabled"])
async def test_safe_default_modes_fall_back_to_python(
    router: GoApiDispatchRouter, routing_row_mode, mode: str
):
    routing_row_mode(mode)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


async def test_shadow_mode_falls_back_to_python_with_distinct_telemetry(
    router: GoApiDispatchRouter, routing_row_mode
):
    """mode='shadow' must NOT silently behave like mode='python' -- the
    client still gets served (falls back), but it must be a distinguishable,
    loudly-logged outcome (go_api_dispatch_telemetry's
    mode_shadow_unimplemented reason), never indistinguishable from
    "not canaried"."""
    routing_row_mode("shadow")
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    from dev_health_ops.api.graphql.go_api_dispatch_telemetry import (
        GO_API_DISPATCH_FALLBACK_TOTAL,
    )

    calls: list[tuple[str, str]] = []
    original_labels = GO_API_DISPATCH_FALLBACK_TOTAL.labels

    def _tracking_labels(*, operation, reason):
        calls.append((operation, reason))
        return original_labels(operation=operation, reason=reason)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(GO_API_DISPATCH_FALLBACK_TOTAL, "labels", _tracking_labels)
        result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is None
    assert (TEST_OPERATION, "mode_shadow_unimplemented") in calls


async def test_unauthenticated_context_never_forwards(
    router: GoApiDispatchRouter, routing_row_mode
):
    routing_row_mode("canary")
    context = _context(user=None, tier=None, licensed_features=None)
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


@pytest.mark.parametrize(
    "tier,licensed_features",
    [(None, []), (LicenseTier.TEAM, None), (None, None)],
)
async def test_missing_envelope_inputs_falls_back(
    router: GoApiDispatchRouter, routing_row_mode, tier, licensed_features
):
    routing_row_mode("canary")
    context = _context(
        user=_sample_user(), tier=tier, licensed_features=licensed_features
    )
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


async def test_envelope_signing_error_falls_back(
    router: GoApiDispatchRouter, routing_row_mode, monkeypatch: pytest.MonkeyPatch
):
    routing_row_mode("canary")

    def _raise(*args, **kwargs):
        raise EnvelopeSigningKeyError("no key configured")

    monkeypatch.setattr(go_api_dispatcher, "issue_effective_principal_envelope", _raise)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is None


@pytest.mark.parametrize("mode", ["canary", "primary"])
async def test_reachable_modes_forward_and_serve_go_response(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
):
    routing_row_mode(mode)
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = request.content
        return httpx.Response(200, json={"data": {"thing": {"id": "1"}}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    original_bytes = json.dumps({"query": TEST_QUERY}).encode()
    request = _make_request("POST", body=original_bytes)

    result = await router._maybe_dispatch_to_go(request, context)

    assert result is not None
    assert result.status_code == 200
    assert seen["url"] == "http://query-api.test:8090/query"
    assert seen["auth"] == "Bearer fake.envelope.jwt"
    assert seen["body"] == original_bytes  # verbatim


_IDENTITY_HEADERS = (
    "x-dh-internal-org-id",
    "x-dh-internal-role",
    "x-dh-internal-superuser",
    "x-dh-internal-impersonation-active",
)


def _capture_outbound(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["headers"] = request.headers
        return httpx.Response(200, json={"data": {"thing": {"id": "1"}}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    monkeypatch.setattr(
        go_api_dispatcher, "_get_internal_http_client", lambda: _mock_transport(handler)
    )
    return seen


async def test_internal_url_set_sends_the_four_identity_headers_and_no_bearer(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    """CHAOS-6144 P3: with QUERY_API_INTERNAL_URL set the edge states the
    identity it authenticated as exactly the four internal headers, each once,
    to the INTERNAL url, with NO Authorization (query-api refuses both
    carriers) and no envelope minted (tier/licensed_features are not needed)."""
    routing_row_mode("primary")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", "http://query-api-internal:8091")

    def _must_not_mint(*args, **kwargs):
        raise AssertionError("the envelope must not be minted on the header carrier")

    monkeypatch.setattr(
        go_api_dispatcher, "issue_effective_principal_envelope", _must_not_mint
    )
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=None, licensed_features=None)

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None and result.status_code == 200
    assert seen["url"] == "http://query-api-internal:8091/query"
    headers = seen["headers"]
    assert headers.get_list("x-dh-internal-org-id") == [
        "11111111-1111-4111-8111-111111111111"
    ]
    assert headers.get_list("x-dh-internal-role") == ["admin"]
    assert headers.get_list("x-dh-internal-superuser") == ["false"]
    assert headers.get_list("x-dh-internal-impersonation-active") == ["false"]
    assert headers.get("authorization") is None


@pytest.mark.parametrize("value", [None, "", "   "])
async def test_internal_url_unset_or_blank_is_todays_envelope_path(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
):
    """The gate: unset (or blank) is unchanged behaviour: the signed envelope as
    a bearer to GO_API_QUERY_API_URL, and none of the identity headers."""
    routing_row_mode("primary")
    if value is None:
        monkeypatch.delenv("QUERY_API_INTERNAL_URL", raising=False)
    else:
        monkeypatch.setenv("QUERY_API_INTERNAL_URL", value)
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None and result.status_code == 200
    assert seen["url"] == "http://query-api.test:8090/query"
    assert seen["headers"].get("authorization") == "Bearer fake.envelope.jwt"
    assert all(seen["headers"].get(name) is None for name in _IDENTITY_HEADERS)


async def test_internal_url_alone_does_not_turn_the_dispatcher_on(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    """GO_API_QUERY_API_URL stays the master kill switch."""
    routing_row_mode("primary")
    monkeypatch.delenv("GO_API_QUERY_API_URL")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", "http://query-api-internal:8091")
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is None
    assert "url" not in seen


@pytest.mark.parametrize(
    "internal_url",
    [
        "https://query-api-internal:8091",  # an Ingress host is https
        "http://user:secret@query-api-internal:8091",  # userinfo
        "http://query-api.example.com:8091",  # a public host
        "http://query-api-internal:8091/path?x=1",  # query string
        "http://query-api:8090",  # the origin of GO_API_QUERY_API_URL
        "ftp://query-api-internal:8091",
        "query-api-internal:8091",
        # r2: what httpx contacts is not what the raw string says.
        "http://attacker\u3002com:8091",  # U+3002 IDNA-normalises to attacker.com
        "http://attacker\uff0ecom:8091",  # fullwidth full stop, same
        "http://attacker\uff61com:8091",  # halfwidth ideographic full stop, same
        "http://QUERY-API.:80",
        "http://2130706433:8091",  # numeric IPv4 spelling of 127.0.0.1
        "http://0x7f000001:8091",
        "http://[::ffff:8.8.8.8]:8091",  # IPv4-mapped public address
        "http://0.0.0.0:8091",  # unspecified
        "http://a%2eb:8091",
    ],
)
async def test_internal_url_not_provably_internal_is_refused_and_nothing_is_sent(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
    internal_url: str,
):
    """The identity must never leave the cluster network: a QUERY_API_INTERNAL_URL
    that is https, carries userinfo, names a public host or is the public
    listener's own origin sends NOTHING and answers a typed, loud failure."""
    routing_row_mode("primary")
    monkeypatch.setenv("GO_API_QUERY_API_URL", "http://query-api:8090")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", internal_url)
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    _assert_go_failure(result, reason="go_internal_url_refused")
    assert "url" not in seen  # not one request was made


@pytest.mark.parametrize(
    ("internal_url", "sent"),
    [
        ("http://query-api-internal:8091", "http://query-api-internal:8091/query"),
        ("http://query-api.dev-health.svc:8091", None),
        ("http://query-api.dev-health.svc.cluster.local:8091", None),
        ("http://10.1.2.3:8091", None),
        ("http://127.0.0.1:8091", None),
        ("http://[::ffff:10.1.2.3]:8091", None),
        # What is validated is what is sent: httpx's normalised form.
        ("http://Query-Api-Internal:8091", "http://query-api-internal:8091/query"),
        ("http://query-api-internal:80", "http://query-api-internal/query"),
    ],
)
async def test_internal_url_classes_that_are_allowed(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
    internal_url: str,
    sent: str | None,
):
    routing_row_mode("primary")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", internal_url)
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=None, licensed_features=None)

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None and result.status_code == 200
    assert seen["url"] == (sent or f"{internal_url}/query")


@pytest.mark.parametrize(
    ("public_url", "internal_url"),
    [
        ("http://query-api", "http://query-api:80"),
        ("http://query-api:80", "http://query-api"),
        ("http://query-api:80", "http://QUERY-API:0080"),
        ("http://Query-Api", "http://query-api:80/"),
    ],
)
async def test_default_port_spelling_cannot_hide_the_public_origin(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
    public_url: str,
    internal_url: str,
):
    """r2: origins are compared after httpx normalisation, so an explicit :80
    and no port are the same origin (the public listener strips the headers)."""
    routing_row_mode("primary")
    monkeypatch.setenv("GO_API_QUERY_API_URL", public_url)
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", internal_url)
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=None, licensed_features=None)

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    _assert_go_failure(result, reason="go_internal_url_refused")
    assert "url" not in seen


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("http://query-api-internal:8091", "http://query-api-internal:8091"),
        ("http://Query-Api-Internal:80", "http://query-api-internal"),
        (
            "http://query-api.dev-health.svc:8091/",
            "http://query-api.dev-health.svc:8091/",
        ),
    ],
)
def test_internal_url_target_is_the_normalised_form_httpx_targets(
    value: str, expected: str
):
    """Validated == sent: the accepted URL is returned in httpx's normalised
    form and the dispatcher sends to exactly that string."""
    assert go_api_dispatcher._internal_url_target(value, None) == (expected, None)


@pytest.mark.parametrize(
    ("public_url", "internal_url"),
    [
        # r3 reviewer reproductions: the same listener under another spelling.
        ("http://172.17.0.101", "http://172.17.0.101:0"),
        ("http://172.17.0.101:80", "http://172.17.0.101:0"),
        ("http://[0:0:0:0:0:0:0:1]:8090", "http://[::1]:8090"),
        ("http://[::1]:8090", "http://[0000:0000:0000:0000:0000:0000:0000:0001]:8090"),
        ("http://127.0.0.1:8090", "http://[::ffff:127.0.0.1]:8090"),
        ("http://127.0.0.1:8090", "http://[::ffff:7f00:1]:8090"),
        ("http://2130706433:8090", "http://127.0.0.1:8090"),
        ("http://0x7f.1:8090", "http://127.0.0.1:8090"),
        ("http://0177.0.0.1:8090", "http://127.0.0.1:8090"),
        ("http://query-api.:8090", "http://query-api:8090"),
        ("http://QUERY-API:8090", "http://query-api.:8090"),
    ],
)
async def test_the_same_endpoint_under_another_spelling_is_still_the_public_origin(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
    public_url: str,
    internal_url: str,
):
    routing_row_mode("primary")
    monkeypatch.setenv("GO_API_QUERY_API_URL", public_url)
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", internal_url)
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=None, licensed_features=None)

    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    _assert_go_failure(result, reason="go_internal_url_refused")
    assert "url" not in seen


_HOST_SPELLINGS = {
    # spelling -> what the OS resolver (getaddrinfo, numeric) says it is, or the
    # lower-cased name. The oracle is the OS, not this module.
    "127.0.0.1": [
        "127.0.0.1",
        "2130706433",
        "0x7f000001",
        "0177.0.0.1",
        "127.1",
        "0x7f.1",
    ],
    "127.0.0.1/mapped": [
        "[::ffff:127.0.0.1]",
        "[::ffff:7f00:1]",
        "[0:0:0:0:0:ffff:127.0.0.1]",
    ],
    "::1": [
        "[::1]",
        "[0:0:0:0:0:0:0:1]",
        "[0000:0000:0000:0000:0000:0000:0000:0001]",
        "[::0001]",
    ],
    "10.1.2.3": ["10.1.2.3", "0xa010203", "167838211", "012.1.2.3"],
    "query-api": ["query-api", "QUERY-API", "query-api.", "Query-Api."],
    "other-svc": ["other-svc", "other-svc."],
}
_PORT_SPELLINGS = ["", ":80", ":0080", ":8090", ":08090", ":8091"]
#: spellings an INTERNAL url may use: only what is accepted as an internal host.
_INTERNAL_SPELLINGS = {
    "127.0.0.1": ["127.0.0.1"],
    "127.0.0.1/mapped": ["[::ffff:127.0.0.1]", "[::ffff:7f00:1]"],
    "::1": ["[::1]", "[0000:0000:0000:0000:0000:0000:0000:0001]"],
    "10.1.2.3": ["10.1.2.3"],
    "query-api": ["query-api", "QUERY-API"],
    "other-svc": ["other-svc"],
}


def _os_endpoint(host_spelling: str, port_spelling: str) -> tuple[str, int]:
    """What connecting to this spelling means, per the operating system's own
    numeric-host parser; names by their lower-case form. Port: none or 0 dials
    the http default."""
    import ipaddress
    import socket

    bare = host_spelling.strip("[]")
    port = int(port_spelling[1:]) if port_spelling else 80
    port = port or 80
    try:
        info = socket.getaddrinfo(
            bare,
            port,
            type=socket.SOCK_STREAM,
            flags=socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
        )[0][4]
        address = ipaddress.ip_address(str(info[0]).split("%")[0])
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        return (str(address), port)
    except socket.gaierror:
        return (bare.lower().rstrip("."), port)


def test_origin_equality_is_the_endpoint_the_socket_dials_over_every_spelling():
    """The class closed by construction (r2, r3): for every spelling of one
    endpoint as the public URL against every accepted spelling of another as the
    internal URL, the internal URL is refused as the public origin EXACTLY when
    the operating system reads both spellings as the same endpoint, and is
    accepted exactly when it does not (so the refusal is not blanket)."""
    checked = {"same": 0, "different": 0, "malformed": 0}
    for public_key, public_hosts in _HOST_SPELLINGS.items():
        for internal_key, internal_hosts in _INTERNAL_SPELLINGS.items():
            for public_host in public_hosts:
                for public_port in _PORT_SPELLINGS:
                    for internal_host in internal_hosts:
                        for internal_port in _PORT_SPELLINGS:
                            public_url = f"http://{public_host}{public_port}"
                            internal_url = f"http://{internal_host}{internal_port}"
                            target, reason = go_api_dispatcher._internal_url_target(
                                internal_url, public_url
                            )
                            try:
                                httpx.URL(public_url)
                            except httpx.InvalidURL:
                                # An origin httpx cannot read cannot be shown to
                                # differ: refused, whatever the internal spelling.
                                assert (target, reason) == (
                                    None,
                                    "public_url_malformed",
                                ), (public_url, internal_url, target, reason)
                                checked["malformed"] += 1
                                continue
                            same = _os_endpoint(
                                public_host, public_port
                            ) == _os_endpoint(internal_host, internal_port)
                            if same:
                                checked["same"] += 1
                                assert (target, reason) == (
                                    None,
                                    "same_as_public_url",
                                ), (
                                    public_url,
                                    internal_url,
                                    target,
                                    reason,
                                )
                            else:
                                checked["different"] += 1
                                assert reason is None and target is not None, (
                                    public_url,
                                    internal_url,
                                    reason,
                                )
    # Both directions must have been exercised in volume, or the property is vacuous.
    assert checked["same"] > 200 and checked["different"] > 200, checked


@pytest.mark.parametrize("port", [":0", ":00", ":000", ":65536", ":99999"])
def test_an_internal_url_with_an_impossible_port_is_refused(port: str):
    target, reason = go_api_dispatcher._internal_url_target(
        f"http://query-api-internal{port}", None
    )
    assert target is None and reason is not None


_HOSTILE_INTERNAL_URLS = [
    "http://attacker\u3002com",
    "http://attacker\u3002com:8091",
    "http://attacker\uff0ecom",
    "http://attacker\uff61com",
    "http://query-api\u3002example\u3002com",
    "http://query-api:80",
    "http://query-api:0080",
    "http://QUERY-API",
    "http://query-api.",
    "http://2130706433",
    "http://0x7f.1",
    "http://017700000001",
    "http://127.1",
    "http://[::ffff:8.8.8.8]",
    "http://[::ffff:808:808]",
    "http://[2001:db8::1]",
    "http://0.0.0.0",
    "http://8.8.8.8",
    "http://a%2eexample%2ecom",
    "http://query-api%2f@attacker.com",
    "http://query-api@attacker.com",
    "http://attacker.com#@query-api",
    "http://attacker.com?@query-api",
    "http://query-api\\@attacker.com",
    "http://query-api .attacker.com",
    "http://\u2460",
    "http://xn--",
    "http://",
    "http:///query-api",
    "  http://attacker.com  ",
]


@pytest.mark.parametrize("internal_url", _HOSTILE_INTERNAL_URLS)
async def test_whatever_httpx_contacts_is_internal_and_not_the_public_origin(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
    internal_url: str,
):
    """The property, on what httpx will ACTUALLY target (its own normalisation:
    IDNA host, default port dropped), not on the raw string: for every hostile
    spelling either nothing is sent, or the request went to an origin that is
    internal and is not the public listener's. Validated == contacted."""
    routing_row_mode("primary")
    monkeypatch.setenv("GO_API_QUERY_API_URL", "http://query-api:8090")
    public = httpx.URL("http://query-api:8090")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", internal_url)
    seen = _capture_outbound(monkeypatch)
    context = _context(user=_sample_user(), tier=None, licensed_features=None)

    await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    if "url" not in seen:
        return
    target = httpx.URL(seen["url"])
    assert target.scheme == "http" and not target.userinfo
    assert go_api_dispatcher._internal_host(target.raw_host.decode("ascii").lower()), (
        seen["url"]
    )
    assert (target.raw_host, target.port) != (public.raw_host, public.port)


async def test_non_ascii_identity_values_pass_through_as_utf8_bytes(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    """httpx encodes a str header value as ASCII and raised
    UnicodeEncodeError; query-api reads raw bytes (the string the envelope carried)."""
    routing_row_mode("primary")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", "http://query-api-internal:8091")
    seen = _capture_outbound(monkeypatch)
    user = AuthenticatedUser(
        user_id="22222222-2222-4222-8222-222222222222",
        email="dev@example.com",
        org_id="org-\u00fc",
        role="r\u00f4le",
        is_superuser=False,
    )
    result = await router._maybe_dispatch_to_go(
        _post_request(TEST_QUERY), _context(user=user)
    )
    assert result is not None and result.status_code == 200
    raw = {name.lower(): value for name, value in seen["headers"].raw}
    assert raw[b"x-dh-internal-org-id"] == "org-\u00fc".encode()
    assert raw[b"x-dh-internal-role"] == "r\u00f4le".encode()


async def test_control_characters_in_identity_values_are_refused_not_sent(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("primary")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", "http://query-api-internal:8091")
    seen = _capture_outbound(monkeypatch)
    user = AuthenticatedUser(
        user_id="22222222-2222-4222-8222-222222222222",
        email="dev@example.com",
        org_id="org-1\r\nX-DH-Internal-Superuser: true",
        role="admin",
        is_superuser=False,
    )
    result = await router._maybe_dispatch_to_go(
        _post_request(TEST_QUERY), _context(user=user)
    )
    _assert_go_failure(result, reason="go_identity_not_header_safe")
    assert "url" not in seen


async def test_go_timeout_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_timeout")


async def test_go_connection_error_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_connection_error")


async def test_go_request_error_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("reset", request=request)

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_request_error")


async def test_malformed_query_api_url_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """httpx.InvalidURL is not an httpx.HTTPError; it must not escape as a 500."""
    routing_row_mode("canary")
    monkeypatch.setenv("GO_API_QUERY_API_URL", "http://query-api.test:notaport")
    monkeypatch.setattr(go_api_dispatcher, "_get_http_client", httpx.AsyncClient)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_request_error")


@pytest.mark.parametrize(
    "content_type",
    ["text/plain", "text/html; charset=utf-8", "text/not-json", "application/jsonx"],
)
async def test_go_200_non_json_answers_typed_error_not_served_go(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    content_type: str,
):
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(
            lambda r: httpx.Response(
                200, content=b"not-json", headers={"content-type": content_type}
            )
        ),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    with caplog.at_level(logging.INFO, logger=go_api_dispatcher.__name__):
        result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_invalid_response", status=200)
    assert not [r for r in caplog.records if "served_go" in r.getMessage()]


@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "application/json; charset=utf-8",
        "application/graphql-response+json",
    ],
)
async def test_go_200_json_media_types_are_served(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
    content_type: str,
):
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(
            lambda r: httpx.Response(
                200, content=b'{"data":{}}', headers={"content-type": content_type}
            )
        ),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is not None
    assert result.body == b'{"data":{}}'


async def test_go_5xx_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(lambda r: httpx.Response(500, text="boom")),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_5xx", status=500)


async def test_go_404_digest_miss_answers_typed_error_and_alerts(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """A LOCAL catalog match that still 404s at query-api is digest DRIFT
    (post-CHAOS-4696), not 'unregistered' -- must be counted distinctly
    (GO_API_DISPATCH_DIGEST_MISS_TOTAL) and answered as a typed error."""
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(lambda r: httpx.Response(404, text="not found")),
    )

    from dev_health_ops.api.graphql.go_api_dispatch_telemetry import (
        GO_API_DISPATCH_DIGEST_MISS_TOTAL,
    )

    incremented = {"n": 0}
    original = GO_API_DISPATCH_DIGEST_MISS_TOTAL.labels

    def _tracking(*, operation):
        incremented["n"] += 1
        return original(operation=operation)

    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(GO_API_DISPATCH_DIGEST_MISS_TOTAL, "labels", _tracking)
        result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    _assert_go_failure(result, reason="go_404_digest_miss", status=404)
    assert incremented["n"] == 1


async def test_go_405_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(
            lambda r: httpx.Response(405, text="method not allowed")
        ),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_405_method_not_allowed", status=405)


async def test_go_unexpected_status_answers_typed_error(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(lambda r: httpx.Response(418, text="teapot")),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    _assert_go_failure(result, reason="go_unexpected_status", status=418)


async def test_go_failure_logs_event_and_counts_and_does_not_fall_back(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """The Go outcome is one event line and one counter increment, and the
    request never reaches the fallback funnel (whose counter means "Python
    ran")."""
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    fallback_labels: list[dict[str, str]] = []
    original_fallback = go_api_dispatcher.GO_API_DISPATCH_FALLBACK_TOTAL.labels

    def _tracking_fallback(**labels: str) -> Any:
        fallback_labels.append(labels)
        return original_fallback(**labels)

    monkeypatch.setattr(
        go_api_dispatcher.GO_API_DISPATCH_FALLBACK_TOTAL, "labels", _tracking_fallback
    )
    seen: list[dict[str, str]] = []
    original = go_api_dispatcher.GO_API_DISPATCH_GO_FAILED_TOTAL.labels

    def _tracking(**labels: str) -> Any:
        seen.append(labels)
        return original(**labels)

    monkeypatch.setattr(
        go_api_dispatcher.GO_API_DISPATCH_GO_FAILED_TOTAL, "labels", _tracking
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    with caplog.at_level(logging.INFO, logger=go_api_dispatcher.__name__):
        result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    _assert_go_failure(result, reason="go_timeout")
    assert seen == [{"operation": TEST_OPERATION, "reason": "go_timeout"}]
    assert fallback_labels == []
    messages = [r.getMessage() for r in caplog.records]
    failed = [m for m in messages if "go_api_dispatch.go_failed" in m]
    assert len(failed) == 1, failed
    assert f"operation={TEST_OPERATION}" in failed[0]
    assert "plane=go" in failed[0]
    assert "reason=go_timeout" in failed[0]
    assert "elapsed_ms=" in failed[0]
    assert not [m for m in messages if "go_api_dispatch.fallback" in m]


async def test_run_answers_go_failure_without_running_python(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """Through the real entrypoint: the response is the edge's typed error
    (plane header names the edge, not the failed Go plane) and strawberry's
    own execution is never invoked."""
    monkeypatch.setenv("GO_API_PLANE_HEADER_ENABLED", "true")
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(lambda r: httpx.Response(503, text="down")),
    )

    async def _boom(self, *args, **kwargs):
        raise AssertionError("Python execution must not run after a Go failure")

    monkeypatch.setattr(GraphQLRouter, "run", _boom)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router.run(_post_request(TEST_QUERY), context=context)

    _assert_go_failure(result, reason="go_5xx", status=503)
    assert result.headers["x-dev-health-plane"] == "python"


async def test_get_request_dispatched_as_post_to_go(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """CHAOS-4706: an original GET is forwarded to query-api as POST."""
    routing_row_mode("canary")
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"data": {}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_get_request(TEST_QUERY), context)

    assert result is not None
    assert result.status_code == 200
    assert seen["method"] == "POST"
    assert seen["body"]["query"] == TEST_QUERY


# ---------------------------------------------------------------------------
# CHAOS-4710: a Go-served request must be observable regardless of OTel
# export posture -- an INFO log line on the plane decision, and an
# env-gated response header reflecting the ACTUAL plane that served the
# response (both directions: go and every fallback to python).
# ---------------------------------------------------------------------------


async def test_served_go_logs_info_line_naming_operation_plane_and_digest(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """RED on the parent commit: before CHAOS-4710, every logger.* call in
    this module sat on a failure/fallback branch -- a request Go served
    successfully logged nothing at all, so this assertion finds zero
    matching records against the unmodified dispatcher."""
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(
            lambda r: httpx.Response(200, json={"data": {"thing": {"id": "1"}}})
        ),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    expected_digest = go_api_dispatcher.document_digest(TEST_QUERY)

    with caplog.at_level(logging.INFO, logger=go_api_dispatcher.__name__):
        result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None
    assert result.status_code == 200
    served = [
        r.getMessage()
        for r in caplog.records
        if "go_api_dispatch.served_go" in r.getMessage()
    ]
    assert len(served) == 1, f"want exactly one served_go log line, got {served}"
    assert f"operation={TEST_OPERATION}" in served[0]
    assert "plane=go" in served[0]
    assert f"document_digest={expected_digest}" in served[0]


@pytest.mark.parametrize(
    "mode,expected_reason",
    [
        (None, "no_routing_row"),
        ("python", "mode_python"),
        ("disabled", "mode_disabled"),
    ],
)
async def test_fallback_logs_info_line_naming_operation_plane_and_reason(
    router: GoApiDispatchRouter,
    routing_row_mode,
    caplog: pytest.LogCaptureFixture,
    mode: str | None,
    expected_reason: str,
):
    """RED on the parent commit: these three fallback branches (no routing
    row; mode='python'; mode='disabled') called ONLY the counter, never a
    logger -- the plane decision was silent on these paths even though a
    handful of OTHER fallback reasons already logged at ERROR. This proves
    the fallback branches now log at a CONSISTENT level too, not just the
    ones that already happened to."""
    routing_row_mode(mode)
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    with caplog.at_level(logging.INFO, logger=go_api_dispatcher.__name__):
        result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is None
    fallback = [
        r.getMessage()
        for r in caplog.records
        if "go_api_dispatch.fallback" in r.getMessage()
    ]
    assert len(fallback) == 1, f"want exactly one fallback log line, got {fallback}"
    assert f"operation={TEST_OPERATION}" in fallback[0]
    assert "plane=python" in fallback[0]
    assert f"reason={expected_reason}" in fallback[0]


async def test_plane_header_absent_by_default_on_go_served_response(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """Default OFF, the security-adjacent half: with
    GO_API_PLANE_HEADER_ENABLED unset, the header must be absent even on a
    response Go actually served."""
    monkeypatch.delenv("GO_API_PLANE_HEADER_ENABLED", raising=False)
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(
            lambda r: httpx.Response(200, json={"data": {"thing": {"id": "1"}}})
        ),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    request = _post_request(TEST_QUERY)

    result = await router.run(request, context=context)

    assert result.status_code == 200
    assert "x-dev-health-plane" not in result.headers


async def test_plane_header_go_direction_when_enabled(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """RED on the parent commit: router.run() never touches headers, so
    this header is never present regardless of the env var."""
    monkeypatch.setenv("GO_API_PLANE_HEADER_ENABLED", "true")
    routing_row_mode("canary")
    monkeypatch.setattr(
        go_api_dispatcher,
        "_get_http_client",
        lambda: _mock_transport(
            lambda r: httpx.Response(200, json={"data": {"thing": {"id": "1"}}})
        ),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    request = _post_request(TEST_QUERY)

    result = await router.run(request, context=context)

    assert result.status_code == 200
    assert result.headers.get("x-dev-health-plane") == "go"


async def test_plane_header_python_direction_when_enabled(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    """The other direction, same env var: a request that falls back to
    Python (no routing row -- _maybe_dispatch_to_go returns None, so
    run() reaches strawberry's own super().run()) must be labeled
    'python', never 'go' and never absent. super().run() itself is
    monkeypatched to a cheap stand-in Response -- this test is about the
    header this dispatcher applies to whatever strawberry returns, not
    about exercising real schema execution."""
    monkeypatch.setenv("GO_API_PLANE_HEADER_ENABLED", "true")
    routing_row_mode(None)

    async def _fake_super_run(self, request, context=UNSET, root_value=UNSET):
        return Response(content=b"{}", status_code=200, media_type="application/json")

    monkeypatch.setattr(GraphQLRouter, "run", _fake_super_run)

    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    request = _post_request(TEST_QUERY)

    result = await router.run(request, context=context)

    assert result.status_code == 200
    assert result.headers.get("x-dev-health-plane") == "python"


async def test_plane_header_absent_by_default_on_python_fallback_response(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    """Default OFF, the other direction: with the env var unset, a
    fallback-to-python response must ALSO carry no header -- proving "off"
    is genuinely off for both planes, not just the go-served one."""
    monkeypatch.delenv("GO_API_PLANE_HEADER_ENABLED", raising=False)
    routing_row_mode(None)

    async def _fake_super_run(self, request, context=UNSET, root_value=UNSET):
        return Response(content=b"{}", status_code=200, media_type="application/json")

    monkeypatch.setattr(GraphQLRouter, "run", _fake_super_run)

    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    request = _post_request(TEST_QUERY)

    result = await router.run(request, context=context)

    assert result.status_code == 200
    assert "x-dev-health-plane" not in result.headers


# ---------------------------------------------------------------------------
# Live-registry acceptance: a real row, inserted and removed, against a
# real (scratch, migrated) Postgres -- the CHAOS-4697 brief's bar
# ("prove it with a row you insert and remove in a test, or against a
# scratch registry"). Opt-in, same convention as test_go_api_registry.py.
# ---------------------------------------------------------------------------

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = "src/dev_health_ops/alembic"


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", _ALEMBIC_DIR)
    return config


@pytest.fixture
def _migrated_scratch_db_l2(monkeypatch: pytest.MonkeyPatch) -> Iterator[URL]:
    """Create+migrate a scratch Postgres DB, sync (not async) -- same
    reason as test_go_api_registry.py's ``migrated_scratch_db``: alembic's
    ``env.py`` calls ``asyncio.run(...)`` internally, which raises
    ``RuntimeError: asyncio.run() cannot be called from a running event
    loop`` if invoked from an already-async pytest fixture.
    """
    import sqlalchemy as sa

    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for this test")
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_chaos_4697_dispatcher_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')

        async_url = configured_url.set(
            drivername="postgresql+asyncpg", database=database_name
        )
        monkeypatch.setenv(
            "POSTGRES_URI", async_url.render_as_string(hide_password=False)
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "application_schema@head")

        yield async_url
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                sa.text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database_name AND pid <> pg_backend_pid()
                    """
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


@pytest_asyncio.fixture
async def live_registry_session(
    _migrated_scratch_db_l2: URL, monkeypatch: pytest.MonkeyPatch
):
    engine = create_async_engine(_migrated_scratch_db_l2)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    @asynccontextmanager
    async def _session_override() -> AsyncIterator[AsyncSession]:
        async with maker() as session:
            yield session

    monkeypatch.setattr(go_api_dispatcher, "get_postgres_session", _session_override)
    try:
        yield maker
    finally:
        await engine.dispose()


async def test_dispatch_reads_a_real_inserted_routing_row_and_falls_back_after_removal(
    router: GoApiDispatchRouter,
    live_registry_session,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """The acceptance bar, proven against a real (migrated, scratch)
    Postgres go_api_routing_state table -- not a mock:

    1. No row -> falls back to Python.
    2. Insert a canary row for this exact (schema_digest, document_digest,
       selected_operation) -> the SAME dispatcher call now forwards to Go.
    3. Flip the row to mode='disabled' (the runbook's revert -- never
       delete) -> the NEXT call falls back to Python again, with no
       process restart.
    4. Clean up the rows this test inserted (append-only candidate_build,
       so the row is neutralised via mode, matching production revert
       practice, and both rows are deleted at the end to leave the
       scratch registry as found).
    """
    from dev_health_ops.models.go_api_registry import CandidateBuild, RoutingState

    # Must be the REAL computed digest, not an arbitrary literal:
    # lookup_routing_state still filters on schema_digest (CHAOS-5013 kept
    # that filter -- see its docstring for why dropping it would let
    # scalar_one_or_none() raise MultipleResultsFound), and the dispatcher
    # under test calls go_api_dispatcher._current_schema_digest() to get
    # the value it queries with -- a mismatched literal here would make
    # step 2 below fall back to Python instead of being served by Go.
    schema_digest = go_api_dispatcher._current_schema_digest()
    document_digest_value = f"test-digest-{uuid.uuid4()}"
    selected_operation = TEST_OPERATION
    candidate_build = f"test-build-{uuid.uuid4()}"

    monkeypatch.setattr(
        go_api_dispatcher,
        "operation_for_digest",
        lambda digest: selected_operation if digest == document_digest_value else None,
    )
    monkeypatch.setattr(
        go_api_dispatcher, "document_digest", lambda text: document_digest_value
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"served": "go"}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )

    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    request_factory = lambda: _post_request(TEST_QUERY)  # noqa: E731

    # 1. No row yet -> Python.
    result = await router._maybe_dispatch_to_go(request_factory(), context)
    assert result is None

    async with live_registry_session() as session:
        session.add(
            CandidateBuild(
                schema_digest=schema_digest,
                document_digest=document_digest_value,
                selected_operation=selected_operation,
                candidate_build=candidate_build,
            )
        )
        session.add(
            RoutingState(
                schema_digest=schema_digest,
                document_digest=document_digest_value,
                selected_operation=selected_operation,
                current_candidate_build=candidate_build,
                owner="go",
                mode="canary",
                rollout_percentage=100,
            )
        )
        await session.commit()

    try:
        # 2. Row present, mode=canary -> served by Go.
        result = await router._maybe_dispatch_to_go(request_factory(), context)
        assert result is not None
        assert result.status_code == 200
        assert json.loads(bytes(result.body))["data"]["served"] == "go"

        # 3. Flip to disabled (the runbook's revert) -- no restart, next
        #    call is Python again.
        async with live_registry_session() as session:
            from sqlalchemy import update

            await session.execute(
                update(RoutingState)
                .where(
                    RoutingState.schema_digest == schema_digest,
                    RoutingState.document_digest == document_digest_value,
                    RoutingState.selected_operation == selected_operation,
                )
                .values(mode="disabled")
            )
            await session.commit()

        result = await router._maybe_dispatch_to_go(request_factory(), context)
        assert result is None
    finally:
        async with live_registry_session() as session:
            from sqlalchemy import delete

            await session.execute(
                delete(RoutingState).where(
                    RoutingState.schema_digest == schema_digest,
                    RoutingState.document_digest == document_digest_value,
                    RoutingState.selected_operation == selected_operation,
                )
            )
            await session.execute(
                delete(CandidateBuild).where(
                    CandidateBuild.schema_digest == schema_digest,
                    CandidateBuild.document_digest == document_digest_value,
                    CandidateBuild.selected_operation == selected_operation,
                )
            )
            await session.commit()


# CHAOS-5479. The reconstructed response used to carry content, status and
# media type only, so every header query-api set was dropped -- including
# `x-dev-health-build`, the one piece of per-request evidence saying WHICH
# query-api process served this request.
#
# That absence is not cosmetic. It is the gap that let an adversarial
# review produce an enablement-eligible proof receipt naming build A for a
# measurement served by build B: with several replicas and a rollout in
# progress, /buildinfo can be answered by one process while the measured
# request is served by another, and nothing downstream could tell.
async def test_the_go_build_and_plane_headers_survive_reconstruction(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"thing": {"id": "1"}}},
            headers={
                "x-dev-health-build": "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e",
                "x-dev-health-plane": "go",
                # Not in the pass-through list: proof that this is a named
                # copy of two headers and not a blanket forward, which
                # would leak whatever query-api happens to set next.
                "x-internal-detail": "must-not-be-copied",
            },
        )

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    # The plane header is gated by its own flag (see
    # test_the_plane_header_passthrough_respects_its_flag); this test is
    # about the copy itself, so the flag is on.
    monkeypatch.setenv("GO_API_PLANE_HEADER_ENABLED", "true")
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None
    assert result.headers.get("x-dev-health-build") == (
        "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e"
    ), (
        "the serving build must survive the reconstruction: without it a "
        "proof receipt cannot be bound to the process that answered"
    )
    assert result.headers.get("x-dev-health-plane") == "go"
    assert result.headers.get("x-internal-detail") is None, (
        "the pass-through is a NAMED copy of two headers, not a blanket "
        "forward of everything query-api sets"
    )


# The other half, and the more important one. An absent header must stay
# absent: "this response was not bound to a build" is a true and useful
# statement, and a fabricated or defaulted value would be a false claim
# made by the exact mechanism that exists to be trustworthy.
async def test_an_absent_build_header_is_never_invented(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        # A query-api old enough to predate the header, which is exactly
        # the deployment where guessing would be most tempting.
        return httpx.Response(200, json={"data": {"thing": {"id": "1"}}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None
    assert result.headers.get("x-dev-health-build") is None, (
        "an absent upstream build header must stay absent -- a defaulted "
        "value would let a receipt claim a binding that was never made"
    )


# r4 P1-4, a regression introduced by the pass-through above.
#
# `x-dev-health-plane` is gated by GO_API_PLANE_HEADER_ENABLED -- default
# OFF, set on the local stack only, by a 2026-09-01 ruling. Copying it
# unconditionally from the Go response turned that documented opt-out into
# always-on: the flag governs the header the ROUTER stamps, and the
# pass-through was a second, ungated way for the same header to reach a
# client.
#
# The build header is deliberately NOT gated. It is new, nothing depends on
# its absence, and a proof receipt cannot be bound to a process without it.
async def test_the_plane_header_passthrough_respects_its_flag(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"thing": {"id": "1"}}},
            headers={
                "x-dev-health-build": "build-A",
                "x-dev-health-plane": "go",
            },
        )

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    monkeypatch.delenv("GO_API_PLANE_HEADER_ENABLED", raising=False)

    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None
    assert result.headers.get("x-dev-health-plane") is None, (
        "the plane header reached a client with GO_API_PLANE_HEADER_ENABLED "
        "unset -- the pass-through must not become a second, ungated route "
        "for a header whose default-off is a standing ruling"
    )
    # The build header is not gated, and must still arrive.
    assert result.headers.get("x-dev-health-build") == "build-A", (
        "the serving build must pass through regardless: a proof receipt "
        "cannot be bound to a process without it"
    )


async def test_the_plane_header_passes_through_when_the_flag_is_on(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": {"thing": {"id": "1"}}},
            headers={"x-dev-health-plane": "go", "x-dev-health-build": "build-A"},
        )

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    monkeypatch.setenv("GO_API_PLANE_HEADER_ENABLED", "true")

    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)

    assert result is not None
    assert result.headers.get("x-dev-health-plane") == "go"
    assert result.headers.get("x-dev-health-build") == "build-A"


# ---------------------------------------------------------------------------
# Mutation documents (CHAOS-6803, R322)
# ---------------------------------------------------------------------------

TEST_MUTATION = "mutation Test { doThing { id } }"


@pytest.fixture
def mutation_catalog(monkeypatch: pytest.MonkeyPatch):
    """The catalog registers ``TEST_OPERATION`` as a mutation document."""
    monkeypatch.setattr(
        go_api_dispatcher,
        "is_mutation_operation",
        lambda operation: operation == TEST_OPERATION,
    )


def _failure_error(result: Response | None) -> dict[str, Any]:
    assert result is not None
    (error,) = json.loads(bytes(result.body))["errors"]
    return error


async def test_mutation_document_over_get_stays_on_python_and_reaches_no_go(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    mutation_catalog,
    monkeypatch: pytest.MonkeyPatch,
):
    """The edge forwards every request as a POST, so dispatching a GET would
    turn GraphQL's refusal of a mutation over GET into an executed write."""
    routing_row_mode("canary")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"data": {}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    fallbacks: list[tuple[str, str]] = []
    monkeypatch.setattr(
        go_api_dispatcher.GoApiDispatchRouter,
        "_fallback",
        staticmethod(lambda operation, reason: fallbacks.append((operation, reason))),
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(_get_request(TEST_MUTATION), context)

    assert result is None
    assert calls == []
    assert fallbacks == [(TEST_OPERATION, "mutation_over_get")]


async def test_query_document_over_get_is_still_dispatched(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"data": {"thing": None}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(_get_request(TEST_QUERY), context)

    assert result is not None and result.status_code == 200
    assert len(calls) == 1 and calls[0].method == "POST"


async def test_mutation_document_over_post_is_forwarded_verbatim(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    mutation_catalog,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"data": {"doThing": {"id": "1"}}})

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    request = _post_request(TEST_MUTATION, {"x": 1})
    original = await request.body()
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(request, context)

    assert result is not None and result.status_code == 200
    assert len(calls) == 1 and calls[0].content == original


@pytest.mark.parametrize(
    "outcome, reason, marked",
    [
        ("timeout", "go_timeout", True),
        ("request_error", "go_request_error", True),
        ("5xx", "go_5xx", True),
        ("unexpected_status", "go_unexpected_status", True),
        ("html_200", "go_invalid_response", True),
        ("connect_error", "go_connection_error", False),
        ("404", "go_404_digest_miss", False),
        ("405", "go_405_method_not_allowed", False),
    ],
)
async def test_mutation_failure_after_go_was_asked_never_runs_python_and_marks_unknown_writes(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    mutation_catalog,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    reason: str,
    marked: bool,
):
    """No Python re-run after Go was asked: the Python resolver has a body and
    would apply the write a second time. A failure after the request was sent
    leaves the write's outcome unknown, and the error says so; one that
    provably ran nothing does not."""
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        if outcome == "timeout":
            raise httpx.TimeoutException("timed out", request=request)
        if outcome == "request_error":
            raise httpx.ReadError("reset", request=request)
        if outcome == "connect_error":
            raise httpx.ConnectError("refused", request=request)
        if outcome == "html_200":
            return httpx.Response(
                200, text="<html></html>", headers={"content-type": "text/html"}
            )
        return httpx.Response(
            {"5xx": 503, "unexpected_status": 418, "404": 404, "405": 405}[outcome]
        )

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    result = await router._maybe_dispatch_to_go(_post_request(TEST_MUTATION), context)

    error = _failure_error(result)  # never None: Python did not run
    assert error["extensions"]["code"] == reason
    if marked:
        assert error["extensions"]["writeOutcome"] == "unknown"
        assert "the write may have been applied" in error["message"]
    else:
        assert "writeOutcome" not in error["extensions"]
        assert "may have been applied" not in error["message"]


async def test_query_failure_carries_no_write_outcome_marker(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("canary")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", lambda: _mock_transport(handler)
    )
    context = _context(user=_sample_user(), tier=LicenseTier.TEAM, licensed_features=[])

    error = _failure_error(
        await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    )

    assert "writeOutcome" not in error["extensions"]
    assert "may have been applied" not in error["message"]


# Every reason the dispatcher can fail a request with after query-api was asked,
# each classified: did the request possibly reach query-api and run the write?
_NEVER_RAN_A_WRITE = frozenset(
    {
        "go_connection_error",
        "go_404_digest_miss",
        "go_405_method_not_allowed",
        # CHAOS-6758: refused before any request is built or sent.
        "go_internal_url_refused",
        "go_identity_not_header_safe",
    }
)


async def test_every_go_failure_reason_is_classified_for_writes():
    """The reasons come from the dispatcher's own source (its ``_go_failed``
    call sites), not a list kept here: a new reason is unclassified, and this
    goes red until it is placed in the may-have-run set or the never-ran set."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(go_api_dispatcher))
    reasons: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_go_failed"
        ):
            literal = node.args[1]
            assert isinstance(literal, ast.Constant) and isinstance(literal.value, str)
            reasons.add(literal.value)
    assert reasons, "found no _go_failed call sites: the detector is stale"
    classified = go_api_dispatcher._WRITE_OUTCOME_UNKNOWN_REASONS | _NEVER_RAN_A_WRITE
    assert reasons == classified
    assert not go_api_dispatcher._WRITE_OUTCOME_UNKNOWN_REASONS & _NEVER_RAN_A_WRITE


def test_the_internal_carrier_client_ignores_the_proxy_environment(
    monkeypatch: pytest.MonkeyPatch,
):
    """A proxy in the path would receive the identity headers: the client the
    header carrier uses must not read HTTP_PROXY / NO_PROXY, and must not follow
    redirects; the envelope client is unchanged."""
    monkeypatch.setattr(go_api_dispatcher, "_internal_http_client", None)
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.test:3128")
    monkeypatch.setenv("http_proxy", "http://proxy.example.test:3128")
    client = go_api_dispatcher._get_internal_http_client()
    assert client.trust_env is False
    assert client.follow_redirects is False
    assert not client._mounts  # no proxy transport was mounted from the environment
    monkeypatch.setattr(go_api_dispatcher, "_internal_http_client", None)


async def test_the_header_carrier_never_uses_the_envelope_client(
    router: GoApiDispatchRouter,
    routing_row_mode,
    monkeypatch: pytest.MonkeyPatch,
):
    routing_row_mode("primary")
    monkeypatch.setenv("QUERY_API_INTERNAL_URL", "http://query-api-internal:8091")
    seen = _capture_outbound(monkeypatch)

    def _envelope_client_must_not_be_used():
        raise AssertionError("the header carrier used the proxy-trusting client")

    monkeypatch.setattr(
        go_api_dispatcher, "_get_http_client", _envelope_client_must_not_be_used
    )
    context = _context(user=_sample_user(), tier=None, licensed_features=None)
    result = await router._maybe_dispatch_to_go(_post_request(TEST_QUERY), context)
    assert result is not None and result.status_code == 200
    assert seen["url"] == "http://query-api-internal:8091/query"
