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


@pytest.fixture
def router() -> GoApiDispatchRouter[GraphQLContext, None]:
    from dev_health_ops.api.graphql.schema import schema

    return GoApiDispatchRouter[GraphQLContext, None](schema=schema, path="")


@pytest.fixture(autouse=True)
def _configure_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GO_API_QUERY_API_URL", "http://query-api.test:8090")
    monkeypatch.setenv("GO_API_SCHEMA_DIGEST", "sha256:test-schema-digest")
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


async def test_go_timeout_falls_back_to_python(
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
    assert result is None


async def test_go_connection_error_falls_back_to_python(
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
    assert result is None


async def test_go_5xx_falls_back_to_python(
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
    assert result is None


async def test_go_404_digest_miss_falls_back_and_alerts(
    router: GoApiDispatchRouter,
    routing_row_mode,
    valid_envelope_inputs,
    monkeypatch: pytest.MonkeyPatch,
):
    """A LOCAL catalog match that still 404s at query-api is digest DRIFT
    (post-CHAOS-4696), not 'unregistered' -- must be counted distinctly
    (GO_API_DISPATCH_DIGEST_MISS_TOTAL) and still fail closed to Python."""
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

    assert result is None
    assert incremented["n"] == 1


async def test_go_405_falls_back_to_python(
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
    assert result is None


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

    schema_digest = os.getenv("GO_API_SCHEMA_DIGEST", "sha256:test-schema-digest")
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
