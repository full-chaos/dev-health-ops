"""Live E2E: validate -> batch -> stream -> worker -> sinks -> status ->
bounded recompute for the CHAOS-2690 customer-push external-ingest pipeline
(CHAOS-2702, capstone test of the epic).

Drives the REAL merged implementation end to end against REAL ClickHouse +
Postgres + Valkey (never mocks/FakeValkey for the boundary under test):

- ``POST /api/v1/external-ingest/validate`` catches an invalid record
  without ever enqueueing to the real Valkey stream.
- ``POST /api/v1/external-ingest/batches`` returns 202 + ``ingestionId`` and
  durably ``XADD``s a pointer to ``external-ingest:<org_id>:batches``.
- Idempotency replay (200, same ingestionId, no new stream entry) and
  conflict (409) -- CHAOS-2695.
- The worker is driven via the REAL production entry point,
  ``dev_health_ops.api.external_ingest.consumer.consume_external_ingest_streams
  (max_iterations=1)`` -- the actual ``ExternalIngestStreamConsumer``, doing a
  real ``XREADGROUP``/entry-parse/``process_batch``/``XACK`` pass over
  whatever this org's real Valkey-enqueued pointer(s) look like, NOT a direct
  hardcoded call to ``process_batch(...)`` (which would bypass the stream
  metadata contract entirely). Run off the event loop via
  ``asyncio.to_thread`` since it's a synchronous, blocking-read function
  (mirrors how Celery actually invokes it -- a separate thread with no
  ambient event loop, which is what lets ``run_async`` inside it create its
  own fresh loop safely). It normalizes and writes through the real
  ClickHouse sinks -- rows verified via ``FINAL`` + ``org_id`` predicate for
  all 9 v1 record-kind families.
- ``GET /batches/{id}`` reports accepted/rejected counts and per-record
  rejection diagnostics for a deliberately-invalid record.
- Bounded metric recompute is proven QUEUED, not executed inline. The real
  dispatch seam (CHAOS-2699) is
  ``workers.external_ingest_recompute.flush_external_ingest_recompute
  .apply_async(countdown=...)`` -- the real ``celery_app.send_task`` calls
  for ``run_daily_metrics``/etc. only happen inside THAT task, which this
  test never runs. Only ``apply_async`` is patched (``patch.object`` on the
  real, registered task object) so the real task/registration/debounce path
  (including the real Valkey SETNX guard key) still runs; the test also
  asserts the task's production dotted name to catch a rename.
- Disabled-source (403) and stream-unavailable (503, never accept-and-warn)
  regressions.

Because ``consume_external_ingest_streams`` discovers ALL orgs' streams via
a wildcard pattern (``external-ingest:*:batches``), a single call can sweep
up pending entries left behind by earlier tests in this module (each uses
its own org_id, so this is inert cross-contamination, not a correctness
bug) -- assertions below are written to tolerate that rather than assume
exactly-one-batch-per-call.

Client selection (black-box vs white-box): most scenarios drive the app via
``client``, which targets a REAL, harness-booted ``dev-hops api`` server
process when ``LIVE_E2E_BASE_URL`` is set (``ci/run_live_backend_e2e.sh``
exports it), falling back to an in-process ``ASGITransport`` for standalone
local runs. The stream-unavailable regression is a deliberate, permanent
EXCEPTION: it monkeypatches an in-process module attribute
(``streams.get_redis_client``), which cannot reach into a separately-booted
server process, so it always uses the always-in-process ``asgi_client``
fixture regardless of ``LIVE_E2E_BASE_URL``.

Opt-in (filtered from unit/CI-unit runs): ``pytest -m clickhouse``. Requires
``CLICKHOUSE_URI``, ``POSTGRES_URI`` (or ``DATABASE_URI``), and ``REDIS_URL``
simultaneously -- the only module in this repo that needs all three live
services at once. Run via ``ci/run_live_backend_e2e.sh`` (extended for this
issue), not ``ci/local_validate.sh`` (ClickHouse-only, per that script's own
docstring).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

LIVE_E2E_BASE_URL = os.environ.get("LIVE_E2E_BASE_URL")

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
POSTGRES_URI = os.environ.get("POSTGRES_URI") or os.environ.get("DATABASE_URI")
REDIS_URL = os.environ.get("REDIS_URL")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not (CLICKHOUSE_URI and POSTGRES_URI and REDIS_URL),
        reason="Requires CLICKHOUSE_URI, POSTGRES_URI/DATABASE_URI, and REDIS_URL "
        "(live customer-push E2E: run via ci/run_live_backend_e2e.sh, not "
        "ci/local_validate.sh).",
    ),
    # Module-scoped event loop (not pytest-asyncio's per-function default):
    # dev_health_ops.db caches its async Postgres engine as a module global
    # keyed to whichever event loop first created it -- a fresh per-function
    # loop would make every test after the first crash with "Future attached
    # to a different loop" the moment it touched the same cached engine the
    # ASGI app and this module's own fixtures both share.
    pytest.mark.asyncio(loop_scope="module"),
]

BASE = "/api/v1/external-ingest"

# All 9 v1 ClickHouse sink tables this batch must land at least one row in
# (git-family + org-scoped kinds via the async ClickHouseStore, work-item
# family via the sync ClickHouseMetricsSink -- brief §2.6). ReplacingMergeTree
# throughout: every read uses FINAL + an org_id predicate (house rule).
_ALL_SINK_TABLES = (
    "repos",
    "git_commits",
    "git_pull_requests",
    "git_pull_request_reviews",
    "identities",
    "teams",
    "work_items",
    "work_item_transitions",
    "work_item_dependencies",
)


def _headers(creds: dict) -> dict:
    return {"Authorization": f"Bearer {creds['token']}"}


def _redis_client():
    import valkey

    return valkey.from_url(REDIS_URL, decode_responses=True)


async def _run_consumer_pass() -> int:
    """Run one bounded ``consume_external_ingest_streams(max_iterations=1)``
    pass off the event loop (mirrors how Celery actually invokes it -- a
    separate thread with no ambient event loop).

    ``run_async`` (inside the consumer's ``handle_entries``) calls
    ``dev_health_ops.db.reset_async_engines()`` before its own
    ``asyncio.run(...)`` so ITS fresh Postgres engine is bound to the worker
    thread's temporary loop, not this test's loop -- correct for that call,
    but it leaves the module-global engine cache pointing at an engine bound
    to a loop that's about to be destroyed when the thread exits. Any
    subsequent Postgres access from THIS test's own (module-scoped) loop --
    e.g. the app's ``GET /batches/{id}`` right after this call -- would then
    crash with "Future attached to a different loop". Reset again here,
    back on this test's own loop, so the next ``get_postgres_engine()`` call
    lazily recreates a fresh engine correctly bound to it.
    """
    from dev_health_ops.api.external_ingest.consumer import (
        consume_external_ingest_streams,
    )
    from dev_health_ops.db import reset_async_engines

    accepted = await asyncio.to_thread(
        consume_external_ingest_streams, max_iterations=1
    )
    reset_async_engines()
    return accepted


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _ensure_postgres_schema():
    """Bootstrap the scratch Postgres schema via ``Base.metadata.create_all``
    (checkfirst) rather than Alembic -- this worktree's migration history has
    a pre-existing 0032/0034 multi-head overlap (per orchestrator note), and
    this is the same bootstrap ``ci/run_live_backend_e2e.sh``'s own
    ``generate_auth_token()`` already relies on for the curl-based checks
    that run before this module's pytest step.

    Importing ``dev_health_ops.api.main`` first registers every ORM model
    reachable from the app's routers (including ``models.ingest_auth``,
    ``models.external_ingest``, ``models.integrations``) onto the shared
    ``Base.metadata`` used below.
    """
    if not POSTGRES_URI:
        yield
        return
    from sqlalchemy import create_engine

    import dev_health_ops.api.main  # noqa: F401 -- registers all ORM models
    from dev_health_ops.models.git import Base

    sync_uri = POSTGRES_URI.replace("+asyncpg", "", 1)
    engine = create_engine(sync_uri)
    try:
        Base.metadata.create_all(engine, checkfirst=True)
    finally:
        engine.dispose()
    yield


@pytest.fixture(scope="module")
def ch_sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    yield sink
    sink.close()


@pytest.fixture(autouse=True)
def _reset_ingest_rate_limiter():
    """The shared slowapi ``limiter`` singleton is process-global in-memory
    state -- reset between tests so this module's ~10 POSTs never risk
    tripping INGEST_BATCH_LIMIT/INGEST_VALIDATE_LIMIT (60/minute) from prior
    test runs sharing the same process (mirrors
    tests/api/external_ingest/test_auth.py's fixture)."""
    from dev_health_ops.api.middleware import rate_limit as rate_limit_module

    reset = getattr(rate_limit_module.limiter, "reset", None)
    if reset is not None:
        try:
            reset()
        except Exception:
            pass
    yield


@pytest_asyncio.fixture(loop_scope="module")
async def client():
    """Black-box client: a real ``httpx.AsyncClient`` against the harness-
    booted ``dev-hops api`` server process when ``LIVE_E2E_BASE_URL`` is set
    (so a route-mounting/startup/uvicorn-config regression in the real
    server process is not false-greened by only ever exercising the FastAPI
    app object in-process); falls back to in-process ``ASGITransport`` for
    standalone local runs where no separate server was booted."""
    if LIVE_E2E_BASE_URL:
        async with AsyncClient(base_url=LIVE_E2E_BASE_URL) as c:
            yield c
        return

    from dev_health_ops.api.main import app

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture(loop_scope="module")
async def asgi_client():
    """ALWAYS in-process, regardless of ``LIVE_E2E_BASE_URL`` -- the
    stream-unavailable regression (scenario 9) monkeypatches
    ``streams_mod.get_redis_client`` on the module object imported into THIS
    test process; that has no effect on a separately-booted server process,
    so that one white-box test must use this fixture, never ``client``."""
    from dev_health_ops.api.main import app

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def org_id() -> str:
    # Fresh org_id per test: avoids idempotency-key/source-instance
    # collisions between tests and lets each test register its own
    # IngestSource/IngestToken without colliding on the
    # (org_id, system, instance) unique constraint.
    return f"chaos2702-e2e-{uuid.uuid4()}"


@pytest_asyncio.fixture(loop_scope="module")
async def source_and_token(org_id) -> dict:
    """Registers a customer_push source (github/acme/api) + mints a real
    ``fcpush_`` bearer token scoped to it, via direct ORM insert against the
    same Postgres engine ``require_ingest_scope`` reads from (CHAOS-2696's
    real models) -- NOT the harness's JWT ``generate_auth_token()``, which
    mints a user token for a completely different auth path."""
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.models.ingest_auth import (
        IngestSource,
        IngestSourceMode,
        IngestToken,
        generate_ingest_token,
        hash_ingest_token,
    )

    plaintext = generate_ingest_token()
    async with get_postgres_session() as session:
        source = IngestSource(
            org_id=org_id,
            system="github",
            instance="acme/api",
            mode=IngestSourceMode.CUSTOMER_PUSH.value,
            enabled=True,
        )
        session.add(source)
        await session.flush()
        source_id = source.id
        session.add(
            IngestToken(
                org_id=org_id,
                source_id=source_id,
                name="chaos-2702-e2e-token",
                token_hash=hash_ingest_token(plaintext),
                token_prefix=plaintext[:12],
                scopes=["schema:read", "ingest:write", "ingest:status"],
            )
        )
    return {"token": plaintext, "source_id": source_id, "org_id": org_id}


@pytest_asyncio.fixture(loop_scope="module")
async def disabled_source_and_token(org_id) -> dict:
    """A registered-but-disabled source + a token bound to it (scenario 8:
    ``require_matching_source``'s ``is_write_eligible()`` check must 403)."""
    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.models.ingest_auth import (
        IngestSource,
        IngestSourceMode,
        IngestToken,
        generate_ingest_token,
        hash_ingest_token,
    )

    plaintext = generate_ingest_token()
    instance = "acme/disabled-repo"
    async with get_postgres_session() as session:
        source = IngestSource(
            org_id=org_id,
            system="github",
            instance=instance,
            mode=IngestSourceMode.CUSTOMER_PUSH.value,
            enabled=False,
        )
        session.add(source)
        await session.flush()
        session.add(
            IngestToken(
                org_id=org_id,
                source_id=source.id,
                name="chaos-2702-e2e-disabled-token",
                token_hash=hash_ingest_token(plaintext),
                token_prefix=plaintext[:12],
                scopes=["ingest:write"],
            )
        )
    return {"token": plaintext, "instance": instance, "org_id": org_id}


# ---------------------------------------------------------------------------
# 1. POST /validate never enqueues
# ---------------------------------------------------------------------------


async def test_validate_rejects_invalid_record_without_enqueueing(
    client, source_and_token, org_id
):
    from dev_health_ops.api.external_ingest.streams import stream_name
    from tests._helpers_external_ingest import build_batch_envelope

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-validate-{uuid.uuid4()}",
        kinds=["repository"],
        invalid_kind="pull_request",
    )
    resp = await client.post(
        f"{BASE}/validate", json=envelope, headers=_headers(source_and_token)
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["valid"] is False
    assert body["itemsRejected"] >= 1
    assert any(
        e["code"] == "missing_required_field"
        and e["path"] == "records[1].payload.state"
        for e in body["errors"]
    ), body["errors"]

    rc = _redis_client()
    try:
        assert rc.xlen(stream_name(org_id)) == 0
    finally:
        rc.close()


# ---------------------------------------------------------------------------
# 2. POST /batches happy path -> real stream gains exactly one entry
# ---------------------------------------------------------------------------


async def test_accept_batch_enqueues_to_real_valkey_stream(
    client, source_and_token, org_id
):
    from dev_health_ops.api.external_ingest.streams import stream_name
    from tests._helpers_external_ingest import build_batch_envelope

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-batch-happy-{uuid.uuid4()}",
        kinds=["repository", "commit"],
    )
    rc = _redis_client()
    try:
        before = rc.xlen(stream_name(org_id))
        resp = await client.post(
            f"{BASE}/batches", json=envelope, headers=_headers(source_and_token)
        )
        assert resp.status_code == 202, resp.text
        body = resp.json()
        assert body["status"] == "accepted"
        assert body["itemsReceived"] == 2
        assert body["stream"] == stream_name(org_id)
        ingestion_id = body["ingestionId"]

        after = rc.xlen(stream_name(org_id))
        assert after == before + 1
        entries = rc.xrange(stream_name(org_id))
        assert entries[-1][1]["ingestion_id"] == ingestion_id
    finally:
        rc.close()


# ---------------------------------------------------------------------------
# 3. Idempotency replay: same envelope+key -> 200, same ingestionId, no
#    second stream entry (CHAOS-2695 contract)
# ---------------------------------------------------------------------------


async def test_idempotent_replay_returns_200_with_no_new_stream_entry(
    client, source_and_token, org_id
):
    from dev_health_ops.api.external_ingest.streams import stream_name
    from tests._helpers_external_ingest import build_batch_envelope

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-replay-{uuid.uuid4()}", kinds=["repository"]
    )
    resp1 = await client.post(
        f"{BASE}/batches", json=envelope, headers=_headers(source_and_token)
    )
    assert resp1.status_code == 202, resp1.text
    ingestion_id = resp1.json()["ingestionId"]

    rc = _redis_client()
    try:
        count_after_first = rc.xlen(stream_name(org_id))

        resp2 = await client.post(
            f"{BASE}/batches", json=envelope, headers=_headers(source_and_token)
        )
        assert resp2.status_code == 200, resp2.text
        body2 = resp2.json()
        assert body2["ingestionId"] == ingestion_id

        assert rc.xlen(stream_name(org_id)) == count_after_first
    finally:
        rc.close()


# ---------------------------------------------------------------------------
# 4. Idempotency conflict: same key, different payload hash -> 409
# ---------------------------------------------------------------------------


async def test_idempotency_conflict_returns_409(client, source_and_token):
    from tests._helpers_external_ingest import build_batch_envelope

    key = f"e2e-conflict-{uuid.uuid4()}"
    envelope1 = build_batch_envelope(idempotency_key=key, kinds=["repository"])
    envelope2 = build_batch_envelope(idempotency_key=key, kinds=["commit"])

    resp1 = await client.post(
        f"{BASE}/batches", json=envelope1, headers=_headers(source_and_token)
    )
    assert resp1.status_code == 202, resp1.text

    resp2 = await client.post(
        f"{BASE}/batches", json=envelope2, headers=_headers(source_and_token)
    )
    assert resp2.status_code == 409, resp2.text
    assert resp2.json()["error"]["code"] == "idempotency_conflict"


# ---------------------------------------------------------------------------
# 5./7. Worker pass -> all 9 kinds land in ClickHouse; bounded recompute is
#    QUEUED (real flush-task apply_async), never executed inline
# ---------------------------------------------------------------------------


async def test_worker_processes_all_nine_kinds_and_recompute_is_queued_not_inline(
    client, source_and_token, org_id, ch_sink
):
    from dev_health_ops.workers.external_ingest_recompute import (
        flush_external_ingest_recompute,
    )
    from tests._helpers_external_ingest import ALL_KINDS, build_batch_envelope

    # Registered production task name -- catches a rename/re-registration
    # regression that would otherwise silently defeat the patch.object below
    # (patching an attribute on the wrong/stale task object).
    assert (
        flush_external_ingest_recompute.name
        == "dev_health_ops.workers.tasks.flush_external_ingest_recompute"
    )

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-worker-full-{uuid.uuid4()}", kinds=ALL_KINDS
    )
    resp = await client.post(
        f"{BASE}/batches", json=envelope, headers=_headers(source_and_token)
    )
    assert resp.status_code == 202, resp.text
    ingestion_id = resp.json()["ingestionId"]

    # Patch ONLY the enqueue call, not the whole task object: the real
    # schedule_or_coalesce() dispatch path (including its real Valkey SETNX
    # debounce guard) still runs; only the actual Celery publish is
    # intercepted.
    with patch.object(
        flush_external_ingest_recompute, "apply_async"
    ) as mock_apply_async:
        # Real production entry point: XREADGROUP -> parse -> process_batch
        # -> XACK, not a hardcoded process_batch(...) call. This discovers
        # ALL orgs' streams via a wildcard pattern, so it may also sweep up
        # pending entries left by earlier tests in this module (each has
        # its own org_id -- harmless cross-contamination) in the same pass;
        # the return value is therefore a cross-org sum, not scoped to this
        # test's own ingestion_id, hence the weak sanity check below rather
        # than an exact-count assertion.
        accepted = await _run_consumer_pass()
    assert accepted > 0

    # (a) recompute was QUEUED for THIS org specifically -- filter by kwargs
    # rather than assert_called_once(), since other orgs swept up in the
    # same consumer pass each trigger their own independent apply_async call.
    matching_calls = [
        call
        for call in mock_apply_async.call_args_list
        if call.kwargs.get("kwargs")
        == {
            "org_id": org_id,
            "source_system": "github",
            "source_instance": "acme/api",
        }
    ]
    assert len(matching_calls) == 1, mock_apply_async.call_args_list
    assert matching_calls[0].kwargs["countdown"] > 0

    status_resp = await client.get(
        f"{BASE}/batches/{ingestion_id}", headers=_headers(source_and_token)
    )
    assert status_resp.status_code == 200, status_resp.text
    status_body = status_resp.json()
    assert status_body["status"] == "completed"
    assert status_body["itemsAccepted"] == len(ALL_KINDS)
    assert status_body["itemsRejected"] == 0

    # (b) recompute was NOT executed inline: dispatch_and_persist_scope (the
    # only writer of the batch's recompute_status/jobs, and of any
    # run_daily_metrics/work_graph/investment-materialize Celery dispatch)
    # runs exclusively from inside the flush task we just proved was merely
    # SCHEDULED, not invoked -- so immediately after the worker's status
    # flip, the batch's recompute block must still show its pre-dispatch
    # default and zero jobs.
    assert status_body["recompute"]["status"] == "not_applicable"
    assert status_body["recompute"]["jobs"] == []

    # ClickHouse rows for all 9 v1 sink tables -- FINAL + org_id predicate
    # (house rule), one row per family is sufficient.
    for table in _ALL_SINK_TABLES:
        result = ch_sink.client.query(
            f"SELECT count() FROM {table} FINAL WHERE org_id = {{org_id:String}}",
            parameters={"org_id": org_id},
        )
        assert result.result_rows[0][0] >= 1, f"no {table} row for org_id={org_id}"


# ---------------------------------------------------------------------------
# 6. GET /batches/{id} rejection diagnostics (partial status)
# ---------------------------------------------------------------------------


async def test_status_reports_partial_with_rejection_diagnostics(
    client, source_and_token, org_id
):
    from dev_health_ops.workers.external_ingest_recompute import (
        flush_external_ingest_recompute,
    )
    from tests._helpers_external_ingest import build_batch_envelope

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-partial-{uuid.uuid4()}",
        kinds=["repository"],
        invalid_kind="pull_request",
    )
    resp = await client.post(
        f"{BASE}/batches", json=envelope, headers=_headers(source_and_token)
    )
    assert resp.status_code == 202, resp.text
    ingestion_id = resp.json()["ingestionId"]

    # Same real production entry point as the previous scenario (finding 2);
    # only the enqueue call is patched (not asserted here -- covered by the
    # dedicated recompute scenario above) so a real, unpatched apply_async
    # never attempts a live Celery broker publish in this test.
    with patch.object(flush_external_ingest_recompute, "apply_async"):
        accepted = await _run_consumer_pass()
    assert accepted > 0

    status_resp = await client.get(
        f"{BASE}/batches/{ingestion_id}", headers=_headers(source_and_token)
    )
    assert status_resp.status_code == 200, status_resp.text
    body = status_resp.json()
    assert body["status"] == "partial"
    assert body["itemsAccepted"] == 1
    assert body["itemsRejected"] == 1
    assert len(body["errors"]) == 1
    err = body["errors"][0]
    assert err["index"] == 1
    assert err["kind"] == "pull_request.v1"
    assert err["code"] == "missing_required_field"
    assert err["message"]
    assert err["path"] == "records[1].payload.state"


# ---------------------------------------------------------------------------
# 8. Disabled source -> 403
# ---------------------------------------------------------------------------


async def test_disabled_source_returns_403(client, disabled_source_and_token):
    from tests._helpers_external_ingest import build_batch_envelope

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-disabled-{uuid.uuid4()}",
        kinds=["repository"],
        source_instance=disabled_source_and_token["instance"],
    )
    resp = await client.post(
        f"{BASE}/batches",
        json=envelope,
        headers={"Authorization": f"Bearer {disabled_source_and_token['token']}"},
    )
    assert resp.status_code == 403, resp.text
    assert resp.json()["error"]["code"] == "source_disabled"


# ---------------------------------------------------------------------------
# 9. Stream-unavailable -> 503, NEVER accept-and-warn (highest-value
#    regression per the brief: a reviewer pattern-matching against the
#    legacy /api/v1/ingest router's accept-and-warn behavior could otherwise
#    "fix" this back to 202 without realizing it's a deliberate divergence)
# ---------------------------------------------------------------------------


async def test_stream_unavailable_returns_503_not_202(
    asgi_client, source_and_token, monkeypatch
):
    # ALWAYS in-process (asgi_client, never the black-box `client` fixture):
    # this monkeypatches an attribute on the `streams` module as imported
    # into THIS test process. Against a separately-booted server process
    # (LIVE_E2E_BASE_URL set) that patch would be invisible -- the server
    # would enqueue successfully and this test would wrongly expect 503. See
    # the module docstring's "Client selection" note.
    from dev_health_ops.api.external_ingest import streams as streams_mod
    from tests._helpers_external_ingest import build_batch_envelope

    # get_redis_client() already swallows connection errors into None (see
    # its own docstring/try-except) -- returning None from the client
    # factory is the real, minimal way to simulate "Valkey unavailable"
    # without needing an actual outage.
    monkeypatch.setattr(streams_mod, "get_redis_client", lambda: None)

    envelope = build_batch_envelope(
        idempotency_key=f"e2e-stream-unavailable-{uuid.uuid4()}",
        kinds=["repository"],
    )
    resp = await asgi_client.post(
        f"{BASE}/batches", json=envelope, headers=_headers(source_and_token)
    )
    assert resp.status_code == 503, resp.text
    assert resp.json()["error"]["code"] == "stream_unavailable"
