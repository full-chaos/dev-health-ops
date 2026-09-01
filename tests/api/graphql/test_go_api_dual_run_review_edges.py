"""CHAOS-4368 Wave 2 stage-2 proof: local dual-run of reviewEdges.

Plan §5 stage 2 ("local dual-run proof"): real Python and Go servers
against the same producer-seeded scratch ClickHouse/Postgres state,
comparing the complete observable response via the CHAOS-4381 comparator
(``go_api_comparator.compare_responses``) -- not merely "both return 200".

This wave is the second production canary after CHAOS-4367's
``featureFlags`` (PR #1975) and the direct beneficiary of CHAOS-4368 Part A
(#1980, commit ``8d34d8b6e``): before Part A, ``_fetch_review_edges``'s
``ORDER BY reviews_count DESC`` had no tie-break, so ClickHouse did not
guarantee a stable row order/set among tied rows -- comparing two
non-deterministic outputs is not a valid parity proof. Part A's fix
(``ORDER BY reviews_count DESC, repo_id, reviewer, author, day``) is a
prerequisite this test relies on, not something it re-proves (that proof
already lives in ``tests/graphql/test_review_edges_tie_order_live.py``).

Side effects (plan §5 stage 2 also requires asserting these): checked by
reading ``resolve_review_edges``/``_fetch_review_edges``
(``src/dev_health_ops/api/graphql/resolvers/review_edges.py``) top to
bottom. One read-only ClickHouse query via ``query_dicts`` and a dataclass
construction; no telemetry/audit hook call exists inside it or anything it
calls. There is therefore no side-effect digest to assert alongside the
response digest for this operation.

Producer note (root AGENTS.md: "an inaccurate coverage claim is worse than
an admitted gap" / "fixtures are producer-derived"): unlike CHAOS-4367's
``featureFlags`` test (no producer exists yet for that table), a REAL
producer exists here -- ``compute_review_edges_daily``
(``src/dev_health_ops/metrics/reviews.py``) -- so this test builds
``PullRequestRow``/``PullRequestReviewRow`` inputs and runs them through
the actual producer, then persists via the real sink entry point
(``ClickHouseMetricsSink.write_review_edges``,
``metrics/sinks/clickhouse/work_graph.py``), not a hand-authored row
dict fed directly to ``client.insert`` (contrast
``test_review_edges_tie_order_live.py``, which -- for its narrower
"prove ClickHouse's ORDER BY guarantee holds" goal -- inserts raw rows
directly; the two tests have different jobs).

Seeds BOTH ends of a directed relation (CHAOS-4368 lane brief): two PRs,
each authored by the other PR's reviewer, so the resulting edges are a
genuine (A reviews B) / (B reviews A) pair -- proving reviewer/author are
not silently symmetrized or collapsed by either implementation.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import cast

import pytest
import sqlalchemy as sa
from _go_registered_documents import registered_document
from _go_schema_digest import producer_schema_digest

# Plain module import, not `from . import ...` -- this directory has no
# __init__.py; see test_go_api_dual_run_feature_flags.py's identical import.
from _tie_boundary_seeding import tied_row_count_for_limit
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.graphql import go_api_comparator, principal_envelope
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.go_api_registry import (
    record_proof_run,
    register_candidate_build,
)
from dev_health_ops.api.graphql.resolvers.review_edges import resolve_review_edges
from dev_health_ops.api.graphql.types.review_edges import ReviewEdgesInput
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.reviews import compute_review_edges_daily
from dev_health_ops.metrics.schemas import PullRequestReviewRow, PullRequestRow
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.git import Base as GitBase
from dev_health_ops.models.go_api_registry import CandidateBuild, ProofRun, RoutingState

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
    pytest.mark.skipif(
        not POSTGRES_TEST_URI,
        reason="Requires DEV_HEALTH_POSTGRES_TEST_URI for the go_api registry tables",
    ),
]

REPO_ROOT = Path(__file__).resolve().parents[3]

# Byte-identical to cmd/query-api/query_route.go's
# registeredReviewEdgesDocument, which is itself byte-identical to the
# REAL production query (web/src/lib/graphql/queries.ts's
# REVIEW_EDGES_QUERY, operation name "ReviewEdges"). CHAOS-4367's own
# dual-run test learned this the hard way (codex round 3): source this
# from the real client file, not from the SDL or an inventory doc, or a
# wrong-name digest can match on BOTH sides here while a real web request
# 404s against the route.

CANDIDATE_BUILD = "wave2-dual-run-test-build"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def query_api_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Builds the real query-api binary once per test module -- same
    "real artifact, not `go run`" discipline as the featureFlags dual-run
    test's fixture of the same name.
    """
    go = shutil.which("go")
    if go is None:
        pytest.skip("go toolchain not on PATH")
    out = tmp_path_factory.mktemp("query-api-bin") / "query-api"
    result = subprocess.run(
        [go, "build", "-o", str(out), "./cmd/query-api"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        pytest.fail(
            f"go build ./cmd/query-api failed:\n{result.stdout}\n{result.stderr}"
        )
    return str(out)


def _sync_engine(uri: str) -> Engine:
    return sa.create_engine(
        make_url(uri).set(drivername="postgresql+psycopg2"),
        isolation_level="AUTOCOMMIT",
    )


def _create_scratch_postgres_db(admin_uri: str) -> tuple[str, str]:
    """Creates a scratch DB, returns (db_name, dsn) where dsn keeps
    admin_uri's own drivername (no dialect override) -- callers derive
    the async/sync/Go-facing variants they need from it.
    """
    db_name = f"chaos_4368_dual_run_{uuid.uuid4().hex}"
    engine = _sync_engine(admin_uri)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
    finally:
        engine.dispose()
    base_url = make_url(admin_uri)
    dsn = base_url.set(database=db_name).render_as_string(hide_password=False)
    return db_name, dsn


def _drop_scratch_postgres_db(admin_uri: str, db_name: str) -> None:
    engine = _sync_engine(admin_uri)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %(db_name)s AND pid <> pg_backend_pid()",
                {"db_name": db_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE IF EXISTS "{db_name}"')
    finally:
        engine.dispose()


@pytest.fixture
def registry_postgres() -> Iterator[dict[str, str]]:
    """A scratch Postgres DB holding the REAL go_api_registry ORM tables
    (``CandidateBuild``/``RoutingState``/``ProofRun`` -- alembic 0114,
    ``dev_health_ops.models.go_api_registry``), unlike the featureFlags
    dual-run test's minimal hand-rolled ``go_api_routing_state`` table.
    CHAOS-4368's lane brief specifically asks for real
    CANDIDATE_BUILD/ROUTING_STATE/PROOF_RUN proof-ledger rows, which the
    real ORM models + ``go_api_registry.py`` helpers (rather than raw SQL)
    give for free. ``Base.metadata.create_all`` is scoped to just these
    three tables -- they only reference each other, never the rest of the
    application schema, so this does not need a full alembic upgrade.

    Returns a dict of DSNs: "go" (plain, for GO_API_REGISTRY_POSTGRES_URI
    / pgx), "async" (+asyncpg, for this test's own AsyncSession calls).
    """
    assert POSTGRES_TEST_URI is not None
    db_name, dsn = _create_scratch_postgres_db(POSTGRES_TEST_URI)
    sync_engine = _sync_engine(dsn)
    try:
        # `__table__` is typed FromClause on the declarative base stubs,
        # not the narrower Table create_all(tables=...) expects -- these
        # three ARE sa.Table instances at runtime (every mapped class's
        # __table__ is), so the cast is a typing correction, not a
        # behavior change.
        registry_tables = cast(
            list[sa.Table],
            [CandidateBuild.__table__, RoutingState.__table__, ProofRun.__table__],
        )
        GitBase.metadata.create_all(sync_engine, tables=registry_tables)
    finally:
        sync_engine.dispose()

    base_url = make_url(dsn)
    go_dsn = base_url.set(drivername="postgresql").render_as_string(hide_password=False)
    async_dsn = base_url.set(drivername="postgresql+asyncpg").render_as_string(
        hide_password=False
    )
    try:
        yield {"go": go_dsn, "async": async_dsn}
    finally:
        _drop_scratch_postgres_db(POSTGRES_TEST_URI, db_name)


def _document_digest(document: str) -> str:
    import hashlib

    return hashlib.sha256(document.strip().encode("utf-8")).hexdigest()


async def _seed_candidate_and_enable_canary(
    async_dsn: str, document_digest: str
) -> None:
    """Writes the real CANDIDATE_BUILD row (via the real
    ``register_candidate_build`` helper, never raw SQL) and a
    ROUTING_STATE row pointing at it with mode='canary' -- the two rows
    ``PostgresSwitch``/the Go route need to consider ``reviewEdges``
    reachable for this test's schema/document/operation triple.
    """
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await register_candidate_build(
                session,
                schema_digest=producer_schema_digest(),
                document_digest=document_digest,
                selected_operation="reviewEdges",
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=producer_schema_digest(),
                    document_digest=document_digest,
                    selected_operation="reviewEdges",
                    current_candidate_build=CANDIDATE_BUILD,
                    owner="go",
                    mode="canary",
                    rollout_percentage=100,
                )
                .on_conflict_do_update(
                    index_elements=[
                        "schema_digest",
                        "document_digest",
                        "selected_operation",
                    ],
                    set_={"mode": "canary", "current_candidate_build": CANDIDATE_BUILD},
                )
            )
            await session.commit()
    finally:
        await engine.dispose()


async def _record_dual_run_proof(
    async_dsn: str,
    *,
    document_digest: str,
    terminal_state: str,
    org_id: str,
) -> None:
    """Records the PROOF_RUN row for this dual-run comparison via the
    real ``record_proof_run`` helper -- the durable proof-ledger evidence
    CHAOS-4368's lane brief asks for, beyond just an in-process assertion.
    """
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await record_proof_run(
                session,
                schema_digest=producer_schema_digest(),
                document_digest=document_digest,
                selected_operation="reviewEdges",
                candidate_build=CANDIDATE_BUILD,
                request_identity=f"dual-run-{uuid.uuid4()}",
                stage="dual_run",
                terminal_state=terminal_state,
                org_id=org_id,
            )
            await session.commit()
    finally:
        await engine.dispose()


async def _proof_run_count(async_dsn: str) -> int:
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            result = await session.execute(select(ProofRun))
            return len(result.scalars().all())
    finally:
        await engine.dispose()


def _mint_envelope(org_id: str) -> tuple[str, dict, str, str]:
    """Mints a REAL envelope with the real Python issuer -- same
    cross-language-real-artifact discipline as the featureFlags dual-run
    test's fixture of the same name.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
    )

    key = Ed25519PrivateKey.generate()
    key_pem = key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    os.environ["GO_API_ENVELOPE_PRIVATE_KEY"] = key_pem

    user = AuthenticatedUser(
        user_id="22222222-2222-4222-8222-222222222222",
        email="dev@example.com",
        org_id=org_id,
        role="admin",
        is_superuser=False,
        is_superuser_verified=False,
        token_version=3,
    )
    token = principal_envelope.issue_effective_principal_envelope(
        user, tier=LicenseTier.TEAM, licensed_features=["ai_review"]
    )
    jwks = principal_envelope.build_envelope_jwks()
    return (
        token,
        jwks,
        principal_envelope.ENVELOPE_ISSUER,
        principal_envelope.ENVELOPE_AUDIENCE,
    )


def _wait_for_ready(base_url: str, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/readyz", timeout=1) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError) as exc:
            last_err = exc
        time.sleep(0.1)
    raise TimeoutError(f"query-api did not become ready: {last_err}")


def _post_graphql(base_url: str, token: str, variables: dict) -> dict:
    body = json.dumps(
        {"query": registered_document("reviewEdges"), "variables": variables}
    ).encode()
    req = urllib.request.Request(
        f"{base_url}/query",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode())


class _RunningGoServer:
    def __init__(self, process: subprocess.Popen, base_url: str) -> None:
        self.process = process
        self.base_url = base_url

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def _go_clickhouse_uri(python_clickhouse_uri: str) -> str:
    """Same native-vs-HTTP-port translation as the featureFlags dual-run
    test's helper of the same name -- see its doc comment for why this is
    a real fact about the two client libraries, not a workaround.
    """
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(python_clickhouse_uri)
    netloc = parts.netloc.replace(":8123", ":9000")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _start_go_server(
    binary: str,
    clickhouse_uri: str,
    registry_uri: str,
    jwks_path: str,
    issuer: str,
    audience: str,
) -> _RunningGoServer:
    clickhouse_uri = _go_clickhouse_uri(clickhouse_uri)
    port = _free_port()
    env = {
        **os.environ,
        "QUERY_API_ADDR": f":{port}",
        "CLICKHOUSE_URI": clickhouse_uri,
        "GO_API_REGISTRY_POSTGRES_URI": registry_uri,
        "GO_API_ENVELOPE_JWKS_PATH": jwks_path,
        "GO_API_ENVELOPE_ISSUER": issuer,
        "GO_API_ENVELOPE_AUDIENCE": audience,
        "GO_API_SCHEMA_DIGEST": producer_schema_digest(),
    }
    process = subprocess.Popen(
        [binary],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_ready(base_url)
    except TimeoutError:
        process.kill()
        out = process.stdout.read() if process.stdout else ""
        pytest.fail(f"query-api never became ready:\n{out}")
    return _RunningGoServer(process, base_url)


def _review_edges_go_response_snapshot(
    payload: dict,
) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _review_edges_python_response_snapshot(
    result,
) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_review_edges's return value into the same
    on-the-wire GraphQL response envelope the Go HTTP endpoint produces --
    the comparator compares RESPONSES, not Python dataclasses vs Go
    structs.

    __typename fields (CHAOS-4696 PR2, same fix as
    test_go_api_dual_run_feature_flags.py's snapshot builder -- see that
    file's comment for the full explanation): registered_document
    ("reviewEdges") now selects __typename on every non-root selection
    set. Type names from contracts/graphql/v1/schema.graphql
    (ReviewEdgesResult, ReviewEdgeRow).
    """
    return go_api_comparator.ResponseSnapshot(
        data={
            "reviewEdges": {
                "edges": [
                    {
                        "reviewer": e.reviewer,
                        "author": e.author,
                        "reviewsCount": e.reviews_count,
                        "day": e.day.isoformat(),
                        "repoId": e.repo_id,
                        "__typename": "ReviewEdgeRow",
                    }
                    for e in result.edges
                ],
                "totalCount": result.total_count,
                "__typename": "ReviewEdgesResult",
            }
        },
        data_present=True,
        errors=(),
    )


@pytest.fixture(scope="module")
def jwks_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("jwks")


@pytest.mark.asyncio
async def test_dual_run_happy_path_matches_directed_edges(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2: real producer-built rows (compute_review_edges_daily),
    real sink write (write_review_edges), real Python resolver call, real
    Go HTTP server -- compared via the CHAOS-4381 comparator. Seeds BOTH
    ends of a directed reviewer/author pair so a bug that symmetrized or
    swapped reviewer<->author would show up as a mismatch, not pass
    silently.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4368-dual-run-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    author_a = f"author-a-{uuid.uuid4().hex[:8]}@example.com"
    author_b = f"author-b-{uuid.uuid4().hex[:8]}@example.com"
    created = datetime(2026, 8, 10, 9, 0, 0, tzinfo=timezone.utc)
    computed_at = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    # PR #1 authored by author_a, reviewed by author_b -> edge
    # (reviewer=author_b, author=author_a). PR #2 authored by author_b,
    # reviewed by author_a -> edge (reviewer=author_a, author=author_b).
    # Directed pair, both ends present, neither collapsed into the other.
    pr_rows: list[PullRequestRow] = [
        {
            "repo_id": repo_id,
            "number": 1,
            "author_email": author_a,
            "author_name": "Author A",
            "created_at": created,
            "merged_at": created,
        },
        {
            "repo_id": repo_id,
            "number": 2,
            "author_email": author_b,
            "author_name": "Author B",
            "created_at": created,
            "merged_at": created,
        },
    ]
    review_rows: list[PullRequestReviewRow] = [
        {
            "repo_id": repo_id,
            "number": 1,
            "reviewer": author_b,
            "submitted_at": created,
            "state": "APPROVED",
        },
        {
            "repo_id": repo_id,
            "number": 2,
            "reviewer": author_a,
            "submitted_at": created,
            "state": "APPROVED",
        },
    ]

    records = compute_review_edges_daily(
        day=day,
        pull_request_rows=pr_rows,
        pull_request_review_rows=review_rows,
        computed_at=computed_at,
    )
    assert len(records) == 2, "expected exactly the two directed edges seeded above"
    records = [replace(record, org_id=org_id) for record in records]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(registered_document("reviewEdges"))
    await _seed_candidate_and_enable_canary(registry_postgres["async"], document_digest)

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        sink.write_review_edges(records)
        sink.client.command("OPTIMIZE TABLE review_edges_daily FINAL")

        python_result = await resolve_review_edges(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            ReviewEdgesInput(
                org_id=org_id,
                since_date=day,
                until_date=day,
                repo_ids=None,
                limit=500,
            ),
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "input": {
                    "orgId": org_id,
                    "sinceDate": day.isoformat(),
                    "untilDate": day.isoformat(),
                    "repoIds": None,
                    "limit": 500,
                }
            },
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE review_edges_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert python_result.total_count == 2, (
        "producer-seeded directed edges did not both reach the Python resolver"
    )
    reviewers = {e.reviewer for e in python_result.edges}
    assert reviewers == {author_a, author_b}, (
        "expected both directed edges' reviewers present, got "
        f"{reviewers} (reviewer/author may have been symmetrized or dropped)"
    )
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _review_edges_python_response_snapshot(python_result)
    candidate = _review_edges_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"reviewEdges dual-run MISMATCH: terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )

    # Durable proof-ledger evidence (CHAOS-4368 lane brief): a real
    # CANDIDATE_BUILD row, a real ROUTING_STATE row, and now a real
    # PROOF_RUN row all exist for this exact
    # (schema_digest, document_digest, selected_operation) triple.
    assert await _proof_run_count(registry_postgres["async"]) >= 1


@pytest.mark.asyncio
async def test_dual_run_tied_reviews_count_at_limit_boundary_matches(
    tmp_path_factory, query_api_binary, jwks_path, registry_postgres
):
    """CHAOS-4513: the happy-path test above seeds exactly 2 edges with
    distinct (repo_id, reviewer, author) identities -- never enough rows to
    reach ``limit``, so the LIMIT boundary is never exercised and this
    operation's ORDER BY (fixed by CHAOS-4421: ``reviews_count DESC,
    repo_id, reviewer, author, day``) is never actually proven total by the
    dual-run. This test is the harness's affordance for that: more tied
    rows than ``limit`` sharing the SAME ``reviews_count``, seeded via
    SEPARATE ``write_review_edges`` calls (CHAOS-4513's shared
    ``_tie_boundary_seeding`` standard -- one INSERT per row, never a
    single batched INSERT, so the read is a genuine multi-part scan and
    not trivially stable regardless of the tie-break).
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4513-tie-boundary-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 15)
    created = datetime(2026, 8, 15, 9, 0, 0, tzinfo=timezone.utc)
    computed_at = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)
    limit = 10
    tied_count = tied_row_count_for_limit(limit)

    # `tied_count` distinct (reviewer, author) pairs, one PR + one review
    # each, on the SAME repo/day -- every resulting edge shares
    # reviews_count=1, a genuine tie on the resolver's PRIMARY sort key.
    pr_rows: list[PullRequestRow] = []
    review_rows: list[PullRequestReviewRow] = []
    for i in range(tied_count):
        author = f"author-{i:03d}-{uuid.uuid4().hex[:6]}@example.com"
        reviewer = f"reviewer-{i:03d}-{uuid.uuid4().hex[:6]}@example.com"
        pr_rows.append(
            {
                "repo_id": repo_id,
                "number": i + 1,
                "author_email": author,
                "author_name": f"Author {i:03d}",
                "created_at": created,
                "merged_at": created,
            }
        )
        review_rows.append(
            {
                "repo_id": repo_id,
                "number": i + 1,
                "reviewer": reviewer,
                "submitted_at": created,
                "state": "APPROVED",
            }
        )

    records = compute_review_edges_daily(
        day=day,
        pull_request_rows=pr_rows,
        pull_request_review_rows=review_rows,
        computed_at=computed_at,
    )
    assert len(records) == tied_count, (
        f"expected exactly {tied_count} tied edges, computed {len(records)}"
    )
    assert {r.reviews_count for r in records} == {1}, (
        "expected every seeded edge to share reviews_count=1 -- a broken "
        "seed here would silently defeat the tie-boundary proof"
    )
    records = [replace(record, org_id=org_id) for record in records]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-tie-boundary.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(registered_document("reviewEdges"))
    await _seed_candidate_and_enable_canary(registry_postgres["async"], document_digest)

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        # One write_review_edges call PER record -- `tied_count` separate
        # INSERTs, never one batched call (CHAOS-4513: a single INSERT
        # typically collapses to one part, which reads back stably with or
        # without a tie-break and proves nothing).
        for record in records:
            sink.write_review_edges([record])

        python_result = await resolve_review_edges(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            ReviewEdgesInput(
                org_id=org_id,
                since_date=day,
                until_date=day,
                repo_ids=None,
                limit=limit,
            ),
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "input": {
                    "orgId": org_id,
                    "sinceDate": day.isoformat(),
                    "untilDate": day.isoformat(),
                    "repoIds": None,
                    "limit": limit,
                }
            },
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE review_edges_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"
    assert python_result.total_count == limit, (
        f"expected exactly {limit} of {tied_count} tied rows to survive the "
        f"LIMIT boundary, got total_count={python_result.total_count}"
    )

    # The deterministic tie-break (repo_id, reviewer, author, day) makes
    # the surviving `limit` rows -- among `tied_count` rows tied on
    # reviews_count -- the lexicographically-smallest (reviewer, author)
    # tuples, IN ASCENDING ORDER (repo_id/day are constant across every
    # seeded row, so the tie-break reduces to (reviewer, author) ASC).
    # Comparing sorted(actual) to sorted(expected) would only prove the
    # SET is right, not that Python's own ORDER BY produced ascending
    # order -- a regression returning the right rows in the WRONG order
    # would pass a sorted-vs-sorted check, and would pass the cross-plane
    # comparator too if Go regressed identically (codex round 2 class,
    # applied here proactively). Compare the RAW resolver order directly,
    # unsorted, against the known-correct ascending sequence.
    # review_rows[i]["reviewer"] pairs with pr_rows[i]["author_email"] by
    # construction above (both built from the same loop index i).
    expected_pairs = sorted(
        (review_rows[i]["reviewer"], pr_rows[i]["author_email"])
        for i in range(tied_count)
    )[:limit]
    actual_pairs = [(e.reviewer, e.author) for e in python_result.edges]
    assert actual_pairs == expected_pairs, (
        f"Python's surviving tied-row order was not the deterministic "
        f"ascending lexicographically-smallest {limit} of {tied_count} -- "
        f"CHAOS-4421 regression: got {actual_pairs}, expected {expected_pairs}"
    )

    baseline = _review_edges_python_response_snapshot(python_result)
    candidate = _review_edges_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"reviewEdges tie-boundary dual-run MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )


@pytest.mark.asyncio
async def test_dual_run_missing_table_errors_on_both_sides_no_degraded_path(
    tmp_path_factory, query_api_binary, jwks_path, registry_postgres
):
    """The deliberate divergence from featureFlags this port documents:
    resolve_review_edges has NO missing-table degraded path (unlike
    resolve_feature_flags's FEATURE_FLAG_NOT_MATERIALIZED result) -- a
    genuinely missing review_edges_daily table must surface as a real
    error on BOTH sides, never a well-formed empty success.

    This test asserts "both sides error", not a full comparator MATCH on
    error content: Strawberry's serialization of an unhandled Python
    exception into a GraphQL error is intentionally generic (it does not
    expose exception internals), so its exact message/path is not a
    meaningful thing to pin against Go's error text. Documented here
    rather than silently claimed as a comparator-verified match (root
    AGENTS.md: "an inaccurate coverage claim is worse than an admitted
    gap").
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")

    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(CLICKHOUSE_URI)
    unmigrated_path = f"/chaos_4368_unmigrated_{uuid.uuid4().hex}"
    unmigrated_uri = urlunsplit(
        (parts.scheme, parts.netloc, unmigrated_path, parts.query, parts.fragment)
    )

    org_id = f"chaos-4368-degraded-{uuid.uuid4()}"
    day = date(2026, 8, 10)

    import clickhouse_connect

    admin_client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    db_name = unmigrated_path.lstrip("/")
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-missing-table.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(registered_document("reviewEdges"))
    await _seed_candidate_and_enable_canary(registry_postgres["async"], document_digest)

    server = _start_go_server(
        query_api_binary,
        unmigrated_uri,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        python_client = clickhouse_connect.get_client(dsn=unmigrated_uri)
        python_raised = False
        try:
            await resolve_review_edges(
                GraphQLContext(
                    org_id=org_id, db_url=unmigrated_uri, client=python_client
                ),
                ReviewEdgesInput(
                    org_id=org_id,
                    since_date=day,
                    until_date=day,
                    repo_ids=None,
                    limit=500,
                ),
            )
        except Exception:
            python_raised = True

        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "input": {
                    "orgId": org_id,
                    "sinceDate": day.isoformat(),
                    "untilDate": day.isoformat(),
                    "repoIds": None,
                    "limit": 500,
                }
            },
        )
    finally:
        server.stop()
        admin_client.command(f"DROP DATABASE IF EXISTS `{db_name}`")

    assert python_raised, (
        "resolve_review_edges did not raise against a missing table -- "
        "review_edges.py has no degraded-result path, so a missing table "
        "must be a real error, not a silent empty result"
    )
    assert "errors" in go_payload, (
        f"Go response did not carry an error for a missing table (no degraded "
        f"path exists for reviewEdges on either side): {go_payload}"
    )

    # terminal_state="dependency_failed", not "match": this test does not
    # run the comparator (see the docstring for why an exact error-content
    # match is not a meaningful claim here) -- a missing ClickHouse table
    # is exactly plan §5's "dependency_failed" terminal state, and
    # recording "match" here would be an unearned claim the comparator
    # never actually verified.
    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state="dependency_failed",
        org_id=org_id,
    )
