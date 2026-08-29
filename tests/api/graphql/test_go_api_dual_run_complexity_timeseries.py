"""CHAOS-4369 Wave 3 stage-2 proof: local dual-run of complexityTimeseries.

Plan §5 stage 2 ("local dual-run proof"): real Python and Go servers
against the same producer-seeded scratch ClickHouse/Postgres state,
comparing the complete observable response via the CHAOS-4381 comparator
(``go_api_comparator.compare_responses``) -- not merely "both return 200".

This wave is the third production canary, after CHAOS-4367's
``featureFlags`` (PR #1975) and CHAOS-4368's ``reviewEdges`` (PR #1982).
Structure follows ``test_go_api_dual_run_review_edges.py`` closely --
same real-Postgres registry-table fixture, same real-envelope minting,
same real Go binary + HTTP server harness.

Producer note (root AGENTS.md: "fixtures are producer-derived", "an
inaccurate coverage claim is worse than an admitted gap"): unlike
``reviewEdges``'s ``compute_review_edges_daily`` (a pure function of
already-fetched PR/review rows), the real producer for
``repo_complexity_daily``/``file_complexity_snapshots``
(``run_complexity_scan_job``, ``src/dev_health_ops/metrics/job_complexity.py``)
scans a REAL git repository on disk via ``ComplexityScanner`` -- there is
no pure-function seam that takes already-fetched rows the way
``compute_review_edges_daily`` does. This test therefore follows
CHAOS-4367's ``featureFlags`` dual-run test's own documented precedent
("no producer exists yet for that table"): it builds the real
``RepoComplexityDaily`` dataclass (``metrics/schemas.py``) directly and
writes it through the real sink entry point
(``ClickHouseMetricsSink.write_repo_complexity_daily``), which is the
actual persistence boundary both the real producer and this test share --
only the git-scanning orchestration layer above that boundary is
bypassed.

Side effects (plan §5 stage 2 also requires asserting these): checked by
reading ``resolve_complexity_timeseries``/``_fetch_repo_timeseries``/
``_fetch_file_timeseries``/``_load_repo_labels``
(``src/dev_health_ops/api/graphql/resolvers/complexity.py``) top to
bottom. Read-only ClickHouse queries and a dataclass construction; no
telemetry/audit hook call exists inside it or anything it calls. There is
therefore no side-effect digest to assert alongside the response digest
for this operation.
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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import cast

import pytest
import sqlalchemy as sa
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
from dev_health_ops.api.graphql.resolvers.complexity import (
    resolve_complexity_timeseries,
)
from dev_health_ops.api.graphql.types.complexity import (
    ComplexityScope,
    ComplexityTimeseriesInput,
    TimeGranularity,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.schemas import RepoComplexityDaily
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
# registeredComplexityTimeseriesDocument, itself byte-identical to the
# REAL production query (web/src/lib/graphql/queries.ts's
# COMPLEXITY_TIMESERIES_QUERY, operation name "ComplexityTimeseries").
# CHAOS-4367's own dual-run test learned this the hard way (codex round
# 3): source this from the real client file, not from the SDL or an
# inventory doc, or a wrong-name digest can match on BOTH sides here
# while a real web request 404s against the route.
COMPLEXITY_TIMESERIES_DOCUMENT = """query ComplexityTimeseries($input: ComplexityTimeseriesInput!) {
  complexityTimeseries(input: $input) {
    points {
      date
      scopeId
      scopeName
      locTotal
      cyclomaticPerKloc
      cyclomaticTotal
      cyclomaticAvg
      highComplexityFunctions
      veryHighComplexityFunctions
    }
    totalScope
  }
}"""

SCHEMA_DIGEST = "sha256:wave3-dual-run-test-schema-digest"
CANDIDATE_BUILD = "wave3-dual-run-test-build"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def query_api_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Builds the real query-api binary once per test module -- same
    "real artifact, not `go run`" discipline as the featureFlags/
    reviewEdges dual-run tests' fixture of the same name.
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
    db_name = f"chaos_4369_dual_run_{uuid.uuid4().hex}"
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
    (``CandidateBuild``/``RoutingState``/``ProofRun`` -- alembic 0114),
    same shape as the reviewEdges dual-run test's fixture of the same
    name.
    """
    assert POSTGRES_TEST_URI is not None
    db_name, dsn = _create_scratch_postgres_db(POSTGRES_TEST_URI)
    sync_engine = _sync_engine(dsn)
    try:
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
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await register_candidate_build(
                session,
                schema_digest=SCHEMA_DIGEST,
                document_digest=document_digest,
                selected_operation="complexityTimeseries",
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation="complexityTimeseries",
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
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await record_proof_run(
                session,
                schema_digest=SCHEMA_DIGEST,
                document_digest=document_digest,
                selected_operation="complexityTimeseries",
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
        user_id="33333333-3333-4333-8333-333333333333",
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
        {"query": COMPLEXITY_TIMESERIES_DOCUMENT, "variables": variables}
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
    """Same native-vs-HTTP-port translation as the featureFlags/
    reviewEdges dual-run tests' helper of the same name.
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
        "GO_API_SCHEMA_DIGEST": SCHEMA_DIGEST,
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


def _complexity_timeseries_go_response_snapshot(
    payload: dict,
) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _complexity_timeseries_python_response_snapshot(
    result,
) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_complexity_timeseries's return value into the
    same on-the-wire GraphQL response envelope the Go HTTP endpoint
    produces -- the comparator compares RESPONSES, not Python dataclasses
    vs Go structs.
    """
    return go_api_comparator.ResponseSnapshot(
        data={
            "complexityTimeseries": {
                "points": [
                    {
                        "date": p.point_date.isoformat(),
                        "scopeId": p.scope_id,
                        "scopeName": p.scope_name,
                        "locTotal": p.loc_total,
                        "cyclomaticPerKloc": p.cyclomatic_per_kloc,
                        "cyclomaticTotal": p.cyclomatic_total,
                        "cyclomaticAvg": p.cyclomatic_avg,
                        "highComplexityFunctions": p.high_complexity_functions,
                        "veryHighComplexityFunctions": p.very_high_complexity_functions,
                    }
                    for p in result.points
                ],
                "totalScope": result.total_scope,
            }
        },
        data_present=True,
        errors=(),
    )


@pytest.fixture(scope="module")
def jwks_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("jwks")


def _insert_repo_catalog_row(
    sink: ClickHouseMetricsSink, *, repo_id: uuid.UUID, org_id: str, full_name: str
) -> None:
    """Inserts one ``repos`` catalog row so ``_load_repo_labels`` resolves
    a real ``scopeName`` instead of falling back to the repo id string --
    same insert shape as ``tests/test_discover_repos_dedup_live.py``'s
    ``_insert_repo_version`` helper.
    """
    now = datetime.now(timezone.utc)
    sink.client.insert(
        "repos",
        [[repo_id, full_name, None, now, None, None, now, "local", org_id]],
        column_names=[
            "id",
            "repo",
            "ref",
            "created_at",
            "settings",
            "tags",
            "last_synced",
            "provider",
            "org_id",
        ],
    )


@pytest.mark.asyncio
async def test_dual_run_happy_path_repo_scope_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2: a real ``RepoComplexityDaily`` row written through the
    real sink entry point (``write_repo_complexity_daily``), a real
    ``repos`` catalog row (proving the label join resolves a real name,
    not just the fallback), real Python resolver call, real Go HTTP
    server -- compared via the CHAOS-4381 comparator.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4369-dual-run-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    computed_at = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    repo_daily = RepoComplexityDaily(
        repo_id=repo_id,
        day=day,
        loc_total=12_000,
        cyclomatic_total=900,
        cyclomatic_per_kloc=75.0,
        high_complexity_functions=8,
        very_high_complexity_functions=2,
        computed_at=computed_at,
        org_id=org_id,
    )

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(COMPLEXITY_TIMESERIES_DOCUMENT)
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
        sink.write_repo_complexity_daily([repo_daily])
        _insert_repo_catalog_row(
            sink, repo_id=repo_id, org_id=org_id, full_name="acme/backend"
        )

        since_utc = datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc)
        until_utc = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)

        python_result = await resolve_complexity_timeseries(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            ComplexityTimeseriesInput(
                org_id=org_id,
                since_utc=since_utc,
                until_utc=until_utc,
                granularity=TimeGranularity.DAY,
                scope=ComplexityScope.REPO,
                repo_ids=None,
                team_ids=None,
                limit=500,
            ),
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "input": {
                    "orgId": org_id,
                    "sinceUtc": since_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "untilUtc": until_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "granularity": "DAY",
                    "scope": "REPO",
                    "repoIds": None,
                    "teamIds": None,
                    "limit": 500,
                }
            },
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE repo_complexity_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.client.command(
            "ALTER TABLE repos DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert python_result.total_scope == 1, (
        "the single producer-seeded repo did not reach the Python resolver"
    )
    assert python_result.points[0].scope_name == "acme/backend", (
        "expected the repos catalog label, not a repo_id fallback"
    )
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _complexity_timeseries_python_response_snapshot(python_result)
    candidate = _complexity_timeseries_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"complexityTimeseries dual-run MISMATCH: terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )

    # Durable proof-ledger evidence: a real CANDIDATE_BUILD row, a real
    # ROUTING_STATE row, and now a real PROOF_RUN row all exist for this
    # exact (schema_digest, document_digest, selected_operation) triple.
    assert await _proof_run_count(registry_postgres["async"]) >= 1


@pytest.mark.asyncio
async def test_dual_run_missing_table_errors_on_both_sides_no_degraded_path(
    query_api_binary, jwks_path, registry_postgres
):
    """Same divergence-from-featureFlags documented for reviewEdges:
    ``resolve_complexity_timeseries`` has NO missing-table degraded path
    (unlike ``resolve_feature_flags``'s ``FEATURE_FLAG_NOT_MATERIALIZED``
    result) -- a genuinely missing ``repo_complexity_daily`` table must
    surface as a real error on BOTH sides, never a well-formed empty
    success.

    This test asserts "both sides error", not a full comparator MATCH on
    error content -- see the reviewEdges dual-run test's identically
    named test for why an exact error-content match is not a meaningful
    claim here.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")

    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(CLICKHOUSE_URI)
    unmigrated_path = f"/chaos_4369_unmigrated_{uuid.uuid4().hex}"
    unmigrated_uri = urlunsplit(
        (parts.scheme, parts.netloc, unmigrated_path, parts.query, parts.fragment)
    )

    org_id = f"chaos-4369-degraded-{uuid.uuid4()}"

    import clickhouse_connect

    admin_client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    db_name = unmigrated_path.lstrip("/")
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-missing-table.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(COMPLEXITY_TIMESERIES_DOCUMENT)
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
        since_utc = datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc)
        until_utc = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)
        python_raised = False
        try:
            await resolve_complexity_timeseries(
                GraphQLContext(
                    org_id=org_id, db_url=unmigrated_uri, client=python_client
                ),
                ComplexityTimeseriesInput(
                    org_id=org_id,
                    since_utc=since_utc,
                    until_utc=until_utc,
                    granularity=TimeGranularity.DAY,
                    scope=ComplexityScope.REPO,
                    repo_ids=None,
                    team_ids=None,
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
                    "sinceUtc": since_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "untilUtc": until_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "granularity": "DAY",
                    "scope": "REPO",
                    "repoIds": None,
                    "teamIds": None,
                    "limit": 500,
                }
            },
        )
    finally:
        server.stop()
        admin_client.command(f"DROP DATABASE IF EXISTS `{db_name}`")

    assert python_raised, (
        "resolve_complexity_timeseries did not raise against a missing table -- "
        "complexity.py has no degraded-result path, so a missing table must "
        "be a real error, not a silent empty result"
    )
    assert "errors" in go_payload, (
        f"Go response did not carry an error for a missing table (no degraded "
        f"path exists for complexityTimeseries on either side): {go_payload}"
    )

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state="dependency_failed",
        org_id=org_id,
    )
