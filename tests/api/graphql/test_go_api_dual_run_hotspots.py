"""CHAOS-4369 Wave 3 stage-2 proof: local dual-run of hotspots.

Plan §5 stage 2 ("local dual-run proof"): real Python and Go servers
against the same producer-seeded scratch ClickHouse/Postgres state,
comparing the complete observable response via the CHAOS-4381 comparator
(``go_api_comparator.compare_responses``) -- not merely "both return 200".

This is Wave 3's second operation, after `complexityTimeseries` (PR
#1992). Structure follows `test_go_api_dual_run_complexity_timeseries.py`
and `test_go_api_dual_run_review_edges.py` closely -- same real-Postgres
registry-table fixture, same real-envelope minting, same real Go binary +
HTTP server harness.

Producer note (root AGENTS.md: "fixtures are producer-derived", "an
inaccurate coverage claim is worse than an admitted gap"): like
`repo_complexity_daily`, the real producer for `file_hotspot_daily`
(``run_hotspot_scan_job`` / the compounding-risk job chain) needs a real
git repository and churn history on disk -- there is no pure-function
seam. This test follows the same documented precedent as the
`featureFlags` and `complexityTimeseries` dual-run tests: it builds the
real ``FileHotspotDaily`` dataclass (``metrics/schemas.py``) directly and
writes it through the real sink entry point
(``ClickHouseMetricsSink.write_file_hotspot_daily``), the actual
persistence boundary both the real producer and this test share.

Side effects (plan §5 stage 2 also requires asserting these): checked by
reading ``resolve_hotspots``/``_fetch_hotspot_rows``/``_load_repo_labels``
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
from dev_health_ops.api.graphql.resolvers.complexity import resolve_hotspots
from dev_health_ops.api.graphql.types.complexity import HotspotsInput
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.schemas import FileHotspotDaily
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
# registeredHotspotsDocument, itself byte-identical to the REAL
# production query (web/src/lib/graphql/queries.ts's HOTSPOTS_QUERY,
# operation name "Hotspots").
HOTSPOTS_DOCUMENT = """query Hotspots($input: HotspotsInput!) {
  hotspots(input: $input) {
    rows {
      filePath
      repoId
      repoName
      churnLoc30d
      churnCommits30d
      cyclomaticTotal
      cyclomaticAvg
      blameConcentration
      riskScore
      evidenceUrl
    }
  }
}"""

SCHEMA_DIGEST = "sha256:wave3-hotspots-dual-run-test-schema-digest"
CANDIDATE_BUILD = "wave3-hotspots-dual-run-test-build"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def query_api_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
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
    db_name = f"chaos_4369_hotspots_dual_run_{uuid.uuid4().hex}"
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
                selected_operation="hotspots",
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation="hotspots",
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
                selected_operation="hotspots",
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
        user_id="44444444-4444-4444-8444-444444444444",
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
    body = json.dumps({"query": HOTSPOTS_DOCUMENT, "variables": variables}).encode()
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


def _hotspots_go_response_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _hotspots_python_response_snapshot(result) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_hotspots's return value into the same
    on-the-wire GraphQL response envelope the Go HTTP endpoint produces.
    """
    return go_api_comparator.ResponseSnapshot(
        data={
            "hotspots": {
                "rows": [
                    {
                        "filePath": r.file_path,
                        "repoId": r.repo_id,
                        "repoName": r.repo_name,
                        "churnLoc30d": r.churn_loc_30d,
                        "churnCommits30d": r.churn_commits_30d,
                        "cyclomaticTotal": r.cyclomatic_total,
                        "cyclomaticAvg": r.cyclomatic_avg,
                        "blameConcentration": r.blame_concentration,
                        "riskScore": r.risk_score,
                        "evidenceUrl": r.evidence_url,
                    }
                    for r in result.rows
                ]
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
async def test_dual_run_happy_path_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2: a real ``FileHotspotDaily`` row written through the real
    sink entry point (``write_file_hotspot_daily``), a real ``repos``
    catalog row (proving the label join resolves a real name, not just
    the fallback), real Python resolver call, real Go HTTP server --
    compared via the CHAOS-4381 comparator.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4369-hotspots-dual-run-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    computed_at = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    hotspot = FileHotspotDaily(
        repo_id=repo_id,
        day=day,
        file_path="src/main.go",
        churn_loc_30d=500,
        churn_commits_30d=20,
        cyclomatic_total=30,
        cyclomatic_avg=4.5,
        blame_concentration=0.75,
        risk_score=92.3,
        computed_at=computed_at,
        org_id=org_id,
    )

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-hotspots.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(HOTSPOTS_DOCUMENT)
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
        sink.write_file_hotspot_daily([hotspot])
        _insert_repo_catalog_row(
            sink, repo_id=repo_id, org_id=org_id, full_name="acme/backend"
        )

        since_utc = datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc)
        until_utc = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)

        python_result = await resolve_hotspots(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            HotspotsInput(
                org_id=org_id,
                since_utc=since_utc,
                until_utc=until_utc,
                repo_ids=None,
                team_ids=None,
                limit=50,
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
                    "repoIds": None,
                    "teamIds": None,
                    "limit": 50,
                }
            },
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE file_hotspot_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.client.command(
            "ALTER TABLE repos DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert len(python_result.rows) == 1, (
        "the single producer-seeded hotspot did not reach the Python resolver"
    )
    assert python_result.rows[0].repo_name == "acme/backend", (
        "expected the repos catalog label, not a repo_id fallback"
    )
    assert python_result.rows[0].evidence_url == "/code?file=src/main.go"
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _hotspots_python_response_snapshot(python_result)
    candidate = _hotspots_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"hotspots dual-run MISMATCH: terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )

    assert await _proof_run_count(registry_postgres["async"]) >= 1


@pytest.mark.asyncio
async def test_dual_run_tied_risk_score_at_limit_boundary_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """CHAOS-4513: the happy-path test above seeds exactly one hotspot --
    never enough rows to reach ``limit``, so the LIMIT boundary is never
    exercised and this operation's ORDER BY (fixed by CHAOS-4472:
    ``risk_score DESC NULLS LAST, repo_id, file_path``) is never actually
    proven total by the dual-run. This test is the harness's affordance for
    that: more tied rows than ``limit`` sharing the SAME ``risk_score``,
    seeded via SEPARATE ``write_file_hotspot_daily`` calls (CHAOS-4513's
    shared ``_tie_boundary_seeding`` standard).
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4513-hotspots-tie-boundary-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    computed_at = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)
    limit = 10
    tied_count = tied_row_count_for_limit(limit)

    # `tied_count` distinct file_paths, same repo_id/day, ALL sharing
    # risk_score=92.3 -- a genuine tie on the resolver's primary sort key.
    hotspots = [
        FileHotspotDaily(
            repo_id=repo_id,
            day=day,
            file_path=f"src/file_{i:03d}.go",
            churn_loc_30d=500,
            churn_commits_30d=20,
            cyclomatic_total=30,
            cyclomatic_avg=4.5,
            blame_concentration=0.75,
            risk_score=92.3,
            computed_at=computed_at,
            org_id=org_id,
        )
        for i in range(tied_count)
    ]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-hotspots-tie-boundary.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(HOTSPOTS_DOCUMENT)
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
        # One write_file_hotspot_daily call PER row -- `tied_count`
        # separate INSERTs, never one batched call (CHAOS-4513: a single
        # INSERT typically collapses to one part, which reads back stably
        # with or without a tie-break and proves nothing).
        for hotspot in hotspots:
            sink.write_file_hotspot_daily([hotspot])
        _insert_repo_catalog_row(
            sink, repo_id=repo_id, org_id=org_id, full_name="acme/backend"
        )

        since_utc = datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc)
        until_utc = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)

        python_result = await resolve_hotspots(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            HotspotsInput(
                org_id=org_id,
                since_utc=since_utc,
                until_utc=until_utc,
                repo_ids=None,
                team_ids=None,
                limit=limit,
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
                    "repoIds": None,
                    "teamIds": None,
                    "limit": limit,
                }
            },
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE file_hotspot_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.client.command(
            "ALTER TABLE repos DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"
    assert len(python_result.rows) == limit, (
        f"expected exactly {limit} of {tied_count} tied rows to survive the "
        f"LIMIT boundary, got {len(python_result.rows)}"
    )

    # repo_id is constant across every seeded row, so the deterministic
    # tie-break (repo_id, file_path) reduces to file_path ascending among
    # rows tied on risk_score -- deciding BOTH the survivor set AND their
    # order. Comparing sorted(actual) to sorted(expected) would only
    # prove the SET is right, not that Python's own ORDER BY produced
    # ascending order -- a regression returning the right rows in the
    # WRONG order would pass a sorted-vs-sorted check, and would pass the
    # cross-plane comparator too if Go regressed identically (codex round
    # 2 class, applied here proactively). Compare the RAW resolver order
    # directly, unsorted, against the known-correct ascending sequence.
    expected_paths = sorted(h.file_path for h in hotspots)[:limit]
    actual_paths = [r.file_path for r in python_result.rows]
    assert actual_paths == expected_paths, (
        f"Python's surviving tied-row order was not the deterministic "
        f"ascending lexicographically-smallest {limit} of {tied_count} -- "
        f"CHAOS-4472 regression: got {actual_paths}, expected {expected_paths}"
    )

    baseline = _hotspots_python_response_snapshot(python_result)
    candidate = _hotspots_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"hotspots tie-boundary dual-run MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )


@pytest.mark.asyncio
async def test_dual_run_missing_table_errors_on_both_sides_no_degraded_path(
    query_api_binary, jwks_path, registry_postgres
):
    """Same divergence-from-featureFlags documented for reviewEdges and
    complexityTimeseries: ``resolve_hotspots`` has NO missing-table
    degraded path -- a genuinely missing ``file_hotspot_daily`` table
    must surface as a real error on BOTH sides, never a well-formed empty
    success.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")

    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(CLICKHOUSE_URI)
    unmigrated_path = f"/chaos_4369_hotspots_unmigrated_{uuid.uuid4().hex}"
    unmigrated_uri = urlunsplit(
        (parts.scheme, parts.netloc, unmigrated_path, parts.query, parts.fragment)
    )

    org_id = f"chaos-4369-hotspots-degraded-{uuid.uuid4()}"

    import clickhouse_connect

    admin_client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    db_name = unmigrated_path.lstrip("/")
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-hotspots-missing-table.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(HOTSPOTS_DOCUMENT)
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
            await resolve_hotspots(
                GraphQLContext(
                    org_id=org_id, db_url=unmigrated_uri, client=python_client
                ),
                HotspotsInput(
                    org_id=org_id,
                    since_utc=since_utc,
                    until_utc=until_utc,
                    repo_ids=None,
                    team_ids=None,
                    limit=50,
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
                    "repoIds": None,
                    "teamIds": None,
                    "limit": 50,
                }
            },
        )
    finally:
        server.stop()
        admin_client.command(f"DROP DATABASE IF EXISTS `{db_name}`")

    assert python_raised, (
        "resolve_hotspots did not raise against a missing table -- "
        "complexity.py has no degraded-result path, so a missing table must "
        "be a real error, not a silent empty result"
    )
    assert "errors" in go_payload, (
        f"Go response did not carry an error for a missing table (no degraded "
        f"path exists for hotspots on either side): {go_payload}"
    )

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state="dependency_failed",
        org_id=org_id,
    )
