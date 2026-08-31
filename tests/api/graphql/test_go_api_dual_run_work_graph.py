"""CHAOS-4504 (Wave 4 Lane A) stage-2 proof: local dual-run of
workGraphEdges / workGraphFlow / workGraphArtifacts.

Same harness discipline as the five prior dual-run tests (plan §5 stage 2:
real Python and Go servers against the same producer-seeded scratch
ClickHouse/Postgres state, compared via the CHAOS-4381 comparator) --
module-level plumbing (Go binary build, Postgres registry fixture, JWT
minting, server start/stop, GraphQL POST helper) is copied from
``test_go_api_dual_run_review_edges.py`` and generalized to accept an
operation name, since this file drives THREE operations against one
running server rather than one.

THIS FILE'S COMPARATOR VERDICT IS NOT UNIFORMLY "MATCH" -- BY DESIGN
======================================================================
``workGraphEdges``' Go port (``cmd/query-api/internal/workgraph``)
carries a dedup fix (CHAOS-4515) that Python's resolver
(``work_graph.py:1183``, no ``FINAL``/argMax on
``work_graph_edges``, a ``ReplacingMergeTree(last_synced)`` table) does
NOT have and, per chris's 06:52 PT 08-29 ruling, never will (Python is
frozen). So:

- ``test_dual_run_edges_dedup_is_expected_divergence`` seeds a genuine
  un-merged duplicate edge version and asserts the comparator returns
  ``mismatch`` -- Python returns BOTH stale/duplicated rows, Go returns
  only the deduped one. A MATCH here would mean the fix did not take
  effect and IS the suspicious result, never the goal.
- Every other test in this file seeds data with NO duplicate edge
  versions, where the two planes are expected (and asserted) to MATCH.

``test_dual_run_edges_edge_type_filtered_splice_matches`` is the other
named risk in CHAOS-4504's brief: ``resolve_work_graph_edges`` runs a
SECOND query (``_query_dependency_edges``, ordered ``last_synced DESC,
edge_id ASC`` -- a different key than the primary query's ``confidence
DESC, edge_id ASC``) and concatenates its rows onto the primary result
WITHOUT re-sorting, activating only behind a narrowing edge_type filter.
That test seeds primary and dependency-derived edges whose relative
order would DIFFER under a (wrong) global re-sort by confidence versus
the (correct) "concatenate, do not re-sort" behavior, so a regression
that re-sorts the merged slice fails this test's plain order-sensitive
comparator call, not just a hand-written order assertion.

NaN-class (CHAOS-4534) applicability, checked by MECHANISM per the
orchestrator's 08:25 PT 08-29 warning, not by absence of obvious risk:
every aggregate this port's Go queries use is ``uniqExact``, ``argMax``,
``any``, or a bare projection -- ``grep -n "avg(" cmd/query-api/internal/
workgraph/*.go`` returns nothing, confirmed before writing this file, not
after. All three queries are GROUP-BY-shaped (edges: GROUP BY the edge
identity; flow: GROUP BY source_type, target_type; artifacts: GROUP BY
node_type, node_id), which is the mechanism the warning names as
structurally immune to the empty-window scalar-average case: a GROUP BY
over zero matching rows returns ZERO ROWS, not one row with a NaN
aggregate value. ``confidence`` (the one non-Nullable float field this
port returns, on ``WorkGraphEdgeResult``) is populated via
``argMax(confidence, last_synced)`` INSIDE that GROUP BY, never a bare
scalar ``avg()`` over a window that can be empty. This port has no
instance of the CHAOS-4534 class; no divergence test for it is included
because there is nothing to seed.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

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
from dev_health_ops.api.graphql.models.inputs import (
    WorkGraphEdgeFilterInput,
    WorkGraphEdgeTypeInput,
)
from dev_health_ops.api.graphql.resolvers.work_graph import (
    resolve_work_graph_artifacts,
    resolve_work_graph_edges,
    resolve_work_graph_flow,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.schemas import WorkGraphEdgeRecord
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.git import Base as GitBase
from dev_health_ops.models.go_api_registry import CandidateBuild, ProofRun, RoutingState
from dev_health_ops.models.work_items import WorkItemDependency

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
# registeredWorkGraphEdgesDocument/Flow/Artifacts, themselves byte-identical
# to the REAL production queries (web/src/lib/graphql/queries.ts:427,462,477).
# Verified by diff against the client file when this test was written, not
# reconstructed from the SDL (CHAOS-4367's codex-round-3 lesson).
WORK_GRAPH_EDGES_DOCUMENT = """query WorkGraphEdges($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphEdges(orgId: $orgId, filters: $filters) {
    edges {
      edgeId
      sourceType
      sourceId
      sourceDisplayName
      targetType
      targetId
      targetDisplayName
      edgeType
      provenance
      confidence
      evidence
      repoId
      provider
      theme
      subcategory
    }
    totalCount
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
    degradedReason
  }
}"""

WORK_GRAPH_FLOW_DOCUMENT = """query WorkGraphFlow($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphFlow(orgId: $orgId, filters: $filters) {
    rows {
      nodeType
      inflow
      outflow
    }
    degradedReason
  }
}"""

WORK_GRAPH_ARTIFACTS_DOCUMENT = """query WorkGraphArtifacts($orgId: String!, $filters: WorkGraphEdgeFilterInput) {
  workGraphArtifacts(orgId: $orgId, filters: $filters) {
    rows {
      nodeType
      nodeId
      displayName
      degree
      evidence
    }
    degradedReason
  }
}"""

SCHEMA_DIGEST = "sha256:wave4-lane-a-workgraph-dual-run-schema-digest"
CANDIDATE_BUILD = "wave4-lane-a-workgraph-dual-run-build"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def query_api_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Builds the real query-api binary once per test module -- same
    "real artifact, not `go run`" discipline as every prior dual-run
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
    db_name = f"chaos_4504_dual_run_{uuid.uuid4().hex}"
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
    """Same fixture as the prior dual-run tests' -- a scratch Postgres DB
    holding the REAL go_api_registry ORM tables, unrelated to which
    operation(s) this test drives.
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
    async_dsn: str, document_digest: str, selected_operation: str
) -> None:
    """Generalizes review_edges's helper of the same name to take
    ``selected_operation`` as a parameter -- this file drives three
    operations, each needing its own CANDIDATE_BUILD/ROUTING_STATE row
    pair for the SAME schema_digest.
    """
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await register_candidate_build(
                session,
                schema_digest=SCHEMA_DIGEST,
                document_digest=document_digest,
                selected_operation=selected_operation,
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation=selected_operation,
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
    selected_operation: str,
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
                selected_operation=selected_operation,
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


def _post_graphql(base_url: str, token: str, document: str, variables: dict) -> dict:
    body = json.dumps({"query": document, "variables": variables}).encode()
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


@pytest.fixture(scope="module")
def jwks_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("jwks")


# --- response snapshot builders --------------------------------------------


def _edges_go_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _edges_python_snapshot(result: Any) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data={
            "workGraphEdges": {
                "edges": [
                    {
                        "edgeId": e.edge_id,
                        "sourceType": e.source_type.value.upper(),
                        "sourceId": e.source_id,
                        "sourceDisplayName": e.source_display_name,
                        "targetType": e.target_type.value.upper(),
                        "targetId": e.target_id,
                        "targetDisplayName": e.target_display_name,
                        "edgeType": e.edge_type.value.upper(),
                        "provenance": e.provenance.value.upper(),
                        "confidence": e.confidence,
                        "evidence": e.evidence,
                        "repoId": e.repo_id,
                        "provider": e.provider,
                        "theme": e.theme,
                        "subcategory": e.subcategory,
                    }
                    for e in result.edges
                ],
                "totalCount": result.total_count,
                "pageInfo": {
                    "hasNextPage": result.page_info.has_next_page,
                    "hasPreviousPage": result.page_info.has_previous_page,
                    "startCursor": result.page_info.start_cursor,
                    "endCursor": result.page_info.end_cursor,
                },
                "degradedReason": result.degraded_reason,
            }
        },
        data_present=True,
        errors=(),
    )


def _flow_go_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _flow_python_snapshot(result: Any) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data={
            "workGraphFlow": {
                "rows": [
                    {
                        "nodeType": r.node_type.value.upper(),
                        "inflow": r.inflow,
                        "outflow": r.outflow,
                    }
                    for r in result.rows
                ],
                "degradedReason": result.degraded_reason,
            }
        },
        data_present=True,
        errors=(),
    )


def _artifacts_go_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _artifacts_python_snapshot(result: Any) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data={
            "workGraphArtifacts": {
                "rows": [
                    {
                        "nodeType": r.node_type.value.upper(),
                        "nodeId": r.node_id,
                        "displayName": r.display_name,
                        "degree": r.degree,
                        "evidence": r.evidence,
                    }
                    for r in result.rows
                ],
                "degradedReason": result.degraded_reason,
            }
        },
        data_present=True,
        errors=(),
    )


# --- seeding helpers ---------------------------------------------------


def _edge_record(
    *,
    edge_id: str,
    source_type: str,
    source_id: str,
    target_type: str,
    target_id: str,
    edge_type: str,
    confidence: float,
    evidence: str,
    org_id: str,
    last_synced: datetime,
    provenance: str = "native",
) -> WorkGraphEdgeRecord:
    return WorkGraphEdgeRecord(
        edge_id=edge_id,
        source_type=source_type,
        source_id=source_id,
        target_type=target_type,
        target_id=target_id,
        edge_type=edge_type,
        repo_id=None,
        provider=None,
        provenance=provenance,
        confidence=confidence,
        evidence=evidence,
        discovered_at=last_synced,
        last_synced=last_synced,
        event_ts=last_synced,
        day=last_synced.date(),
        org_id=org_id,
    )


@pytest.mark.asyncio
async def test_dual_run_flow_artifacts_and_clean_edges_match(
    query_api_binary, registry_postgres, jwks_path
):
    """All three operations, seeded with CLEAN data (no duplicate edge
    versions) -- the expected result on every operation here is MATCH,
    including workGraphEdges: the CHAOS-4515 dedup fix only changes
    behavior when a duplicate version actually exists, so this test also
    proves the fix does NOT introduce a spurious divergence on ordinary
    data.

    Seeds a small graph touching four node types (issue/pr/commit/file)
    so workGraphFlow's inflow/outflow split and workGraphArtifacts'
    degree ranking both have real, non-degenerate structure to compare,
    not just a single trivial edge.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4504-dual-run-{uuid.uuid4()}"
    ts = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)

    # issue-1 --references--> pr-1 --contains--> commit-1 --touches--> file-1
    # issue-2 --references--> pr-1  (pr-1 has in-degree 2: two issues touch it)
    records = [
        _edge_record(
            edge_id="clean-edge-1",
            source_type="issue",
            source_id="ISSUE-1",
            target_type="pr",
            target_id="PR-1",
            edge_type="references",
            confidence=0.8,
            evidence="e1",
            org_id=org_id,
            last_synced=ts,
        ),
        _edge_record(
            edge_id="clean-edge-2",
            source_type="issue",
            source_id="ISSUE-2",
            target_type="pr",
            target_id="PR-1",
            edge_type="references",
            confidence=0.7,
            evidence="e2",
            org_id=org_id,
            last_synced=ts,
        ),
        _edge_record(
            edge_id="clean-edge-3",
            source_type="pr",
            source_id="PR-1",
            target_type="commit",
            target_id="COMMIT-1",
            edge_type="contains",
            confidence=0.9,
            evidence="e3",
            org_id=org_id,
            last_synced=ts,
        ),
        _edge_record(
            edge_id="clean-edge-4",
            source_type="commit",
            source_id="COMMIT-1",
            target_type="file",
            target_id="FILE-1",
            edge_type="touches",
            confidence=0.6,
            evidence="e4",
            org_id=org_id,
            last_synced=ts,
        ),
    ]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-clean.json"
    jwks_file.write_text(json.dumps(jwks))

    edges_digest = _document_digest(WORK_GRAPH_EDGES_DOCUMENT)
    flow_digest = _document_digest(WORK_GRAPH_FLOW_DOCUMENT)
    artifacts_digest = _document_digest(WORK_GRAPH_ARTIFACTS_DOCUMENT)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], edges_digest, "workGraphEdges"
    )
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], flow_digest, "workGraphFlow"
    )
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], artifacts_digest, "workGraphArtifacts"
    )

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        sink.write_work_graph_edges(records)

        ctx = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)

        python_edges = await resolve_work_graph_edges(ctx, None)
        python_flow = await resolve_work_graph_flow(ctx, None)
        python_artifacts = await resolve_work_graph_artifacts(ctx, None)

        go_edges = _post_graphql(
            server.base_url,
            token,
            WORK_GRAPH_EDGES_DOCUMENT,
            {"orgId": org_id, "filters": None},
        )
        go_flow = _post_graphql(
            server.base_url,
            token,
            WORK_GRAPH_FLOW_DOCUMENT,
            {"orgId": org_id, "filters": None},
        )
        go_artifacts = _post_graphql(
            server.base_url,
            token,
            WORK_GRAPH_ARTIFACTS_DOCUMENT,
            {"orgId": org_id, "filters": None},
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert len(python_edges.edges) == 4, (
        "all four clean edges must reach the Python resolver"
    )
    assert "errors" not in go_edges, f"Go workGraphEdges carried errors: {go_edges}"
    assert "errors" not in go_flow, f"Go workGraphFlow carried errors: {go_flow}"
    assert "errors" not in go_artifacts, (
        f"Go workGraphArtifacts carried errors: {go_artifacts}"
    )

    edges_comparison = go_api_comparator.compare_responses(
        _edges_python_snapshot(python_edges), _edges_go_snapshot(go_edges)
    )
    flow_comparison = go_api_comparator.compare_responses(
        _flow_python_snapshot(python_flow), _flow_go_snapshot(go_flow)
    )
    artifacts_comparison = go_api_comparator.compare_responses(
        _artifacts_python_snapshot(python_artifacts),
        _artifacts_go_snapshot(go_artifacts),
    )

    for name, comparison, baseline, candidate in (
        ("workGraphEdges", edges_comparison, python_edges, go_edges),
        ("workGraphFlow", flow_comparison, python_flow, go_flow),
        ("workGraphArtifacts", artifacts_comparison, python_artifacts, go_artifacts),
    ):
        assert comparison.is_match, (
            f"{name} dual-run MISMATCH on CLEAN (no-duplicate-version) data -- "
            f"expected MATCH here (the CHAOS-4515 fix must not diverge on "
            f"ordinary data): terminal_state={comparison.terminal_state} "
            f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
        )

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=edges_digest,
        selected_operation="workGraphEdges",
        terminal_state=edges_comparison.terminal_state,
        org_id=org_id,
    )
    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=flow_digest,
        selected_operation="workGraphFlow",
        terminal_state=flow_comparison.terminal_state,
        org_id=org_id,
    )
    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=artifacts_digest,
        selected_operation="workGraphArtifacts",
        terminal_state=artifacts_comparison.terminal_state,
        org_id=org_id,
    )
    assert await _proof_run_count(registry_postgres["async"]) >= 3


@pytest.mark.asyncio
async def test_dual_run_edges_dedup_is_expected_divergence(
    query_api_binary, registry_postgres, jwks_path
):
    """CHAOS-4515: workGraphEdges is EXPECTED to diverge here, not match.

    Seeds two un-merged physical versions of ONE logical edge (same
    identity: source_type/source_id/edge_type/target_type/target_id, same
    confidence -- tied on Python's ENTIRE ORDER BY, so no tie-break on the
    row's own keys could ever separate them even if Python were fixed to
    add one) via TWO SEPARATE ``write_work_graph_edges`` calls (never one
    insert + ``OPTIMIZE ... FINAL`` -- that collapses to a single part and
    proves nothing), with ``SYSTEM STOP MERGES`` on the scratch table
    (table-qualified) so a background merge cannot collapse them during
    the test.

    Python's raw read (``work_graph.py:1183``, no FINAL/argMax) returns
    BOTH physical rows -- this is the bug CHAOS-4515 names. This port's Go
    query (``fetchDedupedEdgeRows``, argMax(..., last_synced) collapse
    before ORDER BY/LIMIT) returns exactly ONE, the newer version. The
    comparator therefore returns ``mismatch`` and this test asserts that
    explicitly -- a ``match`` here would mean the fix silently regressed.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    from urllib.parse import urlsplit

    scratch_db = urlsplit(CLICKHOUSE_URI).path.lstrip("/")
    assert scratch_db, "CLICKHOUSE_URI must name an explicit scratch database"

    org_id = f"chaos-4504-dedup-{uuid.uuid4()}"
    older = datetime(2026, 8, 15, 0, 0, 0, tzinfo=timezone.utc)
    newer = datetime(2026, 8, 20, 0, 0, 0, tzinfo=timezone.utc)
    edge_id = f"dedup-edge-{uuid.uuid4().hex[:8]}"

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-dedup.json"
    jwks_file.write_text(json.dumps(jwks))

    edges_digest = _document_digest(WORK_GRAPH_EDGES_DOCUMENT)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], edges_digest, "workGraphEdges"
    )

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    merges_stopped = False
    try:
        # Table-qualified, never bare (host rule) -- this scratch database
        # only, never `default`.
        sink.client.command(f"SYSTEM STOP MERGES `{scratch_db}`.work_graph_edges")
        merges_stopped = True

        sink.write_work_graph_edges(
            [
                _edge_record(
                    edge_id=edge_id,
                    source_type="issue",
                    source_id="ISSUE-DEDUP",
                    target_type="pr",
                    target_id="PR-DEDUP",
                    edge_type="references",
                    confidence=0.5,
                    evidence="OLD_EVIDENCE_v1",
                    provenance="heuristic",
                    org_id=org_id,
                    last_synced=older,
                )
            ]
        )
        sink.write_work_graph_edges(
            [
                _edge_record(
                    edge_id=edge_id,
                    source_type="issue",
                    source_id="ISSUE-DEDUP",
                    target_type="pr",
                    target_id="PR-DEDUP",
                    edge_type="references",
                    confidence=0.5,
                    evidence="NEW_EVIDENCE_v2",
                    provenance="native",
                    org_id=org_id,
                    last_synced=newer,
                )
            ]
        )

        parts = sink.client.query(
            "SELECT count() FROM system.parts WHERE database = {db:String} "
            "AND table = 'work_graph_edges' AND active = 1",
            parameters={"db": scratch_db},
        ).result_rows
        assert parts[0][0] >= 2, (
            f"seed must land as >=2 unmerged active parts, got {parts[0][0]} -- "
            "a single part means the two inserts were merged (or coalesced) "
            "before the read, which would make this test vacuous"
        )

        ctx = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
        python_edges = await resolve_work_graph_edges(ctx, None)
        go_edges = _post_graphql(
            server.base_url,
            token,
            WORK_GRAPH_EDGES_DOCUMENT,
            {"orgId": org_id, "filters": None},
        )
    finally:
        if merges_stopped:
            sink.client.command(f"SYSTEM START MERGES `{scratch_db}`.work_graph_edges")
        server.stop()
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert "errors" not in go_edges, f"Go workGraphEdges carried errors: {go_edges}"

    # RED (Python, undeduped): both physical versions surface.
    matching = [e for e in python_edges.edges if e.edge_id == edge_id]
    assert len(matching) == 2, (
        f"expected Python's raw read to return BOTH un-merged versions of "
        f"{edge_id!r} (the CHAOS-4515 bug), got {len(matching)}: {matching}"
    )
    evidences = {e.evidence for e in matching}
    assert evidences == {"OLD_EVIDENCE_v1", "NEW_EVIDENCE_v2"}, (
        f"expected both seeded versions' evidence present in Python's "
        f"duplicated rows, got {evidences}"
    )

    # GREEN (Go, deduped): exactly one row, the NEWER version.
    go_matching = [
        e for e in go_edges["data"]["workGraphEdges"]["edges"] if e["edgeId"] == edge_id
    ]
    assert len(go_matching) == 1, (
        f"expected Go's argMax-deduped query to return exactly ONE row for "
        f"{edge_id!r}, got {len(go_matching)}: {go_matching}"
    )
    assert go_matching[0]["evidence"] == "NEW_EVIDENCE_v2", (
        f"expected Go's deduped row to carry the NEWER version's evidence, "
        f"got {go_matching[0]['evidence']!r}"
    )
    assert go_matching[0]["provenance"] == "NATIVE"

    comparison = go_api_comparator.compare_responses(
        _edges_python_snapshot(python_edges), _edges_go_snapshot(go_edges)
    )
    assert comparison.terminal_state == go_api_comparator.TERMINAL_STATE_MISMATCH, (
        f"workGraphEdges MUST diverge here (CHAOS-4515 fix only exists in Go) "
        f"-- a MATCH would mean the fix silently regressed. "
        f"Got terminal_state={comparison.terminal_state} findings={comparison.findings}"
    )

    # record_proof_run's terminal_state vocabulary (go_api_registry.py's
    # TERMINAL_STATES) has no "expected divergence" state distinct from a
    # real mismatch -- the CHAOS-4504 brief's declaration is carried in
    # THIS TEST's name/docstring/assertions (the durable, reviewable
    # record of "why"), not encoded into the ledger row itself. Recording
    # "mismatch" here is accurate: the comparator DID return mismatch:
    # this proof run's context (this test) is what marks it expected.
    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=edges_digest,
        selected_operation="workGraphEdges",
        terminal_state=go_api_comparator.TERMINAL_STATE_MISMATCH,
        org_id=org_id,
    )


@pytest.mark.asyncio
async def test_dual_run_edges_tied_confidence_at_limit_boundary_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """CHAOS-4513: workGraphEdges' `ORDER BY confidence DESC, edge_id ASC`
    (CHAOS-2442/CHAOS-4493) has a total-order tie-break -- `edge_id` is the
    table's own ReplacingMergeTree dedup key, unique per logical edge. But
    neither the clean-data test above (4 edges, well under any LIMIT) nor
    the dedup-divergence test (a single logical edge) ever seeds enough
    DISTINCT edges tied on `confidence` to exceed `limit` and actually
    exercise the truncation boundary -- so the tie-break was never proven
    to matter. This test is that proof: `tied_count` distinct edges (each
    its own unique `edge_id` -- deliberately NOT the CHAOS-4515
    version-conflict shape, which is out of scope here and already has its
    own dedicated test above), all sharing `confidence=0.5`, seeded via
    SEPARATE `write_work_graph_edges` calls (CHAOS-4513's shared
    `_tie_boundary_seeding` standard).
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4513-edges-tie-boundary-{uuid.uuid4()}"
    ts = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)
    limit = 10
    tied_count = tied_row_count_for_limit(limit)

    records = [
        _edge_record(
            edge_id=f"tie-edge-{i:03d}",
            source_type="issue",
            source_id=f"ISSUE-{i:03d}",
            target_type="pr",
            target_id=f"PR-{i:03d}",
            edge_type="references",
            confidence=0.5,
            evidence=f"e{i:03d}",
            org_id=org_id,
            last_synced=ts,
        )
        for i in range(tied_count)
    ]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-edges-tie-boundary.json"
    jwks_file.write_text(json.dumps(jwks))

    edges_digest = _document_digest(WORK_GRAPH_EDGES_DOCUMENT)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], edges_digest, "workGraphEdges"
    )

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        # One write_work_graph_edges call PER record -- `tied_count`
        # separate INSERTs, never one batched call (CHAOS-4513: a single
        # INSERT typically collapses to one part, which reads back stably
        # with or without a tie-break and proves nothing).
        for record in records:
            sink.write_work_graph_edges([record])

        ctx = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
        python_edges = await resolve_work_graph_edges(
            ctx, WorkGraphEdgeFilterInput(limit=limit)
        )
        go_edges = _post_graphql(
            server.base_url,
            token,
            WORK_GRAPH_EDGES_DOCUMENT,
            {"orgId": org_id, "filters": {"limit": limit}},
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert "errors" not in go_edges, f"Go workGraphEdges carried errors: {go_edges}"
    assert len(python_edges.edges) == limit, (
        f"expected exactly {limit} of {tied_count} tied rows to survive the "
        f"LIMIT boundary, got {len(python_edges.edges)}"
    )

    # Every edge shares confidence=0.5, so the deterministic tie-break
    # (edge_id ASC) alone decides the survivors: the lexicographically
    # smallest `limit` edge_ids. Assert Python matches THAT expected set,
    # independent of Go, before ever comparing the two planes to each
    # other -- two planes agreeing on the WRONG set is not evidence.
    expected_edge_ids = sorted(r.edge_id for r in records)[:limit]
    actual_edge_ids = sorted(e.edge_id for e in python_edges.edges)
    assert actual_edge_ids == expected_edge_ids, (
        f"Python's surviving tied-row set was not the deterministic "
        f"lexicographically-smallest {limit} of {tied_count} -- "
        f"CHAOS-2442/4493 regression: got {actual_edge_ids}, "
        f"expected {expected_edge_ids}"
    )

    comparison = go_api_comparator.compare_responses(
        _edges_python_snapshot(python_edges), _edges_go_snapshot(go_edges)
    )
    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=edges_digest,
        selected_operation="workGraphEdges",
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )
    assert comparison.is_match, (
        f"workGraphEdges tie-boundary dual-run MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={_edges_python_snapshot(python_edges)}\ngo={_edges_go_snapshot(go_edges)}"
    )


@pytest.mark.asyncio
async def test_dual_run_edges_edge_type_filtered_splice_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """THE SPLICE TRAP (BRIEF.md): a proof exercising only the unfiltered
    path never activates ``_query_dependency_edges`` at all
    (``dependencyEdgeFilterValues`` returns ``[]`` without a narrowing
    edge_type/edge_types filter). This test exercises the FILTERED path
    deliberately, with data engineered so a "sort the merged slice by
    confidence" bug and the correct "concatenate, never re-sort" behavior
    produce VISIBLY DIFFERENT orders -- not just a theoretical risk.

    Seeded:
    - TWO primary ``work_graph_edges`` rows, edge_type='blocks',
      confidence 0.9 and 0.1 (source A, B).
    - TWO ``work_item_dependencies`` rows, relationship_type='blocks',
      touching entirely DIFFERENT work items (so neither is excluded by
      the "already present" identity check) -- the dependency query
      hard-codes ``1.0 AS confidence`` for every row it produces
      (work_graph.py:1076), HIGHER than both primary rows' confidence.

    Correct (concatenate, no re-sort): [A(0.9), B(0.1), C, D] -- C/D in
    their OWN order (last_synced DESC), appended at the END regardless of
    their higher confidence value.
    A "sort the merged slice by confidence" regression would instead
    place C, D (confidence 1.0) FIRST: [C, D, A(0.9), B(0.1)]. Since the
    comparator's list comparison is order-sensitive by default
    (``tie_ordering="strict"``, not passed here), a naive re-sort fails
    this test's plain ``compare_responses`` call outright -- no
    hand-written order assertion is needed to catch it, though one is
    included anyway for a readable failure message.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4504-splice-{uuid.uuid4()}"
    ts = datetime(2026, 8, 22, 0, 0, 0, tzinfo=timezone.utc)
    dep_newer = datetime(2026, 8, 21, 0, 0, 0, tzinfo=timezone.utc)
    dep_older = datetime(2026, 8, 18, 0, 0, 0, tzinfo=timezone.utc)

    edge_records = [
        _edge_record(
            edge_id="splice-primary-a",
            source_type="issue",
            source_id="ISSUE-100",
            target_type="issue",
            target_id="ISSUE-101",
            edge_type="blocks",
            confidence=0.9,
            evidence="primary-a",
            org_id=org_id,
            last_synced=ts,
        ),
        _edge_record(
            edge_id="splice-primary-b",
            source_type="issue",
            source_id="ISSUE-102",
            target_type="issue",
            target_id="ISSUE-103",
            edge_type="blocks",
            confidence=0.1,
            evidence="primary-b",
            org_id=org_id,
            last_synced=ts,
        ),
    ]
    dependency_records = [
        WorkItemDependency(
            source_work_item_id="ISSUE-200",
            target_work_item_id="ISSUE-201",
            relationship_type="blocks",
            relationship_type_raw="",
            last_synced=dep_newer,
            org_id=org_id,
        ),
        WorkItemDependency(
            source_work_item_id="ISSUE-202",
            target_work_item_id="ISSUE-203",
            relationship_type="blocks",
            relationship_type_raw="",
            last_synced=dep_older,
            org_id=org_id,
        ),
    ]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-splice.json"
    jwks_file.write_text(json.dumps(jwks))

    edges_digest = _document_digest(WORK_GRAPH_EDGES_DOCUMENT)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], edges_digest, "workGraphEdges"
    )

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres["go"],
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        sink.write_work_graph_edges(edge_records)
        sink.write_work_item_dependencies(dependency_records)

        ctx = GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client)
        filters = WorkGraphEdgeFilterInput(
            edge_type=WorkGraphEdgeTypeInput.BLOCKS, limit=10
        )
        python_edges = await resolve_work_graph_edges(ctx, filters)

        go_edges = _post_graphql(
            server.base_url,
            token,
            WORK_GRAPH_EDGES_DOCUMENT,
            {"orgId": org_id, "filters": {"edgeType": "BLOCKS", "limit": 10}},
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE work_graph_edges DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.client.command(
            "ALTER TABLE work_item_dependencies DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert "errors" not in go_edges, f"Go workGraphEdges carried errors: {go_edges}"

    # Python must see all four: 2 primary (edge_type filter matches both
    # directly) + 2 spliced-in dependency edges (neither identity already
    # present among the primary rows).
    assert len(python_edges.edges) == 4, (
        f"expected 2 primary + 2 spliced dependency edges, got "
        f"{len(python_edges.edges)}: {[e.edge_id for e in python_edges.edges]}"
    )
    python_source_ids = [e.source_id for e in python_edges.edges]
    assert python_source_ids == ["ISSUE-100", "ISSUE-102", "ISSUE-200", "ISSUE-202"], (
        f"expected primary rows FIRST (confidence DESC: A then B), THEN "
        f"dependency rows in THEIR OWN order (last_synced DESC: newer-first, "
        f"i.e. ISSUE-200 before ISSUE-202) -- concatenated, never re-sorted "
        f"as a whole. Got {python_source_ids}"
    )

    go_source_ids = [e["sourceId"] for e in go_edges["data"]["workGraphEdges"]["edges"]]
    assert go_source_ids == python_source_ids, (
        f"Go's splice order diverged from Python's -- this is the splice "
        f"trap: a Go port that sorts the merged slice by confidence would "
        f"place the dependency rows (confidence=1.0) FIRST. "
        f"python={python_source_ids} go={go_source_ids}"
    )

    comparison = go_api_comparator.compare_responses(
        _edges_python_snapshot(python_edges), _edges_go_snapshot(go_edges)
    )
    assert comparison.is_match, (
        f"workGraphEdges edge_type-filtered (splice path) dual-run MISMATCH "
        f"-- expected MATCH (no duplicate edge versions seeded here, so the "
        f"CHAOS-4515 dedup fix should not cause a divergence; only the "
        f"splice ordering is under test): terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={python_edges}\ngo={go_edges}"
    )

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=edges_digest,
        selected_operation="workGraphEdges",
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )
