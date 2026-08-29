"""CHAOS-4506 Wave 4 stage-2 proof: local dual-run of analytics.flowMatrix.

STATUS: WRITTEN, NOT YET EXECUTED. Needs a container slot (live
ClickHouse + Postgres, real Go binary build) -- structure follows
`test_go_api_dual_run_hotspots.py` / `test_go_api_dual_run_review_edges.py`
closely (same real-Postgres registry-table fixture, same real-envelope
minting, same real Go binary + HTTP server harness), adapted for
flowMatrix's specific shape and this ticket's specific proof obligation.

Plan §5 stage 2: real Python and Go servers against the same
producer-seeded scratch ClickHouse/Postgres state, compared via the
CHAOS-4381 comparator (`go_api_comparator.compare_responses`).

Scope: ONLY `flowMatrix` (dimension in {TEAM, REPO, WORK_TYPE}) -- the
one analytics sub-operation this PR REGISTERS
(`registeredFlowMatrixDocument`, query_route.go). The `analytics`
resolver itself IS wired as of 9fe92e16d, and `.timeseries`/
`.breakdowns`/`.sankey` are landed behind it, but the only two documents
that select them (`INVESTMENT_BREAKDOWN_QUERY`, `INVESTMENT_FULL_QUERY`)
both send `useInvestment: true` on every live call and are therefore
blocked on CHAOS-4538 -- so there is no registerable document to
dual-run them against yet. Follow-up, not a silent skip.

TWO STANDING CAVEATS FOR THE FIRST EXECUTION:

* Column lists in the seeding helpers were derived by READING migration
  DDL (`001_metrics_v2.sql`, `009_raw_work_items.sql`, `024_add_org_id.sql`,
  `027_add_org_id_to_sorting_keys.py`, `051_team_attribution_dimensions.sql`)
  and have never been executed against a live ClickHouse. Column order
  and type errors are EXPECTED on the first run and are not evidence of
  a design problem -- fix the column list, do not go looking for one.
* The test 2 seed is deliberately counter-intuitive and is load-bearing.
  Read its docstring before "simplifying" it: the obvious
  duplicate-only-in-`computed_at` seed is INERT against `uniqExact`, and
  moving a day across a month boundary defeats dedup via
  `PARTITION BY toYYYYMM(day)`. Both mistakes fail in the same
  hard-to-read way -- an unexpected `match` that looks like the FINAL fix
  not working.

THREE things this file proves, each load-bearing for a different reason:

1. `test_dual_run_team_dimension_matches`: the ordinary MATCH case, on
   the dimension whose reads were ALREADY correct on the feature tip
   (team_nodes/team_edges both carry `wct FINAL` pre-existing, not part
   of this port's CHAOS-4516 fix). Establishes the harness works at all
   before using it to prove a divergence.

2. `test_dual_run_repo_dimension_diverges_on_unmerged_duplicate`: the
   ticket's central obligation. Seeds TWO coexisting versions of one
   `work_item_cycle_times` row (same identity, different `computed_at`)
   with merges stopped on the table, so ClickHouse cannot self-heal the
   duplicate before either resolver reads it. Python's raw
   (non-FINAL) read is expected to see both versions and produce a
   DIFFERENT node value than Go's FINAL'd read. Asserts `terminal_state
   == "mismatch"`, NOT `"match"` -- a match here would mean the Go FINAL
   fix did not take effect (BRIEF.md: "a dual-run that MATCHES on a query
   you knowingly fixed is the suspicious result"). This is the ONE test
   in this file that asserts a KNOWN, DECLARED divergence rather than
   parity -- do not "fix" it by reverting Go's FINAL if it ever goes
   green on `match` (see Lane A's #2007 precedent, same shape for
   `workGraphEdges`).

3. `test_dual_run_work_type_filtered_rejection_matches`: both planes
   reject a filtered same-dimension flowMatrix (CHAOS-2487) with the SAME
   shape -- an error-parity case, not a data-parity case. Applying this
   port's own layer-masking lesson to a dual-run assertion for the first
   time: the thing that could mask a real divergence here is comparing
   ONLY `"errors" in payload` (true on both sides even if the underlying
   error CODE differs) rather than comparing the actual error path/code
   through the real comparator, which treats error identity as
   `(path, extensions.code)` (go_api_comparator.py's rule 1) -- so this
   test runs both raw errors through `compare_responses`, not a bare
   boolean check, specifically so a code-level divergence cannot hide
   behind two superficially-similar "errors" keys.

Producer note (root AGENTS.md: "fixtures are producer-derived"): for
tests 1 and 3, `work_item_cycle_times`/`work_items`/
`work_item_team_attributions` rows are inserted via `client.insert`
against the exact column set the real sync pipeline writes (verified
against the CREATE TABLE definitions this file cites by migration
file:line), the same "no seam exists, write through the real column
contract" precedent `test_go_api_dual_run_hotspots.py` documents for
`file_hotspot_daily`. Test 2 is DIFFERENT and DELIBERATE: it needs two
coexisting un-merged versions under `SYSTEM STOP MERGES`, which the
normal write path cannot produce (a second producer write to the same
identity is what ReplacingMergeTree is FOR) -- two direct `client.insert`
calls while merges are stopped is the documented CHAOS-4516 technique,
not a shortcut around the producer-derived rule.
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
from dev_health_ops.api.graphql.errors import ValidationError
from dev_health_ops.api.graphql.go_api_registry import (
    record_proof_run,
    register_candidate_build,
)
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    DateRangeInput,
    DimensionInput,
    FilterInput,
    FlowMatrixRequestInput,
    MeasureInput,
    ScopeFilterInput,
    ScopeLevelInput,
)
from dev_health_ops.api.graphql.resolvers.analytics import resolve_analytics
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
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
# registeredFlowMatrixDocument, itself byte-identical to the REAL
# production query (web/src/lib/graphql/queries.ts:56-74's
# FLOW_MATRIX_QUERY, operation name "FlowMatrix").
FLOW_MATRIX_DOCUMENT = """query FlowMatrix($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    flowMatrix {
      nodes {
        id
        label
        dimension
        value
      }
      edges {
        source
        target
        value
      }
    }
  }
}"""

SCHEMA_DIGEST = "sha256:wave4-flowmatrix-dual-run-test-schema-digest"
CANDIDATE_BUILD = "wave4-flowmatrix-dual-run-test-build"


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
    db_name = f"chaos_4506_flowmatrix_dual_run_{uuid.uuid4().hex}"
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
                selected_operation="flowMatrix",
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation="flowMatrix",
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
    async_dsn: str, *, document_digest: str, terminal_state: str, org_id: str
) -> None:
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await record_proof_run(
                session,
                schema_digest=SCHEMA_DIGEST,
                document_digest=document_digest,
                selected_operation="flowMatrix",
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
        user_id="55555555-5555-4555-8555-555555555555",
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
    body = json.dumps({"query": FLOW_MATRIX_DOCUMENT, "variables": variables}).encode()
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
        [binary], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
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


# --- Response snapshot construction -----------------------------------


def _go_response_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _python_response_snapshot(result) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_analytics's AnalyticsResult.flow_matrix into the
    same on-the-wire GraphQL response envelope FLOW_MATRIX_DOCUMENT's
    selection set produces -- ONLY the flowMatrix sub-object, matching
    the document (it selects nothing else on `analytics`).
    """
    fm = result.flow_matrix
    return go_api_comparator.ResponseSnapshot(
        data={
            "analytics": {
                "flowMatrix": {
                    "nodes": [
                        {
                            "id": n.id,
                            "label": n.label,
                            "dimension": n.dimension,
                            "value": n.value,
                        }
                        for n in (fm.nodes if fm else [])
                    ],
                    "edges": [
                        {"source": e.source, "target": e.target, "value": e.value}
                        for e in (fm.edges if fm else [])
                    ],
                }
                if fm is not None
                else None
            }
        },
        data_present=True,
        errors=(),
    )


def _python_error_snapshot(exc: Exception) -> go_api_comparator.ResponseSnapshot:
    """Serializes a raised ValidationError into the ACTUAL GraphQL error
    envelope Strawberry's exception handler produces on the real Python
    HTTP path.

    CORRECTED 2026-08-29 (CHAOS-4506 slot execution) against the PR body's
    "Fifth finding -- dual-run fabricates the error envelope -- is NOT
    verified" flag: this shape was previously guessed, not observed, and
    the guess was wrong in three ways, found by executing the REAL
    production `dev_health_ops.api.graphql.schema.schema` object directly
    (`schema.execute(...)`, the exact code path
    `strawberry.fastapi.GraphQLRouter` in `api/graphql/app.py` calls with
    no `process_errors` override anywhere in this codebase -- grepped, and
    confirmed nothing intercepts the raised `ValidationError`) against this
    file's real FlowMatrix document, real WORK_TYPE-filtered variables:

    1. `path` is `["analytics"]`, NOT `["analytics", "flowMatrix"]`. The
       raise happens inside `compile_flow_matrix`
       (`sql/compiler.py:512,544`), which runs BEFORE any `flowMatrix`
       sub-field is ever selected -- GraphQL's error path only extends
       past the top-level field once resolution actually descends into
       it, and this rejection never gets that far.
    2. `extensions` is `{}`, NOT `{"code": "VALIDATION_ERROR"}`. Python's
       `ValidationError.code` (`api/graphql/errors.py:57`) is a REAL
       attribute on the exception object, but nothing in this codebase
       ever reads it to populate the wire response -- Strawberry's
       default unhandled-exception handling does not know about
       arbitrary `.code` attributes, and there is no `process_errors`
       hook (grepped repo-wide, zero hits) that would surface it. The
       attribute is genuine but wire-dead; `go_api_comparator`'s rule 1
       tolerates a missing `extensions` key (`(err.get("extensions") or
       {}).get("code", "")`), so this is not a comparator gap either.
    3. `data_present` is `True`, NOT `False`. The GraphQL document parsed
       and validated, then a FIELD resolver errored -- per the GraphQL
       spec and this comparator's own `data_present` doc comment, that is
       `"data": null` (key PRESENT, value null), distinct from a
       request-level failure that never reaches execution.

    None of this is a Go defect: Go's real observed response for the same
    scenario also carries no `extensions` key and a `path` of
    `["analytics"]` (its message is prefixed by this port's own error
    wrapping -- `analytics: analytics: flowMatrix: ...` from
    `schema.resolvers.go`'s `fmt.Errorf("analytics: %w", err)` composing
    with `analytics.Resolve`'s own wrap -- message TEXT is not what rule 1
    keys identity on). The mismatch this test previously reported was
    entirely this baseline-construction function disagreeing with the real
    Python wire shape, not the two planes disagreeing with each other.
    """
    return go_api_comparator.ResponseSnapshot(
        data=None,
        data_present=True,
        errors=(
            {
                "message": str(exc),
                "path": ["analytics"],
                "extensions": {},
            },
        ),
    )


# --- Seeding helpers -----------------------------------------------------


def _insert_work_item_team_attribution(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    repo_id: uuid.UUID,
    work_item_id: str,
    team_id: str,
    team_name: str,
    computed_at: datetime,
) -> None:
    """Columns per 051_team_attribution_dimensions.sql:78-91."""
    sink.client.insert(
        "work_item_team_attributions",
        [
            [
                org_id,
                repo_id,
                work_item_id,
                "github",
                team_id,
                team_name,
                "native_team",
                1,  # is_primary
                "high",
                "{}",  # evidence
                computed_at,
            ]
        ],
        column_names=[
            "org_id",
            "repo_id",
            "work_item_id",
            "provider",
            "team_id",
            "team_name",
            "source",
            "is_primary",
            "confidence",
            "evidence",
            "computed_at",
        ],
    )


def _insert_work_item(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    repo_id: uuid.UUID,
    work_item_id: str,
    work_item_type: str,
    last_synced: datetime,
) -> None:
    """Columns per 009_raw_work_items.sql:1-28 plus migration 024's
    org_id ALTER -- a minimal row through the real column contract, not a
    hand-picked subset (every NOT-nullable/no-default column is present).
    """
    sink.client.insert(
        "work_items",
        [
            [
                repo_id,
                work_item_id,
                "github",
                "seed title",
                "github",
                work_item_type,
                "done",
                "done",
                "PROJ",
                "proj-1",
                [],
                "reporter@example.com",
                last_synced,
                last_synced,
                None,
                last_synced,
                None,
                [],
                None,
                "",
                "",
                "",
                "",
                "",
                last_synced,
                org_id,
            ]
        ],
        column_names=[
            "repo_id",
            "work_item_id",
            "provider",
            "title",
            "description",
            "type",
            "status",
            "status_raw",
            "project_key",
            "project_id",
            "assignees",
            "reporter",
            "created_at",
            "updated_at",
            "started_at",
            "completed_at",
            "closed_at",
            "labels",
            "story_points",
            "sprint_id",
            "sprint_name",
            "parent_id",
            "epic_id",
            "url",
            "last_synced",
            "org_id",
        ],
    )


def _insert_work_item_cycle_times(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    work_item_id: str,
    day: date,
    work_scope_id: str,
    team_id: str | None,
    created_at: datetime,
    computed_at: datetime,
) -> None:
    """Columns per 001_metrics_v2.sql:137-155 plus migration 024's
    org_id ALTER."""
    sink.client.insert(
        "work_item_cycle_times",
        [
            [
                work_item_id,
                "github",
                day,
                work_scope_id,
                team_id,
                None,  # team_name
                None,  # assignee
                "feature",
                "done",
                created_at,
                None,  # started_at
                None,  # completed_at
                None,  # cycle_time_hours
                None,  # lead_time_hours
                computed_at,
                org_id,
            ]
        ],
        column_names=[
            "work_item_id",
            "provider",
            "day",
            "work_scope_id",
            "team_id",
            "team_name",
            "assignee",
            "type",
            "status",
            "created_at",
            "started_at",
            "completed_at",
            "cycle_time_hours",
            "lead_time_hours",
            "computed_at",
            "org_id",
        ],
    )


def _flow_matrix_batch(
    dimension: DimensionInput, start: date, end: date
) -> AnalyticsRequestInput:
    return AnalyticsRequestInput(
        flow_matrix=FlowMatrixRequestInput(
            dimension=dimension,
            measure=MeasureInput.COUNT,
            date_range=DateRangeInput(start_date=start, end_date=end),
            max_nodes=50,
            max_edges=200,
        )
    )


def _flow_matrix_variables(org_id: str, dimension: str, start: str, end: str) -> dict:
    return {
        "orgId": org_id,
        "batch": {
            "flowMatrix": {
                "dimension": dimension,
                "measure": "COUNT",
                "dateRange": {"startDate": start, "endDate": end},
                "maxNodes": 50,
                "maxEdges": 200,
            }
        },
    }


# --- Test 1: TEAM dimension, ordinary match -------------------------------


@pytest.mark.asyncio
async def test_dual_run_team_dimension_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """TEAM dimension's reads were ALREADY correct on the feature tip
    (team_nodes/team_edges both carry `wct FINAL` pre-existing) -- this
    establishes the harness itself works before using it to prove a
    divergence in test 2.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4506-flowmatrix-team-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day = date(2026, 8, 10)
    now = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-flowmatrix-team.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(FLOW_MATRIX_DOCUMENT)
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
        for i, (wi_id, team_id, team_name) in enumerate(
            [("wi-team-a-1", "team-a", "Alpha"), ("wi-team-b-1", "team-b", "Bravo")]
        ):
            _insert_work_item_team_attribution(
                sink,
                org_id=org_id,
                repo_id=repo_id,
                work_item_id=wi_id,
                team_id=team_id,
                team_name=team_name,
                computed_at=now,
            )
            _insert_work_item_cycle_times(
                sink,
                org_id=org_id,
                work_item_id=wi_id,
                day=day,
                work_scope_id="scope-1",
                team_id=team_id,
                created_at=now,
                computed_at=now,
            )

        start, end = "2026-08-01", "2026-08-31"
        python_result = await resolve_analytics(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink),
            _flow_matrix_batch(
                DimensionInput.TEAM, date(2026, 8, 1), date(2026, 8, 31)
            ),
        )
        go_payload = _post_graphql(
            server.base_url, token, _flow_matrix_variables(org_id, "TEAM", start, end)
        )
    finally:
        server.stop()
        for table in (
            "work_item_cycle_times",
            "work_items",
            "work_item_team_attributions",
        ):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} SETTINGS mutations_sync=2",
                parameters={"org_id": org_id},
            )
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"
    assert python_result.flow_matrix is not None
    assert len(python_result.flow_matrix.nodes) == 2, (
        "expected both seeded teams as nodes"
    )

    baseline = _python_response_snapshot(python_result)
    candidate = _go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"flowMatrix(TEAM) dual-run MISMATCH: terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )
    assert await _proof_run_count(registry_postgres["async"]) >= 1


# --- Test 2: REPO dimension, EXPECTED divergence on unmerged duplicate ----


def _repo_node_value(snapshot, repo_id) -> float | None:
    """Pulls the single REPO node's value out of a ResponseSnapshot, or
    None if the node is absent. Used to pin WHICH divergence occurred
    rather than merely that one did."""
    fm = ((snapshot.data or {}).get("analytics") or {}).get("flowMatrix")
    if not fm:
        return None
    for node in fm.get("nodes") or []:
        if node.get("id") == f"REPO:{repo_id}":
            return node.get("value")
    return None


@pytest.mark.asyncio
async def test_dual_run_repo_dimension_diverges_on_unmerged_duplicate(
    query_api_binary, registry_postgres, jwks_path
):
    """THE central proof this ticket exists to produce: Python's raw read
    of work_item_cycle_times sees an un-merged duplicate that Go's FINAL'd
    read collapses, so the two disagree.

    THE SEED IS NOT THE OBVIOUS ONE, and the obvious one does not work.
    Three constraints have to hold simultaneously, and each was verified
    against the DDL rather than assumed:

    1. `uniqExact` IS DUPLICATE-INSENSITIVE. Every REPO template
       aggregates `uniqExact(wct.work_item_id)` (flowmatrix.go:383, :406,
       :432) under measure COUNT. Seeding two versions of one row that
       differ ONLY in `computed_at` -- the intuitive "un-merged
       duplicate" -- changes nothing: the distinct set of work_item_id is
       {A} whether or not FINAL collapses them, so BOTH sides return the
       same value and the comparator correctly reports `match`. The
       earlier draft of this test seeded exactly that and would have
       failed while blaming the FINAL fix. The duplicate must therefore
       differ in a column the query FILTERS ON, so that dedup changes
       which rows survive the WHERE -- here `day`, via the date range.

    2. `PARTITION BY toYYYYMM(day)` (001_metrics_v2.sql:155).
       ReplacingMergeTree deduplicates only WITHIN a partition, so the
       two versions must stay in the SAME MONTH. Pushing the second
       version's `day` into September to get it out of the range would
       put it in partition 202609, FINAL would collapse nothing, and both
       sides would agree again -- the failure mode looks identical to
       constraint 1. Both days are August; the RANGE is narrowed instead.

    3. `ENGINE ReplacingMergeTree(computed_at)` with
       `ORDER BY (org_id, provider, work_item_id)`
       (001_metrics_v2.sql:153-156, 027_add_org_id_to_sorting_keys.py:68).
       `day` is NOT in the sorting key, so two rows differing in `day`
       are genuinely two VERSIONS of one row rather than two rows; and
       `computed_at` is the version column, so FINAL deterministically
       keeps the later one.

    Resulting divergence, with the query range 08-01..08-15:
      * work item A has v1 day=08-10 (IN range) and v2 day=08-25 (OUT of
        range, same partition, newer computed_at).
      * work item B is a single clean row, day=08-05 (IN range).
      * Python raw sees A-via-v1 AND B  -> uniqExact = 2
      * Go FINAL collapses A to v2, out of range -> only B -> uniqExact = 1

    WHY B EXISTS -- this is the layer question applied to the assertion.
    Without B, Go's expected result is an EMPTY node list. But
    `resolveFlowMatrix` SWALLOWS execute errors and degrades to exactly
    that empty result (resolve.go:299, mirroring analytics.py:959-961),
    and a swallowed failure carries no GraphQL `errors` key -- so
    `assert "errors" not in go_payload` cannot see it either. A broken Go
    read would have produced the expected empty output and passed this
    test for entirely the wrong reason. B makes Go's expected result
    NON-EMPTY (value 1), so a degraded run yields None and fails.

    A bare `terminal_state == mismatch` assertion would also pass on any
    difference at all -- a seeding error, a column mistake, a server that
    returned nothing. Both sides' values are asserted explicitly so the
    mismatch is pinned to the dedup mechanism.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4506-flowmatrix-repo-divergence-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    dup_item, stable_item = "wi-repo-dup-1", "wi-repo-stable-1"

    # Narrow range: 08-10 is inside, 08-25 is outside, both in partition 202608.
    start, end = "2026-08-01", "2026-08-15"
    start_d, end_d = date(2026, 8, 1), date(2026, 8, 15)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-flowmatrix-repo-divergence.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(FLOW_MATRIX_DOCUMENT)
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
        for wid in (dup_item, stable_item):
            _insert_work_item(
                sink,
                org_id=org_id,
                repo_id=repo_id,
                work_item_id=wid,
                work_item_type="bug",
                last_synced=datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc),
            )

        # Scratch database only -- never `default`. Unqualified because
        # ensure_schema(force=True) created it in this sink's database.
        sink.client.command("SYSTEM STOP MERGES work_item_cycle_times")
        try:
            # Control row: single version, inside the range. Keeps Go's
            # expected result non-empty (see docstring).
            _insert_work_item_cycle_times(
                sink,
                org_id=org_id,
                work_item_id=stable_item,
                day=date(2026, 8, 5),
                work_scope_id="scope-dup",
                team_id=None,
                created_at=datetime(2026, 8, 5, 9, 0, 0, tzinfo=timezone.utc),
                computed_at=datetime(2026, 8, 5, 9, 0, 0, tzinfo=timezone.utc),
            )
            # v1: older, day INSIDE the range.
            _insert_work_item_cycle_times(
                sink,
                org_id=org_id,
                work_item_id=dup_item,
                day=date(2026, 8, 10),
                work_scope_id="scope-dup",
                team_id=None,
                created_at=datetime(2026, 8, 10, 9, 0, 0, tzinfo=timezone.utc),
                computed_at=datetime(2026, 8, 10, 9, 0, 0, tzinfo=timezone.utc),
            )
            # v2: SAME identity, NEWER computed_at, day OUTSIDE the range
            # but inside the same monthly partition.
            _insert_work_item_cycle_times(
                sink,
                org_id=org_id,
                work_item_id=dup_item,
                day=date(2026, 8, 25),
                work_scope_id="scope-dup",
                team_id=None,
                created_at=datetime(2026, 8, 10, 9, 0, 0, tzinfo=timezone.utc),
                computed_at=datetime(2026, 8, 10, 15, 0, 0, tzinfo=timezone.utc),
            )

            python_result = await resolve_analytics(
                GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink),
                _flow_matrix_batch(DimensionInput.REPO, start_d, end_d),
            )
            go_payload = _post_graphql(
                server.base_url,
                token,
                _flow_matrix_variables(org_id, "REPO", start, end),
            )
        finally:
            sink.client.command("SYSTEM START MERGES work_item_cycle_times")
    finally:
        server.stop()
        for table in ("work_item_cycle_times", "work_items"):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} SETTINGS mutations_sync=2",
                parameters={"org_id": org_id},
            )
        sink.close()

    assert "errors" not in go_payload, (
        f"Go response carried unexpected errors: {go_payload}"
    )

    baseline = _python_response_snapshot(python_result)
    candidate = _go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    python_value = _repo_node_value(baseline, repo_id)
    go_value = _repo_node_value(candidate, repo_id)

    # Pin the mechanism, not just "something differed".
    assert python_value == 2, (
        f"Python's raw read should see BOTH the in-range duplicate version and the "
        f"control row (uniqExact=2), got {python_value}. If this is 1, the un-merged "
        f"duplicate was not reproduced -- check SYSTEM STOP MERGES and that both "
        f"versions share (org_id, provider, work_item_id) and the 202608 partition. "
        f"python={baseline}"
    )
    assert go_value == 1, (
        f"Go's FINAL'd read should collapse the duplicate to its newer out-of-range "
        f"version, leaving only the control row (uniqExact=1), got {go_value}. "
        f"None means Go returned NO node at all -- which is also what a SWALLOWED "
        f"ClickHouse failure produces (resolve.go:299 degrades to empty with no "
        f"GraphQL error), so treat None as a broken run, not as a stronger fix. "
        f"go={candidate}"
    )
    assert comparison.terminal_state == go_api_comparator.TERMINAL_STATE_MISMATCH, (
        "EXPECTED a mismatch (CHAOS-4516 fix declared, not a bug): a MATCH here "
        "means Go's FINAL fix on flow_matrix_repo_nodes_template did not take "
        f"effect. terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )


# --- Test 3: filtered same-dimension rejection, error-shape match ---------


@pytest.mark.asyncio
async def test_dual_run_work_type_filtered_rejection_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """CHAOS-2487: a filtered same-dimension flowMatrix must be rejected
    on BOTH planes with the SAME error identity (path + extensions.code),
    never silently answered with unfiltered data on either side.
    Compares the raw errors through go_api_comparator itself (not a bare
    `"errors" in payload` check on each side independently), applying
    this port's own layer-masking lesson: two superficially-similar
    "there were errors" booleans could still hide a real code-level
    divergence a boolean check would never see.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4506-flowmatrix-worktype-filtered-{uuid.uuid4()}"

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-flowmatrix-worktype-filtered.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(FLOW_MATRIX_DOCUMENT)
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
        start, end = "2026-08-01", "2026-08-31"

        python_error: Exception | None = None
        try:
            batch = AnalyticsRequestInput(
                flow_matrix=FlowMatrixRequestInput(
                    dimension=DimensionInput.WORK_TYPE,
                    measure=MeasureInput.COUNT,
                    date_range=DateRangeInput(
                        start_date=date(2026, 8, 1), end_date=date(2026, 8, 31)
                    ),
                    max_nodes=50,
                    max_edges=200,
                ),
                filters=FilterInput(
                    scope=ScopeFilterInput(level=ScopeLevelInput.TEAM, ids=["team-1"])
                ),
            )
            await resolve_analytics(
                GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink),
                batch,
            )
        except ValidationError as exc:
            python_error = exc

        variables = _flow_matrix_variables(org_id, "WORK_TYPE", start, end)
        variables["batch"]["filters"] = {"scope": {"level": "TEAM", "ids": ["team-1"]}}
        go_payload = _post_graphql(server.base_url, token, variables)
    finally:
        server.stop()
        sink.close()

    assert python_error is not None, (
        "resolve_analytics did not raise for a filtered same-dimension "
        "flowMatrix -- CHAOS-2487's rejection must be fatal on the Python side"
    )
    assert "errors" in go_payload, (
        f"Go response did not carry an error for the same filtered request: {go_payload}"
    )

    baseline = _python_error_snapshot(python_error)
    candidate = _go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"flowMatrix(WORK_TYPE, filtered) error-shape MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )
