"""CHAOS-4538 stage-2 proof: local dual-run of the investment path.

Covers BOTH of #2022's registered documents in one file, not two --
`investmentFull`'s selection set (`breakdowns[] + sankey{...}`) is a
strict superset of `investmentBreakdown`'s (`breakdowns[]` alone), so
this is a superset relationship, not two parallel domains. Splitting
would duplicate the expensive, error-prone half (server-launch/envelope-
minting/registry-fixture scaffolding) for no real separation of concerns.
Structure otherwise follows `test_go_api_dual_run_hotspots.py` closely --
same real-Postgres registry-table fixture, same real-envelope minting,
same real Go binary + HTTP server harness, same
`go_api_comparator.compare_responses` comparator.

GO-ONLY INVERSION (chris 08-29 06:52 PT, PR #2022's RISK-NOTES table):
under the GO-ONLY ruling the Go copy of this path deliberately carries
argMax NULL-skip fixes (CHAOS-4547) Python lacks. So unlike every sibling
dual-run file, this one does NOT assert `comparison.is_match` as its
primary claim for the NULL-skip scenario -- a MATCH there would be the
suspicious result (the fix did not take effect). It asserts the SPECIFIC
divergence RISK-NOTES row 1 predicts, and separately proves the harness
itself is not just permanently mismatched with a control case that seeds
no NULLs at all.

Producer note (root AGENTS.md: "fixtures are producer-derived"): the real
producer for `work_unit_investments` is the work_graph/investment
categorization pipeline (`work_graph/investment/materialize.py`), which
needs an LLM call and is not a pure function this test can invoke
directly. Like hotspots' `write_file_hotspot_daily`, this test writes the
real `WorkUnitInvestmentRecord` dataclass through the real sink entry
point (`ClickHouseMetricsSink.write_work_unit_investments`), the actual
persistence boundary both the real producer and this test share -- not
hand-authored JSON.
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
from dev_health_ops.api.graphql.models.inputs import (
    AnalyticsRequestInput,
    BreakdownRequestInput,
    DateRangeInput,
    DimensionInput,
    MeasureInput,
    SankeyRequestInput,
)
from dev_health_ops.api.graphql.resolvers.analytics import resolve_analytics
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.schemas import WorkUnitInvestmentRecord
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
# registeredInvestmentBreakdownDocument (:372-388).
INVESTMENT_BREAKDOWN_DOCUMENT = """query InvestmentBreakdown($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns {
      dimension
      measure
      items {
        key
        value
      }
    }
    evidenceQualityDistribution
    evidenceQualityStats {
      mean
      stddev
      total
      bandCounts
    }
  }
}"""

# Byte-identical to cmd/query-api/query_route.go's
# registeredInvestmentFullDocument (:409-431).
INVESTMENT_FULL_DOCUMENT = """query InvestmentFull($orgId: String!, $batch: AnalyticsRequestInput!) {
  analytics(orgId: $orgId, batch: $batch) {
    breakdowns {
      dimension
      measure
      items {
        key
        value
      }
    }
    sankey {
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
      coverage {
        teamCoverage
        repoCoverage
      }
      unit
    }
  }
}"""

SCHEMA_DIGEST = "sha256:chaos-4538-investment-dual-run-test-schema-digest"
CANDIDATE_BUILD = "chaos-4538-investment-dual-run-test-build"


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
    db_name = f"chaos_4538_investment_dual_run_{uuid.uuid4().hex}"
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
    async_dsn: str, document_digest: str, *, operation: str
) -> None:
    engine = create_async_engine(async_dsn)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await register_candidate_build(
                session,
                schema_digest=SCHEMA_DIGEST,
                document_digest=document_digest,
                selected_operation=operation,
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation=operation,
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
    operation: str,
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
                selected_operation=operation,
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


def _breakdown_python_snapshot(result) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data={
            "analytics": {
                "breakdowns": [
                    {
                        "dimension": bd.dimension,
                        "measure": bd.measure,
                        "items": [{"key": i.key, "value": i.value} for i in bd.items],
                    }
                    for bd in result.breakdowns
                ],
                "evidenceQualityDistribution": result.evidence_quality_distribution,
                "evidenceQualityStats": None,
            }
        },
        data_present=True,
        errors=(),
    )


def _sankey_python_snapshot(result) -> go_api_comparator.ResponseSnapshot:
    sankey = result.sankey
    return go_api_comparator.ResponseSnapshot(
        data={
            "analytics": {
                "breakdowns": [],
                "sankey": None
                if sankey is None
                else {
                    "nodes": [
                        {
                            "id": n.id,
                            "label": n.label,
                            "dimension": n.dimension,
                            "value": n.value,
                        }
                        for n in sankey.nodes
                    ],
                    "edges": [
                        {"source": e.source, "target": e.target, "value": e.value}
                        for e in sankey.edges
                    ],
                    "coverage": None,
                    "unit": sankey.unit.value.upper()
                    if hasattr(sankey.unit, "value")
                    else str(sankey.unit).upper(),
                },
            }
        },
        data_present=True,
        errors=(),
    )


def _go_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _seed_work_unit(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    work_unit_id: str,
    computed_at: datetime,
    work_unit_type: str | None,
) -> None:
    """Seeds ONE row of `work_unit_investments` through the real sink
    entry point. Callers seed 2 rows for the SAME work_unit_id at
    different computed_at to exercise CHAOS-4547's argMax NULL-skip
    fix (investment.go:190, fix at :194-199) -- the older row keeps a
    real work_unit_type, the newer row's is genuinely NULL (a legitimate
    "categorization run mid-transition" state), and the two planes are
    EXPECTED to disagree on which one they report as current (RISK-NOTES
    row 1, PR #2022).
    """
    from_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    to_ts = datetime(2026, 1, 31, tzinfo=timezone.utc)
    sink.write_work_unit_investments(
        [
            WorkUnitInvestmentRecord(
                work_unit_id=work_unit_id,
                work_unit_type=work_unit_type,
                work_unit_name="seed work unit" if work_unit_type is not None else None,
                from_ts=from_ts,
                to_ts=to_ts,
                repo_id=None,
                provider=None,
                effort_metric="fte_days",
                effort_value=1.0,
                theme_distribution_json={"Feature Delivery": 1.0},
                subcategory_distribution_json={"Feature Delivery.build": 1.0},
                structural_evidence_json="{}",
                evidence_quality=0.9,
                evidence_quality_band="high",
                categorization_status="ok",
                categorization_errors_json="[]",
                categorization_model_version="test",
                categorization_input_hash=f"hash-{computed_at.isoformat()}",
                categorization_run_id="run",
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )


def _cleanup_work_unit_investments(sink: ClickHouseMetricsSink, org_id: str) -> None:
    sink.client.command(
        "ALTER TABLE work_unit_investments DELETE WHERE org_id = {org_id:String} "
        "SETTINGS mutations_sync=2",
        parameters={"org_id": org_id},
    )


@pytest.mark.asyncio
async def test_dual_run_investment_breakdown_worktype_argmax_null_skip_diverges(
    query_api_binary, registry_postgres, jwks_path
):
    """RISK-NOTES row 1 (PR #2022): the investment-path WORK_TYPE
    breakdown's argMax NULL-skip fix (investment.go:190, fix at
    :194-199) is EXPECTED to diverge from Python here, not match.

    Mechanism, read from the real driver source before writing this
    assertion (not assumed): clickhouse-go/v2's Nullable.ScanRow
    (lib/column/nullable.go:74-108) only handles a NULL value for
    POINTER-TO-POINTER destinations (**string etc.) or sql.Scanner; a
    plain *string destination (breakdownRow.DimensionValue's type,
    breakdown.go) matches none of those cases and the function falls
    through to a silent `return nil` WITHOUT writing to dest -- so a
    genuinely NULL work_unit_type scans as Go's zero value, "", not an
    error. Python's plain argMax NULL-skips the newer NULL row and
    reports the OLDER row's real value instead.

    So for a work unit whose two computed_at generations are (older,
    work_unit_type="feature_delivery") and (newer, work_unit_type=NULL):
    Python's breakdown item key = "feature_delivery" (stale, WRONG --
    the work unit's true latest state has no resolved type).
    Go's breakdown item key = "" (correctly reflects the true latest
    state has no type, coerced to the Go zero value by the driver).
    A MATCH here would mean the CHAOS-4547 fix did not take effect.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4538-investment-null-skip-{uuid.uuid4()}"
    work_unit_id = f"wu-{uuid.uuid4()}"
    older = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 20, 0, 0, 0, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-investment-null-skip.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(INVESTMENT_BREAKDOWN_DOCUMENT)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], document_digest, operation="investmentBreakdown"
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
        _seed_work_unit(
            sink,
            org_id=org_id,
            work_unit_id=work_unit_id,
            computed_at=older,
            work_unit_type="feature_delivery",
        )
        _seed_work_unit(
            sink,
            org_id=org_id,
            work_unit_id=work_unit_id,
            computed_at=newer,
            work_unit_type=None,
        )

        batch = AnalyticsRequestInput(
            breakdowns=[
                BreakdownRequestInput(
                    dimension=DimensionInput.WORK_TYPE,
                    measure=MeasureInput.COUNT,
                    date_range=DateRangeInput(
                        start_date=date(2026, 1, 1),
                        end_date=date(2026, 1, 31),
                    ),
                    top_n=10,
                )
            ],
            use_investment=True,
        )
        python_result = await resolve_analytics(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink),
            batch,
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            INVESTMENT_BREAKDOWN_DOCUMENT,
            {
                "orgId": org_id,
                "batch": {
                    "breakdowns": [
                        {
                            "dimension": "WORK_TYPE",
                            "measure": "COUNT",
                            "dateRange": {
                                "startDate": "2026-01-01",
                                "endDate": "2026-01-31",
                            },
                            "topN": 10,
                        }
                    ],
                    "useInvestment": True,
                },
            },
        )
    finally:
        server.stop()
        _cleanup_work_unit_investments(sink, org_id)
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"
    assert len(python_result.breakdowns) == 1
    py_items = python_result.breakdowns[0].items
    go_items = (
        go_payload.get("data", {})
        .get("analytics", {})
        .get("breakdowns", [{}])[0]
        .get("items", [])
    )

    baseline = _breakdown_python_snapshot(python_result)
    candidate = _go_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        operation="investmentBreakdown",
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    # THE ASSERTION THAT MATTERS: this is EXPECTED to diverge. Print both
    # sides regardless of outcome so a reader can see the actual observed
    # shape, not just a boolean.
    assert not comparison.is_match, (
        "SUSPICIOUS: investment breakdown WORK_TYPE matched Python on a seeded "
        "argMax NULL-skip case -- this means CHAOS-4547's tuple-wrap fix did "
        f"NOT take effect. python_items={py_items} go_items={go_items}"
    )
    py_keys = {i.key for i in py_items}
    go_keys = {i.get("key") for i in go_items}
    assert "feature_delivery" in py_keys, (
        f"expected Python's plain argMax to report the STALE work_unit_type "
        f"'feature_delivery' (the null-skip bug) -- got {py_keys}"
    )
    # codex round 3 P3 (2026-08-30, repro'd by hand before this fix, not
    # taken on the pasted claim): the two asserts below this comment did
    # not exist yet, so "feature_delivery" not in go_keys alone is
    # ALSO satisfied by Go silently returning items=[] (a swallowed
    # execute error, or any other regression that drops the row
    # entirely) -- the expected-divergence value and a total-failure
    # value were indistinguishable. Manually simulated go_items=[] against
    # the assertions as they stood: every assertion up to this point
    # still passed. Fixed by asserting the SPECIFIC corrected value is
    # present, not merely that the stale one is absent.
    assert len(go_items) >= 1, (
        f"expected at least one Go breakdown item (the CHAOS-4547-fixed "
        f"NULL-work_unit_type bucket) -- got an EMPTY list, which is "
        f"indistinguishable from a swallowed execute error without this "
        f"check. go_payload={go_payload}"
    )
    assert "feature_delivery" not in go_keys, (
        f"expected Go's tuple-wrap fix to NOT report the stale value -- got {go_keys}"
    )
    assert "" in go_keys, (
        f"expected Go's breakdown to report the CORRECTED value: an empty-string "
        f"dimension_value key (the Nullable(String)-scanned-into-*string zero "
        f"value for the genuinely-NULL work_unit_type, per clickhouse-go/v2's "
        f"Nullable.ScanRow) -- got {go_keys}, which could also be produced by an "
        f"unrelated failure, not just the expected fix"
    )

    assert await _proof_run_count(registry_postgres["async"]) >= 1


@pytest.mark.asyncio
async def test_dual_run_investment_full_sankey_control_case_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """Control case for `investmentFull`'s sankey selection set: a
    two-hop REPO->WORK_TYPE sankey path with NO nulled columns seeded
    at all. Neither CHAOS-4547 argMax site this path can reach
    (work_unit_type/repo_id/provider) has a NULL candidate here, so
    plain argMax and the tuple-wrap fix compute the IDENTICAL winning
    value -- this is expected to MATCH, proving the harness and the
    document round-trip end-to-end rather than being permanently
    mismatched by construction. TEAM dimension (site 3, ARGUED NOT
    EXECUTED per PR #2022) is deliberately NOT exercised by this path.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4538-investment-full-control-{uuid.uuid4()}"
    work_unit_id = f"wu-{uuid.uuid4()}"
    computed_at = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    from_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    to_ts = datetime(2026, 1, 31, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-investment-full-control.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(INVESTMENT_FULL_DOCUMENT)
    await _seed_candidate_and_enable_canary(
        registry_postgres["async"], document_digest, operation="investmentFull"
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
        sink.write_work_unit_investments(
            [
                WorkUnitInvestmentRecord(
                    work_unit_id=work_unit_id,
                    work_unit_type="feature_delivery",
                    work_unit_name="control seed",
                    from_ts=from_ts,
                    to_ts=to_ts,
                    repo_id=None,
                    provider=None,
                    effort_metric="fte_days",
                    effort_value=1.0,
                    theme_distribution_json={"Feature Delivery": 1.0},
                    subcategory_distribution_json={"Feature Delivery.build": 1.0},
                    structural_evidence_json="{}",
                    evidence_quality=0.9,
                    evidence_quality_band="high",
                    categorization_status="ok",
                    categorization_errors_json="[]",
                    categorization_model_version="test",
                    categorization_input_hash="hash-control",
                    categorization_run_id="run",
                    computed_at=computed_at,
                    org_id=org_id,
                )
            ]
        )

        batch = AnalyticsRequestInput(
            sankey=SankeyRequestInput(
                path=[DimensionInput.REPO, DimensionInput.WORK_TYPE],
                measure=MeasureInput.COUNT,
                date_range=DateRangeInput(
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 1, 31),
                ),
                max_nodes=50,
                max_edges=200,
            ),
            use_investment=True,
        )
        python_result = await resolve_analytics(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink),
            batch,
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            INVESTMENT_FULL_DOCUMENT,
            {
                "orgId": org_id,
                "batch": {
                    "sankey": {
                        "path": ["REPO", "WORK_TYPE"],
                        "measure": "COUNT",
                        "dateRange": {
                            "startDate": "2026-01-01",
                            "endDate": "2026-01-31",
                        },
                        "maxNodes": 50,
                        "maxEdges": 200,
                    },
                    "useInvestment": True,
                },
            },
        )
    finally:
        server.stop()
        _cleanup_work_unit_investments(sink, org_id)
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    # Sort nodes by id on BOTH sides before comparing. Root cause traced
    # against the real templates, not assumed: sankey_nodes_template
    # (sql/templates.py:97-136) and CompileSankey's identical port
    # (sankey.go) both build the nodes query as `UNION ALL` of one
    # per-dimension branch, each internally `ORDER BY value DESC, node_id
    # ASC LIMIT ...` -- but NEITHER plane wraps the union in an outer
    # ORDER BY, so the order in which ClickHouse interleaves the UNION
    # ALL branches across dimensions is unspecified by both the SQL
    # standard and this port -- this is a pre-existing characteristic of
    # BOTH planes (confirmed identical in Python's own template), not a
    # divergence CHAOS-4547 or this PR introduced. A first, unsorted run
    # of this control case observed Python return `[REPO, WORK_TYPE]` and
    # Go return `[WORK_TYPE, REPO]` -- same 2 nodes, same values, only the
    # cross-dimension order differed. Normalizing by a stable key here is
    # a fix to THIS TEST's comparison, not a claim that either plane
    # needs an outer ORDER BY (CHAOS-4381's tie-order rule governs order
    # WITHIN one dimension's own LIMIT boundary, which each branch's own
    # `ORDER BY value DESC, node_id ASC` already provides and this test
    # does not touch).
    assert python_result.sankey is not None, "expected a non-nil SankeyResult"
    python_result.sankey.nodes.sort(key=lambda n: n.id)
    go_payload["data"]["analytics"]["sankey"]["nodes"].sort(key=lambda n: n["id"])

    baseline = _sankey_python_snapshot(python_result)
    candidate = _go_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(
        baseline, candidate, allowlisted_envelope_keys=frozenset({"coverage"})
    )

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        operation="investmentFull",
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"investmentFull sankey control-case dual-run MISMATCH (unexpected -- no "
        f"NULL-skip site is exercised by this seed): terminal_state="
        f"{comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )

    assert await _proof_run_count(registry_postgres["async"]) >= 1
