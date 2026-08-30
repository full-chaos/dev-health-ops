"""CHAOS-4352 Wave 4 Lane B (CHAOS-4505) stage-2 proof: local dual-run of
operatingReview.

Plan §5 stage 2 ("local dual-run proof"): real Python and Go servers
against the same producer-seeded scratch ClickHouse/Postgres state,
comparing the complete observable response via the CHAOS-4381 comparator
(``go_api_comparator.compare_responses``) -- not merely "both return 200".

No prior dual-run test existed for this operation -- this is the first one
(ticket: "No live test exists today. You write the first one."). Structure
follows ``test_go_api_dual_run_hotspots.py``/``test_go_api_dual_run_cognitive_load.py``
closely: same real-Postgres registry-table fixture, same real-envelope
minting, same real Go binary + HTTP server harness.

THIS OPERATION HAS TWO DECLARED, INTENTIONAL DIVERGENCES -- three tests,
not one, so the clean-match test proves the port faithful and each
divergence is proven in isolation, never smuggled into a "happy path" by
a fixture shortcut that quietly dodges the question.

**Divergence 1 (CHAOS-4527, a value fix)** -- see
``cmd/query-api/internal/operatingreview/operatingreview.go``'s
``fetchAIGovernance`` doc comment for the full defect/fix/blast-radius
writeup: Python's ``ai_governance`` query
(``src/dev_health_ops/metrics/operating_review.py:343-367``) selects four
columns that do not exist in ``ai_governance_coverage_daily`` (migration
``038_ai_governance.sql``); the query cannot execute, is swallowed by
``resolvers/operating_review.py:66-96``'s per-table try/except, and
``ai_governance_coverage``/``ai_opportunity_signals`` are pinned to 0.0 in
EVERY real Python response. The Go port fixes this (ratio semantics
ported from ``AIGovernanceCoverageDaily``,
``audit/ai_governance/models.py:125-158``).

**Divergence 2 (originally a wire-format break; CHAOS-4563 retired the Go
side of it into a declared VALUE divergence -- live-discovered while
proving divergence 1, filed separately)**: ``avg()`` over a ClickHouse
aggregate query whose window has ZERO underlying rows returns ``NaN`` for
a non-Nullable ``Float64`` column (confirmed live: this hits
``repo_metrics``' ``single_owner_file_ratio_30d``/``code_ownership_gini``/
``change_failure_rate``, ``hotspots``' ``risk_score``, and ``complexity``'s
``cyclomatic_per_kloc`` -- all single-row-aggregate queries with no
outer ``GROUP BY``, reachable whenever the PRIOR week has no rows for
those specific tables: a new org's first tracked week, or an established
org whose hotspot/complexity scan jobs (sparser cadence than the daily
metrics jobs) simply did not run for any repo that week). Python's
`json.dumps` (``strawberry/http/base.py:54-55``, the actual encoder
``strawberry/fastapi/router.py:274`` calls to build the real HTTP
response body) allows NaN by default and emits a literal ``NaN`` token --
HTTP 200, "successful" by Python's own measure, but NOT valid JSON per
RFC 8259: confirmed with Node's ``JSON.parse`` (a standard client), which
REJECTS it outright. **CHAOS-4563** (chris-approved 2026-08-29 13:56 PT,
GO-ONLY -- "no more work going into the python graphql or metrics engine")
ported the shipped ``known_count`` guard (``resolvers/analytics.py:262-269``)
into the four Go call sites this hits, so Go no longer lets the NaN reach
``encoding/json``/gqlgen at all: the guard nils the row-level value when
its companion scanned-count column is 0, and ``avgF``'s empty-average-is-
``0.0`` fallback (``operatingreview.go:232``) then surfaces a normal,
finite ``0.0`` on the wire -- a clean response, no GraphQL error. Python
is untouched (GO-ONLY) and still computes NaN, since its own ``_avg``
(``operating_review.py:787-790``) has no such guard. **Neither plane is
actually "right" in an absolute sense** -- Python still emits a response a
spec-compliant client cannot parse; Go now reports a metric as an
unremarkable zero rather than surfacing that its input window was empty.
CHAOS-4534 owns whether Go's "null-safe zero" convention is the platform's
final answer for this class of defect (any Go port that averages over a
possibly-empty window into a non-Nullable float return type hits it) --
this PR narrowly retires Go's crash-on-NaN behaviour for operatingReview's
four reachable call sites, nothing broader.

Three tests:

  1. ``test_dual_run_clean_match_both_periods_populated`` -- the NINE
     correctly-ported tables get real producer-seeded data for BOTH the
     current AND prior week (no ``ai_governance_coverage_daily`` data
     either week). Neither divergence is in play: prior-period rows exist
     for every single-row-aggregate query (no NaN), and ai_governance
     stays empty on both sides (both planes compute exactly 0.0, matching
     by construction). This is the test that proves the *port* is
     faithful -- a true, unqualified MATCH, no divergence anywhere.
  2. ``test_dual_run_ai_governance_fix_is_a_declared_divergence`` -- same
     both-periods-populated fixture as test 1 (so divergence 2 is NOT in
     play here either), PLUS real ``ai_governance_coverage_daily`` data.
     Python is STILL pinned to 0.0 (the query still cannot execute); Go
     computes a real, non-zero ratio. Asserts the comparator finds a
     MISMATCH confined EXACTLY to the two known metric slots
     (``sections[5].metrics[4]`` = ai_governance_coverage,
     ``sections[5].metrics[5]`` = ai_opportunity_signals, per
     ``computeReview``'s fixed section/metric order). A MATCH here would
     mean the Go fix did not take effect and would itself be the bug.
  3. ``test_dual_run_zero_row_average_nan_is_a_declared_divergence`` --
     the ORIGINAL single-period fixture (current week only, prior week
     deliberately empty, no ai_governance data either side -- isolating
     divergence 2 with no divergence-1 noise). Post-CHAOS-4563, asserts a
     CLEAN Go response (no errors) with the four guarded metrics'
     ``delta.priorValue`` at a finite ``0.0``, THEN the comparator finds a
     MISMATCH confined EXACTLY to those four metrics' ``delta``
     sub-objects (Python's un-guarded ``_avg`` still produces NaN there --
     same "confined declared divergence" shape as test 2), AND
     independently proves Python's wire behavior within the test itself
     (not just asserted from source-reading): encoding the same response
     envelope with stdlib ``json.dumps`` -- exactly what
     ``strawberry/http/base.py`` does -- succeeds and contains a literal
     ``NaN`` token.

Producer note (root AGENTS.md: "fixtures are producer-derived", "an
inaccurate coverage claim is worse than an admitted gap"): every fixture
row below is the real dataclass the real producer writes
(``WorkItemMetricsDailyRecord``, ``FileHotspotDaily``,
``AIGovernanceCoverageDaily``, etc., ``metrics/schemas.py`` /
``audit/ai_governance/models.py``), written through the real sink entry
points (``ClickHouseMetricsSink.write_*``) -- never hand-authored JSON.

Side effects: checked by reading ``resolve_operating_review``/
``_fetch_period_rows``/``compute_operating_review`` top to bottom
(``cmd/query-api/internal/operatingreview/operatingreview.go``'s package
doc comment records the same finding): ten read-only ClickHouse queries
per period (twenty total), pure dataclass construction, no writes. There
is no side-effect digest to assert alongside the response digest.

Divergence from every other dual-run test in this directory: this
operation degrades PER TABLE rather than raising a hard error on a
missing/broken table (``_fetch_period_rows``'s try/except) -- there is
therefore no "missing table errors on both sides" test here the way
``test_go_api_dual_run_hotspots.py`` has one; an absent
``ai_governance_coverage_daily`` row set is exactly what test 1 exercises
implicitly (it never seeds that table), and it is a SUCCESS path on both
sides by design, not an error path.
"""

from __future__ import annotations

import json
import math
import os
import re
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
from dev_health_ops.api.graphql.models.inputs import OperatingReviewInput
from dev_health_ops.api.graphql.resolvers.operating_review import (
    resolve_operating_review,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.audit.ai_governance.models import AIGovernanceCoverageDaily
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.schemas import (
    AIImpactMetricsDailyRecord,
    AIOperatingLeverageComponents,
    DeployMetricsDailyRecord,
    FileHotspotDaily,
    IncidentMetricsDailyRecord,
    InvestmentMetricsRecord,
    RepoComplexityDaily,
    RepoMetricsDailyRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
)
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
# registeredOperatingReviewDocument, itself byte-identical to the REAL
# production query (web/src/lib/graphql/queries.ts's
# OPERATING_REVIEW_QUERY, operation name "OperatingReview") -- verified by
# direct string comparison against the .ts source when this port's Go
# document constant was written; kept identical here by construction
# (copy-pasted from the same source, not retyped).
OPERATING_REVIEW_DOCUMENT = """query OperatingReview($orgId: String!, $input: OperatingReviewInput!) {
  operatingReview(orgId: $orgId, input: $input) {
    orgId
    teamId
    weekStart
    priorWeekStart
    sections {
      key
      title
      changed
      improved
      worsened
      metrics {
        key
        label
        value
        unit
        delta {
          value
          priorValue
          absolute
          percent
          status
        }
      }
    }
    recommendations
    recommendationsEmptyState
  }
}"""

SCHEMA_DIGEST = "sha256:wave4-operating-review-dual-run-test-schema-digest"
CANDIDATE_BUILD = "wave4-operating-review-dual-run-test-build"

# computeReview's FIXED section/metric order
# (metrics/operating_review.py:388-395, 617-673) -- ai_workflow_intelligence
# is sections[5]; within it, ai_governance_coverage is metrics[4] and
# ai_opportunity_signals is metrics[5]. Used by the declared-divergence
# test to assert the mismatch is confined to exactly these two slots.
AI_WORKFLOW_SECTION_INDEX = 5
AI_GOVERNANCE_COVERAGE_METRIC_INDEX = 4
AI_OPPORTUNITY_SIGNALS_METRIC_INDEX = 5
EXPECTED_DIVERGENCE_PATH_PREFIXES = (
    f"$.data.operatingReview.sections[{AI_WORKFLOW_SECTION_INDEX}]"
    f".metrics[{AI_GOVERNANCE_COVERAGE_METRIC_INDEX}]",
    f"$.data.operatingReview.sections[{AI_WORKFLOW_SECTION_INDEX}]"
    f".metrics[{AI_OPPORTUNITY_SIGNALS_METRIC_INDEX}]",
    # SECOND-ORDER consequence of the SAME divergence, not a wider leak:
    # buildSection/_section (metrics/operating_review.py:676-697) derives
    # `changed`/`improved`/`worsened` PURELY by iterating that section's
    # own metrics and reading each metric's delta.status -- so when
    # ai_governance_coverage/ai_opportunity_signals' STATUS changes (not
    # just their value) because Go's fix makes them non-zero while Python
    # stays pinned at 0.0, the ai_workflow_intelligence section's derived
    # summary-string lists (and the top-level `recommendations` list,
    # built the same way across ALL sections,
    # _recommendations_from_sections, metrics/operating_review.py:
    # 748-766) diverge too -- confirmed live: exactly these paths, no
    # other section's summary lists, and no other section's `recommendations`
    # entries. Live-verified (not assumed) which of the two metrics'
    # status actually flips per run, so both the section-index-5 summary
    # lists and top-level `recommendations` are declared here as a single
    # named exception, not left to a broad prefix match.
    "$.data.operatingReview.sections[5].changed",
    "$.data.operatingReview.sections[5].improved",
    "$.data.operatingReview.sections[5].worsened",
    "$.data.operatingReview.recommendations",
)


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
    db_name = f"chaos_4505_operating_review_dual_run_{uuid.uuid4().hex}"
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
                selected_operation="operatingReview",
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation="operatingReview",
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
                selected_operation="operatingReview",
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
    body = json.dumps(
        {"query": OPERATING_REVIEW_DOCUMENT, "variables": variables}
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


# --- Fixture seeding: real dataclasses through the real sink entry points ---


def _seed_nine_tables(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    team_id: str,
    repo_id: uuid.UUID,
    day_: date,
    computed_at: datetime,
) -> None:
    """Seeds the NINE verbatim-ported tables with one real producer row
    each, via the real sink write methods. ``ai_governance_coverage_daily``
    is deliberately NOT written here -- see this module's doc comment for
    why the two dual-run tests need different ai_governance fixtures.
    """
    sink.write_work_item_metrics(
        [
            WorkItemMetricsDailyRecord(
                day=day_,
                provider="github",
                work_scope_id="repo-a",
                team_id=team_id,
                team_name="Team One",
                items_started=12,
                items_completed=8,
                items_started_unassigned=0,
                items_completed_unassigned=0,
                wip_count_end_of_day=4,
                wip_unassigned_end_of_day=0,
                cycle_time_p50_hours=5.0,
                cycle_time_p90_hours=9.0,
                lead_time_p50_hours=6.0,
                lead_time_p90_hours=10.0,
                wip_age_p50_hours=1.0,
                wip_age_p90_hours=2.0,
                bug_completed_ratio=0.1,
                story_points_completed=20.0,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )
    sink.write_work_item_state_durations(
        [
            WorkItemStateDurationDailyRecord(
                day=day_,
                provider="github",
                work_scope_id="repo-a",
                team_id=team_id,
                team_name="Team One",
                status="in_review",
                duration_hours=3.0,
                items_touched=6,
                computed_at=computed_at,
                avg_wip=1.5,
                org_id=org_id,
            )
        ]
    )
    sink.write_repo_metrics(
        [
            RepoMetricsDailyRecord(
                repo_id=repo_id,
                day=day_,
                commits_count=15,
                total_loc_touched=500,
                avg_commit_size_loc=33.3,
                large_commit_ratio=0.1,
                prs_merged=5,
                median_pr_cycle_hours=12.0,
                computed_at=computed_at,
                prs_with_first_review=5,
                pr_first_review_p50_hours=4.0,
                single_owner_file_ratio_30d=0.6,
                bus_factor=3,
                code_ownership_gini=0.3,
                mttr_hours=2.0,
                change_failure_rate=0.1,
                org_id=org_id,
            )
        ]
    )
    sink.write_file_hotspot_daily(
        [
            FileHotspotDaily(
                repo_id=repo_id,
                day=day_,
                file_path="src/main.go",
                churn_loc_30d=500,
                churn_commits_30d=20,
                cyclomatic_total=30,
                cyclomatic_avg=4.5,
                blame_concentration=0.75,
                risk_score=0.4,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )
    sink.write_repo_complexity_daily(
        [
            RepoComplexityDaily(
                repo_id=repo_id,
                day=day_,
                loc_total=10000,
                cyclomatic_total=800,
                cyclomatic_per_kloc=12.5,
                high_complexity_functions=3,
                very_high_complexity_functions=1,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )
    sink.write_deploy_metrics(
        [
            DeployMetricsDailyRecord(
                repo_id=repo_id,
                day=day_,
                deployments_count=9,
                failed_deployments_count=1,
                deploy_time_p50_hours=0.5,
                lead_time_p50_hours=3.0,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )
    sink.write_incident_metrics(
        [
            IncidentMetricsDailyRecord(
                repo_id=repo_id,
                day=day_,
                incidents_count=2,
                mttr_p50_hours=3.0,
                mttr_p90_hours=5.0,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )
    sink.write_investment_metrics(
        [
            InvestmentMetricsRecord(
                repo_id=repo_id,
                day=day_,
                team_id=team_id,
                investment_area="feature_delivery",
                project_stream="core",
                delivery_units=7,
                work_items_completed=6,
                prs_merged=5,
                churn_loc=400,
                cycle_p50_hours=8.0,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )
    sink.write_ai_impact_metrics(
        [
            AIImpactMetricsDailyRecord(
                org_id=org_id,
                team_id=team_id,
                repo_id=repo_id,
                work_type="feature",
                day=day_,
                attribution_bucket="human",
                prs_total=10,
                prs_merged=9,
                ai_assisted_prs=2,
                agent_created_prs=1,
                human_prs=6,
                unknown_prs=1,
                ai_assisted_pr_ratio=0.2,
                agent_created_pr_count=1,
                cycle_time_avg_hours=5.0,
                baseline_cycle_time_avg_hours=6.0,
                ai_cycle_time_delta_hours=1.0,
                reviews_per_pr=1.5,
                baseline_reviews_per_pr=1.3,
                ai_review_amplification=1.2,
                changes_requested_per_pr=0.4,
                rework_prs=1,
                rework_drag_rate=0.1,
                followup_commits_count=2,
                revert_prs=0,
                revert_rate=0.0,
                incidents_count=0,
                incident_drag_rate=0.0,
                test_gap_prs=1,
                test_gap_rate=0.05,
                leverage=AIOperatingLeverageComponents(
                    prs_component=0.2,
                    cycle_time_component=0.1,
                    review_component=0.1,
                    rework_component=0.05,
                    test_component=0.05,
                    incident_component=0.0,
                ),
                computed_at=computed_at,
            )
        ]
    )


def _seed_ai_governance(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    team_id: str,
    repo_id: uuid.UUID,
    day_: date,
    computed_at: datetime,
) -> None:
    """Seeds ai_governance_coverage_daily with real, non-trivial coverage
    data -- used ONLY by the declared-divergence test. With this data
    present, Go's fix computes a real ratio while Python stays pinned to
    0.0 (its query still cannot execute) -- exactly the divergence this
    port declares.
    """
    sink.write_ai_governance_coverage_daily(
        [
            AIGovernanceCoverageDaily(
                org_id=org_id,
                team_id=team_id,
                repo_id=repo_id,
                day=day_,
                ai_artifacts=10,
                declared_artifacts=8,
                human_reviewed_prs=5,
                security_scanned_prs=10,
                in_policy_artifacts=9,
                computed_at=computed_at,
            )
        ]
    )


def _delete_org_rows(sink: ClickHouseMetricsSink, org_id: str) -> None:
    tables = (
        "work_item_metrics_daily",
        "work_item_state_durations_daily",
        "repo_metrics_daily",
        "file_hotspot_daily",
        "repo_complexity_daily",
        "deploy_metrics_daily",
        "incident_metrics_daily",
        "investment_metrics_daily",
        "ai_impact_metrics_daily",
        "ai_governance_coverage_daily",
    )
    for table in tables:
        sink.client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )


# --- Response snapshot builders ---


def _go_response_snapshot(payload: dict) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _python_response_snapshot(review) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_operating_review's return value into the same
    on-the-wire GraphQL response envelope the Go HTTP endpoint produces --
    mirrors resolvers/operating_review.py's _to_graphql_review/
    _to_graphql_section/_to_graphql_metric field mapping exactly
    (resolvers/operating_review.py:99-137).
    """
    return go_api_comparator.ResponseSnapshot(
        data={
            "operatingReview": {
                "orgId": review.org_id,
                "teamId": review.team_id,
                "weekStart": review.week_start.isoformat(),
                "priorWeekStart": review.prior_week_start.isoformat(),
                "sections": [
                    {
                        "key": section.key,
                        "title": section.title,
                        "changed": section.changed,
                        "improved": section.improved,
                        "worsened": section.worsened,
                        "metrics": [
                            {
                                "key": metric.key,
                                "label": metric.label,
                                "value": metric.value,
                                "unit": metric.unit,
                                "delta": {
                                    "value": metric.delta.value,
                                    "priorValue": metric.delta.prior_value,
                                    "absolute": metric.delta.absolute,
                                    "percent": metric.delta.percent,
                                    "status": metric.delta.status,
                                },
                            }
                            for metric in section.metrics
                        ],
                    }
                    for section in review.sections
                ],
                "recommendations": review.recommendations,
                "recommendationsEmptyState": review.recommendations_empty_state,
            }
        },
        data_present=True,
        errors=(),
    )


def _section(review, key: str):
    """review.sections lookup by key -- api.graphql.models.outputs.OperatingReview
    (what resolve_operating_review actually returns) is a plain strawberry
    type with no .section()/.metric() helper, unlike
    metrics.operating_review.OperatingReview's dataclass (which has them,
    and is a DIFFERENT type resolve_operating_review only consumes
    internally before converting via _to_graphql_review)."""
    for section in review.sections:
        if section.key == key:
            return section
    raise KeyError(key)


def _metric(section, key: str):
    for metric in section.metrics:
        if metric.key == key:
            return metric
    raise KeyError(key)


def _graphql_variables(org_id: str, week_start: date) -> dict:
    return {
        "orgId": org_id,
        "input": {
            "weekStart": week_start.isoformat(),
            "teamId": None,
        },
    }


@pytest.mark.asyncio
async def test_dual_run_clean_match_both_periods_populated(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2, the nine verbatim-ported tables, BOTH periods populated so
    neither declared divergence is in play (see module doc comment):
    real fixture rows through the real sink entry points, real Python
    resolver call, real Go HTTP server -- compared via the CHAOS-4381
    comparator. This is the test that proves the *port* is faithful: a
    true, unqualified MATCH.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4505-operating-review-dual-run-{uuid.uuid4()}"
    team_id = "team-1"
    repo_id = uuid.uuid4()
    week_start = date(2026, 8, 24)
    current_day = date(2026, 8, 25)
    current_computed_at = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)
    prior_day = date(2026, 8, 18)
    prior_computed_at = datetime(2026, 8, 18, 12, 0, 0, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-operating-review-clean-match.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(OPERATING_REVIEW_DOCUMENT)
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
        _seed_nine_tables(
            sink,
            org_id=org_id,
            team_id=team_id,
            repo_id=repo_id,
            day_=current_day,
            computed_at=current_computed_at,
        )
        _seed_nine_tables(
            sink,
            org_id=org_id,
            team_id=team_id,
            repo_id=repo_id,
            day_=prior_day,
            computed_at=prior_computed_at,
        )

        python_review = await resolve_operating_review(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            OperatingReviewInput(team_id=None, week_start=week_start),
        )
        go_payload = _post_graphql(
            server.base_url, token, _graphql_variables(org_id, week_start)
        )
    finally:
        server.stop()
        _delete_org_rows(sink, org_id)
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"
    assert python_review.org_id == org_id

    # Sanity: the seeded work_items row actually reached the Python
    # resolver (proves the fixture path, not just an empty-both-sides
    # false match), and the PRIOR-period row landed too (a real delta,
    # not "value vs 0").
    delivery = _section(python_review, "delivery_movement")
    assert _metric(delivery, "throughput").value == 8, (
        "seeded items_completed did not reach the Python resolver"
    )
    assert _metric(delivery, "throughput").delta.prior_value == 8, (
        "prior-period seed did not reach the Python resolver -- this test "
        "needs BOTH periods populated to avoid the zero-row-average NaN "
        "case (see module doc comment, divergence 2)"
    )
    # ai_governance_coverage stays exactly 0.0 on the Python side with no
    # rows seeded -- Go's fix over an empty row set also computes exactly
    # 0.0 (aiGovernanceCoverage(nil) == 0.0, proven in unit tests), so this
    # metric is not a source of divergence in this test either.
    ai_section = _section(python_review, "ai_workflow_intelligence")
    assert _metric(ai_section, "ai_governance_coverage").value == 0.0

    baseline = _python_response_snapshot(python_review)
    candidate = _go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"operatingReview dual-run MISMATCH on the nine verbatim-ported "
        f"tables with both periods populated (should be a clean, "
        f"unqualified match -- neither declared divergence is in play "
        f"here): terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )

    assert await _proof_run_count(registry_postgres["async"]) >= 1


@pytest.mark.asyncio
async def test_dual_run_ai_governance_fix_is_a_declared_divergence(
    query_api_binary, registry_postgres, jwks_path
):
    """The declared-divergence proof for CHAOS-4527 (a VALUE fix): same
    both-periods-populated fixture as the clean-match test (so divergence
    2, the zero-row-average NaN break, is NOT in play here either), PLUS
    real ai_governance_coverage_daily data. Python is STILL pinned to 0.0
    for ai_governance_coverage/ai_opportunity_signals (its query still
    cannot execute against the real schema -- this test also proves that
    live, not by assumption); Go computes the real ratio via the declared
    fix. Asserts the comparator finds a MISMATCH confined EXACTLY to the
    two known metric slots -- a comparator MATCH here would mean the Go
    fix did not take effect, which is itself the bug.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4505-operating-review-divergence-{uuid.uuid4()}"
    team_id = "team-1"
    repo_id = uuid.uuid4()
    week_start = date(2026, 8, 24)
    current_day = date(2026, 8, 25)
    current_computed_at = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)
    prior_day = date(2026, 8, 18)
    prior_computed_at = datetime(2026, 8, 18, 12, 0, 0, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-operating-review-divergence.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(OPERATING_REVIEW_DOCUMENT)
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
        _seed_nine_tables(
            sink,
            org_id=org_id,
            team_id=team_id,
            repo_id=repo_id,
            day_=current_day,
            computed_at=current_computed_at,
        )
        _seed_nine_tables(
            sink,
            org_id=org_id,
            team_id=team_id,
            repo_id=repo_id,
            day_=prior_day,
            computed_at=prior_computed_at,
        )
        _seed_ai_governance(
            sink,
            org_id=org_id,
            team_id=team_id,
            repo_id=repo_id,
            day_=current_day,
            computed_at=current_computed_at,
        )

        python_review = await resolve_operating_review(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            OperatingReviewInput(team_id=None, week_start=week_start),
        )
        go_payload = _post_graphql(
            server.base_url, token, _graphql_variables(org_id, week_start)
        )
    finally:
        server.stop()
        _delete_org_rows(sink, org_id)
        sink.close()

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    # LIVE proof that Python's query still cannot execute against the
    # real schema, even with real ai_governance data present -- not an
    # assumption carried over from the source-reading finding.
    ai_section = _section(python_review, "ai_workflow_intelligence")
    assert _metric(ai_section, "ai_governance_coverage").value == 0.0, (
        "expected Python to STILL be pinned to 0.0 with real "
        "ai_governance_coverage_daily data present -- if this fails, "
        "CHAOS-4527 has been fixed on the Python side and this test's "
        "premise (and the declared divergence) needs to be revisited"
    )

    baseline = _python_response_snapshot(python_review)
    candidate = _go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert not comparison.is_match, (
        "expected a MISMATCH -- a MATCH here means the Go ai_governance "
        "fix did not take effect (Go would have to be computing 0.0 too), "
        f"which is itself a defect: findings={comparison.findings}"
    )

    mismatch_findings = [f for f in comparison.findings if f.kind == "mismatch"]
    assert mismatch_findings, "expected at least one mismatch finding"

    stray = [
        f
        for f in mismatch_findings
        if not f.path.startswith(EXPECTED_DIVERGENCE_PATH_PREFIXES)
    ]
    assert not stray, (
        f"mismatch findings outside the declared ai_governance_coverage/"
        f"ai_opportunity_signals divergence -- the fix leaked into fields "
        f"it should not have touched: {stray}"
    )

    # Coverage is required only for the two ROOT metric slots -- the
    # summary-list/recommendations prefixes above are a downstream
    # CONSEQUENCE of whichever delta.status each metric lands on this
    # run (improved/worsened/unchanged/changed), not a guaranteed
    # occurrence: e.g. `sections[5].changed` can never fire for THIS
    # divergence specifically, since neither ai_governance_coverage
    # (higherIsBetter) nor ai_opportunity_signals (lowerIsBetter) uses
    # the `neutral` direction that produces a "changed" status
    # (buildMetric, metrics/operating_review.py:700-735). Requiring it
    # would assert a coincidence of THIS fixture's numbers, not the
    # divergence itself.
    required_prefixes = (
        f"$.data.operatingReview.sections[{AI_WORKFLOW_SECTION_INDEX}]"
        f".metrics[{AI_GOVERNANCE_COVERAGE_METRIC_INDEX}]",
        f"$.data.operatingReview.sections[{AI_WORKFLOW_SECTION_INDEX}]"
        f".metrics[{AI_OPPORTUNITY_SIGNALS_METRIC_INDEX}]",
    )
    covered = {
        prefix: any(f.path.startswith(prefix) for f in mismatch_findings)
        for prefix in required_prefixes
    }
    assert all(covered.values()), (
        f"expected BOTH declared-divergence metrics to actually diverge, "
        f"got coverage={covered} findings={mismatch_findings}"
    )


# computeReview's fixed section order (metrics/operating_review.py:388-395)
# -- the paths test 3 below expects the guarded finite-zero/NaN divergence
# at, for the three single-row-aggregate queries whose non-Nullable
# Float64 columns hit the zero-row avg() -> NaN case with an empty prior
# period: risk (index 2: hotspot_risk_score, ownership_concentration,
# complexity_per_kloc -- ALL THREE of that section's first three metrics)
# and reliability (index 3: change_failure_rate, metric index 1).
RISK_SECTION_INDEX = 2
RELIABILITY_SECTION_INDEX = 3
RISK_GUARDED_METRIC_INDICES = (
    0,
    1,
    2,
)  # hotspot_risk_score, ownership_concentration, complexity_per_kloc
RELIABILITY_GUARDED_METRIC_INDEX = 1  # change_failure_rate

# CHAOS-4563 re-points this declared divergence at the NEW shape. The
# known_count guard (operatingreview.go:351 knownCountGuard, applied at
# the four call sites named in the module doc comment's divergence-2
# paragraph) nils the affected row-level value when its companion
# scanned-count column is 0; avgF (operatingreview.go:232) then treats
# "no present values" the same way Python's own _avg/_sum/etc. already
# treat an empty list -- it returns 0.0, not NaN and not null. So the
# guarded delta.priorValue on the Go side is always a normal, FINITE 0.0.
# The divergence from Python survives regardless: Python's _avg
# (operating_review.py:787-790) has no such guard (GO-ONLY ruling, chris
# 08-29 06:52 PT) and still averages the raw ClickHouse row, which is
# itself NaN for a non-Nullable Float64 column over zero rows -- so
# Python's delta.prior_value for these four metrics stays NaN. This is
# EXPECTED, not a regression: the Go copy now carries a fix Python lacks,
# so a comparator MATCH here would mean the fix did not take effect.
GUARDED_DELTA_PATH_PREFIXES = tuple(
    f"$.data.operatingReview.sections[{RISK_SECTION_INDEX}].metrics[{i}].delta"
    for i in RISK_GUARDED_METRIC_INDICES
) + (
    f"$.data.operatingReview.sections[{RELIABILITY_SECTION_INDEX}]"
    f".metrics[{RELIABILITY_GUARDED_METRIC_INDEX}].delta",
)
# The one delta sub-field GUARANTEED to diverge regardless of this
# fixture's concrete risk/complexity/change-failure-rate numbers: Python's
# prior_value is NaN (a present float) and Go's is a present, finite 0.0
# -- NaN never equals anything, so this field always mismatches
# (go_api_comparator._compare_number's parity-rule-3 "NaN/Infinity is
# ALWAYS a mismatch" branch fires unconditionally). `.absolute` inherits
# the same NaN-vs-finite mismatch by construction (value - NaN is NaN);
# `.percent`/`.status` are a downstream CONSEQUENCE of this fixture's
# concrete numbers (percent: Python's NaN-present vs Go's `prior == 0`
# special case -- None/null when delta_value != 0; status: both sides'
# comparisons against NaN happen to fall through the same "else" branch
# for this fixture's seeded values, but that is not guaranteed for every
# possible seed) -- not required coverage, same reasoning as test 2's
# ai_governance downstream-consequence note below.
REQUIRED_GUARDED_SUFFIX = "priorValue"

# A SECOND downstream consequence, one level further removed than
# `.percent`/`.status`: this fixture's seeded values make all four
# guarded metrics land on status "worsened" on BOTH sides (a real,
# executed finding, not an assumption -- confirmed by running this test:
# `.status` itself does NOT appear in the mismatch findings). But
# `_recommendations_from_sections`/``buildSection``'s `worsened` list
# render `.absolute` into a TEXT string (`"...worsened by %+.1f ..."`,
# metrics/operating_review.py:738-745 / operatingreview.go:1451-1460), so
# Python's `+nan` and Go's real `+0.4`-shaped number produce different
# STRINGS even though the upstream `.status` enum matches.
#
# This is REQUIRED, not merely permitted (confirmed firing by executing
# this test, not argued from source) -- but the exemption is scoped to
# the MECHANISM, not the location: a same-sentence/different-rendered-
# number pair is the ONLY difference this guard is allowed to produce
# here. `recommendations` in particular is the whole, UNSCOPED array --
# CHAOS-4381 makes list tie-ordering a comparator rule for this epic, so
# a dropped/reordered/misattributed recommendation is exactly the class
# this dual-run exists to catch, and a blanket path-prefix exemption
# would make this test blind to it. `_assert_text_list_diverges_only_by_
# numeric_token` below enforces the mechanism directly against the ACTUAL
# list contents (never Finding.detail's repr'd string, which is the
# comparator's INTERNAL representation, not a contract this test should
# depend on) -- the comparator's own list-length/tie-ordering checks
# still apply first (strict positional compare, no relaxed declaration
# for these paths), so a length or order divergence still fails loudly
# via the comparator's own mismatch, never absorbed here.
_NUMERIC_TOKEN_RE = re.compile(r"[+-]?(?:nan|\d+\.?\d*)", re.IGNORECASE)
_NUMERIC_TOKEN_CAPTURE_RE = re.compile(r"([+-]?(?:nan|\d+\.?\d*))", re.IGNORECASE)


def _normalize_numeric_tokens(s: str) -> str:
    """Collapse every signed decimal / nan token to one placeholder, so
    two sentences differing ONLY in a rendered numeric value compare
    equal. Answers "did ONLY the number differ" -- never "was the number
    CORRECT" (that is `_extract_numeric_token` + an exact expected-value
    compare, below); codex R2 P2 flagged that this normalization alone
    would let Go render an outright WRONG number and still pass, since a
    wrong number collapses to the same placeholder as the right one."""
    return _NUMERIC_TOKEN_RE.sub("#", s)


def _extract_numeric_token(s: str) -> str:
    """Extracts the single rendered numeric/nan token from a
    deltaSummary/_delta_summary sentence (e.g.
    `"...worsened by +0.4 score week-over-week."` -> `"+0.4"`) so a
    caller can verify WHICH number rendered, not merely that a number-
    shaped thing did."""
    m = _NUMERIC_TOKEN_CAPTURE_RE.search(s)
    assert m, f"no numeric token found in {s!r}"
    return m.group(1)


def _is_nan_token(token: str) -> bool:
    return token.lstrip("+-").lower() == "nan"


def _expected_guarded_delta(value: float) -> dict:
    """Mirrors buildMetric (operatingreview.go:1398-1440) / _metric
    (metrics/operating_review.py:700-735) verbatim for the ONE case
    CHAOS-4563's guard produces: prior is always the guard's finite
    0.0 fallback (see GUARDED_DELTA_PATH_PREFIXES's doc comment above),
    and all four guarded metrics (hotspot_risk_score,
    ownership_concentration, complexity_per_kloc, change_failure_rate)
    share the same LOWER_IS_BETTER direction. Computing the expected
    delta from the formula -- given the two known inputs, this metric's
    own reported `value` and the guard's already-proven `priorValue`
    of 0.0 -- closes codex R2 P2: a blanket "the whole `delta`
    sub-object may differ" prefix would let an UNRELATED bug in
    `.value`/`.status`/`.percent` for one of these four metrics pass
    silently as "not stray", since nothing checked those sub-fields
    were computed the way the guard is actually supposed to produce
    them.
    """
    prior = 0.0
    delta_value = value - prior
    percent = None if delta_value != 0 else 0.0
    if abs(delta_value) < 0.000001:
        status = "unchanged"
    elif delta_value < 0:  # LOWER_IS_BETTER: negative delta improves
        status = "improved"
    else:
        status = "worsened"
    return {
        "value": value,
        "priorValue": prior,
        "absolute": delta_value,
        "percent": percent,
        "status": status,
    }


def _assert_text_list_diverges_only_by_numeric_token(
    python_entries: list[str], go_entries: list[str], *, list_label: str
) -> set[int]:
    """Pairwise-compares two lists this operation renders with STRICT
    positional ordering (no relaxed-tie-ordering declaration for
    `recommendations`/`sections[N].worsened`, so position i on one side
    is position i on the other by construction). A length mismatch is a
    structural divergence, never this guard's text-rendering consequence
    -- fails loudly here rather than being silently absorbed. Returns the
    set of indices that differ; every differing index is asserted to
    differ ONLY by a rendered numeric token -- ANY other kind of
    per-index difference is a hard failure, not a permitted consequence.
    """
    assert len(python_entries) == len(go_entries), (
        f"{list_label} list LENGTH diverged ({len(python_entries)} vs "
        f"{len(go_entries)}) -- a structural divergence, not this guard's "
        f"numeric-token-only text consequence: python={python_entries!r} "
        f"go={go_entries!r}"
    )
    diverged: set[int] = set()
    for i, (p, g) in enumerate(zip(python_entries, go_entries)):
        if p == g:
            continue
        assert _normalize_numeric_tokens(p) == _normalize_numeric_tokens(g), (
            f"{list_label}[{i}] diverged by more than a rendered numeric "
            f"token -- not a permitted consequence of CHAOS-4563's guard: "
            f"python={p!r} go={g!r}"
        )
        diverged.add(i)
    return diverged


def _index_containing(entries: list[str], label: str) -> int:
    matches = [i for i, e in enumerate(entries) if label in e]
    assert len(matches) == 1, (
        f"expected exactly one entry containing {label!r}, got "
        f"{[entries[i] for i in matches]!r} in {entries!r}"
    )
    return matches[0]


@pytest.mark.asyncio
async def test_dual_run_zero_row_average_nan_is_a_declared_divergence(
    query_api_binary, registry_postgres, jwks_path
):
    """The declared-divergence proof for the zero-row-average NaN break
    (originally a WIRE-FORMAT break; CHAOS-4563 retired the Go side of it
    into a declared VALUE divergence -- live-discovered while proving
    CHAOS-4527, filed separately; see module doc comment, divergence 2).
    The ORIGINAL single-period fixture: current week only, prior week
    deliberately left empty for repo_metrics/hotspots/complexity, no
    ai_governance data either side (isolating this divergence with no
    divergence-1 noise).

    Post-CHAOS-4563 this test's shape matches
    ``test_dual_run_ai_governance_fix_is_a_declared_divergence`` above:
    assert a CLEAN Go response (the known_count guard means the zero-row
    prior-period average never reaches gqlgen's marshaler as NaN), assert
    the four guarded metrics' concrete delta.priorValue on the Go side,
    run the comparator, and confirm the mismatch is CONFINED to those
    four metrics' delta sub-objects -- never a MATCH (would mean
    CHAOS-4563's guard did not take effect) and never a stray mismatch
    elsewhere (the guard, or this fixture, touching something it should
    not have).

    Independently, this also still proves Python's ACTUAL wire behavior
    for the un-guarded side -- encoding the same response envelope with
    stdlib ``json.dumps``, precisely what ``strawberry/http/base.py:54-55``
    does to build the real HTTP body -- succeeds and contains a literal
    ``NaN`` token: the GO-ONLY ruling means this PR cannot and does not
    change Python's own non-compliant-JSON behavior.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4505-operating-review-nan-divergence-{uuid.uuid4()}"
    team_id = "team-1"
    repo_id = uuid.uuid4()
    week_start = date(2026, 8, 24)
    day_ = date(2026, 8, 25)
    computed_at = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-operating-review-nan-divergence.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(OPERATING_REVIEW_DOCUMENT)
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
        # Deliberately ONE period only -- the prior week is empty for
        # repo_metrics_daily/file_hotspot_daily/repo_complexity_daily,
        # which is exactly what makes their avg()-over-zero-rows queries
        # return NaN for the prior side.
        _seed_nine_tables(
            sink,
            org_id=org_id,
            team_id=team_id,
            repo_id=repo_id,
            day_=day_,
            computed_at=computed_at,
        )

        python_review = await resolve_operating_review(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            OperatingReviewInput(team_id=None, week_start=week_start),
        )
        go_payload = _post_graphql(
            server.base_url, token, _graphql_variables(org_id, week_start)
        )
    finally:
        server.stop()
        _delete_org_rows(sink, org_id)
        sink.close()

    # (a) Go: a CLEAN response -- CHAOS-4563's known_count guard means the
    # zero-row prior-period average no longer reaches gqlgen's marshaler
    # as NaN. An error here means the guard regressed to the
    # pre-CHAOS-4563 behaviour this PR exists to retire.
    assert "errors" not in go_payload, (
        "expected a clean Go response under CHAOS-4563's known_count "
        f"guard, got GraphQL errors: {go_payload}"
    )
    # For each guarded metric, assert the ENTIRE delta sub-object equals
    # the EXACT value `_expected_guarded_delta` computes from this
    # metric's own (unguarded, current-period) `value` and the guard's
    # proven `priorValue=0.0` fallback -- not merely that `priorValue`
    # looks right while `.absolute`/`.percent`/`.status` go unchecked.
    go_sections = go_payload["data"]["operatingReview"]["sections"]
    guarded_metric_positions = tuple(
        zip(
            (RISK_SECTION_INDEX,) * len(RISK_GUARDED_METRIC_INDICES),
            RISK_GUARDED_METRIC_INDICES,
            ("Hotspot risk", "Ownership concentration", "Complexity"),
        )
    ) + (
        (
            RELIABILITY_SECTION_INDEX,
            RELIABILITY_GUARDED_METRIC_INDEX,
            "Change failure rate",
        ),
    )
    expected_absolute_by_label: dict[str, float] = {}
    for section_index, metric_index, label in guarded_metric_positions:
        metric = go_sections[section_index]["metrics"][metric_index]
        expected = _expected_guarded_delta(metric["value"])
        assert metric["delta"] == expected, (
            f"expected the guarded metric {label!r} "
            f"(sections[{section_index}].metrics[{metric_index}]) to "
            f"have delta EXACTLY {expected!r} -- computed from "
            f"buildMetric's own formula given its known value="
            f"{metric['value']!r} and the guard's proven priorValue=0.0 "
            f"(operatingreview.go:232's avgF empty-average-is-0.0 "
            f"fallback), got {metric['delta']!r}"
        )
        expected_absolute_by_label[label] = expected["absolute"]

    # (b) Python: encode the SAME response envelope the real production
    # endpoint would build, through the SAME encoder
    # (strawberry/http/base.py:54-55's json.dumps), and prove it succeeds
    # with a literal NaN token rather than raising -- the GO-ONLY ruling
    # means this side is untouched by CHAOS-4563 and must still diverge.
    baseline = _python_response_snapshot(python_review)
    encoded = json.dumps(baseline.data)
    assert "NaN" in encoded, (
        f"expected Python's real encode_json path (stdlib json.dumps) to "
        f"emit a literal NaN token for the zero-row-average case; got no "
        f"NaN in the encoded payload -- if this fails, Python's behavior "
        f"here has changed and this declared divergence needs to be "
        f"revisited: {encoded[:2000]}"
    )
    # Confirm this is genuinely non-compliant JSON, not a false alarm --
    # Python's own json.loads round-trips it (so Python-side consumers
    # never notice), but it is not valid JSON per RFC 8259 (verified
    # separately against a real spec-compliant parser, Node's
    # JSON.parse, which REJECTS it -- see the PR's RISK-NOTES for that
    # command's output).
    reparsed = json.loads(encoded)
    assert reparsed is not None
    # Directly on the Python dataclass too (not just the encoded string):
    # the guarded metrics' prior_value is a real float NaN, not None --
    # Python's un-guarded _avg has no known_count concept to null it out.
    risk = _section(python_review, "risk")
    for i in RISK_GUARDED_METRIC_INDICES:
        assert math.isnan(risk.metrics[i].delta.prior_value), (
            f"expected Python's un-guarded _avg to still produce NaN at "
            f"risk metric index {i}'s delta.prior_value, got "
            f"{risk.metrics[i].delta.prior_value!r} -- if this fails, "
            f"CHAOS-4534/CHAOS-4563's declared divergence needs revisiting"
        )
    reliability = _section(python_review, "reliability")
    assert math.isnan(
        reliability.metrics[RELIABILITY_GUARDED_METRIC_INDEX].delta.prior_value
    ), "expected Python's un-guarded change_failure_rate prior_value to be NaN"

    # (c) The comparator: expect a MISMATCH confined EXACTLY to the four
    # guarded metrics' delta sub-objects. A MATCH here would mean
    # CHAOS-4563's guard did not take effect on the Go side (Go would
    # have to be emitting NaN too, which gqlgen cannot even marshal); a
    # stray mismatch elsewhere means the guard (or this fixture) touched
    # something it should not have.
    candidate = _go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert not comparison.is_match, (
        "expected a MISMATCH -- a MATCH here means CHAOS-4563's "
        "known_count guard did not take effect (Go would have to be "
        f"computing NaN too), which is itself a defect: "
        f"findings={comparison.findings}"
    )

    mismatch_findings = [f for f in comparison.findings if f.kind == "mismatch"]
    assert mismatch_findings, "expected at least one mismatch finding"

    # The two downstream TEXT lists: validate the mechanism directly
    # against the ACTUAL list contents (never Finding.detail's repr'd
    # string). `_assert_text_list_diverges_only_by_numeric_token` first
    # confirms EVERY differing index in the whole list (guarded or not)
    # differs ONLY by a rendered numeric token -- a non-numeric surprise
    # anywhere in these lists fails loudly right here. Then, per chris's
    # standard (an expected-divergence list is enumerated PATH BY PATH,
    # each with the reason it diverges -- never a prefix or a computed
    # "whatever differed" set that could swallow an unrelated neighbour),
    # each of the eight specific entries below is named individually by
    # its guarded-metric label, with its own reason, and is the ONLY
    # thing `allowed_text_paths` ends up containing -- an extra
    # numeric-only divergence at some OTHER, unnamed index would still be
    # caught as a stray finding below, not silently swallowed by however
    # many indices happened to differ.
    go_data = candidate.data["operatingReview"]
    python_data = baseline.data["operatingReview"]

    python_recommendations = python_data["recommendations"]
    go_recommendations = go_data["recommendations"]
    recommendations_diverged = _assert_text_list_diverges_only_by_numeric_token(
        python_recommendations, go_recommendations, list_label="recommendations"
    )

    risk_worsened_python = python_data["sections"][RISK_SECTION_INDEX]["worsened"]
    risk_worsened_go = go_data["sections"][RISK_SECTION_INDEX]["worsened"]
    risk_worsened_diverged = _assert_text_list_diverges_only_by_numeric_token(
        risk_worsened_python, risk_worsened_go, list_label="sections[risk].worsened"
    )

    reliability_worsened_python = python_data["sections"][RELIABILITY_SECTION_INDEX][
        "worsened"
    ]
    reliability_worsened_go = go_data["sections"][RELIABILITY_SECTION_INDEX]["worsened"]
    reliability_worsened_diverged = _assert_text_list_diverges_only_by_numeric_token(
        reliability_worsened_python,
        reliability_worsened_go,
        list_label="sections[reliability].worsened",
    )

    # ENUMERATED expected-divergence list, path by path, each with its
    # reason: the sentence template (deltaSummary/_delta_summary) renders
    # this guarded metric's delta.absolute into a %+.1f-formatted token,
    # so Python's NaN-derived text and Go's finite-value text differ only
    # in that token. (label, python_list, go_list, diverged_indices,
    # json_path_prefix, reason).
    named_text_divergences = (
        (
            "Hotspot risk",
            python_recommendations,
            go_recommendations,
            recommendations_diverged,
            "$.data.operatingReview.recommendations",
            "renders hotspot_risk_score's delta.absolute",
        ),
        (
            "Ownership concentration",
            python_recommendations,
            go_recommendations,
            recommendations_diverged,
            "$.data.operatingReview.recommendations",
            "renders ownership_concentration's delta.absolute",
        ),
        (
            "Complexity",
            python_recommendations,
            go_recommendations,
            recommendations_diverged,
            "$.data.operatingReview.recommendations",
            "renders complexity_per_kloc's delta.absolute",
        ),
        (
            "Change failure rate",
            python_recommendations,
            go_recommendations,
            recommendations_diverged,
            "$.data.operatingReview.recommendations",
            "renders change_failure_rate's delta.absolute",
        ),
        (
            "Hotspot risk",
            risk_worsened_python,
            risk_worsened_go,
            risk_worsened_diverged,
            f"$.data.operatingReview.sections[{RISK_SECTION_INDEX}].worsened",
            "renders hotspot_risk_score's delta.absolute",
        ),
        (
            "Ownership concentration",
            risk_worsened_python,
            risk_worsened_go,
            risk_worsened_diverged,
            f"$.data.operatingReview.sections[{RISK_SECTION_INDEX}].worsened",
            "renders ownership_concentration's delta.absolute",
        ),
        (
            "Complexity",
            risk_worsened_python,
            risk_worsened_go,
            risk_worsened_diverged,
            f"$.data.operatingReview.sections[{RISK_SECTION_INDEX}].worsened",
            "renders complexity_per_kloc's delta.absolute",
        ),
        (
            "Change failure rate",
            reliability_worsened_python,
            reliability_worsened_go,
            reliability_worsened_diverged,
            f"$.data.operatingReview.sections[{RELIABILITY_SECTION_INDEX}].worsened",
            "renders change_failure_rate's delta.absolute",
        ),
    )

    allowed_text_paths: set[str] = set()
    for (
        label,
        python_entries,
        go_entries,
        diverged_indices,
        path_prefix,
        reason,
    ) in named_text_divergences:
        index = _index_containing(python_entries, label)
        assert index == _index_containing(go_entries, label), (
            f"{label!r} entry moved position between Python and Go in "
            f"{path_prefix} -- a structural (ordering) divergence, not "
            f"this guard's consequence ({reason})"
        )
        assert index in diverged_indices, (
            f"expected the {label!r} entry in {path_prefix} (index "
            f"{index}) to diverge -- {reason}; if this fails, "
            f"CHAOS-4563's guard stopped perturbing this metric's "
            f"delta.absolute: python={python_entries[index]!r} "
            f"go={go_entries[index]!r}"
        )
        # codex R2 P2: numeric-token normalization alone (above) only
        # proves "some number differs from nan" -- Go rendering an
        # outright WRONG number (e.g. +0.6 instead of the real +0.4)
        # would still pass that check, since both collapse to the same
        # placeholder. Extract the actual tokens and verify EXACTLY:
        # Python's is nan, Go's equals this metric's own proven
        # delta.absolute (asserted exactly above, reused here).
        python_token = _extract_numeric_token(python_entries[index])
        go_token = _extract_numeric_token(go_entries[index])
        assert _is_nan_token(python_token), (
            f"expected Python's {label!r} entry in {path_prefix} to "
            f"render a nan token, got {python_token!r} in "
            f"{python_entries[index]!r}"
        )
        expected_absolute = expected_absolute_by_label[label]
        # The rendered TEXT surface and the raw JSON delta object are two
        # DIFFERENT surfaces and get two DIFFERENT assertions, deliberately:
        # the JSON assertion above (_expected_guarded_delta ==
        # metric["delta"]) compares the RAW, full-precision float and is
        # what actually pins the value; this one compares the RENDERED
        # token, because that is the strongest true claim available at a
        # text surface. Comparing the extracted token against the raw
        # unrounded `expected_absolute` is not a stricter version of that
        # claim -- it is an IMPOSSIBLE one that fails for any value not
        # already exact to one decimal (this was live-caught: it passed
        # for hotspot_risk_score=0.4, ownership_concentration=0.6, and
        # complexity_per_kloc=12.5, and only change_failure_rate's
        # 1/9-shaped 0.111... value exposed the bug in the assertion
        # itself, not in the guard). `%+.1f` is hardcoded here rather than
        # imported from deltaSummary/_delta_summary's format spec on
        # purpose: if production ever changes to `%+.2f`, that is a
        # wire-visible behaviour change this test SHOULD fail on, which
        # importing the constant would silently swallow. Residual: this
        # text check alone cannot distinguish two values that round to the
        # same one-decimal token (e.g. 0.1111 and 0.1149 both render
        # "+0.1") -- that is a property of the rendered surface, not a gap
        # introduced here, and it is exactly what the raw-float JSON
        # assertion above closes.
        expected_token = f"{expected_absolute:+.1f}"
        assert go_token == expected_token, (
            f"expected Go's {label!r} entry in {path_prefix} to render "
            f"its delta.absolute at the sentence's actual precision "
            f"({expected_token!r}, from the raw {expected_absolute!r}), "
            f"got {go_token!r} in {go_entries[index]!r} -- a wrong "
            f"rendered number would otherwise still pass the "
            f"numeric-token-only mechanism check above"
        )
        allowed_text_paths.add(f"{path_prefix}[{index}]")

    # No UNNAMED index differs -- every divergence in these three lists
    # is one of the eight enumerated, justified entries above.
    assert recommendations_diverged == {
        _index_containing(python_recommendations, label)
        for label in (
            "Hotspot risk",
            "Ownership concentration",
            "Complexity",
            "Change failure rate",
        )
    }, (
        f"recommendations diverged at unnamed indices too: "
        f"{recommendations_diverged}, expected only the four enumerated "
        f"guarded-metric entries"
    )
    assert risk_worsened_diverged == {
        _index_containing(risk_worsened_python, label)
        for label in ("Hotspot risk", "Ownership concentration", "Complexity")
    }, (
        f"sections[risk].worsened diverged at unnamed indices too: "
        f"{risk_worsened_diverged}, expected only the three enumerated "
        f"guarded-metric entries"
    )
    assert reliability_worsened_diverged == {
        _index_containing(reliability_worsened_python, "Change failure rate")
    }, (
        f"sections[reliability].worsened diverged at unnamed indices "
        f"too: {reliability_worsened_diverged}, expected only the one "
        f"enumerated change_failure_rate entry"
    )

    # Now the comparator's own findings: confined EXACTLY to the four
    # guarded metrics' delta sub-objects, plus the eight named,
    # individually-justified recommendations/worsened-list paths above --
    # never a blanket allowance for either list. A MATCH would mean
    # CHAOS-4563's guard did not take effect; a stray mismatch anywhere
    # else means the guard (or this fixture) touched something it should
    # not have.
    stray = [
        f
        for f in mismatch_findings
        if not f.path.startswith(GUARDED_DELTA_PATH_PREFIXES)
        and f.path not in allowed_text_paths
    ]
    assert not stray, (
        f"mismatch findings outside the four guarded metrics' delta "
        f"sub-objects and the exact recommendations/worsened-list "
        f"indices already proven to differ only by a numeric token -- "
        f"the guard (or this fixture) leaked into fields it should not "
        f"have touched: {stray}"
    )

    covered = {
        prefix: any(
            f.path == f"{prefix}.{REQUIRED_GUARDED_SUFFIX}" for f in mismatch_findings
        )
        for prefix in GUARDED_DELTA_PATH_PREFIXES
    }
    assert all(covered.values()), (
        f"expected ALL FOUR guarded metrics to actually diverge at "
        f"delta.{REQUIRED_GUARDED_SUFFIX} (the one sub-field guaranteed "
        f"to differ regardless of this fixture's concrete numbers -- "
        f"Python's NaN never equals Go's finite 0.0), got "
        f"coverage={covered} findings={mismatch_findings}"
    )
