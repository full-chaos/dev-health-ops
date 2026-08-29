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

THIS OPERATION HAS A DECLARED, INTENTIONAL DIVERGENCE (CHAOS-4505; see
``cmd/query-api/internal/operatingreview/operatingreview.go``'s
``fetchAIGovernance`` doc comment for the full defect/fix/blast-radius
writeup): Python's ``ai_governance`` query
(``src/dev_health_ops/metrics/operating_review.py:343-367``) selects four
columns that do not exist in ``ai_governance_coverage_daily``
(migration ``038_ai_governance.sql``); the query cannot execute, is
swallowed by ``resolvers/operating_review.py:66-96``'s per-table
try/except, and ``ai_governance_coverage``/``ai_opportunity_signals`` are
pinned to 0.0 in EVERY real Python response. The Go port fixes this (see
that doc comment for the exact ratio semantics ported from
``AIGovernanceCoverageDaily``, ``audit/ai_governance/models.py:125-158``).
So there are TWO tests here, not one:

  1. ``test_dual_run_happy_path_matches_with_no_ai_governance_data`` -- the
     NINE correctly-ported tables get real producer-seeded data;
     ``ai_governance_coverage_daily`` is left EMPTY. With no rows on
     either side, Python's swallow and Go's fix both compute exactly 0.0
     for ``ai_governance_coverage``/``ai_opportunity_signals`` -- the
     WHOLE response matches, proving the nine verbatim ports end to end
     with no divergence in play at all.
  2. ``test_dual_run_ai_governance_fix_is_a_declared_divergence`` -- the
     SAME nine tables, PLUS real ``ai_governance_coverage_daily`` data.
     Python is STILL pinned to 0.0 (the query still cannot execute); Go
     now computes a real, non-zero ratio. This test asserts the
     comparator finds a MISMATCH, and that EVERY mismatch finding's path
     is confined to the two known metric slots
     (``sections[5].metrics[4]`` = ai_governance_coverage,
     ``sections[5].metrics[5]`` = ai_opportunity_signals, per
     ``computeReview``'s fixed section/metric order) -- proving the
     divergence is exactly as wide as declared, not wider. A comparator
     MATCH in this second test would mean the Go fix did not take
     effect and would itself be the bug.

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
``test_go_api_dual_run_hotspots.py`` has one; a missing table is exactly
what test 1 above already exercises for nine of the ten tables (an absent
``ai_governance_coverage_daily`` row set), and it is a SUCCESS path on
both sides by design, not an error path.
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
async def test_dual_run_happy_path_matches_with_no_ai_governance_data(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2, the nine verbatim-ported tables: real fixture rows through
    the real sink entry points, real Python resolver call, real Go HTTP
    server -- compared via the CHAOS-4381 comparator. No
    ai_governance_coverage_daily rows are seeded (see module doc comment)
    so this is a full end-to-end MATCH proof, untouched by the declared
    divergence.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4505-operating-review-dual-run-{uuid.uuid4()}"
    team_id = "team-1"
    repo_id = uuid.uuid4()
    week_start = date(2026, 8, 24)
    day_ = date(2026, 8, 25)
    computed_at = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-operating-review-happy.json"
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

    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"
    assert python_review.org_id == org_id

    # Sanity: the seeded work_items row actually reached the Python
    # resolver (proves the fixture path, not just an empty-both-sides
    # false match).
    delivery = _section(python_review, "delivery_movement")
    assert _metric(delivery, "throughput").value == 8, (
        "seeded items_completed did not reach the Python resolver"
    )
    # ai_governance_coverage stays exactly 0.0 on the Python side with no
    # rows seeded -- Go's fix over an empty row set also computes exactly
    # 0.0 (aiGovernanceCoverage(nil) == 0.0, proven in unit tests), so this
    # test should be a clean, unqualified MATCH.
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
        f"tables (should be a clean match with no ai_governance data): "
        f"terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )

    assert await _proof_run_count(registry_postgres["async"]) >= 1


@pytest.mark.asyncio
async def test_dual_run_ai_governance_fix_is_a_declared_divergence(
    query_api_binary, registry_postgres, jwks_path
):
    """The declared-divergence proof (CHAOS-4505): same nine tables PLUS
    real ai_governance_coverage_daily data. Python is STILL pinned to
    0.0 for ai_governance_coverage/ai_opportunity_signals (its query
    still cannot execute against the real schema -- this test also
    proves that live, not by assumption); Go computes the real ratio via
    the declared fix. Asserts the comparator finds a MISMATCH confined
    EXACTLY to the two known metric slots -- a comparator MATCH here
    would mean the Go fix did not take effect, which is itself the bug.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4505-operating-review-divergence-{uuid.uuid4()}"
    team_id = "team-1"
    repo_id = uuid.uuid4()
    week_start = date(2026, 8, 24)
    day_ = date(2026, 8, 25)
    computed_at = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)

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
            day_=day_,
            computed_at=computed_at,
        )
        _seed_ai_governance(
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

    covered = {
        prefix: any(f.path.startswith(prefix) for f in mismatch_findings)
        for prefix in EXPECTED_DIVERGENCE_PATH_PREFIXES
    }
    assert all(covered.values()), (
        f"expected BOTH declared-divergence metrics to actually diverge, "
        f"got coverage={covered} findings={mismatch_findings}"
    )
