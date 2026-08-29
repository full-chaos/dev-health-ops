"""CHAOS-4369 Wave 3 stage-2 proof: local dual-run of cognitiveLoad.

Plan §5 stage 2 ("local dual-run proof"): real Python and Go servers
against the same producer-seeded scratch ClickHouse/Postgres state,
comparing the complete observable response via the CHAOS-4381 comparator
(``go_api_comparator.compare_responses``) -- not merely "both return 200".

``resolve_cognitive_load`` picks between TWO distinct read paths on
``teamId``/``repoId`` AT THE ACTUAL FEATURE-BRANCH TIP (see its own
docstring and this port's Go package doc comment,
``cmd/query-api/internal/cognitiveload/cognitiveload.go``); this file
seeds and dual-runs every branch the Python resolver distinguishes:

1. Single-team (``teamId`` set, ``repoId`` unset): a direct read of
   ``team_cognitive_load_daily``.
2. Org-wide (no ``teamId``) OR team+repo combined (both set): the SAME
   two-query merge over ``user_metrics_daily``/``team_metrics_daily`` --
   ``repoId``, when set, narrows only ``user_metrics_daily``.

**Finding, not an assumption (verified, not trusted from the task
briefing):** the CHAOS-4369 lane briefing claimed CHAOS-4406's
ownership-gated team+repo-combined path (commit ``8519cd2a8`` -- a THIRD
branch resolving ``team_repo_ownership``/``teams.repo_patterns`` before
filtering by ``repo_id`` alone) was "already in the feature base". This
is FALSE: ``git merge-base --is-ancestor 8519cd2a8
origin/feature/chaos-4352-go-api`` returns non-zero (not an ancestor) --
that commit exists only on ``origin/main``. Confirmed by reading
``src/dev_health_ops/api/graphql/resolvers/cognitive_load.py`` directly in
this worktree (437 lines, two branches, ends at commit ``4795fc4e2``).
This file dual-runs the Python that ACTUALLY exists at the feature-branch
tip, not the (different, newer) behavior described in the briefing.

Side effects: checked by reading
``resolve_cognitive_load``/its private ``_fetch_*`` helpers
(``src/dev_health_ops/api/graphql/resolvers/cognitive_load.py``) top to
bottom. Every branch is one or more read-only ClickHouse queries and a
dataclass construction; no telemetry/audit hook call exists inside it or
anything it calls. There is therefore no side-effect digest to assert
alongside the response digest for this operation.

Producer note (root AGENTS.md: "fixtures are producer-derived", "an
inaccurate coverage claim is worse than an admitted gap"): this test
writes through the REAL sink entry points --
``ClickHouseMetricsSink.write_user_metrics``/``write_team_metrics``/
``write_team_cognitive_load_daily`` (``metrics/sinks/clickhouse/*.py``) --
using the real ``UserMetricsDailyRecord``/``TeamMetricsDailyRecord``/
``TeamCognitiveLoadDailyRecord`` dataclasses, not hand-authored dicts fed
to ``client.insert`` directly.
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
from dev_health_ops.api.graphql.resolvers.cognitive_load import resolve_cognitive_load
from dev_health_ops.api.graphql.types.cognitive_load import CognitiveLoadInput
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.fixtures.world import _require_scratch_database
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.schemas import (
    TeamCognitiveLoadDailyRecord,
    TeamMetricsDailyRecord,
    UserMetricsDailyRecord,
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
# registeredCognitiveLoadDocument, which is itself byte-identical to the
# REAL production query (web/src/lib/graphql/queries.ts's
# COGNITIVE_LOAD_QUERY, operation name "CognitiveLoad"). Sourced from the
# real client file directly (Wave-1 codex-round-3 lesson), not
# reconstructed from the SDL or an inventory doc.
COGNITIVE_LOAD_DOCUMENT = """query CognitiveLoad($input: CognitiveLoadInput!) {
  cognitiveLoad(input: $input) {
    orgId
    teamId
    totalDays
    signals {
      day
      prInterruptionLoad
      contextSpreadCount
      reviewRequestLoad
      afterHoursCommitRatio
      weekendCommitRatio
    }
  }
}"""

SCHEMA_DIGEST = "sha256:wave3-cognitive-load-dual-run-test-schema-digest"
CANDIDATE_BUILD = "wave3-cognitive-load-dual-run-test-build"


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
    (``CandidateBuild``/``RoutingState``/``ProofRun`` -- alembic 0114,
    ``dev_health_ops.models.go_api_registry``) -- same shape as the
    reviewEdges dual-run test's fixture of the same name, giving real
    CANDIDATE_BUILD/ROUTING_STATE/PROOF_RUN proof-ledger rows.
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
                selected_operation="cognitiveLoad",
                candidate_build=CANDIDATE_BUILD,
            )
            await session.execute(
                pg_insert(RoutingState)
                .values(
                    schema_digest=SCHEMA_DIGEST,
                    document_digest=document_digest,
                    selected_operation="cognitiveLoad",
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
                selected_operation="cognitiveLoad",
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
        {"query": COGNITIVE_LOAD_DOCUMENT, "variables": variables}
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
    """Same native-vs-HTTP-port translation as the featureFlags/reviewEdges
    dual-run tests' helper of the same name."""
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


def _cognitive_load_go_response_snapshot(
    payload: dict,
) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _cognitive_load_python_response_snapshot(
    result,
) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_cognitive_load's return value into the same
    on-the-wire GraphQL response envelope the Go HTTP endpoint produces --
    the comparator compares RESPONSES, not Python dataclasses vs Go
    structs.
    """
    return go_api_comparator.ResponseSnapshot(
        data={
            "cognitiveLoad": {
                "orgId": result.org_id,
                "teamId": result.team_id,
                "totalDays": result.total_days,
                "signals": [
                    {
                        "day": s.day.isoformat(),
                        "prInterruptionLoad": s.pr_interruption_load,
                        "contextSpreadCount": s.context_spread_count,
                        "reviewRequestLoad": s.review_request_load,
                        "afterHoursCommitRatio": s.after_hours_commit_ratio,
                        "weekendCommitRatio": s.weekend_commit_ratio,
                    }
                    for s in result.signals
                ],
            }
        },
        data_present=True,
        errors=(),
    )


@pytest.fixture(scope="module")
def jwks_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("jwks")


def _sink() -> ClickHouseMetricsSink:
    assert CLICKHOUSE_URI is not None
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)
    return sink


def _write_repo(
    sink: ClickHouseMetricsSink,
    *,
    org_id: str,
    repo_id: uuid.UUID,
    full_name: str,
    provider: str = "github",
) -> None:
    """Seeds one ``repos`` catalog row -- required for the repoId filter
    to resolve at all: ``_fetch_user_metrics``'s repo_id predicate is
    ``repo_id IN (SELECT id FROM repos WHERE org_id = ... AND (repo = ...
    OR toString(id) = ...))``, so an unseeded repos table makes that
    subquery match nothing regardless of what repo_id a user_metrics_daily
    row itself carries. Same precedent
    ``tests/providers/test_load_team_repo_ownership_map_live.py`` already
    established for this exact table (no compute-producer exists for a
    synced git-catalog row).
    """
    now = datetime.now(timezone.utc)
    sink.client.insert(
        "repos",
        [[repo_id, full_name, "main", now, None, None, now, org_id, provider, None]],
        column_names=[
            "id",
            "repo",
            "ref",
            "created_at",
            "settings",
            "tags",
            "last_synced",
            "org_id",
            "provider",
            "source_id",
        ],
    )


def _cleanup_org_rows(sink: ClickHouseMetricsSink, org_id: str) -> None:
    for table in (
        "user_metrics_daily",
        "team_metrics_daily",
        "team_cognitive_load_daily",
        "repos",
    ):
        sink.client.command(
            f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )


# ---------------------------------------------------------------------------
# Path 1: org-wide (no teamId)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dual_run_org_wide_merges_user_and_team_metrics(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2, org-wide path: real sink-written user_metrics_daily +
    team_metrics_daily rows, real Python resolver call, real Go HTTP
    server -- compared via the CHAOS-4381 comparator.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = _sink()

    org_id = f"chaos-4369-dual-run-orgwide-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    day1 = date(2026, 8, 10)
    day2 = date(2026, 8, 11)
    author = f"author-{uuid.uuid4().hex[:8]}@example.com"
    computed_at = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)

    sink.write_user_metrics(
        [
            UserMetricsDailyRecord(
                repo_id=repo_id,
                day=day1,
                author_email=author,
                commits_count=3,
                loc_added=100,
                loc_deleted=20,
                files_changed=5,
                large_commits_count=0,
                avg_commit_size_loc=40.0,
                prs_authored=1,
                prs_merged=1,
                avg_pr_cycle_hours=4.0,
                median_pr_cycle_hours=4.0,
                computed_at=computed_at,
                pr_interruption_load=4,
                context_spread_count=2,
                review_request_load=1,
                org_id=org_id,
            )
        ]
    )
    # A weekend day with commit-timing data but no per-developer load row --
    # the union-of-days merge must still emit it (day2 only from the team
    # side, mirroring the mocked resolver test's own regression pin).
    sink.write_team_metrics(
        [
            TeamMetricsDailyRecord(
                day=day2,
                team_id="team-unused-org-wide",
                team_name="Team Unused",
                commits_count=10,
                after_hours_commits_count=2,
                weekend_commits_count=5,
                after_hours_commit_ratio=0.2,
                weekend_commit_ratio=0.5,
                computed_at=computed_at,
                org_id=org_id,
                repo_id=str(repo_id),
            )
        ]
    )

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-orgwide.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(COGNITIVE_LOAD_DOCUMENT)
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
        python_result = await resolve_cognitive_load(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            CognitiveLoadInput(
                org_id=org_id,
                since_date=day1,
                until_date=day2,
                team_id=None,
                repo_id=None,
            ),
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "input": {
                    "orgId": org_id,
                    "sinceDate": day1.isoformat(),
                    "untilDate": day2.isoformat(),
                    "teamId": None,
                    "repoId": None,
                }
            },
        )
    finally:
        server.stop()
        _cleanup_org_rows(sink, org_id)
        sink.close()

    assert python_result.total_days == 2, (
        "expected the union of days from both source tables"
    )
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _cognitive_load_python_response_snapshot(python_result)
    candidate = _cognitive_load_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"cognitiveLoad org-wide dual-run MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )
    assert await _proof_run_count(registry_postgres["async"]) >= 1


# ---------------------------------------------------------------------------
# Path 2: single-team (teamId set, repoId unset) -- team_cognitive_load_daily
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dual_run_single_team_reads_team_cognitive_load_daily(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2, single-team path: a real team_cognitive_load_daily row,
    including a genuinely NULL weekend_commit_ratio (CHAOS-4365's own
    Codex R1 nullable-tuple fix) -- proving null-vs-zero parity holds
    end-to-end, not just in the mocked resolver unit test.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = _sink()

    org_id = f"chaos-4369-dual-run-singleteam-{uuid.uuid4()}"
    team_id = "team-alpha"
    day = date(2026, 8, 10)
    computed_at = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    sink.write_team_cognitive_load_daily(
        [
            TeamCognitiveLoadDailyRecord(
                team_id=team_id,
                day=day,
                pr_interruption_load=4.0,
                context_spread_count=2.0,
                review_request_load=1.0,
                after_hours_commit_ratio=0.25,
                weekend_commit_ratio=None,
                contributing_repo_count=1,
                sample_author_count=1,
                computed_at=computed_at,
                org_id=org_id,
            )
        ]
    )

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-singleteam.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(COGNITIVE_LOAD_DOCUMENT)
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
        python_result = await resolve_cognitive_load(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            CognitiveLoadInput(
                org_id=org_id,
                since_date=day,
                until_date=day,
                team_id=team_id,
                repo_id=None,
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
                    "teamId": team_id,
                    "repoId": None,
                }
            },
        )
    finally:
        server.stop()
        _cleanup_org_rows(sink, org_id)
        sink.close()

    assert python_result.total_days == 1
    assert python_result.signals[0].weekend_commit_ratio is None, (
        "seeded row's weekend_commit_ratio is genuinely NULL -- must stay "
        "None, never default to 0.0"
    )
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _cognitive_load_python_response_snapshot(python_result)
    candidate = _cognitive_load_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"cognitiveLoad single-team dual-run MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )


# ---------------------------------------------------------------------------
# Path 2b: team+repo combined uses the SAME merge path as org-wide
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dual_run_team_repo_combined_uses_the_same_merge_path(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2, team+repo combined sub-case: at the feature-branch tip
    (see the module docstring's finding), team+repo combined is NOT a
    third branch -- it falls through to the identical two-query merge as
    the org-wide path, with user_metrics_daily filtered by BOTH team_id
    AND repo_id (the tainted team_id column CHAOS-4396/CHAOS-4406 flag on
    ``main``, unchanged here) and team_metrics_daily filtered by team_id
    alone (no repo_id argument at all). This proves Go reproduces that
    ACTUAL behavior, not the newer ownership-gated behavior a different
    branch has.
    """
    assert CLICKHOUSE_URI is not None
    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    sink = _sink()

    org_id = f"chaos-4369-dual-run-combined-{uuid.uuid4()}"
    team_id = "team-alpha"
    repo_id = uuid.uuid4()
    repo_full_name = f"acme/repo-{uuid.uuid4().hex[:8]}"
    day = date(2026, 8, 10)
    author = f"author-{uuid.uuid4().hex[:8]}@example.com"
    computed_at = datetime(2026, 8, 11, 0, 0, 0, tzinfo=timezone.utc)

    # Required for the repoId filter to resolve at all -- see
    # _write_repo's own doc comment.
    _write_repo(sink, org_id=org_id, repo_id=repo_id, full_name=repo_full_name)
    sink.write_user_metrics(
        [
            UserMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                author_email=author,
                commits_count=2,
                loc_added=50,
                loc_deleted=10,
                files_changed=3,
                large_commits_count=0,
                avg_commit_size_loc=25.0,
                prs_authored=1,
                prs_merged=1,
                avg_pr_cycle_hours=2.0,
                median_pr_cycle_hours=2.0,
                computed_at=computed_at,
                pr_interruption_load=5,
                context_spread_count=3,
                review_request_load=2,
                team_id=team_id,
                org_id=org_id,
            )
        ]
    )
    sink.write_team_metrics(
        [
            TeamMetricsDailyRecord(
                day=day,
                team_id=team_id,
                team_name="Team Alpha",
                commits_count=4,
                after_hours_commits_count=1,
                weekend_commits_count=0,
                after_hours_commit_ratio=0.25,
                weekend_commit_ratio=0.0,
                computed_at=computed_at,
                org_id=org_id,
                repo_id=str(repo_id),
            )
        ]
    )

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-combined.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(COGNITIVE_LOAD_DOCUMENT)
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
        python_result = await resolve_cognitive_load(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            CognitiveLoadInput(
                org_id=org_id,
                since_date=day,
                until_date=day,
                team_id=team_id,
                repo_id=repo_full_name,
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
                    "teamId": team_id,
                    "repoId": repo_full_name,
                }
            },
        )
    finally:
        server.stop()
        _cleanup_org_rows(sink, org_id)
        sink.close()

    assert python_result.total_days == 1, (
        f"expected the team+repo combined merge to surface the seeded day: {python_result}"
    )
    assert python_result.signals[0].pr_interruption_load == pytest.approx(5.0)
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _cognitive_load_python_response_snapshot(python_result)
    candidate = _cognitive_load_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state=comparison.terminal_state,
        org_id=org_id,
    )

    assert comparison.is_match, (
        f"cognitiveLoad team+repo combined dual-run MISMATCH: "
        f"terminal_state={comparison.terminal_state} findings={comparison.findings}\n"
        f"python={baseline}\ngo={candidate}"
    )


# ---------------------------------------------------------------------------
# Missing-table error path (no degraded path on either side)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dual_run_missing_table_errors_on_both_sides_no_degraded_path(
    query_api_binary, jwks_path, registry_postgres
):
    """Like reviewEdges (unlike featureFlags), resolve_cognitive_load has
    NO missing-table degraded path -- a genuinely missing table must
    surface as a real error on BOTH sides, never a well-formed empty
    success. Asserts "both sides error", not a comparator MATCH on error
    content (Strawberry's serialization of an unhandled exception is
    intentionally generic -- see the reviewEdges dual-run test's identical
    test for the full rationale).
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
    day = date(2026, 8, 10)

    import clickhouse_connect

    admin_client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    db_name = unmigrated_path.lstrip("/")
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-missing-table.json"
    jwks_file.write_text(json.dumps(jwks))

    document_digest = _document_digest(COGNITIVE_LOAD_DOCUMENT)
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
            await resolve_cognitive_load(
                GraphQLContext(
                    org_id=org_id, db_url=unmigrated_uri, client=python_client
                ),
                CognitiveLoadInput(
                    org_id=org_id,
                    since_date=day,
                    until_date=day,
                    team_id=None,
                    repo_id=None,
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
                    "teamId": None,
                    "repoId": None,
                }
            },
        )
    finally:
        server.stop()
        admin_client.command(f"DROP DATABASE IF EXISTS `{db_name}`")

    assert python_raised, (
        "resolve_cognitive_load did not raise against a missing table -- "
        "cognitive_load.py has no degraded-result path, so a missing table "
        "must be a real error, not a silent empty result"
    )
    assert "errors" in go_payload, (
        f"Go response did not carry an error for a missing table (no degraded "
        f"path exists for cognitiveLoad on either side): {go_payload}"
    )

    await _record_dual_run_proof(
        registry_postgres["async"],
        document_digest=document_digest,
        terminal_state="dependency_failed",
        org_id=org_id,
    )
