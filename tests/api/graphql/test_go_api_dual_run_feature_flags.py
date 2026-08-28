"""CHAOS-4367 Wave 1 stage-2 proof: local dual-run of featureFlags.

Plan §5 stage 2 ("local dual-run proof"): real Python and Go servers
against the same producer-seeded scratch ClickHouse/Postgres state,
comparing the complete observable response via the CHAOS-4381 comparator
(``go_api_comparator.compare_responses``) -- not merely "both return 200".

Side effects (plan §5 stage 2 also requires asserting these): checked by
reading ``resolve_feature_flags``
(``src/dev_health_ops/api/graphql/resolvers/feature_flags.py:71``) top to
bottom. It runs two read-only ClickHouse queries via ``query_dicts`` and
constructs a ``FeatureFlagRegistryResult`` dataclass; nothing else. No
telemetry/audit hook call exists inside it or anything it calls (unlike
``home``/investment analytics, which the plan calls out by name for
calling ``record_stale_investment_membership_scope``). There is therefore
no side-effect digest to assert alongside the response digest for this
operation -- this is a positive finding (verified, not assumed), not an
omission.

Producer note (write down what was actually used, per root AGENTS.md's
"an inaccurate coverage claim is worse than an admitted gap"): there is no
``dev-hops fixtures generate`` producer for the ClickHouse ``feature_flag``
table (no provider/normalizer exists for it in this codebase yet -- flag
data arrives only via a future LaunchDarkly-shaped provider that has not
been built). The existing live-ClickHouse precedent for this exact table,
``tests/graphql/test_feature_flags_live.py``, seeds via
``ClickHouseMetricsSink.client.insert(...)`` -- the real sink entry point
(root AGENTS.md: "sinks only for persistence"), not hand-authored JSON
fed to a mock. This test reuses that exact seeding path rather than
inventing a second one. When a real feature-flag provider lands, this
test's seeding should move onto it.
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

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine, make_url

from dev_health_ops.api.graphql import go_api_comparator, principal_envelope
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.feature_flags import (
    FEATURE_FLAG_NOT_MATERIALIZED,
    resolve_feature_flags,
)
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

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
        reason="Requires DEV_HEALTH_POSTGRES_TEST_URI for the go_api_routing_state switch",
    ),
]

REPO_ROOT = Path(__file__).resolve().parents[3]

# Byte-identical to cmd/query-api/query_route.go's
# registeredFeatureFlagsDocument -- the dual-run harness sends this exact
# document to BOTH sides, so any drift between this literal and the Go
# constant is a real bug this test would catch (a document-digest mismatch
# would 404 on the Go side rather than degrade gracefully).
FEATURE_FLAGS_DOCUMENT = """query FeatureFlags($orgId: String!, $provider: String, $project: String, $includeArchived: Boolean, $limit: Int!) {
  featureFlags(orgId: $orgId, provider: $provider, project: $project, includeArchived: $includeArchived, limit: $limit) {
    flags {
      flagId
      flagKey
      provider
      projectKey
      flagType
      createdAt
      archivedAt
    }
    totalCount
    degradedReason
  }
}"""

SCHEMA_DIGEST = "sha256:wave1-dual-run-test-schema-digest"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def query_api_binary(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Builds the real query-api binary once per test module.

    Real binary, not `go run` -- proves the same artifact CI would ship
    actually links and starts, matching this codebase's "a constructor is
    not proof of capability" discipline one layer further: a build that
    only ever ran under `go test` is not evidence the binary itself works.
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
    """Builds a sync SQLAlchemy engine for admin DDL (CREATE/DROP DATABASE,
    which cannot run inside asyncpg's usual transaction context) --
    same postgresql+psycopg2 driver, same AUTOCOMMIT-engine convention
    test_go_api_registry.py's migrated_scratch_db fixture already uses.
    SQLAlchemy ships its own type stubs, so this sidesteps psycopg2's
    missing types-psycopg2 stub package (mypy import-untyped) without
    adding a new dev dependency for a one-off admin connection.
    """
    return sa.create_engine(
        make_url(uri).set(drivername="postgresql+psycopg2"),
        isolation_level="AUTOCOMMIT",
    )


def _create_scratch_postgres_db(admin_uri: str) -> tuple[str, str]:
    db_name = f"chaos_4367_dual_run_{uuid.uuid4().hex}"
    engine = _sync_engine(admin_uri)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
    finally:
        engine.dispose()
    return db_name, admin_uri.rsplit("/", 1)[0] + f"/{db_name}"


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
def registry_postgres() -> Iterator[str]:
    """A scratch Postgres DB holding only go_api_routing_state -- the
    minimal shape PostgresSwitch reads, matching
    cmd/query-api/internal/routeswitch/postgres_switch_integration_test.go's
    own minimal-shape convention rather than running full alembic here.
    """
    assert POSTGRES_TEST_URI is not None
    db_name, dsn = _create_scratch_postgres_db(POSTGRES_TEST_URI)
    _create_routing_state_table(dsn)
    try:
        yield dsn
    finally:
        _drop_scratch_postgres_db(POSTGRES_TEST_URI, db_name)


def _create_routing_state_table(dsn: str) -> None:
    engine = _sync_engine(dsn)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE go_api_routing_state (
                    schema_digest TEXT NOT NULL,
                    document_digest TEXT NOT NULL,
                    selected_operation TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    PRIMARY KEY (schema_digest, document_digest, selected_operation)
                )
                """
            )
    finally:
        engine.dispose()


def _document_digest(document: str) -> str:
    import hashlib

    return hashlib.sha256(document.strip().encode("utf-8")).hexdigest()


def _set_routing_mode(dsn: str, mode: str) -> None:
    engine = _sync_engine(dsn)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                """
                INSERT INTO go_api_routing_state
                    (schema_digest, document_digest, selected_operation, mode)
                VALUES (%s, %s, 'featureFlags', %s)
                ON CONFLICT (schema_digest, document_digest, selected_operation)
                DO UPDATE SET mode = EXCLUDED.mode
                """,
                (SCHEMA_DIGEST, _document_digest(FEATURE_FLAGS_DOCUMENT), mode),
            )
    finally:
        engine.dispose()


def _mint_envelope(org_id: str) -> tuple[str, dict, str, str]:
    """Mints a REAL envelope with the real Python issuer -- the same
    cross-language-real-artifact discipline
    cmd/query-api/internal/principal/live_python_envelope_oracle_test.go
    uses (Go shells to Python for its oracle; this is the same coupling in
    the other direction: Python mints, Go verifies over HTTP).
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
        user_id="11111111-1111-4111-8111-111111111111",
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
        {"query": FEATURE_FLAGS_DOCUMENT, "variables": variables}
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
    """The Go query-api binary and the Python resolver connect to the SAME
    ClickHouse server via different protocols, so they need different
    ports on the same DSN -- not a production bug, a real fact about the
    two client libraries: `clickhouse_connect` (Python,
    metrics/sinks/clickhouse/connection.py's `clickhouse_client_kwargs`)
    is HTTP-only and reads the DSN's port literally (8123 in local dev);
    `github.com/full-chaos/dev-health-go/clickhouse` wraps
    `ClickHouse/clickhouse-go/v2`'s NATIVE protocol client, which fails
    the connection handshake outright against an HTTP port ("[handshake]
    unexpected packet"). Swap 8123 -> 9000 (the container's exposed
    native port, see docker-compose) for the Go side only; the Python
    side keeps using `python_clickhouse_uri` unchanged.
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


def _feature_flags_go_response_snapshot(
    payload: dict,
) -> go_api_comparator.ResponseSnapshot:
    return go_api_comparator.ResponseSnapshot(
        data=payload.get("data"),
        data_present="data" in payload,
        errors=tuple(payload.get("errors") or ()),
    )


def _feature_flags_python_response_snapshot(
    result,
) -> go_api_comparator.ResponseSnapshot:
    """Serializes resolve_feature_flags's return value into the same
    on-the-wire GraphQL response envelope the Go HTTP endpoint produces --
    the comparator compares RESPONSES, not Python dataclasses vs Go
    structs.
    """
    return go_api_comparator.ResponseSnapshot(
        data={
            "featureFlags": {
                "flags": [
                    {
                        "flagId": f.flag_id,
                        "flagKey": f.flag_key,
                        "provider": f.provider,
                        "projectKey": f.project_key,
                        "flagType": f.flag_type,
                        "createdAt": f.created_at,
                        "archivedAt": f.archived_at,
                    }
                    for f in result.flags
                ],
                "totalCount": result.total_count,
                "degradedReason": result.degraded_reason,
            }
        },
        data_present=True,
        errors=(),
    )


@pytest.fixture(scope="module")
def jwks_path(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("jwks")


@pytest.mark.asyncio
async def test_dual_run_happy_path_matches(
    query_api_binary, registry_postgres, jwks_path
):
    """Stage 2: real seeded ClickHouse rows, real Python resolver call,
    real Go HTTP server -- compared via the CHAOS-4381 comparator, not a
    hand-rolled equality check.
    """
    assert CLICKHOUSE_URI is not None
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_schema(force=True)

    org_id = f"chaos-4367-dual-run-{uuid.uuid4()}"
    project_key = f"project-{uuid.uuid4()}"
    flag_key = "checkout-v2"
    created = datetime(2026, 8, 10, 12, 0, 30, 500000, tzinfo=timezone.utc)

    rows = [
        [
            org_id,
            "launchdarkly",
            flag_key,
            project_key,
            "repo-1",
            "production",
            "boolean",
            created,
            None,
            created,
        ],
    ]
    columns = [
        "org_id",
        "provider",
        "flag_key",
        "project_key",
        "repo_id",
        "environment",
        "flag_type",
        "created_at",
        "archived_at",
        "last_synced",
    ]

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks.json"
    jwks_file.write_text(json.dumps(jwks))

    _set_routing_mode(registry_postgres, "canary")

    server = _start_go_server(
        query_api_binary,
        CLICKHOUSE_URI,
        registry_postgres,
        str(jwks_file),
        issuer,
        audience,
    )
    try:
        sink.client.insert("feature_flag", rows, column_names=columns)
        sink.client.command("OPTIMIZE TABLE feature_flag FINAL")

        python_result = await resolve_feature_flags(
            GraphQLContext(org_id=org_id, db_url=CLICKHOUSE_URI, client=sink.client),
            provider="launchdarkly",
            project=project_key,
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "orgId": org_id,
                "provider": "launchdarkly",
                "project": project_key,
                "includeArchived": False,
                "limit": 1000,
            },
        )
    finally:
        server.stop()
        sink.client.command(
            "ALTER TABLE feature_flag DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )
        sink.close()

    assert python_result.total_count == 1, (
        "producer-seeded row did not reach the Python resolver"
    )
    assert "errors" not in go_payload, f"Go response carried errors: {go_payload}"

    baseline = _feature_flags_python_response_snapshot(python_result)
    candidate = _feature_flags_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)

    assert comparison.is_match, (
        f"featureFlags dual-run MISMATCH: terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}\npython={baseline}\ngo={candidate}"
    )


@pytest.mark.asyncio
async def test_dual_run_missing_table_degrades_on_both_sides(
    tmp_path_factory, query_api_binary, jwks_path
):
    """The real non-happy-path this operation was chosen as the canary to
    exercise (CHAOS-4367 lane brief): a fresh, unmigrated scratch
    ClickHouse (feature_flag table does not exist) must degrade
    identically on both sides -- FEATURE_FLAG_NOT_MATERIALIZED, empty
    flags, zero total -- never an error response on either side.
    """
    assert CLICKHOUSE_URI is not None
    assert POSTGRES_TEST_URI is not None
    # A fresh scratch DB name under the same server -- no migration run,
    # so `feature_flag` genuinely does not exist yet.
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(CLICKHOUSE_URI)
    unmigrated_path = f"/chaos_4367_unmigrated_{uuid.uuid4().hex}"
    unmigrated_uri = urlunsplit(
        (parts.scheme, parts.netloc, unmigrated_path, parts.query, parts.fragment)
    )

    org_id = f"chaos-4367-degraded-{uuid.uuid4()}"

    import clickhouse_connect

    admin_client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    db_name = unmigrated_path.lstrip("/")
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

    registry_db_name, registry_dsn = _create_scratch_postgres_db(POSTGRES_TEST_URI)
    _create_routing_state_table(registry_dsn)
    _set_routing_mode(registry_dsn, "canary")

    token, jwks, issuer, audience = _mint_envelope(org_id)
    jwks_file = jwks_path / "jwks-degraded.json"
    jwks_file.write_text(json.dumps(jwks))

    server = _start_go_server(
        query_api_binary, unmigrated_uri, registry_dsn, str(jwks_file), issuer, audience
    )
    try:
        python_client = clickhouse_connect.get_client(dsn=unmigrated_uri)
        python_result = await resolve_feature_flags(
            GraphQLContext(org_id=org_id, db_url=unmigrated_uri, client=python_client),
        )
        go_payload = _post_graphql(
            server.base_url,
            token,
            {
                "orgId": org_id,
                "provider": None,
                "project": None,
                "includeArchived": False,
                "limit": 1000,
            },
        )
    finally:
        server.stop()
        admin_client.command(f"DROP DATABASE IF EXISTS `{db_name}`")
        _drop_scratch_postgres_db(POSTGRES_TEST_URI, registry_db_name)

    assert python_result.degraded_reason == FEATURE_FLAG_NOT_MATERIALIZED
    assert python_result.flags == []
    assert python_result.total_count == 0

    go_flags = go_payload.get("data", {}).get("featureFlags", {})
    assert go_flags.get("degradedReason") == FEATURE_FLAG_NOT_MATERIALIZED, go_payload
    assert go_flags.get("flags") == []
    assert go_flags.get("totalCount") == 0
    assert "errors" not in go_payload, (
        f"Go response carried errors instead of degrading: {go_payload}"
    )

    baseline = _feature_flags_python_response_snapshot(python_result)
    candidate = _feature_flags_go_response_snapshot(go_payload)
    comparison = go_api_comparator.compare_responses(baseline, candidate)
    assert comparison.is_match, (
        f"degraded-path dual-run MISMATCH: terminal_state={comparison.terminal_state} "
        f"findings={comparison.findings}"
    )
