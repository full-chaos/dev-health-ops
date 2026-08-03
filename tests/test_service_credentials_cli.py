from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url

from dev_health_ops import cli, service_credentials
from dev_health_ops.db import normalize_sync_postgres_uri
from dev_health_ops.models import Base
from dev_health_ops.models.internal_service_credential import InternalServiceCredential
from dev_health_ops.models.users import User

_POSTGRES_TEST_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"


def _require_postgres_test_uri() -> None:
    if os.getenv(_POSTGRES_TEST_URI_ENV):
        return
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.fail(f"{_POSTGRES_TEST_URI_ENV} must be configured for PostgreSQL tests")
    pytest.skip(f"requires {_POSTGRES_TEST_URI_ENV}")


@pytest.fixture(scope="module")
def require_postgres_test_uri() -> None:
    _require_postgres_test_uri()


def _postgres_test_uri() -> str:
    _require_postgres_test_uri()
    return os.environ[_POSTGRES_TEST_URI_ENV]


@contextmanager
def _postgres_credential_database(postgres_uri: str) -> Iterator[str]:
    configured_url = make_url(postgres_uri)
    database_name = f"test_service_credentials_{uuid.uuid4().hex}"
    admin_url = configured_url.set(
        drivername="postgresql+psycopg2", database="postgres"
    )
    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    engine = None
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
        test_url = configured_url.set(database=database_name)
        engine = sa.create_engine(
            normalize_sync_postgres_uri(test_url.render_as_string(hide_password=False))
        )
        Base.metadata.create_all(
            engine,
            tables=[
                Base.metadata.tables[User.__tablename__],
                Base.metadata.tables["internal_service_credentials"],
            ],
        )
        yield test_url.render_as_string(hide_password=False)
    finally:
        if engine is not None:
            engine.dispose()
        with admin_engine.connect() as connection:
            connection.execute(
                sa.text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database_name
                      AND pid <> pg_backend_pid()
                    """
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        admin_engine.dispose()


def _run_service_credentials_cli(
    postgres_uri: str, *args: str
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "DISABLE_DOTENV": "1",
            "LOG_LEVEL": "INFO",
            "OTEL_SDK_DISABLED": "true",
            "PYTHONPATH": "src",
        }
    )
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "dev_health_ops.cli",
            "--db",
            postgres_uri,
            "service-credentials",
            *args,
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
        timeout=60,
    )


def test_service_credential_cli_create_reveals_token_once_and_list_redacts_it(
    monkeypatch, capsys
):
    parser = cli.build_parser()
    create_ns = parser.parse_args(
        [
            "service-credentials",
            "create",
            "--service",
            "acr",
            "--scope",
            "entitlements:read",
        ]
    )
    created: list[InternalServiceCredential] = []

    async def _create(ns):
        credential, token = InternalServiceCredential.issue(
            service_name=ns.service, scopes=ns.scope, created_by_user_id=None
        )
        created.append(credential)
        print(token)
        return 0

    monkeypatch.setattr(create_ns, "func", _create)
    assert asyncio.run(create_ns.func(create_ns)) == 0
    token = capsys.readouterr().out.strip()
    assert token.startswith("svc_acr_")

    list_ns = parser.parse_args(["service-credentials", "list"])

    async def _list(_ns):
        print(json.dumps([created[0].public_metadata()]))
        return 0

    monkeypatch.setattr(list_ns, "func", _list)
    assert asyncio.run(list_ns.func(list_ns)) == 0
    assert token not in capsys.readouterr().out


def test_worker_operator_credentials_have_service_bound_scopes_and_prefix():
    parser = cli.build_parser()
    ns = parser.parse_args(
        [
            "service-credentials",
            "create",
            "--service",
            "worker-operator",
            "--scope",
            "workers:read",
            "--scope",
            "workers:operate",
        ]
    )
    assert service_credentials._scopes(ns.service, ns.scope) == [
        "workers:operate",
        "workers:read",
    ]
    credential, token = InternalServiceCredential.issue(
        service_name=ns.service,
        scopes=ns.scope,
        created_by_user_id=None,
    )
    assert token.startswith("svc_worker_")
    assert credential.token_prefix.startswith("svc_worker_")


@pytest.mark.parametrize(
    ("service", "scope"),
    [("acr", "workers:read"), ("worker-operator", "entitlements:read")],
)
def test_service_credential_scopes_cannot_cross_service_boundary(service, scope):
    with pytest.raises(ValueError, match="unsupported"):
        service_credentials._scopes(service, [scope])


@pytest.mark.asyncio
@pytest.mark.parametrize("inactive_field", ["revoked_at", "expires_at"])
async def test_rotate_rejects_an_inactive_credential_before_creating_a_replacement(
    monkeypatch, inactive_field
):
    credential, _ = InternalServiceCredential.issue(
        service_name="acr", scopes=["entitlements:read"], created_by_user_id=None
    )
    credential.id = uuid.uuid4()
    setattr(credential, inactive_field, datetime.now(timezone.utc))

    class Session:
        async def get(self, _model, _credential_id):
            return credential

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc_value, _traceback):
            return None

    monkeypatch.setattr(service_credentials, "get_postgres_session", lambda: Session())
    parser = cli.build_parser()
    ns = parser.parse_args(
        [
            "service-credentials",
            "rotate",
            str(credential.id),
            "--scope",
            "entitlements:read",
        ]
    )
    with pytest.raises(ValueError, match="active"):
        await service_credentials.run_rotate(ns)


@pytest.mark.asyncio
async def test_create_rejects_past_expiry_before_opening_a_database_session(
    monkeypatch,
):
    parser = cli.build_parser()
    expired_at = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    ns = parser.parse_args(
        [
            "service-credentials",
            "create",
            "--scope",
            "entitlements:read",
            "--expires-at",
            expired_at,
        ]
    )

    def _unexpected_session():
        raise AssertionError("database session must not open for past expiry")

    monkeypatch.setattr(
        service_credentials, "get_postgres_session", _unexpected_session
    )
    with pytest.raises(ValueError, match="future"):
        await service_credentials.run_create(ns)


@pytest.mark.asyncio
async def test_rotate_rejects_past_expiry_without_mutating_existing_credential(
    monkeypatch,
):
    credential, _ = InternalServiceCredential.issue(
        service_name="acr", scopes=["entitlements:read"], created_by_user_id=None
    )
    credential.id = uuid.uuid4()
    initial_expiry = credential.expires_at
    expired_at = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()

    class Session:
        added: list[InternalServiceCredential] = []
        commits = 0

        async def get(self, _model, _credential_id):
            return credential

        def add(self, replacement: InternalServiceCredential) -> None:
            self.added.append(replacement)

        async def commit(self) -> None:
            self.commits += 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, _exc_type, _exc_value, _traceback):
            return None

    session = Session()
    monkeypatch.setattr(service_credentials, "get_postgres_session", lambda: session)
    parser = cli.build_parser()
    ns = parser.parse_args(
        [
            "service-credentials",
            "rotate",
            str(credential.id),
            "--scope",
            "entitlements:read",
            "--expires-at",
            expired_at,
        ]
    )
    with pytest.raises(ValueError, match="future"):
        await service_credentials.run_rotate(ns)
    assert credential.expires_at is initial_expiry
    assert session.added == []
    assert session.commits == 0


@pytest.mark.usefixtures("require_postgres_test_uri")
def test_service_credential_create_emits_only_token_and_db_flag_is_honored() -> None:
    postgres_uri = _postgres_test_uri()
    with _postgres_credential_database(postgres_uri) as test_uri:
        created = _run_service_credentials_cli(
            test_uri, "create", "--scope", "entitlements:read"
        )
        assert created.returncode == 0, created.stderr
        token_lines = created.stdout.splitlines()
        assert len(token_lines) == 1
        assert token_lines[0].startswith("svc_acr_")
        assert token_lines[0] not in created.stderr

        listed = _run_service_credentials_cli(test_uri, "list")
        assert listed.returncode == 0, listed.stderr
        assert token_lines[0] not in listed.stdout
        assert isinstance(json.loads(listed.stdout), list)
