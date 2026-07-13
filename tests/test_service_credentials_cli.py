from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops import cli, service_credentials
from dev_health_ops.models.internal_service_credential import InternalServiceCredential


def _postgres_test_uri() -> str:
    uri = os.getenv("DEV_HEALTH_POSTGRES_TEST_URI")
    if uri is None:
        pytest.skip("DEV_HEALTH_POSTGRES_TEST_URI is not configured")
    return uri


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


def test_service_credential_create_emits_only_token_and_db_flag_is_honored() -> None:
    postgres_uri = _postgres_test_uri()
    created = _run_service_credentials_cli(
        postgres_uri, "create", "--scope", "entitlements:read"
    )
    assert created.returncode == 0, created.stderr
    token_lines = created.stdout.splitlines()
    assert len(token_lines) == 1
    assert token_lines[0].startswith("svc_acr_")
    assert token_lines[0] not in created.stderr

    listed = _run_service_credentials_cli(postgres_uri, "list")
    assert listed.returncode == 0, listed.stderr
    assert token_lines[0] not in listed.stdout
    assert isinstance(json.loads(listed.stdout), list)
