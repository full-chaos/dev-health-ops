"""0133 seeds the go-api-prove proof service principal users row.

Real Postgres, against the actual alembic chain up through 0132, matching
test_0125_promote_worker_job_routes_off_celery_postgres.py's harness. The row
must have exactly the shape internal/edgetokenmint.LookupPrincipal accepts as
a service identity, and NO membership: the minter refusing an unmembered
principal is what keeps this revision from granting any org access on its
own (the live e2e tier proves that refusal against this migrated row).
"""

from __future__ import annotations

import importlib
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

from tests._alembic_heads import application_schema_head

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_MODULE = "dev_health_ops.alembic.versions.0133_seed_go_api_prove_service_principal"
_EDGETOKENMINT_GO = (
    Path(__file__).parents[1] / "internal" / "edgetokenmint" / "edgetokenmint.go"
)


def _migration() -> ModuleType:
    return importlib.import_module(_MODULE)


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture(autouse=True, scope="module")
def require_postgres_test_uri() -> None:
    if os.getenv(_POSTGRES_URI_ENV):
        return
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.fail(
            f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL migration tests"
        )
    pytest.skip(f"requires {_POSTGRES_URI_ENV}")


@pytest.fixture
def migrated_to_0132(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    configured_url = make_url(os.environ[_POSTGRES_URI_ENV])
    assert configured_url.get_backend_name() == "postgresql"
    database_name = f"test_0133_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    database_created = False
    engine: Engine | None = None
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True
        monkeypatch.setenv(
            "POSTGRES_URI",
            configured_url.set(
                drivername="postgresql+asyncpg", database=database_name
            ).render_as_string(hide_password=False),
        )
        monkeypatch.setenv("DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", "1")
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_config(), "0132")
        engine = sa.create_engine(
            configured_url.set(drivername="postgresql+psycopg2", database=database_name)
        )
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        if database_created:
            with admin_engine.connect() as connection:
                connection.execute(
                    sa.text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                    ),
                    {"database_name": database_name},
                )
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


def _principal(engine: Engine) -> dict[str, object] | None:
    with engine.connect() as connection:
        row = (
            connection.execute(
                sa.text(
                    "SELECT id::text AS id, email, username, password_hash, full_name, "
                    "auth_provider, auth_provider_id, is_active, is_verified, "
                    "is_superuser, token_version FROM users WHERE id = :id"
                ),
                {"id": _migration().PRINCIPAL_ID},
            )
            .mappings()
            .one_or_none()
        )
        return dict(row) if row is not None else None


def _membership_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return int(
            connection.execute(
                sa.text("SELECT count(*) FROM memberships WHERE user_id = :id"),
                {"id": _migration().PRINCIPAL_ID},
            ).scalar_one()
        )


def _insert_user(engine: Engine, **overrides: object) -> None:
    values: dict[str, object] = {
        "id": _migration().PRINCIPAL_ID,
        "email": _migration().PRINCIPAL_EMAIL,
        "password_hash": None,
        "auth_provider": "service",
    }
    values.update(overrides)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO users (id, email, password_hash, auth_provider) "
                "VALUES (:id, :email, :password_hash, :auth_provider)"
            ),
            values,
        )


def test_0133_is_the_application_schema_head_and_chains_after_0132() -> None:
    """Derived, not typed. See ``tests/_alembic_heads.py``'s own docstring:
    the next migration author renumbers THIS check (or supersedes it) rather
    than leaving a stale pin.
    """
    migration = _migration()
    assert migration.revision == application_schema_head(), (
        "0133 must be the application_schema head; if another migration "
        "landed first, renumber this one and re-run"
    )
    assert migration.down_revision == "0132"


def test_0133_constants_match_the_go_minter() -> None:
    source = _EDGETOKENMINT_GO.read_text(encoding="utf-8")
    migration = _migration()
    assert f'ProvePrincipalID = "{migration.PRINCIPAL_ID}"' in source
    assert f'ServiceAuthProvider = "{migration.SERVICE_AUTH_PROVIDER}"' in source


def test_0133_seeds_one_unmembered_service_identity(migrated_to_0132: Engine) -> None:
    assert _principal(migrated_to_0132) is None

    command.upgrade(_config(), "0133")

    assert _principal(migrated_to_0132) == {
        "id": _migration().PRINCIPAL_ID,
        "email": "go-api-prove@service.dev-health.invalid",
        "username": None,
        "password_hash": None,
        "full_name": None,
        "auth_provider": "service",
        "auth_provider_id": None,
        "is_active": True,
        "is_verified": False,
        "is_superuser": False,
        "token_version": 0,
    }
    assert _membership_count(migrated_to_0132) == 0


def test_0133_accepts_a_row_that_already_has_the_service_shape(
    migrated_to_0132: Engine,
) -> None:
    _insert_user(migrated_to_0132)

    command.upgrade(_config(), "0133")

    principal = _principal(migrated_to_0132)
    assert principal is not None
    assert principal["auth_provider"] == "service"
    assert principal["password_hash"] is None


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"auth_provider": "local"}, id="same id, human provider"),
        pytest.param(
            {"password_hash": "not-a-real-hash"}, id="same id, has a password"
        ),
        pytest.param({"email": "someone@example.com"}, id="same id, other email"),
        pytest.param(
            {"id": str(uuid.uuid4()), "auth_provider": "local"},
            id="other id, same email",
        ),
    ],
)
def test_0133_refuses_a_conflicting_row(
    migrated_to_0132: Engine, overrides: dict[str, object]
) -> None:
    _insert_user(migrated_to_0132, **overrides)

    with pytest.raises(
        RuntimeError, match="conflicts with the go-api-prove service principal"
    ):
        command.upgrade(_config(), "0133")


def test_0133_downgrade_deletes_the_row_and_upgrade_restores_it(
    migrated_to_0132: Engine,
) -> None:
    command.upgrade(_config(), "0133")
    command.downgrade(_config(), "0132")

    assert _principal(migrated_to_0132) is None

    command.upgrade(_config(), "0133")

    assert _principal(migrated_to_0132) is not None


def test_0133_downgrade_leaves_a_row_that_no_longer_has_the_service_shape(
    migrated_to_0132: Engine,
) -> None:
    command.upgrade(_config(), "0133")
    with migrated_to_0132.begin() as connection:
        connection.execute(
            sa.text("UPDATE users SET auth_provider = 'local' WHERE id = :id"),
            {"id": _migration().PRINCIPAL_ID},
        )

    command.downgrade(_config(), "0132")

    principal = _principal(migrated_to_0132)
    assert principal is not None
    assert principal["auth_provider"] == "local"
