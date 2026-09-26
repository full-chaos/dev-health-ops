"""PostgreSQL proof for alembic 0143 -- the single-plane write-proof receipt kind.

0143 widens ``ck_go_api_proof_run_stage`` with ``write_executed`` and adds
``ck_go_api_proof_run_write_executed_shape`` (a write proof carries its
``side_effect_digest`` and records a measurement route). What a
schema-only smoke test would miss, and what this pins:

* a ``write_executed`` receipt with no digest, or with no recorded route, is
  REFUSED by the database (not only by the writers);
* a well-formed one is accepted, and every pre-existing stage still is;
* an unknown stage is still refused;
* ``go_api_rest_proof_run`` keeps its four stages (a REST route has no write
  proof form) even though the model's stage tuple widened: migrations 0114 and
  0134 no longer read the live model constant, so a fresh database and an
  upgraded one build the same tables;
* ``downgrade`` restores both, and refuses while ``write_executed`` rows exist.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import IntegrityError

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_STAGE_CHECK = "ck_go_api_proof_run_stage"
_SHAPE_CHECK = "ck_go_api_proof_run_write_executed_shape"


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


def _constraints(engine: Engine, table: str) -> dict[str, str]:
    with engine.connect() as c:
        rows = c.execute(
            sa.text(
                "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid = to_regclass(:t) AND contype = 'c'"
            ),
            {"t": table},
        ).all()
    return {name: definition for name, definition in rows}


def _insert_receipt(
    engine: Engine, *, stage: str, digest: str | None, route: str | None
) -> None:
    with engine.begin() as c:
        c.execute(
            sa.text(
                "INSERT INTO go_api_candidate_build (schema_digest, document_digest,"
                " selected_operation, candidate_build, registered_at) VALUES"
                " ('s','d','op','b', now()) ON CONFLICT DO NOTHING"
            )
        )
        c.execute(
            sa.text(
                "INSERT INTO go_api_proof_run (id, schema_digest, document_digest,"
                " selected_operation, candidate_build, request_identity, stage,"
                " terminal_state, measurement_route, side_effect_digest,"
                " data_watermark, observed_at) VALUES"
                " (gen_random_uuid(),'s','d','op','b',:identity,:stage,'match',"
                " :route,:digest,'w', now())"
            ),
            {
                "identity": uuid.uuid4().hex,
                "stage": stage,
                "route": route,
                "digest": digest,
            },
        )


@pytest.fixture
def migrated(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    uri = os.environ.get(_POSTGRES_URI_ENV)
    if uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for migration tests")
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")
    url = make_url(uri)
    if url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    name = f"test_chaos_6810_{uuid.uuid4().hex}"
    admin = sa.create_engine(
        url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    engine: Engine | None = None
    try:
        with admin.connect() as c:
            c.exec_driver_sql(f'CREATE DATABASE "{name}"')
        monkeypatch.setenv(
            "POSTGRES_URI",
            url.set(drivername="postgresql+asyncpg", database=name).render_as_string(
                hide_password=False
            ),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_config(), "0142")
        engine = sa.create_engine(
            url.set(drivername="postgresql+psycopg2", database=name)
        )
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        with admin.connect() as c:
            c.exec_driver_sql(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.dispose()


def test_upgrade_widens_the_stage_check_and_adds_the_shape_check(
    migrated: Engine,
) -> None:
    before = _constraints(migrated, "go_api_proof_run")
    assert "write_executed" not in before[_STAGE_CHECK]
    assert _SHAPE_CHECK not in before

    command.upgrade(_config(), "0143")

    after = _constraints(migrated, "go_api_proof_run")
    assert "write_executed" in after[_STAGE_CHECK]
    assert _SHAPE_CHECK in after
    # The REST ledger keeps the four query-proof stages.
    rest = _constraints(migrated, "go_api_rest_proof_run")
    assert all("write_executed" not in definition for definition in rest.values())


def test_the_database_refuses_a_malformed_write_receipt(migrated: Engine) -> None:
    command.upgrade(_config(), "0143")

    with pytest.raises(IntegrityError):  # no digest
        _insert_receipt(migrated, stage="write_executed", digest=None, route="edge")
    with pytest.raises(IntegrityError):  # no route at all
        _insert_receipt(migrated, stage="write_executed", digest="d", route=None)
    with pytest.raises(IntegrityError):  # an unknown stage is still refused
        _insert_receipt(migrated, stage="write_proof", digest="d", route="edge")

    _insert_receipt(migrated, stage="write_executed", digest="d", route="edge")
    _insert_receipt(migrated, stage="write_executed", digest="d", route="proof")
    # Every earlier stage keeps its old shape (no digest required).
    _insert_receipt(migrated, stage="deployed_executed", digest=None, route="edge")


def test_downgrade_restores_both_checks_and_refuses_while_write_rows_exist(
    migrated: Engine,
) -> None:
    command.upgrade(_config(), "0143")
    _insert_receipt(migrated, stage="write_executed", digest="d", route="edge")

    with pytest.raises(Exception):  # the four-stage check cannot hold that row
        command.downgrade(_config(), "0142")

    with migrated.begin() as c:
        c.execute(
            sa.text("DELETE FROM go_api_proof_run WHERE stage = 'write_executed'")
        )
    command.downgrade(_config(), "0142")

    restored = _constraints(migrated, "go_api_proof_run")
    assert "write_executed" not in restored[_STAGE_CHECK]
    assert _SHAPE_CHECK not in restored
