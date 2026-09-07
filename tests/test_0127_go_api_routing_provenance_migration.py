"""PostgreSQL proof for alembic 0127 -- provenance on the Go-API registry.

0127 adds ``review_evidence`` and ``recorded_by`` to BOTH
``go_api_proof_run`` and ``go_api_routing_state``. Three claims a
schema-only smoke test would miss:

1. All four columns land, on both tables -- a migration that added them to
   one table would leave routing-state changes unattributable, which is the
   specific gap that made tonight's fifteen acknowledged-unproven rows
   unreadable.
2. They are NULLABLE, and pre-existing rows survive the upgrade with NULLs.
   Backfilling a guessed operator or reason would be worse than an honest
   NULL: it would read as a decision somebody made.
3. ``downgrade`` actually removes all four. A migration whose downgrade is
   untested is a migration you cannot roll back -- and this PR exists
   because a thing could not be rolled back.
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

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLES = ("go_api_proof_run", "go_api_routing_state")
_COLUMNS = ("review_evidence", "recorded_by")


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


def _columns(engine: Engine, table: str) -> dict[str, bool]:
    with engine.connect() as c:
        rows = c.execute(
            sa.text(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name = :t"
            ),
            {"t": table},
        ).all()
    return {name: nullable == "YES" for name, nullable in rows}


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

    name = f"test_chaos_5446_{uuid.uuid4().hex}"
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
        command.upgrade(_config(), "0126")
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


def test_0127_adds_nullable_provenance_to_both_tables(migrated: Engine) -> None:
    for table in _TABLES:
        before = _columns(migrated, table)
        for column in _COLUMNS:
            assert column not in before, f"{table}.{column} exists before 0127"

    command.upgrade(_config(), "0127")

    for table in _TABLES:
        after = _columns(migrated, table)
        for column in _COLUMNS:
            assert column in after, f"0127 did not add {table}.{column}"
            assert after[column] is True, (
                f"{table}.{column} must be NULLABLE -- every row written before "
                "0127 genuinely has neither, and backfilling a guessed operator "
                "would read as a decision somebody made"
            )


def test_0127_preserves_existing_rows(migrated: Engine) -> None:
    """An upgrade that dropped data would be catastrophic and silent."""
    with migrated.connect() as c:
        c.execute(
            sa.text(
                "INSERT INTO go_api_candidate_build (schema_digest, document_digest,"
                " selected_operation, candidate_build, registered_at) VALUES"
                " ('s','d','op','b', now())"
            )
        )
        c.execute(
            sa.text(
                "INSERT INTO go_api_routing_state (schema_digest, document_digest,"
                " selected_operation, current_candidate_build, owner, mode,"
                " rollout_percentage, updated_at) VALUES"
                " ('s','d','op','b','go','canary',100, now())"
            )
        )
        c.commit()

    command.upgrade(_config(), "0127")

    with migrated.connect() as c:
        row = c.execute(
            sa.text(
                "SELECT mode, review_evidence, recorded_by FROM go_api_routing_state"
            )
        ).one()
    assert row[0] == "canary"
    assert row[1] is None
    assert row[2] is None


def test_0127_downgrade_removes_all_four_columns(migrated: Engine) -> None:
    """A migration whose downgrade is untested cannot be rolled back --
    and this PR exists because something could not be rolled back."""
    command.upgrade(_config(), "0127")
    for table in _TABLES:
        assert all(c in _columns(migrated, table) for c in _COLUMNS)

    command.downgrade(_config(), "0126")

    for table in _TABLES:
        remaining = _columns(migrated, table)
        for column in _COLUMNS:
            assert column not in remaining, f"downgrade left {table}.{column}"
