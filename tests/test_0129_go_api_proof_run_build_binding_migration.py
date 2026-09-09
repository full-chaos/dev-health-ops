"""PostgreSQL proof for alembic 0129 -- build binding on receipts.

0129 adds one column, ``go_api_proof_run.build_binding``, and it carries
two claims a schema-only smoke test would miss.

**Nullable, and pre-existing rows must come out NULL.** Every row written
before this migration genuinely makes no assertion about how strongly its
measurement bound the serving build. Backfilling one from
``measurement_route`` -- the derivation that happens to be true today --
would manufacture exactly the evidence the column exists to stop being
assumed, and it would be indistinguishable afterwards from a row that
really did carry the claim. So this pins that the upgrade leaves an
existing receipt at NULL, not at a guess.

**CHECK-constrained to per_request/absent.** A binding strength outside
those two cannot be compared against the rule that reads it: a reader
would have to guess whether an unknown value is stronger or weaker than
the ones ``enable``'s predicate is written against. Same reasoning as
0128's ``measurement_route`` CHECK.

And ``downgrade`` must actually remove the column and the constraint: a
migration whose downgrade is untested is one you cannot roll back.
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
_TABLE = "go_api_proof_run"
_COLUMN = "build_binding"
_CONSTRAINT = "ck_go_api_proof_run_build_binding"


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


def _constraints(engine: Engine, table: str) -> set[str]:
    with engine.connect() as c:
        rows = c.execute(
            sa.text(
                "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:t)"
            ),
            {"t": table},
        ).all()
    return {name for (name,) in rows}


def _seed_receipt(engine: Engine) -> None:
    """One candidate build plus one proof run, as they exist before 0129."""
    with engine.connect() as c:
        c.execute(
            sa.text(
                "INSERT INTO go_api_candidate_build (schema_digest, document_digest,"
                " selected_operation, candidate_build, registered_at) VALUES"
                " ('s','d','op','b', now())"
            )
        )
        c.execute(
            sa.text(
                "INSERT INTO go_api_proof_run (id, schema_digest, document_digest,"
                " selected_operation, candidate_build, request_identity, stage,"
                " terminal_state, measurement_route, observed_at) VALUES"
                " (gen_random_uuid(),'s','d','op','b','identity',"
                " 'deployed_executed','match','edge', now())"
            )
        )
        c.commit()


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

    name = f"test_chaos_5484_{uuid.uuid4().hex}"
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
        command.upgrade(_config(), "0128")
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


def test_upgrade_adds_a_nullable_build_binding(migrated: Engine) -> None:
    assert _COLUMN not in _columns(migrated, _TABLE)
    command.upgrade(_config(), "0129")

    columns = _columns(migrated, _TABLE)
    assert _COLUMN in columns
    assert columns[_COLUMN] is True, (
        "build_binding must be NULLABLE: a pre-0129 row makes no claim about "
        "how strongly its measurement bound the serving build, and NOT NULL "
        "would force one to be invented"
    )


def test_an_existing_receipt_survives_the_upgrade_with_no_claim(
    migrated: Engine,
) -> None:
    """The whole reason the column is nullable, asserted rather than argued.

    The seeded row is measured on the ``edge`` route, so the derivation
    that holds today would give it ``absent``. It must come out NULL
    anyway: a value written by the migration would be a guess, and after
    the fact it would be indistinguishable from a strength the run
    actually established.
    """
    _seed_receipt(migrated)
    command.upgrade(_config(), "0129")

    with migrated.connect() as c:
        binding = c.execute(
            sa.text("SELECT build_binding FROM go_api_proof_run")
        ).scalar_one()
    assert binding is None, (
        "the upgrade backfilled a build binding onto a pre-0129 receipt: it "
        "cannot know one, and a guess here is indistinguishable later from a "
        "measurement that really made the claim"
    )


def test_the_check_constraint_admits_only_the_two_strengths(
    migrated: Engine,
) -> None:
    _seed_receipt(migrated)
    command.upgrade(_config(), "0129")
    assert _CONSTRAINT in _constraints(migrated, _TABLE)

    for accepted in ("per_request", "absent"):
        with migrated.connect() as c:
            c.execute(
                sa.text("UPDATE go_api_proof_run SET build_binding = :v"),
                {"v": accepted},
            )
            c.commit()

    # And a plausible-looking value outside the vocabulary is refused. The
    # danger is not a typo -- it is a THIRD strength invented later by a
    # writer that the reading rule knows nothing about, which would then
    # be silently sorted with neither of the two the rule handles.
    #
    # ``run_level`` is first in this list on purpose: it is the value an
    # earlier draft of CHAOS-5484 used for the weak case, so it is the one
    # a future writer is most likely to reinvent. It was dropped because
    # every row that exists already has run-level evidence (R70 makes
    # VerifyCandidateBuild a hard refusal), so the value would distinguish
    # nothing -- see the 0129 docstring.
    for rejected in ("run_level", "per-request", "runlevel", "strong", ""):
        with migrated.connect() as c:
            with pytest.raises(sa.exc.IntegrityError):
                c.execute(
                    sa.text("UPDATE go_api_proof_run SET build_binding = :v"),
                    {"v": rejected},
                )
            c.rollback()


def test_downgrade_removes_the_column_and_the_constraint(migrated: Engine) -> None:
    command.upgrade(_config(), "0129")
    assert _COLUMN in _columns(migrated, _TABLE)
    assert _CONSTRAINT in _constraints(migrated, _TABLE)

    command.downgrade(_config(), "0128")

    assert _COLUMN not in _columns(migrated, _TABLE), "downgrade left build_binding"
    assert _CONSTRAINT not in _constraints(migrated, _TABLE)
