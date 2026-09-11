"""PostgreSQL proof for alembic 0128 -- measurement provenance on receipts.

0128 adds three columns to ``go_api_proof_run`` for CHAOS-5425's ``prove``
verb, and each one carries a claim a schema-only smoke test would miss:

1. ``measurement_route`` is nullable and CHECK-constrained to
   ``edge``/``proof``. Nullable because every pre-0128 row genuinely has no
   route and guessing one would read as a fact; constrained because a
   receipt whose route is neither cannot be interpreted at all, and a
   proof-route observation must never be mistaken for served traffic.
2. ``baseline_defect`` is a nullable text array. It ANNOTATES a mismatch --
   Python wrong, Go right (CHAOS-5448, CHAOS-5450) -- and this test pins
   that it does not, and cannot, change ``terminal_state``.
3. ``differences_outside_baseline_defect`` is NOT NULL with default 0. The
   explicit zero IS the claim ("every difference here is a known Python
   defect"); a NULL would leave a reader guessing whether that was asserted
   or simply never computed. Pre-existing rows must therefore come out of
   the upgrade with 0, not NULL.

And ``downgrade`` must actually remove all three plus the constraint: a
migration whose downgrade is untested is one you cannot roll back.
"""

from __future__ import annotations

import os
import re
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
_COLUMNS = (
    "measurement_route",
    "baseline_defect",
    "differences_outside_baseline_defect",
)
_CONSTRAINT = "ck_go_api_proof_run_measurement_route"


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
    """One candidate build plus one proof run, as they exist before 0128."""
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
                " terminal_state, observed_at) VALUES"
                " (gen_random_uuid(),'s','d','op','b','identity','dual_run',"
                " 'match', now())"
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

    name = f"test_chaos_5425_{uuid.uuid4().hex}"
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
        command.upgrade(_config(), "0127")
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


def test_0128_adds_the_three_measurement_columns(migrated: Engine) -> None:
    before = _columns(migrated, _TABLE)
    for column in _COLUMNS:
        assert column not in before, f"{_TABLE}.{column} exists before 0128"

    command.upgrade(_config(), "0128")

    after = _columns(migrated, _TABLE)
    for column in _COLUMNS:
        assert column in after, f"0128 did not add {_TABLE}.{column}"

    assert after["measurement_route"] is True, (
        "measurement_route must be NULLABLE -- every row written before 0128 has "
        "no route, and guessing one would read as a recorded fact"
    )
    assert after["baseline_defect"] is True
    assert after["differences_outside_baseline_defect"] is False, (
        "the explicit zero IS the claim: a NULL would leave a reader guessing "
        "whether 'no uncovered differences' was asserted or never computed"
    )


def test_0128_preserves_existing_rows_with_an_explicit_zero(
    migrated: Engine,
) -> None:
    """An upgrade that dropped data would be catastrophic and silent, and a
    pre-existing receipt must come out with 0, never NULL."""
    _seed_receipt(migrated)

    command.upgrade(_config(), "0128")

    with migrated.connect() as c:
        row = c.execute(
            sa.text(
                "SELECT terminal_state, measurement_route, baseline_defect,"
                " differences_outside_baseline_defect FROM go_api_proof_run"
            )
        ).one()
    assert row[0] == "match"
    assert row[1] is None
    assert row[2] is None
    assert row[3] == 0


def test_0128_check_rejects_an_unknown_measurement_route(migrated: Engine) -> None:
    """The vocabulary is closed: a receipt whose route is neither 'edge' nor
    'proof' cannot be interpreted, so the database refuses it rather than
    trusting every future writer to remember."""
    command.upgrade(_config(), "0128")
    _seed_receipt(migrated)

    assert _CONSTRAINT in _constraints(migrated, _TABLE)

    with migrated.connect() as c:
        with pytest.raises(sa.exc.IntegrityError):
            c.execute(
                sa.text(
                    "INSERT INTO go_api_proof_run (id, schema_digest, document_digest,"
                    " selected_operation, candidate_build, request_identity, stage,"
                    " terminal_state, measurement_route, observed_at) VALUES"
                    " (gen_random_uuid(),'s','d','op','b','identity',"
                    " 'deployed_executed','match','sideways', now())"
                )
            )
        c.rollback()


def test_0128_baseline_defect_annotates_but_never_softens(migrated: Engine) -> None:
    """A mismatch annotated with a known Python defect is still a mismatch.
    The columns record WHY; they do not, and must not, change WHAT."""
    command.upgrade(_config(), "0128")
    _seed_receipt(migrated)

    with migrated.connect() as c:
        c.execute(
            sa.text(
                "INSERT INTO go_api_proof_run (id, schema_digest, document_digest,"
                " selected_operation, candidate_build, request_identity, stage,"
                " terminal_state, measurement_route, baseline_defect,"
                " differences_outside_baseline_defect, observed_at) VALUES"
                " (gen_random_uuid(),'s','d','op','b','identity2',"
                " 'deployed_executed','mismatch','proof',"
                " ARRAY['CHAOS-5448','CHAOS-5450'], 0, now())"
            )
        )
        c.commit()
        row = c.execute(
            sa.text(
                "SELECT terminal_state, measurement_route, baseline_defect,"
                " differences_outside_baseline_defect FROM go_api_proof_run"
                " WHERE request_identity = 'identity2'"
            )
        ).one()

    assert row[0] == "mismatch", "an annotation must never convert the verdict"
    assert row[1] == "proof"
    assert list(row[2]) == ["CHAOS-5448", "CHAOS-5450"]
    assert row[3] == 0


def test_0128_downgrade_removes_the_columns_and_the_constraint(
    migrated: Engine,
) -> None:
    command.upgrade(_config(), "0128")
    assert all(column in _columns(migrated, _TABLE) for column in _COLUMNS)
    assert _CONSTRAINT in _constraints(migrated, _TABLE)

    command.downgrade(_config(), "0127")

    remaining = _columns(migrated, _TABLE)
    for column in _COLUMNS:
        assert column not in remaining, f"downgrade left {_TABLE}.{column}"
    assert _CONSTRAINT not in _constraints(migrated, _TABLE)


def test_registry_ddl_mirror_covers_every_migrated_column() -> None:
    """The Go integration suite builds these three tables from a hand-kept
    DDL string (``registryDDL`` in
    ``internal/goapiproof/receipt_integration_test.go``) so it can exercise
    the real FK and CHECK constraints. A mirror that falls behind the
    migrations would let those tests pass against a schema Postgres does not
    have -- the "relaxed schema" failure that DDL's own comment warns about.

    This check lives on the PYTHON side on purpose. Reading the alembic
    files from the Go test made them inputs to the Go workflow, and
    ``go.yml``'s path filters do not cover ``alembic/versions`` -- so a PR
    changing only a migration would have satisfied ``go-quality``
    vacuously (``tests/tooling/test_go_workflow_path_filters.py`` catches
    exactly that). Python's own workflow already runs on these files, so
    enforcing it here keeps the guard and costs no cross-language trigger.
    """
    repo_root = Path(__file__).parents[1]
    ddl = (repo_root / "internal/goapiproof/receipt_integration_test.go").read_text(
        encoding="utf-8"
    )
    column_pattern = re.compile(r'sa\.Column\(\s*"([a-z_]+)"')

    checked = 0
    missing: list[str] = []
    for migration in (
        "0114_add_go_api_operation_registry.py",
        "0127_add_go_api_routing_provenance.py",
        "0128_add_go_api_proof_run_measurement_provenance.py",
        "0129_add_go_api_proof_run_build_binding.py",
    ):
        source = (_ALEMBIC_DIR / "versions" / migration).read_text(encoding="utf-8")
        for match in column_pattern.finditer(source):
            column = match.group(1)
            checked += 1
            if f"\t{column} " not in ddl:
                missing.append(f"{migration}: {column}")

    assert not missing, (
        "registryDDL does not declare these migrated columns -- update the "
        f"mirror: {missing}"
    )
    # Non-vacuity: 28 columns match across the three migrations today. The
    # floor sits well below that so an ordinary schema edit does not fail
    # here, while a collapse to zero -- the pattern silently ceasing to
    # match, which would make every assertion above a green no-op -- still
    # does.
    assert checked >= 20, (
        f"only {checked} migrated columns were checked -- the column pattern "
        "has stopped matching and this test is now vacuous"
    )
