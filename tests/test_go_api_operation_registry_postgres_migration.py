"""PostgreSQL proof for the Go API operation rollout registry + proof ledger
(alembic 0114, CHAOS-4366 Wave 0).

Three claims a schema-only smoke test would miss:

1. The composite FK from ``go_api_routing_state``/``go_api_proof_run`` to
   ``go_api_candidate_build`` is a full 4-column match, not a bare
   ``candidate_build`` string comparison -- a routing/proof row cannot
   silently point at a build registered for a DIFFERENT operation triple
   that happens to share the same build string.
2. The ``owner``/``mode``/``stage``/``terminal_state`` CHECK constraints
   actually reject out-of-vocabulary values, not just document them.
3. A ``stage='shadow'`` proof run with no ``data_watermark`` is rejected --
   CHAOS-4381 parity rule 4 requires the watermark for the stage-4 same-
   watermark comparison; a proof run recorded without one cannot back that
   claim and must not be insertable.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_CANDIDATE_BUILD = "go_api_candidate_build"
_ROUTING_STATE = "go_api_routing_state"
_PROOF_RUN = "go_api_proof_run"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0113(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(
                f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL migration tests"
            )
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_chaos_4366_{uuid.uuid4().hex}"
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
                drivername="postgresql+asyncpg",
                database=database_name,
            ).render_as_string(hide_password=False),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "0113")

        engine = sa.create_engine(
            configured_url.set(
                drivername="postgresql+psycopg2",
                database=database_name,
            )
        )
        yield PostgresMigrationHarness(engine=engine)
    finally:
        if engine is not None:
            engine.dispose()
        if database_created:
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
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def _seed_two_candidate_builds(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_CANDIDATE_BUILD}
                  (schema_digest, document_digest, selected_operation, candidate_build)
                VALUES
                  ('schema-a', 'doc-a', 'featureFlags', 'build-1'),
                  ('schema-b', 'doc-b', 'reviewEdges', 'build-1')
                """
            )
        )


def test_0114_creates_all_three_tables_with_expected_shape(
    migrated_to_0113: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0114")
    inspector = sa.inspect(migrated_to_0113.engine)

    assert inspector.has_table(_CANDIDATE_BUILD)
    assert inspector.has_table(_ROUTING_STATE)
    assert inspector.has_table(_PROOF_RUN)

    assert inspector.get_pk_constraint(_CANDIDATE_BUILD)["constrained_columns"] == [
        "schema_digest",
        "document_digest",
        "selected_operation",
        "candidate_build",
    ]
    assert inspector.get_pk_constraint(_ROUTING_STATE)["constrained_columns"] == [
        "schema_digest",
        "document_digest",
        "selected_operation",
    ]
    assert inspector.get_pk_constraint(_PROOF_RUN)["constrained_columns"] == ["id"]

    assert _revisions(migrated_to_0113.engine) == {"0114"}


def test_0114_routing_state_fk_is_the_full_four_column_tuple(
    migrated_to_0113: PostgresMigrationHarness,
) -> None:
    """A routing-state row cannot point at a build string registered for a
    DIFFERENT operation triple -- the FK must reject a same-build-string,
    different-operation reference, not just a missing-build reference."""
    command.upgrade(_migration_config(), "0114")
    _seed_two_candidate_builds(migrated_to_0113.engine)

    # build-1 exists for ('schema-a','doc-a','featureFlags') but NOT for
    # ('schema-a','doc-a','reviewEdges') -- same build string, wrong triple.
    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0113.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_ROUTING_STATE}
                      (schema_digest, document_digest, selected_operation,
                       current_candidate_build, owner, mode, rollout_percentage)
                    VALUES
                      ('schema-a', 'doc-a', 'reviewEdges', 'build-1', 'go', 'canary', 10)
                    """
                )
            )

    # The correct triple succeeds.
    with migrated_to_0113.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_ROUTING_STATE}
                  (schema_digest, document_digest, selected_operation,
                   current_candidate_build, owner, mode, rollout_percentage)
                VALUES
                  ('schema-a', 'doc-a', 'featureFlags', 'build-1', 'go', 'canary', 10)
                """
            )
        )


def test_0114_routing_state_rejects_out_of_vocabulary_owner_and_mode(
    migrated_to_0113: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0114")
    _seed_two_candidate_builds(migrated_to_0113.engine)

    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0113.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_ROUTING_STATE}
                      (schema_digest, document_digest, selected_operation,
                       current_candidate_build, owner, mode, rollout_percentage)
                    VALUES
                      ('schema-a', 'doc-a', 'featureFlags', 'build-1', 'rust', 'canary', 0)
                    """
                )
            )

    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0113.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_ROUTING_STATE}
                      (schema_digest, document_digest, selected_operation,
                       current_candidate_build, owner, mode, rollout_percentage)
                    VALUES
                      ('schema-a', 'doc-a', 'featureFlags', 'build-1', 'go', 'yolo', 0)
                    """
                )
            )


def test_0114_proof_run_rejects_out_of_vocabulary_stage_and_terminal_state(
    migrated_to_0113: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0114")
    _seed_two_candidate_builds(migrated_to_0113.engine)

    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0113.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_PROOF_RUN}
                      (id, schema_digest, document_digest, selected_operation,
                       candidate_build, request_identity, stage, terminal_state)
                    VALUES
                      (gen_random_uuid(), 'schema-a', 'doc-a', 'featureFlags',
                       'build-1', 'req-1', 'not_a_real_stage', 'match')
                    """
                )
            )

    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0113.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_PROOF_RUN}
                      (id, schema_digest, document_digest, selected_operation,
                       candidate_build, request_identity, stage, terminal_state)
                    VALUES
                      (gen_random_uuid(), 'schema-a', 'doc-a', 'featureFlags',
                       'build-1', 'req-1', 'dual_run', 'not_a_real_terminal_state')
                    """
                )
            )


def test_0114_shadow_stage_proof_run_requires_a_watermark(
    migrated_to_0113: PostgresMigrationHarness,
) -> None:
    """CHAOS-4381 parity rule 4: a watermark mismatch is 'unsupported', never
    'mismatch' -- but that claim is only checkable if a shadow proof run
    always carries a watermark. Enforce it at the schema, not by convention.
    """
    command.upgrade(_migration_config(), "0114")
    _seed_two_candidate_builds(migrated_to_0113.engine)

    with pytest.raises(sa.exc.IntegrityError):
        with migrated_to_0113.engine.begin() as connection:
            connection.execute(
                sa.text(
                    f"""
                    INSERT INTO {_PROOF_RUN}
                      (id, schema_digest, document_digest, selected_operation,
                       candidate_build, request_identity, stage, terminal_state,
                       data_watermark)
                    VALUES
                      (gen_random_uuid(), 'schema-a', 'doc-a', 'featureFlags',
                       'build-1', 'req-1', 'shadow', 'match', NULL)
                    """
                )
            )

    # A non-shadow stage (e.g. dual_run) does not require one.
    with migrated_to_0113.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_PROOF_RUN}
                  (id, schema_digest, document_digest, selected_operation,
                   candidate_build, request_identity, stage, terminal_state,
                   data_watermark)
                VALUES
                  (gen_random_uuid(), 'schema-a', 'doc-a', 'featureFlags',
                   'build-1', 'req-1', 'dual_run', 'match', NULL)
                """
            )
        )

    # A shadow stage WITH a watermark is accepted.
    with migrated_to_0113.engine.begin() as connection:
        connection.execute(
            sa.text(
                f"""
                INSERT INTO {_PROOF_RUN}
                  (id, schema_digest, document_digest, selected_operation,
                   candidate_build, request_identity, stage, terminal_state,
                   data_watermark)
                VALUES
                  (gen_random_uuid(), 'schema-a', 'doc-a', 'featureFlags',
                   'build-1', 'req-1', 'shadow', 'match', '2026-08-28T00:00:00Z')
                """
            )
        )


def test_0114_downgrade_and_reupgrade_converge(
    migrated_to_0113: PostgresMigrationHarness,
) -> None:
    command.upgrade(_migration_config(), "0114")
    command.downgrade(_migration_config(), "0113")

    inspector = sa.inspect(migrated_to_0113.engine)
    assert not inspector.has_table(_PROOF_RUN)
    assert not inspector.has_table(_ROUTING_STATE)
    assert not inspector.has_table(_CANDIDATE_BUILD)
    assert _revisions(migrated_to_0113.engine) == {"0113"}

    command.upgrade(_migration_config(), "application_schema@head")
    assert sa.inspect(migrated_to_0113.engine).has_table(_CANDIDATE_BUILD)
    assert _revisions(migrated_to_0113.engine) == {"0121"}
