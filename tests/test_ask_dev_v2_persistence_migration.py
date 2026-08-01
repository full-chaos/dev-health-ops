"""Migration proofs for the Wave 3.1 v2 persistence schema (CHAOS-3299).

SQLite covers the fast, CI-default upgrade/downgrade/re-upgrade rehearsal.
CHECK constraint *enforcement*, JSONB, and NOT VALID/VALIDATE CONSTRAINT
semantics don't exist in SQLite at all, so a live-PostgreSQL-gated variant
(``DEV_HEALTH_POSTGRES_TEST_URI``, matching ``test_persistence_postgres.py``'s
skipif pattern) proves the CHECK constraints themselves, not just that the
migration runs.
"""

from __future__ import annotations

import importlib
import os
import uuid

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"

_NEW_TABLES = {
    "dev_run_intents",
    "dev_run_resolutions",
    "dev_run_subject_sets",
    "dev_run_source_observations",
    "dev_answer_frames",
    "dev_run_narratives",
    "dev_run_stage_diagnostics",
}


def _parent_and_v1_tables(connection: sa.Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        "organizations",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
    )
    sa.Table(
        "users",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False),
    )
    metadata.create_all(connection)


def _load(revision_module: str):
    return importlib.import_module(f"dev_health_ops.alembic.versions.{revision_module}")


def test_0074_and_0075_module_metadata() -> None:
    m74 = _load("0074_add_ask_dev_v2_persistence")
    m75 = _load("0075_validate_ask_dev_v2_constraints")
    assert m74.revision == "0074"
    # "0073" is CHAOS-3292's flag-seed migration; this branch stacks on
    # feat/chaos-3292-interpret-preflight, where 0072/0073 exist.
    assert m74.down_revision == "0073"
    assert m75.revision == "0075"
    assert m75.down_revision == "0074"


def test_0074_clean_install_upgrade_and_pre_release_downgrade_are_rehearsable() -> None:
    m68 = _load("0068_add_ask_dev_persistence")
    m69 = _load("0069_add_ask_dev_admission_indexes")
    m74 = _load("0074_add_ask_dev_v2_persistence")
    m75 = _load("0075_validate_ask_dev_v2_constraints")

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as connection:
            _parent_and_v1_tables(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                m68.upgrade()
                m69.upgrade()
                m74.upgrade()
                m75.upgrade()

                tables = set(sa.inspect(connection).get_table_names())
                assert _NEW_TABLES.issubset(tables)
                columns = {
                    c["name"] for c in sa.inspect(connection).get_columns("dev_runs")
                }
                assert {
                    "contract_generation",
                    "public_outcome",
                    "compatibility_projection_version",
                    "plan_id",
                    "plan_version",
                    "plan_step_partition",
                    "relationship_closure_verified",
                }.issubset(columns)

                m75.downgrade()
                m74.downgrade()
                tables = set(sa.inspect(connection).get_table_names())
                assert not (_NEW_TABLES & tables)
                columns = {
                    c["name"] for c in sa.inspect(connection).get_columns("dev_runs")
                }
                assert "contract_generation" not in columns
                assert "plan_step_partition" not in columns
                assert "relationship_closure_verified" not in columns
                assert "dev_runs" in tables  # 0068's tables are untouched

                m74.upgrade()
                m75.upgrade()
                tables = set(sa.inspect(connection).get_table_names())
                assert _NEW_TABLES.issubset(tables)
    finally:
        engine.dispose()


def test_0074_dev_runs_contract_generation_defaults_v1_without_backfill() -> None:
    """Every pre-existing row is legacy the instant the column exists."""

    m68 = _load("0068_add_ask_dev_persistence")
    m69 = _load("0069_add_ask_dev_admission_indexes")
    m74 = _load("0074_add_ask_dev_v2_persistence")

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as connection:
            _parent_and_v1_tables(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                m68.upgrade()
                m69.upgrade()

            org_id, user_id, conv_id, run_id = (str(uuid.uuid4()) for _ in range(4))
            connection.execute(
                sa.text("INSERT INTO organizations (id, name) VALUES (:id, 'o')"),
                {"id": org_id},
            )
            connection.execute(
                sa.text("INSERT INTO users (id, email) VALUES (:id, 'u@example.com')"),
                {"id": user_id},
            )
            connection.execute(
                sa.text(
                    "INSERT INTO dev_conversations "
                    "(id, org_id, user_id, current_scope, retention_days, "
                    "created_at, updated_at) VALUES "
                    "(:id, :org_id, :user_id, '{}', 30, CURRENT_TIMESTAMP, "
                    "CURRENT_TIMESTAMP)"
                ),
                {"id": conv_id, "org_id": org_id, "user_id": user_id},
            )
            connection.execute(
                sa.text(
                    "INSERT INTO dev_runs "
                    "(id, request_id, conversation_id, org_id, user_id, state, "
                    "started_at, created_at) VALUES "
                    "(:id, :req, :conv, :org_id, :user_id, 'accepted', "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {
                    "id": run_id,
                    "req": str(uuid.uuid4()),
                    "conv": conv_id,
                    "org_id": org_id,
                    "user_id": user_id,
                },
            )
            connection.commit()

            with Operations.context(context):
                m74.upgrade()
            row = connection.execute(
                sa.text(
                    "SELECT contract_generation, public_outcome, "
                    "plan_step_partition, relationship_closure_verified "
                    "FROM dev_runs WHERE id = :id"
                ),
                {"id": run_id},
            ).one()
            assert row.contract_generation == "v1"
            assert row.public_outcome is None
            assert row.plan_step_partition is None
            assert row.relationship_closure_verified is None
    finally:
        engine.dispose()


def test_0074_downgrade_refuses_with_folded_column_only_data() -> None:
    """Codex-review finding (CHAOS-3299): the downgrade guard must sweep
    ``plan_step_partition``/``relationship_closure_verified`` independently
    of ``contract_generation`` tagging.

    ``DevPersistenceService.record_investigation_result`` can populate these
    two folded ``dev_runs`` columns before a run ever reaches
    ``record_frame`` -- the call that tags ``contract_generation = 'v2'`` --
    e.g. a failure between the investigation and frame-synthesis stages. A
    run in that state is still tagged 'v1' and would otherwise silently
    pass the existing ``contract_generation = 'v2'`` guard while carrying
    real v2 data. Fail-before/pass-after: the guard must raise while the
    column is populated, and the same downgrade must then succeed once it is
    cleared.
    """

    m68 = _load("0068_add_ask_dev_persistence")
    m69 = _load("0069_add_ask_dev_admission_indexes")
    m74 = _load("0074_add_ask_dev_v2_persistence")

    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as connection:
            _parent_and_v1_tables(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                m68.upgrade()
                m69.upgrade()

            org_id, user_id, conv_id, run_id = (str(uuid.uuid4()) for _ in range(4))
            connection.execute(
                sa.text("INSERT INTO organizations (id, name) VALUES (:id, 'o')"),
                {"id": org_id},
            )
            connection.execute(
                sa.text("INSERT INTO users (id, email) VALUES (:id, 'u@example.com')"),
                {"id": user_id},
            )
            connection.execute(
                sa.text(
                    "INSERT INTO dev_conversations "
                    "(id, org_id, user_id, current_scope, retention_days, "
                    "created_at, updated_at) VALUES "
                    "(:id, :org_id, :user_id, '{}', 30, CURRENT_TIMESTAMP, "
                    "CURRENT_TIMESTAMP)"
                ),
                {"id": conv_id, "org_id": org_id, "user_id": user_id},
            )
            connection.execute(
                sa.text(
                    "INSERT INTO dev_runs "
                    "(id, request_id, conversation_id, org_id, user_id, state, "
                    "started_at, created_at) VALUES "
                    "(:id, :req, :conv, :org_id, :user_id, 'accepted', "
                    "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {
                    "id": run_id,
                    "req": str(uuid.uuid4()),
                    "conv": conv_id,
                    "org_id": org_id,
                    "user_id": user_id,
                },
            )
            connection.commit()

            with Operations.context(context):
                m74.upgrade()

            # The run is still tagged 'v1' (the default) but carries a
            # folded-column fact -- exactly the record_investigation_result-
            # without-record_frame scenario the guard exists to catch.
            connection.execute(
                sa.text(
                    "UPDATE dev_runs SET plan_step_partition = "
                    '\'{"completed":["step_a"],"skipped":[],"failed":[]}\' '
                    "WHERE id = :id"
                ),
                {"id": run_id},
            )
            connection.commit()
            row = connection.execute(
                sa.text(
                    "SELECT contract_generation, plan_step_partition "
                    "FROM dev_runs WHERE id = :id"
                ),
                {"id": run_id},
            ).one()
            assert row.contract_generation == "v1"
            assert row.plan_step_partition is not None

            with Operations.context(context):
                with pytest.raises(RuntimeError, match="plan_step_partition"):
                    m74.downgrade()

            connection.execute(
                sa.text(
                    "UPDATE dev_runs SET plan_step_partition = NULL WHERE id = :id"
                ),
                {"id": run_id},
            )
            connection.commit()
            with Operations.context(context):
                m74.downgrade()  # now succeeds
            tables = set(sa.inspect(connection).get_table_names())
            assert not (_NEW_TABLES & tables)
    finally:
        engine.dispose()


@pytest.mark.skipif(
    not os.getenv(_POSTGRES_URI_ENV), reason=f"requires {_POSTGRES_URI_ENV}"
)
def test_live_postgres_check_constraints_and_not_valid_validate_split() -> None:
    """CHECK-constraint enforcement and the NOT VALID/VALIDATE split, live.

    SQLite silently accepts anything through the ``ck_dev_runs_*`` string
    checks (they're not enforced at all without STRICT tables); only a live
    engine proves the constraint text is real and installed correctly.
    """

    from sqlalchemy.engine import make_url

    configured = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    sync_url = configured.set(drivername="postgresql+psycopg2")
    schema = f"ask_dev_v2_migration_{uuid.uuid4().hex}"

    admin_engine = sa.create_engine(sync_url)
    try:
        with admin_engine.begin() as connection:
            connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        engine = sa.create_engine(
            sync_url, connect_args={"options": f"-csearch_path={schema}"}
        )
        try:
            m68 = _load("0068_add_ask_dev_persistence")
            m69 = _load("0069_add_ask_dev_admission_indexes")
            m74 = _load("0074_add_ask_dev_v2_persistence")
            m75 = _load("0075_validate_ask_dev_v2_constraints")
            with engine.connect() as connection:
                with connection.begin():
                    _parent_and_v1_tables(connection)
                    context = MigrationContext.configure(connection)
                    with Operations.context(context):
                        m68.upgrade()
                        m69.upgrade()
                        m74.upgrade()
                        m75.upgrade()

                        # Constraints are marked VALID after 0075.
                        validated = connection.execute(
                            sa.text(
                                "SELECT conname, convalidated FROM pg_constraint "
                                f"WHERE conrelid = '{schema}.dev_runs'::regclass "
                                "AND conname IN "
                                "('ck_dev_runs_contract_generation', "
                                "'ck_dev_runs_public_outcome')"
                            )
                        ).fetchall()
                        assert {
                            (row.conname, row.convalidated) for row in validated
                        } == {
                            ("ck_dev_runs_contract_generation", True),
                            ("ck_dev_runs_public_outcome", True),
                        }

                        # New-table CHECK constraints reject an
                        # out-of-vocabulary source_class.
                        org_id = connection.execute(
                            sa.text(
                                "INSERT INTO organizations (id, name) "
                                "VALUES (gen_random_uuid(), 'o') RETURNING id"
                            )
                        ).scalar()
                        user_id = connection.execute(
                            sa.text(
                                "INSERT INTO users (id, email) VALUES "
                                "(gen_random_uuid(), 'u@example.com') "
                                "RETURNING id"
                            )
                        ).scalar()
                        conv_id = connection.execute(
                            sa.text(
                                "INSERT INTO dev_conversations (id, org_id, "
                                "user_id, current_scope, retention_days, "
                                "created_at, updated_at) VALUES "
                                "(gen_random_uuid(), :org_id, :user_id, "
                                "'{}', 30, now(), now()) RETURNING id"
                            ),
                            {"org_id": org_id, "user_id": user_id},
                        ).scalar()
                        run_id = connection.execute(
                            sa.text(
                                "INSERT INTO dev_runs (id, request_id, "
                                "conversation_id, org_id, user_id, state, "
                                "started_at, created_at) VALUES "
                                "(gen_random_uuid(), gen_random_uuid(), "
                                ":conv_id, :org_id, :user_id, 'accepted', "
                                "now(), now()) RETURNING id"
                            ),
                            {
                                "conv_id": conv_id,
                                "org_id": org_id,
                                "user_id": user_id,
                            },
                        ).scalar()

                        # CHECK constraints reject an out-of-vocabulary value.
                        with pytest.raises(sa.exc.IntegrityError):
                            with connection.begin_nested():
                                connection.execute(
                                    sa.text(
                                        "UPDATE dev_runs SET "
                                        "contract_generation = 'v3' "
                                        "WHERE id = :run_id"
                                    ),
                                    {"run_id": run_id},
                                )
                        with pytest.raises(sa.exc.IntegrityError):
                            with connection.begin_nested():
                                connection.execute(
                                    sa.text(
                                        "UPDATE dev_runs SET "
                                        "public_outcome = 'bogus_outcome' "
                                        "WHERE id = :run_id"
                                    ),
                                    {"run_id": run_id},
                                )

                        with pytest.raises(sa.exc.IntegrityError):
                            with connection.begin_nested():
                                connection.execute(
                                    sa.text(
                                        "INSERT INTO dev_run_source_observations "
                                        "(id, run_id, org_id, user_id, ordinal, "
                                        "observation_id, source_class, "
                                        "requirement_level, observed_state, "
                                        "data_semantics, usable_fact_count, "
                                        "subject_coverage, payload, "
                                        "observed_at) VALUES "
                                        "(gen_random_uuid(), :run_id, :org_id, "
                                        ":user_id, 0, gen_random_uuid(), "
                                        "'not_a_real_source_class', "
                                        "'mandatory', 'available_current', "
                                        "'measured_zero', 0, 1.0, '{}', now())"
                                    ),
                                    {
                                        "run_id": run_id,
                                        "org_id": org_id,
                                        "user_id": user_id,
                                    },
                                )

                        m75.downgrade()
                        m74.downgrade()
                    tables = set(sa.inspect(connection).get_table_names(schema=schema))
                    assert not (_NEW_TABLES & tables)
        finally:
            engine.dispose()
    finally:
        with admin_engine.begin() as connection:
            connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()


@pytest.mark.skipif(
    not os.getenv(_POSTGRES_URI_ENV), reason=f"requires {_POSTGRES_URI_ENV}"
)
def test_live_postgres_downgrade_refuses_when_v2_data_present() -> None:
    """Observe the pre-release-rehearsal-only guard fail, then pass.

    fail-before/pass-after pair for the safety check itself: a plain
    ``op.drop_table`` sequence (no guard) would silently discard the row;
    the guard must raise instead.
    """

    from sqlalchemy.engine import make_url

    configured = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    sync_url = configured.set(drivername="postgresql+psycopg2")
    schema = f"ask_dev_v2_downgrade_guard_{uuid.uuid4().hex}"

    admin_engine = sa.create_engine(sync_url)
    try:
        with admin_engine.begin() as connection:
            connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
        engine = sa.create_engine(
            sync_url, connect_args={"options": f"-csearch_path={schema}"}
        )
        try:
            m68 = _load("0068_add_ask_dev_persistence")
            m69 = _load("0069_add_ask_dev_admission_indexes")
            m74 = _load("0074_add_ask_dev_v2_persistence")
            with engine.connect() as connection:
                with connection.begin():
                    _parent_and_v1_tables(connection)
                    context = MigrationContext.configure(connection)
                    with Operations.context(context):
                        m68.upgrade()
                        m69.upgrade()
                        m74.upgrade()
                with connection.begin():
                    org_id = connection.execute(
                        sa.text(
                            "INSERT INTO organizations (id, name) VALUES "
                            "(gen_random_uuid(), 'o') RETURNING id"
                        )
                    ).scalar()
                    user_id = connection.execute(
                        sa.text(
                            "INSERT INTO users (id, email) VALUES "
                            "(gen_random_uuid(), 'u@example.com') RETURNING id"
                        )
                    ).scalar()
                    conv_id = connection.execute(
                        sa.text(
                            "INSERT INTO dev_conversations (id, org_id, "
                            "user_id, current_scope, retention_days, "
                            "created_at, updated_at) VALUES "
                            "(gen_random_uuid(), :org_id, :user_id, '{}', "
                            "30, now(), now()) RETURNING id"
                        ),
                        {"org_id": org_id, "user_id": user_id},
                    ).scalar()
                    connection.execute(
                        sa.text(
                            "INSERT INTO dev_runs (id, request_id, "
                            "conversation_id, org_id, user_id, state, "
                            "contract_generation, started_at, created_at) "
                            "VALUES (gen_random_uuid(), gen_random_uuid(), "
                            ":conv_id, :org_id, :user_id, 'accepted', 'v2', "
                            "now(), now())"
                        ),
                        {"conv_id": conv_id, "org_id": org_id, "user_id": user_id},
                    )
                with connection.begin():
                    context = MigrationContext.configure(connection)
                    with Operations.context(context):
                        with pytest.raises(RuntimeError, match="contract_generation"):
                            m74.downgrade()
        finally:
            engine.dispose()
    finally:
        with admin_engine.begin() as connection:
            connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()
