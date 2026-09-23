from __future__ import annotations

import importlib

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from tests._alembic_heads import application_schema_head


def test_migration_0047_chains_after_outbox_and_is_reversible():
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0047_add_worker_operator_audits"
    )
    assert migration.revision == "0047"
    assert migration.down_revision == "0046"

    engine = sa.create_engine("sqlite:///:memory:")
    metadata = sa.MetaData()
    sa.Table(
        "internal_service_credentials",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
    )
    try:
        with engine.connect() as connection:
            metadata.create_all(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                migration.upgrade()
                columns = {
                    column["name"]
                    for column in sa.inspect(connection).get_columns(
                        "worker_operator_audits"
                    )
                }
                assert columns == {
                    "id",
                    "credential_id",
                    "principal_type",
                    "principal_id",
                    "action",
                    "resource_type",
                    "resource_id",
                    "reason_code",
                    "correlation_id",
                    "status",
                    "created_at",
                    "completed_at",
                }
                migration.downgrade()
                assert "worker_operator_audits" not in set(
                    sa.inspect(connection).get_table_names()
                )
    finally:
        engine.dispose()


_next_audit_id = iter(range(1, 1000))


def _insert_audit(connection, principal_type: str) -> None:
    # An explicit id: SQLite does not auto-number a BigInteger key, so an
    # omitted id would fail NOT NULL and hide which check refused the row.
    connection.execute(
        sa.text(
            "INSERT INTO worker_operator_audits (id, credential_id, principal_type, "
            "principal_id, action, resource_type, resource_id, reason_code, "
            "correlation_id, status, created_at) VALUES (:id, NULL, :type, "
            "'dho-workers', 'jobs.cancel', 'job', '1', 'operator_test', 'c1', "
            "'started', '2026-09-23T00:00:00Z')"
        ),
        {"id": next(_next_audit_id), "type": principal_type},
    )


def test_migration_0136_allows_the_operator_principal_and_is_reversible():
    """0136 widens only the principal-type check: the operator principal
    (`dho workers`, no credential) is refused before it and accepted after
    it; an unknown type stays refused; downgrade restores the old check."""
    base = importlib.import_module(
        "dev_health_ops.alembic.versions.0047_add_worker_operator_audits"
    )
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0136_worker_operator_audits_operator_principal"
    )
    assert migration.revision == "0136"
    assert migration.down_revision == "0135"
    # 0138 (the direct-write action check) supersedes the head check:
    # derived, not typed (tests/_alembic_heads.py); the next migration
    # author moves it.
    action_check = importlib.import_module(
        "dev_health_ops.alembic.versions.0137_worker_operator_audits_action_check"
    )
    assert action_check.down_revision == "0136"
    direct_write_check = importlib.import_module(
        "dev_health_ops.alembic.versions.0138_worker_operator_audits_direct_write_actions"
    )
    assert direct_write_check.down_revision == "0137"
    assert direct_write_check.revision == application_schema_head()

    engine = sa.create_engine("sqlite:///:memory:")
    metadata = sa.MetaData()
    sa.Table(
        "internal_service_credentials",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
    )
    try:
        with engine.connect() as connection:
            metadata.create_all(connection)
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                base.upgrade()
                with pytest.raises(sa.exc.IntegrityError, match="principal_type"):
                    with connection.begin_nested():
                        _insert_audit(connection, "operator")
                migration.upgrade()
                _insert_audit(connection, "operator")
                _insert_audit(connection, "service_credential")
                with pytest.raises(sa.exc.IntegrityError, match="principal_type"):
                    with connection.begin_nested():
                        _insert_audit(connection, "user")
                # Downgrade refuses while an operator row exists, and keeps
                # both the row and the widened check.
                with pytest.raises(sa.exc.IntegrityError, match="principal_type"):
                    with connection.begin_nested():
                        migration.downgrade()
                assert (
                    connection.execute(
                        sa.text(
                            "SELECT count(*) FROM worker_operator_audits "
                            "WHERE principal_type = 'operator'"
                        )
                    ).scalar()
                    == 1
                )
                _insert_audit(connection, "operator")
                connection.execute(
                    sa.text(
                        "DELETE FROM worker_operator_audits "
                        "WHERE principal_type = 'operator'"
                    )
                )
                migration.downgrade()
                with pytest.raises(sa.exc.IntegrityError, match="principal_type"):
                    with connection.begin_nested():
                        _insert_audit(connection, "operator")
    finally:
        engine.dispose()
