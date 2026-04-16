"""Guard: ensure composite audit-log index stays in the initial migration.

The index ``ix_audit_logs_org_action_created`` was added to migration 0001 to
support the ``(org_id, action, created_at)`` query pattern. Without it those
queries fall back to the wider ``ix_audit_logs_org_created`` index or a full
scan. This test asserts the composite index is still referenced in both the
SQLAlchemy model and the initial migration file so a future edit cannot
silently remove it.
"""

from __future__ import annotations

from pathlib import Path


INITIAL_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "alembic"
    / "versions"
    / "0001_initial_schema.py"
)


def test_initial_migration_has_audit_composite_index():
    src = INITIAL_MIGRATION.read_text(encoding="utf-8")
    assert "ix_audit_logs_org_action_created" in src, (
        "Composite audit-log index ix_audit_logs_org_action_created is missing "
        "from the initial migration. This index was added to support the "
        "(org_id, action, created_at) query pattern and must not be removed."
    )


def test_audit_model_declares_composite_index():
    from dev_health_ops.models.audit import AuditLog

    index_names = [getattr(ix, "name", None) for ix in AuditLog.__table_args__]
    assert "ix_audit_logs_org_action_created" in index_names, (
        "AuditLog.__table_args__ no longer declares the composite "
        "ix_audit_logs_org_action_created index."
    )
