from __future__ import annotations

import importlib

completion_fences = importlib.import_module(
    "dev_health_ops.alembic.versions.0063_add_worker_job_completion_fences"
)


def test_completion_fence_upgrade_is_bounded_and_canonical(monkeypatch) -> None:
    statements: list[str] = []
    monkeypatch.setattr(completion_fences.op, "execute", statements.append)

    completion_fences.upgrade()

    sql = "\n".join(statements)
    assert "CREATE TABLE worker_job_completion_fences" in sql
    assert "ADD COLUMN prerequisite_completion_key" in sql
    assert "CREATE INDEX ix_worker_job_outbox_prerequisite" in sql
    assert "length(completion_key) BETWEEN 1 AND 256" in sql
    assert "[0-9a-f]{12}" in sql


def test_completion_fence_downgrade_removes_index_column_and_table(monkeypatch) -> None:
    statements: list[str] = []
    monkeypatch.setattr(completion_fences.op, "execute", statements.append)

    completion_fences.downgrade()

    assert statements == [
        "DROP INDEX IF EXISTS ix_worker_job_outbox_prerequisite",
        "ALTER TABLE worker_job_outbox DROP COLUMN IF EXISTS prerequisite_completion_key",
        "DROP TABLE IF EXISTS worker_job_completion_fences",
    ]
