from __future__ import annotations

from pathlib import Path

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
    / "070_acr_branch_digests.sql"
)


def test_migration_adds_indexed_branch_digests_for_every_acr_branch_source() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    expected_columns = {
        "repos": ("ref_sha256", "ifNull(ref, '')"),
        "git_pull_requests": (
            "head_branch_sha256",
            "ifNull(head_branch, '')",
        ),
        "git_pull_requests.base": (
            "base_branch_sha256",
            "ifNull(base_branch, '')",
        ),
        "ci_pipeline_runs": ("branch_sha256", "ifNull(branch, '')"),
        "file_complexity_snapshots": ("ref_sha256", "ref"),
    }
    for source, (column, expression) in expected_columns.items():
        table = source.removesuffix(".base")
        assert f"ALTER TABLE {table}" in sql
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
        assert f"lower(hex(SHA256({expression})))" in sql
        assert f"ADD INDEX IF NOT EXISTS idx_{table}_{column}" in sql
        assert f"MATERIALIZE INDEX IF EXISTS idx_{table}_{column}" in sql


def test_migration_adds_digest_snapshot_projection() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "ADD PROJECTION IF NOT EXISTS prj_acr_file_complexity_digest_runs" in sql
    assert "SELECT org_id, repo_id, ref_sha256, ref, as_of_day, computed_at" in sql
    assert "GROUP BY org_id, repo_id, ref_sha256, ref, as_of_day, computed_at" in sql
    assert "MATERIALIZE PROJECTION IF EXISTS prj_acr_file_complexity_digest_runs" in sql
