from pathlib import Path


def test_ci_acceptance_migration_is_additive_tenant_keyed_and_fail_closed() -> None:
    migration = (
        Path(__file__).parents[1]
        / "src/dev_health_ops/migrations/clickhouse/070_ci_acceptance_checks.sql"
    ).read_text()

    assert "CREATE TABLE IF NOT EXISTS ci_acceptance_checks" in migration
    assert "ReplacingMergeTree(last_synced)" in migration
    assert "ORDER BY (org_id, repo_id, run_id, check_key)" in migration
    assert "'required', 'optional', 'unknown'" in migration
    assert "'passed', 'failed', 'skipped', 'pending', 'unknown'" in migration
