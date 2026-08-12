from pathlib import Path

from dev_health_ops.models.ai_attribution import (
    SOURCE_PRECEDENCE,
    AIAttributionSource,
)

MIGRATION = Path(
    "src/dev_health_ops/migrations/clickhouse/076_ai_attribution_issue_label.sql"
)


def test_issue_label_source_has_distinct_typed_precedence() -> None:
    assert AIAttributionSource.ISSUE_LABEL.value == "issue_label"
    assert SOURCE_PRECEDENCE[AIAttributionSource.MANUAL] == 1
    assert SOURCE_PRECEDENCE[AIAttributionSource.ISSUE_LABEL] == 2
    assert SOURCE_PRECEDENCE[AIAttributionSource.PR_LABEL] == 3
    assert len(SOURCE_PRECEDENCE.values()) == len(set(SOURCE_PRECEDENCE.values()))


def test_migration_076_keeps_repo_scoped_resolution_and_issue_label_priority() -> None:
    sql = MIGRATION.read_text()
    assert "source = 'issue_label',     2" in sql
    assert "source = 'pr_label',        3" in sql
    assert "PARTITION BY org_id, subject_type, repo_id, subject_id" in sql
    assert "WHERE superseded_by IS NULL" in sql
