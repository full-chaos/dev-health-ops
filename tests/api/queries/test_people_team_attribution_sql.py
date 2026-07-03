from __future__ import annotations

from dev_health_ops.api.queries.sql_loader import load_sql


def test_person_team_uses_identity_membership_not_metric_rollups() -> None:
    sql = load_sql("people/person_team.sql")

    assert "FROM identities FINAL" in sql
    assert "arrayJoin(team_ids) AS team_id" in sql
    assert "provider_identities" in sql
    assert "arrayExists" in sql
    assert "JSONExtract(provider_identities, 'github', 'Array(String)')" in sql
    assert "JSONExtract(provider_identities, 'gitlab', 'Array(String)')" in sql
    assert "JSONExtract(provider_identities, 'linear', 'Array(String)')" in sql
    assert "JSONExtract(provider_identities, 'jira', 'Array(String)')" in sql
    assert "replaceRegexpOne(identity, '^jira:accountid:', '')" in sql
    assert "replaceRegexpOne(identity, '^accountid:', '')" in sql
    assert "position(provider_identities" not in sql
    assert "user_metrics_daily" not in sql
    assert "work_item_user_metrics_daily" not in sql
