from __future__ import annotations

from typing import Any

from dev_health_ops.workers import sync_batch


def test_batch_repo_options_preserve_autoimport_and_scope_child() -> None:
    result = sync_batch._repo_sync_options(
        provider="github",
        sync_targets=["git", "work-items"],
        sync_options={
            "auto_import_teams": True,
            "discover": True,
            "all_repos": True,
            "batch_size": 10,
            "search": "full-chaos/*",
        },
        repo_tuple=("full-chaos", "dev-health"),
    )

    assert result == {
        "auto_import_teams": True,
        "search": "full-chaos/dev-health",
        "owner": "full-chaos",
        "repo": "dev-health",
    }


def test_batch_child_autoimport_true_calls_once(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_team_autoimport(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(sync_batch, "run_team_autoimport", fake_run_team_autoimport)

    result = sync_batch._run_team_autoimport_for_batch_child(
        provider="github",
        org_id="org-1",
        credentials={"token": "secret"},
        sync_options={
            "auto_import_teams": True,
            "owner": "full-chaos",
            "repo": "dev-health",
        },
        sync_targets=["git"],
        config_id="cfg-1",
        triggered_by="manual",
        analytics_db_url="clickhouse://batch-dsn",
    )

    assert result == {"status": "success"}
    assert len(calls) == 1
    assert calls[0]["analytics_db_url"] == "clickhouse://batch-dsn"
    assert calls[0]["scope"] == {
        "mode": "batch_child",
        "sync_config_id": "cfg-1",
        "sync_targets": ["git"],
        "sync_options": {
            "auto_import_teams": True,
            "owner": "full-chaos",
            "repo": "dev-health",
        },
        "triggered_by": "manual",
    }
