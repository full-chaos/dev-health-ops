from datetime import datetime, timezone

import pytest

from dev_health_ops.audit.completeness import (
    build_git_commits_query,
    build_git_pull_requests_query,
    build_transitions_query,
    build_work_items_query,
    compile_report,
    infer_repo_source,
)


@pytest.mark.parametrize(
    "builder, expected_table",
    [
        (build_work_items_query, "FROM work_items"),
        (build_transitions_query, "FROM work_item_transitions"),
        (build_git_commits_query, "FROM git_commits"),
        (build_git_pull_requests_query, "FROM git_pull_requests"),
    ],
)
def test_query_builders_include_tables(builder, expected_table):
    query = builder()
    assert expected_table in query
    assert "countIf" in query


def test_infer_repo_source_prefers_settings():
    assert infer_repo_source('{"source": "gitlab"}', '["github"]') == "gitlab"


def test_infer_repo_source_falls_back_to_tags():
    assert infer_repo_source("{}", '["GitHub", "misc"]') == "github"
    assert infer_repo_source(None, '["gitlab"]') == "gitlab"
    assert infer_repo_source("{}", "[]") == "unknown"


def test_compile_report_aggregates_and_flags_ok():
    window_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    window_end = datetime(2025, 2, 1, tzinfo=timezone.utc)
    repo_id_github = "11111111-1111-1111-1111-111111111111"
    repo_id_gitlab = "22222222-2222-2222-2222-222222222222"

    work_item_rows = [
        {"provider": "jira", "count": 3, "last_synced": window_end},
        {"provider": "github", "count": 2, "last_synced": window_end},
        {"provider": "gitlab", "count": 1, "last_synced": window_end},
    ]
    transition_rows = [
        {"provider": "jira", "count": 2, "last_synced": window_end},
        {"provider": "github", "count": 1, "last_synced": window_end},
        {"provider": "gitlab", "count": 1, "last_synced": window_end},
    ]
    repo_rows = [
        {"id": repo_id_github, "settings": '{"source":"github"}', "tags": "[]"},
        {"id": repo_id_gitlab, "settings": None, "tags": '["gitlab"]'},
    ]
    git_rows_by_table = {
        "git_commits": [
            {"repo_id": repo_id_github, "count": 5, "last_synced": window_end},
            {"repo_id": repo_id_gitlab, "count": 0, "last_synced": window_start},
        ],
        "git_pull_requests": [
            {"repo_id": repo_id_github, "count": 0, "last_synced": window_start},
            {"repo_id": repo_id_gitlab, "count": 2, "last_synced": window_end},
        ],
    }
    present_tables = {
        "repos": True,
        "work_items": True,
        "work_item_transitions": True,
        "git_commits": True,
        "git_pull_requests": True,
        "deployments": False,
        "incidents": False,
        "ci_pipeline_runs": False,
    }

    report = compile_report(
        window_start=window_start,
        window_end=window_end,
        window_days=31,
        work_item_rows=work_item_rows,
        transition_rows=transition_rows,
        repo_rows=repo_rows,
        git_rows_by_table=git_rows_by_table,
        present_tables=present_tables,
    )

    assert report["providers"]["jira"]["ok"] is True
    assert report["providers"]["github"]["ok"] is True
    assert report["providers"]["gitlab"]["ok"] is True
    assert "synthetic" in report["providers"]
    assert report["git_sources"]["github"]["git_commits"]["count"] == 5
    assert report["git_sources"]["gitlab"]["git_pull_requests"]["count"] == 2
    assert report["overall_ok"] is True


def test_compile_report_flags_stale_provider():
    window_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    window_end = datetime(2025, 2, 1, tzinfo=timezone.utc)
    stale_time = datetime(2024, 12, 1, tzinfo=timezone.utc)

    report = compile_report(
        window_start=window_start,
        window_end=window_end,
        window_days=31,
        work_item_rows=[{"provider": "jira", "count": 2, "last_synced": stale_time}],
        transition_rows=[{"provider": "jira", "count": 1, "last_synced": stale_time}],
        repo_rows=[],
        git_rows_by_table={},
        present_tables={
            "repos": True,
            "work_items": True,
            "work_item_transitions": True,
            "git_commits": False,
            "git_pull_requests": False,
            "deployments": False,
            "incidents": False,
            "ci_pipeline_runs": False,
        },
    )

    jira = report["providers"]["jira"]
    assert jira["ok"] is False
    assert "work_items_stale" in jira["issues"]
    assert "transitions_stale" in jira["issues"]
