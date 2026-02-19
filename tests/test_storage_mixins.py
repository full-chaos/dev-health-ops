from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dev_health_ops.storage.mixins.cicd import CicdMixin
from dev_health_ops.storage.mixins.git import GitDataMixin
from dev_health_ops.storage.mixins.work_item import WorkItemMixin


class _DummyStore(GitDataMixin, CicdMixin, WorkItemMixin):
    def __init__(self):
        self.calls = []
        self._work_items_table = "work_items"
        self._work_item_dependencies_table = "work_item_dependencies"
        self._work_graph_issue_pr_table = "work_graph_issue_pr"
        self._work_graph_pr_commit_table = "work_graph_pr_commit"
        self._work_item_transitions_table = "work_item_transitions"

    async def _upsert_many(self, model, rows, conflict_columns, update_columns):
        self.calls.append(
            {
                "model": model,
                "rows": rows,
                "conflict_columns": conflict_columns,
                "update_columns": update_columns,
            }
        )


@pytest.mark.asyncio
async def test_git_commit_stats_defaults_file_modes_for_dict_rows():
    store = _DummyStore()

    await store.insert_git_commit_stats(
        [
            {
                "repo_id": uuid4(),
                "commit_hash": "abc",
                "file_path": "x.py",
                "additions": 1,
                "deletions": 2,
                "old_file_mode": None,
                "new_file_mode": None,
            }
        ]
    )

    row = store.calls[0]["rows"][0]
    assert row["old_file_mode"] == "unknown"
    assert row["new_file_mode"] == "unknown"
    assert isinstance(row["last_synced"], datetime)


@pytest.mark.asyncio
async def test_insert_work_items_normalizes_repo_id_and_optional_values():
    store = _DummyStore()
    repo_id = uuid4()

    await store.insert_work_items(
        [
            {
                "work_item_id": "WI-1",
                "repo_id": repo_id,
                "provider": "jira",
                "title": "Do work",
                "type": "Story",
                "status": "In Progress",
            }
        ]
    )

    payload = store.calls[0]
    row = payload["rows"][0]
    assert payload["model"] == "work_items"
    assert row["repo_id"] == str(repo_id)
    assert row["assignees"] == []
    assert row["labels"] == []
    assert row["story_points"] is None


@pytest.mark.asyncio
async def test_insert_work_graph_links_force_repo_id_string_and_last_synced():
    store = _DummyStore()
    repo_id = uuid4()

    await store.insert_work_graph_issue_pr(
        [
            {
                "repo_id": repo_id,
                "work_item_id": "WI-1",
                "pr_number": 1,
                "confidence": 0.8,
                "provenance": "heuristic",
                "evidence": {},
            }
        ]
    )
    await store.insert_work_graph_pr_commit(
        [
            {
                "repo_id": repo_id,
                "pr_number": 1,
                "commit_hash": "abc",
                "confidence": 0.9,
                "provenance": "explicit",
                "evidence": {},
            }
        ]
    )

    first = store.calls[0]["rows"][0]
    second = store.calls[1]["rows"][0]
    assert first["repo_id"] == str(repo_id)
    assert second["repo_id"] == str(repo_id)
    assert isinstance(first["last_synced"], datetime)
    assert isinstance(second["last_synced"], datetime)


@pytest.mark.asyncio
async def test_cicd_mixin_short_circuits_empty_inputs():
    store = _DummyStore()

    await store.insert_ci_pipeline_runs([])
    await store.insert_deployments([])
    await store.insert_incidents([])

    assert store.calls == []


@pytest.mark.asyncio
async def test_cicd_mixin_accepts_object_inputs():
    store = _DummyStore()
    repo_id = uuid4()

    run = SimpleNamespace(
        repo_id=repo_id,
        run_id="run-1",
        status="success",
        queued_at=None,
        started_at=datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 2, 18, 10, 5, tzinfo=timezone.utc),
        last_synced=None,
    )

    await store.insert_ci_pipeline_runs([run])

    payload = store.calls[0]
    assert payload["rows"][0]["repo_id"] == repo_id
    assert payload["rows"][0]["run_id"] == "run-1"
    assert isinstance(payload["rows"][0]["last_synced"], datetime)
