from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.processors import sync as sync_mod


def _ns(**overrides):
    base = dict(
        provider="github",
        sync_target="git",
        auth="token",
        db="clickhouse://localhost:8123/stats",
        db_type="clickhouse",
        org="default",
        repo_path=".",
        owner="octo",
        repo="repo",
        project_id=123,
        gitlab_url="https://gitlab.com",
        group=None,
        search=None,
        batch_size=10,
        max_concurrent=4,
        rate_limit_delay=1.0,
        max_repos=None,
        use_async=False,
        max_commits_per_repo=None,
        since=None,
        date=None,
        backfill=1,
        repo_name=None,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


@pytest.mark.parametrize(
    "target,expected",
    [
        ("git", {"sync_git": True, "sync_prs": False, "blame_only": False}),
        ("prs", {"sync_git": False, "sync_prs": True, "blame_only": False}),
        ("blame", {"sync_git": False, "sync_prs": False, "blame_only": True}),
        ("cicd", {"sync_cicd": True}),
        ("deployments", {"sync_deployments": True}),
        ("incidents", {"sync_incidents": True}),
    ],
)
def test_sync_flags_for_target(target, expected):
    flags = sync_mod._sync_flags_for_target(target)
    for key, value in expected.items():
        assert flags[key] is value


def test_resolve_synthetic_repo_name_defaults_and_variants():
    assert sync_mod._resolve_synthetic_repo_name(_ns(repo_name="team/repo")) == "team/repo"
    assert sync_mod._resolve_synthetic_repo_name(_ns(repo_name=None, owner="a", repo="b")) == "a/b"
    assert sync_mod._resolve_synthetic_repo_name(_ns(repo_name=None, owner=None, repo=None, search="org/repo")) == "org/repo"
    assert (
        sync_mod._resolve_synthetic_repo_name(
            _ns(repo_name=None, owner=None, repo=None, search=None)
        )
        == "acme/demo-app"
    )


def test_resolve_synthetic_repo_name_rejects_pattern_search():
    with pytest.raises(SystemExit, match="does not support pattern search"):
        sync_mod._resolve_synthetic_repo_name(
            _ns(repo_name=None, owner=None, repo=None, search="org/*")
        )


@pytest.mark.asyncio
async def test_sync_local_target_blame_calls_local_blame(monkeypatch):
    ns = _ns(provider="local", repo_path="/repo")
    process_local_blame = AsyncMock()

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id):
        assert org_id == "default"
        await handler(SimpleNamespace())

    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "resolve_db_type", lambda _uri, _db_type: "clickhouse")
    monkeypatch.setattr(sync_mod, "_resolve_since", lambda _ns: "2026-01-01")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "process_local_blame", process_local_blame)

    result = await sync_mod.sync_local_target(ns, "blame")

    assert result == 0
    process_local_blame.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_github_target_batch_mode_calls_batch_processor(monkeypatch):
    ns = _ns(search="org/*", owner=None, repo=None, group="org")
    batch = AsyncMock()

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id):
        assert org_id == "default"
        await handler(SimpleNamespace())

    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "resolve_db_type", lambda _uri, _db_type: "clickhouse")
    monkeypatch.setattr(sync_mod, "_resolve_since", lambda _ns: "2026-01-01")
    monkeypatch.setattr(sync_mod, "_resolve_max_commits", lambda _ns: 50)
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "process_github_repos_batch", batch)

    result = await sync_mod.sync_github_target(ns, "git")

    assert result == 0
    batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_github_target_requires_owner_repo_without_search(monkeypatch):
    ns = _ns(owner=None, repo=None, search=None)

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id == "default"
        await handler(SimpleNamespace())

    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "resolve_db_type", lambda _uri, _db_type: "clickhouse")
    monkeypatch.setattr(sync_mod, "_resolve_since", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "_resolve_max_commits", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)

    with pytest.raises(SystemExit, match="requires --owner and --repo"):
        await sync_mod.sync_github_target(ns, "git")


@pytest.mark.asyncio
async def test_sync_gitlab_target_requires_project_id_without_search(monkeypatch):
    ns = _ns(provider="gitlab", project_id=None, search=None)

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id == "default"
        await handler(SimpleNamespace())

    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "resolve_db_type", lambda _uri, _db_type: "clickhouse")
    monkeypatch.setattr(sync_mod, "_resolve_since", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "_resolve_max_commits", lambda _ns: 10)
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)

    with pytest.raises(SystemExit, match="requires --project-id"):
        await sync_mod.sync_gitlab_target(ns, "git")


def test_run_sync_target_rejects_invalid_provider_or_target():
    with pytest.raises(SystemExit, match="Provider must be one of"):
        sync_mod.run_sync_target(_ns(provider="bogus"))

    with pytest.raises(SystemExit, match="Sync target must be"):
        sync_mod.run_sync_target(_ns(sync_target="bogus"))


def test_run_sync_target_routes_to_local_provider(monkeypatch):
    ns = _ns(provider="local", sync_target="git")
    local_target = AsyncMock(return_value=0)

    def fake_run(coro):
        coro.close()
        return 0

    monkeypatch.setattr(sync_mod, "sync_local_target", local_target)
    monkeypatch.setattr(sync_mod.asyncio, "run", fake_run)

    assert sync_mod.run_sync_target(ns) == 0
    local_target.assert_called_once_with(ns, "git")
