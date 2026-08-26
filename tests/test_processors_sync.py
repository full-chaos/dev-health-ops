from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.fixtures.demo_identity import DEFAULT_DEMO_REPO_NAME
from dev_health_ops.processors import sync as sync_mod


def _ns(**overrides):
    base = dict(
        provider="github",
        sync_target="git",
        auth="token",
        db="clickhouse://localhost:8123/stats",
        sink="clickhouse",
        analytics_db=None,
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
        before=None,
        backfill=1,
        repo_name=None,
        defer_finalize=False,
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
    assert (
        sync_mod._resolve_synthetic_repo_name(_ns(repo_name="team/repo")) == "team/repo"
    )
    assert (
        sync_mod._resolve_synthetic_repo_name(_ns(repo_name=None, owner="a", repo="b"))
        == "a/b"
    )
    assert (
        sync_mod._resolve_synthetic_repo_name(
            _ns(repo_name=None, owner=None, repo=None, search="org/repo")
        )
        == "org/repo"
    )
    assert (
        sync_mod._resolve_synthetic_repo_name(
            _ns(repo_name=None, owner=None, repo=None, search=None)
        )
        == DEFAULT_DEMO_REPO_NAME
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

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "resolve_since_datetime", lambda _ns: "2026-01-01")
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

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "resolve_since_datetime", lambda _ns: "2026-01-01")
    monkeypatch.setattr(sync_mod, "resolve_max_commits", lambda _ns: 50)
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

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "resolve_since_datetime", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_max_commits", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)

    with pytest.raises(SystemExit, match="requires --owner and --repo"):
        await sync_mod.sync_github_target(ns, "git")


@pytest.mark.asyncio
async def test_sync_gitlab_target_requires_project_id_without_search(monkeypatch):
    ns = _ns(provider="gitlab", project_id=None, search=None)

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id == "default"
        await handler(SimpleNamespace())

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "resolve_since_datetime", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_max_commits", lambda _ns: 10)
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


@pytest.mark.parametrize("target", ["cicd", "deployments", "incidents", "tests"])
def test_sync_synthetic_target_requires_org_for_sync_run_backed_targets(
    monkeypatch, target
):
    """CHAOS-4266: cicd/deployments/incidents/tests complete a real sync_run
    scoped to an org (unlike git/prs/blame, which only write analytics rows),
    so they must fail loudly before touching any store if no org resolved."""
    ns = _ns(provider="synthetic", org=None, repo_name="acme/demo")

    async def unreachable_run_with_store(*_args, **_kwargs):
        raise AssertionError(
            "run_with_store must not be called before the org guard fires"
        )

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", unreachable_run_with_store)

    with pytest.raises(SystemExit, match="requires a resolved org"):
        import asyncio

        asyncio.run(sync_mod.sync_synthetic_target(ns, target))


@pytest.mark.parametrize("target", ["cicd", "deployments", "incidents", "tests"])
def test_sync_synthetic_target_requires_ledger_ack_env_var(monkeypatch, target):
    """CHAOS-4266 (codex round 3): _complete_synthetic_sync_run writes to the
    GLOBAL, org-unscoped CHAOS-4114 executed-proof ledger under the real
    "gitlab" provider identity -- a code comment warning against a shared
    database is not a guard, so an explicit env var is required and this must
    fail loudly, before touching any store, when it is unset."""
    ns = _ns(provider="synthetic", org="org-1", repo_name="acme/demo")
    monkeypatch.delenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", raising=False)

    async def unreachable_run_with_store(*_args, **_kwargs):
        raise AssertionError(
            "run_with_store must not be called before the ledger-ack guard fires"
        )

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", unreachable_run_with_store)

    with pytest.raises(SystemExit, match="DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN"):
        import asyncio

        asyncio.run(sync_mod.sync_synthetic_target(ns, target))


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["cicd", "deployments", "incidents", "tests"])
async def test_sync_synthetic_target_defer_finalize_skips_complete(monkeypatch, target):
    """CHAOS-4266: --defer-finalize seeds analytics rows but must NOT complete
    the sync_run or trigger the post_sync fanout -- that is exactly the
    behavior that let a remaining-metric family (dora: cicd+deployments+
    incidents) fan out against a partially-seeded org when a caller finalizes
    each target immediately after seeding it. Assert the write path still
    runs (unlike the org/ledger guards above, which fail before any store
    write) but _complete_synthetic_sync_run is never reached."""
    ns = _ns(
        provider="synthetic",
        org="org-1",
        repo_name="acme/demo",
        backfill=3,
        defer_finalize=True,
    )
    store = SimpleNamespace(
        insert_repo=AsyncMock(),
        insert_ci_pipeline_runs=AsyncMock(),
        insert_deployments=AsyncMock(),
        insert_incidents=AsyncMock(),
        insert_ci_job_runs=AsyncMock(),
        insert_test_suite_results=AsyncMock(),
        insert_test_case_results=AsyncMock(),
    )

    def unreachable_complete(**_kwargs):
        raise AssertionError(
            "_complete_synthetic_sync_run must not be called when defer_finalize is set"
        )

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        await handler(store)

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", unreachable_complete)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")

    result = await sync_mod.sync_synthetic_target(ns, target)

    assert result == 0
    store.insert_repo.assert_awaited_once()


def test_finalize_synthetic_sync_run_requires_ledger_ack_env_var(monkeypatch):
    monkeypatch.delenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", raising=False)
    complete = MagicMock(
        side_effect=AssertionError("must not be reached before the ledger-ack guard")
    )
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    ns = _ns(target="cicd", org="org-1", repo_name="acme/demo")

    with pytest.raises(SystemExit, match="DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN"):
        sync_mod.finalize_synthetic_sync_run(ns, "cicd")
    complete.assert_not_called()


def test_finalize_synthetic_sync_run_requires_org(monkeypatch):
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")
    complete = MagicMock(
        side_effect=AssertionError("must not be reached before the org guard")
    )
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    ns = _ns(target="cicd", org=None, repo_name="acme/demo")

    with pytest.raises(SystemExit, match="requires a resolved org"):
        sync_mod.finalize_synthetic_sync_run(ns, "cicd")
    complete.assert_not_called()


def test_finalize_synthetic_sync_run_rejects_target_outside_sync_run_backed_set(
    monkeypatch,
):
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")
    complete = MagicMock(
        side_effect=AssertionError("must not be reached for an unsupported target")
    )
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    ns = _ns(target="git", org="org-1", repo_name="acme/demo")

    with pytest.raises(SystemExit, match="only valid for"):
        sync_mod.finalize_synthetic_sync_run(ns, "git")
    complete.assert_not_called()


@pytest.mark.parametrize("target", ["cicd", "deployments", "incidents", "tests"])
def test_finalize_synthetic_sync_run_completes_sync_run(monkeypatch, target):
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")
    complete = MagicMock(return_value="run-id")
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    ns = _ns(target=target, org="org-1", repo_name="acme/demo", backfill=3)

    result = sync_mod.finalize_synthetic_sync_run(ns, target)

    assert result == 0
    complete.assert_called_once()
    assert complete.call_args.kwargs["org_id"] == "org-1"
    assert complete.call_args.kwargs["repo_full_name"] == "acme/demo"
    assert complete.call_args.kwargs["target"] == target
    since_at = complete.call_args.kwargs["since_at"]
    before_at = complete.call_args.kwargs["before_at"]
    assert (before_at - since_at).days == 3


@pytest.mark.asyncio
async def test_sync_synthetic_target_cicd_writes_pipeline_runs_and_completes_sync_run(
    monkeypatch,
):
    ns = _ns(provider="synthetic", org="org-1", repo_name="acme/demo", backfill=3)
    store = SimpleNamespace(
        insert_repo=AsyncMock(), insert_ci_pipeline_runs=AsyncMock()
    )
    complete = MagicMock(return_value="run-id")

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        assert org_id == "org-1"
        await handler(store)

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")

    result = await sync_mod.sync_synthetic_target(ns, "cicd")

    assert result == 0
    store.insert_ci_pipeline_runs.assert_awaited_once()
    (pipeline_runs,) = store.insert_ci_pipeline_runs.await_args.args
    assert len(pipeline_runs) > 0
    complete.assert_called_once()
    assert complete.call_args.kwargs["org_id"] == "org-1"
    assert complete.call_args.kwargs["repo_full_name"] == "acme/demo"
    assert complete.call_args.kwargs["target"] == "cicd"


@pytest.mark.asyncio
async def test_sync_synthetic_target_deployments_writes_deployments_and_completes_sync_run(
    monkeypatch,
):
    ns = _ns(provider="synthetic", org="org-1", repo_name="acme/demo", backfill=3)
    store = SimpleNamespace(insert_repo=AsyncMock(), insert_deployments=AsyncMock())
    complete = MagicMock(return_value="run-id")

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        await handler(store)

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")

    result = await sync_mod.sync_synthetic_target(ns, "deployments")

    assert result == 0
    store.insert_deployments.assert_awaited_once()
    complete.assert_called_once()
    assert complete.call_args.kwargs["target"] == "deployments"


@pytest.mark.asyncio
async def test_sync_synthetic_target_incidents_writes_incidents_and_completes_sync_run(
    monkeypatch,
):
    # SimpleNamespace is not a ClickHouseStore instance, so this exercises the
    # SQLAlchemy/legacy insert_incidents branch; the ClickHouseStore ->
    # write_operational_batch(map_issue_incidents(...)) branch is verified
    # against a real ClickHouse instance (not mockable meaningfully here) --
    # see the CHAOS-4266 handoff notes for that manual verification.
    ns = _ns(provider="synthetic", org="org-1", repo_name="acme/demo", backfill=3)
    store = SimpleNamespace(insert_repo=AsyncMock(), insert_incidents=AsyncMock())
    complete = MagicMock(return_value="run-id")

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        await handler(store)

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")

    result = await sync_mod.sync_synthetic_target(ns, "incidents")

    assert result == 0
    store.insert_incidents.assert_awaited_once()
    complete.assert_called_once()
    assert complete.call_args.kwargs["target"] == "incidents"


@pytest.mark.asyncio
async def test_sync_synthetic_target_tests_writes_job_and_test_rows_and_completes_sync_run(
    monkeypatch,
):
    ns = _ns(provider="synthetic", org="org-1", repo_name="acme/demo", backfill=3)
    store = SimpleNamespace(
        insert_repo=AsyncMock(),
        insert_ci_job_runs=AsyncMock(),
        insert_test_suite_results=AsyncMock(),
        insert_test_case_results=AsyncMock(),
    )
    complete = MagicMock(return_value="run-id")

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        await handler(store)

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setattr(sync_mod, "_complete_synthetic_sync_run", complete)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")

    result = await sync_mod.sync_synthetic_target(ns, "tests")

    assert result == 0
    store.insert_ci_job_runs.assert_awaited_once()
    store.insert_test_suite_results.assert_awaited_once()
    store.insert_test_case_results.assert_awaited_once()
    complete.assert_called_once()
    assert complete.call_args.kwargs["target"] == "tests"


@pytest.mark.asyncio
async def test_sync_synthetic_target_tests_requires_clickhouse_sink(monkeypatch):
    ns = _ns(provider="synthetic", org="org-1", repo_name="acme/demo", backfill=3)
    store = SimpleNamespace(
        insert_repo=AsyncMock()
    )  # no insert_ci_job_runs -> not a ClickHouse-shaped store

    async def fake_run_with_store(_db_uri, _db_type, handler, org_id=None):
        await handler(store)

    monkeypatch.setattr(sync_mod, "validate_sink", lambda _ns: None)
    monkeypatch.setattr(sync_mod, "resolve_sink_uri", lambda _ns: "db-uri")
    monkeypatch.setattr(sync_mod, "detect_db_type", lambda _uri: "clickhouse")
    monkeypatch.setattr(sync_mod, "run_with_store", fake_run_with_store)
    monkeypatch.setenv("DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN", "1")

    with pytest.raises(SystemExit, match="requires a ClickHouse sink"):
        await sync_mod.sync_synthetic_target(ns, "tests")
