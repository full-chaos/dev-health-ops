from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.backfill.chunker import chunk_date_range
from dev_health_ops.backfill.runner import run_backfill_for_config
from dev_health_ops.cli import build_parser


def test_chunk_date_range_single_day() -> None:
    since = date(2026, 1, 10)
    before = date(2026, 1, 10)

    assert chunk_date_range(since=since, before=before, chunk_days=7) == [
        (since, before)
    ]


def test_chunk_date_range_exactly_seven_days() -> None:
    since = date(2026, 1, 1)
    before = date(2026, 1, 7)

    assert chunk_date_range(since=since, before=before, chunk_days=7) == [
        (since, before)
    ]


def test_chunk_date_range_ten_days_creates_two_chunks() -> None:
    assert chunk_date_range(
        since=date(2026, 1, 1),
        before=date(2026, 1, 10),
        chunk_days=7,
    ) == [
        (date(2026, 1, 1), date(2026, 1, 7)),
        (date(2026, 1, 8), date(2026, 1, 10)),
    ]


def test_chunk_date_range_empty_range_raises() -> None:
    with pytest.raises(ValueError, match="since must be before or equal to before"):
        chunk_date_range(
            since=date(2026, 1, 11),
            before=date(2026, 1, 10),
            chunk_days=7,
        )


def test_backfill_cli_run_parses_args() -> None:
    parser = build_parser()
    ns = parser.parse_args(
        [
            "--org",
            "11111111-1111-1111-1111-111111111111",
            "backfill",
            "run",
            "--config-id",
            "22222222-2222-2222-2222-222222222222",
            "--since",
            "2026-01-01",
            "--before",
            "2026-01-10",
            "--sink",
            "clickhouse",
        ]
    )

    assert ns.command == "backfill"
    assert ns.backfill_command == "run"
    assert ns.config_id == "22222222-2222-2222-2222-222222222222"
    assert ns.since == date(2026, 1, 1)
    assert ns.before == date(2026, 1, 10)
    assert ns.sink == "clickhouse"
    assert callable(ns.func)


def test_run_backfill_for_config_raises_when_config_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Query:
        def filter(self, *args, **kwargs):
            return self

        def one_or_none(self):
            return None

    class _Session:
        def query(self, *args, **kwargs):
            return _Query()

    class _Ctx:
        def __enter__(self):
            return _Session()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.get_postgres_session_sync",
        lambda: _Ctx(),
    )

    with pytest.raises(ValueError, match="Sync configuration not found"):
        run_backfill_for_config(
            db_url="clickhouse://local",
            sync_config_id="33333333-3333-3333-3333-333333333333",
            org_id="44444444-4444-4444-4444-444444444444",
            since=date(2026, 1, 1),
            before=date(2026, 1, 10),
        )


class _FakeConfig:
    def __init__(
        self,
        org_id: str,
        provider: str = "github",
        sync_options: dict[str, object] | None = None,
        sync_targets: list[str] | None = None,
    ) -> None:
        self.id = org_id
        self.org_id = org_id
        self.provider = provider
        self.sync_options = sync_options or {}
        self.sync_targets = sync_targets or []


def _patch_session_with_config(monkeypatch: pytest.MonkeyPatch, config: object) -> None:
    class _Query:
        def filter(self, *args, **kwargs):
            return self

        def one_or_none(self):
            return config

    class _Session:
        def query(self, *args, **kwargs):
            return _Query()

    class _Ctx:
        def __enter__(self):
            return _Session()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.get_postgres_session_sync",
        lambda: _Ctx(),
    )


def test_run_backfill_derives_org_from_config_when_org_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(monkeypatch, _FakeConfig(config_org))

    captured: dict[str, object] = {}

    def _fake_sync_job(*args, **kwargs):
        captured["org_id"] = kwargs.get("org_id")

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        _fake_sync_job,
    )

    result = run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert result["org_id"] == config_org
    assert captured["org_id"] == config_org


def test_run_backfill_raises_on_org_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_org = "77777777-7777-7777-7777-777777777777"
    _patch_session_with_config(monkeypatch, _FakeConfig(config_org))

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(ValueError, match="Org mismatch"):
        run_backfill_for_config(
            db_url="clickhouse://local",
            sync_config_id="88888888-8888-8888-8888-888888888888",
            org_id="99999999-9999-9999-9999-999999999999",
            since=date(2026, 1, 1),
            before=date(2026, 1, 3),
        )


def test_run_backfill_forwards_jira_query_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(
        monkeypatch,
        _FakeConfig(
            config_org,
            provider="jira",
            sync_options={"project_keys": ["OPS"], "jql": "project = OPS"},
        ),
    )
    captured: dict[str, object] = {}

    def _fake_sync_job(*args, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        _fake_sync_job,
    )

    run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert captured["provider"] == "jira"
    assert captured["jira_project_keys"] == ["OPS"]
    assert captured["jira_jql"] == "project = OPS"


def test_run_backfill_github_includes_prs_when_prs_target_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-646: github backfill ingests PRs as work items when the 'prs' target
    is enabled (mirrors the unitized path's planner-stamped sync_prs)."""
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(
        monkeypatch,
        _FakeConfig(config_org, provider="github", sync_targets=["work-items", "prs"]),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        lambda *args, **kwargs: captured.update(kwargs),
    )

    run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert captured["provider"] == "github"
    assert captured["include_issues"] is True
    assert captured["include_pull_requests"] is True


def test_run_backfill_github_excludes_prs_when_prs_target_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-646 regression: with 'prs' off, github backfill must NOT ingest PRs as
    work items. None would let the provider fall back to GITHUB_INCLUDE_PRS (ON)."""
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(
        monkeypatch,
        _FakeConfig(config_org, provider="github", sync_targets=["work-items"]),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        lambda *args, **kwargs: captured.update(kwargs),
    )

    run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert captured["provider"] == "github"
    assert captured["include_issues"] is True
    assert captured["include_pull_requests"] is False


def test_run_backfill_github_prs_only_excludes_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(
        monkeypatch,
        _FakeConfig(config_org, provider="github", sync_targets=["prs"]),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        lambda *args, **kwargs: captured.update(kwargs),
    )

    run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert captured["provider"] == "github"
    assert captured["include_issues"] is False
    assert captured["include_pull_requests"] is True


def test_run_backfill_github_empty_targets_default_to_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(
        monkeypatch,
        _FakeConfig(config_org, provider="github", sync_targets=[]),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        lambda *args, **kwargs: captured.update(kwargs),
    )

    run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert captured["provider"] == "github"
    assert captured["include_issues"] is True
    assert captured["include_pull_requests"] is False


def test_run_backfill_non_github_leaves_include_pull_requests_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only github threads include_pull_requests; other providers leave it None."""
    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_session_with_config(
        monkeypatch,
        _FakeConfig(config_org, provider="gitlab", sync_targets=["work-items", "prs"]),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "dev_health_ops.backfill.runner.run_work_items_sync_job",
        lambda *args, **kwargs: captured.update(kwargs),
    )

    run_backfill_for_config(
        db_url="clickhouse://local",
        sync_config_id="66666666-6666-6666-6666-666666666666",
        org_id=None,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
    )

    assert captured["provider"] == "gitlab"
    assert captured["include_issues"] is None
    assert captured["include_pull_requests"] is None
