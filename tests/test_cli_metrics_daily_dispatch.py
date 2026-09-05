"""CHAOS-5055: `dev-hops metrics daily`/`rebuild` dispatch to the Go worker.

Pins that these two verbs no longer call `run_daily_metrics_job`
(`job_daily.py`) directly -- the bare CLI path used to recompute (and
rewrite) native daily families the worker's own bridge call had already
written for the same (org, day, repo) scope, because it never passed
`skip_families`. They now shell out to `dev-health-workerctl metrics
daily-start` instead.
"""

from __future__ import annotations

import sys
from datetime import date
from types import SimpleNamespace

from dev_health_ops.metrics import workerctl_dispatch


def _make_ns(**overrides):
    # Deliberately unannotated (matches test_cli_metrics_complexity.py's own
    # _make_ns): mypy infers an unannotated function's return type as Any at
    # call sites, which is what lets a SimpleNamespace stand in for
    # argparse.Namespace here without a real typing conflict.
    data: dict[str, object] = {
        "org": "00000000-0000-4000-8000-000000000001",
        "since": None,
        "before": None,
        "backfill": 1,
        "day": None,
        "date": None,
        "repo_ids": [],
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _recording_run_workerctl(calls: list[list[str]]):
    def _run(argv: list[str]) -> int:
        calls.append(argv)
        return 0

    return _run


def test_workerctl_dispatch_module_never_imports_job_daily() -> None:
    """The whole point of dispatching is to stop calling compute in-process."""
    assert "job_daily" not in workerctl_dispatch.__dict__
    assert not hasattr(workerctl_dispatch, "run_daily_metrics_job")
    assert "dev_health_ops.metrics.job_daily" not in {
        getattr(v, "__module__", None) for v in vars(workerctl_dispatch).values()
    }


def test_daily_start_args_builds_org_day_range_and_repo_ids() -> None:
    ns = _make_ns(before=date(2026, 7, 25), backfill=3, repo_ids=["repo-a", "repo-b"])
    args = workerctl_dispatch._daily_start_args(ns)
    assert args == [
        "metrics",
        "daily-start",
        "--org",
        "00000000-0000-4000-8000-000000000001",
        "--day",
        "2026-07-22",
        "--to",
        "2026-07-24",
        "--repo-id",
        "repo-a",
        "--repo-id",
        "repo-b",
    ]


def test_daily_start_args_requires_org() -> None:
    ns = _make_ns(org=None)
    try:
        workerctl_dispatch._daily_start_args(ns)
        raise AssertionError("expected SystemExit for missing --org")
    except SystemExit as exc:
        assert "--org" in str(exc)


def test_cmd_metrics_daily_and_rebuild_dispatch_to_run_workerctl(monkeypatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        workerctl_dispatch, "run_workerctl", _recording_run_workerctl(calls)
    )
    ns = _make_ns(before=date(2026, 7, 25), backfill=1)

    assert workerctl_dispatch._cmd_metrics_daily(ns) == 0
    assert workerctl_dispatch._cmd_metrics_rebuild(ns) == 0
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert calls[0][:2] == ["metrics", "daily-start"]


def test_run_workerctl_returns_the_subprocess_exit_code(monkeypatch) -> None:
    monkeypatch.setenv(workerctl_dispatch.WORKERCTL_BIN_ENV, "/bin/echo")

    class _FakeResult:
        returncode = 7

    captured = {}

    def _fake_run(argv, stdout, stderr):
        captured["argv"] = argv
        assert stdout is sys.stdout
        assert stderr is sys.stderr
        return _FakeResult()

    monkeypatch.setattr(workerctl_dispatch.subprocess, "run", _fake_run)
    rc = workerctl_dispatch.run_workerctl(["metrics", "daily-start"])
    assert rc == 7
    assert captured["argv"] == ["/bin/echo", "metrics", "daily-start"]


def test_run_workerctl_fails_closed_when_binary_is_missing(monkeypatch) -> None:
    monkeypatch.delenv(workerctl_dispatch.WORKERCTL_BIN_ENV, raising=False)
    monkeypatch.setattr(workerctl_dispatch.shutil, "which", lambda _name: None)
    assert workerctl_dispatch.run_workerctl(["metrics", "daily-start"]) == 1


def test_metrics_daily_and_rebuild_verbs_dispatch_via_workerctl_module() -> None:
    """Build the real argparse tree and confirm `metrics daily`/`rebuild`
    resolve to workerctl_dispatch's handlers, not job_daily's."""
    from dev_health_ops import cli as cli_mod

    parser = cli_mod.build_parser()
    # argparse's own type stubs leave these private, introspection-only
    # attributes as `Any`/`None`-able -- this is a test-only reach into
    # argparse internals to prove wiring, not production code.
    metrics_action = next(
        action
        for action in parser._subparsers._group_actions  # type: ignore[union-attr]
        if action.dest == "command"
    )
    metrics_subparser = metrics_action.choices["metrics"]  # type: ignore[index]
    metrics_sub_action = next(
        action
        for action in metrics_subparser._subparsers._group_actions
        if action.dest == "metrics_command"
    )
    daily_defaults = metrics_sub_action.choices["daily"].get_default("func")
    rebuild_defaults = metrics_sub_action.choices["rebuild"].get_default("func")
    assert daily_defaults is workerctl_dispatch._cmd_metrics_daily
    assert rebuild_defaults is workerctl_dispatch._cmd_metrics_rebuild
