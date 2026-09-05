"""CHAOS-5055: `dev-hops metrics complexity`/`dora`/`release-impact` dispatch
to `dev-health-workerctl metrics remaining trigger-backstop`.

These three used to call their own compute function directly, always
bypassing the worker. They now shell out per day in the requested range,
forwarding `--today` for the current UTC day and `--review-evidence` when
given -- never inventing a justification, and never re-implementing the
verb's own family-write-mode policy (that enforcement lives in the Go verb;
see manual_backfill.go / lane-5016's CHAOS-5016 work).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
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
        "review_evidence": None,
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _recording_run_workerctl(calls: list[list[str]]):
    def _run(argv: list[str]) -> int:
        calls.append(argv)
        return 0

    return _run


def test_workerctl_dispatch_module_never_imports_the_old_compute_modules() -> None:
    for name in ("job_complexity_db", "job_dora", "job_release_impact"):
        assert name not in workerctl_dispatch.__dict__
    module_names = {
        getattr(v, "__module__", None) for v in vars(workerctl_dispatch).values()
    }
    assert "dev_health_ops.metrics.job_complexity_db" not in module_names
    assert "dev_health_ops.metrics.job_dora" not in module_names
    assert "dev_health_ops.metrics.job_release_impact" not in module_names


def test_trigger_backstop_argv_omits_today_flag_for_a_past_day() -> None:
    argv = workerctl_dispatch._trigger_backstop_argv_for_day(
        family="dora",
        org="00000000-0000-4000-8000-000000000001",
        day=date(2026, 7, 1),
        today_utc=date(2026, 7, 2),
        review_evidence=None,
    )
    assert argv == [
        "metrics",
        "remaining",
        "trigger-backstop",
        "--family",
        "dora",
        "--org",
        "00000000-0000-4000-8000-000000000001",
        "--day",
        "2026-07-01",
    ]


def test_trigger_backstop_argv_adds_today_and_review_evidence() -> None:
    argv = workerctl_dispatch._trigger_backstop_argv_for_day(
        family="release_impact",
        org="00000000-0000-4000-8000-000000000001",
        day=date(2026, 7, 2),
        today_utc=date(2026, 7, 2),
        review_evidence="CHAOS-9999 -- confirmed no automatic run yet",
    )
    assert argv == [
        "metrics",
        "remaining",
        "trigger-backstop",
        "--family",
        "release_impact",
        "--org",
        "00000000-0000-4000-8000-000000000001",
        "--day",
        "2026-07-02",
        "--today",
        "--review-evidence",
        "CHAOS-9999 -- confirmed no automatic run yet",
    ]


def test_dispatch_trigger_backstop_range_calls_run_workerctl_once_per_day(
    monkeypatch,
) -> None:
    fixed_today = datetime(2026, 7, 10, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_today

    monkeypatch.setattr(workerctl_dispatch, "datetime", _FixedDatetime)

    calls: list[list[str]] = []
    monkeypatch.setattr(
        workerctl_dispatch, "run_workerctl", _recording_run_workerctl(calls)
    )
    ns = _make_ns(before=date(2026, 7, 11), backfill=3)  # 2026-07-08..07-10
    rc = workerctl_dispatch._cmd_metrics_dora(ns)

    assert rc == 0
    assert len(calls) == 3
    days = [c[c.index("--day") + 1] for c in calls]
    assert days == ["2026-07-08", "2026-07-09", "2026-07-10"]
    # Only the last (today, 2026-07-10) should carry --today.
    assert "--today" not in calls[0]
    assert "--today" not in calls[1]
    assert "--today" in calls[2]


def test_dispatch_trigger_backstop_range_stops_on_first_failure(monkeypatch) -> None:
    calls = []

    def _fake_run_workerctl(argv):
        calls.append(argv)
        return 1

    monkeypatch.setattr(workerctl_dispatch, "run_workerctl", _fake_run_workerctl)
    ns = _make_ns(before=date(2026, 1, 4), backfill=3)
    rc = workerctl_dispatch._cmd_metrics_complexity(ns)

    assert rc == 1
    assert len(calls) == 1  # stopped after the first day's failure


def test_dispatch_trigger_backstop_range_requires_org() -> None:
    ns = _make_ns(org=None)
    try:
        workerctl_dispatch._cmd_metrics_dora(ns)
        raise AssertionError("expected SystemExit for missing --org")
    except SystemExit as exc:
        assert "--org" in str(exc)


def test_metrics_complexity_dora_release_impact_verbs_dispatch_via_workerctl_module() -> (
    None
):
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
    assert (
        metrics_sub_action.choices["complexity"].get_default("func")
        is workerctl_dispatch._cmd_metrics_complexity
    )
    assert (
        metrics_sub_action.choices["dora"].get_default("func")
        is workerctl_dispatch._cmd_metrics_dora
    )
    assert (
        metrics_sub_action.choices["release-impact"].get_default("func")
        is workerctl_dispatch._cmd_metrics_release_impact
    )
    assert (
        metrics_sub_action.choices["capacity"].get_default("func")
        is workerctl_dispatch._cmd_metrics_capacity
    )


# ---------------------------------------------------------------------------
# `dev-hops metrics capacity` -- team-scoped, not day-range-scoped.
# ---------------------------------------------------------------------------


def _make_capacity_ns(**overrides):
    data: dict[str, object] = {
        "org": "00000000-0000-4000-8000-000000000001",
        "team_id": None,
        "all_teams": False,
        "day": None,
        "review_evidence": "CHAOS-1234 -- testing",
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_capacity_dispatch_requires_org() -> None:
    ns = _make_capacity_ns(org=None)
    try:
        workerctl_dispatch._cmd_metrics_capacity(ns)
        raise AssertionError("expected SystemExit for missing --org")
    except SystemExit as exc:
        assert "--org" in str(exc)


def test_capacity_dispatch_requires_exactly_one_of_team_id_or_all_teams(
    monkeypatch,
) -> None:
    monkeypatch.setattr(workerctl_dispatch, "run_workerctl", lambda argv: 0)

    for ns in (
        _make_capacity_ns(),  # neither
        _make_capacity_ns(
            team_id="00000000-0000-4000-8000-000000000002", all_teams=True
        ),  # both
    ):
        try:
            workerctl_dispatch._cmd_metrics_capacity(ns)
            raise AssertionError("expected SystemExit for an invalid team scope")
        except SystemExit as exc:
            assert "--team-id" in str(exc) and "--all-teams" in str(exc)


def test_capacity_dispatch_builds_expected_argv_for_all_teams_today(
    monkeypatch,
) -> None:
    fixed_today = datetime(2026, 7, 10, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_today

    monkeypatch.setattr(workerctl_dispatch, "datetime", _FixedDatetime)
    calls: list[list[str]] = []
    monkeypatch.setattr(
        workerctl_dispatch, "run_workerctl", _recording_run_workerctl(calls)
    )

    ns = _make_capacity_ns(all_teams=True)
    rc = workerctl_dispatch._cmd_metrics_capacity(ns)

    assert rc == 0
    assert calls == [
        [
            "metrics",
            "remaining",
            "trigger-backstop",
            "--family",
            "capacity",
            "--org",
            "00000000-0000-4000-8000-000000000001",
            "--day",
            "2026-07-10",
            "--review-evidence",
            "CHAOS-1234 -- testing",
            "--all-teams",
            "--today",
        ]
    ]


def test_capacity_dispatch_builds_expected_argv_for_a_team_on_a_past_day(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        workerctl_dispatch, "run_workerctl", _recording_run_workerctl(calls)
    )
    ns = _make_capacity_ns(
        team_id="00000000-0000-4000-8000-000000000002", day="2026-07-01"
    )
    rc = workerctl_dispatch._cmd_metrics_capacity(ns)

    assert rc == 0
    assert calls == [
        [
            "metrics",
            "remaining",
            "trigger-backstop",
            "--family",
            "capacity",
            "--org",
            "00000000-0000-4000-8000-000000000001",
            "--day",
            "2026-07-01",
            "--review-evidence",
            "CHAOS-1234 -- testing",
            "--team",
            "00000000-0000-4000-8000-000000000002",
        ]
    ]
