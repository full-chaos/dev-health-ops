from __future__ import annotations

import argparse
from datetime import date

import pytest

from dev_health_ops.backfill.chunker import chunk_date_range
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
        ]
    )

    assert ns.command == "backfill"
    assert ns.backfill_command == "run"
    assert ns.config_id == "22222222-2222-2222-2222-222222222222"
    assert ns.since == date(2026, 1, 1)
    assert ns.before == date(2026, 1, 10)
    assert callable(ns.func)


# CHAOS-5351: run_backfill_for_config (and its ~17 dedicated tests that used
# to live below this point, pinning its internals directly) is deleted along
# with run_work_items_sync_job -- the Python compute it looped over chunked
# windows. `dev-hops backfill run` is repointed at run_backfill_via_planner
# (the native plan_sync_run/dispatch_sync_run seam), whose own dispatch
# behaviour -- including that plan_sync_run arms CHAOS-4498 strict reference
# discovery for every mode, backfill included, and native dispatch blocks on
# it -- is already covered by tests/test_backfill_fanout.py and
# tests/test_reference_discovery_stage.py::test_backfill_runner_dispatch_path_blocks_until_discovery.
# What remains genuinely local to this CLI verb (_cmd_backfill_run in
# backfill/cli.py) is: resolving --config-id into an integration_id/org_id/
# dataset_keys and calling run_backfill_via_planner with them, the --org
# mismatch assertion, and the CHAOS-4500 canonical-config guard (a real bug
# codex review caught in run_backfill_for_config's predecessor) -- so those
# are what the tests below cover, with run_backfill_via_planner itself
# mocked out (a "mocked-dispatch" test, not a re-test of the planner).


_TEST_CONFIG_ID = "66666666-6666-6666-6666-666666666666"


class _FakeConfig:
    def __init__(
        self,
        org_id: str,
        *,
        provider: str = "github",
        sync_targets: list[str] | None = None,
        integration_id: str | None = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        config_id: str = _TEST_CONFIG_ID,
        name: str = "test-config",
    ) -> None:
        self.id = config_id
        self.name = name
        self.org_id = org_id
        self.provider = provider
        self.sync_targets = sync_targets or []
        self.integration_id = integration_id


class _FakeCanonicalConfig:
    """Stands in for the parent SyncConfiguration
    canonical_sync_config_for_sync_run would actually resolve, when it
    differs from the operator's requested config."""

    def __init__(self, config_id: str, name: str) -> None:
        self.id = config_id
        self.name = name


def _patch_cmd_backfill_run_session(
    monkeypatch: pytest.MonkeyPatch,
    config: object,
    *,
    canonical_config: object | None = "__unset__",
) -> None:
    """Patch _cmd_backfill_run's config lookup + the CHAOS-4500 canonical-
    config resolver. Defaults canonical_config to `config` itself (the happy
    path: the shared resolver picks the operator's own config)."""

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
        "dev_health_ops.backfill.cli.get_postgres_session_sync", lambda: _Ctx()
    )
    resolved_canonical = (
        config if canonical_config == "__unset__" else canonical_config
    )
    monkeypatch.setattr(
        "dev_health_ops.sync.trigger_routing.canonical_sync_config_for_sync_run",
        lambda session, sync_run: resolved_canonical,
    )


def _run_ns(config_id: str = _TEST_CONFIG_ID, org: str | None = None):
    return argparse.Namespace(
        config_id=config_id,
        since=date(2026, 1, 1),
        before=date(2026, 1, 3),
        org=org,
    )


def test_cmd_backfill_run_calls_planner_with_resolved_window_and_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.backfill import cli as backfill_cli

    config_org = "55555555-5555-5555-5555-555555555555"
    integration_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    _patch_cmd_backfill_run_session(
        monkeypatch,
        _FakeConfig(config_org, sync_targets=["work-items", "prs"]),
    )
    captured: dict[str, object] = {}

    def _fake_planner(integration_id_arg, since, before, **kwargs):
        captured["integration_id"] = integration_id_arg
        captured["since"] = since
        captured["before"] = before
        captured.update(kwargs)
        return {"status": "success", "unit_count": 3, "sync_run_id": "run-1"}

    monkeypatch.setattr(backfill_cli, "run_backfill_via_planner", _fake_planner)

    assert backfill_cli._cmd_backfill_run(_run_ns()) == 0

    # ns.before is EXCLUSIVE (resolve_date_range's contract); _cmd_backfill_run
    # passes run_backfill_via_planner the resolved INCLUSIVE end_day instead,
    # so a --before of Jan 3 resolves to Jan 2 here.
    assert captured["integration_id"] == integration_id
    assert captured["since"] == date(2026, 1, 1)
    assert captured["before"] == date(2026, 1, 2)
    assert captured["org_id"] == config_org
    dataset_keys = captured["dataset_keys"]
    assert isinstance(dataset_keys, tuple)
    assert set(dataset_keys) == {"work-items", "prs"}
    assert captured["triggered_by"] == "operator_backfill"


def test_cmd_backfill_run_derives_org_from_config_when_org_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.backfill import cli as backfill_cli

    config_org = "55555555-5555-5555-5555-555555555555"
    _patch_cmd_backfill_run_session(monkeypatch, _FakeConfig(config_org))
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        backfill_cli,
        "run_backfill_via_planner",
        lambda *args, **kwargs: (captured.update(kwargs), {"status": "success"})[1],
    )

    assert backfill_cli._cmd_backfill_run(_run_ns(org=None)) == 0
    assert captured["org_id"] == config_org


def test_cmd_backfill_run_raises_on_org_mismatch(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from dev_health_ops.backfill import cli as backfill_cli

    config_org = "77777777-7777-7777-7777-777777777777"
    _patch_cmd_backfill_run_session(monkeypatch, _FakeConfig(config_org))
    monkeypatch.setattr(
        backfill_cli, "run_backfill_via_planner", lambda *a, **k: pytest.fail(
            "must not dispatch on an org mismatch"
        )
    )

    with caplog.at_level("ERROR"):
        result = backfill_cli._cmd_backfill_run(
            _run_ns(org="99999999-9999-9999-9999-999999999999")
        )

    assert result == 1
    assert "Org mismatch" in caplog.text


def test_cmd_backfill_run_rejects_child_config_id(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """CHAOS-4498 config-context guard (codex review P2 / CHAOS-4500):
    --config-id points at a CHILD config (parent_id set). The shared
    discovery resolver (canonical_sync_config_for_sync_run) only ever picks
    a PARENT config for the integration, so it would silently apply the
    parent's selection instead of the child's -- must fail, naming the
    parent it would have used instead."""
    from dev_health_ops.backfill import cli as backfill_cli

    config_org = "55555555-5555-5555-5555-555555555555"
    child_config_id = "aaaabbbb-cccc-dddd-eeee-ffff00001111"
    parent_config_id = "aaaabbbb-cccc-dddd-eeee-ffff00002222"
    child_config = _FakeConfig(
        config_org, config_id=child_config_id, name="child-config"
    )
    parent_config = _FakeCanonicalConfig(parent_config_id, "parent-config")
    _patch_cmd_backfill_run_session(
        monkeypatch, child_config, canonical_config=parent_config
    )
    monkeypatch.setattr(
        backfill_cli, "run_backfill_via_planner", lambda *a, **k: pytest.fail(
            "must not dispatch when the canonical-config guard fires"
        )
    )

    with caplog.at_level("ERROR"):
        result = backfill_cli._cmd_backfill_run(_run_ns(config_id=child_config_id))

    assert result == 1
    assert parent_config_id in caplog.text
    assert "CHAOS-4500" in caplog.text
