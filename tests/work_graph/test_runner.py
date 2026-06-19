"""Tests for the work-graph CLI runner (``dev-hops work-graph build``).

These guard the org-scope wiring flagged in CHAOS-2375 round-3: the CLI must
flow ``--org`` into ``BuildConfig.org_id`` so every read/write the builder
performs is tenant-scoped, and must fail closed when ``--org`` is supplied but
blank rather than silently building under the empty org.
"""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import so this module runs in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.work_graph.runner import (
    run_investment_materialization,
    run_work_graph_build,
)


def _ns(**overrides) -> argparse.Namespace:
    base = dict(
        db="clickhouse://localhost:9000/default",
        from_date="2024-01-01",
        to_date="2024-02-01",
        repo_id=None,
        heuristic_window=7,
        heuristic_confidence=0.3,
        check_components=False,
        allow_degenerate=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _patched_builder():
    """Patch WorkGraphBuilder so build() returns trivially and the verification
    edge-count query reports a non-empty graph (so run returns 0)."""
    fake_builder = MagicMock()
    fake_builder.build.return_value = {"pr_commit_edges": 1}
    client = MagicMock()
    client.query.return_value.result_rows = [[1]]
    fake_builder.client = client
    return fake_builder


def test_org_flows_into_build_config():
    """``--org X`` must produce ``BuildConfig.org_id == X``."""
    captured = {}

    def _capture(config):
        captured["config"] = config
        return _patched_builder()

    with patch(
        "dev_health_ops.work_graph.runner.WorkGraphBuilder", side_effect=_capture
    ):
        rc = run_work_graph_build(_ns(org="org-abc"))

    assert rc == 0
    assert captured["config"].org_id == "org-abc"


def test_no_org_builds_full_rebuild_with_empty_org():
    """Omitting ``--org`` is a legitimate full rebuild: org_id is empty, not a
    failure."""
    captured = {}

    def _capture(config):
        captured["config"] = config
        return _patched_builder()

    with patch(
        "dev_health_ops.work_graph.runner.WorkGraphBuilder", side_effect=_capture
    ):
        rc = run_work_graph_build(_ns(org=None))

    assert rc == 0
    assert captured["config"].org_id == ""


def test_blank_org_fails_closed():
    """An explicit but blank ``--org`` must fail closed (return 2) and never
    construct a builder -- otherwise the tenant build silently runs unscoped."""
    with patch("dev_health_ops.work_graph.runner.WorkGraphBuilder") as builder_cls:
        rc = run_work_graph_build(_ns(org="   "))

    assert rc == 2
    builder_cls.assert_not_called()


def test_verification_uses_builder_sink_client_when_builder_has_no_client():
    fake_builder = MagicMock()
    fake_builder.build.return_value = {"pr_commit_edges": 1}
    fake_builder.client = None
    client = MagicMock()
    client.query.return_value.result_rows = [[1]]
    fake_builder.sink.client = client

    with patch(
        "dev_health_ops.work_graph.runner.WorkGraphBuilder",
        return_value=fake_builder,
    ):
        rc = run_work_graph_build(_ns(org="org-abc"))

    assert rc == 0
    client.query.assert_called_once()


# ---------------------------------------------------------------------------
# CHAOS-2433 round-4 finding #1: the CLI `investment materialize` entry point
# must publish a full-coverage membership marker after an ORG-WIDE materialization
# (the materializer writes investments only). Scoped/windowed manual runs must
# NOT publish an org-wide marker.
# ---------------------------------------------------------------------------


def _materialize_ns(**overrides) -> argparse.Namespace:
    base: dict[str, object] = dict(
        db="clickhouse://localhost:9000/default",
        from_date=None,
        to_date=None,
        window_days=None,
        repo_id=[],
        team_id=[],
        org="org-abc",
        llm_provider="auto",
        persist_evidence_snippets=True,
        model=None,
        llm_api_key=None,
        llm_base_url=None,
        llm_concurrency=None,
        force=False,
        allow_unscoped=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _patch_materialize_and_projection():
    """Patch materialize_investments (async) and backfill_memberships (sync).

    Returns (materialize_mock, backfill_mock) as MagicMocks recording calls.
    """
    materialize_mock = MagicMock(
        return_value={"components": 2, "records": 2, "quotes": 0}
    )

    async def _fake_materialize(config):
        return materialize_mock(config)

    backfill_mock = MagicMock(
        return_value={
            "components": 2,
            "matched": 2,
            "skipped": 0,
            "memberships": 4,
        }
    )
    return _fake_materialize, materialize_mock, backfill_mock


def test_cli_org_wide_materialize_runs_membership_projection():
    """A bare org-wide `investment materialize` (no window/scope) runs the no-LLM
    projection synchronously to publish a full-coverage completion marker."""
    fake_materialize, materialize_mock, backfill_mock = (
        _patch_materialize_and_projection()
    )

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            fake_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        rc = run_investment_materialization(_materialize_ns())

    assert rc == 0
    # The materializer ran...
    materialize_mock.assert_called_once()
    # ...and the no-LLM projection ran to publish the full-coverage marker.
    backfill_mock.assert_called_once()
    proj_config = backfill_mock.call_args.args[0]
    assert proj_config.org_id == "org-abc"
    assert proj_config.repo_ids is None  # org-wide projection
    assert proj_config.is_org_wide is True


def test_cli_repo_scoped_materialize_skips_org_marker():
    """A repo-scoped manual materialize refreshes investments only and does NOT
    run the org-wide projection (no org marker published)."""
    fake_materialize, materialize_mock, backfill_mock = (
        _patch_materialize_and_projection()
    )

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            fake_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        rc = run_investment_materialization(_materialize_ns(repo_id=["repo-uuid-1"]))

    assert rc == 0
    materialize_mock.assert_called_once()
    backfill_mock.assert_not_called()


def test_cli_team_scoped_materialize_skips_org_marker():
    """A team-scoped manual materialize also skips the org-wide projection."""
    fake_materialize, materialize_mock, backfill_mock = (
        _patch_materialize_and_projection()
    )

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            fake_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        rc = run_investment_materialization(_materialize_ns(team_id=["team-1"]))

    assert rc == 0
    backfill_mock.assert_not_called()


def test_cli_materialize_threads_inline_llm_credentials_and_concurrency():
    fake_materialize, materialize_mock, backfill_mock = (
        _patch_materialize_and_projection()
    )

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            fake_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        rc = run_investment_materialization(
            _materialize_ns(
                repo_id=["repo-uuid-1"],
                llm_api_key="sk-inline-secret",
                llm_base_url="https://inline.invalid/v1",
                llm_concurrency=1,
            )
        )

    assert rc == 0
    materialize_mock.assert_called_once()
    config = materialize_mock.call_args.args[0]
    assert config.llm_api_key == "sk-inline-secret"
    assert config.llm_base_url == "https://inline.invalid/v1"
    assert config.llm_concurrency == 1
    assert "sk-inline-secret" not in repr(config)
    backfill_mock.assert_not_called()


def test_cli_materialize_threads_allow_unscoped():
    fake_materialize, materialize_mock, backfill_mock = (
        _patch_materialize_and_projection()
    )

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            fake_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        rc = run_investment_materialization(
            _materialize_ns(org=None, allow_unscoped=True, repo_id=["repo-uuid-1"])
        )

    assert rc == 0
    materialize_mock.assert_called_once()
    config = materialize_mock.call_args.args[0]
    assert config.org_id is None
    assert config.allow_unscoped is True
    backfill_mock.assert_not_called()


def test_cli_date_windowed_materialize_skips_org_marker():
    """An explicitly date-windowed manual materialize (--window-days / --from /
    --to) refreshes investments only and does NOT publish an org-wide marker —
    it could otherwise blank out-of-window components under the new marker."""
    fake_materialize, materialize_mock, backfill_mock = (
        _patch_materialize_and_projection()
    )

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            fake_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        # Explicit window via --window-days.
        rc = run_investment_materialization(_materialize_ns(window_days=7))

    assert rc == 0
    materialize_mock.assert_called_once()
    backfill_mock.assert_not_called()


def test_cli_materialize_failure_skips_projection():
    """If materialization fails, the projection must NOT run and rc is 1."""
    backfill_mock = MagicMock()

    async def _failing_materialize(config):
        raise RuntimeError("boom")

    with (
        patch(
            "dev_health_ops.work_graph.runner.materialize_investments",
            _failing_materialize,
        ),
        patch(
            "dev_health_ops.work_graph.investment.backfill.backfill_memberships",
            backfill_mock,
        ),
    ):
        rc = run_investment_materialization(_materialize_ns())

    assert rc == 1
    backfill_mock.assert_not_called()
