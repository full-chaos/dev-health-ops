"""Tests for the work-graph CLI runner's `investment materialize` entry point.

`dev-hops work-graph build` and its tests were deleted under CHAOS-4924: the
CLI drove `WorkGraphBuilder.build()`, which is a 0-stats no-op shell now that
every stage it used to run has been ported natively (see builder.py's own
history). `investment materialize` is unrelated and unaffected.
"""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

# Import connectors first to defuse the providers._base <-> connectors circular
# import so this module runs in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.work_graph.runner import run_investment_materialization

# ---------------------------------------------------------------------------
# CHAOS-2433 round-4 finding #1 (AMENDED by CHAOS-2776): the CLI `investment
# materialize` entry point must publish a full-coverage membership marker after an
# UNSCOPED materialization (the materializer writes investments only). The publish
# gate is SCOPE, not window: because the no-LLM projection is full-coverage by
# construction (independent of --from/--to/--window-days), an unscoped WINDOWED run
# still publishes the org-wide marker. Only repo/team-SCOPED runs skip it.
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


def test_cli_materialize_threads_batch_settings():
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
                llm_batch_mode="auto",
                llm_batch_min_items=3,
                llm_batch_poll_interval_seconds=0.5,
                llm_batch_timeout_seconds=12.0,
            )
        )

    assert rc == 0
    config = materialize_mock.call_args.args[0]
    assert config.llm_batch_mode == "auto"
    assert config.llm_batch_min_items == 3
    assert config.llm_batch_poll_interval_seconds == 0.5
    assert config.llm_batch_timeout_seconds == 12.0
    backfill_mock.assert_not_called()


def test_cli_materialize_uses_env_batch_defaults(monkeypatch):
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_MODE", "auto")
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_MIN_ITEMS", "7")
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_POLL_INTERVAL_SECONDS", "1.5")
    monkeypatch.setenv("INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS", "42")
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
    config = materialize_mock.call_args.args[0]
    assert config.llm_batch_mode == "auto"
    assert config.llm_batch_min_items == 7
    assert config.llm_batch_poll_interval_seconds == 1.5
    assert config.llm_batch_timeout_seconds == 42.0
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


def test_cli_date_windowed_org_wide_materialize_runs_org_marker():
    """CHAOS-2776: an explicitly date-windowed BUT UNSCOPED manual materialize
    (--window-days / --from / --to, no repo/team scope) STILL publishes a
    full-coverage org-wide marker. The no-LLM projection iterates the full current
    work graph independent of the materialize window, so it cannot publish partial
    coverage — and running it re-arms the read-path stale-generation guard."""
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
    backfill_mock.assert_called_once()
    proj_config = backfill_mock.call_args.args[0]
    assert proj_config.repo_ids is None  # org-wide projection
    assert proj_config.is_org_wide is True


def test_cli_date_windowed_repo_scoped_materialize_skips_org_marker():
    """A date-windowed AND repo-scoped run still skips the org-wide marker — the
    scope (not the window) is what suppresses publishing (CHAOS-2776)."""
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
            _materialize_ns(window_days=7, repo_id=["repo-uuid-1"])
        )

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
