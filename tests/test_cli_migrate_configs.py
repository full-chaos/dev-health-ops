"""Tests for `dev-hops migrate configs-to-integrations` CLI command."""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from dev_health_ops.sync.config_migration import MigrationIssue, MigrationReport

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CONFIG_ENV = (
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "DATABASE_URL",
    "ORG_ID",
)

_POSTGRES_URI = "postgresql://user:pass@localhost:5432/devhealth"


def _run_cli(*args: str, env_overrides: dict[str, str] | None = None):
    env = os.environ.copy()
    env["DISABLE_DOTENV"] = "1"
    env["OTEL_SDK_DISABLED"] = "true"
    env["PYTHONPATH"] = "src"
    for key in _CONFIG_ENV:
        env.pop(key, None)
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-m", "dev_health_ops.cli", *args],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


@contextmanager
def _fake_session_ctx(session: MagicMock) -> Generator[MagicMock, None, None]:
    """Context manager that yields the given mock session."""
    yield session


def _make_report(**kwargs) -> MigrationReport:
    defaults: dict = dict(
        dry_run=False,
        integrations_created=0,
        sources_created=0,
        datasets_created=0,
        configs_linked=0,
        sources_linked=0,
        issues=[],
    )
    defaults.update(kwargs)
    return MigrationReport(**defaults)


def _invoke_handler(
    dry_run: bool, report: MigrationReport
) -> tuple[int, str, MagicMock, MagicMock]:
    """
    Call _run_configs_to_integrations directly with mocked session and
    migration function. Returns (exit_code, stdout, mock_session, mock_migrate).

    The handler uses lazy imports inside the function body, so we patch at the
    source module paths rather than at dev_health_ops.migrate.
    """
    from dev_health_ops.migrate import _run_configs_to_integrations

    ns = argparse.Namespace(dry_run=dry_run, db=_POSTGRES_URI)
    mock_session = MagicMock()

    with (
        patch(
            "dev_health_ops.db.get_postgres_session_sync_for_uri",
            return_value=_fake_session_ctx(mock_session),
        ),
        patch(
            "dev_health_ops.sync.config_migration.migrate_configs_to_integrations",
            return_value=report,
        ) as mock_migrate,
        patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
    ):
        exit_code = _run_configs_to_integrations(ns)
        output = mock_stdout.getvalue()

    return exit_code, output, mock_session, mock_migrate


# ---------------------------------------------------------------------------
# Preflight: missing Postgres fast-fails with exit 2
# ---------------------------------------------------------------------------


def test_missing_postgres_fast_fails():
    result = _run_cli("migrate", "configs-to-integrations")
    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert "missing required input" in result.stderr
    assert "PostgreSQL" in result.stderr


def test_preflight_passes_when_postgres_set():
    """With POSTGRES_URI set, preflight passes (handler may fail to connect, not on missing input)."""
    result = _run_cli(
        "migrate",
        "configs-to-integrations",
        env_overrides={"POSTGRES_URI": _POSTGRES_URI},
    )
    assert "missing required input" not in result.stderr


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------


def test_help_shows_dry_run_flag():
    result = _run_cli("migrate", "configs-to-integrations", "--help")
    assert result.returncode == 0
    assert "--dry-run" in result.stdout


def test_help_shows_postgres_requirement():
    result = _run_cli("migrate", "configs-to-integrations", "--help")
    assert result.returncode == 0
    assert "Requires:" in result.stdout
    assert "PostgreSQL" in result.stdout


# ---------------------------------------------------------------------------
# Handler unit tests (mock session + migration function)
# ---------------------------------------------------------------------------


class TestHandlerDryRun:
    def test_calls_migration_with_dry_run_true(self):
        report = _make_report(dry_run=True, integrations_created=3, sources_created=5)
        _, _, _, mock_migrate = _invoke_handler(dry_run=True, report=report)
        mock_migrate.assert_called_once()
        _, kwargs = mock_migrate.call_args
        assert kwargs["dry_run"] is True

    def test_rolls_back_on_dry_run(self):
        report = _make_report(dry_run=True)
        _, _, session, _ = _invoke_handler(dry_run=True, report=report)
        session.rollback.assert_called_once()
        session.commit.assert_not_called()

    def test_prints_dry_run_mode(self):
        report = _make_report(dry_run=True)
        _, output, _, _ = _invoke_handler(dry_run=True, report=report)
        assert "DRY RUN" in output

    def test_prints_no_commit_notice(self):
        report = _make_report(dry_run=True)
        _, output, _, _ = _invoke_handler(dry_run=True, report=report)
        assert "no changes committed" in output

    def test_returns_zero_on_no_issues(self):
        report = _make_report(dry_run=True)
        exit_code, _, _, _ = _invoke_handler(dry_run=True, report=report)
        assert exit_code == 0


class TestHandlerApply:
    def test_calls_migration_with_dry_run_false(self):
        report = _make_report(dry_run=False)
        _, _, _, mock_migrate = _invoke_handler(dry_run=False, report=report)
        mock_migrate.assert_called_once()
        _, kwargs = mock_migrate.call_args
        assert kwargs["dry_run"] is False

    def test_does_not_rollback_on_apply(self):
        report = _make_report(dry_run=False)
        _, _, session, _ = _invoke_handler(dry_run=False, report=report)
        session.rollback.assert_not_called()

    def test_prints_applied_mode(self):
        report = _make_report(dry_run=False)
        _, output, _, _ = _invoke_handler(dry_run=False, report=report)
        assert "APPLIED" in output

    def test_returns_zero_on_no_issues(self):
        report = _make_report(dry_run=False)
        exit_code, _, _, _ = _invoke_handler(dry_run=False, report=report)
        assert exit_code == 0


class TestHandlerReportFields:
    def test_prints_all_report_counts(self):
        report = _make_report(
            dry_run=False,
            integrations_created=2,
            sources_created=4,
            datasets_created=6,
            configs_linked=8,
            sources_linked=3,
        )
        _, output, _, _ = _invoke_handler(dry_run=False, report=report)
        assert "integrations_created" in output
        assert "2" in output
        assert "sources_created" in output
        assert "4" in output
        assert "datasets_created" in output
        assert "6" in output
        assert "configs_linked" in output
        assert "8" in output
        assert "sources_linked" in output
        assert "3" in output

    def test_prints_no_issues_when_empty(self):
        report = _make_report(dry_run=False, issues=[])
        _, output, _, _ = _invoke_handler(dry_run=False, report=report)
        assert "none" in output

    def test_prints_issues_when_present(self):
        issues = [
            MigrationIssue(
                config_id="abc-123",
                provider="github",
                reason="github_child_missing_owner_repo",
                repaired=False,
            )
        ]
        report = _make_report(dry_run=False, issues=issues)
        _, output, _, _ = _invoke_handler(dry_run=False, report=report)
        assert "abc-123" in output
        assert "github" in output
        assert "github_child_missing_owner_repo" in output

    def test_prints_repaired_tag_for_repaired_issues(self):
        issues = [
            MigrationIssue(
                config_id="xyz-456",
                provider="gitlab",
                reason="gitlab_child_project_id_repaired_from_repo",
                repaired=True,
            )
        ]
        report = _make_report(dry_run=False, issues=issues)
        _, output, _, _ = _invoke_handler(dry_run=False, report=report)
        assert "[repaired]" in output


class TestHandlerExitCode:
    def test_returns_one_on_unrepaired_issues(self):
        issues = [
            MigrationIssue(
                config_id="bad-1",
                provider="github",
                reason="github_child_missing_owner_repo",
                repaired=False,
            )
        ]
        report = _make_report(dry_run=False, issues=issues)
        exit_code, _, _, _ = _invoke_handler(dry_run=False, report=report)
        assert exit_code == 1

    def test_returns_zero_when_all_issues_repaired(self):
        issues = [
            MigrationIssue(
                config_id="ok-1",
                provider="gitlab",
                reason="gitlab_child_project_id_repaired_from_repo",
                repaired=True,
            )
        ]
        report = _make_report(dry_run=False, issues=issues)
        exit_code, _, _, _ = _invoke_handler(dry_run=False, report=report)
        assert exit_code == 0
