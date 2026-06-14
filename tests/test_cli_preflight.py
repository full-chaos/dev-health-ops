"""Preflight requirement checks for CLI commands (fast-fail on missing inputs).

Commands that need a database (ClickHouse/PostgreSQL) or an organization id
supplied via global flags/env vars must fail with an argparse usage error
(exit 2) naming the missing input, instead of failing deep in the handler with
a logged error or raw traceback (exit 1). See ``dev_health_ops.cli``.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

# Env vars that satisfy preflight requirements; cleared for "missing" cases.
_CONFIG_ENV = (
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "DATABASE_URL",
    "ORG_ID",
)


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


# Commands that must fast-fail when no database/org is configured, plus the
# requirement tokens expected to appear in the error message.
_MISSING_CASES = [
    (("metrics", "compounding-risk"), ("ClickHouse", "organization")),
    (("metrics", "daily"), ("ClickHouse",)),
    (("metrics", "dora"), ("ClickHouse",)),
    (("metrics", "complexity"), ("ClickHouse",)),
    (("metrics", "release-impact"), ("ClickHouse",)),
    (("metrics", "validate-flags"), ("ClickHouse",)),
    (("metrics", "rebuild"), ("ClickHouse",)),
    (("sync", "work-items"), ("ClickHouse",)),
    (("audit", "perf"), ("ClickHouse",)),
    (("audit", "schema"), ("ClickHouse",)),
    (("recommendations", "compute", "--team", "t1"), ("ClickHouse",)),
    (("investment", "materialize"), ("ClickHouse",)),
    (("billing", "reconcile"), ("PostgreSQL",)),
    (("migrate", "postgres", "upgrade"), ("PostgreSQL",)),
    (("migrate", "clickhouse", "status"), ("ClickHouse",)),
    (("backfill", "run", "--config-id", "x"), ("organization",)),
    # Bare migrate forms default to upgrade and must be guarded too.
    (("migrate", "postgres"), ("PostgreSQL",)),
    (("migrate", "clickhouse"), ("ClickHouse",)),
    # sync teams persists to ClickHouse after generating teams.
    (("sync", "teams", "--provider", "synthetic"), ("ClickHouse",)),
]


@pytest.mark.parametrize("args,expected", _MISSING_CASES)
def test_missing_config_fast_fails_with_usage(
    args: tuple[str, ...], expected: tuple[str, ...]
) -> None:
    result = _run_cli(*args)

    # argparse usage error, not a deep handler failure.
    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert "missing required input" in result.stderr
    assert result.stderr.startswith("usage:") or "usage:" in result.stderr
    for token in expected:
        assert token in result.stderr


def test_configured_command_passes_preflight() -> None:
    """A configured command gets past preflight (no 'missing required input')."""
    result = _run_cli(
        "metrics",
        "compounding-risk",
        env_overrides={
            "CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:9/default",
            "ORG_ID": "org-test",
        },
    )

    # Preflight passed: it may still fail to connect, but not on a missing input.
    assert "missing required input" not in result.stderr


def test_investment_materialize_accepts_clickhouse_via_db_flag() -> None:
    """`investment materialize` carries its ClickHouse DSN on --db, not
    --analytics-db; the preflight must accept it and not false-positive."""
    result = _run_cli(
        "investment",
        "materialize",
        "--db",
        "clickhouse://ch:ch@localhost:9/default",
    )

    assert "missing required input" not in result.stderr


def test_help_lists_requirements_in_epilog() -> None:
    result = _run_cli("metrics", "compounding-risk", "--help")

    assert result.returncode == 0
    assert "Requires:" in result.stdout
    assert "ClickHouse" in result.stdout
    assert "organization" in result.stdout


def test_unrelated_command_has_no_requirements() -> None:
    """A command with no DB/org need is unaffected by preflight."""
    result = _run_cli("maintenance", "--help")

    assert result.returncode == 0
    assert "Requires:" not in result.stdout


def test_missing_requirements_unit() -> None:
    """missing_requirements reflects ns._requires + resolved presence."""
    from argparse import Namespace

    from dev_health_ops import cli

    ns = Namespace(
        _requires=frozenset({cli._REQ_CLICKHOUSE, cli._REQ_ORG}),
        analytics_db=None,
        db=None,
        org=None,
    )
    for key in _CONFIG_ENV:
        os.environ.pop(key, None)

    missing = cli.missing_requirements(ns)
    assert any("ClickHouse" in m for m in missing)
    assert any("organization" in m for m in missing)

    # Satisfy one requirement via the namespace value.
    ns.analytics_db = "clickhouse://localhost"
    missing = cli.missing_requirements(ns)
    assert not any("ClickHouse" in m for m in missing)
    assert any("organization" in m for m in missing)
