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
# The child inherits os.environ, so anything the parent shell carries and this
# list omits reaches the CLI as real configuration. `admin licenses create` is
# asserted to report LICENSE_PRIVATE_KEY as the missing input, which only holds
# when no signing key is configured -- a developer shell carries a real one from
# ops/.env, and a 64-byte key where a 32-byte seed is required made the command
# fail with "Invalid private key" instead (CHAOS-3402).
_CONFIG_ENV = (
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "DATABASE_URL",
    "ORG_ID",
    "LICENSE_KEY",
    "LICENSE_PRIVATE_KEY",
    "LICENSE_PUBLIC_KEY",
    "LICENSE_SECRET_KEY",
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
    # CHAOS-5308: `metrics compounding-risk` used to be the exemplar here
    # for a command requiring BOTH ClickHouse AND organization together --
    # its whole CLI verb (job_compounding_risk.py) is deleted now, no
    # remaining Python producer of this family at any scope. No other verb
    # below requires both tokens together; nothing replaces this row.
    #
    # CHAOS-5055: daily/rebuild/dora/complexity/release-impact/capacity
    # dispatch to dev-health-workerctl (worker/Postgres-scoped) instead of
    # connecting to ClickHouse directly -- they need --org, not
    # --analytics-db/CLICKHOUSE_URI. dora/complexity/release-impact/capacity
    # also require --review-evidence at the argparse level now (uniform
    # policy) -- supplied here so THIS test actually exercises the org
    # preflight rather than tripping argparse's own required-flag error
    # first.
    (("metrics", "daily"), ("organization",)),
    (("metrics", "dora", "--review-evidence", "x"), ("organization",)),
    (("metrics", "complexity", "--review-evidence", "x"), ("organization",)),
    (("metrics", "release-impact", "--review-evidence", "x"), ("organization",)),
    (
        ("metrics", "capacity", "--all-teams", "--review-evidence", "x"),
        ("organization",),
    ),
    (("metrics", "validate-flags"), ("ClickHouse",)),
    (("metrics", "rebuild"), ("organization",)),
    (("sync", "work-items"), ("ClickHouse",)),
    (
        ("audit", "completeness", "--db", "clickhouse://localhost"),
        ("organization",),
    ),
    (("audit", "perf"), ("ClickHouse",)),
    (("audit", "schema"), ("ClickHouse",)),
    (("investment", "materialize"), ("ClickHouse",)),
    (
        (
            "admin",
            "users",
            "create",
            "--email",
            "x@example.com",
            "--password",
            "yyyyyyyy",
        ),
        ("PostgreSQL",),
    ),
    (("admin", "users", "list"), ("PostgreSQL",)),
    (
        ("admin", "users", "update", "--email", "x@example.com", "--full-name", "X"),
        ("PostgreSQL",),
    ),
    (("admin", "orgs", "create", "--name", "x"), ("PostgreSQL",)),
    (
        ("admin", "orgs", "delete", "--org-id", "00000000-0000-0000-0000-000000000000"),
        ("PostgreSQL",),
    ),
    (("admin", "orgs", "list"), ("PostgreSQL",)),
    (("admin", "features", "seed"), ("PostgreSQL",)),
    (("admin", "billing", "seed"), ("PostgreSQL",)),
    (("admin", "billing", "list"), ("PostgreSQL",)),
    (("admin", "billing", "pull-stripe", "--dry-run"), ("PostgreSQL",)),
    (("admin", "billing", "sync-stripe"), ("PostgreSQL",)),
    (
        (
            "admin",
            "bundles",
            "create",
            "--key",
            "bundle-x",
            "--name",
            "Bundle X",
            "--features",
            "api_access",
        ),
        ("PostgreSQL",),
    ),
    (("admin", "bundles", "list"), ("PostgreSQL",)),
    (
        (
            "admin",
            "bundles",
            "assign-plan",
            "--bundle-key",
            "bundle-x",
            "--plan-key",
            "team",
        ),
        ("PostgreSQL",),
    ),
    (
        (
            "admin",
            "bundles",
            "assign-org",
            "--org-id",
            "00000000-0000-0000-0000-000000000000",
            "--feature-key",
            "api_access",
        ),
        ("PostgreSQL",),
    ),
    (("billing", "reconcile"), ("PostgreSQL",)),
    (("migrate", "postgres", "upgrade"), ("PostgreSQL",)),
    (("migrate", "clickhouse", "status"), ("ClickHouse",)),
    # backfill run reads the SyncConfiguration from Postgres; the org is
    # derived from --config-id, so the missing input is PostgreSQL, not --org.
    (("backfill", "run", "--config-id", "x"), ("PostgreSQL",)),
    # Bare migrate forms default to upgrade and must be guarded too.
    (("migrate", "postgres"), ("PostgreSQL",)),
    (("migrate", "clickhouse"), ("ClickHouse",)),
    (("service-credentials", "list"), ("PostgreSQL",)),
    # sync teams persists to ClickHouse after generating teams.
    (("sync", "teams", "--provider", "synthetic"), ("ClickHouse",)),
    # finalize-synthetic-sync (CHAOS-4266) only completes a durable sync_run
    # in Postgres -- it never touches ClickHouse -- and needs a resolved org.
    (
        ("sync", "finalize-synthetic-sync", "--target", "cicd"),
        ("PostgreSQL", "organization"),
    ),
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


# CHAOS-5308: test_configured_command_passes_preflight used to live here,
# exercising `metrics compounding-risk` (CLICKHOUSE_URI + ORG_ID both set)
# to prove a fully-configured command clears preflight -- its sole subject
# is deleted whole (job_compounding_risk.py), and no other command needs
# BOTH ClickHouse and organization together the way it did, so nothing
# replaces it.


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


def test_work_graph_build_rejects_unsupported_db_scheme_cleanly() -> None:
    result = _run_cli("work-graph", "build", "--db", "sqlite:///x.db")

    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert "Unknown or unsupported sink scheme 'sqlite'" in result.stderr
    assert "Only ClickHouse is supported" in result.stderr


def test_service_credentials_rejects_non_postgres_db_flag_cleanly(tmp_path) -> None:
    invalid_db = tmp_path / "not-postgres.db"
    result = _run_cli("--db", f"sqlite:///{invalid_db}", "service-credentials", "list")

    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert "PostgreSQL" in result.stderr
    invalid_db.unlink(missing_ok=True)
    assert not invalid_db.exists()


def test_sync_rejects_unsupported_analytics_scheme_cleanly() -> None:
    result = _run_cli(
        "sync",
        "git",
        "--provider",
        "synthetic",
        "--analytics-db",
        "sqlite:///x.db",
    )

    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert "Unknown or unsupported sink scheme 'sqlite'" in result.stderr
    assert "Only ClickHouse is supported" in result.stderr


@pytest.mark.parametrize(
    "args",
    [
        ("investment", "materialize", "--db", "sqlite:///x.db"),
        # CHAOS-5055: `metrics capacity` no longer takes its own --db / reads
        # a ClickHouse DSN directly -- it dispatches to dev-health-workerctl
        # (org/team-scoped), so this case no longer applies here.
        # CHAOS-5307: `recommendations compute` deleted entirely (the whole
        # `dev-hops recommendations` group had exactly this one verb) -- no
        # replacement case needed here.
    ],
)
def test_clickhouse_commands_reject_unsupported_analytics_scheme_cleanly(
    args: tuple[str, ...],
) -> None:
    result = _run_cli(*args)

    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert "Unknown or unsupported sink scheme 'sqlite'" in result.stderr
    assert "Only ClickHouse is supported" in result.stderr


def test_admin_license_create_is_not_a_postgres_preflight_false_positive() -> None:
    result = _run_cli(
        "admin",
        "licenses",
        "create",
        "--org-id",
        "00000000-0000-0000-0000-000000000000",
        "--tier",
        "team",
    )

    assert result.returncode == 1
    assert "missing required input" not in result.stderr
    assert "PostgreSQL" not in result.stderr
    assert "LICENSE_PRIVATE_KEY" in result.stdout


# CHAOS-5308: test_help_lists_requirements_in_epilog used to live here,
# exercising `metrics compounding-risk --help`'s "Requires: ClickHouse,
# organization" epilog line -- its sole subject is deleted whole
# (job_compounding_risk.py). cli.py's own `_COMMAND_REQUIREMENTS` registry
# entry for ("metrics", "compounding-risk") is deleted too (same commit),
# so there is no epilog left to render, let alone assert on.


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
