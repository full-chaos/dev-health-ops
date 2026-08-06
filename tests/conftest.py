"""Shared test fixtures for the test suite."""

import logging
import mimetypes
import os
import uuid
from pathlib import Path

import pytest
from git import Repo as GitRepo

from tests._env_isolation import (
    ALLOW_ENV,
    exempted_names,
    scrub_ambient_env,
)

_SCRUBBED_ENV_NAMES: list[str] = []


@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Ensure a default DATABASE_URI and JWT_SECRET_KEY are set for tests."""
    monkeypatch.setenv("DATABASE_URI", "sqlite:///:memory:")
    # JWT_SECRET_KEY is now a hard requirement with no derivation fallback
    # (CHAOS-1266). Provide a safe default for tests; tests that need to
    # assert the "unset" behaviour use monkeypatch.delenv to override.
    monkeypatch.setenv(
        "JWT_SECRET_KEY",
        "test-jwt-secret-key-at-least-32-characters-long",
    )
    # Rate limiter (CHAOS-1554) hard-fails at startup when REDIS_URL is unset
    # in non-dev environments. Mark tests as a dev-equivalent environment.
    monkeypatch.setenv("ENVIRONMENT", "test")


@pytest.fixture(autouse=True)
def mock_analytics_db_url(monkeypatch):
    """Mock analytics DB URL so endpoints don't return 503 in tests."""
    monkeypatch.setattr(
        "dev_health_ops.api.main._analytics_db_url",
        lambda: "clickhouse://localhost:8123/default",
    )


@pytest.fixture(autouse=True)
def _reset_sync_db_engine():
    """Reset the cached global sync Postgres engine around every test.

    ``get_postgres_sync_engine()`` caches a process-global engine keyed off
    POSTGRES_URI on first use. Without this, a test running earlier on an xdist
    worker can leave a cached engine bound to its own database, so a later test
    that monkeypatches POSTGRES_URI reads the wrong (empty) database and sees
    missing rows (CHAOS-2586). Resetting before and after each test keeps every
    test bound to its own env.
    """
    from dev_health_ops.db import reset_sync_engine

    reset_sync_engine()
    yield
    reset_sync_engine()


@pytest.fixture
def repo_path():
    """Return the path to the current repository for testing."""
    return str(Path(__file__).parent.parent)


@pytest.fixture
def repo_uuid():
    """Return a test UUID for the repository."""
    return uuid.uuid4()


@pytest.fixture
def git_repo(repo_path):
    """Return a GitRepo instance for testing."""
    return GitRepo(repo_path)


@pytest.fixture
def test_file(repo_path):
    """Return a path to an existing file in the repository."""
    return os.path.join(repo_path, "README.md")


@pytest.fixture
def quiet_aiosqlite_logger():
    """Stop the test harness's own SQL driver from echoing bind parameters.

    ``aiosqlite`` logs every operation — including INSERT statements with their
    bind parameters — at DEBUG. Tests that assert "this secret never reaches the
    logs" by sweeping every captured record therefore rediscover their own
    planted secret whenever the root level is DEBUG, even though production never
    runs on aiosqlite (it is a test-fixture-only driver, see ops/AGENTS.md) and
    the code under test never logged it.

    ``LOG_LEVEL`` is scrubbed suite-wide (CHAOS-3402), so this is the belt to
    that braces: it states the precondition at the tests that depend on it, and
    keeps holding if a future run configures logging some other way. It narrows
    only the driver, so a genuine DEBUG-level leak from application code is still
    captured and still fails the assertion.
    """
    driver_logger = logging.getLogger("aiosqlite")
    previous = driver_logger.level
    driver_logger.setLevel(logging.INFO)
    try:
        yield
    finally:
        driver_logger.setLevel(previous)


def pytest_configure(config):
    # Ensure TypeScript files are treated as text, not video/mp2t.
    mimetypes.add_type("text/x-typescript", ".ts")

    # CHAOS-3402: make the process environment CI-equivalent before any test
    # module is imported. A developer shell carries direnv-loaded `ops/.env`,
    # so tests whose intent is "this variable is ABSENT" would otherwise see a
    # real-looking-but-wrong value and take a different code path. Scrubbing
    # here rather than in an autouse fixture also covers import-time reads and
    # subprocesses that inherit via `os.environ.copy()`. Rationale and the
    # keep-list justification live in tests/_env_isolation.py.
    _SCRUBBED_ENV_NAMES[:] = scrub_ambient_env(os.environ, exempt=exempted_names())


def pytest_report_header(config):
    """Say out loud what the shell was carrying.

    A scrub that silently succeeded is indistinguishable from a clean shell, and
    an exemption that silently applied is indistinguishable from a scrubbed run.
    Both are reported so neither can be mistaken for the other.
    """
    lines = []
    if _SCRUBBED_ENV_NAMES:
        lines.append(
            "ambient env scrubbed (CHAOS-3402): " + ", ".join(_SCRUBBED_ENV_NAMES)
        )
    exempt = exempted_names()
    if exempt:
        lines.append(
            f"ambient env NOT scrubbed via {ALLOW_ENV}: " + ", ".join(sorted(exempt))
        )
    return lines
