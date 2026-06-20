"""Shared test fixtures for the test suite."""

import mimetypes
import os
import uuid
from pathlib import Path

import pytest
from git import Repo as GitRepo


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


def pytest_configure(config):
    # Ensure TypeScript files are treated as text, not video/mp2t.
    mimetypes.add_type("text/x-typescript", ".ts")
