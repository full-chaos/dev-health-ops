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
    SCRUB_ENV_NAMES,
    exempted_names,
    lane_conditional_keeps,
    scrub_ambient_env,
)

#: Env var carrying the scrub record across a process boundary (CHAOS-3462).
#: Deliberately NOT a name any ``src/`` module reads and not in
#: ``.env.example``, so it is absent from ``SCRUB_ENV_NAMES`` and the scrub
#: never deletes its own record.
SCRUB_RECORD_ENV = "DEV_HEALTH_TEST_SCRUBBED_ENV_NAMES"

#: Pre-scrub values that mean "the operator deliberately turned this OFF".
#: A name scrubbed while holding one of these is NOT recorded, because the
#: record's only consumer asks "did someone intend to switch this on".
#:
#: Without this, ``ASK_DEV_LIVE_ACCEPTANCE=0`` -- an explicit REFUSAL to arm
#: -- was recorded identically to ``=1`` and the corpus runner concluded the
#: run had been armed and scrubbed, turning a deliberately disarmed run RED
#: (reproduced). The record can only ever carry names, never values: it is
#: exported to child processes, and several scrubbed names hold real
#: credentials.
_DISABLING_VALUES = frozenset({"", "0", "false", "no", "off"})

_SCRUBBED_ENV_NAMES: list[str] = []
#: The union of this process's scrub and any inherited from a parent (an
#: xdist controller). See :func:`scrubbed_env_names`.
_SCRUB_RECORD: list[str] = []
_KEPT_ENV_NAMES: list[str] = []
#: Scrub-list names still present in ``os.environ`` immediately AFTER the scrub.
#: Must stay empty. Snapshotted at configure time rather than read live at
#: assertion time because tests legitimately set some of these during the run
#: (``tests/test_core_extraction.py`` writes SETTINGS_ENCRYPTION_KEY and never
#: cleans up), and a live read would blame the scrub for another test's leftovers.
_POST_SCRUB_RESIDUE: list[str] = []
_SCRUB_RAN = False


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
    global _SCRUB_RAN

    kept = exempted_names() | lane_conditional_keeps()
    _KEPT_ENV_NAMES[:] = sorted(kept)
    inherited = _inherited_scrub_record()
    # Snapshot values BEFORE the scrub so the record can distinguish "was set
    # to something meaningful" from "was explicitly set to off". Kept process
    # local and never exported -- see _DISABLING_VALUES.
    prescrub = {
        name: os.environ[name] for name in SCRUB_ENV_NAMES if name in os.environ
    }
    _SCRUBBED_ENV_NAMES[:] = scrub_ambient_env(os.environ, exempt=kept)
    # CHAOS-3462: carry the record ACROSS the process boundary. The list
    # above is process-local, but an xdist controller scrubs and then spawns
    # workers whose environment no longer has the variables -- so a worker's
    # own scrub removes nothing and records nothing, and it cannot tell
    # "never set" from "the controller ate it". Writing the union into an
    # env var the scrub itself does not touch (it is read by no src/ module
    # and absent from .env.example, so it is not in SCRUB_ENV_NAMES) makes
    # the evidence survive the fork, which is exactly what the corpus
    # runner's armed-but-scrubbed guard needs. Unioned, never overwritten,
    # so a worker cannot erase what the controller recorded.
    enabling = {
        name
        for name in _SCRUBBED_ENV_NAMES
        if prescrub.get(name, "").strip().casefold() not in _DISABLING_VALUES
    }
    _SCRUB_RECORD[:] = sorted(inherited | enabling)
    if _SCRUB_RECORD:
        os.environ[SCRUB_RECORD_ENV] = ",".join(_SCRUB_RECORD)
    _POST_SCRUB_RESIDUE[:] = sorted(
        name for name in SCRUB_ENV_NAMES if name not in kept and name in os.environ
    )
    _SCRUB_RAN = True


def _inherited_scrub_record() -> set[str]:
    """The parent's scrub record -- trusted ONLY inside an xdist worker.

    The record exists to cross exactly one boundary: an xdist controller
    scrubs, then spawns workers whose environment no longer carries the
    variables, so a worker cannot otherwise tell "never set" from "the
    controller ate it". ``PYTEST_XDIST_WORKER`` is present in workers and
    absent everywhere else, which makes it a precise test for that boundary.

    Trusting the value unconditionally was wrong, and reproducibly so: with
    ``DEV_HEALTH_TEST_SCRUBBED_ENV_NAMES=ASK_DEV_LIVE_ACCEPTANCE`` exported
    by anything at all -- a stale value, a nested pytest, a copy-pasted
    shell line -- an ordinary UNARMED contributor run turned RED, because
    the corpus runner concluded it had been armed and scrubbed. That is a
    false red on the standing unit gate, produced by an env var a caller can
    set to any value they like. Gating on the worker sentinel keeps the
    xdist recovery path and removes the forgery surface for every other run.
    """

    if "PYTEST_XDIST_WORKER" not in os.environ:
        return set()
    raw = os.environ.get(SCRUB_RECORD_ENV, "")
    return {part.strip() for part in raw.split(",") if part.strip()}


def scrubbed_env_names() -> tuple[str, ...]:
    """Names the CHAOS-3402 scrub removed, in THIS process or an ancestor.

    A name appears here only if it was PRESENT in some process's environment
    and then deleted, which makes this list positive evidence about what the
    invoking shell was carrying -- evidence that survives the deletion of
    the variables themselves. CHAOS-3462 B1 needs exactly that: the live
    corpus runner's arming flag is in ``SCRUB_ENV_NAMES``, so a
    correctly-armed run has no other way to know it was armed.

    Includes the record inherited from a parent process (see
    :data:`SCRUB_RECORD_ENV`), because under xdist the process that does the
    scrubbing is the controller and the process that runs the tests is a
    worker. Reporting only this process's own scrub would make every worker
    look like a clean, never-armed run.

    Public (unlike the private lists it reads) because it is a supported
    read for test modules, and returns a tuple so a caller cannot mutate the
    record.
    """

    return tuple(_SCRUB_RECORD)


@pytest.fixture(scope="session")
def scrubbed_ambient_env_names() -> tuple[str, ...]:
    """Fixture form of :func:`scrubbed_env_names`."""

    return scrubbed_env_names()


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
    lane_kept = lane_conditional_keeps()
    if lane_kept:
        lines.append(
            "ambient env kept for an announced lane: " + ", ".join(sorted(lane_kept))
        )
    return lines
