"""Contract tests for the ambient-environment scrub (CHAOS-3402).

The scrub is a guard, and a guard that cannot fail reads as coverage while
covering nothing. These tests pin four separate ways it could rot:

* the derivation stops finding anything (a rename of ``src/`` or ``.env.example``
  would otherwise silently produce an empty scrub list),
* ``src/`` starts reading a variable nobody added to the checked-in list,
* the keep-list and the scrub-list start disagreeing, or
* ``pytest_configure`` stops running the scrub at all.
"""

from __future__ import annotations

import os

import pytest

from tests import _env_isolation
from tests._env_isolation import (
    KEEP_ENV_NAMES,
    SCRUB_ENV_NAMES,
    derive_scrub_names,
    discover_env_example_names,
    discover_src_env_names,
    exempted_names,
    scrub_ambient_env,
)

# The four ambient variables that were observed flipping code paths in fifteen
# tests across five files. Each is here because a run with it set was watched
# failing and a run with it absent was watched passing -- not because it looked
# risky.
OBSERVED_ROOT_CAUSES = (
    "GITHUB_APP_PRIVATE_KEY_PATH",
    "AUTH_AUTO_CREATE_ORG_ON_REGISTER",
    "LOG_LEVEL",
    "LICENSE_PRIVATE_KEY",
)

# Scrubbing any of these would break the lane that supplies it: ci/local_validate.sh
# points CLICKHOUSE_URI at a per-worktree scratch database for the live stage, and
# DEV_HEALTH_POSTGRES_TEST_URI gates the opt-in live-Postgres tier -- a scrub there
# would turn a whole tier into silent skips rather than a visible failure.
LANE_CRITICAL_KEEPS = (
    "CLICKHOUSE_URI",
    "DATABASE_URI",
    "DEV_HEALTH_POSTGRES_TEST_URI",
)


def test_discovery_finds_env_reads_in_src():
    """An empty scan must fail, not quietly produce an empty scrub list."""
    names = discover_src_env_names()
    assert len(names) > 100, f"src/ env-var scan found only {len(names)} names"
    # A literal read and a module-constant read: the constant form is what
    # AUTH_AUTO_CREATE_ORG_ON_REGISTER uses, and a scanner that only handled
    # literals would miss the cause of eight of the fifteen failures.
    assert "LOG_LEVEL" in names
    assert "AUTH_AUTO_CREATE_ORG_ON_REGISTER" in names


def test_discovery_finds_declarations_in_env_example():
    names = discover_env_example_names()
    assert len(names) > 50, f".env.example scan found only {len(names)} names"
    assert "CLICKHOUSE_URI" in names


def test_checked_in_scrub_list_matches_derivation():
    derived = derive_scrub_names()
    missing = sorted(derived - SCRUB_ENV_NAMES)
    stale = sorted(SCRUB_ENV_NAMES - derived)
    assert not missing and not stale, (
        "SCRUB_ENV_NAMES has drifted from the tree.\n"
        f"  add to the list:    {missing}\n"
        f"  remove from list:   {stale}\n"
        "Regenerate with _env_isolation.render_scrub_literal(), or add the name "
        "to KEEP_ENV_NAMES with the CI lane or test tier that supplies it."
    )


def test_keep_and_scrub_lists_are_disjoint():
    assert not (KEEP_ENV_NAMES & SCRUB_ENV_NAMES)


@pytest.mark.parametrize("name", OBSERVED_ROOT_CAUSES)
def test_observed_root_causes_are_scrubbed(name):
    assert name in SCRUB_ENV_NAMES


@pytest.mark.parametrize("name", LANE_CRITICAL_KEEPS)
def test_lane_critical_variables_are_never_scrubbed(name):
    assert name in KEEP_ENV_NAMES
    assert name not in SCRUB_ENV_NAMES


def test_scrub_removes_offenders_and_preserves_lane_variables():
    environ = {
        "GITHUB_APP_PRIVATE_KEY_PATH": "./github-app-local.pem",
        "AUTH_AUTO_CREATE_ORG_ON_REGISTER": "false",
        "LOG_LEVEL": "debug",
        "LICENSE_PRIVATE_KEY": "x" * 88,
        "CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/ci_local_validate",
        "DEV_HEALTH_POSTGRES_TEST_URI": "postgresql+asyncpg://u:p@localhost/test",
        "PATH": "/usr/bin",
    }

    removed = scrub_ambient_env(environ)

    assert sorted(removed) == sorted(OBSERVED_ROOT_CAUSES)
    assert environ == {
        "CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/ci_local_validate",
        "DEV_HEALTH_POSTGRES_TEST_URI": "postgresql+asyncpg://u:p@localhost/test",
        "PATH": "/usr/bin",
    }


def test_scrub_honours_the_debug_exemption():
    environ = {"LOG_LEVEL": "debug", "LICENSE_PRIVATE_KEY": "x"}

    removed = scrub_ambient_env(environ, exempt={"LOG_LEVEL"})

    assert removed == ["LICENSE_PRIVATE_KEY"]
    assert environ == {"LOG_LEVEL": "debug"}


def test_exempted_names_parses_the_allow_list():
    assert exempted_names({_env_isolation.ALLOW_ENV: " LOG_LEVEL , LOG_JSON ,"}) == {
        "LOG_LEVEL",
        "LOG_JSON",
    }
    assert exempted_names({}) == frozenset()


def test_scrub_actually_ran_in_this_process():
    """The wiring, not just the data.

    If ``pytest_configure`` stops calling the scrub, every other test in this
    module still passes -- they only inspect lists. This one fails.
    """
    exempt = exempted_names()
    leaked = sorted(
        name for name in SCRUB_ENV_NAMES if name not in exempt and name in os.environ
    )
    assert not leaked, (
        f"ambient environment still carries {leaked} during the test run; "
        "the conftest scrub did not run"
    )
