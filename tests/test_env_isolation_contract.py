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

import pytest

from tests import _env_isolation, conftest
from tests._env_isolation import (
    CONDITIONAL_KEEP_ENV_NAMES,
    KEEP_ENV_NAMES,
    SCRUB_ENV_NAMES,
    derive_scrub_names,
    discover_env_example_names,
    discover_src_env_names,
    exempted_names,
    lane_conditional_keeps,
    scrub_ambient_env,
)

# The ambient variables that were observed flipping code paths, across six test
# files. Each is here because a run with it set was watched failing and a run
# with it absent was watched passing -- not because it looked risky.
OBSERVED_ROOT_CAUSES = (
    "GITHUB_APP_PRIVATE_KEY_PATH",
    "AUTH_AUTO_CREATE_ORG_ON_REGISTER",
    "LOG_LEVEL",
    "LICENSE_PRIVATE_KEY",
    # Fifth, and initially excluded by CHAOS-3402 as a "pure concurrency flake"
    # because it passes when run alone. It does not pass alongside its siblings:
    # ops/.env points REDIS_URL at the live shared valkey container, whose
    # rate-limit state survives between tests. Whole file, populated shell:
    # 1 failed (assert 5 == 4); same file with REDIS_URL unset: 65 passed.
    "REDIS_URL",
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
    # CHAOS-3452: an ANNOTATED module constant (``NAME: Final = "..."``,
    # llm/qua_shadow_budget.py's own style) is a THIRD constant-resolution
    # shape, distinct from AUTH_AUTO_CREATE_ORG_ON_REGISTER's plain
    # ``NAME = "..."`` above -- _module_string_constants originally only
    # matched ast.Assign, silently missing every ast.AnnAssign constant.
    assert "ASK_DEV_QUA_SHADOW_MAX_BUDGET_MICRO_USD" in names


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
        "REDIS_URL": "redis://localhost:6379/0",
        "CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/ci_local_validate",
        "DEV_HEALTH_POSTGRES_TEST_URI": "postgresql+asyncpg://u:p@localhost/test",
        "PATH": "/usr/bin",
    }

    removed = scrub_ambient_env(environ, exempt=lane_conditional_keeps(environ))

    assert sorted(removed) == sorted(OBSERVED_ROOT_CAUSES)
    assert environ == {
        "CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/ci_local_validate",
        "DEV_HEALTH_POSTGRES_TEST_URI": "postgresql+asyncpg://u:p@localhost/test",
        "PATH": "/usr/bin",
    }


def test_conditional_keep_returns_redis_url_to_the_live_e2e_lane():
    """Both directions, because only the pair proves the condition does anything.

    ci/run_live_backend_e2e.sh exports REDIS_URL and LIVE_E2E_BASE_URL together,
    then runs the one module that skipifs itself without REDIS_URL. Scrubbing it
    there would convert that lane's coverage into a silent skip.
    """
    assert CONDITIONAL_KEEP_ENV_NAMES["REDIS_URL"] == "LIVE_E2E_BASE_URL"

    unit_tier = {"REDIS_URL": "redis://localhost:6379/0"}
    assert scrub_ambient_env(unit_tier, exempt=lane_conditional_keeps(unit_tier)) == [
        "REDIS_URL"
    ]
    assert unit_tier == {}

    live_lane = {
        "REDIS_URL": "redis://localhost:6379/0",
        "LIVE_E2E_BASE_URL": "http://127.0.0.1:8000",
    }
    assert scrub_ambient_env(live_lane, exempt=lane_conditional_keeps(live_lane)) == []
    assert live_lane["REDIS_URL"] == "redis://localhost:6379/0"


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

    It asserts the residue snapshotted immediately after the scrub rather than a
    live read of ``os.environ``: tests legitimately write scrub-listed names
    during a run and do not always clean up (``tests/test_core_extraction.py:19``
    sets SETTINGS_ENCRYPTION_KEY unconditionally), and under ``--dist loadscope``
    such a leftover lands on this worker. A live read blamed the scrub for
    another test's leftover and failed the whole-suite run.
    """
    assert conftest._SCRUB_RAN, "conftest.pytest_configure did not run the scrub"
    assert not conftest._POST_SCRUB_RESIDUE, (
        f"scrub-listed names survived the scrub: {conftest._POST_SCRUB_RESIDUE}"
    )
