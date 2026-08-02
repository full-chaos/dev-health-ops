"""The run-state vocabulary must agree in four places at once (CHAOS-3292).

``RunState`` is enforced by a database CHECK constraint, not by an import, so
adding a member without the matching Alembic revision fails at persistence —
on the one request that happens to hit the new state, in production. These are
pure-Python drift guards that run in every tier and fail loudly at build time
instead.
"""

from __future__ import annotations

import importlib

from sqlalchemy import CheckConstraint, String

from dev_health_ops.api.dev.orchestrator_states import TERMINAL_STATES, RunState
from dev_health_ops.api.dev.persistence.service import (
    _RUN_STATES,
    _TERMINAL_RUN_STATES,
)
from dev_health_ops.models.dev_persistence import DevRun

_MIGRATION = importlib.import_module(
    "dev_health_ops.alembic.versions.0072_widen_dev_run_states"
)


def _check_constraint_text() -> str:
    for constraint in DevRun.__table_args__:
        if isinstance(constraint, CheckConstraint) and (
            constraint.name == "ck_dev_runs_state"
        ):
            return str(constraint.sqltext)
    raise AssertionError("ck_dev_runs_state is not declared on DevRun")


def test_model_check_constraint_covers_every_run_state() -> None:
    sqltext = _check_constraint_text()
    missing = sorted(
        state.value for state in RunState if f"'{state.value}'" not in sqltext
    )
    assert missing == []


def test_persistence_service_vocabulary_matches_the_enum_exactly() -> None:
    assert _RUN_STATES == {state.value for state in RunState}
    assert _TERMINAL_RUN_STATES == {state.value for state in TERMINAL_STATES}


def test_migration_0072_enumerates_exactly_the_current_run_states() -> None:
    # Both halves: the migration must not omit a state the model requires, and
    # must not invent one the enum does not have.
    migration_states = set(_MIGRATION._PRIOR_STATES) | set(_MIGRATION._NEW_STATES)
    assert migration_states == {state.value for state in RunState}
    assert set(_MIGRATION._NEW_STATES) == {"interpreting", "resolving_subjects"}
    assert _MIGRATION.down_revision == "0071"


def test_the_new_preflight_states_are_not_terminal() -> None:
    """A run parked mid-preflight must read as still running, not as finished.

    The replay path in ``router`` treats any non-terminal state as an in-flight
    run (409 ``concurrency_limited``); classifying either new state as terminal
    would make a half-finished run replay as a completed one.
    """

    assert RunState.INTERPRETING not in TERMINAL_STATES
    assert RunState.RESOLVING_SUBJECTS not in TERMINAL_STATES


def test_router_replay_allowlist_matches_the_terminal_set() -> None:
    import inspect

    from dev_health_ops.api.dev import router

    source = inspect.getsource(router)
    marker = "if replay_run.state not in {"
    assert marker in source, "the replay allow-list moved; re-point this guard"
    block = source.split(marker, 1)[1].split("}", 1)[0]
    listed = {
        line.strip().rstrip(",").removeprefix("RunState.").removesuffix(".value")
        for line in block.splitlines()
        if line.strip()
    }
    assert listed == {state.name for state in TERMINAL_STATES}


def test_run_diagnostic_columns_are_present_and_bounded() -> None:
    columns = DevRun.__table__.columns
    for name, length in (("preflight_outcome", 32), ("legacy_guard_reason", 64)):
        column_type = columns[name].type
        assert isinstance(column_type, String)
        assert column_type.length == length
        assert columns[name].nullable
