"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the armed-or-throw check, factored out
as a pure function so it is unit-testable without invoking pytest's own
fixture/collection machinery.

Generalizes the existing smoke-script convention
(``os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"`` -> exit 64, see
``smoke_ask_dev_exact_commit.py``'s ``main()``) for a pytest-native runner:
there is no meaningful "exit code" inside a pytest test, so the live corpus
runner's session fixture converts :class:`NotArmedError` into a
``pytest.skip`` (nobody asked for this run) and
:class:`ArmedButScrubbedError` into ``pytest.fail(..., pytrace=False)`` -- a
definite, red FAILED outcome.

THREE STATES, NOT TWO (CHAOS-3462 B1, from the CHAOS-3219 Phase 2 exit
evidence run)
------------------------------------------------------------------------
The original design had two: armed (run) and not-armed (skip). A blocked
evidence run found the third, and it is the dangerous one.

CHAOS-3402's ``tests/_env_isolation.py`` scrubs ambient environment in
``pytest_configure`` -- before any module is imported and long before any
fixture runs -- and ``ASK_DEV_LIVE_ACCEPTANCE`` is in its ``SCRUB_ENV_NAMES``.
So an operator who boots the acceptance stack and correctly exports
``ASK_DEV_LIVE_ACCEPTANCE=1`` gets that variable *deleted* out from under
them. The arming check then sees an unset variable, concludes "nobody asked
for this run", and skips all 144 cases: ``144 skipped``, exit 0. A green,
entirely-skipped session, indistinguishable at a glance from a real pass --
precisely the false green this lane exists to prevent.

The variable itself is gone by the time anything can look at it, so the
recovery signal has to be something the scrub cannot erase: the scrub's own
RECORD of what it removed. ``scrub_ambient_env`` returns exactly the names
it deleted -- and a name only appears there if it was PRESENT and then
removed. That list is therefore positive evidence the operator armed this
run, and it survives the deletion. :func:`require_armed` takes it as
``scrubbed_names`` and raises :class:`ArmedButScrubbedError` for that third
state.

:class:`ArmedButScrubbedError` deliberately does NOT inherit from
:class:`NotArmedError`. Any existing or future caller that knows only about
``NotArmedError`` (and therefore skips) must not silently swallow the loud
case -- with a subclass relationship, the original ``except NotArmedError:
pytest.skip(...)`` would have kept producing the exact false green this
distinction exists to kill. Unrelated exception types make the safe
behavior (propagate, fail loud) the default one.

THE STANDING FIX is :data:`ARMED_RUN_ENV_ALLOW_NAMES` wired through
``DEV_HEALTH_TEST_ENV_ALLOW`` (``_env_isolation.py``'s own documented
exemption hatch) by the launcher, ``scripts/acceptance/run_wave4_corpus.sh``
-- the guard above is the belt to that braces, for a run invoked some other
way. Note the list covers SIX names, not the four the exit run's hand-rolled
workaround used: ``ASK_DEV_SCRIPTED_PROVIDER_ROLE`` and
``ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR`` are scrubbed too and are read by
the runner's own ``role_script`` session fixture (``current_role`` /
``load_role_script``), so a run missing them would silently fall back to the
default role/scripts dir rather than the one the operator selected.
``tests/acceptance/test_wave4_corpus_arming_false_green.py`` pins the list
against the real ``SCRUB_ENV_NAMES`` so the next omission is caught by the
unit gate rather than by another blocked evidence run.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping

__all__ = [
    "ALLOW_ENV",
    "ARM_ENV_VAR",
    "ARMED_RUN_ENV_ALLOW_NAMES",
    "ArmedButScrubbedError",
    "NotArmedError",
    "env_allow_value",
    "require_armed",
]

ARM_ENV_VAR = "ASK_DEV_LIVE_ACCEPTANCE"

#: ``tests/_env_isolation.py``'s own opt-out hatch. Named here (rather than
#: imported from ``tests._env_isolation``) so this module stays importable
#: by the launcher without pulling the test package in.
ALLOW_ENV = "DEV_HEALTH_TEST_ENV_ALLOW"

#: Every environment variable the ARMED corpus-runner path reads that
#: ``tests/_env_isolation.py`` would otherwise scrub. Sorted, and pinned
#: against the real ``SCRUB_ENV_NAMES`` by
#: ``tests/acceptance/test_wave4_corpus_arming_false_green.py``.
#:
#: * ``ASK_DEV_LIVE_ACCEPTANCE`` -- the arming flag itself.
#: * ``ASK_DEV_PLATFORM_MONTHLY_*`` -- ``QuotaBudget.from_env``'s two
#:   ceilings, read from the RUNNER's env (not the container's), and a
#:   missing ceiling is a hard ``QuotaConfigurationError``, never "no limit".
#: * ``ASK_DEV_SCRIPTED_PROVIDER_ROLE`` / ``_SCRIPTS_DIR`` -- the scripted
#:   provider role and script location the ``role_script`` fixture selects
#:   via ``current_role()`` / ``load_role_script()``. Scrubbing these does
#:   not fail loud; it silently substitutes the DEFAULT role, so a run would
#:   assert a different script's expectations than the operator chose.
#: * ``BUGSINK_SECRET_KEY`` -- required for the acceptance Compose config to
#:   resolve at all (the launcher shells out to ``docker compose`` for the
#:   exec verification plane).
ARMED_RUN_ENV_ALLOW_NAMES: tuple[str, ...] = (
    "ASK_DEV_LIVE_ACCEPTANCE",
    "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD",
    "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX",
    "ASK_DEV_SCRIPTED_PROVIDER_ROLE",
    "ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR",
    "BUGSINK_SECRET_KEY",
)


class NotArmedError(Exception):
    """Nobody asked for this run: the arming env var was never set.

    The ONE legitimately skippable state -- an ordinary contributor's unit
    gate collects this module without having booted the acceptance stack.
    """


class ArmedButScrubbedError(Exception):
    """This run WAS armed, and the CHAOS-3402 env scrub deleted the proof.

    Never a skip. A run in this state has an operator waiting on real
    evidence, so reporting a green, entirely-skipped session would be the
    worst possible outcome -- worse than an honest red, because a reader who
    sees green stops checking.

    Deliberately NOT a subclass of :class:`NotArmedError`; see the module
    docstring.
    """


def env_allow_value(existing: str | None = None) -> str:
    """The ``DEV_HEALTH_TEST_ENV_ALLOW`` value an armed run needs.

    Merges :data:`ARMED_RUN_ENV_ALLOW_NAMES` into whatever the operator
    already set rather than clobbering it -- someone debugging with
    ``DEV_HEALTH_TEST_ENV_ALLOW=LOG_LEVEL`` must not silently lose that
    exemption by running through the launcher. Deduplicated and sorted so
    the value is stable (a launcher that emitted a different string on every
    invocation would make two otherwise-identical runs look different in
    the pytest header this value is echoed into).
    """

    names = {part.strip() for part in (existing or "").split(",") if part.strip()}
    names.update(ARMED_RUN_ENV_ALLOW_NAMES)
    return ",".join(sorted(names))


def require_armed(
    env: Mapping[str, str] | None = None,
    *,
    scrubbed_names: Iterable[str] = (),
) -> None:
    """Return silently iff this run is armed; otherwise raise.

    ``scrubbed_names`` is the list of environment variables the CHAOS-3402
    scrub actually REMOVED this session (``tests/conftest.py``'s
    ``scrubbed_env_names()``). Defaulting it to empty keeps every existing
    caller's two-state behavior intact and makes the third state something a
    caller opts into by supplying the evidence -- a caller with no access to
    the scrub record genuinely cannot distinguish "never set" from "set and
    eaten", and should not pretend to.
    """

    source = env if env is not None else os.environ
    if source.get(ARM_ENV_VAR) == "1":
        return
    if ARM_ENV_VAR in set(scrubbed_names):
        raise ArmedButScrubbedError(
            f"{ARM_ENV_VAR} was exported for this run but the CHAOS-3402 "
            "ambient-env scrub (tests/_env_isolation.py, pytest_configure) "
            "deleted it before the arming check could see it. This run was "
            "ARMED: reporting it as a skip would be a false green. Re-run "
            f"via scripts/acceptance/run_wave4_corpus.sh, or export "
            f"{ALLOW_ENV}={env_allow_value()} yourself before invoking "
            "pytest."
        )
    raise NotArmedError(
        f"{ARM_ENV_VAR}=1 is required before the Wave 4 corpus runner "
        "touches the network -- this must fail loud, never silently "
        "skip or proceed unarmed"
    )
