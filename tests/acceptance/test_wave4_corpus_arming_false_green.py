"""CHAOS-3462 B1: the armed-but-scrubbed false-green, reproduced end to end.

WHAT THIS EXISTS TO CATCH (CHAOS-3219 Phase 2 exit evidence run, comment
286cfa66): an operator boots the acceptance stack and exports
``ASK_DEV_LIVE_ACCEPTANCE=1``. CHAOS-3402's ``tests/_env_isolation.py``
scrub runs in ``pytest_configure`` -- BEFORE any fixture -- and deletes that
variable (it is in ``SCRUB_ENV_NAMES``). The corpus runner's arming fixture
then sees an unset variable, concludes "nobody asked for this run", and
SKIPS every case. pytest reports ``144 skipped``, exit 0. A green,
entirely-skipped session is indistinguishable at a glance from a real pass
-- the exact false-green this whole lane exists to prevent.

WHY A SUBPROCESS AND NOT A UNIT TEST: the defect lives in the INTERACTION
between ``pytest_configure`` (which runs once, at session start, before this
module is even imported) and a session fixture. Nothing inside an
already-configured pytest session can observe it -- by the time any test
body runs, the scrub has already happened and cannot be un-happened. The
only faithful reproduction is a real, separate pytest session with the real
``tests/conftest.py`` and the real runner module. Each subprocess targets
ONE specific test id (never this module), so there is no recursion.

Neither subprocess touches the network: the armed-but-scrubbed path must
fail at the arming guard itself, which is the very first session fixture,
long before any HTTP/compose call. That is also the assertion -- a run that
reached the network would be a different (and much slower) failure.

The four verification rules, applied:

* rule 1 (assert the state the system exists to reach): the assertion is on
  the pytest OUTCOME (failed / skipped), not on "the guard function
  returned something" -- a guard that ran and then let the session report
  green is exactly the defect.
* rule 2 (observe the guard failing): :class:`TestUnarmedRunStillSkips` is
  the negative control. It proves the fix did not simply turn the arming
  guard into an unconditional failure -- an ordinary contributor's run,
  which genuinely never asked for the live gate, must STILL skip.
* rule 4 (an unmeasured measurement must FAIL loudly): if the subprocess
  produces no parseable pytest summary at all, these tests fail rather than
  silently treating "no evidence" as "no problem".
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.acceptance.corpus.arming import (
    ALLOW_ENV,
    ARM_ENV_VAR,
    ARMED_RUN_ENV_ALLOW_NAMES,
)
from tests._env_isolation import SCRUB_ENV_NAMES
from tests.conftest import SCRUB_RECORD_ENV

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: One collected, network-free test in the live corpus runner module. Chosen
#: because it exercises the session arming fixture (autouse) without needing
#: the acceptance stack: whatever the arming guard decides happens BEFORE
#: this test's own body would run.
_TARGET = (
    "tests/acceptance/test_wave4_corpus_runner_live.py"
    "::test_at_least_one_corpus_case_is_collected"
)


def _run_inner_pytest(extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run one real, isolated pytest session against :data:`_TARGET`.

    The parent's own ``DEV_HEALTH_TEST_ENV_ALLOW`` and arming variables are
    stripped first, so this test states the child's arming environment
    exactly rather than inheriting whatever shell the suite was started
    from (a test whose result depends on the developer's ambient env is not
    a test).
    """

    env = dict(os.environ)
    for name in (ALLOW_ENV, ARM_ENV_VAR, *ARMED_RUN_ENV_ALLOW_NAMES):
        env.pop(name, None)
    # This module is itself collected by the gate's xdist run, so the
    # parent's per-worker state would otherwise leak into the child and make
    # it behave like a worker of a session that does not exist.
    for name in (
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "PYTEST_XDIST_TESTRUNUID",
        "PYTEST_CURRENT_TEST",
        # The parent pytest sets this; inheriting it would make the child
        # believe an ancestor scrubbed the arming variable on its behalf, and
        # the negative control below would then depend on whatever the
        # developer's shell happened to be carrying.
        SCRUB_RECORD_ENV,
    ):
        env.pop(name, None)
    env.update(extra_env)
    # Keep the child single-process and cache-free: xdist/cache would only
    # add noise to a two-outcome assertion.
    return subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            _TARGET,
            "-p",
            "no:cacheprovider",
            "-p",
            "no:xdist",
            "-rs",
            "-q",
        ],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )


def _require_measured(result: subprocess.CompletedProcess[str]) -> str:
    """Rule 4: a subprocess that produced no pytest summary at all has
    MEASURED NOTHING -- fail loudly instead of letting an unparseable run
    read as agreement with whatever this test hoped to see."""

    combined = f"{result.stdout}\n{result.stderr}"
    if not any(
        token in combined for token in (" passed", " failed", " skipped", " error")
    ):
        pytest.fail(
            "inner pytest produced no recognizable outcome summary -- this "
            "test measured nothing and must not report success.\n"
            f"exit={result.returncode}\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}",
            pytrace=False,
        )
    return combined


class TestAllowListCoversEveryScrubbedNameTheArmedRunNeeds:
    """The allow-list is only correct if it actually covers every arming
    variable the scrub would otherwise delete.

    The Phase 2 exit run's hand-rolled workaround listed four names and was
    INCOMPLETE -- it omitted ``ASK_DEV_SCRIPTED_PROVIDER_ROLE`` and
    ``ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR``, both scrubbed and both read
    by the runner's own ``role_script`` session fixture (``current_role`` /
    ``load_role_script``). Pinning the list against the real scrub set is
    what stops the next omission from being discovered by another blocked
    evidence run.
    """

    def test_every_allowed_name_is_actually_scrubbed(self) -> None:
        not_scrubbed = [
            name for name in ARMED_RUN_ENV_ALLOW_NAMES if name not in SCRUB_ENV_NAMES
        ]
        assert not not_scrubbed, (
            f"{not_scrubbed!r} are in the armed-run allow-list but are NOT "
            "scrubbed -- exempting a name that was never at risk makes the "
            "list read as broader protection than it gives"
        )

    def test_the_arming_variable_itself_is_covered(self) -> None:
        # The precondition of the whole B1 defect, asserted rather than
        # assumed: if this ever stops being true, the false green is gone
        # for a different reason and this module should be revisited.
        assert ARM_ENV_VAR in SCRUB_ENV_NAMES
        assert ARM_ENV_VAR in ARMED_RUN_ENV_ALLOW_NAMES


class TestArmedButScrubbedRunFailsLoud:
    """The B1 reproduction: armed shell, no allow-list, must NOT skip-green."""

    @pytest.fixture(scope="class")
    def result(self) -> subprocess.CompletedProcess[str]:
        return _run_inner_pytest({ARM_ENV_VAR: "1"})

    def test_does_not_report_a_green_skipped_session(
        self, result: subprocess.CompletedProcess[str]
    ) -> None:
        combined = _require_measured(result)
        assert result.returncode != 0, (
            "an ARMED run whose arming variable was scrubbed reported exit 0 "
            "-- this is the CHAOS-3462 B1 false green: a fully-skipped "
            f"session masquerading as a pass.\n{combined}"
        )

    def test_names_the_scrub_as_the_cause(
        self, result: subprocess.CompletedProcess[str]
    ) -> None:
        combined = _require_measured(result)
        assert ALLOW_ENV in combined, (
            "the failure must name DEV_HEALTH_TEST_ENV_ALLOW so the operator "
            "can act on it -- an unexplained red is only marginally better "
            f"than a false green.\n{combined}"
        )

    def test_fails_at_the_arming_guard_before_touching_the_stack(
        self, result: subprocess.CompletedProcess[str]
    ) -> None:
        combined = _require_measured(result)
        assert "docker" not in combined.lower(), (
            "the armed-but-scrubbed guard must fire before any compose/HTTP "
            f"work is attempted.\n{combined}"
        )


class TestAForgedScrubRecordCannotTurnAnUnarmedRunRed:
    """The scrub record is an env var, so anything can set it to anything.

    Found by attacking my own fix, and reproduced before it was closed: with
    ``DEV_HEALTH_TEST_SCRUBBED_ENV_NAMES=ASK_DEV_LIVE_ACCEPTANCE`` present,
    a genuinely UNARMED run concluded it had been armed-and-scrubbed and
    went RED. That is a false red on the standing unit gate, triggered by a
    value any caller can forge -- and a gate that can be turned red by a
    stray environment variable is a gate people learn to ignore.

    The record is now trusted only inside an xdist worker
    (``PYTEST_XDIST_WORKER`` present), which is the one boundary it exists
    to cross. Outside that, it is ignored entirely.
    """

    @pytest.fixture(scope="class")
    def result(self) -> subprocess.CompletedProcess[str]:
        return _run_inner_pytest(
            {SCRUB_RECORD_ENV: f"{ARM_ENV_VAR},LOG_LEVEL,OPENAI_API_KEY"}
        )

    def test_still_skips_green(self, result: subprocess.CompletedProcess[str]) -> None:
        combined = _require_measured(result)
        assert result.returncode == 0, (
            "a forged/stale scrub record turned an unarmed run red -- the "
            f"record must not be trusted outside an xdist worker.\n{combined}"
        )
        assert "skipped" in combined


class TestUnarmedRunStillSkips:
    """Negative control (rule 2): the fix must not turn every ordinary
    contributor's unit-gate run red.

    ``ci/local_validate.sh`` runs the whole ``tests/`` directory
    unconditionally, so this module is collected by everyone. A genuinely
    unarmed run -- nobody exported anything, nobody booted the stack -- is
    the ONE legitimate skip, and it must survive the fix.
    """

    @pytest.fixture(scope="class")
    def result(self) -> subprocess.CompletedProcess[str]:
        return _run_inner_pytest({})

    def test_exits_green(self, result: subprocess.CompletedProcess[str]) -> None:
        combined = _require_measured(result)
        assert result.returncode == 0, (
            "an unarmed run must still skip cleanly -- failing here would "
            f"break the standing unit gate for every contributor.\n{combined}"
        )

    def test_reports_a_skip_not_a_pass(
        self, result: subprocess.CompletedProcess[str]
    ) -> None:
        combined = _require_measured(result)
        assert "skipped" in combined, (
            "an unarmed run must report a SKIP, not a silent pass -- a pass "
            "would mean the case actually executed without the stack.\n"
            f"{combined}"
        )
