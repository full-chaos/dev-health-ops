"""CHAOS-3617: the guard-injection harness's own verification.

The harness decides which guards this lane claims are proven. Nothing else
checks it, so an error here does not produce a red build — it produces a
*confident green* over guards that were never exercised. That makes it the
one piece of verification infrastructure that most needs verification of its
own.

The specific hole this closes was demonstrated in a sibling lane: pytest
echoes the failing test's SOURCE up to the failing line, so any phrase in a
docstring, a comment, or an assertion that PASSED earlier in the same test
appears in the output. A checker that searched the whole output would credit
that phrase and report an unearned kill.

The fix is the *region*, not a cleverer phrase. Echoed source is indented;
only real failures produce ``E ``-prefixed lines or a ``FAILED <nodeid>``
summary. These tests prove the region check rejects each echo shape and still
accepts a genuine failure — because a checker asserted to be correct is worth
no more than the guards it was supposed to be checking.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "chaos_3617_guard_injection.py"
)


def _harness():
    """Load the harness script as a module.

    Loaded by path because it is a script rather than a package member. The
    alternative — duplicating its logic here — would test a copy and leave
    the thing that actually runs unverified.
    """

    spec = importlib.util.spec_from_file_location("chaos_3617_guard_injection", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _harness()


#: A pytest run in which the phrase appears ONLY in echoed source: once in a
#: docstring, once in a comment, and once in an assertion that passed before
#: the real failure. This is the exact shape that produced an unearned kill.
_ECHOED_SOURCE_OUTPUT = """
=================================== FAILURES ===================================
______________________ test_a_guard_that_did_not_fire __________________________

    def test_a_guard_that_did_not_fire() -> None:
        \"\"\"causality is inverted when the direction is ignored.\"\"\"

        # causality is inverted -- named here so the next reader sees it
        assert "causality is inverted" in DOC
>       assert subject.standing is DriverStanding.PRINCIPAL_DRIVER
E       AssertionError: assert <DriverStanding.EXCLUDED> is <DriverStanding.PRINCIPAL_DRIVER>

=========================== short test summary info ============================
FAILED tests/context_fabric/test_x.py::test_a_guard_that_did_not_fire - Assert...
1 failed in 0.10s
"""

#: The same phrase, this time where it belongs: in the failure itself.
_REAL_FAILURE_OUTPUT = """
=================================== FAILURES ===================================
______________________ test_the_guard_that_did_fire ____________________________

    def test_the_guard_that_did_fire() -> None:
>       assert not inverted, "causality is inverted"
E       AssertionError: causality is inverted
E       assert not ['drv_block_proj_identity_rewrite']

=========================== short test summary info ============================
FAILED tests/context_fabric/test_x.py::test_the_guard_that_did_fire - Assertion...
1 failed in 0.10s
"""


class TestTheRegionExcludesEchoedSource:
    def test_a_phrase_only_in_a_docstring_or_comment_is_not_evidence(
        self, harness
    ) -> None:
        """The unearned-kill shape, rejected.

        Every occurrence of the phrase here sits in echoed source. The test
        did fail — but for an unrelated reason — so crediting the phrase
        would report a guard as proven when it never ran.
        """

        region = harness.failure_region(_ECHOED_SOURCE_OUTPUT)
        assert "causality is inverted" not in region

    def test_a_phrase_in_an_assertion_that_PASSED_is_not_evidence(
        self, harness
    ) -> None:
        """The subtlest half: the assertion ran, and succeeded.

        pytest echoes source up to the failing line, so a passing assertion
        above the failure is printed verbatim. Its text is not evidence of
        anything failing.
        """

        region = harness.failure_region(_ECHOED_SOURCE_OUTPUT)
        assert 'assert "causality is inverted" in DOC' not in region

    def test_a_real_failure_message_IS_evidence(self, harness) -> None:
        """The positive control.

        Without this, a region function that returned the empty string would
        pass both tests above and reject every real kill.
        """

        region = harness.failure_region(_REAL_FAILURE_OUTPUT)
        assert "causality is inverted" in region

    def test_skip_and_xfail_reason_prose_is_not_evidence(self, harness) -> None:
        """The hole this lane actually had.

        The previous filter admitted any line containing ``" - "``, which is
        the shape of an xfail/skip reason line. Those carry long free-text
        explanations and are not failures at all — a token appearing in one
        was credited exactly like an assertion message.
        """

        output = (
            "XFAIL tests/x.py::test_y - NAMED RESIDUAL: causality is inverted "
            "here only as an accepted, documented residual\n"
            "SKIPPED [1] tests/z.py:9: causality is inverted - needs a live store\n"
            "1 xfailed, 1 skipped in 0.10s\n"
        )
        assert "causality is inverted" not in harness.failure_region(output)

    def test_the_failed_summary_must_be_anchored(self, harness) -> None:
        """A line merely mentioning FAILED is not the summary line."""

        output = "    # this asserts the run FAILED for the right reason\n"
        assert harness.failure_region(output).strip() == ""


class TestTheReasonCheckCannotBeVacuous:
    def test_an_empty_expected_token_credits_nothing(self, harness) -> None:
        """``"" in region`` is true for every region ever produced.

        A mutation that declared no expected reason would otherwise be
        credited by any failure at all — the harness's WRONG-REASON rule
        silently disabled for that entry.
        """

        proven, why = harness._reason_is_proven(_REAL_FAILURE_OUTPUT, "")
        assert not proven
        assert "empty" in why

    def test_an_empty_failure_region_proves_nothing(self, harness) -> None:
        """No lines attributed to a failure means no evidence to read."""

        proven, why = harness._reason_is_proven("", "causality is inverted")
        assert not proven

    def test_a_present_reason_in_a_real_region_is_proven(self, harness) -> None:
        """The positive control for the checker itself."""

        region = harness.failure_region(_REAL_FAILURE_OUTPUT)
        proven, why = harness._reason_is_proven(region, "causality is inverted")
        assert proven
        assert why == ""


class TestEveryMutationDeclaresACheckableReason:
    def test_no_mutation_declares_an_empty_expected_failure(self, harness) -> None:
        assert harness.MUTATIONS
        for mutation in harness.MUTATIONS:
            assert mutation.expect_failure.strip(), mutation.mutation_id

    def test_mutation_ids_are_unique(self, harness) -> None:
        ids = [mutation.mutation_id for mutation in harness.MUTATIONS]
        assert len(set(ids)) == len(ids)

    def test_the_bare_assert_token_count_is_pinned_and_falling(self, harness) -> None:
        """An honest ledger of how much of the harness is still undiscriminating.

        ``expect_failure="assert"`` is satisfied by ANY failing assertion, so
        those entries prove the suite went red and nothing about WHY. That is
        a red check wearing a category check's clothes.

        The region check above makes every token trustworthy; it does not
        make a weak token specific. This pin records how many remain and
        fails if the number grows, so the count can only be paid down.
        """

        bare = [
            mutation.mutation_id
            for mutation in harness.MUTATIONS
            if mutation.expect_failure == "assert"
        ]
        assert len(bare) <= 36, sorted(bare)
