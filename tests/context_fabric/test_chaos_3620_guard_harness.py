"""CHAOS-3620: the guard-injection harness's own verification.

The harness decides which safety guards this lane claims are proven. Nothing
else checks it, so an error here does not produce a red build — it produces a
*confident green* over guards that were never exercised. That makes it the
one piece of verification infrastructure that most needs verification of its
own.

**Why the full run is not executed here.** The harness edits files under
``src/`` and restores them. The unit tier runs under pytest-xdist with four
workers by default (``ci/run_tests.sh:59``), and a sibling worker importing a
module mid-mutation would fail for a reason that has nothing to do with the
change under test — or worse, pass while reading a disabled guard. The run
therefore stays a deliberate, single-process invocation
(``uv run python scripts/chaos_3620_guard_injection.py``, ~2m35s, 15/15
killed at the commit this suite landed on).

That is a real gap, and it is closed from the other side rather than
narrated. Every way the harness could silently stop proving anything is a
*static* property, and each one is asserted below: an anchor that stopped
matching, a test node id that no longer exists, a token that any failure
would satisfy, a token hiding inside its own node id, two mutations sharing
an anchor, and a region checker that drifted from the one the repository
already verified. A harness that passes all of these and is then run cannot
report an unearned kill; a harness that fails any of them is red here before
anyone reads its output.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_3620 = _REPO_ROOT / "scripts" / "chaos_3620_guard_injection.py"
_SCRIPT_3617 = _REPO_ROOT / "scripts" / "chaos_3617_guard_injection.py"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def harness():
    return _load(_SCRIPT_3620, "chaos_3620_guard_injection")


@pytest.fixture(scope="module")
def harness_3617():
    return _load(_SCRIPT_3617, "chaos_3617_guard_injection_for_3620_test")


#: A pytest run in which the phrase appears ONLY in echoed source: once in a
#: docstring, once in a comment, and once in an assertion that passed before
#: the real failure. This is the exact shape that produces an unearned kill.
_ECHOED_SOURCE_OUTPUT = """
=================================== FAILURES ===================================
______________________ test_a_guard_that_did_not_fire __________________________

    def test_a_guard_that_did_not_fire() -> None:
        \"\"\"the restricted project leaked into a packet.\"\"\"

        # the restricted project leaked into a packet -- named for the reader
        assert "the restricted project leaked into a packet" in DOC
>       assert quarry not in sightings
E       AssertionError: assert 'proj_quarry' not in {'proj_quarry'}

=========================== short test summary info ============================
FAILED tests/context_fabric/test_x.py::test_a_guard_that_did_not_fire - Assert...
1 failed in 0.10s
"""

_REAL_FAILURE_OUTPUT = """
=================================== FAILURES ===================================
______________________ test_the_guard_that_did_fire ____________________________

    def test_the_guard_that_did_fire() -> None:
>       assert world.PROJ_QUARRY not in sightings, "the restricted project leaked into a packet"
E       AssertionError: the restricted project leaked into a packet

=========================== short test summary info ============================
FAILED tests/context_fabric/test_x.py::test_the_guard_that_did_fire - Assertion...
1 failed in 0.10s
"""


class TestThereIsExactlyOneRegionChecker:
    """A copied checker is a checker nothing checks.

    The region rule has already had two holes found by adversarial review in
    this repository, and both fixes landed in one place. A second
    implementation would be the one that drifts, and it would drift silently
    because a checker's own bugs are invisible from its output.
    """

    def test_the_3620_harness_uses_the_3617_region_function_itself(
        self, harness
    ) -> None:
        """Checked by where the code is DEFINED, not by object identity.

        Loading a script by path twice produces two module objects with
        distinct function objects, so ``is`` would compare loader accidents.
        ``__code__.co_filename`` answers the question actually being asked:
        which file does the rule that decides what counts as evidence live
        in.
        """

        assert harness.failure_region.__code__.co_filename == str(_SCRIPT_3617), (
            "the CHAOS-3620 harness's region checker is defined in "
            f"{harness.failure_region.__code__.co_filename}, not in the "
            "CHAOS-3617 harness; a copy has to be verified separately and is "
            "the thing that drifts"
        )

    def test_and_the_reason_checker_too(self, harness) -> None:
        assert harness._reason_is_proven.__code__.co_filename == str(_SCRIPT_3617), (
            "the CHAOS-3620 harness's reason checker is defined in "
            f"{harness._reason_is_proven.__code__.co_filename}, not in the "
            "CHAOS-3617 harness"
        )

    def test_the_shared_functions_are_byte_identical_to_the_3617_module(
        self, harness, harness_3617
    ) -> None:
        """Belt and braces against a same-named shadow file on the path."""

        assert (
            harness.failure_region.__code__.co_code
            == harness_3617.failure_region.__code__.co_code
        ), "the imported region checker differs from the CHAOS-3617 one"


class TestTheRegionRejectsEchoedSourceOnTheObjectThisHarnessUSES:
    """Re-run against the object the 3620 script actually calls.

    The 3617 suite verifies its own module. This verifies the *binding* —
    that what ``chaos_3620_guard_injection.failure_region`` resolves to still
    behaves, which is a different claim from "the file it was imported from
    is correct".
    """

    def test_a_phrase_only_in_echoed_source_is_not_evidence(self, harness) -> None:
        region = harness.failure_region(_ECHOED_SOURCE_OUTPUT)
        assert "the restricted project leaked into a packet" not in region, (
            "a phrase appearing only in echoed source was admitted to the "
            "failure region, so a guard that never fired can be credited"
        )

    def test_a_real_failure_message_IS_evidence(self, harness) -> None:
        """The positive control. Without it, a region function returning the
        empty string would pass every rejection test above."""

        region = harness.failure_region(_REAL_FAILURE_OUTPUT)
        assert "the restricted project leaked into a packet" in region

    def test_an_all_green_run_produces_an_EMPTY_region(self, harness) -> None:
        """The empty-region control.

        A run in which nothing failed must attribute nothing to a failure. If
        this ever returns text, every mutation is credited by whatever that
        text happens to contain.
        """

        assert harness.failure_region("48 passed, 42 warnings in 1.25s\n") == ""

    def test_an_empty_region_proves_no_reason(self, harness) -> None:
        proven, why = harness._reason_is_proven("", "the restricted project leaked")
        assert not proven, why

    def test_an_empty_expected_token_credits_nothing(self, harness) -> None:
        region = harness.failure_region(_REAL_FAILURE_OUTPUT)
        proven, why = harness._reason_is_proven(region, "")
        assert not proven
        assert "empty" in why

    def test_skip_and_xfail_prose_is_not_evidence(self, harness) -> None:
        output = (
            "SKIPPED [1] tests/z.py:9: the restricted project leaked into a "
            "packet - needs a live store\n1 skipped in 0.10s\n"
        )
        assert "leaked into a packet" not in harness.failure_region(output)


class TestEveryMutationDeclaresACheckableReason:
    def test_the_table_is_not_empty(self, harness) -> None:
        assert harness.MUTATIONS, "the mutation table is empty"

    def test_there_is_deliberately_NO_citation_cap_mutation(self, harness) -> None:
        """Why the set is 15 and not 16, measured rather than asserted.

        My round-2 reports said the flood work would move this pin 15 → 16 by
        adding a distinct "cap disabled" mutation alongside the ordering one.
        It did not, and adversarial review caught the discrepancy. **The
        reports were wrong; the tree was right**, and the reason is worth
        recording so nobody adds the mutation later thinking it was an
        oversight.

        Disabling the citation cap does not need a CHAOS-3620 mutation
        because the cap is already guarded by something stronger: the frozen
        contract bounds ``RelatedEntity.supporting_path_ids`` at 10, so an
        uncapped emission fails validation outright. Measured by disabling
        ``if len(ordered) > _MAX_PATH_CITATIONS`` and running the suite —
        six CHAOS-3617 tests go red, including the packet-validator parity
        tests. A 3620 mutation would re-prove 3617's coverage.

        What genuinely needed a 3620 mutation is the ORDERING, because the
        cap is safe only if what it keeps is chosen well. That mutation
        exists and kills on the flood test.
        """

        cap_mutations = [
            mutation.mutation_id
            for mutation in harness.MUTATIONS
            if "_MAX_PATH_CITATIONS" in mutation.anchor
        ]
        assert not cap_mutations, (
            "a citation-cap mutation was added. If that is deliberate, "
            "update this test and the count claims; if it was added believing "
            f"the cap was unguarded, it is not: {cap_mutations}"
        )
        ordering = [
            mutation.mutation_id
            for mutation in harness.MUTATIONS
            if mutation.mutation_id == "path-citations-unordered"
        ]
        assert ordering, (
            "the ordering mutation is gone; the cap is now unguarded against "
            "keeping the wrong paths"
        )

    def test_the_mutation_SET_is_pinned_by_name(self, harness) -> None:
        """ "Not empty" is not a coverage claim.

        Adversarial review pointed out that the runner reports
        ``selected/selected``, so deleting fourteen mutations would still
        print ``GUARD PROOF PASSED: 1/1`` and every static check here would
        accept a one-entry table. The recorded result "15/15 killed" would
        then be true and meaningless.

        Pinned by exact id set rather than by count: a count pins how many,
        and the thing worth pinning is WHICH. Adding a mutation is a
        one-line, deliberate edit here; silently losing one is impossible.
        """

        assert {mutation.mutation_id for mutation in harness.MUTATIONS} == {
            "unauthorized-seed-investigated",
            "emitter-trusts-the-traversal",
            "candidate-withheld-after-ranking",
            "cohort-peer-authorization-dropped",
            "cohort-anchor-authorization-dropped",
            "injected-document-approved-for-extraction",
            "untrusted-record-vouches-for-an-attribution",
            "ended-relationship-still-drives",
            "driver-cites-evidence-the-packet-lacks",
            "reversed-hop-accepted-by-the-contract",
            "stale-index-reported-as-current",
            "truncation-without-a-reason-accepted",
            "path-citations-unordered",
            "shadow-accepts-non-canonical-evidence",
            "shadow-accepts-another-organizations-packet",
        }, (
            "the guard-injection mutation set changed. Update this pin "
            "deliberately and re-run the harness; a mutation that disappears "
            "silently takes its guard's proof with it"
        )

    def test_no_mutation_declares_an_empty_expected_failure(self, harness) -> None:
        for mutation in harness.MUTATIONS:
            assert mutation.expect_failure.strip(), mutation.mutation_id

    def test_NO_mutation_uses_the_bare_assert_token(self, harness) -> None:
        """This lane starts at zero and stays there.

        ``expect_failure="assert"`` is satisfied by any failing assertion, so
        such an entry proves the suite went red and nothing about why — a red
        check wearing a category check's clothes. The CHAOS-3617 harness
        carries a pinned, falling count of them because it inherited them;
        this one has never had any, and a pin that permitted some would be an
        invitation.
        """

        bare = [
            mutation.mutation_id
            for mutation in harness.MUTATIONS
            if mutation.expect_failure.strip().lower() in {"assert", "assertionerror"}
        ]
        assert not bare, (
            f"these mutations declare an undiscriminating token: {sorted(bare)}"
        )

    def test_no_expected_reason_hides_inside_its_own_node_id(self, harness) -> None:
        """``FAILED <nodeid> - <message>`` is inside the region, and has to be.

        The consequence is that a token which is a substring of the
        mutation's own test node id would be credited by the mere existence
        of a failure, whatever failed and for whatever reason.
        """

        offenders = [
            (mutation.mutation_id, mutation.expect_failure, node_id)
            for mutation in harness.MUTATIONS
            for node_id in mutation.tests
            if mutation.expect_failure in node_id
        ]
        assert not offenders, offenders
        # Anti-vacuity: the comparison really can fire.
        assert any(
            mutation.expect_failure in f"x{mutation.expect_failure}y"
            for mutation in harness.MUTATIONS
        )

    def test_mutation_ids_are_unique(self, harness) -> None:
        ids = [mutation.mutation_id for mutation in harness.MUTATIONS]
        assert len(set(ids)) == len(ids), sorted(ids)

    def test_every_mutation_states_the_defect_it_claims_to_prove(self, harness) -> None:
        """The recorded RED line is checked against this by a reader.

        A one-word defect makes that check impossible, which is how a
        mutation that died in unrelated setup passes for a kill.
        """

        for mutation in harness.MUTATIONS:
            assert len(mutation.defect.split()) >= 8, (
                f"{mutation.mutation_id} states its defect in "
                f"{len(mutation.defect.split())} words; too few to check a "
                "failure line against"
            )


class TestNoMutationCanSilentlyStopApplying:
    """An anchor that stopped matching is the harness's worst failure mode.

    The script raises INVALID at run time, but the run is deliberate rather
    than gated (see the module docstring), so a refactor could leave every
    anchor stale for weeks with nothing red. These checks make the drift
    visible in the standing suite instead.
    """

    def test_every_target_file_exists(self, harness) -> None:
        for mutation in harness.MUTATIONS:
            assert mutation.path.is_file(), (
                f"{mutation.mutation_id} targets {mutation.path}, which does not exist"
            )

    def test_every_anchor_appears_exactly_once_in_its_file(self, harness) -> None:
        offenders = []
        for mutation in harness.MUTATIONS:
            count = mutation.path.read_text(encoding="utf-8").count(mutation.anchor)
            if count != 1:
                offenders.append((mutation.mutation_id, count))
        assert not offenders, (
            "these anchors no longer match exactly one place in their file, "
            f"so the mutation would be INVALID or would disable the wrong "
            f"line: {offenders}"
        )

    def test_no_two_mutations_share_an_anchor(self, harness) -> None:
        """Two guards disabled by one substitution are one guard.

        Cohort construction authorizes at two sites and an early draft
        mutated only one of them while claiming both — the peer check kept
        filtering and the mutation SURVIVED. Distinct anchors are what make
        "this guard alone" true.
        """

        seen: dict[tuple[str, str], str] = {}
        for mutation in harness.MUTATIONS:
            key = (str(mutation.path), mutation.anchor)
            assert key not in seen, (
                f"{mutation.mutation_id} and {seen[key]} disable the same "
                "line, so neither proves its own guard"
            )
            seen[key] = mutation.mutation_id

    def test_the_replacement_actually_changes_the_source(self, harness) -> None:
        for mutation in harness.MUTATIONS:
            assert mutation.replacement != mutation.anchor, (
                f"{mutation.mutation_id} substitutes the anchor for itself"
            )

    def test_every_named_test_still_exists(self, harness) -> None:
        """A node id that no longer resolves runs zero tests and exits green.

        Resolved by import and attribute lookup rather than by a pytest
        collection subprocess: same answer, and it cannot be confused by an
        unrelated collection error elsewhere in the suite.
        """

        missing = []
        for mutation in harness.MUTATIONS:
            for node_id in mutation.tests:
                path, _, selector = node_id.partition("::")
                module_path = _REPO_ROOT / path
                if not module_path.is_file():
                    missing.append((mutation.mutation_id, node_id, "no such file"))
                    continue
                module = _load(
                    module_path, f"_guard_target_{module_path.stem}_{len(missing)}"
                )
                target = module
                for part in selector.split("::"):
                    if not hasattr(target, part):
                        missing.append((mutation.mutation_id, node_id, part))
                        break
                    target = getattr(target, part)
        assert not missing, (
            f"these mutations name tests that no longer exist, so they would "
            f"run nothing and report a kill: {missing}"
        )

    def test_every_mutation_names_at_least_one_test(self, harness) -> None:
        for mutation in harness.MUTATIONS:
            assert mutation.tests, (
                f"{mutation.mutation_id} names no tests; a mutation with no "
                "tests always passes"
            )
