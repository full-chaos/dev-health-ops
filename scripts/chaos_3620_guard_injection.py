#!/usr/bin/env python3
"""CHAOS-3620: prove each safety guard the proof suite relies on is load-bearing.

The CHAOS-3620 suites make a lot of negative claims — the restricted project
never appears, the false dependency is never asserted, the injected document
is never approved. A negative claim is satisfied by a system that does
nothing, so each one has to be paid for: disable exactly one guard, run only
the tests that claim to cover it, and require them to fail **for the reason
the guard exists**.

Three rules, inherited from ``chaos_3617_guard_injection.py`` because they
were learned the hard way and a harness that breaks any of them reports
coverage it does not have:

1. **A mutation that does not apply is INVALID, not KILLED.** An anchor that
   stopped matching after a refactor would otherwise become a permanently
   green "the guard is proven" line.
2. **The restore is verified by re-running the tests, and the file bytes are
   compared.** A disabled guard still compiles and ``git diff`` calls a
   restored file clean whatever it contains — including an untracked one.
3. **Where the mutation died is recorded**, and checked against what the
   guard claims, so a collapse in unrelated setup cannot pass for a kill.

**There is exactly one failure-region checker in this repository.** This
script imports ``failure_region`` and ``_reason_is_proven`` from the
CHAOS-3617 harness rather than copying them. A copy would be a checker
nothing checks, and the copy is precisely what would drift: the region rule
already had two holes found by adversarial review, and both fixes landed in
one place. ``test_chaos_3620_guard_harness.py`` asserts the shared identity
and re-runs the empty-region control against the object this script actually
uses.

**No mutation may declare ``expect_failure="assert"``.** A bare token is
satisfied by any failing assertion and proves the suite went red, not why.
The 3617 harness carries a pinned, falling count of such entries; this one
starts at zero and a test keeps it there.

Usage::

    uv run python scripts/chaos_3620_guard_injection.py            # all
    uv run python scripts/chaos_3620_guard_injection.py --only ID  # one
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib.util
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ARM = REPO_ROOT / "src" / "dev_health_ops" / "context_fabric" / "graph_arm"
DEV = REPO_ROOT / "src" / "dev_health_ops" / "api" / "dev"
TESTS = "tests/context_fabric"

_AUTHZ = f"{TESTS}/test_chaos_3620_authorization.py"
_ADVERSARIAL = f"{TESTS}/test_chaos_3620_adversarial.py"
_PROVENANCE = f"{TESTS}/test_chaos_3620_provenance.py"
_SEMANTIC = f"{TESTS}/test_chaos_3620_semantic_safety.py"


def _shared_checker():
    """Load the CHAOS-3617 harness for its region checker.

    By path because it is a script, not a package member — the same
    mechanism its own tests use. Imported rather than reimplemented so the
    rule that decides what counts as evidence exists once.
    """

    script = REPO_ROOT / "scripts" / "chaos_3617_guard_injection.py"
    spec = importlib.util.spec_from_file_location("chaos_3617_guard_injection", script)
    if spec is None or spec.loader is None:  # pragma: no cover - import plumbing
        raise RuntimeError(f"cannot load the shared region checker from {script}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_CHECKER = _shared_checker()
failure_region = _CHECKER.failure_region
_reason_is_proven = _CHECKER._reason_is_proven


@dataclasses.dataclass(frozen=True)
class Mutation:
    """One guard, disabled by an exact substitution."""

    mutation_id: str
    #: What the guard prevents, in the words of CHAOS-3620. The recorded RED
    #: line is checked against this by a reader, so it has to be specific
    #: enough to check against.
    defect: str
    path: Path
    anchor: str
    replacement: str
    tests: tuple[str, ...]
    #: The failure the disabled guard must produce, as a substring of the
    #: pytest FAILURE REGION — ``E ``-prefixed lines and the anchored
    #: ``FAILED <nodeid>`` summary, nothing else. Required, and never the
    #: bare word ``assert``.
    expect_failure: str


MUTATIONS: tuple[Mutation, ...] = (
    Mutation(
        mutation_id="unauthorized-seed-investigated",
        defect=(
            "an investigation seeded at an entity the caller may not see is "
            "carried out, confirming the entity exists to anyone who can time "
            "the response"
        ),
        path=ARM / "readback.py",
        anchor=(
            "        seed for seed in seed_canonical_ids "
            "if seed in known and seed in authorized"
        ),
        replacement="        seed for seed in seed_canonical_ids if seed in known",
        tests=(
            f"{_AUTHZ}::TestTheRestrictedProjectNeverReachesAConsumer::"
            "test_seeding_the_investigation_AT_the_restricted_project_returns_nothing",
        ),
        expect_failure="an unauthorized seed produced a neighbourhood",
    ),
    Mutation(
        mutation_id="emitter-trusts-the-traversal",
        defect=(
            "the emitter accepts a readout whose paths escape the grant it is "
            "about to declare, which is the shape a re-used or stale readout "
            "produces. NOTE what the recorded RED line shows: with this guard "
            "disabled the packet still does not escape -- the frozen "
            "contract's validate_paths_stay_inside_authorized_set refuses it "
            "instead. So this mutation proves the emitter check is the layer "
            "that raises EARLY and names the entity, and simultaneously "
            "proves the contract is a real second enforcer rather than an "
            "assumed one"
        ),
        path=ARM / "packet_builder.py",
        anchor="                if endpoint not in authorized_entity_ids:",
        replacement="                if False:",
        tests=(
            f"{_AUTHZ}::TestPathsNeverMixAuthorizedAndUnauthorizedEntities::"
            "test_the_builder_refuses_it_before_the_contract_ever_sees_it",
        ),
        expect_failure="1 validation error for RelatedContext",
    ),
    Mutation(
        mutation_id="candidate-withheld-after-ranking",
        defect=(
            "a restricted entity is matched and then dropped, so it still "
            "occupies a rank, a clarification slot or a count that discloses it"
        ),
        path=ARM / "discovery.py",
        anchor="        if node.canonical_id not in authorized:",
        replacement="        if False:",
        tests=(
            f"{_AUTHZ}::TestCandidateSearchWithholdsBeforeRanking::"
            "test_the_analyst_cannot_find_it_by_label_id_or_partial_name",
        ),
        expect_failure="returned the restricted project",
    ),
    # Cohort construction authorizes at TWO sites and disabling either alone
    # leaves the other standing, so each gets its own mutation. The first
    # draft mutated only the anchor site, aimed it at the membership test,
    # and SURVIVED -- the peer site was still filtering. That miss is why
    # both are here and why the tests they name are different.
    Mutation(
        mutation_id="cohort-peer-authorization-dropped",
        defect=(
            "a peer the caller cannot see enters the comparison cohort as a "
            "member, or is named in an exclusion, disclosing it through its "
            "own inclusion rationale"
        ),
        path=ARM / "cohort.py",
        anchor="            if peer not in authorized:",
        replacement="            if False:",
        tests=(
            f"{_AUTHZ}::TestCohortConstructionWithholdsAndCounts::"
            "test_an_unauthorized_peer_is_withheld_and_counted",
            f"{_AUTHZ}::TestCohortConstructionWithholdsAndCounts::"
            "test_an_unauthorized_peer_reaches_the_EXCLUSION_list_too",
        ),
        expect_failure="was proposed as a cohort member",
    ),
    Mutation(
        mutation_id="cohort-anchor-authorization-dropped",
        defect=(
            "a peer reached only through an anchor the caller cannot see is "
            "admitted, and the withheld count silently goes to zero so the "
            "narrowing is never disclosed"
        ),
        path=ARM / "cohort.py",
        anchor="            if far not in authorized:",
        replacement="            if False:",
        tests=(
            f"{_AUTHZ}::TestCohortConstructionWithholdsAndCounts::"
            "test_an_unauthorized_ANCHOR_is_withheld_and_counted_separately",
        ),
        expect_failure="ANCHOR was skipped without being counted",
    ),
    Mutation(
        mutation_id="injected-document-approved-for-extraction",
        defect=(
            "a document carrying a prompt injection is approved for "
            "extraction, which is what points a model at it"
        ),
        path=ARM / "corpus_adapter.py",
        anchor="    if document.contains_injection:",
        replacement="    if False:",
        tests=(
            f"{_ADVERSARIAL}::TestPromptInjectionNeverReachesAConsumer::"
            "test_an_injected_document_is_refused_even_when_its_trust_says_otherwise",
        ),
        expect_failure="carrying an injection was",
    ),
    Mutation(
        mutation_id="untrusted-record-vouches-for-an-attribution",
        defect=(
            "an untrusted source note is accepted as support, which is exactly "
            "how the corpus's planted false dependency reached PRINCIPAL "
            "DRIVER once before"
        ),
        path=ARM / "drivers.py",
        anchor="    return trust is not None and trust in TRUSTED_ATTRIBUTION_LEVELS",
        replacement="    return True",
        tests=(
            f"{_ADVERSARIAL}::TestAPoisonedLinkageIsPresentAndRefused::"
            "test_the_same_probe_with_an_untrusted_voucher_is_refused",
        ),
        expect_failure="an untrusted voucher was enough to assert a blockage",
    ),
    Mutation(
        mutation_id="ended-relationship-still-drives",
        defect=(
            "a relationship that closed before the trial instant is asserted "
            "as a current driver"
        ),
        path=ARM / "drivers.py",
        anchor="    if not _currency(paths, context.as_of):",
        replacement="    if False:",
        tests=(
            f"{_PROVENANCE}::TestHistoricalRelationshipsAreEmittedAsCurrent::"
            "test_the_driver_layer_correctly_refuses_to_assert_on_it",
        ),
        expect_failure="a driver was asserted on a relationship that ended",
    ),
    # DELIBERATELY ABSENT: the measurement-only *category* guard
    # (``drivers.py:544-560``). It was written as a mutation, run, and
    # SURVIVED — and the reason is structural, not a missing test: no
    # structural rule in this revision produces a measurement-only category,
    # so the guard's condition can never be true. Deleting the guard would be
    # wrong (it is the right guard for the next rule) and leaving a surviving
    # mutation here would be a permanent red that teaches nothing.
    # ``test_the_measurement_only_CATEGORY_guard_is_currently_unreachable``
    # in the provenance suite holds the finding and goes red the moment a
    # structural rule makes it reachable, which is when this mutation becomes
    # worth writing.
    Mutation(
        mutation_id="driver-cites-evidence-the-packet-lacks",
        defect=(
            "a driver citing evidence this packet never indexed is emitted "
            "with less support than it was built from, and the packet still "
            "validates"
        ),
        path=ARM / "packet_builder.py",
        # The disabled guard must still PRODUCE a packet, not die on the
        # missing key two lines down. A replacement that only skipped the
        # raise would fail with KeyError -- red for a reason unrelated to
        # closure, which is the miss ``expect_failure`` exists to catch.
        anchor=(
            "        missing = sorted(set(ids) - set(handle_by_observation))\n"
            "        if missing:"
        ),
        replacement=(
            "        missing = sorted(set(ids) - set(handle_by_observation))\n"
            "        ids = [item for item in ids if item in handle_by_observation]\n"
            "        if False:"
        ),
        tests=(
            f"{_PROVENANCE}::TestEveryAssertedDriverClosesToEvidenceInThisPacket::"
            "test_the_closure_check_REJECTS_a_driver_citing_unindexed_evidence",
        ),
        expect_failure="DID NOT RAISE <class 'ValueError'>",
    ),
    Mutation(
        mutation_id="reversed-hop-accepted-by-the-contract",
        defect=(
            "a lineage hop pointing against its canonical orientation is "
            "accepted, so causality is inverted and reads plausibly"
        ),
        path=DEV / "investigation_contract" / "packet.py",
        anchor="        if not orientation.permits(source_kind, target_kind):",
        replacement="        if False:",
        tests=(
            f"{_PROVENANCE}::TestCanonicalIdsAndDirectionSurviveEmission::"
            "test_the_contract_REFUSES_a_reversed_hop",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="stale-index-reported-as-current",
        defect=(
            "an index far behind the question's window is reported as current, "
            "so a consumer reads months-old structure as today's"
        ),
        path=ARM / "watermark.py",
        anchor="        if self.indexed_through + tolerance < window_end:",
        replacement="        if False:",
        tests=(
            f"{_ADVERSARIAL}::TestAStaleIndexIsDisclosedEverywhere::"
            "test_every_lineage_path_reports_the_stale_source_state",
        ),
        expect_failure="reports path health",
    ),
    Mutation(
        mutation_id="truncation-without-a-reason-accepted",
        defect=(
            "a partial result is emitted with no reason, which is "
            "indistinguishable from a complete one"
        ),
        path=ARM / "budgets.py",
        anchor="        if not self.within_budget and self.truncation_reason is None:",
        replacement="        if False:",
        tests=(
            f"{_ADVERSARIAL}::TestTruncationIsDisclosedNotSilent::"
            "test_a_truncation_flag_without_a_reason_is_refused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="path-citations-unordered",
        defect=(
            "the per-entity citation cap keeps whichever paths were "
            "enumerated first, so a flood of long low-quality lineage "
            "displaces the short explanatory one"
        ),
        path=ARM / "packet_builder.py",
        anchor="            set(touched[canonical_id]), key=lambda pid: (path_length[pid], pid)",
        replacement="            set(touched[canonical_id]), key=lambda pid: (-path_length[pid], pid)",
        tests=(
            f"{_ADVERSARIAL}::TestTruncationIsDisclosedNotSilent::"
            "test_a_flood_of_low_quality_paths_cannot_displace_the_required_one",
            f"{_ADVERSARIAL}::TestTruncationIsDisclosedNotSilent::"
            "test_shorter_lineage_is_cited_before_longer_lineage",
        ),
        expect_failure="was displaced from the citation set",
    ),
    Mutation(
        mutation_id="shadow-accepts-non-canonical-evidence",
        defect=(
            "an arm introduces evidence of its own and the shadow seam records "
            "it as if a canonical service had minted it"
        ),
        path=DEV / "investigation_shadow.py",
        anchor="            if offenders:",
        replacement="            if False:",
        tests=(
            f"{_SEMANTIC}::TestTheTelemetryIsContentSafe::"
            "test_a_packet_whose_evidence_did_not_come_from_a_canonical_service_is_refused",
        ),
        expect_failure="no canonical service minted was accepted",
    ),
    Mutation(
        mutation_id="shadow-accepts-another-organizations-packet",
        defect=(
            "a packet claiming one organization is recorded against another "
            "organization's run, comparing a tenant against another tenant's "
            "canonical records"
        ),
        path=DEV / "investigation_shadow.py",
        anchor="            if packet.organization_id != organization_id:",
        replacement="            if False:",
        tests=(
            f"{_SEMANTIC}::TestTheTelemetryIsContentSafe::"
            "test_a_packet_claiming_another_organization_is_refused",
        ),
        expect_failure="was recorded against a Lumen run",
    ),
)


def _run_tests(node_ids: tuple[str, ...]) -> tuple[int, str]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            *node_ids,
            "-q",
            "--no-header",
            "-p",
            "no:randomly",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout + completed.stderr


def _summary_line(output: str) -> str:
    for line in reversed(output.splitlines()):
        if " passed" in line or " failed" in line or " error" in line:
            return line.strip()
    return "(no pytest summary line)"


def _first_failure(output: str) -> str:
    region = failure_region(output)
    for line in region.splitlines():
        if line.startswith("E "):
            return line.strip()
    return region.splitlines()[0].strip() if region.strip() else "(no failure region)"


def _apply(mutation: Mutation) -> str:
    """Substitute the anchor, returning the original text.

    An anchor that is absent, or present more than once, aborts the run.
    Absent means the guard moved and the mutation proves nothing; ambiguous
    means the substitution would disable something other than the guard
    named, and a harness that disabled the wrong line would report a kill
    for a guard it never touched.
    """

    original = mutation.path.read_text(encoding="utf-8")
    occurrences = original.count(mutation.anchor)
    if occurrences == 0:
        raise SystemExit(
            f"INVALID {mutation.mutation_id}: anchor not found verbatim in "
            f"{mutation.path.relative_to(REPO_ROOT)}. The guard moved or was "
            "rewritten; this mutation proves nothing until the anchor is "
            "re-derived from the current source"
        )
    if occurrences > 1:
        raise SystemExit(
            f"INVALID {mutation.mutation_id}: anchor appears {occurrences} "
            f"times in {mutation.path.relative_to(REPO_ROOT)}; the "
            "substitution would disable more than the guard named"
        )
    mutation.path.write_text(
        original.replace(mutation.anchor, mutation.replacement), encoding="utf-8"
    )
    return original


def _restore(mutation: Mutation, original: str) -> None:
    """Put the bytes back, and verify they are the bytes that were there.

    Byte comparison rather than a ``git`` check: ``git diff`` calls an
    untracked file clean whatever it contains, and a partially restored file
    still compiles.
    """

    mutation.path.write_text(original, encoding="utf-8")
    restored = mutation.path.read_text(encoding="utf-8")
    if restored != original:
        raise SystemExit(
            f"RESTORE FAILED for {mutation.mutation_id}: "
            f"{mutation.path.relative_to(REPO_ROOT)} does not match the bytes "
            "read before the mutation. Do not commit this tree"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", default="", help="run a single mutation id")
    arguments = parser.parse_args()

    selected = [
        mutation
        for mutation in MUTATIONS
        if not arguments.only or mutation.mutation_id == arguments.only
    ]
    if not selected:
        raise SystemExit(f"no mutation named {arguments.only!r}")

    print(f"CHAOS-3620 guard injection: {len(selected)} mutation(s)\n")
    survived: list[str] = []
    wrong_reason: list[tuple[str, str]] = []

    for mutation in selected:
        print(f"-- {mutation.mutation_id}")
        print(f"   defect: {mutation.defect}")

        baseline_code, baseline_output = _run_tests(mutation.tests)
        if baseline_code != 0:
            raise SystemExit(
                f"BASELINE RED for {mutation.mutation_id}: the tests fail "
                f"before any mutation is applied. {_summary_line(baseline_output)}"
            )

        original = _apply(mutation)
        try:
            mutated_code, mutated_output = _run_tests(mutation.tests)
        finally:
            _restore(mutation, original)

        if mutated_code == 0:
            survived.append(mutation.mutation_id)
            print("   SURVIVED -- the tests pass with the guard disabled\n")
            continue

        region = failure_region(mutated_output)
        proven, why = _reason_is_proven(region, mutation.expect_failure)
        print(f"   RED at: {_first_failure(mutated_output)}")
        if not proven:
            wrong_reason.append((mutation.mutation_id, why))
            print(f"   WRONG REASON -- {why}\n")
            continue
        print("   KILLED\n")

        restored_code, restored_output = _run_tests(mutation.tests)
        if restored_code != 0:
            raise SystemExit(
                f"RESTORE UNVERIFIED for {mutation.mutation_id}: the tests do "
                f"not pass again. {_summary_line(restored_output)}"
            )

    if survived or wrong_reason:
        print("GUARD INJECTION FAILED")
        for mutation_id in survived:
            print(f"  SURVIVED: {mutation_id}")
        for mutation_id, why in wrong_reason:
            print(f"  WRONG REASON: {mutation_id} -- {why}")
        return 1

    print(
        f"GUARD PROOF PASSED: {len(selected)}/{len(selected)} guards observed failing"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
