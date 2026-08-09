#!/usr/bin/env python3
"""CHAOS-3617: prove each guard's test actually catches the defect it names.

RED-first evidence, the CHAOS-3615 house way: for every guard the arm relies
on, disable *that guard alone* by an exact source substitution, run the tests
that claim to cover it, and require them to FAIL. Then restore and require
them to PASS again.

Three rules this harness follows, learned the hard way and worth stating
because a mutation harness that breaks any of them reports coverage it does
not have:

1. **A mutation that does not apply is INVALID, not KILLED.** If the anchor
   text is not found verbatim, the run aborts naming the mutation — an
   anchor that silently stopped matching after a refactor would otherwise
   turn into a permanently green "the guard is proven" line.
2. **The restore is verified by re-running the tests, never by a git check.**
   A disabled guard still compiles and ``git diff`` calls a restored file
   clean whatever it contains. Only green tests prove the guard is back.
3. **Where the mutation died is recorded**, so a reader can check that the
   failure is the one the guard exists to prevent rather than an unrelated
   collapse somewhere in setup.

Usage::

    uv run python scripts/chaos_3617_guard_injection.py            # all
    uv run python scripts/chaos_3617_guard_injection.py --only ID  # one
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src" / "dev_health_ops" / "context_fabric" / "graph_arm"
TESTS = "tests/context_fabric"


@dataclasses.dataclass(frozen=True)
class Mutation:
    """One guard, disabled by an exact substitution."""

    mutation_id: str
    #: What the guard prevents, in the words of the issue or the fault-mode
    #: registry. This is what the recorded RED line has to be checked against.
    defect: str
    path: Path
    anchor: str
    replacement: str
    tests: tuple[str, ...]
    #: The failure the disabled guard must produce, as a substring of pytest's
    #: FAILED/error line. Required, and it is the whole point: a mutation that
    #: merely makes the suite red has proved nothing about the guard it names.
    #:
    #: Adversarial review caught exactly that here. Disabling the prose-fact
    #: guard with ``if False and match is None`` let ``None`` reach
    #: ``match.group(...)``, so the test failed with ``AttributeError`` from a
    #: downstream dereference rather than because prose was accepted -- and
    #: the harness reported KILLED. Recording *where* a mutation died was
    #: never enough; the category has to be checked against the claim.
    expect_failure: str


MUTATIONS: tuple[Mutation, ...] = (
    Mutation(
        mutation_id="reversed-relationship-accepted",
        defect=(
            "a reversed relationship record is stored, so lineage reads "
            "plausibly and points the wrong way"
        ),
        path=SRC / "projection.py",
        anchor="    if not orientation.permits(source_kind, target_kind):",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestDirectionSurvivesTheRoundTrip::"
            "test_a_reversed_relationship_record_is_refused_at_ingestion",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="cross-tenant-records-accepted",
        defect=(
            "a record belonging to another organization is written into this "
            "organization's partition"
        ),
        path=SRC / "records.py",
        anchor="    if foreign:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestIngestionRefusals::test_a_foreign_record_never_reaches_the_store",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="org-dropped-from-node-identity",
        defect=(
            "two organizations holding the same canonical id collide onto one "
            "node, sharing data no downstream filter could unshare"
        ),
        path=SRC / "identity.py",
        anchor='    name = "\\0".join((org_id, discriminator, kind, canonical_id))',
        replacement='    name = "\\0".join((discriminator, kind, canonical_id))',
        tests=(f"{TESTS}/test_chaos_3617_identity.py::TestTenantScopedAddressing",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="unauthorized-entity-traversed",
        defect=(
            "a path routes through a restricted entity, disclosing that it "
            "exists and links two things the caller can see"
        ),
        path=SRC / "readback.py",
        anchor="            if other not in authorized:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_authorization.py::TestAuthorizationFiltering",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="partition-trusted-not-rederived",
        defect=(
            "a caller-supplied graph partition is accepted as an authorization claim"
        ),
        path=SRC / "identity.py",
        anchor="    if partition != expected:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_identity.py::TestServerDerivedPartition::"
            "test_a_partition_from_another_org_is_rejected",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="prose-allowed-in-structured-attributes",
        defect=(
            "a structured record is converted into hand-authored prose via an "
            "attribute value"
        ),
        path=SRC / "projection.py",
        anchor="        if isinstance(value, str) and len(value) > MAX_ATTRIBUTE_CHARS:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::"
            "test_an_attribute_cannot_hold_a_sentence",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="prose-fact-accepted-on-read",
        defect="a stored prose fact is presented to a consumer as evidence",
        path=SRC / "backend.py",
        # The disabled guard must PARSE the prose, not crash on it. The
        # earlier version of this mutation only skipped the raise, so `None`
        # reached `match.group(...)` and the test failed with AttributeError
        # -- red for a reason that had nothing to do with prose being
        # accepted. Adversarial review caught it; `expect_failure` below now
        # makes that class of miss impossible.
        anchor=(
            "    match = TRIPLE_FACT_PATTERN.fullmatch(fact)\n    if match is None:"
        ),
        replacement=(
            "    match = TRIPLE_FACT_PATTERN.fullmatch(fact)\n"
            "    if match is None:\n"
            "        parts = fact.split()\n"
            "        return (parts[0], RelationshipType.REFERENCES, parts[-1])\n"
            "    if False:"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::test_a_prose_fact_is_rejected_on_read",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="unapproved-document-reaches-extraction",
        defect=("model extraction is pointed at unstructured material nobody approved"),
        path=SRC / "projection.py",
        anchor="    approved = tuple(document for document in batch.documents if document.approved)",
        replacement="    approved = tuple(batch.documents)",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::"
            "test_unapproved_documents_never_reach_extraction",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="projection-flag-ignored",
        defect="the shadow arm writes to the trial store with its flag off",
        path=SRC / "store.py",
        anchor="        if not graph_projection_enabled():",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestIndexingFailureAndFallback::"
            "test_a_write_with_the_projection_flag_off_is_refused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="never-projected-store-reports-fresh",
        defect=(
            "an empty store answers as though it were current, so a packet "
            "built on nothing looks complete"
        ),
        path=SRC / "watermark.py",
        anchor="        if self.indexed_through is None:\n            return SourceRequirementState.UNAVAILABLE",
        replacement="        if self.indexed_through is None:\n            return SourceRequirementState.AVAILABLE_CURRENT",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestWatermark::"
            "test_a_never_projected_store_is_unavailable_not_fresh",
            f"{TESTS}/test_chaos_3617_packet_contract.py::"
            "TestSourceHealthAndFreshness::"
            "test_a_never_projected_store_reports_unavailable_not_empty",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="traversal-order-dependent",
        defect=(
            "which explanatory paths survive the per-entity cap depends on "
            "edge arrival order, so a recorded trial run is irreproducible "
            "(the defect the live differential oracle actually found)"
        ),
        path=SRC / "readback.py",
        anchor=(
            "    return tuple(\n"
            "        sorted(\n"
            "            adjacency.edges.get(canonical_id, ()),\n"
            "            key=lambda edge: (edge[0].value, edge[1], edge[2].value),\n"
            "        )\n"
            "    )"
        ),
        replacement="    return tuple(adjacency.edges.get(canonical_id, ()))",
        tests=(f"{TESTS}/test_chaos_3617_determinism.py::TestOrderIndependence",),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="builder-trusts-the-readouts-authorization-claim",
        defect=(
            "the arm stops refusing an unauthorized hop endpoint itself and "
            "leaves it to the frozen contract. NOTE the honest scope, found "
            "by the failure-category check: with this guard disabled the "
            "packet is still refused -- by RelatedContext's own "
            "validate_paths_stay_inside_authorized_set. So this mutation "
            "proves the arm refuses EARLIER and with a typed PermissionError "
            "naming the endpoint, not that the endpoint would otherwise "
            "reach a consumer. Two independent refusals is the actual state, "
            "and claiming more would be the over-claim this harness exists "
            "to catch"
        ),
        path=SRC / "packet_builder.py",
        anchor="                if endpoint not in authorized_entity_ids:",
        replacement="                if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_authorization.py::TestAuthorizationFiltering::"
            "test_the_builder_refuses_a_readout_whose_paths_escape_the_set",
        ),
        expect_failure="ValidationError",
    ),
    Mutation(
        mutation_id="graphiti-telemetry-left-on",
        defect=(
            "the trial phones one organization's structure home to a "
            "third-party analytics host"
        ),
        path=SRC / "backend.py",
        anchor='    os.environ[TELEMETRY_ENV_VAR] = "false"',
        replacement="    pass",
        tests=(
            f"{TESTS}/test_chaos_3617_containment.py::TestTelemetryContainment::"
            "test_telemetry_is_forced_off",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="traversal-work-is-unbounded",
        defect=(
            "path enumeration expands without limit in a dense neighbourhood, "
            "reaching no new entity while doing unbounded work"
        ),
        path=SRC / "readback.py",
        anchor="        if not work_outcome.within_budget:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_node_visit_budget_bounds_work_not_only_results",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="traversal-has-no-wall-clock-backstop",
        defect=(
            "a traversal shape a work count cannot predict runs past its time "
            "budget with nothing to stop it"
        ),
        path=SRC / "readback.py",
        anchor="        if not elapsed_outcome.within_budget:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_wall_clock_budget_bounds_the_traversal",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="packet-byte-budget-not-applied",
        defect="a packet larger than any declared bound reaches a consumer",
        path=SRC / "packet_builder.py",
        anchor="    if not outcome.within_budget:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::"
            "TestPacketByteBudget::"
            "test_a_packet_over_the_byte_budget_is_refused_not_trimmed",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="preview-creates-the-keyspace-it-previewed",
        defect=(
            "a dry-run org deletion constructs a store and thereby creates an "
            "empty keyspace for every organization it previewed"
        ),
        path=SRC / "store.py",
        anchor="    if not exists:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_live_store.py::TestDeterministicCleanup::"
            "test_previewing_an_absent_organization_constructs_no_store",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="separator-bytes-accepted-into-joined-attributes",
        defect=(
            "a source value containing the storage join byte is stored and "
            "comes back split into several, inventing an alias no source "
            "supplied -- which a later alias search would then match"
        ),
        path=SRC / "projection.py",
        anchor="        if separator in value:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestSeparatorBytesAreRefused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="default-read-path-truncates-silently",
        defect=(
            "the walk stops at the hop ceiling with edges still unexplored "
            "and reports complete -- reachable authorized entities missing, "
            "every flag False, on the path nobody configures"
        ),
        path=SRC / "readback.py",
        anchor="    if declined_with_edges_remaining:",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_default_read_path_discloses_when_it_stops_early",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="one-shared-truncation-reason-misattributes",
        defect=(
            "path truncation and evidence truncation collapse onto one "
            "reason, so a consumer asking why lineage is partial is told "
            "evidence_budget"
        ),
        path=SRC / "readback.py",
        anchor="        evidence_reason = evidence_outcome.truncation_reason",
        replacement="        paths_reason = evidence_outcome.truncation_reason\n        evidence_reason = evidence_outcome.truncation_reason",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_the_evidence_budget_sets_the_evidence_flag_not_the_path_flag",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="packet-sections-share-one-truncation-reason",
        defect=(
            "the packet's evidence section reports the path bound's reason "
            "when both fire -- the defect the readout-only fix MOVED rather "
            "than killed, because both sections kept reading the first-wins "
            "convenience property"
        ),
        path=SRC / "packet_builder.py",
        anchor="        truncation_reason=readout.evidence_truncation_reason,",
        replacement="        truncation_reason=readout.truncation_reason,",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_each_flag_carries_its_own_reason_when_two_bounds_fire",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="control-characters-accepted",
        defect=(
            "a NUL is silently dropped by the live store (so the readers "
            "disagree about what the source supplied) and defeats the "
            "NUL-separated hash inputs identity.py relies on to keep two "
            "relationships from sharing one address"
        ),
        path=SRC / "projection.py",
        anchor="    found = sorted(_CONTROL_CHARACTERS & set(value))",
        replacement="    found = []",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestSeparatorBytesAreRefused::test_a_control_character_is_refused",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="entity-flag-over-reports-on-a-diamond",
        defect=(
            "the entity-truncation flag fires whenever an edge was left "
            "unfollowed, even when every entity was returned by another "
            "branch -- over-reporting reads as noise as fast as silence"
        ),
        path=SRC / "readback.py",
        anchor="                if any(other not in reached for other in unfollowed):",
        replacement="                if True:",
        tests=(
            f"{TESTS}/test_chaos_3617_operational_controls.py::TestBudgets::"
            "test_a_diamond_does_not_claim_missing_entities",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="semantic-claim-allowed-under-a-hash-embedder",
        defect=(
            "a match produced by similarity over non-semantic hash vectors is "
            "emitted as a real subject match, and scores as a retrieval "
            "capability the arm does not have"
        ),
        path=SRC / "packet_builder.py",
        anchor="    if embedder.semantic:\n        return",
        replacement="    if True:\n        return",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::"
            "TestSemanticClaimsAreRefusedUnderANonSemanticEmbedder",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="embedding-budget-not-checked-before-spending",
        defect=(
            "a projection spends unbounded embedding calls, and an "
            "over-budget run pays for most of them before stopping"
        ),
        path=SRC / "store.py",
        anchor="            if not outcome.within_budget:",
        replacement="            if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::TestEmbeddingBudget::"
            "test_a_projection_over_the_embedding_budget_is_refused_before_any_call",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="projection-version-omits-the-embedder",
        defect=(
            "two runs embedded with different models report the same "
            "projection version, making incomparable results look comparable"
        ),
        path=SRC / "packet_builder.py",
        anchor=(
            "        projection_version=(\n"
            "            f\"{PROJECTION_VERSION.removesuffix('.v1')}.\"\n"
            '            f"{embedder_projection_suffix(active_embedder)}.v1"\n'
            "        ),"
        ),
        replacement="        projection_version=PROJECTION_VERSION,",
        tests=(
            f"{TESTS}/test_chaos_3617_packet_contract.py::"
            "TestReproducibilityMetadata::"
            "test_the_projection_version_names_the_embedder_that_produced_it",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="cloud-embedder-degrades-silently-without-a-key",
        defect=(
            "a run with no API key silently uses hash vectors while every "
            "artifact says it was semantic"
        ),
        path=SRC / "backend.py",
        anchor="        if not key:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_semantic_claims.py::TestEmbedderContracts::"
            "test_the_cloud_embedder_refuses_to_degrade_silently",
        ),
        expect_failure="DID NOT RAISE",
    ),
    Mutation(
        mutation_id="candidate-search-ranks-a-withheld-entity",
        defect=(
            "a restricted entity that matches the query is filtered AFTER "
            "ranking, so its position leaks even though its record is never "
            "returned -- and the corpus's restricted project is same-tenant, "
            "so no tenant check catches it"
        ),
        path=SRC / "discovery.py",
        anchor="        if node.canonical_id not in authorized:",
        replacement="        if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_candidate_search.py::"
            "TestAuthorizationBoundsTheSearch",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="candidate-ranking-depends-on-node-order",
        defect=(
            "the ranking falls back on iteration order, so the same world and "
            "the same query rank differently between runs and a recorded "
            "trial result cannot be reproduced"
        ),
        path=SRC / "discovery.py",
        anchor="    ranked = sorted(matches.values(), key=lambda item: item.rank_key)",
        replacement="    ranked = list(matches.values())",
        tests=(
            f"{TESTS}/test_chaos_3617_candidate_search.py::"
            "TestRankingIsTotalAndContentDerived::"
            "test_shuffling_the_node_order_does_not_change_the_ranking",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="fuzzy-match-accepts-an-infix",
        defect=(
            "fuzzy matching becomes substring containment, so 'acr' matches "
            "'sacred' and a wrong-but-confident subject reaches rank"
        ),
        path=SRC / "discovery.py",
        anchor="    return any(query_tokens <= set(text.split()) for text in haystacks)",
        replacement="    return any(normalized_query in text for text in haystacks)",
        tests=(
            f"{TESTS}/test_chaos_3617_candidate_search.py::"
            "TestFuzzyMatchingIsConservative::"
            "test_a_query_appearing_only_as_an_infix_matches_nothing",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="corpus-authorization-falls-back-to-tenancy",
        defect=(
            "the arm authorizes by tenant instead of by grant, so the "
            "same-tenant restricted project is returned to a principal who "
            "cannot see it -- invisible to every tenant-level check"
        ),
        path=SRC / "corpus_adapter.py",
        anchor="    return world.authorized_entity_ids(principal_id)",
        replacement=(
            "    return frozenset(\n"
            "        seed_ids_for_tenant(world.PRINCIPALS[principal_id].tenant_id)\n"
            "    )"
        ),
        tests=(
            f"{TESTS}/test_chaos_3617_corpus_adapter.py::"
            "TestAuthorizationIsByGrantNotTenancy",
        ),
        expect_failure="assert",
    ),
    Mutation(
        mutation_id="live-gate-skips-when-a-run-was-required",
        defect=(
            "a live-store measurement that did not happen reads as coverage "
            "in a recorded reproduction"
        ),
        path=REPO_ROOT / TESTS / "live_gate.py",
        anchor="    if live_store_required():",
        replacement="    if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_live_gate.py::"
            "test_the_gate_fails_rather_than_skips_when_a_live_run_is_required",
        ),
        expect_failure="Failed",
    ),
)

_ENV = {
    **os.environ,
    "OTEL_ENABLED": "false",
    "PYTHONPATH": str(REPO_ROOT / "src"),
}


def _run_tests(node_ids: tuple[str, ...]) -> tuple[int, str]:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *node_ids, "-q", "-p", "no:randomly"],
        cwd=REPO_ROOT,
        env=_ENV,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout + result.stderr


#: pytest's summary line, e.g. ``2 failed, 13 passed in 1.24s``.
#:
#: Written with the separator INSIDE the repeated group rather than as an
#: optional suffix. The earlier form -- ``(?:\d+ \w+(?:, )?)+`` -- made the
#: comma optional, so ``\d+ \w+`` could match with or without a trailing
#: separator and the engine had exponentially many ways to split an input
#: like ``"0 " + "000 " * n`` before failing. That is a real ReDoS on a
#: pattern fed with subprocess output, and CodeQL was right to block on it.
#: With the separator mandatory between repetitions there is exactly one way
#: to parse any candidate, so backtracking is linear.
_SUMMARY = re.compile(r"^\d+ \w+(?:, \d+ \w+)* in [\d.]+s$", re.MULTILINE)


def _summary_line(output: str) -> str:
    matches = _SUMMARY.findall(output)
    for line in reversed(output.splitlines()):
        if " in " in line and ("passed" in line or "failed" in line):
            return line.strip()
    return matches[-1] if matches else "<no pytest summary>"


def _first_failure(output: str) -> str:
    """Where the mutation died.

    Recorded so a reader can check the failure is the one the guard exists
    to prevent rather than an unrelated collapse in setup -- a mutation that
    dies in a fixture proves nothing about the invariant it claims to test.
    """

    for line in output.splitlines():
        if line.startswith("FAILED "):
            return line.strip()
    for line in output.splitlines():
        if line.startswith("E   ") and line.strip() != "E":
            return line.strip()
    return "<no failure line captured>"


def _failure_evidence(output: str) -> str:
    """Every line pytest attributes to a failure, joined.

    Includes the ``E   `` assertion/exception lines and the ``FAILED``
    summary, because the category can appear in either: an assertion message
    for a guard that raises a typed error, or the exception name for one that
    refuses by construction.
    """

    return "\n".join(
        line
        for line in output.splitlines()
        if line.startswith(("E ", "FAILED ", "E\t")) or " - " in line
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="run a single mutation by id")
    args = parser.parse_args()

    selected = [
        mutation
        for mutation in MUTATIONS
        if args.only is None or mutation.mutation_id == args.only
    ]
    if not selected:
        print(f"no mutation matches --only {args.only!r}", file=sys.stderr)
        return 2

    print(f"CHAOS-3617 guard injection: {len(selected)} mutation(s)\n")
    verdicts: list[tuple[str, str, str]] = []
    for mutation in selected:
        original = mutation.path.read_text()
        if mutation.anchor not in original:
            # INVALID, not KILLED. A silently non-matching anchor would turn
            # this whole harness into a green light with no bulb behind it.
            print(
                f"INVALID  {mutation.mutation_id}: anchor not found verbatim in "
                f"{mutation.path.relative_to(REPO_ROOT)}. The guard was probably "
                "refactored; update the anchor before trusting this run.",
                file=sys.stderr,
            )
            return 1
        if original.count(mutation.anchor) != 1:
            print(
                f"INVALID  {mutation.mutation_id}: anchor appears "
                f"{original.count(mutation.anchor)} times; a mutation that "
                "disables more than the guard under test proves nothing about "
                "that guard.",
                file=sys.stderr,
            )
            return 1

        mutation.path.write_text(
            original.replace(mutation.anchor, mutation.replacement)
        )
        try:
            code, output = _run_tests(mutation.tests)
        finally:
            mutation.path.write_text(original)

        if code == 0:
            print(
                f"SURVIVED {mutation.mutation_id}: the guard was disabled and "
                "every test still passed. That test does not cover the defect "
                f"it names: {mutation.defect}",
                file=sys.stderr,
            )
            print(output, file=sys.stderr)
            return 1

        died_at = _first_failure(output)
        evidence = _failure_evidence(output)
        if mutation.expect_failure not in evidence:
            print(
                f"WRONG-REASON {mutation.mutation_id}: the tests failed, but "
                f"not for the reason this mutation claims to prove.\n"
                f"      expected the failure to mention: "
                f"{mutation.expect_failure!r}\n"
                f"      actual: {died_at}\n"
                "      A mutation that merely turns the suite red is not "
                "evidence that the guard catches the defect it names.",
                file=sys.stderr,
            )
            return 1
        verdicts.append((mutation.mutation_id, _summary_line(output), died_at))
        print(f"RED   {mutation.mutation_id}")
        print(f"      defect: {mutation.defect}")
        print(f"      {_summary_line(output)}")
        print(f"      died at: {died_at}\n")

        # Rule 2: the restore is verified by re-running, never by a git check.
        code, output = _run_tests(mutation.tests)
        if code != 0:
            print(
                f"RESTORE FAILED {mutation.mutation_id}: the guard did not come "
                f"back. {_summary_line(output)}",
                file=sys.stderr,
            )
            return 1
        print(f"GREEN {mutation.mutation_id} restored: {_summary_line(output)}\n")

    print(f"\nAll {len(verdicts)} mutation(s) KILLED and restored green.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
