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
    ),
    Mutation(
        mutation_id="prose-fact-accepted-on-read",
        defect="a stored prose fact is presented to a consumer as evidence",
        path=SRC / "backend.py",
        anchor="    if match is None:",
        replacement="    if False and match is None:",
        tests=(
            f"{TESTS}/test_chaos_3617_structured_ingestion.py::"
            "TestStructuredRecordsAreNotProse::test_a_prose_fact_is_rejected_on_read",
        ),
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
    ),
    Mutation(
        mutation_id="builder-trusts-the-readouts-authorization-claim",
        defect=(
            "a readback bug (or a future second reader) smuggles an "
            "unauthorized hop endpoint past the emission step"
        ),
        path=SRC / "packet_builder.py",
        anchor="                if endpoint not in authorized_entity_ids:",
        replacement="                if False:",
        tests=(
            f"{TESTS}/test_chaos_3617_authorization.py::TestAuthorizationFiltering::"
            "test_the_builder_refuses_a_readout_whose_paths_escape_the_set",
        ),
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


_SUMMARY = re.compile(r"^(?:\d+ \w+(?:, )?)+ in [\d.]+s$", re.MULTILINE)


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
