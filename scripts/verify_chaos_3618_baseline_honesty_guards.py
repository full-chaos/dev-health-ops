#!/usr/bin/env python3
"""Fault injection proof: every CHAOS-3618 honesty guard is load-bearing.

``tests/context_fabric/test_chaos_3618_*.py`` and
``tests/api/dev/test_chaos_3618_investigation_shadow.py`` prove that an
honest baseline is what the code produces today. That, on its own, is not
proof that the *named guard* is what keeps it honest — a check could be
passing incidentally, or be satisfied by an unrelated constraint, and the
suite would stay green with the real guard gone. This script closes that
gap the only way it can be closed: it **removes one guard at a time and
watches the dishonest baseline become expressible.**

For each case it runs one subprocess that:

1. runs the named test against the pristine tree and requires it to PASS
   (baseline — a case whose test already fails proves nothing);
2. neutralises exactly one guard via an in-process patch injected through a
   generated ``sitecustomize.py`` on ``PYTHONPATH``;
3. runs the same test again and requires it to FAIL.

A guard whose removal changes nothing is not the guard the docs claim it
is, and this script says so by name.

Subprocess isolation is not decoration. Several plants mutate module-level
tables that other tests read, and a few of the guards are import-time
checks that only run once per interpreter — patching in-process would both
corrupt the session and miss the exact class of defect those guards catch.
No on-disk source file is ever mutated; the generated ``sitecustomize``
lives in a temporary directory and is gone when the run ends.

Run it directly::

    .venv/bin/python scripts/verify_chaos_3618_baseline_honesty_guards.py

Exit code 0 means every named guard was observed failing. Any other exit
code names the case that did not behave as claimed.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class GuardCase(NamedTuple):
    """One honesty guard, and the single plant claimed to defeat it."""

    #: Short id, used in output and in the ``--only`` filter.
    case_id: str
    #: What a reader loses if this guard stops working.
    stake: str
    #: ``pytest`` node id (may include a ``-k`` style parametrisation).
    test: str
    #: Python source written to a generated ``sitecustomize.py``. Runs at
    #: interpreter startup, before pytest collects anything.
    plant: str


CASES: tuple[GuardCase, ...] = (
    GuardCase(
        case_id="overclaimed_relationship",
        stake=(
            "a relationship marked available whose endpoint kinds the native "
            "path cannot express -- inflated lineage recall"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_available_relationship_has_an_expressible_endpoint_pair"
        ),
        plant="""
import dataclasses
from dev_health_ops.api.dev.investigation_contract import RelationshipType
from dev_health_ops.context_fabric.native_arm import capabilities as caps

table = dict(caps.NATIVE_RELATIONSHIP_CAPABILITY)
table[RelationshipType.DEPLOYS] = dataclasses.replace(
    table[RelationshipType.DEPLOYS],
    state=caps.NativeRelationshipState.AVAILABLE,
    content_slot="deployments",
    gap_mechanism=None,
)
caps.NATIVE_RELATIONSHIP_CAPABILITY = table
""",
    ),
    GuardCase(
        case_id="misattributed_gap_mechanism",
        stake=(
            "a gap blamed on an absent subject kind when the real blocker is a "
            "missing adapter -- the trial report recommends the wrong fix"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_subject_kind_absent_is_only_blamed_when_it_is_actually_the_blocker"
        ),
        plant="""
import dataclasses
from dev_health_ops.api.dev.investigation_contract import RelationshipType
from dev_health_ops.context_fabric.native_arm import capabilities as caps

table = dict(caps.NATIVE_RELATIONSHIP_CAPABILITY)
table[RelationshipType.DEPENDS_ON] = dataclasses.replace(
    table[RelationshipType.DEPENDS_ON],
    gap_mechanism=caps.NativeGapMechanism.SUBJECT_KIND_ABSENT,
)
caps.NATIVE_RELATIONSHIP_CAPABILITY = table
""",
    ),
    GuardCase(
        case_id="corpus_tunable_classifier",
        stake=(
            "a family classifier that can see the question text, which is the "
            "first step of tuning the baseline to the trial corpus"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_classify_question_family_cannot_see_the_question"
        ),
        plant="""
from dev_health_ops.context_fabric.native_arm import capabilities as caps

_real = caps.classify_question_family


def classify_question_family(*, intent_id, shape, question=None):
    return _real(intent_id=intent_id, shape=shape)


caps.classify_question_family = classify_question_family
""",
    ),
    GuardCase(
        case_id="unsafe_organization_widening",
        stake=(
            "an unresolved named reference widening into portfolio scope, which "
            "answers a question nobody asked"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_an_unresolved_reference_is_what_separates_widening_from_portfolio_scope"
        ),
        plant="""
from dev_health_ops.api.dev.contracts_v2.base import Cardinality
from dev_health_ops.api.dev.investigation_contract import ComparisonShape
from dev_health_ops.context_fabric.native_arm import capabilities as caps


def comparison_shape_for(*, cardinality, has_unresolved_mentions):
    if cardinality is Cardinality.SINGULAR:
        return ComparisonShape.SINGULAR_SUBJECT
    if cardinality is Cardinality.PLURAL_COHORT:
        return ComparisonShape.EXPLICIT_COHORT
    return ComparisonShape.PORTFOLIO_WIDE


caps.comparison_shape_for = comparison_shape_for
""",
    ),
    GuardCase(
        case_id="unauthorized_evidence_admitted",
        stake=(
            "evidence naming an entity outside the authorized set reaching the "
            "packet's evidence index"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_evidence_outside_the_authorized_set_is_dropped_not_admitted"
        ),
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj


def _restrict_evidence_to_authorized(entries, *, authorized):
    return entries, 0


proj._restrict_evidence_to_authorized = _restrict_evidence_to_authorized
""",
    ),
    GuardCase(
        case_id="driver_asserted_without_lineage",
        stake=(
            "a driver asserted for a family whose related-context section the "
            "native arm cannot fill -- an unsupported attribution"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_a_deficiency_finding_stops_at_candidate_only"
        ),
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj

_real = proj._driver_analysis


def _driver_analysis(payload, *, subject_ids, evidence_handles, may_assert, limitations):
    return _real(
        payload,
        subject_ids=subject_ids,
        evidence_handles=evidence_handles,
        may_assert=True,
        limitations=limitations,
    )


proj._driver_analysis = _driver_analysis
""",
    ),
    GuardCase(
        case_id="driver_on_unindexed_evidence",
        stake=(
            "a driver resting on an evidence handle nobody can dereference -- "
            "an attribution with no closure"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_a_driver_citing_an_unindexed_handle_is_not_asserted"
        ),
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj

_real = proj._driver_analysis


def _driver_analysis(payload, *, subject_ids, evidence_handles, may_assert, limitations):
    # Pretend every cited handle was indexed, and let the family's own
    # section requirement stop being the thing that demotes the driver.
    everything = frozenset(
        handle
        for observation in (
            payload.investigation_result.observations
            if payload.investigation_result
            else ()
        )
        for finding in (
            observation.content.deficiency_findings if observation.content else ()
        )
        for handle in finding.evidence_ref_ids
    )
    return _real(
        payload,
        subject_ids=subject_ids,
        evidence_handles=evidence_handles | everything,
        may_assert=True,
        limitations=limitations,
    )


proj._driver_analysis = _driver_analysis
""",
    ),
    GuardCase(
        case_id="canonical_bypass_allowed",
        stake=(
            "a packet citing evidence no canonical service minted -- the exact "
            "shape a fabricated measurement arrives in"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_packet_citing_uncoined_evidence_is_rejected"
        ),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam


def canonical_bypass_offenders(*, packet_evidence_handles, canonical_evidence):
    return ()


seam.canonical_bypass_offenders = canonical_bypass_offenders
""",
    ),
    GuardCase(
        case_id="unattributed_comparison_record",
        stake=(
            "a persisted comparison record that cannot say which arm produced "
            "its packet -- the differential becomes unreadable"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_recorded_evaluation_without_an_arm_is_unconstructable"
        ),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam


def _post_init(self):
    return None


seam.InvestigationShadowRecord.__post_init__ = _post_init
""",
    ),
)


def _run_pytest(
    test: str, *, plant_dir: Path | None
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-m",
        "pytest",
        test,
        "-q",
        "-p",
        "no:randomly",
        "--no-header",
    ]
    env_overlay = {}
    if plant_dir is not None:
        # sitecustomize is imported during interpreter startup, before pytest
        # collects anything -- which is what lets a plant land before the
        # module under test binds any name it reads at import time.
        env_overlay["PYTHONPATH"] = str(plant_dir)
    return subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        env={**_base_env(), **env_overlay},
    )


def _base_env() -> dict[str, str]:
    import os

    env = dict(os.environ)
    # Keep the plants deterministic and the output quiet.
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    return env


def _verify(case: GuardCase, *, verbose: bool) -> str | None:
    """Return an error string, or ``None`` when the guard behaved as claimed."""

    baseline = _run_pytest(case.test, plant_dir=None)
    if baseline.returncode != 0:
        return (
            f"{case.case_id}: the named test does not pass against the pristine "
            f"tree, so the plant would prove nothing\n{baseline.stdout[-2000:]}"
        )

    workspace = Path(tempfile.mkdtemp(prefix=f"chaos3618-{case.case_id}-"))
    try:
        # The plant must land before the test module imports anything. A
        # conftest.py beside the tests would do it, but writing into the repo
        # is exactly what this script promises not to do -- so the plant is
        # installed as a sitecustomize on PYTHONPATH instead, which Python
        # imports during interpreter startup, before pytest collects anything.
        (workspace / "sitecustomize.py").write_text(case.plant)
        planted = _run_pytest(case.test, plant_dir=workspace)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    if planted.returncode == 0:
        return (
            f"{case.case_id}: removing the guard changed nothing -- the test "
            "still passes, so it is not the guard that keeps this honest\n"
            f"  stake: {case.stake}"
        )
    if verbose:
        print(planted.stdout[-1500:])
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", help="run a single case by id")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cases = CASES
    if args.only:
        cases = tuple(case for case in CASES if case.case_id == args.only)
        if not cases:
            print(f"no such case: {args.only}", file=sys.stderr)
            print(f"known: {[case.case_id for case in CASES]}", file=sys.stderr)
            return 2

    failures: list[str] = []
    for index, case in enumerate(cases, start=1):
        print(f"[{index}/{len(cases)}] {case.case_id} ... ", end="", flush=True)
        error = _verify(case, verbose=args.verbose)
        if error is None:
            print("guard observed failing")
        else:
            print("NOT LOAD-BEARING")
            failures.append(error)

    print()
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
            print(file=sys.stderr)
        print(
            f"{len(failures)} of {len(cases)} guards are not load-bearing.",
            file=sys.stderr,
        )
        return 1
    print(f"All {len(cases)} guards observed failing when removed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
