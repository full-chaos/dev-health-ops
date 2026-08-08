"""Fault injection proof: every CHAOS-3615 guard is load-bearing.

``tests/api/dev/test_chaos_3615_fault_modes.py`` proves each arm-shaped bad
packet is *rejected*. That, on its own, is not proof that the named guard is
what rejects it — a payload could be caught incidentally by an unrelated
field constraint and the test would still be green with the real guard
missing. This script closes that gap the only way it can be closed: it
**removes the named validator and watches the bad packet be accepted.**

For each of the eleven named fault modes it runs one subprocess that:

1. validates the arm-shaped payload against the pristine contract and
   requires a rejection (baseline);
2. neutralizes exactly one guard — deleting a single ``model_validator``
   from ``__pydantic_decorators__`` and rebuilding the affected models, or,
   for the required-field fault, giving the disclosure field the flattering
   default it does not have;
3. validates the same payload again and requires it to be **accepted**.

A guard whose removal changes nothing is not the guard the fault-mode
registry claims it is, and the script fails.

Subprocess isolation is not decoration: step 2 mutates class-level pydantic
state, and doing that inside the test process would corrupt every later
test in the session.

Run it directly::

    python scripts/verify_chaos_3615_fault_mode_guards.py

Exit code 0 means every named guard was observed failing. Any other exit
code names the case that did not behave as claimed. The script also fails
if the case table does not cover every fault mode in
``FAULT_MODE_REGISTRY`` — a fault mode with no injection case would
otherwise be an unmeasured coverage claim.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import BaseModel, ValidationError

from dev_health_ops.api.dev.investigation_contract import (
    ALL_FAULT_MODE_IDS,
    FAULT_MODE_REGISTRY,
    INVESTIGATION_CONTRACT_MODELS,
    FaultModeID,
    RejectingMechanism,
)
from dev_health_ops.api.dev.investigation_contract import packet as packet_module
from dev_health_ops.api.dev.investigation_contract.fixtures import negative_fixtures

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class GuardCase(NamedTuple):
    """One fault mode, and the single guard claimed to reject it."""

    fault_mode: FaultModeID
    contract: str
    fixture_label: str
    #: The class whose validator (or field) is neutralized.
    target_model: str
    #: A ``model_validator`` to delete, or ``None`` for a required-field case.
    validator: str | None
    #: For required-field cases: the field to give a flattering default, and
    #: the value a forgetful producer would have inherited.
    defaulted_field: tuple[str, Any] | None


_F = FaultModeID

CASES: tuple[GuardCase, ...] = (
    GuardCase(
        _F.WRONG_BUT_SIMILAR_SUBJECT_RANKED_FIRST,
        "ask_dev_subject_discovery.v1",
        "commitment_on_fuzzy_label_alone",
        "SubjectDiscovery",
        "validate_commitment_is_evidenced",
        None,
    ),
    GuardCase(
        _F.ORGANIZATION_WIDENING_AFTER_UNRESOLVED_REFERENCE,
        "ask_dev_investigation_packet.v1",
        "organization_widening_after_unresolved_reference",
        "AskDevInvestigationPacket",
        "validate_no_unsafe_organization_widening",
        None,
    ),
    GuardCase(
        _F.IRRELEVANT_EVIDENCE_DISPLACES_LINEAGE,
        "ask_dev_evidence_coverage.v1",
        "evidence_supporting_nothing",
        "InvestigationEvidenceEntry",
        "validate_supports_something",
        None,
    ),
    GuardCase(
        _F.SYMPTOM_LABELLED_AS_PRINCIPAL_DRIVER,
        "ask_dev_driver_analysis.v1",
        "symptom_promoted_to_principal_driver",
        "DriverCandidate",
        "validate_principal_standing_is_earned",
        None,
    ),
    GuardCase(
        _F.STAFFING_CERTAINTY_WITHOUT_ALLOCATION_EVIDENCE,
        "ask_dev_driver_analysis.v1",
        "staffing_certainty_without_denominator",
        "DriverCandidate",
        "validate_staffing_claims_are_qualified",
        None,
    ),
    GuardCase(
        _F.UNRELATED_COHORT_MEMBER,
        "ask_dev_comparison_cohort.v1",
        "unrelated_member_without_inclusion_evidence",
        "CohortMember",
        "validate_inclusion_is_evidenced",
        None,
    ),
    GuardCase(
        _F.REVERSED_RELATIONSHIP_DIRECTION,
        "ask_dev_related_context.v1",
        "reversed_relationship_direction",
        "LineageHop",
        "validate_direction_matches_allowlist",
        None,
    ),
    GuardCase(
        _F.PATH_CROSSES_UNAUTHORIZED_ENTITY,
        "ask_dev_related_context.v1",
        "path_crosses_unauthorized_entity",
        "RelatedContext",
        "validate_paths_stay_inside_authorized_set",
        None,
    ),
    GuardCase(
        _F.DASHBOARD_REDIRECT_WITHOUT_DIRECT_JUDGMENT,
        "ask_dev_investigation_packet.v1",
        "dashboard_redirect_without_direct_judgment",
        "AskDevInvestigationPacket",
        "validate_supported_outcome_asserts_a_judgment",
        None,
    ),
    GuardCase(
        _F.ABSENT_REQUIRED_FIELD_SILENTLY_DEFAULTS,
        "ask_dev_subject_discovery.v1",
        "absent_truncation_disclosure_field",
        "SubjectDiscovery",
        None,
        ("candidates_truncated", False),
    ),
    GuardCase(
        _F.WILDCARD_OR_OPTIONAL_FIELD_MAKES_CHECK_VACUOUS,
        "ask_dev_comparison_cohort.v1",
        "comparison_claimed_without_dimensions",
        "ComparisonCohort",
        "validate_comparison_is_not_vacuous",
        None,
    ),
)


def _case_by_name(name: str) -> GuardCase:
    for case in CASES:
        if str(case.fault_mode) == name:
            return case
    raise SystemExit(f"unknown case {name}")


def _fixture(contract: str, label: str) -> dict[str, Any]:
    for case_label, payload in negative_fixtures()[contract]:
        if case_label == label:
            return payload
    raise SystemExit(f"no negative fixture {contract}/{label}")


def _contract_models() -> list[type[BaseModel]]:
    models: list[type[BaseModel]] = []
    for name in dir(packet_module):
        candidate = getattr(packet_module, name)
        if (
            isinstance(candidate, type)
            and issubclass(candidate, BaseModel)
            and candidate.__module__ == packet_module.__name__
        ):
            models.append(candidate)
    return models


def _rebuild_all() -> None:
    """Regenerate every core schema so a neutralized guard really is gone.

    Several passes because a parent's schema embeds its children's: one pass
    would leave an outer model still validating against the pristine inner
    schema, which would make the injection look like a no-op and produce a
    false 'guard is not load-bearing' verdict. Six passes comfortably
    exceeds the deepest nesting in the contract (packet -> coverage -> entry
    -> evidence ref).
    """

    models = _contract_models()
    for _ in range(6):
        for model in models:
            model.model_rebuild(force=True)


def _neutralize(case: GuardCase) -> None:
    model = getattr(packet_module, case.target_model)
    if case.validator is not None:
        validators = model.__pydantic_decorators__.model_validators
        if case.validator not in validators:
            raise SystemExit(
                f"{case.target_model} has no model validator named "
                f"{case.validator}; the fault-mode registry names a guard "
                "that does not exist"
            )
        del validators[case.validator]
    else:
        assert case.defaulted_field is not None
        field_name, default = case.defaulted_field
        field = model.model_fields[field_name]
        if not field.is_required():
            raise SystemExit(
                f"{case.target_model}.{field_name} already has a default, so "
                "this injection proves nothing"
            )
        field.default = default
        field.default_factory = None
    _rebuild_all()


def _validates(contract: str, payload: dict[str, Any]) -> tuple[bool, str]:
    model = INVESTIGATION_CONTRACT_MODELS[contract]
    try:
        model.model_validate(payload)
    except ValidationError as error:
        first = error.errors()[0]
        return False, f"{'.'.join(str(p) for p in first['loc'])}: {first['msg']}"
    return True, ""


def _run_case(name: str) -> int:
    case = _case_by_name(name)
    payload = _fixture(case.contract, case.fixture_label)

    accepted, detail = _validates(case.contract, payload)
    if accepted:
        print(
            f"FAIL {name}: the arm-shaped payload validates against the "
            "pristine contract; there is no guard here at all"
        )
        return 1
    print(f"  baseline REJECTED  <- {detail}")

    injection = (
        f"validator {case.target_model}.{case.validator}"
        if case.validator is not None
        else f"default on {case.target_model}."
        f"{case.defaulted_field[0] if case.defaulted_field else '?'}"
    )
    _neutralize(case)

    accepted, detail = _validates(case.contract, payload)
    if not accepted:
        print(
            f"FAIL {name}: with {injection} removed the payload is STILL "
            f"rejected ({detail}); the registry names a guard that is not "
            "what actually rejects this fault"
        )
        return 1
    print(f"  neutralised ACCEPTED <- removed {injection}")
    print(f"OK   {name}")
    return 0


def _cross_check_against_registry() -> list[str]:
    """Every case must name exactly the guard the registry names.

    Adversarial review round 1, finding M8. The case table duplicated the
    model and validator names by hand and only the *set of fault ids* was
    checked, so a validator renamed in the registry would leave this script
    neutralizing a stale mapping and still printing GUARD PROOF PASSED — a
    green proof of the wrong thing, which is worse than no proof.
    """

    problems: list[str] = []
    for case in CASES:
        fault = FAULT_MODE_REGISTRY[case.fault_mode]
        mechanism = fault.rejecting_mechanism
        if mechanism is RejectingMechanism.CONTRACT_VALIDATOR:
            reference = fault.validator_reference
            if reference is None:
                problems.append(
                    f"{case.fault_mode}: registry claims a contract validator "
                    "but names none"
                )
                continue
            if case.validator is None:
                problems.append(
                    f"{case.fault_mode}: registry names validator "
                    f"{reference.model_name}.{reference.validator_name} but the "
                    "case injects a field default instead"
                )
                continue
            if (case.target_model, case.validator) != (
                reference.model_name,
                reference.validator_name,
            ):
                problems.append(
                    f"{case.fault_mode}: case neutralizes "
                    f"{case.target_model}.{case.validator} but the registry "
                    f"names {reference.model_name}.{reference.validator_name}"
                )
        elif mechanism is RejectingMechanism.REQUIRED_FIELD:
            if case.validator is not None or case.defaulted_field is None:
                problems.append(
                    f"{case.fault_mode}: registry says the field grammar "
                    "rejects this, so the case must inject a field default"
                )
        else:
            problems.append(
                f"{case.fault_mode}: rejecting mechanism {mechanism} has no "
                "injection semantics; an oracle-judged fault must not claim a "
                "contract-level proof"
            )
    return problems


def _run_all() -> int:
    covered = {case.fault_mode for case in CASES}
    missing = sorted(str(item) for item in set(ALL_FAULT_MODE_IDS) - covered)
    if missing:
        print(
            "FAIL: fault modes with no injection case, so their guards are "
            f"unproved: {missing}"
        )
        return 1
    if len(covered) != len(CASES):
        print("FAIL: the case table lists a fault mode twice")
        return 1
    drift = _cross_check_against_registry()
    if drift:
        print("FAIL: injection cases have drifted from the fault-mode registry:")
        for problem in drift:
            print(f"  - {problem}")
        return 1

    failures: list[str] = []
    for case in CASES:
        name = str(case.fault_mode)
        print(f"\n=== {name} ===")
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--case", name],
            check=False,
            capture_output=True,
            text=True,
        )
        sys.stdout.write(completed.stdout)
        if completed.returncode != 0:
            sys.stderr.write(completed.stderr)
            failures.append(name)
    print("\n" + "=" * 70)
    if failures:
        print(f"GUARD PROOF FAILED for {len(failures)} case(s): {failures}")
        return 1
    print(
        f"GUARD PROOF PASSED: {len(CASES)}/{len(CASES)} named guards observed failing"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", default=None, help="run a single fault mode")
    args = parser.parse_args()
    if args.case is not None:
        return _run_case(args.case)
    return _run_all()


if __name__ == "__main__":
    raise SystemExit(main())
