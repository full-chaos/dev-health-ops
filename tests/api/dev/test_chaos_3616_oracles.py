"""CHAOS-3616: the oracles are total, grounded and satisfiable.

The single most important test in this file is
:func:`test_every_authored_oracle_is_satisfied_by_a_correct_packet`. CHAOS-3612
records what happens without it: a corpus expectation that no correct
implementation can meet produces a failure that reads as model quality, which
the issue calls "the most expensive kind of defect this trial can carry". The
reference witness turns that from a review question into an executed one.

The witness is derived from the oracles, so a green run proves the
expectations are *reachable*, never that they are *right*. Correctness of the
expectations rests on the world being the independent construction record it
claims to be, which ``test_chaos_3616_world.py`` checks separately. Stating
the limit here rather than leaving it to be inferred: this file is a
satisfiability proof and nothing more.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from dev_health_ops.api.dev.investigation_contract import AskDevInvestigationPacket
from dev_health_ops.api.dev.investigation_corpus import oracles as oracles_module
from dev_health_ops.api.dev.investigation_corpus.cases import (
    CASE_REGISTRY,
    CaseDisposition,
    authored_cases,
)
from dev_health_ops.api.dev.investigation_corpus.evaluate import (
    Verdict,
    evaluate_payload,
)
from dev_health_ops.api.dev.investigation_corpus.oracles import (
    CASE_ORACLES,
    CaseOracle,
    ForbiddenEvidence,
    ForbiddenReason,
    PathExpectation,
    oracle_for,
    required_evidence_handles,
    validate_oracles,
)
from dev_health_ops.api.dev.investigation_corpus.reference import reference_packet
from dev_health_ops.api.dev.investigation_corpus.world import (
    EVIDENCE_BY_HANDLE,
    EVIDENCE_BY_SLUG,
    authorized_entity_ids,
)

AUTHORED = [case.case_id for case in authored_cases()]


# --------------------------------------------------------------------------
# Totality
# --------------------------------------------------------------------------


def test_oracles_validate() -> None:
    validate_oracles()


def test_every_authored_case_has_an_oracle() -> None:
    missing = sorted(set(AUTHORED) - set(CASE_ORACLES))
    assert not missing, f"authored cases nothing measures: {missing}"


def test_no_oracle_measures_a_skipped_case() -> None:
    """A skipped case with an oracle would be scored despite its disposition."""

    skipped = {
        case.case_id
        for case in CASE_REGISTRY.values()
        if case.disposition is not CaseDisposition.AUTHORED
    }
    assert not (skipped & set(CASE_ORACLES))


# --------------------------------------------------------------------------
# The satisfiability proof
# --------------------------------------------------------------------------


@pytest.mark.parametrize("case_id", AUTHORED)
def test_the_reference_witness_survives_the_canonical_validator(case_id: str) -> None:
    """Schema validity is not enough, and the manifest says so.

    ``validation_policy.schema_only_validation_is_sufficient`` is ``false``,
    so the witness is validated through the Pydantic model — the only check
    that covers authorization scope, evidence closure, driver standing and
    family obligations.
    """

    AskDevInvestigationPacket.model_validate(reference_packet(case_id))


@pytest.mark.parametrize("case_id", AUTHORED)
def test_every_authored_oracle_is_satisfied_by_a_correct_packet(case_id: str) -> None:
    evaluation = evaluate_payload(case_id, reference_packet(case_id))
    assert evaluation.contract_valid, evaluation.contract_error
    failures = [
        (result.dimension_id.value, result.detail) for result in evaluation.failures()
    ]
    assert not failures, (
        f"{case_id}: a packet built to satisfy this oracle does not. Either the "
        f"oracle is unsatisfiable or the scorer is wrong: {failures}"
    )


@pytest.mark.parametrize("case_id", AUTHORED)
def test_every_scored_dimension_reaches_a_verdict(case_id: str) -> None:
    """Silence is not a verdict.

    Every dimension a case declares must come back PASS or NOT_APPLICABLE
    with a reason. A dimension that produced nothing would render as an empty
    cell that reads like a clean sheet.
    """

    evaluation = evaluate_payload(case_id, reference_packet(case_id))
    case = CASE_REGISTRY[case_id]
    assert {result.dimension_id for result in evaluation.results} == set(
        case.scoring_dimension_ids
    )
    for result in evaluation.results:
        assert result.verdict in {Verdict.PASS, Verdict.NOT_APPLICABLE}
        assert result.detail.strip(), (
            f"{case_id}/{result.dimension_id}: a verdict with no stated reason"
        )


def test_the_witness_is_not_uniformly_not_applicable() -> None:
    """A witness that passed by scoring nothing would prove nothing.

    Without this, every oracle could be 'satisfied' by a scorer that returned
    NOT_APPLICABLE everywhere, which is the vacuous-coverage shape in its
    purest form.
    """

    passes = 0
    for case_id in AUTHORED:
        evaluation = evaluate_payload(case_id, reference_packet(case_id))
        passes += sum(
            1 for result in evaluation.results if result.verdict is Verdict.PASS
        )
    assert passes >= 4 * len(AUTHORED), (
        f"only {passes} PASS verdicts across {len(AUTHORED)} cases; the corpus "
        "is being satisfied by not-applicable rather than by evidence"
    )


# --------------------------------------------------------------------------
# No oracle is satisfiable by fabricated or unauthorized evidence
# --------------------------------------------------------------------------


@pytest.mark.parametrize("case_id", AUTHORED)
def test_required_evidence_exists_is_citable_and_is_authorized(case_id: str) -> None:
    """The CHAOS-3612 / C14 recurrence guard, restated as a test.

    Three ways an expectation becomes unsatisfiable by a correct arm, and all
    three are checked: the handle does not exist; the handle exists but a
    correct arm must not cite it; the handle exists and is about something
    the caller cannot see.
    """

    oracle = oracle_for(case_id)
    case = CASE_REGISTRY[case_id]
    visible = authorized_entity_ids(case.principal_id)
    slugs = set(oracle.required_evidence_slugs)
    for driver in oracle.expected_principal_drivers + oracle.expected_non_drivers:
        slugs.update(driver.supporting_evidence_slugs)
    for slug in sorted(slugs):
        record = EVIDENCE_BY_SLUG.get(slug)
        assert record is not None, f"{case_id} requires unminted evidence {slug}"
        assert record.is_citable, f"{case_id} requires non-citable evidence {slug}"
        assert record.entity_id in visible, (
            f"{case_id} requires evidence about {record.entity_id}, which "
            f"principal {case.principal_id} cannot see"
        )


def test_the_two_evidence_vocabularies_are_the_same_vocabulary() -> None:
    """Ground truth and fixture sources cannot drift apart here.

    CHAOS-3612's defect was two id sets with an empty intersection: the
    ground truth cited one, the authored sources supplied the other, and no
    arm could satisfy an expectation over either. This corpus has one mint,
    and this test is the executable statement of that.
    """

    required: set[str] = set()
    for case_id in AUTHORED:
        required |= required_evidence_handles(case_id)
    assert required, "no oracle requires any evidence at all"
    supplied = set(EVIDENCE_BY_HANDLE)
    assert required <= supplied, (
        "oracles require handles the world's sources do not supply: "
        f"{sorted(required - supplied)}"
    )


@pytest.mark.parametrize("case_id", AUTHORED)
def test_forbidden_evidence_is_forbidden_for_a_stated_world_reason(
    case_id: str,
) -> None:
    oracle = oracle_for(case_id)
    for forbidden in oracle.forbidden_evidence:
        record = EVIDENCE_BY_SLUG[forbidden.slug]
        if forbidden.reason is ForbiddenReason.ADVERSARIAL:
            assert record.is_adversarial
        elif forbidden.reason is ForbiddenReason.NOT_CITABLE:
            assert record.state.value != "active"
        elif forbidden.reason is ForbiddenReason.UNAUTHORIZED:
            visible = authorized_entity_ids(CASE_REGISTRY[case_id].principal_id)
            assert record.entity_id not in visible
        elif forbidden.reason is ForbiddenReason.CROSS_TENANT:
            assert record.tenant_id != "org_helio"


def test_the_corpus_forbids_something_in_every_reason_class() -> None:
    """A reason nobody uses is a rule nobody has tested.

    Enforced as equality rather than containment. When a reason turned out to
    be the wrong tool -- an earlier HISTORICAL_ONLY member, which would have
    scored a correct arm wrong for citing a removed dependency as an excluded
    candidate -- the right response was to delete the member, not to grant it
    an exemption here.
    """

    used = {
        forbidden.reason
        for oracle in CASE_ORACLES.values()
        for forbidden in oracle.forbidden_evidence
    }
    missing = sorted(str(item) for item in set(ForbiddenReason) - used)
    assert not missing, f"forbidden-evidence reasons nothing exercises: {missing}"


# --------------------------------------------------------------------------
# The guards reject what they claim to
# --------------------------------------------------------------------------


def _replace(case_id: str, **changes: Any) -> dict[str, CaseOracle]:
    registry = dict(CASE_ORACLES)
    registry[case_id] = dataclasses.replace(CASE_ORACLES[case_id], **changes)
    return registry


def test_an_oracle_requiring_unminted_evidence_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "T01_clearly_struggling_team",
            required_evidence_slugs=("ev_that_was_never_minted",),
        ),
    )
    with pytest.raises(RuntimeError, match="which the world never minted"):
        validate_oracles()


def test_an_oracle_requiring_revoked_evidence_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "H06_prior_attempt_reference",
            required_evidence_slugs=("rv_vertex_revoked",),
            forbidden_evidence=(),
        ),
    )
    with pytest.raises(RuntimeError, match="A correct arm must not cite it"):
        validate_oracles()


def test_an_oracle_requiring_unauthorized_evidence_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shape only an unauthorized arm could satisfy."""

    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "T01_clearly_struggling_team",
            required_evidence_slugs=("cc_quarry_activity",),
            forbidden_evidence=(),
        ),
    )
    with pytest.raises(RuntimeError, match="Only an unauthorized arm could satisfy"):
        validate_oracles()


def test_an_unjustified_forbiddance_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "T01_clearly_struggling_team",
            forbidden_evidence=(
                ForbiddenEvidence("ci_pulse_green", ForbiddenReason.ADVERSARIAL),
            ),
        ),
    )
    with pytest.raises(RuntimeError, match="the world does not flag it so"):
        validate_oracles()


def test_requiring_and_forbidding_the_same_evidence_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "T01_clearly_struggling_team",
            forbidden_evidence=(
                ForbiddenEvidence("rv_atlas_queue", ForbiddenReason.NOT_CITABLE),
            ),
        ),
    )
    with pytest.raises(RuntimeError, match="both requires and forbids"):
        validate_oracles()


def test_requiring_a_path_the_world_lacks_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invented = PathExpectation(
        "proj_meridian",
        oracles_module.RelationshipType.DEPENDS_ON,
        "dep_authcore",
    )
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "S03_shared_dependency_portfolio_risk",
            required_paths=(invented,),
        ),
    )
    with pytest.raises(RuntimeError, match="which the world does not contain"):
        validate_oracles()


def test_forbidding_a_currently_true_path_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forbidding a true edge would score a correct arm as wrong."""

    real = PathExpectation(
        "proj_pulse",
        oracles_module.RelationshipType.DEPENDS_ON,
        "dep_authcore",
    )
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace("S03_shared_dependency_portfolio_risk", forbidden_paths=(real,)),
    )
    with pytest.raises(RuntimeError, match="is currently true in the world"):
        validate_oracles()


def test_a_driver_affecting_an_undeclared_entity_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The contract would reject the packet, so the oracle must reject first."""

    oracle = CASE_ORACLES["T01_clearly_struggling_team"]
    broken = dataclasses.replace(
        oracle.expected_principal_drivers[0],
        affected_entity_ids=("proj_meridian",),
    )
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace("T01_clearly_struggling_team", expected_principal_drivers=(broken,)),
    )
    with pytest.raises(RuntimeError, match="never asks the packet to declare"):
        validate_oracles()


def test_a_required_entity_no_path_reaches_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oracle = CASE_ORACLES["T01_clearly_struggling_team"]
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "T01_clearly_struggling_team",
            required_entity_ids=(*oracle.required_entity_ids, "proj_meridian"),
        ),
    )
    with pytest.raises(RuntimeError, match="no expected path reaches"):
        validate_oracles()


def test_a_supported_outcome_without_a_principal_driver_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace("T01_clearly_struggling_team", expected_principal_drivers=()),
    )
    with pytest.raises(RuntimeError, match="permits a supported outcome but expects"):
        validate_oracles()


def test_permitting_and_forbidding_the_same_candidate_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The near-miss/forbidden distinction is load-bearing, not cosmetic."""

    oracle = CASE_ORACLES["H03_the_auth_work"]
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(
            "H03_the_auth_work",
            forbidden_subject_ids=(oracle.permitted_candidate_ids[1],),
        ),
    )
    with pytest.raises(RuntimeError, match="both permits and forbids candidates"):
        validate_oracles()


def test_an_oracle_whose_driver_and_symptom_share_evidence_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The disjointness guard had no test, and it is load-bearing twice over.

    Independent verification round 2 found that neutering
    ``_check_driver_evidence_is_unambiguous`` produced **zero** failures --
    nothing measured it. That matters more than an ordinary untested guard:
    the driver matcher binds a principal to the one expectation its evidence
    overlaps, so with disjointness off an overlapping oracle makes a
    *correct* witness fail. The guard and the matcher are coupled, and this
    test is the only thing holding the coupling.
    """

    case_id = "T01_clearly_struggling_team"
    oracle = CASE_ORACLES[case_id]
    principal = oracle.expected_principal_drivers[0]
    symptom = oracle.expected_non_drivers[0]
    overlapping = dataclasses.replace(
        symptom,
        supporting_evidence_slugs=(
            *symptom.supporting_evidence_slugs,
            *principal.supporting_evidence_slugs,
        ),
    )
    monkeypatch.setattr(
        oracles_module,
        "CASE_ORACLES",
        _replace(case_id, expected_non_drivers=(overlapping,)),
    )
    with pytest.raises(RuntimeError, match="shares evidence with an expected"):
        validate_oracles()


def test_the_prose_discriminator_holds_over_the_whole_alias_set() -> None:
    """Every contract string alias is safe to classify by its pattern.

    The prose scan decides whether a field is free text by asking whether the
    contract constrains it with a ``pattern``. That rests on an invariant of
    the *contract*, not of the corpus, and nothing on the contract side
    asserted it.

    The two directions are not symmetric, and the test is written around
    that. Scanning an identifier as prose is **noise** -- a handle contains no
    English and matches no person word. Skipping prose as an identifier is a
    **hole**, because it is a field a producer chose the words in. So:

    * every alias whose values can carry a sentence must be pattern-free, so
      the scan reaches it;
    * every pattern-constrained alias must be genuinely unable to carry one,
      proved by testing its own pattern against a string with a space in it.

    Written over the alias set rather than over today's fields, so a new
    alias fails this rather than a new field failing something subtler later.
    """

    import re as _re

    from dev_health_ops.api.dev import contracts
    from dev_health_ops.api.dev.contracts_v2 import base

    #: Aliases whose values are producer-authored text. Losing pattern-freedom
    #: on any of these silently removes every field using it from the scan.
    prose_bearing = {"Label", "ShortText", "LongText"}

    def _pattern_of(alias: object) -> str | None:
        for item in getattr(alias, "__metadata__", ()):
            pattern = getattr(item, "pattern", None)
            if pattern:
                return str(pattern)
        return None

    seen: set[str] = set()
    for module in (contracts, base):
        for name in dir(module):
            alias = getattr(module, name)
            if getattr(alias, "__origin__", None) is not str:
                continue
            pattern = _pattern_of(alias)
            if name in prose_bearing:
                seen.add(name)
                assert pattern is None, (
                    f"{name} is a prose-bearing alias and now carries the "
                    f"pattern {pattern!r}. Every field using it would be "
                    "skipped by the person-attribution scan -- the dangerous "
                    "direction."
                )
                continue
            if pattern is None:
                # Pattern-free and not on the prose list: the scan will read
                # it as text. Harmless over-inclusion, and better than the
                # alternative, so this is allowed rather than asserted away.
                continue
            seen.add(name)
            assert not _re.match(pattern, "alpha beta"), (
                f"{name} is pattern-constrained but its pattern admits a "
                "string containing a space, so it could carry a sentence the "
                f"scan would skip: {pattern!r}"
            )

    missing = sorted(prose_bearing - seen)
    assert not missing, (
        f"these prose-bearing aliases were not found on the contract modules: "
        f"{missing}; the invariant is unverified for them"
    )
