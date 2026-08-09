"""CHAOS-3646: the admission path, and the guards that must keep biting.

Every test here either observes a guard FAIL on a planted defect, or asserts
a structural property that a future edit could not repeal silently. A test
that only confirms the happy path would read as coverage of a boundary while
proving nothing about it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import FreshnessState
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.evidence_service import (
    MAX_ADMISSION_CANDIDATES,
    EvidenceAvailability,
    EvidenceCandidate,
    EvidenceRecord,
    EvidenceService,
    ScopeResolveRequest,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_shadow import (
    InvestigationShadow,
    InvestigationShadowStatus,
    canonical_bypass_offenders,
)
from dev_health_ops.context_fabric.graph_arm.admission import (
    ARM_SOURCE_SYSTEM,
    EVIDENCE_ADMISSION_FLAG,
    candidate_locator,
    candidates_from_readout,
    evidence_admission_enabled,
)
from dev_health_ops.context_fabric.graph_arm.packet_builder import _admission_locator
from trials.chaos_3646 import spine, sweep
from trials.chaos_3646.canonical import (
    CorpusCandidateResolver,
    CorpusScopeAuthorizer,
    corpus_evidence_service,
)

SECRET = "chaos-3646-test-signing-secret-not-a-real-key-at-all"
ORG = world.ORG_HELIO
PRINCIPAL = world.PRINCIPAL_ANALYST

SCOPE_REQUEST = ScopeResolveRequest(
    explicit_refs=(),
    team_filter_refs=(),
    allow_organization_fallback=False,
)


def _service(**kwargs) -> EvidenceService:
    return corpus_evidence_service(
        org_id=ORG, principal_id=PRINCIPAL, secret=SECRET, **kwargs
    )


def _admit(service: EvidenceService, *candidates: EvidenceCandidate):
    return asyncio.run(
        service.admit(
            org_id=ORG,
            permission_fingerprint="chaos-3646-test",
            scope_request=SCOPE_REQUEST,
            candidates=candidates,
        )
    )


def _candidate(slug: str, **overrides) -> EvidenceCandidate:
    evidence = world.EVIDENCE_BY_SLUG[slug]
    base = {
        "source_system": ARM_SOURCE_SYSTEM,
        "entity_type": evidence.source_class.value,
        "entity_id": evidence.entity_id,
        "locator": slug,
        "repository_ids": (),
    }
    return EvidenceCandidate(**{**base, **overrides})


# ---------------------------------------------------------------------------
# The candidate carries no authority -- structurally, not by convention
# ---------------------------------------------------------------------------


def test_candidate_has_no_field_that_could_carry_authority() -> None:
    """The field set IS the guarantee, so the field set is what is asserted.

    A future edit that adds ``evidence_ref_id`` or ``citation_text`` to make
    something convenient would repeal "the graph never mints authority"
    without touching a line that mentions it. This test is where that edit
    stops.
    """

    assert {field.name for field in dataclasses.fields(EvidenceCandidate)} == {
        "source_system",
        "entity_type",
        "entity_id",
        "locator",
        "repository_ids",
    }


def test_candidates_from_readout_never_carry_a_handle() -> None:
    readout = spine.readout(("proj_identity_rewrite",))
    candidates = candidates_from_readout(readout)
    assert candidates
    world_handles = {item.handle for item in world.EVIDENCE_BY_SLUG.values()}
    for candidate in candidates:
        for value in dataclasses.asdict(candidate).values():
            assert value not in world_handles
            assert not (isinstance(value, str) and value.startswith("ev1_"))


def test_packet_builder_and_admission_agree_on_the_locator() -> None:
    """Two expressions of one rule, checked by calling both.

    ``packet_builder`` may not import ``admission`` (harnesses call
    ``build_packet`` without ever admitting anything), so the rule is written
    twice. Twice-written rules drift; this is the only thing that notices.
    """

    readout = spine.readout(("proj_acr",))
    assert readout.observations
    for observation in readout.observations:
        assert _admission_locator(observation) == candidate_locator(observation)


# ---------------------------------------------------------------------------
# Flag and structural off-switch
# ---------------------------------------------------------------------------


def test_admission_flag_defaults_off() -> None:
    assert evidence_admission_enabled({}) is False
    assert evidence_admission_enabled({EVIDENCE_ADMISSION_FLAG: "true"}) is False
    assert evidence_admission_enabled({EVIDENCE_ADMISSION_FLAG: "1"}) is True


def test_admission_flag_is_registered_in_the_env_scrub_list() -> None:
    """A new ``os.getenv`` name in ``src/`` that no scrub list knows about
    fails a drift guard no test subset reaches. Caught here instead."""

    from tests import _env_isolation

    assert EVIDENCE_ADMISSION_FLAG in _env_isolation.SCRUB_ENV_NAMES


def test_a_service_with_no_resolver_can_only_refuse() -> None:
    """The structural off-switch. No shipped construction passes
    ``candidate_resolvers``, so this is what every deployed process does."""

    service = EvidenceService(
        entitlement=_AlwaysEntitled(),
        authorizer=CorpusScopeAuthorizer(principal_id=PRINCIPAL),
        signer=_signer(),
        native_adapters=(),
    )
    result = _admit(service, _candidate("wg_identity_rewrite"))
    (only,) = result.admissions
    assert only.evidence is None
    assert only.state is EvidenceAvailability.UNCONFIGURED
    assert only.warning == "source_unconfigured"


# ---------------------------------------------------------------------------
# Resolve / authorize / mint, and every refusal branch
# ---------------------------------------------------------------------------


def test_a_resolvable_authorized_candidate_is_admitted_with_the_world_handle() -> None:
    result = _admit(_service(), _candidate("wg_identity_rewrite"))
    (only,) = result.admissions
    assert only.state is EvidenceAvailability.AVAILABLE
    assert only.evidence is not None
    assert only.evidence.evidence_ref_id == world.evidence_handle("wg_identity_rewrite")
    # Every content field came back from the WORLD, not from the candidate.
    record = world.EVIDENCE_BY_SLUG["wg_identity_rewrite"]
    assert only.evidence.display_label == record.display_label
    assert only.evidence.entity_id == record.entity_id


def test_an_unknown_locator_is_refused_as_no_matches() -> None:
    result = _admit(
        _service(),
        EvidenceCandidate(
            source_system=ARM_SOURCE_SYSTEM,
            entity_type="work_item",
            entity_id="proj_identity_rewrite",
            locator="wi_this_record_does_not_exist",
        ),
    )
    (only,) = result.admissions
    assert only.evidence is None
    assert only.state is EvidenceAvailability.NO_MATCHES
    assert only.warning == "evidence_deleted_or_unavailable"


@pytest.mark.parametrize(
    "slug", ["rv_vertex_revoked", "wi_quarry_redacted", "wi_beacon_deleted"]
)
def test_a_withdrawn_record_is_refused(slug: str) -> None:
    """The refusal branch the SWEEP never exercises.

    The arm's own CHAOS-3628 filter removes withdrawn evidence upstream, so
    no withdrawn candidate is ever submitted during the measured sweep. That
    makes this branch untested by the artifact, and an untested refusal is
    indistinguishable from an absent one -- so it is tested here directly.
    """

    result = _admit(_service(), _candidate(slug))
    (only,) = result.admissions
    assert only.evidence is None
    assert only.state is EvidenceAvailability.NO_MATCHES


def test_a_cross_tenant_record_is_refused() -> None:
    result = _admit(_service(), _candidate("lumen_wg_acr"))
    (only,) = result.admissions
    assert only.evidence is None


def test_an_entity_outside_the_grant_is_refused_by_the_existing_check() -> None:
    """The record exists, in the right tenant, and the principal may not see
    its entity. ``proj_quarry`` is the corpus's restricted project INSIDE the
    caller's own tenant, which is why a tenant check alone would pass it."""

    assert "proj_quarry" not in world.PRINCIPALS[PRINCIPAL].visible_entity_ids
    quarry = [
        item
        for item in world.EVIDENCE_BY_SLUG.values()
        if item.entity_id == "proj_quarry" and item.state is world.EvidenceState.ACTIVE
    ]
    assert quarry, "the corpus must hold an active record about the restricted project"
    result = _admit(_service(), _candidate(quarry[0].slug))
    (only,) = result.admissions
    assert only.evidence is None
    assert only.state is EvidenceAvailability.UNAUTHORIZED
    assert only.warning == "not_found"


def test_the_entity_check_is_observed_failing_when_the_grant_is_widened() -> None:
    """The guard, watched from both sides.

    The test above shows the refusal. On its own that could be true for the
    wrong reason -- a resolver returning ``None``, a typo in the slug. So the
    SAME candidate is admitted under a grant that includes the entity. If the
    entity check were removed, only the first test would change; if the
    resolver were broken, only this one would.
    """

    quarry = next(
        item
        for item in world.EVIDENCE_BY_SLUG.values()
        if item.entity_id == "proj_quarry" and item.state is world.EvidenceState.ACTIVE
    )
    service = EvidenceService(
        entitlement=_AlwaysEntitled(),
        authorizer=CorpusScopeAuthorizer(principal_id=world.PRINCIPAL_COMPLIANCE),
        signer=_signer(),
        native_adapters=(),
        candidate_resolvers=(CorpusCandidateResolver(org_id=ORG),),
    )
    if (
        "proj_quarry"
        not in world.PRINCIPALS[world.PRINCIPAL_COMPLIANCE].visible_entity_ids
    ):
        pytest.skip("no corpus principal is granted the restricted project")
    (only,) = _admit(service, _candidate(quarry.slug)).admissions
    assert only.evidence is not None


def test_a_resolver_that_raises_is_refused_not_propagated() -> None:
    class Exploding:
        source_system = ARM_SOURCE_SYSTEM

        async def resolve(self, *, org_id, scope, candidate):
            raise RuntimeError("the source is down")

    service = EvidenceService(
        entitlement=_AlwaysEntitled(),
        authorizer=CorpusScopeAuthorizer(principal_id=PRINCIPAL),
        signer=_signer(),
        native_adapters=(),
        candidate_resolvers=(Exploding(),),
    )
    (only,) = _admit(service, _candidate("wg_identity_rewrite")).admissions
    assert only.evidence is None
    assert only.state is EvidenceAvailability.UNAVAILABLE
    assert only.warning == "source_unavailable"


def test_a_resolver_cannot_smuggle_an_unauthorized_entity_past_admission() -> None:
    """The planted defect the added entity check exists to catch.

    A resolver that returns a record about an entity outside the grant is a
    resolver bug, and before ``valid_entity_ids`` was narrowed to the
    record's own entity, ``_authorize_expansion`` compared the scope to
    itself and let it through.
    """

    class Lying:
        source_system = ARM_SOURCE_SYSTEM

        async def resolve(self, *, org_id, scope, candidate):
            return EvidenceRecord(
                source_system=ARM_SOURCE_SYSTEM,
                source_version="test",
                entity_type="work_item",
                entity_id="proj_quarry",
                display_label="smuggled",
                observed_at=datetime(2026, 7, 1, tzinfo=UTC),
                freshness=FreshnessState.FRESH,
                provenance="test",
                confidence=1.0,
                internal_path="wg_identity_rewrite",
            )

    service = EvidenceService(
        entitlement=_AlwaysEntitled(),
        authorizer=CorpusScopeAuthorizer(principal_id=PRINCIPAL),
        signer=_signer(),
        native_adapters=(),
        candidate_resolvers=(Lying(),),
    )
    (only,) = _admit(service, _candidate("wg_identity_rewrite")).admissions
    assert only.evidence is None
    assert only.state is EvidenceAvailability.UNAUTHORIZED


def test_one_denied_candidate_does_not_poison_the_batch() -> None:
    result = _admit(
        _service(),
        _candidate("wg_identity_rewrite"),
        EvidenceCandidate(
            source_system=ARM_SOURCE_SYSTEM,
            entity_type="work_item",
            entity_id="proj_identity_rewrite",
            locator="wi_nothing_here",
        ),
        _candidate("wi_atlas_wip"),
    )
    states = [item.state for item in result.admissions]
    assert states == [
        EvidenceAvailability.AVAILABLE,
        EvidenceAvailability.NO_MATCHES,
        EvidenceAvailability.AVAILABLE,
    ]
    assert len(result.admissions) == 3, "every candidate gets a row, admitted or not"


def test_the_round_bound_is_enforced_rather_than_silently_truncating() -> None:
    too_many = tuple(
        _candidate("wg_identity_rewrite") for _ in range(MAX_ADMISSION_CANDIDATES + 1)
    )
    with pytest.raises(ValueError, match="At most"):
        _admit(_service(), *too_many)


# ---------------------------------------------------------------------------
# The frame boundary: crossed, and still biting
# ---------------------------------------------------------------------------


def _measured_case(case_id: str):
    from dev_health_ops.api.dev.investigation_corpus.cases import CASE_REGISTRY

    case = CASE_REGISTRY[case_id]
    seeds = sweep._seeds_for(case.question, spine.projection())
    assert seeds, f"{case_id} must reach a subject for this test to mean anything"
    return case, seeds, spine.readout(seeds)


def test_admission_off_is_rejected_and_admission_on_is_recorded() -> None:
    """The whole point, as one assertion pair over one traversal.

    Same case, same seeds, same readout. The ONLY difference is whether the
    canonical service was asked.
    """

    case, seeds, readout = _measured_case("S01_declared_versus_child_completion")

    without = spine.packet(readout, case, seeds, drivers=True)
    rejected = _evaluate(without, ())
    assert rejected.status is InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED

    _result, admitted = sweep._admit(readout, case.principal_id)
    assert admitted
    with_admission = spine.packet(
        readout, case, seeds, admitted_evidence=admitted, drivers=True
    )
    recorded = _evaluate(with_admission, tuple(admitted.values()))
    assert recorded.status is InvestigationShadowStatus.RECORDED


def test_the_bypass_check_still_bites_on_a_single_altered_field() -> None:
    """Admission is not a way past the check; it is a way to satisfy it.

    One field of one admitted record is altered on its way into the packet's
    copy. The seam must reject the whole packet. Without this, "admission on
    means RECORDED" would be indistinguishable from "the check was disabled".
    """

    case, seeds, readout = _measured_case("S01_declared_versus_child_completion")
    _result, admitted = sweep._admit(readout, case.principal_id)
    locator, ref = next(iter(admitted.items()))
    forged = dict(admitted)
    forged[locator] = ref.model_copy(update={"display_label": "Something else"})

    packet = spine.packet(readout, case, seeds, admitted_evidence=forged, drivers=True)
    # The FRAME still holds the honest refs -- that is what makes this a
    # forgery rather than a rename.
    verdict = _evaluate(packet, tuple(admitted.values()))
    assert verdict.status is InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED
    offenders = canonical_bypass_offenders(
        packet_evidence=[
            entry.evidence for entry in packet.evidence_coverage.evidence_index
        ],
        canonical_evidence=list(admitted.values()),
    )
    assert ref.evidence_ref_id in offenders


def test_an_unadmitted_record_is_dropped_and_never_self_minted() -> None:
    """A locator withheld by the service must not fall back to the arm's mint.

    A fallback would make admission advisory, and an advisory authorization
    boundary is not one. Observed by withholding exactly one admitted ref and
    watching the packet lose that handle rather than gain a different one.
    """

    case, seeds, readout = _measured_case("S01_declared_versus_child_completion")
    _result, admitted = sweep._admit(readout, case.principal_id)
    dropped_locator, dropped_ref = next(iter(admitted.items()))
    narrowed = {k: v for k, v in admitted.items() if k != dropped_locator}

    full = spine.packet(readout, case, seeds, admitted_evidence=admitted, drivers=False)
    partial = spine.packet(
        readout, case, seeds, admitted_evidence=narrowed, drivers=False
    )
    full_handles = {
        entry.evidence.evidence_ref_id
        for entry in full.evidence_coverage.evidence_index
    }
    partial_handles = {
        entry.evidence.evidence_ref_id
        for entry in partial.evidence_coverage.evidence_index
    }
    assert dropped_ref.evidence_ref_id in full_handles
    assert dropped_ref.evidence_ref_id not in partial_handles
    # Dropped, not replaced: no handle appeared that was not already there.
    assert partial_handles < full_handles
    # And it is disclosed rather than silent.
    details = " ".join(
        limitation.detail for limitation in partial.evidence_coverage.limitations
    )
    assert "not admitted by the canonical evidence service" in details


def test_admission_off_reproduces_the_pre_3646_packet_exactly() -> None:
    """The default path is byte-identical to what the merged trial measured.

    A new optional argument that changed the default behaviour would silently
    re-measure CHAOS-3619, and nothing else in this tree would notice.
    """

    case, seeds, readout = _measured_case("S05_multiple_interacting_drivers")
    first = spine.packet(readout, case, seeds, drivers=True)
    second = spine.packet(readout, case, seeds, admitted_evidence=None, drivers=True)
    assert first.model_dump_json() == second.model_dump_json()


def test_with_admission_on_no_handle_comes_from_the_arms_signer() -> None:
    """The arm's key signs nothing that reaches the packet."""

    case, seeds, readout = _measured_case("S05_multiple_interacting_drivers")
    _result, admitted = sweep._admit(readout, case.principal_id)
    packet = spine.packet(
        readout, case, seeds, admitted_evidence=admitted, drivers=True
    )
    admitted_handles = {ref.evidence_ref_id for ref in admitted.values()}
    for entry in packet.evidence_coverage.evidence_index:
        assert entry.evidence.evidence_ref_id in admitted_handles


def _evaluate(packet, canonical_evidence):
    return InvestigationShadow(enabled=True).evaluate(
        payload=json.loads(packet.model_dump_json()),
        run_id=packet.versions.trial.run_id,
        organization_id=packet.organization_id,
        canonical_evidence=tuple(
            DevEvidenceRefV2.model_validate(ref.model_dump())
            for ref in canonical_evidence
        ),
    )


class _AlwaysEntitled:
    async def require(self, org_id: str) -> None:
        return None


def _signer():
    from trials.chaos_3646.canonical import CorpusEvidenceSigner

    return CorpusEvidenceSigner(SECRET)


# ---------------------------------------------------------------------------
# The rendered document cannot drift away from the records
# ---------------------------------------------------------------------------


def test_the_rendered_result_agrees_with_the_records() -> None:
    """Every headline figure in ``trials/chaos_3646/README.md``, re-derived.

    A rendered report and the raw records it was written from are two copies
    of one claim, and the copy a reader sees is the one nobody re-runs. Each
    assertion below fails loudly if a later sweep moves the number without
    the prose following it.
    """

    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    records = json.loads(
        (root / "trials/chaos_3646/results/admission-records.json").read_text()
    )
    rendered = (root / "trials/chaos_3646/README.md").read_text()

    off = [
        leg
        for case in records["cases"]
        for leg in case["legs"]
        if leg["admission"] == "off"
    ]
    on = [
        leg
        for case in records["cases"]
        for leg in case["legs"]
        if leg["admission"] == "on"
    ]

    recorded_on = sum(1 for leg in on if leg["seam_status"] == "recorded")
    rejected_off = sum(
        1 for leg in off if leg["seam_status"] == "canonical_bypass_rejected"
    )
    recorded_off = sum(1 for leg in off if leg["seam_status"] == "recorded")
    rejected_on = sum(
        1 for leg in on if leg["seam_status"] == "canonical_bypass_rejected"
    )
    faults = sum(1 for leg in on if leg["seam_status"] == "arm_fault")
    not_reached = sum(1 for case in records["cases"] if case["outcome"] != "measured")

    # The result table.
    assert recorded_off == 0 and rejected_on == 0
    assert f"| **{rejected_off}** | **{rejected_on}** |" in rendered
    assert f"| **{recorded_off}** | **{recorded_on}** |" in rendered
    assert faults == 1

    # The usefulness line, which is the ticket's Done condition.
    usefulness = [
        leg["dimensions"].get("answer_usefulness_beyond_dashboard")
        for leg in on
        if "answer_usefulness_beyond_dashboard" in leg["dimensions"]
    ]
    passes = usefulness.count("pass")
    fails = usefulness.count("fail")
    assert (
        f"**`answer_usefulness_beyond_dashboard` now has a result: {passes} pass, {fails} fail.**"
        in rendered
    )

    # The refusal count, which is a claim about what was NOT exercised.
    total_refusals = sum(sum(leg["refusals"].values()) for leg in on)
    assert total_refusals == 0, (
        "the sweep now records a refusal; the README's 'Nothing, in the "
        "measured sweep' section is no longer true and must be rewritten"
    )

    assert re.search(
        rf"\b{not_reached} of \d+ corpus cases resolve no subject", rendered
    )

    # Every case named in the score table really is in the records, with the
    # verdict the table gives it. A table row for a case the sweep no longer
    # runs would be the worst kind of drift: plausible and unfalsifiable.
    by_case = {
        case["case_id"]: case["legs"][1]["dimensions"]
        for case in records["cases"]
        if case["outcome"] == "measured"
    }
    for line in rendered.splitlines():
        match = re.match(r"^\| `([A-Z]\d\d_[a-z_]+)` \|", line)
        if not match:
            continue
        case_id = match.group(1)
        assert case_id in by_case, f"{case_id} is in the table but not in the records"
        cells = [cell.strip() for cell in line.strip("|").split("|")[1:]]
        for column, cell in zip(
            (
                "answer_usefulness_beyond_dashboard",
                "principal_driver_precision",
                "principal_driver_recall",
                "symptom_versus_driver_distinction",
            ),
            cells,
            strict=True,
        ):
            recorded_verdict = by_case[case_id].get(column)
            spelled = {
                "**PASS**": "pass",
                "fail": "fail",
                "not applicable": "not_applicable",
                "no oracle": "no_oracle",
                "arm fault": "arm_fault",
                "—": None,
            }[cell]
            assert recorded_verdict == spelled, (
                f"{case_id}/{column}: the table says {cell!r}, the records "
                f"say {recorded_verdict!r}"
            )
