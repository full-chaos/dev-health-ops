"""Round-5 closure (CHAOS-3296 Codex round 4, 2026-08-02): recomputed-
signature verification, replacing the per-run receipt comparison rounds
1-4 all built.

Round 4's closure (the evidence identity table + require-known-good) still
compared a re-derived SUBSET of a handle's identity -- (source_system,
source_version, entity_type, entity_id) -- against a per-run RECEIPT this
executor maintained itself. An adversary only had to vary whatever the
receipt didn't carry:

1. [HIGH] The receipt never recorded org_id/repository_ids, though the real
   ``EvidenceReferenceSigner``'s HMAC binds both. A handle genuinely minted
   for a DIFFERENT tenant/repository scope verified clean as long as the
   remaining four fields happened to match.
2. [HIGH] No round bound the fact's own asserted CONTENT at all. A genuine
   handle for a failing CI check verified a fabricated fact claiming it
   passed.

The fix is a structural inversion, not a wider receipt: verification
recomputes the ACTUAL signature via ``EvidenceReferenceSigner.verify`` --
the exact code every real mint call already goes through -- from (a) the
fact's own re-derived identity and (b) the CURRENT ``StepContext``'s
already-authorized org_id/repositories, never from anything carried by the
fact or the handle being checked.

Content binding (finding 2) was NOT scoped to ``ci_checks`` -- a provisional
first pass claimed it was the sole category with a collision-prone identity,
but the underlying gap is general: ANY category whose minted identity tuple
does not already uniquely determine its content lets a genuinely-minted
handle for one claim "verify" a fabricated, different claim about the same
identity (a status fact citing a real handle while asserting a DIFFERENT
status than what was observed at mint; a deployment/incident/PR the same
way; an observed change with swapped before/after; a graph edge with a
forged relationship/orientation; a metric with a fabricated value). Every
``builtin_steps.py`` ``wire_*``/mint call site now folds a digest of that
category's own claim fields -- the exact fields that survive onto its
``DevSourceContent`` wire type -- into the minted ``source_version`` via ONE
shared ``builtin_steps._bind_content``, and every
``relationship_matrix.EVIDENCE_IDENTITY_TABLE`` cell's ``derive`` recomputes
the SAME claim fields from the wire fact and calls the SAME function. Two
facts differing only in their claim mint different, non-interchangeable
handles; there is no receipt to bind content to, because there is no
receipt.

This file:

* Proves both round-4 findings closed (cross-tenant/cross-repository
  handles; content-swapped CI claims) -- the checkA/checkB content-swap
  repro and the three round-1/2/3 repros (evidence-free fact, graph-edge
  reuse, checkA-on-checkB run-level coarsening) already live as permanent
  REDs in ``test_chaos_3296_round4_evidence_identity_table.py`` and
  ``test_chaos_3296_round2_budget_and_receipts.py``, updated in the same
  commit as this file to mint through the real signer and assert the new
  ``evidence_signature_invalid:`` limitation -- not duplicated here.
* Adds the two NEW round-4 findings this round closes (cross-tenant,
  cross-repository-scope) as permanent REDs.
* Adds representative content-swap REDs for two NON-ci_checks categories
  (status_facts, observed_changes) proving the generalized closure -- not
  exhaustive over all 9 categories (that would duplicate
  ``test_evidence_identity_table_is_total_over_content_slot_fields`` and the
  source-anchored per-cell tests in the round-4 file, which already prove
  every cell's ``derive`` matches its real mint call site, content fields
  included), but enough to demonstrate the pattern holds outside CI.
* Adds the drift guard: a structural identity assertion that every
  category's mint and verify call the literal same shared content-digest
  function, plus a mutation test proving that if
  ``EvidenceReferenceSigner``'s payload construction ever drifts between
  mint and verify time, the result is REJECTION (fail-closed), never a
  silent accept.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import (
    DevScope,
    DevTimeRange,
    DirectScope,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import DevCIFactV2, DevStatusFactV2
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.contracts_v2.result import (
    DevObservedChangeV2,
    DevSourceContent,
)
from dev_health_ops.api.dev.evidence_service import (
    EvidenceRecord,
    EvidenceReferenceSigner,
)
from dev_health_ops.api.dev.investigation_plans import (
    PlanExecutor,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)
from dev_health_ops.api.dev.investigation_plans import (
    builtin_steps as builtin_steps_module,
)
from dev_health_ops.api.dev.investigation_plans import (
    relationship_matrix as relationship_matrix_module,
)
from dev_health_ops.api.dev.investigation_plans.builtin_steps import (
    _CHANGE_EVIDENCE_SOURCE_VERSION,
    _STATUS_EVIDENCE_SOURCE_VERSION,
    _bind_content,
    _ci_check_source_version,
)
from dev_health_ops.api.dev.investigation_plans.executor import _CandidateIdentity
from tests._chaos_3295_plan_executor import TEST_EVIDENCE_SIGNER, sign_evidence

ORG_ID = "org_fullchaos"
OTHER_ORG_ID = "org_intruder"
ROOT_ENTITY_ID = "project-1"
OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


def _now() -> datetime:
    return OBSERVED_AT


def _scope(*, repositories: tuple[str, ...] = ()) -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.PROJECT,
        repositories=list(repositories),
        entity_refs=[
            {
                "entity_type": "project",
                "entity_id": ROOT_ENTITY_ID,
                "display_label": "Project One",
                "repository_id": None,
            }
        ],
        time_range=DevTimeRange(
            start=datetime(2026, 7, 1, tzinfo=UTC),
            end=datetime(2026, 7, 31, tzinfo=UTC),
            timezone="UTC",
        ),
    )


def _context(*, repositories: tuple[str, ...] = ()) -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=_scope(repositories=repositories),
        run_id="run-1",
        now=_now(),
    )


def _plan(source_class: SourceClass) -> DevInvestigationPlan:
    return DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="status.entity.v2",
        plan_version="status.entity.v2.1",
        intent_id=QuestionIntentID.ENTITY_STATUS,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
        mandatory_steps=("one",),
        conditional_steps=(),
        step_dependencies=(),
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=source_class,
                adapter_id="test.one.v1",
                requirement_level="mandatory",
                freshness_policy="p.v1",
                minimum_usable_facts=0,
            ),
        ),
        batch_strategy="single",
        per_step_timeout_seconds=5,
        max_rows_per_step=10,
        max_bytes_per_step=1_000,
        enrichment_allowed=False,
        completion_rule_id="test.rule",
        completion_rule_version="1",
    )


async def _run_single_step(
    *, source_class: SourceClass, run, context: StepContext | None = None
):
    plan = _plan(source_class)
    registry = StepRegistry()
    registry.register(
        PlanStepDefinition(
            step_id="one",
            plan_id=plan.plan_id,
            source_class=source_class,
            adapter_id="test.one.v1",
            requirement_level="mandatory",
            run=run,
        )
    )
    executor = PlanExecutor(
        registry=registry, now=_now, evidence_signer=TEST_EVIDENCE_SIGNER
    )
    result = await executor.run(
        plan=plan,
        context=context or _context(),
        run_id="run-1",
        subject_entity_id=ROOT_ENTITY_ID,
    )
    assert len(result.observations) == 1
    return result, result.observations[0]


def _queried_outcome(content: DevSourceContent) -> StepOutcome:
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics="measured_zero",
        usable_fact_count=1,
        content=content,
    )


def _ci_fact(*, entity_id: str, conclusion: str, handle: str) -> DevCIFactV2:
    return DevCIFactV2(
        entity_id=entity_id,
        display_label="build",
        conclusion=conclusion,
        required=True,
        skipped_required_work=False,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=(handle,),
    )


# -- round-4 finding 1: cross-tenant / cross-repository receipt -------------


@pytest.mark.asyncio
async def test_red_cross_tenant_handle_is_rejected():
    """RED (Codex round 4, [HIGH]): a handle genuinely minted for a
    DIFFERENT organization than the one running this step verifies clean
    under a receipt comparison that never carried org_id at all -- round 5
    supplies org_id fresh from the CURRENT ``StepContext`` on every check,
    never from the handle or the fact citing it, so this can no longer
    happen."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            conclusion="success",
            required=True,
            skipped_required_work=False,
        )
        # Minted for OTHER_ORG_ID -- a different tenant than the one this
        # step actually runs under (ORG_ID, via ``_context()`` below).
        forged_handle = sign_evidence(
            org_id=OTHER_ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        fact = _ci_fact(
            entity_id="repo#ci7#checkA", conclusion="success", handle=forged_handle
        )
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", ci_checks=(fact,))
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_red_cross_repository_scope_handle_is_rejected():
    """RED (Codex round 4, [HIGH], repository-scope half): a handle
    genuinely minted for a DIFFERENT repository scope than the one this
    step's own ``StepContext`` authorizes. ``repository_ids`` is bound into
    the real signer's HMAC exactly like ``org_id`` -- round 5 supplies it
    fresh from ``context.scope.repositories`` on every check, never from the
    handle."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            conclusion="success",
            required=True,
            skipped_required_work=False,
        )
        forged_handle = sign_evidence(
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
            # Minted against a repository the CURRENT step's scope
            # (``repo-authorized`` below) never authorized.
            repository_ids=("repo-unauthorized",),
        )
        fact = _ci_fact(
            entity_id="repo#ci7#checkA", conclusion="success", handle=forged_handle
        )
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", ci_checks=(fact,))
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE,
        run=run,
        context=_context(repositories=("repo-authorized",)),
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_genuine_same_tenant_and_repository_handle_is_accepted():
    """Positive control: a handle minted for the SAME org and repository
    scope the step actually runs under must still verify -- round 5 must
    not over-reject legitimate same-tenant evidence."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            conclusion="success",
            required=True,
            skipped_required_work=False,
        )
        handle = sign_evidence(
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
            repository_ids=("repo-authorized",),
        )
        fact = _ci_fact(
            entity_id="repo#ci7#checkA", conclusion="success", handle=handle
        )
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", ci_checks=(fact,))
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE,
        run=run,
        context=_context(repositories=("repo-authorized",)),
    )

    assert observation.content is not None
    assert len(observation.content.ci_checks) == 1
    assert result.relationship_closure_verified is True


# -- generalized content binding: representative non-ci_checks REDs ---------


@pytest.mark.asyncio
async def test_red_status_fact_content_swap_is_rejected():
    """RED: a handle genuinely minted for a status fact asserting
    "in_progress" reused verbatim on a fabricated fact for the SAME entity
    claiming "done" instead. Proves the generalized closure holds for
    ``status_facts``, not just ``ci_checks`` -- a real handle for one status
    claim can never verify a fabricated different status claim about the
    same entity."""

    async def run(_ctx: StepContext) -> StepOutcome:
        genuine_text = "Issue One: in_progress"
        handle = sign_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=_bind_content(
                _STATUS_EVIDENCE_SOURCE_VERSION, claim=genuine_text
            ),
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_fact = DevStatusFactV2(
            fact_id="issue:issue-1", text=genuine_text, evidence_ref_ids=(handle,)
        )
        forged_fact = DevStatusFactV2(
            fact_id="issue:issue-1",
            text="Issue One: done (fabricated)",
            evidence_ref_ids=(handle,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                status_facts=(genuine_fact, forged_fact),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_red_observed_change_before_after_swap_is_rejected():
    """RED: a handle genuinely minted for an observed change asserting
    before="open"/after="closed" reused verbatim on a fabricated change for
    the SAME entity/change_id claiming a DIFFERENT before/after pair. Proves
    the generalized closure holds for ``observed_changes``."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _bind_content(
            _CHANGE_EVIDENCE_SOURCE_VERSION, before="open", after="closed"
        )
        handle = sign_evidence(
            org_id=ORG_ID,
            source_system="work_items",
            source_version=source_version,
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One closed",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_change = DevObservedChangeV2(
            change_id="change-1",
            category="entity",
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One closed",
            before="open",
            after="closed",
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle,),
        )
        forged_change = DevObservedChangeV2(
            change_id="change-1",
            category="entity",
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One reopened (fabricated)",
            before="closed",
            after="open",
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(handle,),
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                observed_changes=(genuine_change, forged_change),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


# -- drift guard --------------------------------------------------------


def test_content_binding_is_shared_by_identity_between_every_mint_and_verify_site():
    """Structural drift guard (round-5 item 3): EVERY category's mint
    (``builtin_steps.py``'s ``wire_*``/mint closures) and verify
    (``relationship_matrix.py``'s ``EVIDENCE_IDENTITY_TABLE`` cells) fold
    content into ``source_version`` through the LITERAL SAME shared
    function -- asserted by identity, not merely by matching behavior, so
    no category's mint and verify can independently drift apart the way a
    hand-duplicated constant could."""

    assert (
        relationship_matrix_module._bind_content is builtin_steps_module._bind_content
    )
    assert (
        relationship_matrix_module._ci_check_source_version
        is builtin_steps_module._ci_check_source_version
    )


def test_signer_payload_drift_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutation-style drift guard (round-5 item 3): if
    ``EvidenceReferenceSigner``'s payload construction ever binds a field at
    MINT time that the verifier's rebuilt candidate does not (or cannot)
    supply, the result must be REJECTION, never a silent accept. Simulated
    by minting through a deliberately WIDENED payload (one extra byte folded
    in, standing in for a hypothetical future field the round-5 verifier
    was never updated to rebuild) and then verifying the resulting handle
    through the real, un-widened ``_payload`` -- exactly the shape an
    accidental drift between mint and verify would take. If this test ever
    fails (a widened payload still verifies), the signature check has
    stopped being load-bearing."""

    signer = EvidenceReferenceSigner(b"round5-drift-guard-test-secret-000")
    real_payload = EvidenceReferenceSigner._payload

    def widened_payload(org_id: str, evidence) -> bytes:
        return (
            real_payload(org_id, evidence)
            + hashlib.sha256(b"a-field-the-verifier-does-not-know-about").digest()
        )

    monkeypatch.setattr(
        EvidenceReferenceSigner, "_payload", staticmethod(widened_payload)
    )
    record = EvidenceRecord(
        source_system="ci_runs",
        source_version="status-snapshot-evidence.v1:repo#ci7#checkA",
        entity_type="ci_run",
        entity_id="repo#ci7",
        display_label="checkA",
        observed_at=OBSERVED_AT,
        freshness=FreshnessState.FRESH,
        provenance="ci_runs",
        confidence=1.0,
        repository_ids=(),
    )
    drifted_handle = signer.issue(ORG_ID, record)

    monkeypatch.setattr(EvidenceReferenceSigner, "_payload", staticmethod(real_payload))
    candidate = _CandidateIdentity(
        evidence_ref_id=drifted_handle,
        source_system=record.source_system,
        source_version=record.source_version,
        entity_type=record.entity_type,
        entity_id=record.entity_id,
        repository_ids=record.repository_ids,
    )

    assert signer.verify(ORG_ID, candidate) is False


def test_signer_payload_field_set_is_the_documented_allowlist() -> None:
    """Second half of the drift guard: pin the EXACT set of keys
    ``EvidenceReferenceSigner._payload`` produces today. If a future change
    adds (or removes) a bound field, this fails immediately and loudly --
    the reviewer's cue to check whether ``_CandidateIdentity``/
    ``_evidence_signature_failures`` (``executor.py``) still supply
    everything the signer now binds, rather than discovering the gap only
    when a real forgery slips through."""

    import json as _json

    record = EvidenceRecord(
        source_system="ci_runs",
        source_version="v1",
        entity_type="ci_run",
        entity_id="repo#ci7",
        display_label="checkA",
        observed_at=OBSERVED_AT,
        freshness=FreshnessState.FRESH,
        provenance="ci_runs",
        confidence=1.0,
        repository_ids=("r1",),
    )
    payload_bytes = EvidenceReferenceSigner._payload(ORG_ID, record)
    keys = set(_json.loads(payload_bytes))
    assert keys == {
        "org",
        "source",
        "source_version",
        "entity_type",
        "entity_id",
        "repositories",
    }
