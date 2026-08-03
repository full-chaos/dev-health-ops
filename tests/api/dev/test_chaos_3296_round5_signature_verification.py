"""Round-5/6 closure (CHAOS-3296 Codex rounds 4-5, 2026-08-02): recomputed-
signature verification, an authorization-scope snapshot, and PROGRAMMATIC
content-projection binding -- replacing the per-run receipt comparison
rounds 1-4 built, and the hand-picked claim-field lists round 5 built.

Round 4's closure (the evidence identity table + require-known-good) still
compared a re-derived SUBSET of a handle's identity -- (source_system,
source_version, entity_type, entity_id) -- against a per-run RECEIPT this
executor maintained itself:

1. [HIGH] The receipt never recorded org_id/repository_ids, though the real
   ``EvidenceReferenceSigner``'s HMAC binds both. A handle genuinely minted
   for a DIFFERENT tenant/repository scope verified clean.
2. [HIGH] No round bound a fact's own asserted CONTENT at all.

Round 5 replaced the receipt with recomputed-signature verification
(``EvidenceReferenceSigner.verify``, never re-implemented) and folded a
content digest into ``source_version`` -- but round 5's own content binding
used HAND-PICKED claim-field lists, and its ambient-scope binding read
``context.scope`` fresh at verify time. Codex round 5 review confirmed 3
MORE findings, closed here:

1. [HIGH] ``DevScope`` is frozen, but its ``repositories``/``team_ids``
   fields are mutable LIST values -- a step that mutated
   ``context.scope.repositories`` in place mid-run, then minted against its
   own self-chosen value, verified clean (verification re-read the SAME
   mutated list AFTER the step had already run). And a TEAM-scoped
   investigation has an EMPTY ``repositories`` with ``team_ids`` never
   bound at all, so two DIFFERENT teams' investigations minted identical
   handles for identical content. Fixed: ``PlanExecutor.run`` snapshots a
   deeply immutable ``_AuthorizationScope`` (direct_scope/team_ids/
   entity_ids/repositories) BEFORE any step runs; verification uses ONLY
   this snapshot, never a fresh ``context.scope`` read; its team_ids/
   direct_scope/entity_ids digest is folded into every minted
   ``source_version`` via ``builtin_steps._scope_bound_mint``.
2. [HIGH] ``metric_refs``' hand-picked claim list bound only value/
   comparison_value -- Codex re-labeled a metric to ``avg_wip`` with a
   forged 999-point series and it verified.
3. [HIGH] ``graph_edges``' hand-picked claim list omitted provenance/
   confidence/observed_at -- Codex forged all three on a genuine handle.

Fixed generally, not per-category: ``builtin_steps._claim_projection``
derives a fact's bound claim fields PROGRAMMATICALLY from the wire model's
own ``model_fields`` (minus schema_version/evidence_ref_ids) -- a future
field addition is bound automatically, never silently missed the way a
hand-picked list can be. Every ``wire_*`` helper now constructs its wire
model FIRST (evidence_ref_ids still empty), projects, mints, then patches
in the real handle -- the digest reflects EXACTLY what the wire presents.
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
    MetricID,
)
from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import (
    DevCIFactV2,
    DevGraphEdgeV2,
    DevMetricRefV2,
    DevScopeV2,
    DevStatusFactV2,
    MetricEvidenceClassification,
)
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
    _GRAPH_EVIDENCE_SOURCE_VERSION,
    _METRIC_EVIDENCE_SOURCE_VERSION,
    _STATUS_EVIDENCE_SOURCE_VERSION,
    _bind_content,
    _ci_check_source_version,
    _claim_projection,
    _metric_ref_id,
)
from dev_health_ops.api.dev.investigation_plans.executor import _CandidateIdentity
from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
    _identity_metric_ref,
)
from tests._chaos_3295_plan_executor import (
    TEST_EVIDENCE_SIGNER,
    sign_evidence_for_scope,
)

ORG_ID = "org_fullchaos"
OTHER_ORG_ID = "org_intruder"
ROOT_ENTITY_ID = "project-1"
OBSERVED_AT = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


def _now() -> datetime:
    return OBSERVED_AT


def _time_range() -> DevTimeRange:
    return DevTimeRange(
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 31, tzinfo=UTC),
        timezone="UTC",
    )


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
        time_range=_time_range(),
    )


def _team_scope(team_id: str) -> DevScope:
    """A DirectScope.TEAM scope -- ``repositories`` must be empty (a team
    direct scope has no repository list of its own; see ``DevScope.
    validate_direct_scope``), which is exactly why ``team_ids`` needs its
    own binding independent of ``repositories``."""

    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=ORG_ID,
        direct_scope=DirectScope.TEAM,
        team_ids=[team_id],
        entity_refs=[
            {
                "entity_type": "team",
                "entity_id": team_id,
                "display_label": team_id,
                "repository_id": None,
            }
        ],
        time_range=_time_range(),
    )


def _context(*, scope: DevScope | None = None) -> StepContext:
    return StepContext(
        org_id=ORG_ID,
        permission_fingerprint="fingerprint",
        scope=scope or _scope(),
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


def _ci_claim(*, entity_id: str, conclusion: str) -> dict[str, object]:
    """The exact projection a genuine ``wire_ci`` mint would compute for a
    CI fact with this entity_id/conclusion (the rest of ``_ci_fact``'s
    fields are fixed) -- built from the SAME provisional-then-project
    pattern production uses, never a hand-picked subset. ``evidence_ref_ids``
    stays empty (never ``("",)`` -- ``OpaqueID`` rejects an empty string):
    :func:`_claim_projection` excludes it from the digest regardless, so its
    placeholder value here can never matter."""

    provisional = DevCIFactV2(
        entity_id=entity_id,
        display_label="build",
        conclusion=conclusion,
        required=True,
        skipped_required_work=False,
        observed_at=OBSERVED_AT,
        evidence_ref_ids=(),
    )
    return _claim_projection(provisional)


# -- round-5 finding 1: cross-tenant / cross-repository receipt -------------


@pytest.mark.asyncio
async def test_red_cross_tenant_handle_is_rejected():
    """RED (Codex round 4/5, [HIGH]): a handle genuinely minted for a
    DIFFERENT organization than the one running this step verifies clean
    under a receipt comparison that never carried org_id at all -- round 5
    supplies org_id fresh from the CURRENT ``StepContext`` on every check,
    never from the handle or the fact citing it, so this can no longer
    happen."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        # Minted for OTHER_ORG_ID -- a different tenant than the one this
        # step actually runs under (ORG_ID, via ``_context()`` below).
        forged_handle = sign_evidence_for_scope(
            scope=_scope(),
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
    """RED (Codex round 4/5, [HIGH], repository-scope half): a handle
    genuinely minted for a DIFFERENT repository scope than the one this
    step's own ``StepContext`` authorizes."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        forged_handle = sign_evidence_for_scope(
            # Minted against a repository the CURRENT step's scope
            # (``repo-authorized`` below) never authorized.
            scope=_scope(repositories=("repo-unauthorized",)),
            org_id=ORG_ID,
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
        source_class=SourceClass.STATUS_CHANGE,
        run=run,
        context=_context(scope=_scope(repositories=("repo-authorized",))),
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_genuine_same_tenant_and_repository_handle_is_accepted():
    """Positive control: a handle minted for the SAME org and repository
    scope the step actually runs under must still verify."""

    scope = _scope(repositories=("repo-authorized",))

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        handle = sign_evidence_for_scope(
            scope=scope,
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
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
        context=_context(scope=scope),
    )

    assert observation.content is not None
    assert len(observation.content.ci_checks) == 1
    assert result.relationship_closure_verified is True


# -- round-6 finding 1: mutable ambient scope / unbound team_ids ------------


@pytest.mark.asyncio
async def test_red_mid_step_repository_mutation_does_not_verify():
    """RED (Codex round 6, [HIGH]): a step mutates ``context.scope.
    repositories`` IN PLACE mid-step (``DevScope`` is frozen, but the LIST
    its ``repositories`` attribute points to is not), then mints against
    that self-chosen value. Verification must use the snapshot captured
    BEFORE this step ran, never a fresh ``context.scope`` re-read -- so the
    step's own mutation can never make its own forged handle verify."""

    async def run(ctx: StepContext) -> StepOutcome:
        # Attacker-controlled: swap the authorized repo list to one this
        # run was never actually authorized for, AFTER the executor already
        # snapshotted the true scope.
        ctx.scope.repositories.clear()
        ctx.scope.repositories.append("repo-self-chosen")
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        handle = sign_evidence_for_scope(
            scope=ctx.scope,  # the NOW-mutated scope
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
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
        context=_context(scope=_scope(repositories=("repo-authorized",))),
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_red_cross_team_handle_is_rejected():
    """RED (Codex round 6, [HIGH]): a TEAM-scoped investigation has an EMPTY
    ``repositories`` (structurally -- see ``DevScope.validate_direct_scope``)
    -- a handle genuinely minted under a DIFFERENT team's authorization
    scope must not verify under this team's, even though ``repositories``
    is identically empty for both and could never itself distinguish them."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        forged_handle = sign_evidence_for_scope(
            scope=_team_scope("team-alpha"),  # a DIFFERENT team than team-beta
            org_id=ORG_ID,
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
        source_class=SourceClass.STATUS_CHANGE,
        run=run,
        context=_context(scope=_team_scope("team-beta")),
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


@pytest.mark.asyncio
async def test_genuine_team_scoped_handle_is_accepted():
    """Positive control: a handle minted under a TEAM scope, cited within
    the SAME team's investigation, must still verify -- round 6 must not
    over-reject legitimate team-scoped evidence."""

    scope = _team_scope("team-beta")

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        handle = sign_evidence_for_scope(
            scope=scope,
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
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
        context=_context(scope=scope),
    )

    assert observation.content is not None
    assert len(observation.content.ci_checks) == 1
    assert result.relationship_closure_verified is True


# -- round-6 finding 4: relationship-path rooting used a live context.scope
# -- read, not the pre-step snapshot ----------------------------------------


FOREIGN_ENTITY_ID = "project-foreign"


@pytest.mark.asyncio
async def test_red_post_mint_entity_swap_does_not_re_root_relationship_paths():
    """RED (Codex round 6, [HIGH], the one finding that survived the rest of
    round 6): a step mints its evidence honestly against the REAL committed
    scope, then -- AFTER minting, still within the same step -- mutates
    ``context.scope.entity_refs`` in place to point at a project this
    investigation was never scoped to. ``_mint_relationship_paths`` ran
    AFTER every step, so it used to call ``_root_entity_id(context.scope)``
    and see the step's own mutation: every relationship path ended up
    rooted at (``source_entity_id=``) the FOREIGN entity, with
    ``relationship_closure_verified`` still reporting True and
    ``result.subject_entity_id`` (passed in from outside, never re-derived)
    the only field still showing the truth. Rooting from the pre-step
    ``_AuthorizationScope`` snapshot instead means the mutation can no
    longer reach relationship-path construction at all: every minted path
    must still be rooted at the ORIGINAL committed entity."""

    async def run(ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        # Minted honestly, BEFORE the mutation below, against the scope this
        # run was actually authorized under -- the evidence signature itself
        # is completely genuine.
        handle = sign_evidence_for_scope(
            scope=ctx.scope,
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        fact = _ci_fact(
            entity_id="repo#ci7#checkA", conclusion="success", handle=handle
        )
        # Attacker-controlled: swap the committed subject to a foreign
        # project AFTER minting, but still before this step returns -- the
        # executor already snapshotted the true scope before any step ran.
        ctx.scope.entity_refs[0] = ctx.scope.entity_refs[0].model_copy(
            update={"entity_id": FOREIGN_ENTITY_ID}
        )
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", ci_checks=(fact,))
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert observation.content is not None
    assert len(observation.relationship_paths) == 1
    path = observation.relationship_paths[0]
    # The load-bearing assertion: rooted at the ORIGINAL committed entity,
    # never the post-mint swap -- before the fix this was FOREIGN_ENTITY_ID.
    assert path.source_entity_id == ROOT_ENTITY_ID
    assert path.source_entity_id != FOREIGN_ENTITY_ID
    assert result.relationship_closure_verified is True
    assert result.subject_entity_id == ROOT_ENTITY_ID


@pytest.mark.asyncio
async def test_genuine_no_mutation_relationship_paths_still_root_correctly():
    """Positive control: with no mid-step mutation at all, relationship
    paths must still root at the real committed entity -- round 6's fix
    must not leave every path unrooted (``root_entity_id is None``)."""

    async def run(_ctx: StepContext) -> StepOutcome:
        source_version = _ci_check_source_version(
            "repo#ci7#checkA",
            claim=_ci_claim(entity_id="repo#ci7#checkA", conclusion="success"),
        )
        handle = sign_evidence_for_scope(
            scope=_scope(),
            org_id=ORG_ID,
            source_system="ci_runs",
            source_version=source_version,
            entity_type="ci_run",
            entity_id="repo#ci7",
            display_label="checkA",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        fact = _ci_fact(
            entity_id="repo#ci7#checkA", conclusion="success", handle=handle
        )
        return _queried_outcome(
            DevSourceContent(schema_version="dev_source_content.v1", ci_checks=(fact,))
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.STATUS_CHANGE, run=run
    )

    assert len(observation.relationship_paths) == 1
    assert observation.relationship_paths[0].source_entity_id == ROOT_ENTITY_ID
    assert result.relationship_closure_verified is True


# -- generalized (round-6, programmatic) content binding: representative REDs


@pytest.mark.asyncio
async def test_red_status_fact_content_swap_is_rejected():
    """RED: a handle genuinely minted for a status fact asserting
    "in_progress" reused verbatim on a fabricated fact for the SAME entity
    claiming "done" instead."""

    scope = _scope()

    async def run(_ctx: StepContext) -> StepOutcome:
        # DevStatusFactV2.evidence_ref_ids carries min_length=1 at the
        # contract layer -- a placeholder handle-shaped string, discarded
        # by _claim_projection's exclusion regardless of its value.
        genuine = DevStatusFactV2(
            fact_id="issue:issue-1",
            text="Issue One: in_progress",
            evidence_ref_ids=("ev1_" + "0" * 40,),
        )
        source_version = _bind_content(
            _STATUS_EVIDENCE_SOURCE_VERSION, _claim_projection(genuine)
        )
        handle = sign_evidence_for_scope(
            scope=scope,
            org_id=ORG_ID,
            source_system="work_items",
            source_version=source_version,
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_fact = genuine.model_copy(update={"evidence_ref_ids": (handle,)})
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
        source_class=SourceClass.STATUS_CHANGE, run=run, context=_context(scope=scope)
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
    the SAME entity/change_id claiming a DIFFERENT before/after pair."""

    scope = _scope()

    async def run(_ctx: StepContext) -> StepOutcome:
        genuine = DevObservedChangeV2(
            change_id="change-1",
            category="entity",
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One closed",
            before="open",
            after="closed",
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
        source_version = _bind_content(
            _CHANGE_EVIDENCE_SOURCE_VERSION, _claim_projection(genuine)
        )
        handle = sign_evidence_for_scope(
            scope=scope,
            org_id=ORG_ID,
            source_system="work_items",
            source_version=source_version,
            entity_type="issue",
            entity_id="issue-1",
            display_label="Issue One closed",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_change = genuine.model_copy(update={"evidence_ref_ids": (handle,)})
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
        source_class=SourceClass.STATUS_CHANGE, run=run, context=_context(scope=scope)
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


def _metric_fact(
    *,
    metric_id: MetricID,
    dimensions: tuple[str, ...] = (),
    value: float,
    series: tuple[tuple[str, float], ...] = (),
) -> DevMetricRefV2:
    window = _time_range()
    scope_v2 = DevScopeV2.model_validate(_scope().model_dump(mode="json"))
    metric_ref_id = _metric_ref_id(
        metric_id=metric_id.value,
        dimensions=dimensions,
        window_start=window.start.isoformat(),
        window_end=window.end.isoformat(),
    )
    return DevMetricRefV2(
        schema_version="dev_metric_ref.v1",
        metric_ref_id=metric_ref_id,
        metric_id=metric_id,
        label="Cycle time",
        definition_version="v1",
        unit="hours",
        aggregation="p50",
        display_precision=1,
        resolved_scope=scope_v2,
        dimensions=dimensions,
        current_window=window,
        comparison_window=None,
        value=value,
        comparison_value=None,
        series=tuple({"timestamp": ts, "value": v} for ts, v in series),
        query_version="v1",
        source_version="v1",
        freshness=FreshnessState.FRESH,
        coverage=1.0,
        evidence_ref_ids=(),
        # F10 (CHAOS-3297 stack #3): a bootstrapping placeholder satisfying
        # the evidence-XOR-classification requirement at construction time,
        # exactly like builtin_steps.py's own provisional-then-mint pattern
        # -- every call site below that later attaches real evidence via
        # model_copy must ALSO clear this in the SAME update (model_copy
        # never revalidates, so leaving it set would silently produce an
        # invalid wire object nothing catches).
        evidence_classification=MetricEvidenceClassification.LEGACY_V1_UNMINTED,
    )


@pytest.mark.asyncio
async def test_red_metric_full_field_swap_is_rejected():
    """RED (Codex round 6, [HIGH]): round 5 bound only value/
    comparison_value -- a genuine handle for CYCLE_TIME_P50_HOURS=10.0
    verified a fabricated fact re-labeled AVG_WIP with a forged dense
    series, because neither metric_id nor series were bound."""

    scope = _scope()

    async def run(_ctx: StepContext) -> StepOutcome:
        genuine = _metric_fact(metric_id=MetricID.CYCLE_TIME_P50_HOURS, value=10.0)
        source_version = _bind_content(
            _METRIC_EVIDENCE_SOURCE_VERSION, _claim_projection(genuine)
        )
        handle = sign_evidence_for_scope(
            scope=scope,
            org_id=ORG_ID,
            source_system="metrics",
            source_version=source_version,
            entity_type="metric",
            entity_id=genuine.metric_ref_id,
            display_label="Cycle time",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_ref = genuine.model_copy(
            update={"evidence_ref_ids": (handle,), "evidence_classification": None}
        )
        forged_ref = genuine.model_copy(
            update={
                "metric_id": MetricID.AVG_WIP,
                "series": tuple(
                    {"timestamp": OBSERVED_AT, "value": float(i)} for i in range(999)
                ),
                "evidence_ref_ids": (handle,),
                "evidence_classification": None,
            }
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                metric_refs=(genuine_ref, forged_ref),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_ITEM, run=run, context=_context(scope=scope)
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


def test_metric_ref_id_is_recomputed_never_trusted_from_the_fact() -> None:
    """Structural proof (round-6 ask: "recompute/validate metric_ref_id
    from the same inputs"): two ``DevMetricRefV2`` facts sharing the
    IDENTICAL (spoofed) ``metric_ref_id`` field but different
    ``metric_id``s must derive DIFFERENT entity_ids -- ``_identity_
    metric_ref`` never trusts ``ref.metric_ref_id`` as a free-form id, it
    is always recomputed from the fact's own metric_id/dimensions/window."""

    a = _metric_fact(metric_id=MetricID.CYCLE_TIME_P50_HOURS, value=1.0)
    spoofed = a.model_copy(update={"metric_id": MetricID.AVG_WIP})
    assert spoofed.metric_ref_id == a.metric_ref_id  # same spoofed field value

    _source_a, _sv_a, _et_a, entity_id_a = _identity_metric_ref(a)
    _source_b, _sv_b, _et_b, entity_id_b = _identity_metric_ref(spoofed)
    assert entity_id_a != entity_id_b


@pytest.mark.asyncio
async def test_red_graph_edge_provenance_confidence_timestamp_swap_is_rejected():
    """RED (Codex round 6, [HIGH]): round 5 bound only relationship/
    source_entity_id/target_entity_id -- a genuine handle verified a fact
    with a forged provenance ("fabricated-authoritative-source"), a forged
    confidence, and a year-2036 observed_at."""

    scope = _scope()

    async def run(_ctx: StepContext) -> StepOutcome:
        genuine = DevGraphEdgeV2(
            edge_id="edge-1",
            source_entity_id=ROOT_ENTITY_ID,
            relationship="references",
            target_entity_id="pr-1",
            provenance="ci_pipeline",
            confidence=0.8,
            observed_at=OBSERVED_AT,
            evidence_ref_ids=(),
        )
        source_version = _bind_content(
            _GRAPH_EVIDENCE_SOURCE_VERSION, _claim_projection(genuine)
        )
        handle = sign_evidence_for_scope(
            scope=scope,
            org_id=ORG_ID,
            source_system="work_graph",
            source_version=source_version,
            entity_type="work_graph_edge",
            entity_id="edge-1",
            display_label="issue-1 references pr-1",
            observed_at=OBSERVED_AT,
            freshness=FreshnessState.FRESH,
        )
        genuine_edge = genuine.model_copy(update={"evidence_ref_ids": (handle,)})
        forged_edge = genuine.model_copy(
            update={
                "provenance": "fabricated-authoritative-source",
                "confidence": 1.0,
                "observed_at": datetime(2036, 1, 1, tzinfo=UTC),
                "evidence_ref_ids": (handle,),
            }
        )
        return _queried_outcome(
            DevSourceContent(
                schema_version="dev_source_content.v1",
                graph_edges=(genuine_edge, forged_edge),
            )
        )

    result, observation = await _run_single_step(
        source_class=SourceClass.WORK_GRAPH, run=run, context=_context(scope=scope)
    )

    assert observation.content is None
    assert observation.observed_state is SourceRequirementState.UNAVAILABLE
    assert observation.limitation is not None
    assert observation.limitation.startswith("evidence_signature_invalid:")
    assert result.relationship_closure_verified is False


# -- drift guard --------------------------------------------------------


def test_content_binding_is_shared_by_identity_between_every_mint_and_verify_site():
    """Structural drift guard: EVERY category's mint (``builtin_steps.py``'s
    ``wire_*``/mint closures) and verify (``relationship_matrix.py``'s
    ``EVIDENCE_IDENTITY_TABLE`` cells) fold content into ``source_version``
    through the LITERAL SAME shared functions -- asserted by identity, not
    merely by matching behavior."""

    assert (
        relationship_matrix_module._bind_content is builtin_steps_module._bind_content
    )
    assert (
        relationship_matrix_module._claim_projection
        is builtin_steps_module._claim_projection
    )
    assert (
        relationship_matrix_module._ci_check_source_version
        is builtin_steps_module._ci_check_source_version
    )
    assert (
        relationship_matrix_module._metric_ref_id is builtin_steps_module._metric_ref_id
    )


def test_signer_payload_drift_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutation-style drift guard: if ``EvidenceReferenceSigner``'s payload
    construction ever binds a field at MINT time that the verifier's
    rebuilt candidate does not (or cannot) supply, the result must be
    REJECTION, never a silent accept."""

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
    ``EvidenceReferenceSigner._payload`` produces today."""

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
