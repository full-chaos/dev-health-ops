"""CHAOS-3567: flag-off scaffold guard -- ``SourceClass.TEMPORAL_CONTEXT``
must be inert.

Adding a ``SourceClass`` member is a real, totality-checked contract change
(``base.py``'s own docstring at ``SourceClass.__doc__``; ``relationship_
matrix.py``'s two import-time completeness checks, ``RELATIONSHIP_MATRIX``
and ``APPROVED_CONTENT_SLOTS``). CHAOS-3567 authorizes exactly that
mechanical addition and NOTHING else -- no plan, no step, no
``DataHealthService`` branch, no wire requirement. Full recognizer/plan/
source wiring stays blocked on the CHAOS-3499 ADR and CHAOS-3500/3501
contracts (Linear project doc, Context Fabric project).

Every assertion below is an EXACT-SET pin, not a one-sided "does not
contain" check: a one-sided check only catches the specific string this
issue happens to add and silently tolerates any other unexpected widening
of the same table. Exact-set pins fail on ANY drift, named or not.

Swept call sites for "every place a source class becomes live" (review
finding, 2026-08-07): every real (non-test, non-fixture) caller of
``DataHealthService.inspect`` in this tree --
``production_runtime.py:1533,2401`` (both pass the literal
``NATIVE_EVIDENCE_SOURCES`` constant, pinned exactly below),
``tools/evidence.py:67`` (a thin passthrough of its own caller's
``required_sources``, itself only ever called with the same constant), and
``graphql/resolvers/dev_evidence.py:182-186`` (the one caller-influenceable
site -- a GraphQL request may pass ``input.required_sources``, but the
resolver rejects anything outside ``{*NATIVE_EVIDENCE_SOURCES, "acr"}``
before calling ``inspect`` at all). Because that allowlist's only two
ingredients are the exactly-pinned ``NATIVE_EVIDENCE_SOURCES`` tuple and the
literal string ``"acr"``, pinning ``NATIVE_EVIDENCE_SOURCES`` exactly (this
module) transitively pins this resolver's allowlist too -- there is no
second, independently-drifting copy of the source list for a request to
reach. ``fixtures/world_verify.py`` is fixture/test tooling, not a request
path, and is out of scope for "live" the same way test files are.

RED-first / fault-injection provenance -- corrected 2026-08-07 after review
(the original docstring here overstated what the single-file plant proved):
see ``scripts/verify_chaos_3567_temporal_context_inertness.py``, a
permanently runnable, committed, in-repo proof (no PR-body-only evidence).
Run it with ``python scripts/verify_chaos_3567_temporal_context_inertness.py``.
It exercises, in fresh subprocesses so the module-level import-time
totality checks below actually fire:

1. Baseline (no plant): this module's tests all pass.
2. Single-file plant -- widening ``NATIVE_EVIDENCE_SOURCES`` alone: the
   ``test_native_evidence_sources_is_exactly_pinned_to_eight_members``
   assertion below fails on its own merits (a clean, isolated
   ``AssertionError`` at the named test).
3. Single-file plant -- widening one registered plan's
   ``source_requirements`` to include ``SourceClass.TEMPORAL_CONTEXT``
   ALONE (leaving ``persistence.service._SOURCE_CLASSES`` untouched): this
   module never reaches collection at all. A DIFFERENT, pre-existing guard
   fires first -- ``wave_3_1_plans.py``'s own CHAOS-3337 import-time
   totality check (``wave_3_1_plans.py:886-895``) raises ``RuntimeError``
   naming the exact injected class, because the plan now emits a
   ``SourceClass`` the persistence allowlist doesn't carry. This is a real,
   valid, even-earlier RED signal (defense in depth), but it is NOT this
   module's own ``test_temporal_context_is_not_referenced_by_any_
   registered_plan`` failing -- claiming that would be exactly the
   "inaccurate coverage claim" this repo's verification discipline forbids.
4. Two-file plant -- widening BOTH the plan's ``source_requirements`` AND
   ``persistence.service._SOURCE_CLASSES`` together: the CHAOS-3337 guard no
   longer fires (the allowlist now accepts the class), so the module
   imports cleanly and execution reaches this module's own
   ``test_temporal_context_is_not_referenced_by_any_registered_plan``
   assertion, which THEN fails on its own merits.
5. Each plant is reverted (in-process only -- no on-disk files are ever
   mutated) and the baseline re-confirmed passing.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
)
from dev_health_ops.api.dev.contracts_v2.plan import (
    DevInvestigationPlan,
    DevSourceRequirement,
)
from dev_health_ops.api.dev.data_health_service import NATIVE_EVIDENCE_SOURCES
from dev_health_ops.api.dev.investigation_plans import (
    PlanStepDefinition,
    StepRegistry,
    validate_registry,
)
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.investigation_plans.registry_validation import (
    StepRequirementMismatchError,
)
from dev_health_ops.api.dev.investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
)

#: Every SourceClass member no registered plan's steps can emit today --
#: pinned EXACTLY (not "temporal_context is somewhere in an unbounded
#: leftover set"), so this test fails if this set ever changes for ANY
#: reason, not only the one this issue adds. Independently re-derived by
#: hand against contracts_v2/base.py's SourceClass member list, not copied
#: from relationship_matrix.py's own "not yet landed" comments -- two
#: independent readings of the same ground truth.
_EXPECTED_UNREFERENCED_SOURCE_CLASSES: frozenset[SourceClass] = frozenset(
    {
        SourceClass.PULL_REQUEST,
        SourceClass.CODE_CHANGE,
        SourceClass.REVIEW,
        SourceClass.CI_RUN,
        SourceClass.TEST_REPORT,
        SourceClass.DEPLOYMENT,
        SourceClass.INCIDENT,
        SourceClass.OPERATIONAL_CONTROL,
        SourceClass.COGNITIVE_LOAD,
        SourceClass.INVESTMENT_ALLOCATION,
        SourceClass.TEMPORAL_CONTEXT,
    }
)


def _every_registered_plan_document():
    """The real, fully-assembled plan registry.

    Same union ``wave_3_1_plans.py``'s own import-time totality check
    (CHAOS-3337) and ``test_chaos_3337_source_class_persistence_
    allowlist.py`` use as "the real registries" -- not a hand-picked subset.
    Confirmed exhaustive, not merely assumed: ``PLAN_REGISTRY``
    (``contracts_v2/plan.py:49-76``) has exactly thirteen members; eleven
    have a real ``DevInvestigationPlan`` document, split exactly between
    ``CORE_PLANS_BY_INTENT`` (six) and ``WAVE_3_1_PLANS_BY_INTENT`` (five);
    the remaining two (``investigation.bounded.v1``,
    ``legacy.tool_choice.v1``) are compatibility markers with no document
    object to scan at all (see the Linear project doc "CHAOS-3567: Ask Dev
    temporal source -- design + registry-impact map", Context Fabric
    project, §2.3, for the full file:line trail). There is no third
    plan-document registry anywhere in this package for this union to miss.
    """

    return {**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT}


def _reachable_source_classes() -> frozenset[SourceClass]:
    """Every ``SourceClass`` any registered plan's steps can emit.

    Independently re-derived from ``plan.source_requirements`` rather than
    imported from ``wave_3_1_plans._source_classes_missing_from_persistence_
    allowlist`` -- this guard must not depend on that helper's internal
    shape staying the same; both reading ``source_requirements`` directly is
    the point (two independent readers of the same ground truth).
    """

    return frozenset(
        requirement.source_class
        for plan in _every_registered_plan_document().values()
        for requirement in plan.source_requirements
    )


def test_temporal_context_is_a_closed_source_class_member() -> None:
    """The one change this issue authorizes: SourceClass gains the member."""

    assert SourceClass.TEMPORAL_CONTEXT == "temporal_context"


def test_native_evidence_sources_is_exactly_pinned_to_eight_members() -> None:
    """The default ``DataHealthService.inspect`` required-sources tuple.

    Exact-tuple pin, not a one-sided "temporal_context is absent" check: any
    addition, removal, or reorder fails this test, not only the specific
    string this issue happens to introduce. This is also the tuple every
    real caller in the tree passes unmodified (see module docstring's call
    site sweep) except the one caller-influenceable GraphQL resolver, whose
    own allowlist is built directly from this same tuple plus the literal
    ``"acr"`` -- pinning this tuple exactly transitively pins that resolver
    too.
    """

    assert NATIVE_EVIDENCE_SOURCES == (
        "work_items",
        "work_units",
        "pull_requests",
        "reviews",
        "commits",
        "ci_runs",
        "deployments",
        "incidents",
    )
    assert "temporal_context" not in NATIVE_EVIDENCE_SOURCES


def test_temporal_context_is_not_referenced_by_any_registered_plan() -> None:
    """No plan's steps can emit content under ``TEMPORAL_CONTEXT`` (yet).

    Exact-set pin against ``_EXPECTED_UNREFERENCED_SOURCE_CLASSES``, not a
    one-sided "TEMPORAL_CONTEXT specifically is absent" check -- this fails
    if the unreferenced set ever changes for ANY reason (a class gets wired
    up, or an unrelated class's plan wiring regresses), not only if this
    one issue's own class gets wired.
    """

    unreferenced = frozenset(SourceClass) - _reachable_source_classes()
    assert unreferenced == _EXPECTED_UNREFERENCED_SOURCE_CLASSES


def test_relationship_matrix_and_content_slots_carry_an_honest_inert_entry() -> None:
    """The two totality-checked ``SourceClass`` tables both import cleanly
    with an entry for ``TEMPORAL_CONTEXT`` -- proving the module-level
    completeness ``RuntimeError`` in ``investigation_plans/
    relationship_matrix.py`` did not need to be (and was not) weakened to
    accept the new member, and that the entry follows the same "not yet
    landed, honest empty vocabulary" posture as every other unwired
    ``SourceClass`` (``CODE_CHANGE``/``REVIEW``/``TEST_REPORT``/
    ``OPERATIONAL_CONTROL``/``COGNITIVE_LOAD``/``INVESTMENT_ALLOCATION``).
    """

    from dev_health_ops.api.dev.investigation_plans.relationship_matrix import (
        APPROVED_CONTENT_SLOTS,
        RELATIONSHIP_MATRIX,
    )

    entry = RELATIONSHIP_MATRIX[SourceClass.TEMPORAL_CONTEXT]
    assert entry.role == "supporting"
    assert entry.requirement == "not_applicable"
    assert entry.approved_relationship_types == frozenset()
    assert entry.evidence_expansion_capability is False
    assert APPROVED_CONTENT_SLOTS[SourceClass.TEMPORAL_CONTEXT] == frozenset()


def test_no_step_can_register_against_temporal_context_without_a_declared_requirement() -> (
    None
):
    """Proves the mechanism, not just asserts its conclusion.

    ``registry_validation.validate_registry`` cross-checks every registered
    ``PlanStepDefinition``'s ``(source_class, adapter_id)`` against the
    owning plan's own declared ``DevSourceRequirement``s
    (``registry_validation.py:118-135``,
    ``StepRequirementMismatchError`` on a mismatch). Because
    ``test_temporal_context_is_not_referenced_by_any_registered_plan``
    above already proves no plan declares a requirement for
    ``TEMPORAL_CONTEXT``, this check makes it structurally impossible for
    any step registered against a real plan to carry
    ``source_class=SourceClass.TEMPORAL_CONTEXT`` -- production only ever
    builds a ``StepRegistry`` through ``build_default_registry``/
    ``build_registry_with_wave_3_1``, both of which call
    ``validate_registry`` before returning. This test exercises that
    rejection directly, with a minimal, self-contained plan + step (the
    same pattern ``test_chaos_3295_investigation_plans_registry.py``'s
    ``test_step_registered_against_the_wrong_adapter_is_rejected`` uses),
    rather than only asserting the conclusion by construction.
    """

    plan = DevInvestigationPlan(
        schema_version="dev_investigation_plan.v1",
        plan_id="investigation.bounded.v1",
        plan_version="investigation.bounded.v1.0",
        intent_id=QuestionIntentID.BOUNDED_INVESTIGATION,
        supported_subject_kinds=(EntityKind.PROJECT,),
        supported_cardinalities=(Cardinality.SINGULAR,),
        mandatory_steps=("temporal_probe",),
        conditional_steps=(),
        step_dependencies=(),
        # Deliberately does NOT declare a requirement for TEMPORAL_CONTEXT --
        # the defect under test is a step registered against a source class
        # its own plan never declared.
        source_requirements=(
            DevSourceRequirement(
                schema_version="dev_source_requirement.v1",
                source_class=SourceClass.STATUS_CHANGE,
                adapter_id="test.probe.v1",
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

    registry = StepRegistry()

    async def run(_ctx):
        raise AssertionError("never executed by this structural test")

    registry.register(
        PlanStepDefinition(
            step_id="temporal_probe",
            plan_id=plan.plan_id,
            source_class=SourceClass.TEMPORAL_CONTEXT,
            adapter_id="temporal_probe.scratch.v1",
            requirement_level="mandatory",
            run=run,
        )
    )

    with pytest.raises(StepRequirementMismatchError):
        validate_registry(
            plans_by_intent={QuestionIntentID.BOUNDED_INVESTIGATION: plan},
            steps=registry,
            core_intents=frozenset({QuestionIntentID.BOUNDED_INVESTIGATION}),
        )
