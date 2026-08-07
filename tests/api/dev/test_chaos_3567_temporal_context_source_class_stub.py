"""CHAOS-3567: flag-off scaffold guard -- ``SourceClass.TEMPORAL_CONTEXT``
must be inert.

Adding a ``SourceClass`` member is a real, totality-checked contract change
(``base.py``'s own docstring at ``SourceClass.__doc__``; ``relationship_
matrix.py``'s two import-time completeness checks, ``RELATIONSHIP_MATRIX``
and ``APPROVED_CONTENT_SLOTS``). CHAOS-3567 authorizes exactly that
mechanical addition and NOTHING else -- no plan, no step, no
``DataHealthService`` branch, no wire requirement. Full recognizer/plan/
source wiring stays blocked on the CHAOS-3499 ADR and CHAOS-3500/3501
contracts (see ``design/CHAOS-3567-ask-dev-temporal-source-design.md``).

This module is the guard proving that claim: ``TEMPORAL_CONTEXT`` must be a
real, closed-vocabulary ``SourceClass`` member, and it must be UNREACHABLE
from every place a source class becomes "live" today -- the default
``DataHealthService`` required-sources tuple, and every registered plan's
declared ``source_requirements``.

RED-first / fault-injection provenance (see the CHAOS-3567 PR body for the
paired RED/GREEN observations, not re-derivable from this file alone):

1. Written BEFORE ``SourceClass.TEMPORAL_CONTEXT`` existed -- this module
   failed to *import* (``AttributeError: TEMPORAL_CONTEXT``), a genuine RED
   with no member to add tests around yet.
2. After adding the member (``contracts_v2/base.py``) and the two
   ``relationship_matrix.py`` completeness entries and regenerating
   ``contracts/ask-dev/v2`` schemas, every test below passes GREEN.
3. To prove this guard actually detects an *enablement* and not merely "the
   enum has the member", the defect each assertion exists to catch was
   planted locally in a scratch (never-committed) diff and reverted after
   observing the failure:
   * temporarily adding ``"temporal_context"`` to
     ``data_health_service.NATIVE_EVIDENCE_SOURCES`` --
     ``test_temporal_context_is_not_a_data_health_required_source`` failed
     RED;
   * temporarily adding a ``SourceClass.TEMPORAL_CONTEXT``
     ``DevSourceRequirement`` to an existing registered plan's
     ``source_requirements`` --
     ``test_temporal_context_is_not_referenced_by_any_registered_plan``
     failed RED.
   Both scratch diffs were reverted and the full module re-run GREEN
   afterward, confirming the restore itself, not just the plant.
"""

from __future__ import annotations

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.data_health_service import NATIVE_EVIDENCE_SOURCES
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.api.dev.investigation_plans.wave_3_1_plans import (
    WAVE_3_1_PLANS_BY_INTENT,
)


def _every_registered_plan_document():
    """The real, fully-assembled plan registry.

    Same union ``wave_3_1_plans.py``'s own import-time totality check
    (CHAOS-3337) and ``test_chaos_3337_source_class_persistence_
    allowlist.py`` use as "the real registries" -- not a hand-picked subset,
    so a plan added anywhere else is still covered by this guard.
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
    assert SourceClass.TEMPORAL_CONTEXT in set(SourceClass)


def test_temporal_context_is_not_a_data_health_required_source() -> None:
    """No live ``DataHealthService.inspect`` call defaults to checking it.

    This is the exact shape CHAOS-3502's amendment authorizes reusing later
    (the ``acr`` precedent, ``data_health_service.py:398-406``,
    ``required=False``) -- but not yet: ``NATIVE_EVIDENCE_SOURCES`` is the
    default ``required_sources`` tuple every caller uses unless it names
    something else explicitly, and nothing in this issue names
    ``"temporal_context"`` anywhere.
    """

    assert "temporal_context" not in NATIVE_EVIDENCE_SOURCES


def test_temporal_context_is_not_referenced_by_any_registered_plan() -> None:
    """No plan's steps can emit content under ``TEMPORAL_CONTEXT`` (yet).

    A ``SourceClass`` member only reaches a real run by being named in a
    ``DevInvestigationPlan.source_requirements`` with a matching registered
    step (``steps.StepRegistry``). Neither exists for ``TEMPORAL_CONTEXT``
    after this issue's changeset.
    """

    assert SourceClass.TEMPORAL_CONTEXT not in _reachable_source_classes()


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
