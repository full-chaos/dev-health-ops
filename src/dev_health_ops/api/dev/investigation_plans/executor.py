"""The bounded plan executor (CHAOS-3295 Amendment TRD v2 §5).

Runs a :class:`~.plan_documents`'s ``DevInvestigationPlan`` deterministically:
independent steps concurrently, dependent steps blocked until every
prerequisite *succeeds*, and a prerequisite that never completes stops only
the steps that actually depend on it -- never the whole plan. This module
never talks to a provider and never chooses which steps to run: that is
entirely the plan document plus each step's own ``applicable`` predicate
(:mod:`.steps`).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from ..contracts import DevScope, DirectScope
from ..contracts_v2.base import SourceClass, SourceRequirementState
from ..contracts_v2.plan import DevInvestigationPlan, DevSourceRequirement
from ..contracts_v2.result import (
    DevInvestigationResult,
    DevRelationshipPath,
    DevSourceContent,
    DevSourceObservation,
)
from .relationship_matrix import MIN_RELATIONSHIP_CONFIDENCE, approved_relationship
from .steps import (
    PlanRegistryError,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)

__all__ = ["PlanExecutionError", "PlanExecutor"]

#: Fixed namespace for every CHAOS-3295-minted id. A constant, not a secret --
#: it exists only so ``uuid5`` output cannot collide with a UUID minted by an
#: unrelated namespace elsewhere in the platform. Determinism (P3, CHAOS-3297
#: dependency): the same ``(run_id, plan_id[, step_id])`` triple always mints
#: the same handle, so two executions over identical inputs produce
#: byte-identical ``DevInvestigationResult`` objects -- required for 3297's
#: frame-purity property (F2) and for any differential oracle over the
#: builder.
_MINT_NAMESPACE = uuid.UUID("f91838e6-6d11-5c43-8fb6-10d72f647684")


def _mint(*parts: str) -> str:
    return str(uuid.uuid5(_MINT_NAMESPACE, ":".join(parts)))


class PlanExecutionError(RuntimeError):
    """Raised only for a structural defect the registry validator should
    already have caught (e.g. a dependency cycle surviving construction).
    Never raised for an ordinary step failure -- those are recorded, not
    thrown.
    """


@dataclass(slots=True)
class _Attempt:
    step_id: str
    outcome: StepOutcome | None
    failed: bool


@dataclass(frozen=True, slots=True)
class _PathCandidate:
    """One un-verified hop extracted from a step's ``DevSourceContent``,
    before relationship-matrix/confidence/self-loop/touching-root checks.

    ``source_entity_id`` is ``None`` for every fact category except
    ``graph_edges``: a status/PR/CI/deployment/incident/change/metric fact
    has no orientation of its own (the canonical service that produced it
    already scoped the query to the committed subject, so it is always
    read as subject -> fact). A work-graph edge is bidirectional by
    construction (``GraphDirection.BOTH``), so its own recorded
    ``source_entity_id``/``target_entity_id`` must be checked against the
    committed root explicitly.
    """

    relationship: str
    target_entity_id: str
    source_entity_id: str | None
    provenance: str
    confidence: float
    observed_at: datetime
    evidence_ref_ids: tuple[str, ...]


def _root_entity_id(scope: DevScope) -> str | None:
    """The committed single subject a relationship path closes over, or
    ``None`` when there is none to close over (organization/repository
    scope legitimately shows broad facts -- PRD v2 §3.2/§7)."""

    if scope.direct_scope in (DirectScope.ORGANIZATION, DirectScope.REPOSITORY):
        return None
    if not scope.entity_refs:
        return None
    return scope.entity_refs[0].entity_id


def _content_candidates(
    content: DevSourceContent, *, now: datetime
) -> list[_PathCandidate]:
    """Every fact in ``content``, projected to one un-verified path candidate
    each. Only the slot(s) matching the observation's own ``source_class``
    are ever non-empty (CHAOS-3295's own invariant), so this always iterates
    a small, single-category list in practice.
    """

    candidates: list[_PathCandidate] = []
    for status_fact in content.status_facts:
        candidates.append(
            _PathCandidate(
                "status_assessment",
                status_fact.fact_id,
                None,
                "status_change_service",
                1.0,
                now,
                status_fact.evidence_ref_ids,
            )
        )
    for child_fact in content.required_children:
        candidates.append(
            _PathCandidate(
                "required_child",
                child_fact.fact_id,
                None,
                "status_change_service",
                1.0,
                now,
                child_fact.evidence_ref_ids,
            )
        )
    for pr_fact in content.pull_requests:
        candidates.append(
            _PathCandidate(
                "linked_pull_request",
                pr_fact.entity_id,
                None,
                "status_change_service:pull_requests",
                1.0,
                pr_fact.observed_at,
                pr_fact.evidence_ref_ids,
            )
        )
    for ci_fact in content.ci_checks:
        candidates.append(
            _PathCandidate(
                "linked_ci_run",
                ci_fact.entity_id,
                None,
                "status_change_service:ci_runs",
                1.0,
                ci_fact.observed_at,
                ci_fact.evidence_ref_ids,
            )
        )
    for deployment_fact in content.deployments:
        candidates.append(
            _PathCandidate(
                "linked_deployment",
                deployment_fact.entity_id,
                None,
                "status_change_service:deployments",
                1.0,
                deployment_fact.observed_at,
                deployment_fact.evidence_ref_ids,
            )
        )
    for incident_fact in content.incidents:
        candidates.append(
            _PathCandidate(
                "linked_incident",
                incident_fact.entity_id,
                None,
                "status_change_service:incidents",
                1.0,
                incident_fact.observed_at,
                incident_fact.evidence_ref_ids,
            )
        )
    for edge in content.graph_edges:
        candidates.append(
            _PathCandidate(
                edge.relationship,
                edge.target_entity_id,
                edge.source_entity_id,
                edge.provenance,
                edge.confidence,
                edge.observed_at,
                edge.evidence_ref_ids,
            )
        )
    for change in content.observed_changes:
        candidates.append(
            _PathCandidate(
                "observed_change",
                change.entity_id,
                None,
                "status_change_service:change_summary",
                1.0,
                change.observed_at,
                change.evidence_ref_ids,
            )
        )
    for ref in content.metric_refs:
        candidates.append(
            _PathCandidate(
                "metric_scoped_to_subject",
                ref.metric_ref_id,
                None,
                "metrics_service",
                1.0,
                now,
                ref.evidence_ref_ids,
            )
        )
    return candidates


def _resolve_target(candidate: _PathCandidate, root_entity_id: str) -> str | None:
    """The candidate's "other end" relative to ``root_entity_id``, or
    ``None`` when the candidate does not actually touch the root at all
    (cross-tenant/forged-ID acceptance criterion) -- never a fabricated
    orientation."""

    if candidate.source_entity_id is None:
        return candidate.target_entity_id
    if candidate.source_entity_id == root_entity_id:
        return candidate.target_entity_id
    if candidate.target_entity_id == root_entity_id:
        return candidate.source_entity_id
    return None


class PlanExecutor:
    def __init__(
        self,
        *,
        registry: StepRegistry,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._registry = registry
        self._now = now

    async def run(
        self,
        *,
        plan: DevInvestigationPlan,
        context: StepContext,
        run_id: str,
        subject_entity_id: str | None = None,
        subject_set_fingerprint: str | None = None,
    ) -> DevInvestigationResult:
        registered = self._registry.for_plan(plan.plan_id)
        known_steps = set(plan.mandatory_steps) | set(plan.conditional_steps)
        missing = known_steps - registered.keys()
        if missing:
            # Registry-construction validation (registry_validation.
            # validate_registry) should already reject this at import time;
            # this is a defensive belt, never the primary gate.
            raise PlanRegistryError(
                f"plan {plan.plan_id!r} declares unregistered steps: {sorted(missing)}"
            )

        applicable_conditional = {
            step_id
            for step_id in plan.conditional_steps
            if registered[step_id].applicable(context)
        }
        #: Conditional steps whose applicability predicate said "no" --
        #: distinct from ``blocked`` below (a mandatory or applicable-
        #: conditional step that never ran because a prerequisite failed).
        #: The two share ``skipped_steps`` on the wire (the contract does
        #: not distinguish them at the step level) but need different
        #: terminal source states: NOT_APPLICABLE vs UNAVAILABLE.
        not_applicable: set[str] = set(plan.conditional_steps) - applicable_conditional
        blocked: set[str] = set()
        runnable = set(plan.mandatory_steps) | applicable_conditional

        # Codex finding (MEDIUM, 2026-08-01): dependencies were previously
        # filtered to `runnable` here, which *deleted* the edge to any
        # conditional prerequisite whose applicability predicate said "no" --
        # a mandatory step depending on an inapplicable conditional gate then
        # saw no dependency at all and ran immediately, instead of being
        # blocked. Retain every declared edge unfiltered; `not_applicable`
        # (like `failed`/`blocked`) is already folded into `unresolved` and
        # `blocked_now` below, so an inapplicable prerequisite still blocks
        # its dependents correctly -- it is simply never itself scheduled.
        dependencies = {
            dep.step_id: set(dep.depends_on) for dep in plan.step_dependencies
        }
        remaining = {step_id: dependencies.get(step_id, set()) for step_id in runnable}
        completed: dict[str, _Attempt] = {}
        failed: set[str] = set()

        while remaining:
            unresolved = completed.keys() | failed | not_applicable | blocked
            ready = [
                step_id for step_id, deps in remaining.items() if deps <= unresolved
            ]
            if not ready:
                # A dependency cycle survived plan validation. Treat every
                # leftover step as blocked rather than looping forever or
                # raising mid-run -- the run still terminates with a correct,
                # if degraded, disclosed result.
                blocked |= set(remaining)
                break
            blocked_now = {
                step_id
                for step_id in ready
                if dependencies.get(step_id, set())
                & (failed | not_applicable | blocked)
            }
            blocked |= blocked_now
            runnable_now = [step_id for step_id in ready if step_id not in blocked_now]
            outcomes = await asyncio.gather(
                *(
                    self._run_one(registered[step_id], context, plan)
                    for step_id in runnable_now
                ),
                return_exceptions=True,
            )
            for step_id, outcome in zip(runnable_now, outcomes, strict=True):
                if isinstance(outcome, BaseException):
                    failed.add(step_id)
                    continue
                completed[step_id] = _Attempt(step_id, outcome, failed=False)
            for step_id in ready:
                remaining.pop(step_id, None)

        # One observation per *declared* source requirement, not per step
        # that happened to run: "every required source category has exactly
        # one terminal state" (acceptance criterion) means a conditional step
        # that was inapplicable, or one blocked because its prerequisite
        # failed, still needs a typed terminal observation -- silently
        # omitting it is exactly the "the model cannot claim a source was
        # checked" gap this plan closes, from the other direction.
        steps_by_source: dict[tuple[SourceClass, str], list[str]] = {}
        for step_id, definition in registered.items():
            steps_by_source.setdefault(
                (definition.source_class, definition.adapter_id), []
            ).append(step_id)

        observation_results = [
            self._observation_for_requirement(
                requirement,
                sorted(
                    steps_by_source.get(
                        (requirement.source_class, requirement.adapter_id), ()
                    )
                ),
                completed=completed,
                failed=failed,
                not_applicable=not_applicable,
                blocked=blocked,
                run_id=run_id,
                plan_id=plan.plan_id,
                context=context,
            )
            for requirement in plan.source_requirements
        ]
        observations = tuple(
            observation for observation, _closed in observation_results
        )
        # CHAOS-3296: true only when every content-bearing observation's
        # facts all minted a verified relationship path back to the
        # committed subject -- one rejected/unrelated/self-referential/
        # unapproved/low-confidence candidate anywhere is enough to flip
        # this False. Never claims a check that did not run: an unmeasured
        # observation (no content) and a broad org/repository scope (no
        # single subject to close over) both contribute a vacuous True.
        relationship_closure_verified = all(
            closed for _observation, closed in observation_results
        )

        return DevInvestigationResult(
            schema_version="dev_investigation_result.v1",
            result_id=_mint("result", run_id, plan.plan_id),
            plan_id=plan.plan_id,
            plan_version=plan.plan_version,
            # ``ServerHandle`` requires the canonical minted-UUID grammar,
            # which the orchestrator's own ``run_id`` string is not
            # guaranteed to satisfy (it is a correlation key, not a wire
            # contract) -- folded through the same deterministic mint as
            # every other id here. The DB-level correlation to the real run
            # is ``PersistenceRunRecorder``'s constructor-bound run UUID,
            # never this field.
            run_id=_mint("run", run_id),
            subject_set_fingerprint=subject_set_fingerprint,
            subject_entity_id=subject_entity_id,
            observations=observations,
            completed_steps=tuple(sorted(completed)),
            skipped_steps=tuple(sorted(not_applicable | blocked)),
            failed_steps=tuple(sorted(failed)),
            relationship_closure_verified=relationship_closure_verified,
            completed_at=self._now(),
        )

    async def _run_one(
        self,
        definition: PlanStepDefinition,
        context: StepContext,
        plan: DevInvestigationPlan,
    ) -> StepOutcome:
        return await asyncio.wait_for(
            definition.run(context), timeout=plan.per_step_timeout_seconds
        )

    def _observation_for_requirement(
        self,
        requirement: DevSourceRequirement,
        step_ids: list[str],
        *,
        completed: dict[str, _Attempt],
        failed: set[str],
        not_applicable: set[str],
        blocked: set[str],
        run_id: str,
        plan_id: str,
        context: StepContext,
    ) -> tuple[DevSourceObservation, bool]:
        # A deterministic representative step_id for this requirement, used
        # only to seed the observation's minted id -- ``step_ids`` is already
        # sorted by the caller, so this is stable across runs regardless of
        # dict/registration order. registry_validation.validate_registry
        # rejects a declared requirement with no matching registration before
        # any run reaches this branch; the requirement's own (source_class,
        # adapter_id) is folded into the fallback seed as defense in depth
        # (codex finding, MEDIUM, 2026-08-01) so two different unregistered
        # requirements in the same plan can never mint colliding observation
        # ids from an identical literal "unregistered" seed.
        identity_step_id = (
            step_ids[0]
            if step_ids
            else f"unregistered:{requirement.source_class.value}:{requirement.adapter_id}"
        )
        observation_id = _mint("observation", run_id, plan_id, identity_step_id)

        completed_step = next((s for s in step_ids if s in completed), None)
        if completed_step is not None:
            outcome = completed[completed_step].outcome
            assert outcome is not None
            return self._to_observation(requirement, outcome, observation_id, context)
        if step_ids and all(step_id in not_applicable for step_id in step_ids):
            return self._unmeasured_observation(
                requirement,
                SourceRequirementState.NOT_APPLICABLE,
                "step_not_applicable",
                observation_id,
            )
        if step_ids and any(step_id in failed for step_id in step_ids):
            return self._unmeasured_observation(
                requirement,
                SourceRequirementState.UNAVAILABLE,
                "step_execution_failed",
                observation_id,
            )
        if step_ids and any(step_id in blocked for step_id in step_ids):
            return self._unmeasured_observation(
                requirement,
                SourceRequirementState.UNAVAILABLE,
                "step_blocked_by_prerequisite",
                observation_id,
            )
        # No step is registered for this declared requirement at all.
        # registry_validation.validate_registry rejects this at import time;
        # this branch only guards a runtime plan built outside that path.
        return self._unmeasured_observation(
            requirement,
            SourceRequirementState.UNAVAILABLE,
            "step_unregistered",
            observation_id,
        )

    def _to_observation(
        self,
        requirement: DevSourceRequirement,
        outcome: StepOutcome,
        observation_id: str,
        context: StepContext,
    ) -> tuple[DevSourceObservation, bool]:
        relationship_paths, closed = self._mint_relationship_paths(
            source_class=requirement.source_class,
            content=outcome.content,
            context=context,
            observation_id=observation_id,
        )
        observation = DevSourceObservation(
            schema_version="dev_source_observation.v1",
            observation_id=observation_id,
            source_class=requirement.source_class,
            adapter_id=requirement.adapter_id,
            requirement_level=requirement.requirement_level,
            observed_state=outcome.observed_state,
            data_semantics=outcome.data_semantics,
            watermark=outcome.watermark,
            subject_coverage=outcome.subject_coverage,
            usable_fact_count=outcome.usable_fact_count,
            sample_count=outcome.sample_count,
            relationship_paths=relationship_paths,
            evidence_ref_ids=(),
            limitation=outcome.limitation,
            observed_at=self._now(),
            query_version=outcome.query_version,
            content=outcome.content,
        )
        return observation, closed

    def _unmeasured_observation(
        self,
        requirement: DevSourceRequirement,
        state: SourceRequirementState,
        limitation: str,
        observation_id: str,
    ) -> tuple[DevSourceObservation, bool]:
        observation = DevSourceObservation(
            schema_version="dev_source_observation.v1",
            observation_id=observation_id,
            source_class=requirement.source_class,
            adapter_id=requirement.adapter_id,
            requirement_level=requirement.requirement_level,
            observed_state=state,
            data_semantics="not_measured",
            watermark=None,
            subject_coverage=0.0,
            usable_fact_count=0,
            sample_count=None,
            relationship_paths=(),
            evidence_ref_ids=(),
            limitation=limitation,
            observed_at=self._now(),
            query_version="unversioned",
            content=None,
        )
        # An unmeasured source never ran -- nothing to close over. That gap
        # is disclosed separately (subject_coverage/limitation on this same
        # observation), not folded into relationship_closure_verified.
        return observation, True

    def _mint_relationship_paths(
        self,
        *,
        source_class: SourceClass,
        content: DevSourceContent | None,
        context: StepContext,
        observation_id: str,
    ) -> tuple[tuple[DevRelationshipPath, ...], bool]:
        if content is None:
            return (), True
        root_entity_id = _root_entity_id(context.scope)
        if root_entity_id is None:
            return (), True
        accepted: dict[tuple[str, str], tuple[_PathCandidate, str]] = {}
        rejected = 0
        for candidate in _content_candidates(content, now=context.now):
            resolved_target = _resolve_target(candidate, root_entity_id)
            if (
                resolved_target is None
                or resolved_target == root_entity_id
                or candidate.confidence < MIN_RELATIONSHIP_CONFIDENCE
                or not approved_relationship(source_class, candidate.relationship)
            ):
                rejected += 1
                continue
            key = (candidate.relationship, resolved_target)
            existing = accepted.get(key)
            if existing is None or candidate.confidence > existing[0].confidence:
                accepted[key] = (candidate, resolved_target)
        paths = tuple(
            DevRelationshipPath(
                path_id=_mint(
                    "relationship_path", context.run_id, observation_id, str(index)
                ),
                source_entity_id=root_entity_id,
                relationship=candidate.relationship,
                target_entity_id=resolved_target,
                provenance=candidate.provenance[:2_048],
                confidence=max(0.0, min(1.0, candidate.confidence)),
                observed_at=candidate.observed_at,
                evidence_ref_ids=candidate.evidence_ref_ids[:25],
            )
            for index, (_key, (candidate, resolved_target)) in enumerate(
                sorted(accepted.items())
            )
        )
        return paths, rejected == 0
