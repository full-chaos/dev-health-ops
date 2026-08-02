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
import contextvars
import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ..contracts import DevScope, DirectScope, FreshnessState
from ..contracts_v2.base import EvidenceHandle, SourceClass, SourceRequirementState
from ..contracts_v2.plan import DevInvestigationPlan, DevSourceRequirement
from ..contracts_v2.result import (
    DevInvestigationResult,
    DevRelationshipPath,
    DevSourceContent,
    DevSourceObservation,
)
from .builtin_steps import PlanExecutorRuntime
from .relationship_matrix import (
    CONTENT_SLOT_FIELDS,
    MAX_RELATIONSHIP_PATHS,
    MIN_RELATIONSHIP_CONFIDENCE,
    approved_relationship,
    content_slot_violations,
)
from .state_mapping import queried_semantics
from .steps import (
    PlanRegistryError,
    PlanStepDefinition,
    StepContext,
    StepOutcome,
    StepRegistry,
)

__all__ = ["PlanExecutionError", "PlanExecutor", "wrap_runtime_with_mint_receipts"]


@dataclass(frozen=True, slots=True)
class _MintedReceipt:
    """The identity a ``mint_evidence`` call actually issued a handle
    against -- the same three fields ``EvidenceReferenceSigner._payload``
    binds into its HMAC (``evidence_service.py``), captured directly from
    the mint call's own keyword arguments rather than re-derived from
    anything a step could shape afterward.

    Codex finding (MEDIUM, round 2, 2026-08-02): recording only the bare
    handle string (round 1's fix) proved merely that *some* handle was
    minted during this step -- a step could mint once for a real entity and
    reuse that same string on an arbitrary number of fabricated facts, and
    the round-1 check could not tell the difference. Binding identity here
    closes that: verification compares each fact's own claimed identity
    against the receipt of every handle it cites (see
    ``_evidence_identity_mismatches``).
    """

    source_system: str
    entity_type: str
    entity_id: str


#: Per-step-invocation scope (CHAOS-3296 Codex finding, MEDIUM 2026-08-01):
#: every evidence handle a ``PlanExecutorRuntime`` actually issues during one
#: step's own ``run()`` call is recorded here, keyed by the handle and
#: mapped to the identity it was minted against (see
#: :class:`_MintReceiptRuntime`), isolated per ``asyncio`` task -- concurrent
#: sibling steps (``asyncio.gather`` in :meth:`PlanExecutor.run`) each run in
#: their own task with their own copy of this context var, so one step's
#: receipts can never leak into another's. ``None`` (the default, and the
#: only value any pre-3296 test or ``verify_mint_receipts=False`` run ever
#: sees) means "no receipt tracking is active" -- content is accepted
#: unverified, exactly today's behavior.
_MINTED_EVIDENCE_HANDLES: contextvars.ContextVar[dict[str, _MintedReceipt] | None] = (
    contextvars.ContextVar("chaos_3296_minted_evidence_handles", default=None)
)

#: A shared, never-mutated empty receipt map -- used as ``_to_observation``'s
#: default so an ordinary mutable-default pitfall never applies (nothing
#: ever writes into it; every real receipt map is its own fresh ``dict``
#: from :meth:`PlanExecutor._run_one`).
_EMPTY_RECEIPTS: Mapping[str, _MintedReceipt] = {}

#: Codex finding (HIGH, 2026-08-02, round 2): raising the persistence
#: envelope (``persistence/service.py``'s ``_SOURCE_OBSERVATION_PAYLOAD_
#: MAX_BYTES``) from 16KiB to 64KiB only moved the failure -- a genuinely
#: contract-valid observation (every category at its own ``max_length``,
#: every string at its own bound) still serializes past 64KiB by an order
#: of magnitude (~615KiB at the true maximum), and a legal metric
#: observation with dense 366-point series alone cleared 64KiB in
#: isolation. Bounding the CONTRACT does not bound the WIRE PAYLOAD --
#: the fix has to happen at construction, before persistence ever sees it,
#: so the persisted object is exactly the presented object (never a
#: silent divergence between "what we showed" and "what we stored").
#: Kept as a literal duplicate of the numeric relationship to the
#: persistence envelope (64KiB), not an import -- this domain-layer module
#: has no dependency on the storage layer (see ``_STATUS_ENTITY_SOURCE_
#: SYSTEM`` in ``builtin_steps.py`` for the same by-value-not-import
#: posture); the two constants must be changed together.
_CONTENT_BYTE_BUDGET_DEFAULT = 56 * 1024


def _observation_json_bytes(observation: DevSourceObservation) -> int:
    """Byte-for-byte the same encoding ``persistence.service._bounded_json``
    measures the persisted payload with -- so a budget check here is the
    exact same measurement the persistence backstop would otherwise apply,
    just performed before construction commits to a shape that might not
    fit."""

    encoded = json.dumps(
        observation.model_dump(mode="json"), separators=(",", ":"), sort_keys=True
    )
    return len(encoded.encode("utf-8"))


def _total_content_facts(content: DevSourceContent) -> int:
    return sum(len(getattr(content, slot)) for slot in CONTENT_SLOT_FIELDS)


def _drop_lowest_priority_item(
    content: DevSourceContent, dropped: dict[str, int]
) -> DevSourceContent | None:
    """Remove exactly one item from the lowest-priority non-empty
    ``DevSourceContent`` field (tail-drop -- fields are already tuples in
    stable, service-determined order, so this never touches dict/set
    iteration order). ``CONTENT_SLOT_FIELDS`` is walked in reverse: the
    last field in that priority list is the first ever dropped. Returns
    ``None`` once every field is empty -- nothing left to drop."""

    for slot in reversed(CONTENT_SLOT_FIELDS):
        items = getattr(content, slot)
        if items:
            dropped[slot] = dropped.get(slot, 0) + 1
            return content.model_copy(update={slot: items[:-1]})
    return None


def _dropped_summary(dropped: dict[str, int]) -> str:
    return ",".join(
        f"{field}:{count}" for field, count in sorted(dropped.items()) if count
    )


def _downgrade_for_truncation(state: SourceRequirementState) -> SourceRequirementState:
    """A truncated result can never claim to be fully current -- downgrade
    exactly once, never re-upgrade an already-imperfect state. Mirrors
    ``state_mapping.work_graph_result_state_to_requirement_state``'s own
    "truncated but real facts survive" reasoning: PARTIAL/STALE, never
    the zero-fact TRUNCATED state, as long as at least one fact remains."""

    if state is SourceRequirementState.AVAILABLE_CURRENT:
        return SourceRequirementState.AVAILABLE_STALE
    return state


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
class _MintReceiptRuntime:
    """A ``PlanExecutorRuntime`` pass-through that records every evidence
    handle it actually issues -- and the identity it issued that handle
    against -- into whichever step-scoped receipt map
    :data:`_MINTED_EVIDENCE_HANDLES` names for the calling ``asyncio`` task.

    CHAOS-3296 Codex finding (MEDIUM, 2026-08-01; sharpened round 2,
    2026-08-02): a step's own ``StepOutcome.content`` is entirely
    producer-controlled -- nothing previously stopped a step from embedding
    a syntactically-valid-looking ``ev1_...`` string it fabricated inline
    rather than one this exact ``mint_evidence`` call actually signed, and
    round 1's existence-only receipt set could not stop a step from minting
    once for a real entity and reusing that same handle on an arbitrary
    number of fabricated facts. Wrapping the runtime here, once, at
    registration (see :func:`wrap_runtime_with_mint_receipts`), means every
    builtin step's ``mint=runtime.mint_evidence`` (``builtin_steps.py``)
    transparently goes through this recorder with no change to that module --
    the executor, not any individual step, becomes the sole authority on
    which handles a step's own run genuinely minted, and what each one was
    minted to prove.
    """

    _inner: PlanExecutorRuntime

    async def status_snapshot(self, *, org_id, permission_fingerprint, scope):
        return await self._inner.status_snapshot(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )

    async def change_summary(self, *, org_id, permission_fingerprint, scope):
        return await self._inner.change_summary(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )

    def list_metrics(self, scope):
        return self._inner.list_metrics(scope)

    async def query_metric(self, *, org_id, permission_fingerprint, metric_id, scope):
        return await self._inner.query_metric(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            metric_id=metric_id,
            scope=scope,
        )

    async def work_graph_neighbors(self, *, org_id, permission_fingerprint, scope):
        return await self._inner.work_graph_neighbors(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )

    async def data_health(self, *, org_id, permission_fingerprint, scope):
        return await self._inner.data_health(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )

    def mint_evidence(
        self,
        *,
        org_id: str,
        source_system: str,
        source_version: str,
        entity_type: str,
        entity_id: str,
        display_label: str,
        observed_at: datetime,
        freshness: FreshnessState,
        confidence: float = 1.0,
        valid_entity_ids=(),
        repository_ids=(),
    ) -> EvidenceHandle:
        handle = self._inner.mint_evidence(
            org_id=org_id,
            source_system=source_system,
            source_version=source_version,
            entity_type=entity_type,
            entity_id=entity_id,
            display_label=display_label,
            observed_at=observed_at,
            freshness=freshness,
            confidence=confidence,
            valid_entity_ids=valid_entity_ids,
            repository_ids=repository_ids,
        )
        receipts = _MINTED_EVIDENCE_HANDLES.get()
        if receipts is not None:
            receipts[handle] = _MintedReceipt(
                source_system=source_system,
                entity_type=entity_type,
                entity_id=entity_id,
            )
        return handle


def wrap_runtime_with_mint_receipts(
    runtime: PlanExecutorRuntime,
) -> PlanExecutorRuntime:
    """Wrap ``runtime`` so :class:`PlanExecutor` can verify -- not merely
    trust -- which evidence handles a step's own execution actually minted.

    Call this exactly once, before ``register_builtin_steps``/
    ``build_default_registry`` bakes ``runtime`` into every step closure
    (``production_runtime.py``'s only call site). Has no effect unless the
    owning :class:`PlanExecutor` was also constructed with
    ``verify_mint_receipts=True`` -- with that flag off (every pre-3296 test
    fixture, and any caller that has not opted in), :data:`
    _MINTED_EVIDENCE_HANDLES` is never set, so this wrapper degrades to a
    plain pass-through.
    """

    return _MintReceiptRuntime(runtime)


@dataclass(slots=True)
class _Attempt:
    step_id: str
    outcome: StepOutcome | None
    failed: bool
    minted_evidence_handles: Mapping[str, _MintedReceipt] = field(default_factory=dict)


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


def _unminted_evidence_handles(
    content: DevSourceContent,
    minted_evidence_handles: Mapping[str, _MintedReceipt],
    *,
    now: datetime,
) -> frozenset[str]:
    """Every evidence handle ``content`` claims that this step's own receipt
    map (:data:`_MINTED_EVIDENCE_HANDLES`, captured per-step in
    :meth:`PlanExecutor._run_one`) never actually recorded -- i.e. a handle
    that is not proven to have come from this exact step's
    ``runtime.mint_evidence`` calls. Reuses :func:`_content_candidates`
    (already a total per-fact-category walk) rather than a second, parallel
    traversal of ``DevSourceContent``'s nine fields. Existence-only: a
    handle that *was* minted, but for a different entity, passes this check
    and is instead caught by :func:`_evidence_identity_mismatches`.
    """

    claimed = {
        handle
        for candidate in _content_candidates(content, now=now)
        for handle in candidate.evidence_ref_ids
    }
    return frozenset(claimed - minted_evidence_handles.keys())


#: Codex finding (MEDIUM, round 2, 2026-08-02): the exact identity each
#: content category minted its evidence against, mirroring
#: ``builtin_steps.py``'s own ``_wire_*_content`` derivation *by value*
#: (matching this package's established ``_STATUS_ENTITY_SOURCE_SYSTEM``
#: posture) so this can never spuriously reject a legitimately-minted fact
#: by guessing wrong at how identity was derived. ``graph_edges`` is
#: deliberately absent: ``DevGraphEdgeV2`` never preserves the ``edge_id``
#: ``_wire_work_graph_content`` minted against on the wire (only
#: source/target/relationship survive), so no identity claim can be
#: faithfully reconstructed for that one category here -- its handles still
#: go through :func:`_unminted_evidence_handles`'s existence-only check,
#: never through identity comparison. Only ``entity_type``/``entity_id`` are
#: compared (not ``source_system``): those two answer "is this citing the
#: right real-world thing," the question this finding is about; verifying
#: the more failure-prone, per-category ``source_system`` derivation too
#: would add drift risk for a lower-value check.
def _content_fact_claims(
    content: DevSourceContent,
) -> list[tuple[str, str, tuple[str, ...]]]:
    claims: list[tuple[str, str, tuple[str, ...]]] = []
    for status_fact in content.status_facts:
        entity_type, _sep, entity_id = status_fact.fact_id.partition(":")
        claims.append((entity_type, entity_id, status_fact.evidence_ref_ids))
    for child_fact in content.required_children:
        entity_type, _sep, entity_id = child_fact.fact_id.partition(":")
        claims.append((entity_type, entity_id, child_fact.evidence_ref_ids))
    for pr_fact in content.pull_requests:
        claims.append(("pull_request", pr_fact.entity_id, pr_fact.evidence_ref_ids))
    for ci_fact in content.ci_checks:
        # Mirrors builtin_steps._ci_evidence_identity's coarsening exactly:
        # a ci_acceptance_checks row's "{repo}#ci{run}#check{key}" entity_id
        # was minted against the coarsened "{repo}#ci{run}" run identity.
        lookup_entity_id = ci_fact.entity_id.split("#check", 1)[0]
        claims.append(("ci_run", lookup_entity_id, ci_fact.evidence_ref_ids))
    for deployment_fact in content.deployments:
        claims.append(
            ("deployment", deployment_fact.entity_id, deployment_fact.evidence_ref_ids)
        )
    for incident_fact in content.incidents:
        claims.append(
            ("incident", incident_fact.entity_id, incident_fact.evidence_ref_ids)
        )
    for change in content.observed_changes:
        # Mirrors builtin_steps._wire_change_summary_content's
        # change_evidence_identity exactly: a "relationship" category change
        # was minted against its own change_id, every other category against
        # entity_id.
        lookup_entity_id = (
            change.change_id if change.category == "relationship" else change.entity_id
        )
        claims.append((change.entity_type, lookup_entity_id, change.evidence_ref_ids))
    for ref in content.metric_refs:
        claims.append(("metric", ref.metric_ref_id, ref.evidence_ref_ids))
    return claims


def _evidence_identity_mismatches(
    content: DevSourceContent, minted_evidence_handles: Mapping[str, _MintedReceipt]
) -> tuple[str, ...]:
    """Every claimed ``entity_id`` from a fact citing a handle that WAS
    minted this step -- just not for that fact's own entity. A handle
    absent from ``minted_evidence_handles`` entirely is
    :func:`_unminted_evidence_handles`'s concern, not this function's; here
    a receipt exists, it is simply bound to a different identity than the
    fact citing it claims -- exactly the "mint once for issue-1, reuse on a
    fabricated issue-999 fact" forgery this finding closes.
    """

    mismatched: set[str] = set()
    for entity_type, entity_id, evidence_ref_ids in _content_fact_claims(content):
        for handle in evidence_ref_ids:
            receipt = minted_evidence_handles.get(handle)
            if receipt is not None and (
                receipt.entity_type != entity_type or receipt.entity_id != entity_id
            ):
                mismatched.add(entity_id)
                break
    return tuple(sorted(mismatched))


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
        verify_mint_receipts: bool = False,
        content_byte_budget: int = _CONTENT_BYTE_BUDGET_DEFAULT,
    ) -> None:
        self._registry = registry
        self._now = now
        #: CHAOS-3296 Codex finding (HIGH, round 2): the deterministic,
        #: disclosed-truncation byte budget every constructed observation is
        #: held to (see :meth:`_budgeted_observation`). Overridable so tests
        #: can pin an exact, small budget and construct boundary fixtures at
        #: precisely-at-budget / one-item-over without needing to reverse
        #: engineer the real ~56KiB target's byte arithmetic.
        self._content_byte_budget = content_byte_budget
        #: CHAOS-3296 Codex finding (MEDIUM, 2026-08-01): off by default so
        #: every pre-3296 test/harness that hand-builds ``StepOutcome.content``
        #: (bypassing ``register_builtin_steps``/real minting entirely) is
        #: completely unaffected. ``production_runtime.py`` is the one caller
        #: that turns this on, paired with ``wrap_runtime_with_mint_receipts``
        #: wrapping the runtime it registers steps against -- without both
        #: halves, this flag alone has nothing to verify against and would
        #: reject every real fact's evidence citation.
        self._verify_mint_receipts = verify_mint_receipts

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
            for step_id, attempt_result in zip(runnable_now, outcomes, strict=True):
                if isinstance(attempt_result, BaseException):
                    failed.add(step_id)
                    continue
                step_outcome, minted = attempt_result
                completed[step_id] = _Attempt(
                    step_id,
                    step_outcome,
                    failed=False,
                    minted_evidence_handles=minted,
                )
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
    ) -> tuple[StepOutcome, Mapping[str, _MintedReceipt]]:
        # Each ``_run_one`` call is its own ``asyncio`` task (``run``'s
        # ``asyncio.gather`` wraps each coroutine passed to it), so setting
        # this context var here is isolated from every concurrent sibling
        # step by construction -- no cross-step leakage, no locking needed.
        token = _MINTED_EVIDENCE_HANDLES.set({}) if self._verify_mint_receipts else None
        try:
            outcome = await asyncio.wait_for(
                definition.run(context), timeout=plan.per_step_timeout_seconds
            )
            minted = dict(_MINTED_EVIDENCE_HANDLES.get() or {})
            return outcome, minted
        finally:
            if token is not None:
                _MINTED_EVIDENCE_HANDLES.reset(token)

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
            attempt = completed[completed_step]
            outcome = attempt.outcome
            assert outcome is not None
            return self._to_observation(
                requirement,
                outcome,
                observation_id,
                context,
                minted_evidence_handles=attempt.minted_evidence_handles,
            )
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
        *,
        minted_evidence_handles: Mapping[str, _MintedReceipt] = _EMPTY_RECEIPTS,
    ) -> tuple[DevSourceObservation, bool]:
        content = outcome.content
        if content is not None:
            # Codex finding (MEDIUM, 2026-08-01): a step registered under one
            # source_class returning content shaped for a different one (e.g.
            # STATUS_CHANGE content carrying graph_edges) previously reached
            # relationship-path minting and persistence unfiltered -- the
            # matrix is a closed, code-owned vocabulary of what a source
            # class may ever claim, same posture as ``approved_relationship``
            # below. Reject structurally: demote to unmeasured rather than
            # pass mismatched content through, and disclose the failure via
            # ``closed=False`` rather than a vacuous True.
            slot_violations = content_slot_violations(requirement.source_class, content)
            if slot_violations:
                return self._unmeasured_observation(
                    requirement,
                    SourceRequirementState.UNAVAILABLE,
                    "content_source_class_mismatch:" + ",".join(slot_violations),
                    observation_id,
                    closed=False,
                )
            if self._verify_mint_receipts:
                # Codex finding (MEDIUM, 2026-08-01): pydantic's ``EvidenceHandle``
                # pattern only proves a string is *shaped* like ``ev1_...`` --
                # never that this exact step run actually minted it through
                # the signer. A step that fabricated a plausible-looking
                # handle inline (bug or forged adapter) would otherwise pass
                # through unnoticed; every real handle a builtin step embeds
                # came from ``runtime.mint_evidence`` via
                # ``wrap_runtime_with_mint_receipts``, so any evidence_ref_id
                # in ``content`` that this step's own receipt map never
                # recorded is a handle this run did not actually mint.
                unminted = _unminted_evidence_handles(
                    content, minted_evidence_handles, now=context.now
                )
                if unminted:
                    return self._unmeasured_observation(
                        requirement,
                        SourceRequirementState.UNAVAILABLE,
                        "unminted_evidence_handle",
                        observation_id,
                        closed=False,
                    )
                # Codex finding (MEDIUM, round 2, 2026-08-02): a handle
                # existing in the receipt map is not enough -- round 1's
                # check could not stop a step minting once for a real
                # entity and reusing that exact handle to "prove" an
                # arbitrary, fabricated second fact. Every fact's own
                # claimed identity must match the identity its cited
                # handle(s) were actually minted against.
                mismatched = _evidence_identity_mismatches(
                    content, minted_evidence_handles
                )
                if mismatched:
                    return self._unmeasured_observation(
                        requirement,
                        SourceRequirementState.UNAVAILABLE,
                        "evidence_entity_mismatch:" + ",".join(mismatched),
                        observation_id,
                        closed=False,
                    )
        return self._budgeted_observation(
            requirement, outcome, content, observation_id, context
        )

    def _budgeted_observation(
        self,
        requirement: DevSourceRequirement,
        outcome: StepOutcome,
        content: DevSourceContent | None,
        observation_id: str,
        context: StepContext,
    ) -> tuple[DevSourceObservation, bool]:
        """Construct the final observation, then -- before returning it to
        persistence or presentation -- shrink ``content`` deterministically
        until the *exact* payload persistence will store fits
        ``self._content_byte_budget``.

        Codex finding (HIGH, 2026-08-02, round 2): raising the persistence
        envelope only moved the "valid run becomes internal_error" bug to a
        higher threshold -- the true contract-legal maximum is far larger
        than any envelope this layer could reasonably hold. The fix has to
        cap what gets CONSTRUCTED, using the contract's own disclosed
        mechanisms (a downgraded ``observed_state``/``TRUNCATED`` plus a
        bounded ``limitation``), never a second silent truncation notion and
        never converting a legitimate answer into an opaque platform error.

        Every iteration re-derives ``relationship_paths`` from the CURRENT
        (possibly already-shrunk) ``content`` -- never the original -- so a
        dropped fact can never leave behind an orphaned relationship path
        that cites content no longer present on the wire.
        """

        dropped: dict[str, int] = {}
        working_content = content
        while True:
            relationship_paths, paths_closed = self._mint_relationship_paths(
                source_class=requirement.source_class,
                content=working_content,
                context=context,
                observation_id=observation_id,
            )
            usable_fact_count = outcome.usable_fact_count
            observed_state = outcome.observed_state
            data_semantics = outcome.data_semantics
            limitation = outcome.limitation
            if dropped:
                # Recomputed from what actually survived -- never the
                # step's original count, which may describe facts this
                # observation no longer carries.
                usable_fact_count = (
                    _total_content_facts(working_content)
                    if working_content is not None
                    else 0
                )
                if usable_fact_count == 0:
                    return self._unmeasured_observation(
                        requirement,
                        SourceRequirementState.TRUNCATED,
                        "budget_truncated_to_empty:" + _dropped_summary(dropped),
                        observation_id,
                        closed=False,
                    )
                observed_state = _downgrade_for_truncation(observed_state)
                data_semantics = queried_semantics(usable_fact_count)
                limitation = "budget_truncated:" + _dropped_summary(dropped)
            observation = DevSourceObservation(
                schema_version="dev_source_observation.v1",
                observation_id=observation_id,
                source_class=requirement.source_class,
                adapter_id=requirement.adapter_id,
                requirement_level=requirement.requirement_level,
                observed_state=observed_state,
                data_semantics=data_semantics,
                watermark=outcome.watermark,
                subject_coverage=outcome.subject_coverage,
                usable_fact_count=usable_fact_count,
                sample_count=outcome.sample_count,
                relationship_paths=relationship_paths,
                evidence_ref_ids=(),
                limitation=limitation,
                observed_at=self._now(),
                query_version=outcome.query_version,
                content=working_content,
            )
            if _observation_json_bytes(observation) <= self._content_byte_budget:
                return observation, paths_closed and not dropped
            if working_content is None:
                # Nothing left to drop yet still over budget: the non-content
                # fields alone (e.g. an extreme ``provenance``/handle set on
                # relationship_paths) exceed the budget. Fail closed exactly
                # like the empty-after-truncation case above rather than
                # persisting an oversized payload.
                return self._unmeasured_observation(
                    requirement,
                    SourceRequirementState.TRUNCATED,
                    "budget_truncated_to_empty:" + _dropped_summary(dropped),
                    observation_id,
                    closed=False,
                )
            working_content = _drop_lowest_priority_item(working_content, dropped)

    def _unmeasured_observation(
        self,
        requirement: DevSourceRequirement,
        state: SourceRequirementState,
        limitation: str,
        observation_id: str,
        *,
        closed: bool = True,
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
        # An unmeasured source never ran -- nothing to close over -- so
        # ``closed`` stays True by default. ``_to_observation`` overrides it
        # to False for the two content-integrity guards above: those *did*
        # run a check, and it failed, which must never present as the
        # vacuous "nothing to verify" case.
        return observation, closed

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
        # Codex finding (HIGH, 2026-08-01): ``DevSourceObservation.
        # relationship_paths`` is bounded to ``MAX_RELATIONSHIP_PATHS`` at
        # the contract layer (``max_length=25``) -- a dense-but-legitimate
        # result that accepted more than that previously reached
        # ``DevRelationshipPath``/``DevSourceObservation`` construction
        # unbudgeted, raising ``too_long`` deep inside pydantic and turning
        # the whole run into an ``internal_error`` instead of a disclosed,
        # partial answer. Apply the budget here, deterministically --
        # ``sorted(accepted.items())`` is already keyed on
        # ``(relationship, target_entity_id)``, so truncation always drops
        # the same tail regardless of dict/candidate-discovery order -- and
        # flip closure False whenever the budget actually bites, exactly
        # like any other dropped candidate.
        sorted_accepted = sorted(accepted.items())
        truncated = len(sorted_accepted) > MAX_RELATIONSHIP_PATHS
        budgeted = sorted_accepted[:MAX_RELATIONSHIP_PATHS]
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
            for index, (_key, (candidate, resolved_target)) in enumerate(budgeted)
        )
        return paths, rejected == 0 and not truncated
