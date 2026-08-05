"""CHAOS-3297 stack #3 / CHAOS-3393: plan wiring for the CHAOS-3303/3304/
3305/3393 services.

``health.project.v1`` / ``health.team.v1`` / ``balance.team_workload.v1`` /
``deficiency.operational.v1`` / ``status.portfolio.v1`` -- registered
against the SAME :class:`~.steps.StepRegistry` ``plan_documents.py``'s six
core plans use, per that module's own docstring promise ("CHAOS-3303/3304/
3305 register those plans and their steps against the same StepRegistry
this module uses, without needing to change this module or the
orchestrator").

``status.portfolio.v1`` was deliberately NOT registered here through
CHAOS-3297: wiring it found a real gap -- :class:`~.steps.StepContext`
carried exactly one :class:`~..contracts.DevScope`
(``DevScope.validate_direct_scope`` requires exactly one ``entity_ref`` for
every non-``ORGANIZATION`` direct scope), and
:meth:`~.executor.PlanExecutor.run`'s own ``subject_set_fingerprint``
parameter was result metadata only, not a channel for handing a step the
several project scopes ``PortfolioStatusService.evaluate_portfolio`` needs.
CHAOS-3393 closes that gap additively: ``StepContext.subject_set_scopes``
(a new, empty-by-default tuple field every other plan ignores) carries the
committed batch, built by the orchestrator from the preflight-committed
``DevSubjectSet`` -- see ``orchestrator.run()``'s own PLURAL_COHORT/
ORGANIZATION_WIDE gate.

Every step here calls exactly one CHAOS-3303/3304/3305/3393 service, over
the SAME :class:`~.builtin_steps.PlanExecutorRuntime` port the six core
plans' steps already use -- ``ProjectHealthService``/``TeamHealthService``/
``TeamWorkloadService``/``OperationalDeficiencyService``/
``PortfolioStatusService`` all take that runtime (or, for
``PortfolioStatusService``, a ``ProjectHealthService`` built over it)
directly in their own constructors, so ``production_runtime.py``
constructs ONE runtime instance and threads it into both
``builtin_steps.register_builtin_steps`` and this module's
:func:`register_wave_3_1_steps` -- never a second, parallel query path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol

from ..contracts import DevScope, DirectScope
from ..contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from ..contracts_v2.deficiency import (
    DeficiencyFinding,
    OperationalDeficiencyInventory,
    finding_sort_key,
)
from ..contracts_v2.health_rules import (
    DevPortfolioProjectStatusV2,
    DimensionState,
    HealthRuleFinding,
)
from ..contracts_v2.plan import DevInvestigationPlan, DevSourceRequirement
from ..contracts_v2.result import HEALTH_FINDING_SEVERITY_RANK, DevSourceContent
from ..health_profile_synthesis import HealthProfileResult
from ..persistence.service import _SOURCE_CLASSES as _PERSISTENCE_SOURCE_CLASSES
from ..portfolio_status_service import (
    PortfolioProjectScope,
    PortfolioStatusResult,
    worst_project_state,
)
from ..preflight_outcomes import PLAN_ID_BY_INTENT
from .builtin_steps import PlanExecutorRuntime, register_builtin_steps
from .plan_documents import CORE_PLANS_BY_INTENT, CORE_QUESTION_INTENT_IDS
from .registry_validation import validate_registry
from .state_mapping import queried_semantics
from .steps import PlanStepDefinition, StepContext, StepOutcome, StepRegistry

__all__ = [
    "PORTFOLIO_PROJECT_TIMEOUT_CEILING_SECONDS",
    "PORTFOLIO_PLAN_TIMEOUT_SECONDS",
    "WAVE_3_1_PLANS_BY_INTENT",
    "WAVE_3_1_QUESTION_INTENT_IDS",
    "build_registry_with_wave_3_1",
    "capped_deficiency_findings",
    "capped_health_findings",
    "capped_portfolio_project_statuses",
    "portfolio_project_timeout_seconds",
    "register_wave_3_1_steps",
]


def _source_classes_missing_from_persistence_allowlist(
    plans_by_intent: Mapping[QuestionIntentID, DevInvestigationPlan],
    *,
    allowlist: frozenset[str],
) -> frozenset[str]:
    """Every ``SourceClass`` value any of ``plans_by_intent``'s steps can
    emit that ``allowlist`` does not contain (CHAOS-3337).

    ``persistence.service._SOURCE_CLASSES`` is a separate, hand-maintained
    frozenset -- ``SourceClass`` being a closed pydantic enum only proves a
    plan document's own ``source_requirements`` are internally consistent,
    never that the persistence layer's own allowlist was updated to match.
    A registered plan whose steps emit a ``SourceClass`` this table does
    not carry crashes the FIRST live run that reaches
    ``DevPersistenceService.append_source_observation`` with
    ``DevPersistenceValidationError('invalid source_class')`` -- CHAOS-3337
    was exactly this, for ``HEALTH_PROFILE``/``DEFICIENCY_INVENTORY``
    (CHAOS-3297 stack #3), the third total-table to miss a ``SourceClass``
    reconciliation (the CHAOS-3296/3297 relationship-matrix tables at
    #1374's merge were the first two).

    Pure and directly testable (see ``test_chaos_3337_source_class_
    persistence_allowlist.py``), and also invoked below at THIS module's
    own import time against the real registries and the real allowlist --
    so the next ``SourceClass`` addition to a registered plan's
    ``source_requirements`` fails at import, not live.
    """

    emitted = {
        requirement.source_class.value
        for plan in plans_by_intent.values()
        for requirement in plan.source_requirements
    }
    return frozenset(emitted) - allowlist


#: A finding-emitting service surface: exactly what this module's steps
#: call. Narrower than the full ``ProjectHealthService`` class so a test
#: double only has to implement what a step actually calls -- mirrors
#: ``builtin_steps.PlanExecutorRuntime``'s own "exact canonical-service
#: surface" discipline.
class _ProjectHealthEvaluator(Protocol):
    async def evaluate_project(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> HealthProfileResult: ...


class _TeamHealthEvaluator(Protocol):
    async def evaluate_team(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> HealthProfileResult: ...


class _TeamWorkloadEvaluator(Protocol):
    async def evaluate_workload(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> HealthProfileResult: ...


class _OperationalDeficiencyEvaluator(Protocol):
    async def evaluate_project(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> OperationalDeficiencyInventory: ...

    async def evaluate_team(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> OperationalDeficiencyInventory: ...


class _PortfolioStatusEvaluator(Protocol):
    async def evaluate_portfolio(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        projects: Sequence[PortfolioProjectScope],
        now: datetime,
        unresolved_mention_ids: Sequence[str] = (),
        ambiguous_mention_ids: Sequence[str] = (),
        warnings: Sequence[str] = (),
        per_project_timeout_seconds: float | None = None,
    ) -> PortfolioStatusResult: ...


_MAX_FINDINGS = 50


def capped_health_findings(
    findings: tuple[HealthRuleFinding, ...],
) -> tuple[tuple[HealthRuleFinding, ...], bool]:
    """Sort ``findings`` (launch-eligible only -- see the caller) into the
    canonical worst-severity-first-then-``finding_id`` order
    ``DevSourceContent.validate_finding_order``/``DevAnswerFrame.
    validate_frame_semantics`` require, then cap at :data:`_MAX_FINDINGS`,
    returning whether the cap actually bit. Reuses ``contracts_v2.result.
    HEALTH_FINDING_SEVERITY_RANK`` by reference -- the exact table both
    contract-layer validators check against, never a second copy that
    could silently disagree with them. Public (not module-private): also
    used by ``terminal_frames.wrap_legacy_answer_as_frame`` to flatten a
    ``DevInvestigationResult``'s observations into ``DevAnswerFrame.
    health_findings`` -- one capping function, never a second copy that
    could disagree on which 50 survive.
    """

    ordered = tuple(
        sorted(
            findings,
            key=lambda finding: (
                HEALTH_FINDING_SEVERITY_RANK[finding.state],
                finding.finding_id,
            ),
        )
    )
    truncated = len(ordered) > _MAX_FINDINGS
    return ordered[:_MAX_FINDINGS], truncated


def _health_profile_content(result: HealthProfileResult) -> DevSourceContent:
    """Wire a :class:`HealthProfileResult` into ``content.health_findings``.

    ONLY ``launch_findings`` -- never ``shadow_findings``/
    ``suppressed_findings`` (CHAOS-3302's own three-way split: every rule
    shipped today is ``provisional``, so this is empty in production until
    a rule is calibration-approved, which is expected, not a bug -- see
    ``health_profile_synthesis``'s own module docstring).
    """

    capped, truncated = capped_health_findings(result.launch_findings)
    return DevSourceContent(
        schema_version="dev_source_content.v1",
        health_findings=capped,
        health_findings_truncated=truncated,
    )


def _health_profile_outcome(result: HealthProfileResult) -> StepOutcome:
    """``synthesize_health_profile`` never raises and never reports an
    "unmeasured" result of its own -- an unresolved cohort/attribution
    failure still produces a well-formed profile whose OWN rules report
    themselves unavailable/suppressed (disclosed at the finding/observation
    level, which the eventual frame builder reads directly). The step
    itself is therefore always AVAILABLE_CURRENT/queried, exactly like
    ``list_metrics_run``'s identical posture: the catalog/registry always
    "ran"; what varies is how much it found.
    """

    content = _health_profile_content(result)
    usable = len(content.health_findings)
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_CURRENT,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        query_version="health-profile-synthesis.v1",
        content=content,
    )


def capped_deficiency_findings(
    findings: tuple[DeficiencyFinding, ...],
) -> tuple[tuple[DeficiencyFinding, ...], bool]:
    """Sort ``findings`` by ``deficiency.finding_sort_key`` (severity,
    category, finding_id) -- the exact key ``OperationalDeficiencyInventory.
    findings`` is already validated against at its own contract layer, and
    ``DevSourceContent``/``DevAnswerFrame``'s ``validate_finding_order``
    check deficiency_findings against -- then cap at :data:`_MAX_FINDINGS`.

    Always re-sorts, even though a SINGLE ``OperationalDeficiencyInventory.
    findings`` tuple already arrives in this order (a no-op re-sort there):
    ``terminal_frames.wrap_legacy_answer_as_frame`` calls this over a
    FLATTENED concatenation of every observation's own
    ``content.deficiency_findings`` -- each individually sorted, but their
    concatenation is not -- so trusting pre-sorted input here would be
    correct only for today's single-observation callers and silently wrong
    the moment a plan emits more than one.
    """

    ordered = tuple(sorted(findings, key=finding_sort_key))
    truncated = len(ordered) > _MAX_FINDINGS
    return ordered[:_MAX_FINDINGS], truncated


def _deficiency_inventory_content(
    inventory: OperationalDeficiencyInventory,
) -> DevSourceContent:
    """CHAOS-3297 s3 codex full-branch review round 1 (FINDING 2, CONFIRMED
    HIGH, 2026-08-02): the original version of this function only ever
    copied ``inventory.findings`` -- ``inventory.category_statuses`` was
    discarded entirely, so eight valid UNEVALUATED categories produced
    content indistinguishable from eight genuinely-evaluated,
    genuinely-zero-finding categories. ``category_statuses`` is always
    exactly the eight closed categories at its own contract layer
    (``OperationalDeficiencyInventory``'s ``Field(min_length=8,
    max_length=8)``), so it is passed through verbatim -- never re-derived,
    never capped (nothing to cap: it is fixed-size).
    """

    capped, truncated = capped_deficiency_findings(inventory.findings)
    return DevSourceContent(
        schema_version="dev_source_content.v1",
        deficiency_findings=capped,
        deficiency_findings_truncated=truncated,
        deficiency_category_statuses=inventory.category_statuses,
    )


def _deficiency_inventory_outcome(
    inventory: OperationalDeficiencyInventory,
) -> StepOutcome:
    """Mirrors ``_health_profile_outcome``'s reasoning for the "did this
    step run at all" question, but (CHAOS-3297 s3 codex full-branch review
    round 1, FINDING 2, CONFIRMED HIGH, 2026-08-02) derives
    ``observed_state``/``limitation`` from ``inventory.category_statuses``
    rather than hardcoding ``AVAILABLE_CURRENT``/no limitation regardless
    of it -- the original version claimed a fully current result even when
    some or all categories were genuinely unevaluated, discarding that
    distinction one hop downstream of the disclosure
    ``OperationalDeficiencyService`` already computed.

    Deliberately never reports an UNMEASURED-family ``observed_state``
    (``UNAVAILABLE``/``UNCONFIGURED``/...) here, even when EVERY category is
    unevaluated: the service genuinely ran and returned a real, disclosed
    inventory -- each ``DeficiencyCategoryStatus`` IS a measured fact, with
    its own bounded reason when ``evaluated`` is ``False``. That is a
    QUERIED result, just one this step cannot claim is fully current.
    Reporting an unmeasured state here would also violate
    ``DevSourceObservation.validate_content_semantics`` (content is
    forbidden on an unmeasured observation), which would silently discard
    the very coverage block this fix exists to preserve.
    """

    content = _deficiency_inventory_content(inventory)
    usable = len(content.deficiency_findings)
    unevaluated = sorted(
        status.category.value
        for status in inventory.category_statuses
        if not status.evaluated
    )
    if not unevaluated:
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics=queried_semantics(usable),
            usable_fact_count=usable,
            query_version="deficiency-operational-inventory.v1",
            content=content,
        )
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_STALE,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        query_version="deficiency-operational-inventory.v1",
        limitation="deficiency_categories_unevaluated:" + ",".join(unevaluated),
        content=content,
    )


#: CHAOS-3393. ``status.portfolio.v1``'s own ``per_step_timeout_seconds``
#: (the plan document's contract ceiling, ``DevInvestigationPlan.
#: per_step_timeout_seconds`` admits at most 120 -- ``Field(ge=1, le=120)``).
#: A single source of truth for both the plan document field below and the
#: per-project timeout slice, so the two can never drift apart.
PORTFOLIO_PLAN_TIMEOUT_SECONDS = 120

#: The most one project's evaluation may take within a portfolio batch,
#: regardless of how few projects are in it -- a 1-project "portfolio"
#: still may not consume the whole 120s step budget on one slow project.
PORTFOLIO_PROJECT_TIMEOUT_CEILING_SECONDS = 15.0


def portfolio_project_timeout_seconds(project_count: int) -> float:
    """CHAOS-3393 budget: ``min(120 / N, 15s)`` -- the plan's own budget
    ceiling divided evenly across the batch, capped per-project so no
    single slow project can starve the rest of the batch's share of the
    step's 120s contract ceiling. ``project_count <= 0`` (defensive; the
    step never calls this with an empty batch -- see
    ``portfolio_status_evaluation_run``) falls back to the per-project
    ceiling itself.
    """

    if project_count <= 0:
        return PORTFOLIO_PROJECT_TIMEOUT_CEILING_SECONDS
    return min(
        PORTFOLIO_PLAN_TIMEOUT_SECONDS / project_count,
        PORTFOLIO_PROJECT_TIMEOUT_CEILING_SECONDS,
    )


def capped_portfolio_project_statuses(
    statuses: tuple[DevPortfolioProjectStatusV2, ...],
) -> tuple[tuple[DevPortfolioProjectStatusV2, ...], bool]:
    """Sort ``statuses`` into the canonical worst-state-first-then-
    ``project_id`` order ``DevSourceContent``/``DevAnswerFrame`` require,
    then cap at :data:`_MAX_FINDINGS` (mirrors ``capped_health_findings``
    exactly; a portfolio batch is already bounded at
    ``portfolio_status_service.MAX_PORTFOLIO_PROJECTS`` (25), well under
    this cap, but the same disclosed-truncation discipline applies
    regardless of which bound would bite first).
    """

    ordered = tuple(
        sorted(
            statuses,
            key=lambda status: (
                HEALTH_FINDING_SEVERITY_RANK[status.worst_state],
                status.project_id,
            ),
        )
    )
    truncated = len(ordered) > _MAX_FINDINGS
    return ordered[:_MAX_FINDINGS], truncated


def _bounded_portfolio_failure_reason(error: str) -> str:
    """A bounded, closed-vocabulary failure classification for one failed
    project's ``DevPortfolioProjectStatusV2.failure_reason`` -- never the
    raw ``PortfolioProjectFailure.error`` (an exception ``repr()``), which
    can carry provider/internal detail this wire contract must never
    disclose (mirrors ``no_match_terminal``'s internal-token-leak
    discipline one layer up)."""

    return "evaluation_timeout" if error == "timeout" else "evaluation_error"


def _portfolio_status_content(
    result: PortfolioStatusResult, *, labels_by_project_id: Mapping[str, str]
) -> DevSourceContent:
    """Wire a :class:`PortfolioStatusResult` into ``content.health_findings``
    (every evaluated project's own launch-eligible findings, flattened and
    re-capped -- mirrors ``_health_profile_content`` exactly, so a portfolio
    finding is indistinguishable in shape from a single-project one) PLUS
    ``content.portfolio_project_statuses`` -- the per-project rollup
    (worst state, finding count, evaluated/failed) that carries the batch's
    own per-project attribution, including a disclosed row for every
    project ``PortfolioStatusService`` could not evaluate at all (never
    silently dropped -- see ``PortfolioProjectFailure``'s own docstring).
    """

    all_findings = tuple(
        finding for project in result.projects for finding in project.launch_findings
    )
    capped_findings, health_truncated = capped_health_findings(all_findings)
    evaluated_rows = tuple(
        DevPortfolioProjectStatusV2(
            schema_version="dev_portfolio_project_status.v1",
            project_id=project.subject_id,
            display_label=labels_by_project_id.get(
                project.subject_id, project.subject_id
            ),
            worst_state=worst_project_state(project),
            finding_count=len(project.launch_findings),
            evaluated=True,
            failure_reason=None,
        )
        for project in result.projects
    )
    failed_rows = tuple(
        DevPortfolioProjectStatusV2(
            schema_version="dev_portfolio_project_status.v1",
            project_id=failure.project_id,
            display_label=labels_by_project_id.get(
                failure.project_id, failure.project_id
            ),
            worst_state=DimensionState.UNKNOWN,
            finding_count=0,
            evaluated=False,
            failure_reason=_bounded_portfolio_failure_reason(failure.error),
        )
        for failure in result.failures
    )
    portfolio_statuses, portfolio_truncated = capped_portfolio_project_statuses(
        evaluated_rows + failed_rows
    )
    return DevSourceContent(
        schema_version="dev_source_content.v1",
        health_findings=capped_findings,
        health_findings_truncated=health_truncated,
        portfolio_project_statuses=portfolio_statuses,
        portfolio_project_statuses_truncated=portfolio_truncated,
    )


def _portfolio_status_outcome(
    result: PortfolioStatusResult, *, labels_by_project_id: Mapping[str, str]
) -> StepOutcome:
    """Mirrors ``_deficiency_inventory_outcome``'s "did this step run at
    all" reasoning: ``PortfolioStatusService.evaluate_portfolio`` never
    raises for an individual project failure (isolated as a
    ``PortfolioProjectFailure``, see that service's own comment on why
    concurrent fan-out is unsafe), so this step is always QUERIED --
    AVAILABLE_CURRENT when every project in the batch evaluated, or
    AVAILABLE_STALE (never an UNMEASURED-family state) with a disclosed,
    bounded limitation naming which projects failed, when one or more did.
    Never fabricates a status for a failed project and never refuses the
    whole batch over a partial failure (CHAOS-3393 semantics).
    """

    content = _portfolio_status_content(
        result, labels_by_project_id=labels_by_project_id
    )
    usable = len(content.health_findings)
    if not result.failures:
        return StepOutcome(
            observed_state=SourceRequirementState.AVAILABLE_CURRENT,
            data_semantics=queried_semantics(usable),
            usable_fact_count=usable,
            query_version="portfolio-status-evaluation.v1",
            content=content,
        )
    failed_ids = ",".join(sorted(failure.project_id for failure in result.failures))
    return StepOutcome(
        observed_state=SourceRequirementState.AVAILABLE_STALE,
        data_semantics=queried_semantics(usable),
        usable_fact_count=usable,
        query_version="portfolio-status-evaluation.v1",
        limitation="portfolio_projects_failed:" + failed_ids,
        content=content,
    )


def register_wave_3_1_steps(
    registry: StepRegistry,
    *,
    project_health: _ProjectHealthEvaluator,
    team_health: _TeamHealthEvaluator,
    team_workload: _TeamWorkloadEvaluator,
    operational_deficiency: _OperationalDeficiencyEvaluator,
    portfolio_status: _PortfolioStatusEvaluator,
) -> None:
    """Populate ``registry`` with every step :data:`WAVE_3_1_PLANS_BY_INTENT`
    declares. Called alongside (never instead of)
    ``builtin_steps.register_builtin_steps`` against the SAME registry
    instance -- ``registry_validation.validate_registry`` only requires
    that every step a plan declares has a matching registration under
    that plan's own ``plan_id``; a step registered under a different
    plan_id (the six core plans') is explicitly not a conflict (see that
    module's own comment).
    """

    async def health_evaluation_project_run(ctx: StepContext) -> StepOutcome:
        result = await project_health.evaluate_project(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
            now=ctx.now,
        )
        return _health_profile_outcome(result)

    async def health_evaluation_team_run(ctx: StepContext) -> StepOutcome:
        team_id = ctx.scope.team_ids[0]
        result = await team_health.evaluate_team(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
            team_id=team_id,
            now=ctx.now,
        )
        return _health_profile_outcome(result)

    async def workload_evaluation_run(ctx: StepContext) -> StepOutcome:
        team_id = ctx.scope.team_ids[0]
        result = await team_workload.evaluate_workload(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            scope=ctx.scope,
            team_id=team_id,
            now=ctx.now,
        )
        return _health_profile_outcome(result)

    async def deficiency_evaluation_run(ctx: StepContext) -> StepOutcome:
        # deficiency.operational.v1 supports both PROJECT and TEAM subject
        # kinds (OperationalDeficiencyService.evaluate_project/evaluate_team
        # -- see the plan document below); the step branches on the
        # committed scope's own direct_scope, exactly like
        # OperationalDeficiencyService's own two entry points do.
        if ctx.scope.direct_scope is DirectScope.TEAM:
            team_id = ctx.scope.team_ids[0]
            inventory = await operational_deficiency.evaluate_team(
                org_id=ctx.org_id,
                permission_fingerprint=ctx.permission_fingerprint,
                scope=ctx.scope,
                team_id=team_id,
                now=ctx.now,
            )
        else:
            inventory = await operational_deficiency.evaluate_project(
                org_id=ctx.org_id,
                permission_fingerprint=ctx.permission_fingerprint,
                scope=ctx.scope,
                now=ctx.now,
            )
        return _deficiency_inventory_outcome(inventory)

    async def portfolio_status_evaluation_run(ctx: StepContext) -> StepOutcome:
        # CHAOS-3393: batches over ctx.subject_set_scopes -- the several
        # committed per-project scopes the orchestrator's PLURAL_COHORT/
        # ORGANIZATION_WIDE gate built from the preflight-committed
        # DevSubjectSet, never ctx.scope itself (which stays the single
        # org-level authorized scope for the whole run -- see StepContext's
        # own docstring). registry_validation/orchestrator gating both
        # guarantee this is non-empty by the time a step actually runs
        # (plan_eligible requires a committed, >=1-entry subject_set).
        scopes = ctx.subject_set_scopes
        labels_by_project_id = {
            scope.entity_refs[0].entity_id: scope.entity_refs[0].display_label
            for scope in scopes
            if scope.entity_refs
        }
        projects = tuple(PortfolioProjectScope(scope=scope) for scope in scopes)
        result = await portfolio_status.evaluate_portfolio(
            org_id=ctx.org_id,
            permission_fingerprint=ctx.permission_fingerprint,
            projects=projects,
            now=ctx.now,
            per_project_timeout_seconds=portfolio_project_timeout_seconds(
                len(projects)
            ),
        )
        return _portfolio_status_outcome(
            result, labels_by_project_id=labels_by_project_id
        )

    registry.register(
        PlanStepDefinition(
            step_id="health_evaluation",
            plan_id="health.project.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="project_health_service.evaluate_project.v1",
            requirement_level="mandatory",
            run=health_evaluation_project_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="health_evaluation",
            plan_id="health.team.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="team_health_service.evaluate_team.v1",
            requirement_level="mandatory",
            run=health_evaluation_team_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="workload_evaluation",
            plan_id="balance.team_workload.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="team_workload_service.evaluate_workload.v1",
            requirement_level="mandatory",
            run=workload_evaluation_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="deficiency_evaluation",
            plan_id="deficiency.operational.v1",
            source_class=SourceClass.DEFICIENCY_INVENTORY,
            adapter_id="operational_deficiency_service.evaluate.v1",
            requirement_level="mandatory",
            run=deficiency_evaluation_run,
        )
    )
    registry.register(
        PlanStepDefinition(
            step_id="portfolio_status_evaluation",
            plan_id="status.portfolio.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="portfolio_status_service.evaluate_portfolio.v1",
            requirement_level="mandatory",
            run=portfolio_status_evaluation_run,
        )
    )


_HEALTH_PROJECT = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="health.project.v1",
    plan_version="health.project.v1.0",
    intent_id=QuestionIntentID.PROJECT_HEALTH,
    supported_subject_kinds=(EntityKind.PROJECT,),
    supported_cardinalities=(Cardinality.SINGULAR,),
    mandatory_steps=("health_evaluation",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="project_health_service.evaluate_project.v1",
            requirement_level="mandatory",
            freshness_policy="health_profile_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="health_profile_synthesis.no_completion_concept",
    completion_rule_version="1",
)

_HEALTH_TEAM = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="health.team.v1",
    plan_version="health.team.v1.0",
    intent_id=QuestionIntentID.TEAM_HEALTH,
    supported_subject_kinds=(EntityKind.TEAM,),
    supported_cardinalities=(Cardinality.SINGULAR,),
    mandatory_steps=("health_evaluation",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="team_health_service.evaluate_team.v1",
            requirement_level="mandatory",
            freshness_policy="health_profile_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="health_profile_synthesis.no_completion_concept",
    completion_rule_version="1",
)

_BALANCE_TEAM_WORKLOAD = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="balance.team_workload.v1",
    plan_version="balance.team_workload.v1.0",
    intent_id=QuestionIntentID.TEAM_WORKLOAD_BALANCE,
    supported_subject_kinds=(EntityKind.TEAM,),
    supported_cardinalities=(Cardinality.SINGULAR,),
    mandatory_steps=("workload_evaluation",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="team_workload_service.evaluate_workload.v1",
            requirement_level="mandatory",
            freshness_policy="health_profile_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="health_profile_synthesis.no_completion_concept",
    completion_rule_version="1",
)

_DEFICIENCY_OPERATIONAL = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="deficiency.operational.v1",
    plan_version="deficiency.operational.v1.0",
    intent_id=QuestionIntentID.OPERATIONAL_DEFICIENCY_INVENTORY,
    supported_subject_kinds=(EntityKind.PROJECT, EntityKind.TEAM),
    supported_cardinalities=(Cardinality.SINGULAR,),
    mandatory_steps=("deficiency_evaluation",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.DEFICIENCY_INVENTORY,
            adapter_id="operational_deficiency_service.evaluate.v1",
            requirement_level="mandatory",
            freshness_policy="deficiency_inventory_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=200,
    max_bytes_per_step=131_072,
    enrichment_allowed=True,
    completion_rule_id="operational_deficiency_service.no_completion_concept",
    completion_rule_version="1",
)

#: CHAOS-3393. A cohort/org-wide batch of ``ProjectHealthService.
#: evaluate_project`` calls, never a per-project SINGULAR commit --
#: ``PROJECT`` only (the preflight gate this plan relies on -- see
#: ``subject_preflight.py``'s homogeneous-cohort branch -- only PROCEEDs a
#: PROJECT-kind cohort for this intent; every other kind still terminates
#: UNSUPPORTED). ``batched_fan_out`` names the plan's own batching
#: strategy; the executor still runs this ONE mandatory step exactly like
#: any SINGULAR plan's -- ``PortfolioStatusService.evaluate_portfolio``
#: does the actual per-project batching, sequentially, over the several
#: scopes ``StepContext.subject_set_scopes`` carries (see that service's
#: own docstring for why concurrent fan-out over the shared production
#: runtime is unsafe).
_STATUS_PORTFOLIO = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="status.portfolio.v1",
    plan_version="status.portfolio.v1.0",
    intent_id=QuestionIntentID.PORTFOLIO_STATUS,
    supported_subject_kinds=(EntityKind.PROJECT,),
    supported_cardinalities=(Cardinality.PLURAL_COHORT, Cardinality.ORGANIZATION_WIDE),
    mandatory_steps=("portfolio_status_evaluation",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.HEALTH_PROFILE,
            adapter_id="portfolio_status_service.evaluate_portfolio.v1",
            requirement_level="mandatory",
            freshness_policy="health_profile_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="batched_fan_out",
    per_step_timeout_seconds=PORTFOLIO_PLAN_TIMEOUT_SECONDS,
    max_rows_per_step=200,
    max_bytes_per_step=131_072,
    enrichment_allowed=True,
    completion_rule_id="portfolio_status_service.no_completion_concept",
    completion_rule_version="1",
)

#: Every wave-3.1-extension plan document this module defines, keyed by
#: the intent it governs. Mirrors ``plan_documents.CORE_PLANS_BY_INTENT``'s
#: own shape exactly, including the PLAN_ID_BY_INTENT cross-check below.
WAVE_3_1_PLANS_BY_INTENT: dict[QuestionIntentID, DevInvestigationPlan] = {
    plan.intent_id: plan
    for plan in (
        _HEALTH_PROJECT,
        _HEALTH_TEAM,
        _BALANCE_TEAM_WORKLOAD,
        _DEFICIENCY_OPERATIONAL,
        _STATUS_PORTFOLIO,
    )
}

_plan_id_mismatches = sorted(
    f"{intent.value}: plan declares {plan.plan_id!r}, "
    f"PLAN_ID_BY_INTENT says {PLAN_ID_BY_INTENT[intent]!r}"
    for intent, plan in WAVE_3_1_PLANS_BY_INTENT.items()
    if plan.plan_id != PLAN_ID_BY_INTENT[intent]
)
if _plan_id_mismatches:
    raise RuntimeError(
        f"wave_3_1 plan_id disagrees with PLAN_ID_BY_INTENT: {_plan_id_mismatches}"
    )

#: CHAOS-3337: every SourceClass the six core plans PLUS this module's four
#: can emit, checked against persistence's own allowlist at THIS module's
#: import time -- see ``_source_classes_missing_from_persistence_allowlist``'s
#: own docstring. Covers both registries (not just this module's own) since
#: either side could add a new SourceClass a plan's steps emit.
_missing_from_persistence_allowlist = (
    _source_classes_missing_from_persistence_allowlist(
        {**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT},
        allowlist=_PERSISTENCE_SOURCE_CLASSES,
    )
)
if _missing_from_persistence_allowlist:
    raise RuntimeError(
        "SourceClass(es) "
        f"{sorted(_missing_from_persistence_allowlist)} are emitted by a "
        "registered plan's source_requirements, but "
        "persistence.service._SOURCE_CLASSES does not allow them -- every "
        "observation for this source class would be rejected at write "
        "time with DevPersistenceValidationError('invalid source_class') "
        "(CHAOS-3337)"
    )

#: The five question classes this module wires (CHAOS-3393 adds
#: ``PORTFOLIO_STATUS`` -- previously deliberately excluded, see the
#: module docstring).
WAVE_3_1_QUESTION_INTENT_IDS: frozenset[QuestionIntentID] = frozenset(
    WAVE_3_1_PLANS_BY_INTENT.keys()
)


def build_registry_with_wave_3_1(
    runtime: PlanExecutorRuntime,
    *,
    project_health: _ProjectHealthEvaluator,
    team_health: _TeamHealthEvaluator,
    team_workload: _TeamWorkloadEvaluator,
    operational_deficiency: _OperationalDeficiencyEvaluator,
    portfolio_status: _PortfolioStatusEvaluator,
) -> StepRegistry:
    """The registry-construction entry point ``production_runtime.py``
    calls: the six CHAOS-3295 core plans' steps PLUS this module's five,
    registered into ONE shared :class:`StepRegistry` and validated
    TOGETHER -- never two separate registries, so a plan_id collision or a
    step registered under the wrong plan between the two groups fails
    construction here rather than at the first request that reaches it.

    Deliberately does NOT modify ``build_default_registry`` or
    ``plan_documents.py`` (CHAOS-3295's own promise to CHAOS-3303/3304/3305:
    "register ... without needing to change this module or the
    orchestrator") -- this function composes the existing entry point with
    this module's own registration, both against the SAME registry
    instance.
    """

    registry = StepRegistry()
    register_builtin_steps(registry, runtime)
    register_wave_3_1_steps(
        registry,
        project_health=project_health,
        team_health=team_health,
        team_workload=team_workload,
        operational_deficiency=operational_deficiency,
        portfolio_status=portfolio_status,
    )
    validate_registry(
        plans_by_intent={**CORE_PLANS_BY_INTENT, **WAVE_3_1_PLANS_BY_INTENT},
        steps=registry,
        core_intents=CORE_QUESTION_INTENT_IDS | WAVE_3_1_QUESTION_INTENT_IDS,
    )
    return registry
