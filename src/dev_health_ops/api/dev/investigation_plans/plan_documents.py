"""The six Wave 3.1 core investigation plans (CHAOS-3295 Amendment TRD v2 §4.3).

Each :class:`~dev_health_ops.api.dev.contracts_v2.plan.DevInvestigationPlan`
here corresponds 1:1 to one of the six launch intents that already exist as
``QuestionIntentID`` members and one of the ``PLAN_REGISTRY`` vocabulary
tokens (both landed by CHAOS-3294); this module is the first thing that
actually *populates* that vocabulary, per its own docstring ("the plan
registry itself ... is populated by the consuming orchestrator issues").

Portfolio/project-health/team-health/workload/operational-deficiency plans
(``status.portfolio.v1``, ``health.project.v1``, ``health.team.v1``,
``balance.team_workload.v1``, ``deficiency.operational.v1``) are explicitly
*not* built here -- CHAOS-3303/3304/3305 register those plans and their
steps against the same :class:`~.steps.StepRegistry` this module uses,
without needing to change this module or the orchestrator.
"""

from __future__ import annotations

from ..contracts_v2.base import Cardinality, EntityKind, QuestionIntentID, SourceClass
from ..contracts_v2.plan import (
    DevInvestigationPlan,
    DevPlanStepDependency,
    DevSourceRequirement,
)
from ..preflight_outcomes import PLAN_ID_BY_INTENT

__all__ = ["CORE_PLANS_BY_INTENT", "CORE_QUESTION_INTENT_IDS"]

_ALL_SUBJECT_KINDS: tuple[EntityKind, ...] = (
    EntityKind.REPOSITORY,
    EntityKind.PROJECT,
    EntityKind.WORK_UNIT,
    EntityKind.ISSUE,
    EntityKind.PULL_REQUEST,
    EntityKind.TEAM,
)

_STATUS_ENTITY = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="status.entity.v2",
    plan_version="status.entity.v2.1",
    intent_id=QuestionIntentID.ENTITY_STATUS,
    supported_subject_kinds=_ALL_SUBJECT_KINDS,
    supported_cardinalities=(Cardinality.SINGULAR,),
    mandatory_steps=("status_snapshot", "required_source_health"),
    conditional_steps=("work_graph_expansion", "evidence_expansion"),
    step_dependencies=(
        DevPlanStepDependency(
            step_id="work_graph_expansion", depends_on=("status_snapshot",)
        ),
        DevPlanStepDependency(
            step_id="evidence_expansion", depends_on=("status_snapshot",)
        ),
    ),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.status_snapshot.v1",
            requirement_level="mandatory",
            freshness_policy="status_snapshot_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            freshness_policy="data_health_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="work_graph_neighbors_service.neighbors.v1",
            requirement_level="conditional",
            applicability_rule_id="work_graph_expansion_required.v1",
            applicability_rule_version="1",
            freshness_policy="work_graph_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="evidence_service.search.v1",
            requirement_level="conditional",
            applicability_rule_id="prioritized_evidence_required.v1",
            applicability_rule_version="1",
            freshness_policy="evidence_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="status_change_service.actual_completion",
    completion_rule_version="actual-completion.v4",
)

_WORK_REMAINING = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="work.remaining.v1",
    plan_version="work.remaining.v1.0",
    intent_id=QuestionIntentID.REMAINING_WORK,
    supported_subject_kinds=_ALL_SUBJECT_KINDS,
    supported_cardinalities=(Cardinality.SINGULAR,),
    mandatory_steps=("status_snapshot",),
    conditional_steps=("work_graph_expansion",),
    step_dependencies=(
        DevPlanStepDependency(
            step_id="work_graph_expansion", depends_on=("status_snapshot",)
        ),
    ),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.status_snapshot.v1",
            requirement_level="mandatory",
            freshness_policy="status_snapshot_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.WORK_GRAPH,
            adapter_id="work_graph_neighbors_service.neighbors.v1",
            requirement_level="conditional",
            applicability_rule_id="work_graph_expansion_required.v1",
            applicability_rule_version="1",
            freshness_policy="work_graph_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="status_change_service.actual_completion",
    completion_rule_version="actual-completion.v4",
)

_CHANGE_OBSERVED = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="change.observed.v1",
    plan_version="change.observed.v1.0",
    intent_id=QuestionIntentID.OBSERVED_CHANGE,
    supported_subject_kinds=_ALL_SUBJECT_KINDS,
    supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
    mandatory_steps=("change_summary", "required_source_health"),
    conditional_steps=("registered_metric_deltas",),
    step_dependencies=(
        DevPlanStepDependency(
            step_id="registered_metric_deltas", depends_on=("change_summary",)
        ),
    ),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.STATUS_CHANGE,
            adapter_id="status_change_service.change_summary.v1",
            requirement_level="mandatory",
            freshness_policy="change_summary_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            freshness_policy="data_health_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.query_metric.v1",
            requirement_level="conditional",
            applicability_rule_id="registered_metrics_present.v1",
            applicability_rule_version="1",
            freshness_policy="metric_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="status_change_service.change_summary",
    completion_rule_version="1",
)

_STATISTICS_REGISTERED = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="statistics.registered.v1",
    plan_version="statistics.registered.v1.0",
    intent_id=QuestionIntentID.REGISTERED_STATISTICS,
    supported_subject_kinds=_ALL_SUBJECT_KINDS,
    supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
    mandatory_steps=("list_metrics",),
    conditional_steps=("readiness_data_health",),
    step_dependencies=(
        DevPlanStepDependency(
            step_id="readiness_data_health", depends_on=("list_metrics",)
        ),
    ),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.list_metrics.v1",
            requirement_level="mandatory",
            freshness_policy="metric_catalog_freshness.v1",
            minimum_usable_facts=0,
        ),
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="conditional",
            applicability_rule_id="usability_requested.v1",
            applicability_rule_version="1",
            freshness_policy="data_health_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=True,
    completion_rule_id="metrics.list_metrics",
    completion_rule_version="1",
)

_METRIC_COMPARISON = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="metric.comparison.v1",
    plan_version="metric.comparison.v1.0",
    intent_id=QuestionIntentID.METRIC_COMPARISON,
    supported_subject_kinds=_ALL_SUBJECT_KINDS,
    supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
    mandatory_steps=("registered_metric_query",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.WORK_ITEM,
            adapter_id="metrics.query_metric.v1",
            requirement_level="mandatory",
            freshness_policy="metric_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="batched_fan_out",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    max_sample_per_step=100_000,
    enrichment_allowed=False,
    completion_rule_id="metrics.query_metric",
    completion_rule_version="1",
)

_TRUST_DATA = DevInvestigationPlan(
    schema_version="dev_investigation_plan.v1",
    plan_id="trust.data.v1",
    plan_version="trust.data.v1.0",
    intent_id=QuestionIntentID.DATA_TRUST,
    supported_subject_kinds=_ALL_SUBJECT_KINDS,
    supported_cardinalities=(Cardinality.SINGULAR, Cardinality.ORGANIZATION_WIDE),
    mandatory_steps=("required_source_health",),
    conditional_steps=(),
    step_dependencies=(),
    source_requirements=(
        DevSourceRequirement(
            schema_version="dev_source_requirement.v1",
            source_class=SourceClass.SOURCE_HEALTH,
            adapter_id="data_health_service.inspect.v1",
            requirement_level="mandatory",
            freshness_policy="data_health_freshness.v1",
            minimum_usable_facts=0,
        ),
    ),
    batch_strategy="single",
    per_step_timeout_seconds=15,
    max_rows_per_step=100,
    max_bytes_per_step=65_536,
    enrichment_allowed=False,
    completion_rule_id="data_health_service.complete_eligible",
    completion_rule_version="1",
)

#: Every core-question-class plan document, keyed by the intent it governs.
#: 1:1 by construction -- see ``registry_validation.validate_registry`` for
#: the check that every ``CORE_QUESTION_INTENT_IDS`` member is present.
CORE_PLANS_BY_INTENT: dict[QuestionIntentID, DevInvestigationPlan] = {
    plan.intent_id: plan
    for plan in (
        _STATUS_ENTITY,
        _WORK_REMAINING,
        _CHANGE_OBSERVED,
        _STATISTICS_REGISTERED,
        _METRIC_COMPARISON,
        _TRUST_DATA,
    )
}

# Build on `preflight_outcomes.PLAN_ID_BY_INTENT` rather than duplicating its
# intent->plan_id association: that table is the one already reached from the
# preflight-termination path (a denied answer's `versions.plan_id` discloses
# "which plan would have run" without running it), so every plan_id minted
# here must agree with it, checked at import time rather than left to drift.
_plan_id_mismatches = sorted(
    f"{intent.value}: plan declares {plan.plan_id!r}, "
    f"PLAN_ID_BY_INTENT says {PLAN_ID_BY_INTENT[intent]!r}"
    for intent, plan in CORE_PLANS_BY_INTENT.items()
    if plan.plan_id != PLAN_ID_BY_INTENT[intent]
)
if _plan_id_mismatches:
    raise RuntimeError(
        f"core plan_id disagrees with PLAN_ID_BY_INTENT: {_plan_id_mismatches}"
    )

#: The Wave 3.1 "core question classes" this issue is scoped to (Amendment
#: TRD v2 §4.3's "Core plans" list). Portfolio/health/workload/deficiency
#: intents are deliberately excluded -- CHAOS-3303/3304/3305 register those.
CORE_QUESTION_INTENT_IDS: frozenset[QuestionIntentID] = frozenset(
    CORE_PLANS_BY_INTENT.keys()
)
