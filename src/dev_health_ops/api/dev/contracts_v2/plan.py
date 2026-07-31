"""``dev_investigation_plan.v1`` and ``dev_source_requirement.v1``.

Amendment TRD v2 §4.3 and §5 (plan registry). The plan registry itself
(``status.entity.v2``, ``status.portfolio.v1``, ``health.project.v1``,
``health.team.v1``, ``balance.team_workload.v1``,
``deficiency.operational.v1``, ...) is populated by the consuming
orchestrator issues; this module only defines the wire shape a plan
document must satisfy.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, FiniteFloat, model_validator

from .base import (
    Cardinality,
    ContractModelV2,
    EntityKind,
    OpaqueID,
    PlatformVersionToken,
    QuestionIntentID,
    ShortText,
    SourceClass,
    Version,
)

__all__ = [
    "PLAN_REGISTRY",
    "DevInvestigationPlan",
    "DevPlanStepDependency",
    "DevSourceRequirement",
    "PlanRegistryID",
]

RequirementLevel = Literal["mandatory", "conditional", "optional", "not_applicable"]

#: The Wave 3.1 plan registry: one server-owned plan per launch intent
#: (``base.QuestionIntentID``). Closed rather than open because a plan ID is
#: echoed on ``dev_answer_frame.v1.versions`` and therefore reaches the wire
#: on outcomes that must disclose nothing about the subject; the identifier a
#: producer *would* choose is exactly the channel round-3 adversarial review
#: used. A new plan is added by adding a member here.
#:
#: The registry is the *vocabulary*; the plan documents themselves (steps,
#: source requirements, budgets) are still authored by the consuming
#: orchestrator issues, which validate against ``DevInvestigationPlan``.
PLAN_REGISTRY: frozenset[str] = frozenset(
    {
        "status.entity.v2",
        "status.portfolio.v1",
        "work.remaining.v1",
        "change.observed.v1",
        "statistics.registered.v1",
        "metric.comparison.v1",
        "trust.data.v1",
        "health.project.v1",
        "health.team.v1",
        "balance.team_workload.v1",
        "deficiency.operational.v1",
        "investigation.bounded.v1",
    }
)

#: The plan-registry token grammar, applied at the type level. Membership of
#: ``PLAN_REGISTRY`` is the stronger, *closed* check and is applied where a
#: plan ID reaches a no-answer projection (see ``validators``).
PlanRegistryID = PlatformVersionToken


class DevSourceRequirement(ContractModelV2):
    schema_version: Literal["dev_source_requirement.v1"]
    source_class: SourceClass
    adapter_id: OpaqueID
    requirement_level: RequirementLevel
    applicability_rule_id: OpaqueID | None = None
    applicability_rule_version: Version | None = None
    freshness_policy: ShortText
    minimum_usable_facts: int = Field(ge=0, le=1_000)
    minimum_sample: int | None = Field(default=None, ge=0, le=100_000)
    minimum_coverage: FiniteFloat | None = Field(default=None, ge=0, le=1)
    allowed_relationship_paths: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=10
    )

    @model_validator(mode="after")
    def validate_applicability(self) -> Self:
        needs_rule = self.requirement_level == "conditional"
        has_rule = (
            self.applicability_rule_id is not None
            or self.applicability_rule_version is not None
        )
        if needs_rule and not (
            self.applicability_rule_id is not None
            and self.applicability_rule_version is not None
        ):
            raise ValueError(
                "conditional requirements need an applicability rule id+version"
            )
        if not needs_rule and has_rule:
            raise ValueError(
                "only conditional requirements may carry an applicability rule"
            )
        return self


class DevPlanStepDependency(ContractModelV2):
    step_id: ShortText
    depends_on: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=10)


class DevInvestigationPlan(ContractModelV2):
    schema_version: Literal["dev_investigation_plan.v1"]
    plan_id: PlanRegistryID
    plan_version: PlatformVersionToken
    intent_id: QuestionIntentID
    supported_subject_kinds: tuple[EntityKind, ...] = Field(min_length=1, max_length=6)
    supported_cardinalities: tuple[Cardinality, ...] = Field(min_length=1, max_length=3)
    mandatory_steps: tuple[ShortText, ...] = Field(min_length=1, max_length=25)
    conditional_steps: tuple[ShortText, ...] = Field(
        default_factory=tuple, max_length=25
    )
    step_dependencies: tuple[DevPlanStepDependency, ...] = Field(
        default_factory=tuple, max_length=50
    )
    source_requirements: tuple[DevSourceRequirement, ...] = Field(
        min_length=1, max_length=25
    )
    batch_strategy: Literal["single", "batched_fan_out"]
    per_step_timeout_seconds: int = Field(ge=1, le=120)
    max_rows_per_step: int = Field(ge=1, le=100_000)
    max_bytes_per_step: int = Field(ge=1, le=10_000_000)
    max_sample_per_step: int | None = Field(default=None, ge=1, le=100_000)
    enrichment_allowed: bool
    completion_rule_id: OpaqueID
    completion_rule_version: Version

    @model_validator(mode="after")
    def validate_plan_invariants(self) -> Self:
        mandatory = set(self.mandatory_steps)
        conditional = set(self.conditional_steps)
        if len(mandatory) != len(self.mandatory_steps):
            raise ValueError("mandatory steps must be unique")
        if len(conditional) != len(self.conditional_steps):
            raise ValueError("conditional steps must be unique")
        if mandatory & conditional:
            raise ValueError("a step cannot be both mandatory and conditional")
        known_steps = mandatory | conditional
        declared_dependency_ids = {dep.step_id for dep in self.step_dependencies}
        if len(declared_dependency_ids) != len(self.step_dependencies):
            raise ValueError("step dependency declarations must be unique per step")
        for dependency in self.step_dependencies:
            if dependency.step_id not in known_steps:
                raise ValueError("step dependency declared for a step not in the plan")
            if not set(dependency.depends_on) <= known_steps:
                raise ValueError("step dependency references an unknown step")
            if dependency.step_id in dependency.depends_on:
                raise ValueError("a step cannot depend on itself")
        source_classes = [
            (req.source_class, req.adapter_id) for req in self.source_requirements
        ]
        if len(source_classes) != len(set(source_classes)):
            raise ValueError(
                "source requirements must be unique per source/adapter pair"
            )
        return self
