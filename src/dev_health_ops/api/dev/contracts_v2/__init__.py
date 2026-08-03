"""Ask Dev Wave 3.1 v2 wire contracts (CHAOS-3294).

These Pydantic models are the source of truth for the checked-in Draft
2020-12 schemas under ``contracts/ask-dev/v2``, mirroring how
``dev_health_ops.api.dev.contracts`` (v1) backs ``contracts/ask-dev/v1``.
Runtime services may adapt domain objects into these models, but must not
redeclare their wire shape. v1 contracts are untouched: this package is
purely additive, alongside the v1 module, per CHAOS-3294 (this issue lands
*contracts only* — orchestrator/router/scope_service wiring is out of
scope; see downstream Wave 3.1 issues CHAOS-3292/3295/3297/3301).

See ``docs/contribute/architecture/ask-dev-contracts-v2.md`` for the full
contract map, the TRD/PRD cross-references, and the list of TRD
ambiguities this package resolved.
"""

from __future__ import annotations

from .answer import DevAnswerV2
from .base import (
    ANSWERED_OUTCOMES,
    EMPTY_CONTENT_OUTCOMES,
    Cardinality,
    ContractModelV2,
    EntityKind,
    EvidenceHandle,
    FactDisclosure,
    NarrativeFailureCode,
    PublicOutcome,
    QuestionIntentID,
    ServerHandle,
    SourceClass,
    SourceRequirementState,
)
from .compat import project_answer_v2_to_v1
from .deficiency import (
    DEFICIENCY_CATEGORIES,
    DeficiencyCategory,
    DeficiencyCategoryStatus,
    DeficiencyEvidenceClassification,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
    OperationalDeficiencyInventory,
    finding_sort_key,
)
from .embedded import (
    DevCIFactV2,
    DevCoverageV2,
    DevDeploymentFactV2,
    DevErrorV2,
    DevEvidenceRefV2,
    DevGraphEdgeV2,
    DevIncidentFactV2,
    DevMetricRefV2,
    DevPullRequestFactV2,
    DevRequiredChildFactV2,
    DevScopeV2,
    DevStatusFactV2,
    DevSurfaceContextV2,
)
from .frame import (
    DevAnswerFact,
    DevAnswerFrame,
    DevAnswerSection,
    DevComparisonPoint,
    DevCompletionBlock,
    DevFrameConflict,
    DevFrameVersions,
    DevReadinessBlock,
)
from .health_rules import (
    CalibrationRecord,
    CalibrationState,
    DimensionObservation,
    DimensionState,
    HealthDimension,
    HealthRuleDefinition,
    HealthRuleFinding,
    RuleApplicability,
    RuleDirection,
    TeamQualificationBasis,
    TeamQualificationResult,
)
from .intent import DevMessageRequestV2, DevQuestionIntent
from .narrative import DevNarrative
from .plan import (
    PLAN_REGISTRY,
    DevInvestigationPlan,
    DevPlanStepDependency,
    DevSourceRequirement,
    PlanRegistryID,
)
from .result import (
    DevInvestigationResult,
    DevObservedChangeV2,
    DevRelationshipPath,
    DevSourceContent,
    DevSourceObservation,
)
from .stream import (
    DevStreamEventV2,
    ProgressStateV2,
    StreamEventTypeV2,
    validate_stream_v2,
)
from .subject import (
    DevEntityRefV2,
    DevResolutionCandidate,
    DevResolutionEntry,
    DevResolutionLedger,
    DevSubjectMention,
    DevSubjectSet,
    ResolutionOutcome,
    validate_ledger_extends,
)
from .validators import (
    CANONICAL_NO_ANSWER_COPY,
    NO_ANSWER_ANSWER_FIELD_POLICY,
    NO_ANSWER_FRAME_FIELD_POLICY,
    NO_ANSWER_OUTCOMES,
    SOURCE_CLASS_VOCABULARY,
    NoAnswerFieldPolicy,
    scan_public_text,
    validate_completion_denominator,
    validate_narrative_fact_references,
    validate_narrative_frame_consistency,
    validate_no_answer_content_leaks,
    validate_no_answer_projection,
    validate_no_internal_leakage,
    validate_outcome_consistency,
    validate_plan_registry_membership,
    validate_relationship_refs_within_frame,
    validate_structural_closure,
    validate_versions_presence,
)

#: Every top-level, independently versioned v2 wire contract, keyed by its
#: `schema_version` value — mirrors `dev_health_ops.api.dev.contracts.CONTRACT_MODELS`.
#:
#: The CHAOS-3302 health-rule governance contracts
#: (``HealthRuleDefinition``/``DimensionObservation``/``HealthRuleFinding``/
#: ``TeamQualificationResult``/``CalibrationRecord``) and the CHAOS-3305
#: operational-deficiency-inventory contracts
#: (``DeficiencyFinding``/``DeficiencyCategoryStatus``/
#: ``OperationalDeficiencyInventory``/``DeficiencyRemediation``) are
#: deliberately NOT registered here. This registry backs the Ask Dev
#: answer-frame contract family specifically: ``export_contracts_v2``'s
#: checked-in schema/fixture export, and the no-answer-projection totality
#: check (``no_answer_policy.assert_no_answer_policy_is_total`` /
#: ``test_round4_every_v2_identifier_is_classified``) that walks every
#: member of this dict looking for the answer frame's own disclosure
#: surface. Health rules and deficiency findings are separate, code-owned
#: governance contract families (health rules have their own manifest,
#: ``health_rule_manifest.v1`` — see ``health_rule_manifest.py``) and are
#: never embedded in, or projected through, a no-answer outcome directly
#: -- the frame only ever carries them as opaque ``OpaqueID`` pointers
#: (``finding_refs``/``deficiency_refs``, ``frame.py``), so they do not
#: belong in this dict.
#:
#: They ARE imported directly above (mirroring the ``health_rules`` import
#: a few lines up), which is the load-bearing half of "registered": the
#: reflection sweeps in ``test_contracts_v2.py`` (``_all_v2_contract_models``,
#: which walks ``ContractModelV2.__subclasses__()``) only discover a model
#: once *something* has imported its module into the process. Importing
#: these types here — at ``contracts_v2`` package-init time, which every
#: consumer (including every test file that does ``import
#: dev_health_ops.api.dev.contracts_v2 as v2``) already triggers — makes
#: that discovery unconditional rather than an accident of test collection
#: order (Codex finding, 2026-08-02: without this import,
#: ``test_round4_every_v2_identifier_is_classified`` silently skipped every
#: deficiency contract field when run in isolation, passing only because
#: some *other* test file happened to import ``contracts_v2.deficiency``
#: first during full-suite collection).
CONTRACT_MODELS_V2: dict[str, type[ContractModelV2]] = {
    "dev_message_request.v2": DevMessageRequestV2,
    "dev_question_intent.v1": DevQuestionIntent,
    "dev_subject_mention.v1": DevSubjectMention,
    "dev_resolution_ledger.v1": DevResolutionLedger,
    "dev_subject_set.v1": DevSubjectSet,
    "dev_source_requirement.v1": DevSourceRequirement,
    "dev_investigation_plan.v1": DevInvestigationPlan,
    "dev_source_observation.v1": DevSourceObservation,
    "dev_investigation_result.v1": DevInvestigationResult,
    "dev_answer_frame.v1": DevAnswerFrame,
    "dev_narrative.v1": DevNarrative,
    "dev_answer.v2": DevAnswerV2,
    "dev_stream_event.v2": DevStreamEventV2,
}

__all__ = [
    "ANSWERED_OUTCOMES",
    "CANONICAL_NO_ANSWER_COPY",
    "CONTRACT_MODELS_V2",
    "DEFICIENCY_CATEGORIES",
    "PLAN_REGISTRY",
    "SOURCE_CLASS_VOCABULARY",
    "CalibrationRecord",
    "CalibrationState",
    "Cardinality",
    "ContractModelV2",
    "DeficiencyCategory",
    "DeficiencyCategoryStatus",
    "DeficiencyEvidenceClassification",
    "DeficiencyFinding",
    "DeficiencyRemediation",
    "DeficiencySeverity",
    "DimensionObservation",
    "DimensionState",
    "DevAnswerFact",
    "DevAnswerFrame",
    "DevAnswerSection",
    "DevAnswerV2",
    "DevCompletionBlock",
    "DevCIFactV2",
    "DevComparisonPoint",
    "DevCoverageV2",
    "DevDeploymentFactV2",
    "DevEntityRefV2",
    "DevErrorV2",
    "DevEvidenceRefV2",
    "DevFrameConflict",
    "DevFrameVersions",
    "DevGraphEdgeV2",
    "DevIncidentFactV2",
    "DevInvestigationPlan",
    "DevInvestigationResult",
    "DevMessageRequestV2",
    "DevMetricRefV2",
    "DevNarrative",
    "DevObservedChangeV2",
    "DevPlanStepDependency",
    "DevPullRequestFactV2",
    "DevQuestionIntent",
    "DevReadinessBlock",
    "DevRelationshipPath",
    "DevRequiredChildFactV2",
    "DevResolutionCandidate",
    "DevResolutionEntry",
    "DevResolutionLedger",
    "DevScopeV2",
    "DevSourceContent",
    "DevSourceObservation",
    "DevSourceRequirement",
    "DevStatusFactV2",
    "DevStreamEventV2",
    "DevSubjectMention",
    "DevSubjectSet",
    "DevSurfaceContextV2",
    "EMPTY_CONTENT_OUTCOMES",
    "EntityKind",
    "EvidenceHandle",
    "FactDisclosure",
    "HealthDimension",
    "HealthRuleDefinition",
    "HealthRuleFinding",
    "NO_ANSWER_ANSWER_FIELD_POLICY",
    "NO_ANSWER_FRAME_FIELD_POLICY",
    "NO_ANSWER_OUTCOMES",
    "NarrativeFailureCode",
    "NoAnswerFieldPolicy",
    "OperationalDeficiencyInventory",
    "PlanRegistryID",
    "ProgressStateV2",
    "PublicOutcome",
    "QuestionIntentID",
    "ResolutionOutcome",
    "RuleApplicability",
    "RuleDirection",
    "ServerHandle",
    "SourceClass",
    "SourceRequirementState",
    "StreamEventTypeV2",
    "TeamQualificationBasis",
    "TeamQualificationResult",
    "finding_sort_key",
    "project_answer_v2_to_v1",
    "scan_public_text",
    "validate_completion_denominator",
    "validate_ledger_extends",
    "validate_narrative_fact_references",
    "validate_narrative_frame_consistency",
    "validate_no_answer_content_leaks",
    "validate_no_answer_projection",
    "validate_no_internal_leakage",
    "validate_outcome_consistency",
    "validate_plan_registry_membership",
    "validate_relationship_refs_within_frame",
    "validate_stream_v2",
    "validate_structural_closure",
    "validate_versions_presence",
]
