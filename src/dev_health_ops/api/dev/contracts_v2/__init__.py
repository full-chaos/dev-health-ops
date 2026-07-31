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
    PublicOutcome,
    QuestionIntentID,
    SourceClass,
    SourceRequirementState,
)
from .compat import project_answer_v2_to_v1
from .embedded import (
    DevCoverageV2,
    DevErrorV2,
    DevEvidenceRefV2,
    DevMetricRefV2,
    DevScopeV2,
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
from .intent import DevMessageRequestV2, DevQuestionIntent
from .narrative import DevNarrative
from .plan import (
    PLAN_REGISTRY,
    DevInvestigationPlan,
    DevPlanStepDependency,
    DevSourceRequirement,
)
from .result import DevInvestigationResult, DevRelationshipPath, DevSourceObservation
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
    validate_relationship_refs_within_frame,
    validate_structural_closure,
    validate_versions_presence,
)

#: Every top-level, independently versioned v2 wire contract, keyed by its
#: `schema_version` value — mirrors `dev_health_ops.api.dev.contracts.CONTRACT_MODELS`.
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
    "PLAN_REGISTRY",
    "SOURCE_CLASS_VOCABULARY",
    "Cardinality",
    "ContractModelV2",
    "DevAnswerFact",
    "DevAnswerFrame",
    "DevAnswerSection",
    "DevAnswerV2",
    "DevCompletionBlock",
    "DevComparisonPoint",
    "DevCoverageV2",
    "DevEntityRefV2",
    "DevErrorV2",
    "DevEvidenceRefV2",
    "DevFrameConflict",
    "DevFrameVersions",
    "DevInvestigationPlan",
    "DevInvestigationResult",
    "DevMessageRequestV2",
    "DevMetricRefV2",
    "DevNarrative",
    "DevPlanStepDependency",
    "DevQuestionIntent",
    "DevReadinessBlock",
    "DevRelationshipPath",
    "DevResolutionCandidate",
    "DevResolutionEntry",
    "DevResolutionLedger",
    "DevScopeV2",
    "DevSourceObservation",
    "DevSourceRequirement",
    "DevStreamEventV2",
    "DevSubjectMention",
    "DevSubjectSet",
    "DevSurfaceContextV2",
    "EMPTY_CONTENT_OUTCOMES",
    "EntityKind",
    "NO_ANSWER_ANSWER_FIELD_POLICY",
    "NO_ANSWER_FRAME_FIELD_POLICY",
    "NO_ANSWER_OUTCOMES",
    "NoAnswerFieldPolicy",
    "ProgressStateV2",
    "PublicOutcome",
    "QuestionIntentID",
    "ResolutionOutcome",
    "SourceClass",
    "SourceRequirementState",
    "StreamEventTypeV2",
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
    "validate_relationship_refs_within_frame",
    "validate_stream_v2",
    "validate_structural_closure",
    "validate_versions_presence",
]
