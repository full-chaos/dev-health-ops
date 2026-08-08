"""The frozen question-family registry for the corrected trial (CHAOS-3615).

Ten families, each with *exact* variants (the phrasing the corrective plan
and the correction addendum use verbatim) and *natural* variants (how the
question actually arrives — elliptical, colloquial, mid-conversation).

The registry exists to stop two specific drifts, and both are enforced by
:func:`validate_question_family_registry` at import time rather than by
review:

* **A family is never one metric and never one prompt.** Every family
  declares at least one exact and at least two natural variants, at least
  two source classes, at least two packet sections and at least three
  scoring dimensions — and every family lists ``SINGLE_DASHBOARD_METRIC``
  and ``SINGLE_EXACT_PROMPT`` among its prohibited reductions. An arm that
  answers "which teams are struggling?" by returning one chart has not
  answered the family.
* **Missing staffing denominators qualify, they do not disqualify.** Every
  family whose questions can turn on capacity carries
  ``StaffingDenominatorPolicy.QUALIFY_WHEN_ABSENT`` and lists
  ``AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR`` as a prohibited reduction. The
  correction addendum is explicit: absent allocation data reduces confidence
  and requires qualification; it must not make capacity questions
  unsupported.

``PERSON_LEVEL_RANKING`` is prohibited on **every** family, not only the
staffing ones. The packet already makes a person subject unrepresentable;
this is the second, independent statement of the same rule, at the level of
what may be *asked*.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Literal

from dev_health_ops.api.dev.contracts import Label, LongText, ShortText
from dev_health_ops.api.dev.contracts_v2.base import ContractModelV2, SourceClass

from .allowlists import TRIAL_SOURCE_ALLOWLIST
from .scoring import ALL_SCORING_DIMENSION_IDS, ScoringDimensionID
from .vocabulary import ComparisonShape, PacketSection

__all__ = [
    "ALL_PROHIBITED_REDUCTIONS",
    "ALL_QUESTION_FAMILY_IDS",
    "MANDATORY_PROHIBITED_REDUCTIONS",
    "QUESTION_FAMILY_REGISTRY",
    "ProhibitedReduction",
    "QuestionFamily",
    "QuestionFamilyID",
    "QuestionVariant",
    "QuestionVariantKind",
    "StaffingDenominatorPolicy",
    "validate_question_family_registry",
]


class QuestionFamilyID(StrEnum):
    """The ten frozen families of the corrected trial."""

    STRUGGLING_TEAMS = "struggling_teams"
    PRESSURE_SIGNALS = "pressure_signals"
    PROJECT_CAPACITY = "project_capacity"
    STAFFING_LANGUAGE = "staffing_language"
    PROJECT_STATUS_DRIVERS = "project_status_drivers"
    PORTFOLIO_DEPENDENCY_RISK = "portfolio_dependency_risk"
    DECLARED_VERSUS_ACTUAL = "declared_versus_actual"
    AMBIGUOUS_IDENTITY = "ambiguous_identity"
    COLLOQUIAL_FOLLOW_UP = "colloquial_follow_up"
    CLARIFICATION_AND_NO_MATCH = "clarification_and_no_match"


ALL_QUESTION_FAMILY_IDS: tuple[QuestionFamilyID, ...] = (
    QuestionFamilyID.STRUGGLING_TEAMS,
    QuestionFamilyID.PRESSURE_SIGNALS,
    QuestionFamilyID.PROJECT_CAPACITY,
    QuestionFamilyID.STAFFING_LANGUAGE,
    QuestionFamilyID.PROJECT_STATUS_DRIVERS,
    QuestionFamilyID.PORTFOLIO_DEPENDENCY_RISK,
    QuestionFamilyID.DECLARED_VERSUS_ACTUAL,
    QuestionFamilyID.AMBIGUOUS_IDENTITY,
    QuestionFamilyID.COLLOQUIAL_FOLLOW_UP,
    QuestionFamilyID.CLARIFICATION_AND_NO_MATCH,
)


class QuestionVariantKind(StrEnum):
    """``EXACT`` is the canonical phrasing; ``NATURAL`` is how it arrives."""

    EXACT = "exact"
    NATURAL = "natural"


class ProhibitedReduction(StrEnum):
    """Shapes an arm must not collapse a family into."""

    SINGLE_DASHBOARD_METRIC = "single_dashboard_metric"
    SINGLE_EXACT_PROMPT = "single_exact_prompt"
    PERSON_LEVEL_RANKING = "person_level_ranking"
    DASHBOARD_REDIRECT_AS_ANSWER = "dashboard_redirect_as_answer"
    UNQUALIFIED_STAFFING_CERTAINTY = "unqualified_staffing_certainty"
    AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR = "auto_unsupported_on_missing_denominator"
    PRE_ENUMERATED_INTENT_REQUIRED = "pre_enumerated_intent_required"


ALL_PROHIBITED_REDUCTIONS: tuple[ProhibitedReduction, ...] = (
    ProhibitedReduction.SINGLE_DASHBOARD_METRIC,
    ProhibitedReduction.SINGLE_EXACT_PROMPT,
    ProhibitedReduction.PERSON_LEVEL_RANKING,
    ProhibitedReduction.DASHBOARD_REDIRECT_AS_ANSWER,
    ProhibitedReduction.UNQUALIFIED_STAFFING_CERTAINTY,
    ProhibitedReduction.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
    ProhibitedReduction.PRE_ENUMERATED_INTENT_REQUIRED,
)

#: Prohibited on every family without exception. These three are the
#: registry's own anti-vacuity floor: a family that permitted any of them
#: could be "supported" by a chart, a single hard-coded prompt, or a
#: leaderboard of people.
MANDATORY_PROHIBITED_REDUCTIONS: tuple[ProhibitedReduction, ...] = (
    ProhibitedReduction.SINGLE_DASHBOARD_METRIC,
    ProhibitedReduction.SINGLE_EXACT_PROMPT,
    ProhibitedReduction.PERSON_LEVEL_RANKING,
)


class StaffingDenominatorPolicy(StrEnum):
    """What a family does when allocation/headcount data is missing."""

    NOT_APPLICABLE = "not_applicable"
    QUALIFY_WHEN_ABSENT = "qualify_when_absent"


class QuestionVariant(ContractModelV2):
    variant_id: Label
    kind: QuestionVariantKind
    text: ShortText


class QuestionFamily(ContractModelV2):
    """One frozen question family and everything an arm owes it."""

    schema_version: Literal["ask_dev_question_family.v1"]
    family_id: QuestionFamilyID
    title: Label
    analytical_job_statement: LongText
    exact_variants: tuple[QuestionVariant, ...]
    natural_variants: tuple[QuestionVariant, ...]
    permitted_comparison_shapes: tuple[ComparisonShape, ...]
    required_source_classes: tuple[SourceClass, ...]
    required_packet_sections: tuple[PacketSection, ...]
    scoring_dimension_ids: tuple[ScoringDimensionID, ...]
    staffing_denominator_policy: StaffingDenominatorPolicy
    prohibited_reductions: tuple[ProhibitedReduction, ...]


def _variants(
    family_id: QuestionFamilyID,
    kind: QuestionVariantKind,
    texts: tuple[str, ...],
) -> tuple[QuestionVariant, ...]:
    prefix = "e" if kind is QuestionVariantKind.EXACT else "n"
    return tuple(
        QuestionVariant(
            variant_id=f"{family_id.value}.{prefix}{index + 1}",
            kind=kind,
            text=text,
        )
        for index, text in enumerate(texts)
    )


def _family(
    family_id: QuestionFamilyID,
    title: str,
    analytical_job_statement: str,
    exact: tuple[str, ...],
    natural: tuple[str, ...],
    shapes: tuple[ComparisonShape, ...],
    sources: tuple[SourceClass, ...],
    sections: tuple[PacketSection, ...],
    dimensions: tuple[ScoringDimensionID, ...],
    staffing_policy: StaffingDenominatorPolicy,
    extra_prohibitions: tuple[ProhibitedReduction, ...] = (),
) -> QuestionFamily:
    return QuestionFamily(
        schema_version="ask_dev_question_family.v1",
        family_id=family_id,
        title=title,
        analytical_job_statement=analytical_job_statement,
        exact_variants=_variants(family_id, QuestionVariantKind.EXACT, exact),
        natural_variants=_variants(family_id, QuestionVariantKind.NATURAL, natural),
        permitted_comparison_shapes=shapes,
        required_source_classes=sources,
        required_packet_sections=sections,
        scoring_dimension_ids=dimensions,
        staffing_denominator_policy=staffing_policy,
        prohibited_reductions=MANDATORY_PROHIBITED_REDUCTIONS + extra_prohibitions,
    )


_ID = QuestionFamilyID
_SC = SourceClass
_S = PacketSection
_D = ScoringDimensionID
_R = ProhibitedReduction
_SHAPE = ComparisonShape
_STAFF = StaffingDenominatorPolicy

#: Dimensions every family is scored on. Kept small on purpose: a family's
#: own list adds what is specific to it, and the union is what CHAOS-3616
#: reports per family x per dimension.
_UNIVERSAL_DIMENSIONS: tuple[ScoringDimensionID, ...] = (
    _D.EVIDENCE_CLOSURE,
    _D.USEFUL_UNCERTAINTY_BEHAVIOUR,
    _D.ZERO_UNAUTHORIZED_RESULTS,
    _D.ZERO_PERSON_LEVEL_RANKING,
    _D.ZERO_GRAPH_NATIVE_SURFACE_LEAKAGE,
)


QUESTION_FAMILY_REGISTRY: Mapping[QuestionFamilyID, QuestionFamily] = {
    _ID.STRUGGLING_TEAMS: _family(
        _ID.STRUGGLING_TEAMS,
        "Struggling teams and teams needing attention",
        "Identify which teams are currently under strain and explain why, "
        "from cross-source evidence rather than a single health chart. The "
        "subject set is discovered, not supplied: the question names no team, "
        "so an arm that requires a committed subject before it will "
        "investigate cannot answer this family at all.",
        (
            "What teams are currently struggling, and why?",
            "Which teams need attention right now?",
        ),
        (
            "who's having a rough time at the moment?",
            "any teams we should be worried about?",
            "where are things not going well?",
        ),
        (_SHAPE.DISCOVERED_COHORT, _SHAPE.PORTFOLIO_WIDE),
        (
            _SC.WORK_ITEM,
            _SC.REVIEW,
            _SC.COGNITIVE_LOAD,
            _SC.HEALTH_PROFILE,
            _SC.INCIDENT,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.COMPARISON_COHORT,
            _S.RELATED_CONTEXT,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.COHORT_PRECISION,
            _D.COHORT_RECALL,
            _D.COHORT_INCLUSION_EXPLAINABILITY,
            _D.PRINCIPAL_DRIVER_PRECISION,
            _D.PRINCIPAL_DRIVER_RECALL,
            _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
            _D.CROSS_SOURCE_ASSOCIATION,
        ),
        _STAFF.QUALIFY_WHEN_ABSENT,
        (
            _R.DASHBOARD_REDIRECT_AS_ANSWER,
            _R.PRE_ENUMERATED_INTENT_REQUIRED,
            _R.UNQUALIFIED_STAFFING_CERTAINTY,
            _R.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
        ),
    ),
    _ID.PRESSURE_SIGNALS: _family(
        _ID.PRESSURE_SIGNALS,
        "Delivery, operational, review, dependency and investment pressure",
        "Locate where pressure is concentrated and of what kind. Five "
        "distinct pressure classes, deliberately in one family: an arm that "
        "can only see delivery pressure will score well on a delivery-only "
        "family and badly here, which is the point.",
        (
            "Which teams need attention because of delivery, operational, "
            "review, dependency, or investment pressure?",
            "Where is delivery pressure highest right now?",
        ),
        (
            "what's under the most pressure?",
            "where are we bottlenecked?",
            "who's drowning in reviews?",
        ),
        (_SHAPE.DISCOVERED_COHORT, _SHAPE.PORTFOLIO_WIDE, _SHAPE.EXPLICIT_COHORT),
        (
            _SC.WORK_ITEM,
            _SC.PULL_REQUEST,
            _SC.REVIEW,
            _SC.INCIDENT,
            _SC.INVESTMENT_ALLOCATION,
            _SC.DEFICIENCY_INVENTORY,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.COMPARISON_COHORT,
            _S.RELATED_CONTEXT,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.COHORT_PRECISION,
            _D.COHORT_RECALL,
            _D.RELEVANT_RELATIONSHIP_RECALL,
            _D.PRINCIPAL_DRIVER_PRECISION,
            _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
            _D.COMPARATIVE_JUDGMENT_SUPPORT,
            _D.CROSS_SOURCE_ASSOCIATION,
        ),
        _STAFF.QUALIFY_WHEN_ABSENT,
        (
            _R.DASHBOARD_REDIRECT_AS_ANSWER,
            _R.UNQUALIFIED_STAFFING_CERTAINTY,
            _R.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
        ),
    ),
    _ID.PROJECT_CAPACITY: _family(
        _ID.PROJECT_CAPACITY,
        "Project capacity constraints and lightly loaded projects",
        "Judge which projects look capacity-constrained and which look "
        "unusually lightly loaded relative to demand. Both directions are in "
        "scope: an arm that only ever reports overload is not measuring "
        "capacity, it is measuring activity.",
        (
            "Which projects appear capacity-constrained, understaffed, "
            "overstaffed, or unusually lightly loaded relative to demand?",
            "Which projects are capacity-constrained right now?",
        ),
        (
            "what are we under-resourcing?",
            "anything that looks over-provisioned?",
            "where do we have slack?",
        ),
        (_SHAPE.DISCOVERED_COHORT, _SHAPE.PORTFOLIO_WIDE, _SHAPE.EXPLICIT_COHORT),
        (
            _SC.WORK_ITEM,
            _SC.INVESTMENT_ALLOCATION,
            _SC.COGNITIVE_LOAD,
            _SC.WORK_GRAPH,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.COMPARISON_COHORT,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.COHORT_PRECISION,
            _D.COHORT_RECALL,
            _D.COMPARATIVE_JUDGMENT_SUPPORT,
            _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
            _D.UNSUPPORTED_ATTRIBUTION_RATE,
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        ),
        _STAFF.QUALIFY_WHEN_ABSENT,
        (
            _R.UNQUALIFIED_STAFFING_CERTAINTY,
            _R.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
            _R.DASHBOARD_REDIRECT_AS_ANSWER,
        ),
    ),
    _ID.STAFFING_LANGUAGE: _family(
        _ID.STAFFING_LANGUAGE,
        "Understaffed and overstaffed language with qualified evidence",
        "Answer questions phrased in staffing language without inventing a "
        "staffing denominator the platform does not have. The correct "
        "behaviour when allocation data is absent is a qualified answer that "
        "says what it is inferring from -- not silence, and not certainty.",
        (
            "Which projects are understaffed?",
            "Is this project overstaffed for what it is being asked to deliver?",
        ),
        (
            "do we have enough people on this?",
            "are we spreading ourselves too thin here?",
            "is anyone sitting idle on that project?",
        ),
        (_SHAPE.SINGULAR_SUBJECT, _SHAPE.EXPLICIT_COHORT, _SHAPE.DISCOVERED_COHORT),
        (
            _SC.INVESTMENT_ALLOCATION,
            _SC.WORK_ITEM,
            _SC.COGNITIVE_LOAD,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.ZERO_UNSUPPORTED_STAFFING_CERTAINTY,
            _D.UNSUPPORTED_ATTRIBUTION_RATE,
            _D.SUBJECT_TOP_1,
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        ),
        _STAFF.QUALIFY_WHEN_ABSENT,
        (
            _R.UNQUALIFIED_STAFFING_CERTAINTY,
            _R.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
        ),
    ),
    _ID.PROJECT_STATUS_DRIVERS: _family(
        _ID.PROJECT_STATUS_DRIVERS,
        "Project status and principal current drivers",
        "State where a named project actually stands and what is principally "
        "driving that, distinguishing drivers from symptoms and current "
        "causes from historical ones.",
        (
            "What is the actual status of Project XYZ, and what are the "
            "principal current drivers?",
            "Why is ACR still not finished?",
        ),
        (
            "what's the deal with the auth work?",
            "where did that project get to?",
            "what's holding it up?",
        ),
        (_SHAPE.SINGULAR_SUBJECT,),
        (
            _SC.STATUS_CHANGE,
            _SC.WORK_ITEM,
            _SC.PULL_REQUEST,
            _SC.REVIEW,
            _SC.DEPLOYMENT,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.RELATED_CONTEXT,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.SUBJECT_TOP_1,
            _D.SUBJECT_TOP_3,
            _D.PRINCIPAL_DRIVER_PRECISION,
            _D.PRINCIPAL_DRIVER_RECALL,
            _D.SYMPTOM_VERSUS_DRIVER_DISTINCTION,
            _D.LINEAGE_PATH_PRECISION,
            _D.LINEAGE_DIRECTION_CORRECTNESS,
            _D.CURRENT_RELEVANCE,
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        ),
        _STAFF.QUALIFY_WHEN_ABSENT,
        (
            _R.DASHBOARD_REDIRECT_AS_ANSWER,
            _R.UNQUALIFIED_STAFFING_CERTAINTY,
            _R.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
        ),
    ),
    _ID.PORTFOLIO_DEPENDENCY_RISK: _family(
        _ID.PORTFOLIO_DEPENDENCY_RISK,
        "Portfolio and shared-dependency risk",
        "Find projects exposed through a dependency they share with "
        "something already in trouble. This family is the clearest test of "
        "whether relationship traversal adds anything: the answer is not "
        "visible in any single project's own metrics.",
        (
            "Which projects are at risk because of shared dependencies?",
            "What else is exposed if this dependency slips?",
        ),
        (
            "what else does that break?",
            "who else is on the hook for this?",
            "if that library is stuck, what else is stuck?",
        ),
        (_SHAPE.DISCOVERED_COHORT, _SHAPE.PORTFOLIO_WIDE),
        (
            _SC.WORK_GRAPH,
            _SC.WORK_ITEM,
            _SC.DEPLOYMENT,
            _SC.INCIDENT,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.COMPARISON_COHORT,
            _S.RELATED_CONTEXT,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.RELEVANT_ENTITY_RECALL,
            _D.RELEVANT_RELATIONSHIP_RECALL,
            _D.LINEAGE_PATH_PRECISION,
            _D.LINEAGE_DIRECTION_CORRECTNESS,
            _D.COHORT_PRECISION,
            _D.COHORT_RECALL,
            _D.CROSS_SOURCE_ASSOCIATION,
        ),
        _STAFF.NOT_APPLICABLE,
        (_R.DASHBOARD_REDIRECT_AS_ANSWER,),
    ),
    _ID.DECLARED_VERSUS_ACTUAL: _family(
        _ID.DECLARED_VERSUS_ACTUAL,
        "Declared status versus actual delivery and readiness evidence",
        "Find work declared complete that lacks the delivery, release, "
        "documentation or operational evidence completion would imply. "
        "Requires both halves -- the declaration and the evidence -- from "
        "different source classes, by construction.",
        (
            "Which declared-complete projects lack delivery, release, "
            "documentation, or operational evidence?",
            "Is this actually done?",
        ),
        (
            "is that really finished?",
            "did that ever actually ship?",
            "anything marked done that isn't?",
        ),
        (_SHAPE.SINGULAR_SUBJECT, _SHAPE.DISCOVERED_COHORT, _SHAPE.PORTFOLIO_WIDE),
        (
            _SC.STATUS_CHANGE,
            _SC.PULL_REQUEST,
            _SC.DEPLOYMENT,
            _SC.CI_RUN,
            _SC.TEST_REPORT,
            _SC.DEFICIENCY_INVENTORY,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.RELATED_CONTEXT,
            _S.DRIVER_ANALYSIS,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.CROSS_SOURCE_ASSOCIATION,
            _D.PRINCIPAL_DRIVER_PRECISION,
            _D.UNSUPPORTED_ATTRIBUTION_RATE,
            _D.CURRENT_RELEVANCE,
            _D.ANSWER_USEFULNESS_BEYOND_DASHBOARD,
        ),
        _STAFF.NOT_APPLICABLE,
        (_R.DASHBOARD_REDIRECT_AS_ANSWER,),
    ),
    _ID.AMBIGUOUS_IDENTITY: _family(
        _ID.AMBIGUOUS_IDENTITY,
        "Ambiguous aliases, acronyms and renamed entities",
        "Resolve a reference that is not the canonical name -- an acronym, "
        "an old name, a provider key, a nickname -- to the right canonical "
        "subject, or say honestly that it is ambiguous. The failure to score "
        "hardest is confident resolution to the wrong similarly-named thing.",
        (
            "What about the auth work?",
            "How is ACR doing?",
        ),
        (
            "how's the thing we used to call Northstar going?",
            "status on DHO?",
            "what happened to the payments rewrite?",
        ),
        (_SHAPE.SINGULAR_SUBJECT, _SHAPE.EXPLICIT_COHORT),
        (
            _SC.WORK_GRAPH,
            _SC.WORK_ITEM,
            _SC.STATUS_CHANGE,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.SUBJECT_TOP_1,
            _D.SUBJECT_TOP_3,
            _D.ALIAS_ACRONYM_RENAME_RESOLUTION,
            _D.CLARIFICATION_CANDIDATE_PRECISION,
            _D.NO_UNSAFE_ORGANIZATION_WIDENING,
        ),
        _STAFF.NOT_APPLICABLE,
        (_R.PRE_ENUMERATED_INTENT_REQUIRED,),
    ),
    _ID.COLLOQUIAL_FOLLOW_UP: _family(
        _ID.COLLOQUIAL_FOLLOW_UP,
        "Colloquial references and conversational follow-ups",
        "Resolve a reference that only makes sense against the conversation "
        "or the surface the user is on. 'What about the other project we "
        "discussed?' has no answer without context and must not be answered "
        "by guessing.",
        (
            "What about the other project we discussed?",
            "What happened with the project that kept cycling in review?",
        ),
        (
            "and the other one?",
            "what about that thing from earlier?",
            "same question for the second team",
        ),
        (_SHAPE.SINGULAR_SUBJECT, _SHAPE.EXPLICIT_COHORT),
        (
            _SC.WORK_GRAPH,
            _SC.REVIEW,
            _SC.WORK_ITEM,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.CONVERSATIONAL_REFERENCE_RESOLUTION,
            _D.SUBJECT_TOP_1,
            _D.CLARIFICATION_CANDIDATE_PRECISION,
            _D.NO_UNSAFE_ORGANIZATION_WIDENING,
        ),
        _STAFF.NOT_APPLICABLE,
        (_R.PRE_ENUMERATED_INTENT_REQUIRED,),
    ),
    _ID.CLARIFICATION_AND_NO_MATCH: _family(
        _ID.CLARIFICATION_AND_NO_MATCH,
        "Clarification and safe no-match behaviour",
        "Behave well when the question cannot be answered as asked: ask a "
        "useful clarifying question with a short, plausible candidate list, "
        "or say there is no match. Never widen to the whole organization, "
        "and never manufacture a subject.",
        (
            "What is going sideways?",
            "How is Atlas doing?",
        ),
        (
            "what about that other team, the new one?",
            "how's the project going",
            "give me the update",
        ),
        (_SHAPE.SINGULAR_SUBJECT, _SHAPE.DISCOVERED_COHORT),
        (
            _SC.WORK_GRAPH,
            _SC.SOURCE_HEALTH,
        ),
        (
            _S.ANALYTICAL_JOB,
            _S.SUBJECT_DISCOVERY,
            _S.EVIDENCE_COVERAGE,
        ),
        _UNIVERSAL_DIMENSIONS
        + (
            _D.CLARIFICATION_CANDIDATE_PRECISION,
            _D.NO_UNSAFE_ORGANIZATION_WIDENING,
            _D.SUBJECT_TOP_3,
        ),
        _STAFF.NOT_APPLICABLE,
        (_R.PRE_ENUMERATED_INTENT_REQUIRED,),
    ),
}

#: Minimum shape of a family. Below any of these it stops being a family and
#: becomes a prompt with a metric attached.
_MIN_EXACT_VARIANTS = 1
_MIN_NATURAL_VARIANTS = 2
_MIN_SOURCE_CLASSES = 2
_MIN_PACKET_SECTIONS = 2
_MIN_SCORING_DIMENSIONS = 3


def validate_question_family_registry() -> None:
    """Raise unless the family registry is total, bounded and non-vacuous."""

    if set(QUESTION_FAMILY_REGISTRY) != set(ALL_QUESTION_FAMILY_IDS):
        missing = sorted(
            str(item)
            for item in set(ALL_QUESTION_FAMILY_IDS) - set(QUESTION_FAMILY_REGISTRY)
        )
        extra = sorted(
            str(item)
            for item in set(QUESTION_FAMILY_REGISTRY) - set(ALL_QUESTION_FAMILY_IDS)
        )
        raise RuntimeError(
            f"question family registry is not total; missing={missing}, extra={extra}"
        )

    seen_variant_ids: set[str] = set()
    seen_variant_texts: set[str] = set()
    for family_id, family in QUESTION_FAMILY_REGISTRY.items():
        if family.family_id is not family_id:
            raise RuntimeError(
                f"question family key {family_id} is filed under {family.family_id}"
            )
        if len(family.exact_variants) < _MIN_EXACT_VARIANTS:
            raise RuntimeError(f"family {family_id} declares no exact variant")
        if len(family.natural_variants) < _MIN_NATURAL_VARIANTS:
            raise RuntimeError(
                f"family {family_id} declares fewer than {_MIN_NATURAL_VARIANTS} "
                "natural variants; a family with one phrasing is a prompt"
            )
        if len(set(family.required_source_classes)) < _MIN_SOURCE_CLASSES:
            raise RuntimeError(
                f"family {family_id} requires fewer than {_MIN_SOURCE_CLASSES} "
                "source classes; a single-source family is a dashboard metric"
            )
        if len(set(family.required_packet_sections)) < _MIN_PACKET_SECTIONS:
            raise RuntimeError(
                f"family {family_id} requires fewer than {_MIN_PACKET_SECTIONS} "
                "packet sections"
            )
        if len(set(family.scoring_dimension_ids)) < _MIN_SCORING_DIMENSIONS:
            raise RuntimeError(
                f"family {family_id} is scored on fewer than "
                f"{_MIN_SCORING_DIMENSIONS} dimensions"
            )
        if not family.permitted_comparison_shapes:
            raise RuntimeError(f"family {family_id} permits no comparison shape")

        off_allowlist = sorted(
            str(item)
            for item in set(family.required_source_classes)
            - set(TRIAL_SOURCE_ALLOWLIST)
        )
        if off_allowlist:
            raise RuntimeError(
                f"family {family_id} requires non-allowlisted sources: {off_allowlist}"
            )
        unknown_dimensions = sorted(
            str(item)
            for item in set(family.scoring_dimension_ids)
            - set(ALL_SCORING_DIMENSION_IDS)
        )
        if unknown_dimensions:
            raise RuntimeError(
                f"family {family_id} names unknown scoring dimensions: "
                f"{unknown_dimensions}"
            )
        missing_prohibitions = sorted(
            str(item)
            for item in set(MANDATORY_PROHIBITED_REDUCTIONS)
            - set(family.prohibited_reductions)
        )
        if missing_prohibitions:
            raise RuntimeError(
                f"family {family_id} omits mandatory prohibited reductions: "
                f"{missing_prohibitions}"
            )
        # The two halves of the staffing rule travel together. A family that
        # says "qualify when the denominator is absent" but does not also
        # forbid auto-unsupported has only stated the half that is easy to
        # honour, and a family that says the denominator is irrelevant must
        # not carry staffing prohibitions that would then never be exercised.
        qualifies = (
            family.staffing_denominator_policy
            is StaffingDenominatorPolicy.QUALIFY_WHEN_ABSENT
        )
        staffing_prohibitions = {
            ProhibitedReduction.UNQUALIFIED_STAFFING_CERTAINTY,
            ProhibitedReduction.AUTO_UNSUPPORTED_ON_MISSING_DENOMINATOR,
        }
        declared_staffing = staffing_prohibitions & set(family.prohibited_reductions)
        if qualifies and declared_staffing != staffing_prohibitions:
            omitted = sorted(
                str(item) for item in staffing_prohibitions - declared_staffing
            )
            raise RuntimeError(
                f"family {family_id} qualifies on a missing staffing "
                f"denominator but omits {omitted}; both halves of the rule "
                "(qualify, and never auto-unsupport) must be declared"
            )
        if not qualifies and declared_staffing:
            raise RuntimeError(
                f"family {family_id} declares staffing prohibitions "
                f"{sorted(str(item) for item in declared_staffing)} while "
                "treating the staffing denominator as not applicable"
            )

        for variant in family.exact_variants + family.natural_variants:
            if variant.variant_id in seen_variant_ids:
                raise RuntimeError(
                    f"duplicate question variant id {variant.variant_id}"
                )
            seen_variant_ids.add(variant.variant_id)
            normalized = variant.text.strip().casefold()
            if normalized in seen_variant_texts:
                raise RuntimeError(
                    f"duplicate question variant text {variant.text!r}; the same "
                    "prompt appearing in two families makes both families' "
                    "scores unattributable"
                )
            seen_variant_texts.add(normalized)


validate_question_family_registry()
