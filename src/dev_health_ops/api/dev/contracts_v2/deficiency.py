"""``deficiency_finding.v1`` and ``deficiency_operational_inventory.v1`` (CHAOS-3305).

The canonical answer to "what operational deficiencies do we have?" -- a
bounded, versioned, evidence-backed inventory across the eight taxonomy
categories named verbatim in the issue: data & integration, planning &
relationships, delivery flow, review & CI, deployment & reliability,
ownership & code risk, capacity & cognitive load, and investment balance.
This module defines the wire shapes; :mod:`.operational_deficiency_service`
computes them from CHAOS-3303's canonical services and CHAOS-3302's health
rule registry -- never from free-form model output (guardrail: "no
free-form model-created deficiency").

Two structural decisions carry the ticket's applicability/evidence
discipline at the type level rather than by convention:

* ``DeficiencyCategory`` is a closed eight-member enum, matching the issue's
  taxonomy verbatim -- a finding always declares exactly one, never a
  free-form string.
* ``DeficiencyFinding.data_semantics`` must agree with ``observed_state``
  (:func:`DeficiencyFinding.validate_zero_semantics`, mirroring
  ``DimensionObservation``/``DevSourceObservation`` exactly): a finding
  about a genuinely queried condition reports ``measured_zero``, and a
  finding about an unconfigured/unavailable/unauthorized source -- itself
  the deficiency -- reports ``not_measured``, never a fabricated
  ``measured_zero`` standing in for "we never checked". A category that
  was never evaluated at all (no rule registered, calibration pending, the
  lookup failed) is reported through :class:`DeficiencyCategoryStatus`
  instead of a finding. This is the type-level enforcement of "no missing
  data represented as zero or healthy".
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Literal, Self, final

from pydantic import AwareDatetime, Field, FiniteFloat, model_validator

from .base import (
    ContractModelV2,
    DevRelationshipPath,
    OpaqueID,
    PlatformVersionToken,
    ServerHandle,
    ShortText,
    SourceRequirementState,
)
from .health_rules import DataSemantics, FactKind, RuleApplicability

__all__ = [
    "DEFICIENCY_CATEGORIES",
    "DeficiencyCategory",
    "DeficiencyCategoryStatus",
    "DeficiencyEvidenceClassification",
    "DeficiencyFinding",
    "DeficiencyRemediation",
    "DeficiencySeverity",
    "OperationalDeficiencyInventory",
    "finding_sort_key",
]

#: The two ``SourceRequirementState`` partitions ``DeficiencyFinding.
#: validate_zero_semantics`` keys ``data_semantics`` off of -- mirrors
#: ``contracts_v2.result``'s own private ``_QUERIED_STATES``/
#: ``_UNMEASURED_STATES`` module constants (a deliberate re-derivation,
#: not an import, per that module's own docstring rationale). Hoisted to
#: module level (rather than defined inline in the validator, Codex
#: finding, round 2) so the totality assertion below can prove, at import
#: time, that every ``SourceRequirementState`` member falls into EXACTLY
#: one partition -- a future member added to that enum without updating
#: either set here fails the import, not one request that reaches it.
_QUERIED_OBSERVED_STATES = frozenset(
    {
        SourceRequirementState.AVAILABLE_CURRENT,
        SourceRequirementState.AVAILABLE_STALE,
        SourceRequirementState.AVAILABLE_UNKNOWN,
    }
)
_UNMEASURED_OBSERVED_STATES = frozenset(
    {
        SourceRequirementState.UNCONFIGURED,
        SourceRequirementState.UNAVAILABLE,
        SourceRequirementState.UNAUTHORIZED_OR_NOT_VISIBLE,
        SourceRequirementState.NOT_APPLICABLE,
        SourceRequirementState.TRUNCATED,
    }
)

if _QUERIED_OBSERVED_STATES & _UNMEASURED_OBSERVED_STATES:
    raise RuntimeError(
        "contracts_v2.deficiency: _QUERIED_OBSERVED_STATES and "
        "_UNMEASURED_OBSERVED_STATES overlap -- they must be disjoint"
    )
_uncovered_observed_states = (
    frozenset(SourceRequirementState)
    - _QUERIED_OBSERVED_STATES
    - _UNMEASURED_OBSERVED_STATES
)
if _uncovered_observed_states:
    raise RuntimeError(
        "contracts_v2.deficiency: SourceRequirementState member(s) "
        f"{sorted(state.value for state in _uncovered_observed_states)} "
        "are in neither _QUERIED_OBSERVED_STATES nor "
        "_UNMEASURED_OBSERVED_STATES -- DeficiencyFinding.data_semantics "
        "would have no rule for them"
    )


class DeficiencyCategory(StrEnum):
    """The eight taxonomy categories named verbatim in CHAOS-3305."""

    DATA_INTEGRATION = "data_integration"
    PLANNING_RELATIONSHIPS = "planning_relationships"
    DELIVERY_FLOW = "delivery_flow"
    REVIEW_CI = "review_ci"
    DEPLOYMENT_RELIABILITY = "deployment_reliability"
    OWNERSHIP_CODE_RISK = "ownership_code_risk"
    CAPACITY_COGNITIVE_LOAD = "capacity_cognitive_load"
    INVESTMENT_BALANCE = "investment_balance"


#: Every category, in the issue's own taxonomy order -- the closed set
#: :class:`OperationalDeficiencyInventory` requires exactly one
#: :class:`DeficiencyCategoryStatus` per member of.
DEFICIENCY_CATEGORIES: tuple[DeficiencyCategory, ...] = tuple(DeficiencyCategory)


class DeficiencySeverity(StrEnum):
    """A deficiency's own severity -- deliberately not ``DimensionState``.

    ``DimensionState`` (CHAOS-3302) also carries ``healthy``/``unknown``/
    ``not_applicable``, which are never valid on a *finding* (a finding is,
    by definition, a real deficiency -- an unmeasured or healthy check is
    reported through :class:`DeficiencyCategoryStatus` instead, never
    minted as a zero-severity finding). Scoping this enum to the three
    genuinely triggerable states keeps that guardrail structural rather
    than conventional.
    """

    WATCH = "watch"
    AT_RISK = "at_risk"
    CRITICAL = "critical"


class DeficiencyEvidenceClassification(StrEnum):
    """Closed reasons a finding may carry no per-item evidence ref (F10).

    Every :class:`DeficiencyFinding` carries either a non-empty
    ``evidence_ref_ids`` or exactly one of these -- never neither (see
    :func:`DeficiencyFinding.validate_evidence_or_classification`).
    """

    #: The deficiency *is* an absence (an unconfigured/unavailable source,
    #: a missing declared status) -- there is no positive fact to cite,
    #: only the absence itself, which the finding's own category/kind/
    #: observed_state already state precisely.
    STRUCTURAL_ABSENCE = "structural_absence"
    #: The finding is a cohort/aggregate signal by design (capacity,
    #: portfolio-level) -- citing a per-item evidence ref would risk
    #: person-level attribution the guardrails forbid.
    AGGREGATE_ONLY = "aggregate_only"
    #: The finding is derived from a ``health_rule_finding.v1`` (CHAOS-3302),
    #: whose own contract discloses evidentiary backing only as
    #: ``evidence_source_classes`` (a closed set of source *categories*),
    #: not per-item evidence handles -- there is no per-fact ref to carry
    #: at this layer without changing that upstream contract.
    SOURCE_CLASS_ONLY = "source_class_only"


class DeficiencyRemediation(ContractModelV2):
    """A bounded, server-owned remediation + verification pair.

    Never a rendering of arbitrary producer copy and never an action with
    write/execution side effects (explicit guardrail) -- both fields are
    fixed, code-owned template text per finding kind, mirroring
    ``HealthRuleDefinition.remediation_template``'s own posture.
    """

    schema_version: Literal["deficiency_remediation.v1"]
    remediation_template: ShortText
    verification_condition: ShortText


@final
class DeficiencyFinding(ContractModelV2):
    """One deterministic, evidence-backed operational deficiency.

    ``rule_id``/``rule_version`` name the deterministic finding kind: for
    categories drawn directly from :data:`.health_rule_registry.
    HEALTH_RULE_REGISTRY` (delivery flow, review/CI, deployment/
    reliability, ownership/code risk) these are the real
    ``health_rule.<name>.vN`` ids from CHAOS-3302; for categories computed
    directly from canonical service results with no rule-registry
    equivalent yet (data/integration, planning/relationships), these are a
    parallel, equally closed ``deficiency_rule.<name>.v1`` namespace (see
    ``operational_deficiency_service._DEFICIENCY_RULE_CATEGORY`` for the
    exhaustive table). Either way, ``rule_id`` is never a free-form or
    caller-supplied string.

    Subclassing is structurally forbidden (:func:`__init_subclass__` below,
    plus :func:`typing.final` for the type checker) -- Codex finding,
    round 3, 2026-08-02, ratified decision: these contract models are a
    CLOSED family, and subclassing is not a supported use. A subclass
    changes serialization semantics silently (``model_dump``/
    ``model_json_schema`` honor the subclass's own fields, not the
    declared contract type) and can carry extra state (private
    attributes, computed fields) that pydantic's containment/revalidation
    machinery was never designed to reason about when the subclass
    instance is embedded in a ``DeficiencyFinding``-typed field elsewhere.
    Making this structurally impossible closes that whole class of attack
    rather than chasing each individual symptom.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError(
            f"{cls.__name__} may not subclass DeficiencyFinding -- this is "
            "a closed, frozen wire-contract family (see the class "
            "docstring). Add a new top-level contract type instead."
        )

    schema_version: Literal["deficiency_finding.v1"]
    finding_id: ServerHandle
    category: DeficiencyCategory
    rule_id: PlatformVersionToken
    rule_version: PlatformVersionToken
    subject_kind: RuleApplicability
    subject_id: OpaqueID
    severity: DeficiencySeverity
    fact_kind: FactKind
    observed_state: SourceRequirementState
    data_semantics: DataSemantics
    sample_count: int | None = Field(default=None, ge=0, le=1_000_000)
    coverage: FiniteFloat = Field(ge=0, le=1)
    current_window_days: int = Field(ge=1, le=365)
    comparison_window_days: int | None = Field(default=None, ge=1, le=365)
    relationship_paths: tuple[DevRelationshipPath, ...] = Field(
        default_factory=tuple, max_length=10
    )
    evidence_ref_ids: tuple[OpaqueID, ...] = Field(default_factory=tuple, max_length=25)
    evidence_classification: DeficiencyEvidenceClassification | None = None
    blast_radius: ShortText
    remediation: DeficiencyRemediation
    limitations: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=10)
    evaluated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_zero_semantics(self) -> Self:
        """Mirrors ``DimensionObservation.validate_zero_semantics`` /
        ``DevSourceObservation.validate_zero_semantics`` exactly (Codex
        finding, 2026-08-02): ``observed_state`` decides which
        ``data_semantics`` values are honest, not a blanket
        "findings are always measured_zero" rule.

        A finding about a genuinely *queried* condition (e.g. a stale or
        empty-but-checked source) must report ``measured_zero`` or
        ``no_data`` -- ``not_measured`` would misrepresent a real, checked
        deficiency as unchecked. A finding about an *unmeasured* condition
        (e.g. an unconfigured, unavailable, or unauthorized required
        source) must report ``not_measured`` **exactly** (Codex finding,
        round 2: rejecting only ``measured_zero`` in the unmeasured branch
        left ``no_data`` unrejected -- an unconfigured source is not
        indistinguishable from a queried source that came back empty; the
        two are different facts and must not share a spelling). Either way
        the finding itself is legitimate content (a required source being
        unconfigured IS an operational deficiency); only the semantics
        label was wrong.
        """

        if self.observed_state in _QUERIED_OBSERVED_STATES:
            if self.data_semantics == "not_measured":
                raise ValueError(
                    "a finding about a queried observed_state must report "
                    "measured_zero or no_data, never not_measured"
                )
        else:
            if self.data_semantics != "not_measured":
                raise ValueError(
                    "a finding about an unmeasured observed_state "
                    f"({self.observed_state.value!r}) must report "
                    f"data_semantics='not_measured' exactly, not "
                    f"{self.data_semantics!r} -- measured_zero would claim "
                    "a checked, real zero, and no_data would claim a "
                    "queried-but-empty result; neither is true of an "
                    "unconfigured/unavailable/unauthorized/not_applicable/"
                    "truncated source"
                )
        return self

    @model_validator(mode="after")
    def validate_evidence_or_classification(self) -> Self:
        """F10 discipline: evidence refs XOR an explicit no-evidence reason."""

        has_evidence = bool(self.evidence_ref_ids)
        has_classification = self.evidence_classification is not None
        if has_evidence and has_classification:
            raise ValueError(
                "a finding with real evidence_ref_ids must not also carry an "
                "evidence_classification -- the classification exists only "
                "for the no-evidence case"
            )
        if not has_evidence and not has_classification:
            raise ValueError(
                "a DeficiencyFinding requires either evidence_ref_ids or an "
                "explicit evidence_classification (F10) -- neither is not a "
                "valid disclosure"
            )
        return self


class DeficiencyCategoryStatus(ContractModelV2):
    """One evaluation-status record per taxonomy category.

    Always present for exactly the eight :data:`DEFICIENCY_CATEGORIES`
    (see :func:`OperationalDeficiencyInventory.validate_category_coverage`)
    so "no deficiencies in this category" (``evaluated=True,
    finding_count=0``) is structurally distinguishable from "this category
    was never evaluated" (``evaluated=False``) -- the same distinction
    ``DataHealthState`` draws between a genuine zero and an unmeasured
    source, applied one level up at the category granularity.
    """

    schema_version: Literal["deficiency_category_status.v1"]
    category: DeficiencyCategory
    evaluated: bool
    finding_count: int = Field(ge=0, le=200)
    applicability_states_observed: tuple[SourceRequirementState, ...] = Field(
        default_factory=tuple, max_length=8
    )
    limitation: ShortText | None = None

    @model_validator(mode="after")
    def validate_status_consistency(self) -> Self:
        if not self.evaluated and self.finding_count != 0:
            raise ValueError("an unevaluated category cannot report findings")
        if not self.evaluated and self.limitation is None:
            raise ValueError(
                "an unevaluated category requires a bounded limitation explaining why"
            )
        return self


@final
class OperationalDeficiencyInventory(ContractModelV2):
    """``deficiency.operational.v1``'s result: one subject's full inventory.

    ``findings`` is ordered worst-severity-first, then by category, then by
    ``finding_id`` for stability (validated below rather than merely
    documented, so a caller reading the wire payload never needs to
    re-sort) -- "Order by approved severity, blast radius, evidence
    quality, and remediation dependency; do not let model prose reorder or
    invent findings."

    Two after-validators below (Codex findings, 2026-08-02) reconcile the
    two collections a caller could otherwise silently desynchronize:
    :func:`validate_category_counts_match_findings` (a declared
    ``finding_count`` of 0 with a real finding present for that category,
    or vice versa) and :func:`validate_findings_match_subject` (a finding
    naming a different subject than the inventory itself). A third,
    :func:`validate_findings_satisfy_evidence_discipline`, re-checks F10
    (evidence XOR an explicit no-evidence classification) for every
    finding at this containment boundary -- not because
    ``DeficiencyFinding``'s own validator is insufficient in the ordinary
    case, but because neither a per-model validator nor this one can be
    made total over ``model_construct``/``model_copy(update=...)``, which
    both bypass validation entirely by design. This re-check closes the
    gap for a forged *finding* smuggled into an otherwise normally
    constructed inventory. It does **not** close the gap for a forged
    *inventory* itself (``OperationalDeficiencyInventory.model_construct``
    skips every validator here, including this one) -- that case is
    caught one layer out, at the persistence/record boundary (mirrors
    CHAOS-3297 s1's frame validation at ``record_frame``), not here.

    ``model_copy`` is overridden below (Codex finding, round 2,
    2026-08-02, mirroring ``DevScope.model_copy`` exactly): pydantic's
    base ``model_copy`` is a raw field copy that never reruns any of the
    validators above, so ``model_copy(update={"findings": ()})`` on a
    valid inventory returned a serializable inventory whose
    ``category_statuses`` still claimed the old, now-wrong finding
    counts -- the same class of hole as ``DevScope``'s own
    ``repositories``-on-a-TEAM-scope bug (CHAOS-3301), reopened here for
    counts, subject matching, ordering, uniqueness, and F10.

    Subclassing is structurally forbidden (:func:`__init_subclass__`
    below, plus :func:`typing.final`) -- Codex finding, round 3,
    2026-08-02, ratified decision: this contract model family is CLOSED,
    subclassing is unsupported, and codex's own repro class (a runtime
    subtype losing its identity through the revalidating ``model_copy``
    round-trip above, a ``PrivateAttr`` reset, a computed-field subclass
    raising) is dead by construction rather than patched piecemeal. A
    consequence worth stating plainly: ``model_copy``'s round-trip is
    therefore a REVALIDATING copy, not a reference-preserving one --
    ``deep=False`` does not skip rebuilding nested models, because
    ``model_validate(copied.model_dump())`` always reconstructs the whole
    tree. Callers must compare copies by VALUE, never by object identity
    of nested fields.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError(
            f"{cls.__name__} may not subclass OperationalDeficiencyInventory "
            "-- this is a closed, frozen wire-contract family (see the "
            "class docstring). Add a new top-level contract type instead."
        )

    schema_version: Literal["deficiency_operational_inventory.v1"]
    inventory_id: ServerHandle
    subject_kind: RuleApplicability
    subject_id: OpaqueID
    findings: tuple[DeficiencyFinding, ...] = Field(
        default_factory=tuple, max_length=200
    )
    category_statuses: tuple[DeficiencyCategoryStatus, ...] = Field(
        min_length=8, max_length=8
    )
    evaluated_at: AwareDatetime

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        """Revalidating copy -- see the class docstring. Every production
        caller only patches ``evaluated_at``-adjacent metadata, so
        round-tripping the update through ``model_validate`` costs nothing
        real and closes the construction path.
        """

        copied = super().model_copy(update=update, deep=deep)
        return type(self).model_validate(copied.model_dump())

    @model_validator(mode="after")
    def validate_category_coverage(self) -> Self:
        categories = [status.category for status in self.category_statuses]
        if len(set(categories)) != len(categories):
            raise ValueError("category_statuses must not repeat a category")
        if set(categories) != set(DEFICIENCY_CATEGORIES):
            raise ValueError(
                "category_statuses must cover exactly the eight closed "
                f"deficiency categories, got: {sorted(categories)}"
            )
        return self

    @model_validator(mode="after")
    def validate_finding_ids_unique(self) -> Self:
        finding_ids = [finding.finding_id for finding in self.findings]
        if len(finding_ids) != len(set(finding_ids)):
            raise ValueError(
                "findings must not repeat a finding_id -- duplicate "
                "observations must dedupe to one canonical finding"
            )
        return self

    @model_validator(mode="after")
    def validate_findings_are_ordered(self) -> Self:
        keys = [finding_sort_key(finding) for finding in self.findings]
        if keys != sorted(keys):
            raise ValueError(
                "findings must be ordered worst-severity-first, then by "
                "category, then by finding_id"
            )
        return self

    @model_validator(mode="after")
    def validate_category_counts_match_findings(self) -> Self:
        actual_counts: dict[DeficiencyCategory, int] = dict.fromkeys(
            DEFICIENCY_CATEGORIES, 0
        )
        for finding in self.findings:
            actual_counts[finding.category] += 1
        for status in self.category_statuses:
            if status.finding_count != actual_counts[status.category]:
                raise ValueError(
                    f"category {status.category.value!r} declares "
                    f"finding_count={status.finding_count} but {actual_counts[status.category]} "
                    "finding(s) with that category are actually present in "
                    "findings -- the declared count and the findings "
                    "themselves must never diverge"
                )
        return self

    @model_validator(mode="after")
    def validate_findings_match_subject(self) -> Self:
        for finding in self.findings:
            if (
                finding.subject_kind != self.subject_kind
                or finding.subject_id != self.subject_id
            ):
                raise ValueError(
                    f"finding {finding.finding_id!r} names subject "
                    f"({finding.subject_kind.value!r}, {finding.subject_id!r}) "
                    f"but this inventory is for "
                    f"({self.subject_kind.value!r}, {self.subject_id!r}) -- "
                    "a finding from a different subject cannot appear in "
                    "another subject's inventory"
                )
        return self

    @model_validator(mode="after")
    def validate_findings_satisfy_evidence_discipline(self) -> Self:
        """F10, re-checked at the containment boundary -- see class docstring."""

        for finding in self.findings:
            has_evidence = bool(finding.evidence_ref_ids)
            has_classification = finding.evidence_classification is not None
            if has_evidence == has_classification:
                raise ValueError(
                    f"finding {finding.finding_id!r} violates F10: exactly "
                    "one of evidence_ref_ids or evidence_classification "
                    "must be set, never both and never neither -- this "
                    "finding either bypassed DeficiencyFinding's own "
                    "validator (model_construct/model_copy) or was "
                    "otherwise smuggled in"
                )
        return self


#: Worst-first ordinal for :class:`DeficiencySeverity` -- lower sorts
#: first, mirroring ``portfolio_status_service._DIMENSION_STATE_SEVERITY``'s
#: own worst-first convention.
_SEVERITY_ORDER: dict[DeficiencySeverity, int] = {
    DeficiencySeverity.CRITICAL: 0,
    DeficiencySeverity.AT_RISK: 1,
    DeficiencySeverity.WATCH: 2,
}


def finding_sort_key(finding: DeficiencyFinding) -> tuple[int, str, str]:
    return (
        _SEVERITY_ORDER[finding.severity],
        finding.category.value,
        finding.finding_id,
    )
