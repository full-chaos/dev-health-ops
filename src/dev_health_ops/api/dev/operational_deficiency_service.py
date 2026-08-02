"""``OperationalDeficiencyService`` (CHAOS-3305): ``deficiency.operational.v1``.

The canonical answer to "what operational deficiencies do we have?" for one
committed project or team subject -- a bounded, versioned, evidence-backed
inventory across the eight taxonomy categories in
:mod:`.contracts_v2.deficiency`. Composes the exact canonical calls
``ProjectHealthService``/``TeamHealthService`` (CHAOS-3303) already make
through the ``PlanExecutorRuntime`` seam -- never a second, parallel query
path -- and evaluates only rules approved by CHAOS-3302's
``HEALTH_RULE_REGISTRY``. No dimension, severity, percentage, or finding is
ever computed here from anything other than an already-computed canonical
service result.

Coverage today (2026-08-02), stated precisely rather than implied:

* **Category 1 (data & integration)** is fully bound: every ``DataHealthSource``
  a required source reports maps directly onto a finding kind, except
  "unusable returned data" (no canonical signal for it exists in
  ``DataHealthResult`` yet -- see ``_DATA_INTEGRATION_LIMITATION``).
  **Not attribution-independent for a TEAM subject** (corrected, Codex
  finding HIGH, 2026-08-02): a TEAM ``DevScope`` carries no
  ``repository_id`` of its own, so querying ``data_health`` with the raw
  TEAM scope resolves zero explicit repositories and falls back to
  querying every repository in the org. ``evaluate_team`` therefore
  re-scopes the ``data_health`` call to an explicit
  ``DirectScope.REPOSITORY`` built from the same attribution snapshot
  used for ``cohort_size``, never the raw TEAM scope.
* **Category 2 (planning & relationships)** is bound for unresolved
  blocking dependencies, incomplete required declarations, missing
  declared status, and declared-complete/observed-work conflicts, all
  reused directly from ``StatusSnapshotResult.actual``. "Orphaned work /
  missing project-work-unit mapping" (the ``work_graph_neighbors_service``
  arm the CHAOS-3305 planning brief named) is **not** bound this round --
  see ``_PLANNING_RELATIONSHIPS_LIMITATION``.
* **Categories 3-6 (delivery flow, review/CI, deployment/reliability,
  ownership/code risk)** are wired to ``HEALTH_RULE_REGISTRY`` via
  ``health_profile_synthesis.synthesize_health_profile`` -- the same
  synthesis 3303 already runs. Every rule shipped today is
  ``calibration_state=provisional`` (3302's own totality test), so every
  finding is ``shadow_only`` and **none reaches this inventory** (the
  explicit guardrail: "shadow_only findings never in status/counts"). The
  moment a future calibration review promotes a rule to a reviewed
  calibration state, the exact same code path produces a real finding with
  no change required here -- see ``_deficiency_from_health_rule_finding``.
  Six of the nine registry rules additionally have no canonical, scope-safe
  source wired at all yet (``health_profile_synthesis._UNBOUND_RULE_LIMITATIONS``);
  this module does not attempt to bind any of them, because doing so
  honestly requires a new canonical ``MetricQueryService``/
  ``PlanExecutorRuntime`` source (wip_congestion, review latency, flaky
  test rate, high churn) that does not exist yet -- approximating with a
  different unit's value is exactly what CHAOS-3303's own module docstring
  forbids.
* **Category 7 (capacity & cognitive load)** is not bound. A canonical
  aggregate-only source likely exists (``graphql/resolvers/cognitive_load.py``),
  but the CHAOS-3305 planning brief explicitly calls for its aggregation
  boundary to be *re-verified, not assumed* before any binding -- that
  verification did not happen this round, so the category is reported
  ``evaluated=False`` rather than risk a person-level leak.
* **Category 8 (investment balance)** is bound for TEAM subjects only
  (2026-08-02, following CHAOS-3304's merge): ``evaluate_team`` calls
  ``_investment_balance_profile``, an INVESTMENT-ONLY path that fetches
  only ``investment_mix`` and reuses CHAOS-3304's own adapter/rule pieces
  (``synthesize_health_profile``/``investment_allocation_shift_observation``/
  ``HEALTH_RULE_REGISTRY``) -- never ``TeamWorkloadService``'s whole
  service (Codex finding, HIGH, round 5: composing the full service made
  its own extra status_snapshot/data_health calls against the raw TEAM
  scope, reintroducing category 1's own org-wide-widening bug, and an
  unhandled dependency failure there could lose every category, not just
  8). Packages the resulting finding exactly like categories 3-6, through
  the same ``_rule_driven_category_result``. That rule is CHAOS-3331-blocked
  (``investment_allocation_shift_observation`` reports
  ``attribution_present=False`` unconditionally, see
  ``dimension_observation_adapters.py``) -- a genuinely computed shift
  therefore lands in ``suppressed_findings`` with ``missing_attribution``,
  never a launch finding, until CHAOS-3331 lands. PROJECT subjects have no
  team-workload equivalent (the rule's own ``applicability`` is
  ``(RuleApplicability.TEAM,)``), so category 8 stays ``evaluated=False``
  for a project inventory -- see
  ``_INVESTMENT_BALANCE_NOT_APPLICABLE_TO_PROJECT_LIMITATION``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from uuid import UUID, uuid5

from .contracts import DevScope, DirectScope, MetricID
from .contracts_v2.base import SourceRequirementState
from .contracts_v2.deficiency import (
    DeficiencyCategory,
    DeficiencyCategoryStatus,
    DeficiencyEvidenceClassification,
    DeficiencyFinding,
    DeficiencyRemediation,
    DeficiencySeverity,
    OperationalDeficiencyInventory,
    finding_sort_key,
)
from .contracts_v2.health_rules import (
    DimensionObservation,
    DimensionState,
    HealthDimension,
    HealthRuleFinding,
    RuleApplicability,
)
from .data_health_service import (
    NATIVE_EVIDENCE_SOURCES,
    DataHealthResult,
    DataHealthSource,
    DataHealthState,
)
from .health_profile_synthesis import (
    HealthEvaluationSources,
    HealthProfileResult,
    synthesize_health_profile,
)
from .health_rule_registry import HEALTH_RULE_REGISTRY
from .investigation_plans.builtin_steps import PlanExecutorRuntime
from .investigation_plans.state_mapping import (
    UNMEASURED_REQUIREMENT_STATES,
    data_health_state_to_requirement_state,
    status_result_state_to_requirement_state,
)
from .native_team_workload import TeamInvestmentMixResult
from .project_health_service import CHANGE_FAILURE_RATE_SUPPORTED_SCOPES
from .status_change_service import StatusResultState, StatusSnapshotResult
from .team_health_service import TeamAttributionSource
from .team_workload_service import TeamWorkloadDataSource

__all__ = ["OperationalDeficiencyService"]

#: Namespace for deterministic ``finding_id``/``inventory_id`` minting --
#: distinct from ``health_rule_registry._FINDING_ID_NAMESPACE`` (a
#: different contract's identity space; reusing the same namespace would
#: risk two unrelated finding kinds colliding on the same UUID5 output for
#: coincidentally identical input strings).
_DEFICIENCY_ID_NAMESPACE = UUID("5886b318-7f24-52bd-b661-671ed463397e")

#: A data-health check has no rolling comparison window of its own (it is a
#: point-in-time freshness/configuration read, not a threshold trend) --
#: this is the documented, fixed value every category-1 finding reports
#: for ``current_window_days``, never a fabricated windowed computation.
_POINT_IN_TIME_WINDOW_DAYS = 1

_DATA_INTEGRATION_LIMITATION = (
    "unusable_returned_data is not yet bound: DataHealthResult carries no "
    "canonical signal distinguishing usable-but-low-quality data from "
    "genuinely complete data."
)
_PLANNING_RELATIONSHIPS_LIMITATION = (
    "orphaned_work and missing_project_work_unit_mapping are not yet "
    "bound: binding them requires work_graph_neighbors_service, deferred "
    "in this version."
)
_CAPACITY_LIMITATION = (
    "capacity_cognitive_load is not evaluated: no HEALTH_RULE_REGISTRY "
    "rule exists for the cognitive_workload_pressure dimension, and the "
    "existing cognitive_load resolver's aggregation boundary has not been "
    "re-verified for this service (CHAOS-3305 planning brief requires "
    "re-verification, not assumption, before binding)."
)
_INVESTMENT_BALANCE_NOT_APPLICABLE_TO_PROJECT_LIMITATION = (
    "health_rule.investment_allocation_shift.v1 -- the only HEALTH_RULE_"
    "REGISTRY rule bound to the investment_balance dimension -- applies "
    "only to team subjects (its own RuleApplicability is (TEAM,)). There "
    "is no project-level investment-balance rule for this service to "
    "evaluate; this is a scope mismatch, not a missing binding."
)
_INVESTMENT_BALANCE_DEPENDENCY_UNAVAILABLE_LIMITATION = (
    "the investment-mix source failed (raised or timed out) during this "
    "evaluation, so category 8 could not be assessed for this team -- an "
    "expected, contained dependency outage, isolated from every other "
    "category, never a claim that the rule was evaluated and simply "
    "produced no finding."
)
_RULE_DRIVEN_SHADOW_LIMITATION = (
    "every applicable HEALTH_RULE_REGISTRY rule for this category is "
    "calibration_state=provisional today -- shadow_only findings are "
    "computed for calibration review but never surfaced as a deficiency."
)
_RULE_DRIVEN_SUPPRESSED_LIMITATION = (
    "every applicable HEALTH_RULE_REGISTRY rule for this category was "
    "guardrail-suppressed for this subject (insufficient sample, "
    "coverage, cohort, or a missing denominator/attribution) -- "
    "suppressed findings are never surfaced as a deficiency."
)
_RULE_DRIVEN_SHADOW_AND_SUPPRESSED_LIMITATION = (
    "this category's applicable HEALTH_RULE_REGISTRY rules are a mix of "
    "calibration_state=provisional (shadow_only) and guardrail-suppressed "
    "(insufficient sample, coverage, cohort, or a missing "
    "denominator/attribution) for this subject -- neither is ever "
    "surfaced as a deficiency."
)
_RULE_DRIVEN_PARTIAL_LIMITATION = (
    "not every applicable HEALTH_RULE_REGISTRY rule for this category "
    "contributed to these findings -- some are calibration_state="
    "provisional (shadow_only) and/or guardrail-suppressed (insufficient "
    "sample, coverage, cohort, or a missing denominator/attribution) for "
    "this subject and are never surfaced as a deficiency, even though "
    "other rules for this category did produce the finding(s) above."
)
_RULE_DRIVEN_UNREGISTERED_LIMITATION = (
    "no HEALTH_RULE_REGISTRY rule is registered for this category yet."
)
_TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION = (
    "team_repository_ids lookup failed as of the committed scope's window "
    "end -- cohort size could not be verified, so every team-cohort-gated "
    "category is honestly unmeasured, never insufficient_cohort (which "
    "would claim attribution was checked and found too small)."
)

#: ``HealthDimension`` -> the one ``DeficiencyCategory`` its findings feed,
#: or ``None`` when a dimension is deliberately excluded (see below).
#: Total over every ``HealthDimension`` member (enforced at import time)
#: so a future dimension added to CHAOS-3302 fails import here rather than
#: silently never reaching the inventory.
_HEALTH_DIMENSION_TO_DEFICIENCY_CATEGORY: Mapping[str, DeficiencyCategory | None] = {
    "execution_completion": DeficiencyCategory.DELIVERY_FLOW,
    "delivery_flow": DeficiencyCategory.DELIVERY_FLOW,
    "reliability_release": DeficiencyCategory.DEPLOYMENT_RELIABILITY,
    "review_ci_pressure": DeficiencyCategory.REVIEW_CI,
    "code_ownership_risk": DeficiencyCategory.OWNERSHIP_CODE_RISK,
    "cognitive_workload_pressure": DeficiencyCategory.CAPACITY_COGNITIVE_LOAD,
    "investment_balance": DeficiencyCategory.INVESTMENT_BALANCE,
    "dependencies_blockers": DeficiencyCategory.PLANNING_RELATIONSHIPS,
    # DATA_TRUST is intentionally excluded: category 1 (data_integration)
    # is already computed directly from DataHealthResult (see
    # _data_integration_findings). Also folding in health_rule.
    # data_trust_broken.v1 -- the *same* underlying DataHealthResult.
    # complete_eligible signal, re-expressed as a HealthRuleFinding --
    # would violate "one canonical finding per observation" by minting a
    # second finding for the identical condition.
    "data_trust": None,
}

_undocumented_dimensions = frozenset(HealthDimension) - frozenset(
    HealthDimension(value) for value in _HEALTH_DIMENSION_TO_DEFICIENCY_CATEGORY
)
if _undocumented_dimensions:
    raise RuntimeError(
        "operational_deficiency_service has no deficiency-category binding "
        f"for HealthDimension member(s): {sorted(_undocumented_dimensions)}"
    )

#: The rule-registry-fed categories, in a fixed order used only for
#: deterministic iteration (final ordering of the inventory's own
#: ``findings`` is enforced by ``finding_sort_key``, not this tuple).
#: ``INVESTMENT_BALANCE`` (category 8) is included even though its one
#: bound rule is TEAM-only -- see ``_rule_driven_results``' ``workload_
#: profile`` handling, which is what actually supplies its bucket for a
#: TEAM subject and reports it honestly not-applicable for a PROJECT one.
#: ``CAPACITY_COGNITIVE_LOAD`` (category 7) is deliberately NOT here: no
#: HEALTH_RULE_REGISTRY rule maps to it via this pipeline today (see the
#: module docstring and ``_CAPACITY_LIMITATION``).
_RULE_DRIVEN_CATEGORIES: tuple[DeficiencyCategory, ...] = (
    DeficiencyCategory.DELIVERY_FLOW,
    DeficiencyCategory.REVIEW_CI,
    DeficiencyCategory.DEPLOYMENT_RELIABILITY,
    DeficiencyCategory.OWNERSHIP_CODE_RISK,
    DeficiencyCategory.INVESTMENT_BALANCE,
)


def _mint_deficiency_finding_id(
    *,
    org_id: str,
    category: DeficiencyCategory,
    rule_id: str,
    subject_kind: str,
    subject_id: str,
    discriminator: str,
) -> str:
    """Deterministic, REPLAY-STABLE finding identity.

    Deliberately excludes any timestamp (Codex finding, 2026-08-02): a
    finding's identity is *what it is about* -- the closed
    (org, category, rule, subject, discriminator) tuple -- never *when it
    was last evaluated*. ``evaluated_at`` is carried on the finding as
    ordinary metadata, not folded into this payload; including it here
    made re-evaluating the exact same underlying condition one second
    apart mint two different ids, breaking both the dedupe guarantee this
    function backs (see ``_dedupe_findings``) and the replay/determinism
    expectation the same canonical deficiency has the same identity across
    every evaluation that observes it, not just the first one. This
    intentionally diverges from ``health_rule_registry._mint_finding_id``
    (which *does* fold in ``observed_at``/``window_index`` because a
    ``HealthRuleFinding`` identifies one evaluated window instance, not a
    persistent condition) -- a ``DeficiencyFinding`` identifies the
    deficiency itself. ``discriminator`` carries whatever distinguishes two
    findings of the same category/rule/subject that are not the same
    observation (e.g. a data-integration finding's ``source_system``, or a
    planning finding's reason code) -- the dedupe key this function's
    output *is* the finding_id, so two calls with identical inputs
    (including an identical discriminator) always collapse to one finding.
    """

    payload = "|".join(
        (org_id, category.value, rule_id, subject_kind, subject_id, discriminator)
    )
    return str(uuid5(_DEFICIENCY_ID_NAMESPACE, payload))


def _mint_inventory_id(
    *, org_id: str, subject_kind: str, subject_id: str, evaluated_at: datetime
) -> str:
    normalized = evaluated_at.astimezone(UTC).isoformat(timespec="microseconds")
    payload = "|".join((org_id, subject_kind, subject_id, normalized))
    return str(uuid5(_DEFICIENCY_ID_NAMESPACE, f"inventory|{payload}"))


def _evidence_or_classification(
    ids: Sequence[str],
) -> tuple[tuple[str, ...], DeficiencyEvidenceClassification | None]:
    deduped = tuple(dict.fromkeys(ids))[:25]
    if deduped:
        return deduped, None
    return (), DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE


# ---------------------------------------------------------------------------
# Category 1: data & integration
# ---------------------------------------------------------------------------

#: (finding-kind suffix, severity) per non-complete, non-not-applicable
#: ``DataHealthState`` -- ``COMPLETE`` never fires (no deficiency).
_DATA_INTEGRATION_KIND_SEVERITY: Mapping[
    DataHealthState, tuple[str, DeficiencySeverity]
] = {
    DataHealthState.UNCONFIGURED: (
        "unconfigured_required_source",
        DeficiencySeverity.CRITICAL,
    ),
    DataHealthState.UNAVAILABLE: ("active_sync_failure", DeficiencySeverity.CRITICAL),
    DataHealthState.STALE: ("stale_watermark", DeficiencySeverity.AT_RISK),
    DataHealthState.NO_DATA: ("missing_subject_coverage", DeficiencySeverity.WATCH),
    DataHealthState.UNAUTHORIZED: ("source_unauthorized", DeficiencySeverity.AT_RISK),
}


def _data_semantics_for_observed_state(
    observed_state: SourceRequirementState,
) -> str:
    """Mirrors ``DeficiencyFinding.validate_zero_semantics``'s own queried/
    unmeasured split (Codex finding, 2026-08-02): a finding about an
    unconfigured/unavailable/unauthorized source -- itself the deficiency
    -- must report ``not_measured``, never a fabricated ``measured_zero``
    standing in for "we never checked".
    """

    if observed_state in UNMEASURED_REQUIREMENT_STATES:
        return "not_measured"
    return "measured_zero"


def _data_integration_finding(
    source: DataHealthSource,
    *,
    org_id: str,
    subject_kind: RuleApplicability,
    subject_id: str,
    now: datetime,
) -> DeficiencyFinding | None:
    if not source.required or source.state not in _DATA_INTEGRATION_KIND_SEVERITY:
        return None
    kind, severity = _DATA_INTEGRATION_KIND_SEVERITY[source.state]
    rule_id = f"deficiency_rule.{kind}.v1"
    observed_state = data_health_state_to_requirement_state(source.state)
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_mint_deficiency_finding_id(
            org_id=org_id,
            category=DeficiencyCategory.DATA_INTEGRATION,
            rule_id=rule_id,
            subject_kind=subject_kind.value,
            subject_id=subject_id,
            discriminator=source.source_system,
        ),
        category=DeficiencyCategory.DATA_INTEGRATION,
        rule_id=rule_id,
        rule_version=rule_id,
        subject_kind=subject_kind,
        subject_id=subject_id,
        severity=severity,
        fact_kind="observed",
        observed_state=observed_state,
        data_semantics=_data_semantics_for_observed_state(observed_state),
        sample_count=None,
        coverage=source.coverage,
        current_window_days=_POINT_IN_TIME_WINDOW_DAYS,
        comparison_window_days=None,
        evidence_ref_ids=(),
        evidence_classification=DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE,
        blast_radius=(
            f"Required source '{source.source_system}' is {source.state.value}; "
            "findings drawing on this source may be degraded or unavailable."
        ),
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template=(
                f"Restore or (re)configure the '{source.source_system}' source integration."
            ),
            verification_condition=(
                f"Resolves once '{source.source_system}' reports DataHealthState.complete "
                "on the next inventory evaluation."
            ),
        ),
        limitations=(),
        evaluated_at=now,
    )


def _data_integration_result(
    data_health: DataHealthResult,
    *,
    org_id: str,
    subject_kind: RuleApplicability,
    subject_id: str,
    now: datetime,
) -> tuple[DeficiencyCategoryStatus, tuple[DeficiencyFinding, ...]]:
    if not data_health.sources:
        return (
            DeficiencyCategoryStatus(
                schema_version="deficiency_category_status.v1",
                category=DeficiencyCategory.DATA_INTEGRATION,
                evaluated=False,
                finding_count=0,
                applicability_states_observed=(),
                limitation="data_health_never_queried: no required sources were evaluated.",
            ),
            (),
        )
    findings = tuple(
        finding
        for source in data_health.sources
        if (
            finding := _data_integration_finding(
                source,
                org_id=org_id,
                subject_kind=subject_kind,
                subject_id=subject_id,
                now=now,
            )
        )
        is not None
    )
    observed_states = tuple(
        sorted(
            {
                data_health_state_to_requirement_state(source.state)
                for source in data_health.sources
            },
            key=lambda state: state.value,
        )
    )
    status = DeficiencyCategoryStatus(
        schema_version="deficiency_category_status.v1",
        category=DeficiencyCategory.DATA_INTEGRATION,
        evaluated=True,
        finding_count=len(findings),
        applicability_states_observed=observed_states,
        limitation=_DATA_INTEGRATION_LIMITATION,
    )
    return status, findings


# ---------------------------------------------------------------------------
# Team data_health batching (Codex finding, HIGH, 2026-08-02): DevScope.
# repositories caps at 20 (Field(max_length=20)); a team attributed more
# than 20 repositories raised ValidationError building the REPOSITORY scope
# and lost its ENTIRE inventory, not just category 1. The public DevScope
# wire contract is not widened to fix this (no internal scope type crosses
# the PlanExecutorRuntime protocol boundary either -- data_health's only
# parameter is a DevScope) -- instead, evaluate_team batches the attributed
# repository set into <=20-repo DevScope chunks and reconciles the merged
# result, exactly, never a silent truncation to the first chunk.
# ---------------------------------------------------------------------------

#: Matches DevScope.repositories' own ``Field(max_length=20)`` exactly --
#: not an independently chosen batch size.
_MAX_REPOSITORIES_PER_SCOPE = 20

#: Worst-to-best ordinal for ``DataHealthState`` -- lower is worse. Mirrors
#: ``dimension_observation_adapters._STATE_SEVERITY``'s own worst-first
#: convention, at the ``DataHealthState`` layer instead of the mapped
#: ``SourceRequirementState`` layer (the two enums are not identical, so a
#: separate table is needed rather than reusing that one directly).
_DATA_HEALTH_STATE_SEVERITY: Mapping[DataHealthState, int] = {
    DataHealthState.UNAUTHORIZED: 0,
    DataHealthState.UNAVAILABLE: 1,
    DataHealthState.UNCONFIGURED: 2,
    DataHealthState.STALE: 3,
    DataHealthState.NO_DATA: 4,
    DataHealthState.COMPLETE: 5,
}


def _earliest(a: datetime | None, b: datetime | None) -> datetime | None:
    """The "worse" of two optional timestamps for freshness merging: a
    missing timestamp on either side means that side's source never
    succeeded, which is at least as bad as any real timestamp the other
    side carries, so the merged result must not claim a real one either.
    Otherwise, the earlier (staler) of the two.
    """

    if a is None or b is None:
        return None
    return min(a, b)


#: Synthesized ``warning`` marker for a source_system a batch omitted
#: entirely -- distinguishes a fail-closed placeholder from anything a
#: real ``DataHealthService.inspect`` call ever produces.
_OMITTED_BATCH_SOURCE_WARNING = "batch_omitted_source_system"

#: Every one of ``NATIVE_EVIDENCE_SOURCES`` is unconditionally
#: ``required=True`` by construction in
#: ``NativeDataHealthReader.read()`` (the "acr" special case is NOT a
#: member of this canonical set). Kept as an explicit lookup rather than a
#: blanket literal so a placeholder's requiredness is always traceable to
#: this ONE table, not a second, independently-drifting assumption.
_CANONICAL_SOURCE_REQUIRED: Mapping[str, bool] = dict.fromkeys(
    NATIVE_EVIDENCE_SOURCES, True
)


def _placeholder_for_omitted_source(
    source_system: str, batch_repository_ids: Sequence[str]
) -> DataHealthSource:
    """A fail-closed stand-in for a source_system one batch never reported
    at all, scoped to exactly that batch's own repositories -- see
    ``_merge_data_health_sources``'s docstring for why this must exist.

    ``required`` comes from ``_CANONICAL_SOURCE_REQUIRED`` (Codex finding,
    MEDIUM, 2026-08-02) -- never a blanket ``True`` literal, which would
    misreport a genuinely optional source_system as required the moment a
    batch omits it. Defaults to ``True`` only for a source_system this
    table has no entry for at all, which is itself a fail-closed choice:
    an unrecognized source_system omitted by a batch is treated as if it
    mattered, never silently ignored.
    """

    return DataHealthSource(
        source_system=source_system,
        state=DataHealthState.UNAVAILABLE,
        required=_CANONICAL_SOURCE_REQUIRED.get(source_system, True),
        last_successful_at=None,
        watermark=None,
        missing_repository_ids=tuple(sorted(batch_repository_ids)),
        missing_entity_ids=(),
        coverage=0.0,
        confidence_impact="insufficient_evidence",
        freshness_policy_version=None,
        warning=_OMITTED_BATCH_SOURCE_WARNING,
    )


def _merge_data_health_sources(
    sources_by_batch: Sequence[tuple[DataHealthSource, ...]],
    batch_repository_ids: Sequence[Sequence[str]],
    *,
    total_repositories: int,
) -> tuple[DataHealthSource, ...]:
    """Reconcile one team's chunked ``data_health`` calls into one honest
    picture, per ``source_system``.

    Fail-closed against an omitted or short batch (Codex finding, HIGH,
    2026-08-02, round 2): the expected source_system set is
    ``NATIVE_EVIDENCE_SOURCES`` -- the SAME canonical, production-owned
    default set ``DataHealthService.inspect`` itself queries against
    (``PlanExecutorRuntime.data_health`` carries no ``required_sources``
    parameter of its own, so this is not merely "a" reasonable default,
    it is the ONLY set any real batch call can ever be answering for) --
    never derived from what the batches themselves happened to return.
    The first fix (round 1) unioned the batches' own reported systems,
    which still silently passed when EVERY batch returned ``sources=()``
    (empty union, nothing to flag). Pinning the expected set to the
    canonical contract instead means a total measurement failure across
    every batch is caught exactly like a partial one -- any batch missing
    a member of ``NATIVE_EVIDENCE_SOURCES`` gets an explicit
    ``UNAVAILABLE`` placeholder synthesized for it, scoped to that
    batch's own repositories, before merging.

    ``required`` is the OR of every batch's own ``required`` flag for that
    source_system (Codex finding, MEDIUM, round 2) -- "did ANY batch need
    this source" is independent of which batch's state wins below.

    ``state``/``missing_repository_ids``/``missing_entity_ids``/``coverage``
    are aggregated over the REQUIRED records ONLY when at least one batch
    actually required this source_system -- never blended with an
    unrelated OPTIONAL batch's own failure (Codex finding, MEDIUM PLAUSIBLE,
    round 5): the round-2 fix above still let an optional+UNAVAILABLE
    batch's state win the worst-state comparison against a required+
    COMPLETE batch for the SAME source_system, reporting the merged source
    as ``required=True``/``UNAVAILABLE`` (blocking ``complete_eligible``)
    even though every batch that actually required it was fully COMPLETE.
    "Blocking state" and "required coverage" must answer "how did the
    batches that needed this source do", not "how did every batch that
    happened to touch it do, required or not". A source_system no batch
    ever required falls back to aggregating its optional records instead
    (still an honest, reportable state -- just never able to block
    ``complete_eligible`` on its own, since ``required`` stays ``False``).
    Within whichever pool is chosen, ``state`` is still the WORST record (a
    degraded batch must never be masked by a healthy one) and
    ``missing_repository_ids``/``missing_entity_ids`` are still the union
    (repositories never overlap between chunks by construction, so this is
    also automatically deduplicated). ``coverage`` is recomputed EXACTLY
    from ``total_repositories`` (the caller's own known, authoritative
    repository count) minus the merged missing count -- never a weighted
    average of each batch's own ratio, which would compound rounding error
    across an arbitrary number of chunks.
    """

    expected_source_systems = set(NATIVE_EVIDENCE_SOURCES)
    normalized_batches: list[tuple[DataHealthSource, ...]] = []
    for batch_sources, batch_repos in zip(
        sources_by_batch, batch_repository_ids, strict=True
    ):
        present = {source.source_system for source in batch_sources}
        omitted = expected_source_systems - present
        placeholders = tuple(
            _placeholder_for_omitted_source(system, batch_repos) for system in omitted
        )
        normalized_batches.append(batch_sources + placeholders)

    # Two independent record pools per source_system -- see the docstring.
    required_records: dict[str, list[DataHealthSource]] = {}
    optional_records: dict[str, list[DataHealthSource]] = {}
    required_by_source: dict[str, bool] = {}
    for batch_sources in normalized_batches:
        for source in batch_sources:
            required_by_source[source.source_system] = (
                required_by_source.get(source.source_system, False) or source.required
            )
            pool = required_records if source.required else optional_records
            pool.setdefault(source.source_system, []).append(source)

    results: list[DataHealthSource] = []
    for source_system, required in required_by_source.items():
        records = required_records.get(source_system) or optional_records.get(
            source_system, []
        )
        missing_repository_ids: set[str] = set()
        missing_entity_ids: set[str] = set()
        worst = records[0]
        for record in records:
            missing_repository_ids.update(record.missing_repository_ids)
            missing_entity_ids.update(record.missing_entity_ids)
            if (
                _DATA_HEALTH_STATE_SEVERITY[record.state]
                < _DATA_HEALTH_STATE_SEVERITY[worst.state]
            ):
                worst = record
            elif record is not worst:
                worst = DataHealthSource(
                    source_system=worst.source_system,
                    state=worst.state,
                    required=worst.required,
                    last_successful_at=_earliest(
                        worst.last_successful_at, record.last_successful_at
                    ),
                    watermark=_earliest(worst.watermark, record.watermark),
                    missing_repository_ids=worst.missing_repository_ids,
                    missing_entity_ids=worst.missing_entity_ids,
                    coverage=worst.coverage,
                    confidence_impact=worst.confidence_impact
                    or record.confidence_impact,
                    freshness_policy_version=(
                        worst.freshness_policy_version
                        or record.freshness_policy_version
                    ),
                    warning=worst.warning or record.warning,
                )
        missing_repository_ids_tuple = tuple(sorted(missing_repository_ids))
        coverage = (
            (total_repositories - len(missing_repository_ids_tuple))
            / total_repositories
            if total_repositories
            else 0.0
        )
        results.append(
            DataHealthSource(
                source_system=source_system,
                state=worst.state,
                required=required,
                last_successful_at=worst.last_successful_at,
                watermark=worst.watermark,
                missing_repository_ids=missing_repository_ids_tuple,
                missing_entity_ids=tuple(sorted(missing_entity_ids)),
                coverage=max(0.0, min(1.0, coverage)),
                confidence_impact=worst.confidence_impact,
                freshness_policy_version=worst.freshness_policy_version,
                warning=worst.warning,
            )
        )
    return tuple(results)


# ---------------------------------------------------------------------------
# Category 2: planning & relationships
# ---------------------------------------------------------------------------

#: (reason code -> (finding-kind suffix, severity)). Reused directly from
#: ``StatusSnapshotResult.actual.reason_codes`` -- never a re-derivation of
#: the private "open"/"incomplete" status predicates ``_assess`` computes
#: (see the module docstring's evidence-aggregation tradeoff note).
_PLANNING_REASON_CODE_KIND: Mapping[str, tuple[str, DeficiencySeverity]] = {
    "open_blocker": ("unresolved_blocking_dependency", DeficiencySeverity.AT_RISK),
    "required_child_incomplete": (
        "incomplete_required_declaration",
        DeficiencySeverity.AT_RISK,
    ),
    "declared_status_missing": ("missing_declared_status", DeficiencySeverity.WATCH),
}
_DECLARED_COMPLETE_CONFLICT_CODE = "declared_complete_conflicts_with_observed_work"


def _planning_relationships_result(
    snapshot: StatusSnapshotResult,
    *,
    org_id: str,
    subject_kind: RuleApplicability,
    subject_id: str,
    now: datetime,
) -> tuple[DeficiencyCategoryStatus, tuple[DeficiencyFinding, ...]]:
    if snapshot.state is StatusResultState.INSUFFICIENT_EVIDENCE:
        return (
            DeficiencyCategoryStatus(
                schema_version="deficiency_category_status.v1",
                category=DeficiencyCategory.PLANNING_RELATIONSHIPS,
                evaluated=False,
                finding_count=0,
                applicability_states_observed=(),
                limitation="status_snapshot reported insufficient_evidence: no required source was queried.",
            ),
            (),
        )

    findings: list[DeficiencyFinding] = []
    blocker_evidence = [
        ref_id for blocker in snapshot.blockers for ref_id in blocker.evidence_ref_ids
    ]
    child_evidence = [
        ref_id
        for child in snapshot.actual.required_children
        for ref_id in child.evidence_ref_ids
    ]
    for reason_code, (kind, severity) in _PLANNING_REASON_CODE_KIND.items():
        if reason_code not in snapshot.actual.reason_codes:
            continue
        rule_id = f"deficiency_rule.{kind}.v1"
        if reason_code == "open_blocker":
            evidence_ids, classification = _evidence_or_classification(blocker_evidence)
            blast_radius = "One or more declared blocking dependencies are still open."
        elif reason_code == "required_child_incomplete":
            evidence_ids, classification = _evidence_or_classification(child_evidence)
            blast_radius = "One or more required child work items are not yet complete."
        else:
            evidence_ids, classification = (
                (),
                DeficiencyEvidenceClassification.STRUCTURAL_ABSENCE,
            )
            blast_radius = "No declared status was found for this subject."
        findings.append(
            DeficiencyFinding(
                schema_version="deficiency_finding.v1",
                finding_id=_mint_deficiency_finding_id(
                    org_id=org_id,
                    category=DeficiencyCategory.PLANNING_RELATIONSHIPS,
                    rule_id=rule_id,
                    subject_kind=subject_kind.value,
                    subject_id=subject_id,
                    discriminator=reason_code,
                ),
                category=DeficiencyCategory.PLANNING_RELATIONSHIPS,
                rule_id=rule_id,
                rule_version=rule_id,
                subject_kind=subject_kind,
                subject_id=subject_id,
                severity=severity,
                fact_kind="observed",
                observed_state=status_result_state_to_requirement_state(snapshot.state),
                data_semantics="measured_zero",
                sample_count=None,
                coverage=1.0,
                current_window_days=_POINT_IN_TIME_WINDOW_DAYS,
                comparison_window_days=None,
                evidence_ref_ids=evidence_ids,
                evidence_classification=classification,
                blast_radius=blast_radius,
                remediation=DeficiencyRemediation(
                    schema_version="deficiency_remediation.v1",
                    remediation_template=(
                        "Review and resolve the outstanding planning/relationship "
                        "gap with the team before the next assessment."
                    ),
                    verification_condition=(
                        f"Resolves once '{reason_code}' no longer appears in the "
                        "subject's actual-completion reason codes."
                    ),
                ),
                limitations=(),
                evaluated_at=now,
            )
        )
    for conflict in snapshot.actual.conflicts:
        if conflict.code != _DECLARED_COMPLETE_CONFLICT_CODE:
            continue
        rule_id = "deficiency_rule.declared_complete_conflict.v1"
        evidence_ids, classification = _evidence_or_classification(
            conflict.evidence_ref_ids
        )
        findings.append(
            DeficiencyFinding(
                schema_version="deficiency_finding.v1",
                finding_id=_mint_deficiency_finding_id(
                    org_id=org_id,
                    category=DeficiencyCategory.PLANNING_RELATIONSHIPS,
                    rule_id=rule_id,
                    subject_kind=subject_kind.value,
                    subject_id=subject_id,
                    discriminator=conflict.code,
                ),
                category=DeficiencyCategory.PLANNING_RELATIONSHIPS,
                rule_id=rule_id,
                rule_version=rule_id,
                subject_kind=subject_kind,
                subject_id=subject_id,
                severity=DeficiencySeverity.CRITICAL,
                fact_kind="observed",
                observed_state=status_result_state_to_requirement_state(snapshot.state),
                data_semantics="measured_zero",
                sample_count=None,
                coverage=1.0,
                current_window_days=_POINT_IN_TIME_WINDOW_DAYS,
                comparison_window_days=None,
                evidence_ref_ids=evidence_ids,
                evidence_classification=classification,
                blast_radius=conflict.message,
                remediation=DeficiencyRemediation(
                    schema_version="deficiency_remediation.v1",
                    remediation_template=(
                        "Reconcile the declared status against the observed required "
                        "work before trusting either."
                    ),
                    verification_condition=(
                        "Resolves once the declared-complete/observed-work conflict "
                        "no longer appears in the subject's actual-completion result."
                    ),
                ),
                limitations=(),
                evaluated_at=now,
            )
        )

    status = DeficiencyCategoryStatus(
        schema_version="deficiency_category_status.v1",
        category=DeficiencyCategory.PLANNING_RELATIONSHIPS,
        evaluated=True,
        finding_count=len(findings),
        applicability_states_observed=(
            status_result_state_to_requirement_state(snapshot.state),
        ),
        limitation=_PLANNING_RELATIONSHIPS_LIMITATION,
    )
    return status, tuple(findings)


# ---------------------------------------------------------------------------
# Categories 3-6: rule-registry-driven
# ---------------------------------------------------------------------------


def _deficiency_from_health_rule_finding(
    finding: HealthRuleFinding,
    *,
    category: DeficiencyCategory,
    observation: DimensionObservation | None,
    org_id: str,
) -> DeficiencyFinding:
    rule = HEALTH_RULE_REGISTRY.rule(finding.rule_id)
    observed_state = (
        observation.observed_states[0]
        if observation is not None and observation.observed_states
        else SourceRequirementState.AVAILABLE_CURRENT
    )
    coverage = (
        observation.coverage if observation is not None else rule.minimum_coverage
    )
    sample_count = observation.sample_count if observation is not None else None
    return DeficiencyFinding(
        schema_version="deficiency_finding.v1",
        finding_id=_mint_deficiency_finding_id(
            org_id=org_id,
            category=category,
            rule_id=finding.rule_id,
            subject_kind=finding.subject_kind.value,
            subject_id=finding.subject_id,
            discriminator="",
        ),
        category=category,
        rule_id=finding.rule_id,
        rule_version=finding.rule_version,
        subject_kind=finding.subject_kind,
        subject_id=finding.subject_id,
        severity=DeficiencySeverity(finding.state.value),
        fact_kind=finding.fact_kind,
        observed_state=observed_state,
        data_semantics="measured_zero",
        sample_count=sample_count,
        coverage=coverage,
        current_window_days=rule.current_window_days,
        comparison_window_days=rule.comparison_window_days,
        evidence_ref_ids=(),
        evidence_classification=DeficiencyEvidenceClassification.SOURCE_CLASS_ONLY,
        blast_radius=(
            f"{rule.dimension.value} for {finding.subject_kind.value} "
            f"'{finding.subject_id}' is {finding.state.value}."
        ),
        remediation=DeficiencyRemediation(
            schema_version="deficiency_remediation.v1",
            remediation_template=rule.remediation_template,
            verification_condition=(
                f"Resolves once {finding.rule_id} reports healthy for "
                f"{rule.sustained_periods_required} consecutive window(s)."
            ),
        ),
        limitations=(),
        evaluated_at=finding.evaluated_at,
    )


def _rule_driven_category_result(
    category: DeficiencyCategory,
    *,
    launch: Sequence[HealthRuleFinding],
    shadow: Sequence[HealthRuleFinding],
    suppressed: Sequence[HealthRuleFinding],
    observations_by_rule: Mapping[str, DimensionObservation],
    org_id: str,
    unregistered_limitation: str = _RULE_DRIVEN_UNREGISTERED_LIMITATION,
) -> tuple[DeficiencyCategoryStatus, tuple[DeficiencyFinding, ...]]:
    if not launch and not shadow and not suppressed:
        return (
            DeficiencyCategoryStatus(
                schema_version="deficiency_category_status.v1",
                category=category,
                evaluated=False,
                finding_count=0,
                applicability_states_observed=(),
                limitation=unregistered_limitation,
            ),
            (),
        )
    # Only HealthProfileResult.launch_findings ever becomes a deficiency --
    # shadow_findings (calibration-only, every rule shipped today) and
    # suppressed_findings (guardrail-suppressed) must never feed
    # status/counts, mirroring PortfolioStatusService's own post-Codex-fix
    # posture (03da63aeb): a caller that read from a merged bucket would
    # treat pure calibration data as launch authority.
    real = [f for f in launch if f.state is not DimensionState.HEALTHY]
    deficiencies = tuple(
        _deficiency_from_health_rule_finding(
            f,
            category=category,
            observation=observations_by_rule.get(f.rule_id),
            org_id=org_id,
        )
        for f in real
    )
    # Checks BOTH shadow and suppressed, never shadow alone (fixed after
    # CHAOS-3304 changed health_rule_registry's own partition order to
    # check suppressed_reason before shadow_only: a rule that is both
    # provisional AND guardrail-suppressed -- e.g. incident_load with
    # sample_count=0 clearing the "queried" short-circuit but failing
    # minimum_sample -- now lands in `suppressed`, not `shadow`. A
    # category whose only non-launch findings are all suppressed would
    # otherwise report finding_count=0/limitation=None, indistinguishable
    # from "genuinely nothing to disclose" when real, silenced
    # information exists. This was previously masked in every reachable
    # test scenario only because a SECOND bound rule for the same
    # category happened to always stay in shadow -- a coincidence of
    # today's specific rule set, not a structural guarantee, so it is
    # fixed here rather than left to keep being accidentally protected.
    #
    # `launch` is checked FIRST and separately (Codex finding, MEDIUM,
    # 2026-08-02, round 2): the three wordings above each say "every
    # applicable rule" was shadow/suppressed -- literally false the
    # moment `launch` is also non-empty (some rule DID clear calibration
    # and every guardrail). A mixed category gets its own, honestly
    # weaker claim ("not every rule contributed") instead.
    if launch and (shadow or suppressed):
        limitation = _RULE_DRIVEN_PARTIAL_LIMITATION
    elif shadow and suppressed:
        limitation = _RULE_DRIVEN_SHADOW_AND_SUPPRESSED_LIMITATION
    elif shadow:
        limitation = _RULE_DRIVEN_SHADOW_LIMITATION
    elif suppressed:
        limitation = _RULE_DRIVEN_SUPPRESSED_LIMITATION
    else:
        limitation = None
    status = DeficiencyCategoryStatus(
        schema_version="deficiency_category_status.v1",
        category=category,
        evaluated=True,
        finding_count=len(deficiencies),
        applicability_states_observed=(),
        limitation=limitation,
    )
    return status, deficiencies


def _rule_driven_results(
    health_profile: HealthProfileResult,
    *,
    org_id: str,
    workload_profile: HealthProfileResult | None = None,
    workload_dependency_failed: bool = False,
) -> tuple[tuple[DeficiencyCategoryStatus, ...], tuple[DeficiencyFinding, ...]]:
    """Bucket every rule-driven category's launch/shadow/suppressed findings.

    ``INVESTMENT_BALANCE`` (category 8) is a special case: its one bound
    rule (``health_rule.investment_allocation_shift.v1``) is TEAM-only and
    reads ``investment_mix``, a source ``health_profile`` never carries
    (see ``evaluate_team``'s ``rule_engine_sources`` docstring) -- so its
    bucket in ``health_profile`` is always empty by construction, never a
    genuine "nothing to report". ``workload_profile`` (``_investment_
    balance_profile``'s own ``HealthProfileResult``, TEAM subjects only) is
    the ONLY source this function ever reads that category's bucket from;
    when it is ``None`` the category reports an honest, scope-specific "not
    applicable" limitation instead of the generic "unregistered" one -- a
    rule genuinely IS registered for this dimension, it simply cannot apply
    to a project subject. ``workload_profile`` is also ``None`` for a TEAM
    subject whose attribution lookup failed (``evaluate_team`` skips the
    call entirely in that case) -- the "not applicable to project" wording
    computed here is discarded either way by ``_assemble``'s own
    ``attribution_unavailable`` override, which is the actually-correct
    text for that case.
    """

    def _bucket(
        findings: Sequence[HealthRuleFinding],
    ) -> dict[DeficiencyCategory, list[HealthRuleFinding]]:
        by_category: dict[DeficiencyCategory, list[HealthRuleFinding]] = {
            category: [] for category in _RULE_DRIVEN_CATEGORIES
        }
        for finding in findings:
            category = _HEALTH_DIMENSION_TO_DEFICIENCY_CATEGORY[finding.dimension.value]
            if category in by_category:
                by_category[category].append(finding)
        return by_category

    launch_by_category = _bucket(health_profile.launch_findings)
    shadow_by_category = _bucket(health_profile.shadow_findings)
    suppressed_by_category = _bucket(health_profile.suppressed_findings)
    observations_by_rule: Mapping[str, DimensionObservation] = (
        health_profile.observations_by_rule
    )

    investment_balance_unregistered_limitation = _RULE_DRIVEN_UNREGISTERED_LIMITATION
    if workload_profile is not None:
        workload_launch = _bucket(workload_profile.launch_findings)
        workload_shadow = _bucket(workload_profile.shadow_findings)
        workload_suppressed = _bucket(workload_profile.suppressed_findings)
        investment_findings = (
            workload_launch[DeficiencyCategory.INVESTMENT_BALANCE]
            + workload_shadow[DeficiencyCategory.INVESTMENT_BALANCE]
            + workload_suppressed[DeficiencyCategory.INVESTMENT_BALANCE]
        )
        launch_by_category[DeficiencyCategory.INVESTMENT_BALANCE] = workload_launch[
            DeficiencyCategory.INVESTMENT_BALANCE
        ]
        shadow_by_category[DeficiencyCategory.INVESTMENT_BALANCE] = workload_shadow[
            DeficiencyCategory.INVESTMENT_BALANCE
        ]
        suppressed_by_category[DeficiencyCategory.INVESTMENT_BALANCE] = (
            workload_suppressed[DeficiencyCategory.INVESTMENT_BALANCE]
        )
        # Overlay ONLY the observation(s) behind the findings actually
        # harvested into INVESTMENT_BALANCE's buckets above -- NEVER the
        # whole workload_profile.observations_by_rule map (Codex finding,
        # HIGH, round 6): synthesize_health_profile builds a synthesized-
        # unavailable observation for EVERY OTHER TEAM-applicable rule too
        # (the three cognitive-load ones today), keyed by their real
        # rule_id -- a wholesale dict-merge silently overwrote the PRIMARY
        # profile's own REAL observation for any rule bound in both
        # profiles (e.g. the moment a shared rule like incident_load is
        # promoted to launch), corrupting a launch finding's reported
        # coverage/observed_state/sample_count with unrelated "not
        # measured" noise and raising ValidationError in
        # _deficiency_from_health_rule_finding. Keying strictly off the
        # harvested findings' own rule_ids makes this structurally
        # impossible: only a rule_id investment_findings actually names can
        # ever be written here.
        observations_by_rule = dict(health_profile.observations_by_rule)
        for finding in investment_findings:
            observation = workload_profile.observations_by_rule.get(finding.rule_id)
            if observation is not None:
                observations_by_rule[finding.rule_id] = observation
    else:
        # No workload profile at all -- clear whatever health_profile
        # itself produced for this dimension (always empty in practice,
        # since it never carries investment_mix) so this category can
        # ONLY ever be populated through the branch above, never an
        # accidental fallback to a source that structurally can't evaluate
        # this rule. Three distinct reasons land here, each with its own
        # honest wording (Codex finding, HIGH, round 5): a PROJECT subject
        # (scope mismatch -- a rule genuinely exists, just not for this
        # subject kind), a TEAM subject whose investment-mix dependency
        # raised this evaluation (a real, contained outage -- see
        # _investment_balance_profile), or a TEAM subject whose attribution
        # lookup failed (wording irrelevant here, since _assemble's own
        # attribution_unavailable branch unconditionally overrides
        # whatever this function computes for every rule-driven category).
        launch_by_category[DeficiencyCategory.INVESTMENT_BALANCE] = []
        shadow_by_category[DeficiencyCategory.INVESTMENT_BALANCE] = []
        suppressed_by_category[DeficiencyCategory.INVESTMENT_BALANCE] = []
        investment_balance_unregistered_limitation = (
            _INVESTMENT_BALANCE_DEPENDENCY_UNAVAILABLE_LIMITATION
            if workload_dependency_failed
            else _INVESTMENT_BALANCE_NOT_APPLICABLE_TO_PROJECT_LIMITATION
        )

    statuses: list[DeficiencyCategoryStatus] = []
    all_findings: list[DeficiencyFinding] = []
    for category in _RULE_DRIVEN_CATEGORIES:
        unregistered_limitation = (
            investment_balance_unregistered_limitation
            if category is DeficiencyCategory.INVESTMENT_BALANCE
            else _RULE_DRIVEN_UNREGISTERED_LIMITATION
        )
        status, findings = _rule_driven_category_result(
            category,
            launch=launch_by_category[category],
            shadow=shadow_by_category[category],
            suppressed=suppressed_by_category[category],
            observations_by_rule=observations_by_rule,
            org_id=org_id,
            unregistered_limitation=unregistered_limitation,
        )
        statuses.append(status)
        all_findings.extend(findings)
    return tuple(statuses), tuple(all_findings)


# ---------------------------------------------------------------------------
# Category 7 & 8: not evaluated this version -- see module docstring.
# ---------------------------------------------------------------------------


def _unevaluated_status(
    category: DeficiencyCategory, limitation: str
) -> DeficiencyCategoryStatus:
    return DeficiencyCategoryStatus(
        schema_version="deficiency_category_status.v1",
        category=category,
        evaluated=False,
        finding_count=0,
        applicability_states_observed=(),
        limitation=limitation,
    )


def _dedupe_findings(
    findings: Sequence[DeficiencyFinding],
) -> tuple[DeficiencyFinding, ...]:
    """Collapse duplicate observations to one canonical finding.

    Keyed by ``finding_id`` -- which is only a valid dedupe key *because*
    ``_mint_deficiency_finding_id`` is deterministic over
    (category, rule_id, subject, discriminator, evaluated_at): two calls
    describing the same underlying condition always mint the identical id,
    and two calls describing different conditions (different
    discriminator, different rule_id, ...) never collide. See
    ``test_chaos_3305_operational_deficiency_service.py``'s dedupe mutation
    control, which breaks this precondition directly.
    """

    seen: dict[str, DeficiencyFinding] = {}
    for finding in findings:
        seen.setdefault(finding.finding_id, finding)
    return tuple(seen.values())


class OperationalDeficiencyService:
    """Evaluate the ``deficiency.operational.v1`` inventory for one subject."""

    def __init__(
        self,
        runtime: PlanExecutorRuntime,
        attribution: TeamAttributionSource,
        workload_source: TeamWorkloadDataSource,
    ) -> None:
        self._runtime = runtime
        self._attribution = attribution
        # NEVER composes TeamWorkloadService (Codex finding, HIGH, round 5,
        # 2026-08-02): that service's own evaluate_workload additionally
        # fetches status_snapshot/data_health with the RAW TEAM scope and
        # cognitive_load/active_contributor_count -- none of which this
        # service reads from it (only the investment_balance bucket is
        # ever harvested, see _rule_driven_results), so composing it
        # reintroduced the exact raw-TEAM-scope data_health widening this
        # service's OWN category-1 fix exists to prevent, PLUS made
        # every evaluate_team call pay for cognitive-load/contributor-count
        # queries with no caller. Category 8 gets its own narrow,
        # investment-only path instead -- see _investment_balance_profile.
        self._workload_source = workload_source

    async def _team_scoped_data_health(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        repository_ids: Sequence[str],
    ) -> DataHealthResult:
        """``data_health`` for a team's attributed repositories, batched
        <=20 at a time (``DevScope.repositories``' own bound) and merged
        exactly -- see the module-level batching/merge helpers above.

        Sequential, never ``asyncio.gather``: ``PlanExecutorRuntime`` is
        backed by a single request-scoped session that forbids concurrent
        use (the same discipline ``PortfolioStatusService``'s own Codex fix
        established, 03da63aeb) -- an arbitrarily large repository count
        costs proportionally more round trips, never a race.
        """

        if not repository_ids:
            return DataHealthResult(sources=(), complete_eligible=False)
        batches = [
            repository_ids[i : i + _MAX_REPOSITORIES_PER_SCOPE]
            for i in range(0, len(repository_ids), _MAX_REPOSITORIES_PER_SCOPE)
        ]
        batch_results: list[DataHealthResult] = []
        for batch in batches:
            batch_scope = DevScope(
                schema_version="dev_scope.v1",
                organization_id=org_id,
                direct_scope=DirectScope.REPOSITORY,
                repositories=list(batch),
                time_range=scope.time_range,
                comparison_range=scope.comparison_range,
            )
            batch_results.append(
                await self._runtime.data_health(
                    org_id=org_id,
                    permission_fingerprint=permission_fingerprint,
                    scope=batch_scope,
                )
            )
        merged_sources = _merge_data_health_sources(
            [result.sources for result in batch_results],
            batches,
            total_repositories=len(repository_ids),
        )
        # Recomputed from the NORMALIZED merged sources, never trusted from
        # each batch's own complete_eligible flag (Codex finding, HIGH,
        # 2026-08-02): a batch that omits a source_system entirely could
        # otherwise still report complete_eligible=True for itself (nothing
        # in that batch's own result contradicts it), silently overriding
        # the fail-closed placeholder _merge_data_health_sources just
        # synthesized for exactly this case.
        complete_eligible = all(
            not source.required or source.state is DataHealthState.COMPLETE
            for source in merged_sources
        )
        return DataHealthResult(
            sources=merged_sources, complete_eligible=complete_eligible
        )

    async def _investment_balance_profile(
        self,
        *,
        org_id: str,
        team_id: str,
        scope: DevScope,
        cohort_size: int | None,
        now: datetime,
    ) -> HealthProfileResult | None:
        """Category 8 (investment balance): fetch ONLY ``investment_mix``
        from ``self._workload_source`` and evaluate ONLY the rules
        ``synthesize_health_profile`` can reach with just that source --
        reuses CHAOS-3304's exact adapter/rule pieces (``synthesize_health_
        profile`` -> ``_observation_for_rule`` ->
        ``investment_allocation_shift_observation`` ->
        ``HEALTH_RULE_REGISTRY``'s production ``evaluate_registry`` seam),
        never a second, hand-rolled evaluation path, and never
        ``TeamWorkloadService``'s WHOLE service (Codex finding, HIGH, round
        5 -- see this class's own docstring and ``evaluate_team``'s call
        site for why).

        This deliberately calls ``synthesize_health_profile`` with a
        ``HealthEvaluationSources`` that leaves ``data_health``/
        ``status_snapshot``/``cognitive_load`` all at their ``None``
        defaults -- every OTHER TEAM-applicable rule (the three
        cognitive-load ones) therefore reports an honest, harmless
        unavailable observation in the returned profile's own
        ``shadow_findings`` bucket, which ``_rule_driven_results`` never
        reads from this profile (only ``DeficiencyCategory.
        INVESTMENT_BALANCE``'s bucket is ever harvested) -- there is no
        second source of truth to keep in sync, just an unused byproduct
        of reusing the shared synthesis function instead of hand-rolling a
        single-rule evaluator.

        Never touches ``self._runtime`` -- ``investment_mix`` takes
        ``team_id`` directly as a parameter (no ``DevScope`` to widen), so
        there is no raw-TEAM-scope org-wide-fallback risk here at all,
        unlike ``data_health``.

        Returns ``None`` -- never a profile built from partial/default
        data -- on an EXPECTED dependency failure (the workload source
        raising or timing out): the caller (``evaluate_team``) reports an
        honest, dedicated "dependency unavailable" status for category 8
        rather than letting the raise propagate and lose categories 1-7
        (Codex finding, HIGH, round 5: ``cognitive_load()`` raising
        ``RuntimeError`` propagated out of ``evaluate_team`` in the prior
        ``TeamWorkloadService``-composing design -- this method is the one
        and only place category 8's dependency is called from
        ``evaluate_team``, so containing the failure here contains it for
        the whole inventory). Distinct from a genuinely QUERIED-but-
        unmeasured source (the workload source returns normally with
        ``measured=False``) -- that case still calls
        ``synthesize_health_profile`` below and lands on the SAME honest
        shadow/provisional path every other never-fired rule does; only an
        actual exception short-circuits to ``None``.
        """

        current_range = scope.time_range
        try:
            investment_mix = await self._workload_source.investment_mix(
                org_id=org_id,
                team_id=team_id,
                start=current_range.start,
                end=current_range.end,
            )
            investment_mix_comparison: TeamInvestmentMixResult | None = None
            if scope.comparison_range is not None:
                comparison_range = scope.comparison_range
                investment_mix_comparison = await self._workload_source.investment_mix(
                    org_id=org_id,
                    team_id=team_id,
                    start=comparison_range.start,
                    end=comparison_range.end,
                )
        except Exception:
            # An expected, contained dependency failure -- never lets a
            # raise from the investment-mix source escape into
            # evaluate_team and lose categories 1-7 (Codex finding, HIGH,
            # round 5). `None` is a distinct sentinel from "queried,
            # unmeasured" -- see the docstring -- so the caller can report
            # the dedicated dependency-outage limitation, not the generic
            # shadow/provisional one.
            return None

        return synthesize_health_profile(
            applicability=RuleApplicability.TEAM,
            subject_id=team_id,
            cohort_size=cohort_size,
            sources=HealthEvaluationSources(
                investment_mix=investment_mix,
                investment_mix_comparison=investment_mix_comparison,
                change_failure_rate_not_applicable=True,
            ),
            org_id=org_id,
            observed_at=now,
        )

    async def evaluate_project(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        now: datetime,
    ) -> OperationalDeficiencyInventory:
        if scope.direct_scope is not DirectScope.PROJECT:
            raise ValueError(
                "OperationalDeficiencyService requires a project direct scope"
            )
        # Mirrors ProjectHealthService's own post-Codex-fix identity rule
        # (03da63aeb): project_id is never a caller-supplied label -- it is
        # always the committed DevScope's own (validator-guaranteed unique)
        # entity_ref, so the same scope can't be submitted under two labels
        # to mint two "different" subjects with identical underlying data.
        project_id = scope.entity_refs[0].entity_id
        if scope.comparison_range is None:
            raise ValueError(
                "OperationalDeficiencyService requires scope.comparison_range to "
                "be resolved for trend/comparison observations"
            )

        status_snapshot = await self._runtime.status_snapshot(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        data_health = await self._runtime.data_health(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        change_failure_rate_not_applicable = (
            scope.direct_scope not in CHANGE_FAILURE_RATE_SUPPORTED_SCOPES
        )
        change_failure_rate_metric = None
        if not change_failure_rate_not_applicable:
            change_failure_rate_metric = await self._runtime.query_metric(
                org_id=org_id,
                permission_fingerprint=permission_fingerprint,
                metric_id=MetricID.CHANGE_FAILURE_RATE,
                scope=scope,
            )
        health_profile = synthesize_health_profile(
            applicability=RuleApplicability.PROJECT,
            subject_id=project_id,
            cohort_size=None,
            sources=HealthEvaluationSources(
                data_health=data_health,
                status_snapshot=status_snapshot,
                change_failure_rate_metric=change_failure_rate_metric,
                change_failure_rate_not_applicable=change_failure_rate_not_applicable,
            ),
            org_id=org_id,
            observed_at=now,
        )
        return self._assemble(
            org_id=org_id,
            subject_kind=RuleApplicability.PROJECT,
            subject_id=project_id,
            data_health=data_health,
            status_snapshot=status_snapshot,
            health_profile=health_profile,
            now=now,
        )

    async def evaluate_team(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope: DevScope,
        team_id: str,
        now: datetime,
    ) -> OperationalDeficiencyInventory:
        if scope.direct_scope is not DirectScope.TEAM:
            raise ValueError(
                "OperationalDeficiencyService requires a team direct scope"
            )
        if scope.team_ids != [team_id]:
            raise ValueError("scope.team_ids must name exactly this team subject")

        # The ONLY source of cohort_size -- verified, queried at evaluation
        # time, never a caller-supplied assertion. Mirrors TeamHealthService's
        # own post-Codex-fix rule (03da63aeb): a naked caller-supplied int
        # could claim attribution a team never earned and fabricate a
        # healthy finding for an unattributed team.
        #
        # Resolved as of the committed scope's OWN window end, never `now`
        # (Codex finding, round 2, a37caf322) -- matches the exact instant
        # TeamHealthService now resolves at and the instant the runtime's
        # own internal team-repository lookup (status_snapshot's `as_of`
        # default) uses. team_repo_ownership can change between the scope's
        # own end and the moment this evaluation happens to run, so
        # resolving at `now` would judge historical findings against
        # attribution that did not hold at the time those findings
        # describe. `ClickHouseStatusChangeSource.team_repository_ids`
        # caches by the exact (org_id, team_id, as_of) key, so when this
        # service and the wrapped runtime share one source instance the
        # call below is a cache hit, not a second round trip.
        attribution = await self._attribution.team_repository_ids(
            org_id, team_id, as_of=scope.time_range.end
        )
        # The failure/empty distinction now rides TeamAttributionResult
        # itself (native_status_change.TeamAttributionResult, a37caf322) --
        # `measured=False` means the lookup failed and cohort_size is
        # UNKNOWABLE, never a caller-visible zero; `measured=True` with an
        # empty repository_ids is a genuine, resolved zero cohort. No
        # try/except needed here: a raised exception from a well-formed
        # TeamAttributionSource would be a genuine bug to surface, not a
        # case this service papers over.
        attribution_unavailable = not attribution.measured
        cohort_size = len(attribution.repository_ids) if attribution.measured else None

        # status_snapshot is safe to query with the raw TEAM scope directly:
        # ClickHouseStatusChangeSource re-derives team_repo_ownership (via
        # canonical-primary work-item attribution, CHAOS-3303 round 2)
        # internally, so it is already team-aware and never leaks
        # co-located-but-not-owned repository facts.
        status_snapshot = await self._runtime.status_snapshot(
            org_id=org_id, permission_fingerprint=permission_fingerprint, scope=scope
        )
        # data_health is NOT safe to query with the raw TEAM scope (Codex
        # finding, HIGH, 2026-08-02): a TEAM DevScope's own entity_refs
        # carry no repository_id, so DataHealthService/NativeDataHealthReader
        # resolves zero explicit repositories and falls back to querying
        # EVERY repository in the org -- cross-team disclosure plus false
        # positive/negative category-1 findings. Category 1 is therefore
        # NOT attribution-independent for a TEAM subject, unlike category 2
        # (corrected from an earlier, wrong claim in this module). Fix:
        # resolve explicit DirectScope.REPOSITORY scope(s) from the SAME
        # attribution snapshot used for cohort_size, batched <=20 at a time
        # (Codex finding, HIGH, 2026-08-02: DevScope.repositories caps at
        # 20 -- a naive single call for a >20-repository team raised
        # ValidationError and lost the ENTIRE inventory, not just category
        # 1) and merged exactly -- never the raw TEAM scope, never a silent
        # truncation to the first batch, and never queried at all when
        # attribution did not measure a real, non-empty repository set
        # (that case reports category 1 as never-queried, matching
        # ``_data_integration_result``'s own empty-sources branch, exactly
        # as an org-wide fallback must never stand in for "unmeasured").
        data_health = await self._team_scoped_data_health(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            scope=scope,
            repository_ids=attribution.repository_ids if attribution.measured else (),
        )

        # Unlike TeamHealthService (which has nothing else to report and
        # skips the runtime entirely on attribution failure), this service
        # still reports category 2 (planning & relationships) from
        # status_snapshot above regardless of attribution outcome -- it is
        # genuinely independent of team cohort attribution. Category 1 now
        # already reflects attribution honestly (real, team-scoped
        # data_health when attribution measured a non-empty cohort; never-
        # queried otherwise), so no further branching is needed for it
        # here. Only the rule-driven categories (3-6), which take
        # cohort_size as an input to CHAOS-3302's rule engine, need the
        # unmeasured treatment: on attribution failure they are synthesized
        # from EMPTY sources (mirroring TeamHealthService's own fix
        # exactly) rather than the real, already-fetched ones -- feeding
        # real facts through with cohort_size=None would still let
        # `evaluate_rule`'s cohort guard suppress them as the misleading
        # "insufficient_cohort" (a claim that attribution was checked and
        # found too small), since that guard runs after the observation is
        # already considered "measured". Empty sources short-circuit
        # through `_observation_for_rule`'s "sources is None" branch
        # instead, landing on the honest not-measured/unavailable path
        # before the cohort guard is ever reached.
        rule_engine_sources = (
            HealthEvaluationSources(change_failure_rate_not_applicable=True)
            if attribution_unavailable
            else HealthEvaluationSources(
                data_health=data_health,
                status_snapshot=status_snapshot,
                change_failure_rate_metric=None,
                change_failure_rate_not_applicable=True,
            )
        )
        health_profile = synthesize_health_profile(
            applicability=RuleApplicability.TEAM,
            subject_id=team_id,
            cohort_size=cohort_size,
            sources=rule_engine_sources,
            org_id=org_id,
            observed_at=now,
        )
        # Category 8 (investment balance): an INVESTMENT-ONLY path -- see
        # _investment_balance_profile -- never TeamWorkloadService's full
        # evaluate_workload (Codex finding, HIGH, round 5: that made its
        # own extra status_snapshot/data_health calls against the raw TEAM
        # scope, reintroducing the exact org-wide-widening bug category 1's
        # own fix above exists to prevent). Skipped entirely when
        # attribution is unmeasured -- already-resolved `attribution` is
        # reused directly rather than re-resolved a second time, and
        # `_assemble`'s own attribution_unavailable branch uniformly
        # overrides every rule-driven category's status (now including
        # this one) with the honest "cohort could not be verified"
        # limitation regardless, so there is nothing this call could add
        # in that case.
        workload_profile = (
            None
            if attribution_unavailable
            else await self._investment_balance_profile(
                org_id=org_id,
                team_id=team_id,
                scope=scope,
                cohort_size=cohort_size,
                now=now,
            )
        )
        # Distinguishes "no profile because attribution never resolved"
        # (wording irrelevant -- _assemble's own override wins regardless)
        # from "no profile because the investment-mix dependency itself
        # failed" (Codex finding, HIGH, round 5) -- the ONLY case that
        # needs the dedicated dependency-outage limitation text.
        workload_dependency_failed = (
            not attribution_unavailable and workload_profile is None
        )
        return self._assemble(
            org_id=org_id,
            subject_kind=RuleApplicability.TEAM,
            subject_id=team_id,
            data_health=data_health,
            status_snapshot=status_snapshot,
            health_profile=health_profile,
            now=now,
            attribution_unavailable=attribution_unavailable,
            workload_profile=workload_profile,
            workload_dependency_failed=workload_dependency_failed,
        )

    def _assemble(
        self,
        *,
        org_id: str,
        subject_kind: RuleApplicability,
        subject_id: str,
        data_health: DataHealthResult,
        status_snapshot: StatusSnapshotResult,
        health_profile: HealthProfileResult,
        now: datetime,
        attribution_unavailable: bool = False,
        workload_profile: HealthProfileResult | None = None,
        workload_dependency_failed: bool = False,
    ) -> OperationalDeficiencyInventory:
        data_status, data_findings = _data_integration_result(
            data_health,
            org_id=org_id,
            subject_kind=subject_kind,
            subject_id=subject_id,
            now=now,
        )
        planning_status, planning_findings = _planning_relationships_result(
            status_snapshot,
            org_id=org_id,
            subject_kind=subject_kind,
            subject_id=subject_id,
            now=now,
        )
        rule_statuses, rule_findings = _rule_driven_results(
            health_profile,
            org_id=org_id,
            workload_profile=workload_profile,
            workload_dependency_failed=workload_dependency_failed,
        )
        if attribution_unavailable:
            # Categories 1 (data & integration) and 2 (planning &
            # relationships) are computed from data_health/status_snapshot,
            # which are queried directly against `scope` -- independent of
            # team cohort attribution -- so they are unaffected. Only the
            # rule-driven categories consume cohort_size, and a failed
            # attribution lookup means whatever they computed (with
            # cohort_size=None, indistinguishable at the rule-engine layer
            # from a genuine empty cohort) must not be presented as if
            # attribution had actually been checked.
            rule_statuses = tuple(
                _unevaluated_status(
                    status.category, _TEAM_ATTRIBUTION_UNAVAILABLE_LIMITATION
                )
                for status in rule_statuses
            )
            rule_findings = ()

        all_statuses = (
            data_status,
            planning_status,
            # rule_statuses now includes INVESTMENT_BALANCE (category 8) --
            # see _RULE_DRIVEN_CATEGORIES and _rule_driven_results.
            *rule_statuses,
            _unevaluated_status(
                DeficiencyCategory.CAPACITY_COGNITIVE_LOAD, _CAPACITY_LIMITATION
            ),
        )
        all_findings = _dedupe_findings(
            (*data_findings, *planning_findings, *rule_findings)
        )
        ordered_findings = tuple(sorted(all_findings, key=finding_sort_key))

        return OperationalDeficiencyInventory(
            schema_version="deficiency_operational_inventory.v1",
            inventory_id=_mint_inventory_id(
                org_id=org_id,
                subject_kind=subject_kind.value,
                subject_id=subject_id,
                evaluated_at=now,
            ),
            subject_kind=subject_kind,
            subject_id=subject_id,
            findings=ordered_findings,
            category_statuses=all_statuses,
            evaluated_at=now,
        )
