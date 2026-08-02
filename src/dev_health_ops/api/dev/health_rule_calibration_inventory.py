"""CHAOS-3302 calibration inventory: every threshold currently used by

Operating Review, forecast/opportunity detectors, and compounding-risk
scoring on the ``dev-health-ops`` side, with an explicit calibration
record for each -- sample sizes, distribution notes, false-positive/
false-negative review, small-cohort behavior, and owner decision.

Scope note (owner ratification, 2026-08-01, recorded on CHAOS-3302): this
issue is ops-only backend work. The "web-side calibration inventory"
concern the pre-implementation plan flagged is resolved -- web page
components (Diagnose/Bottlenecks/Cognitive Load/Complexity/Investment/DORA
as rendered in ``dev-health-web``) are out of scope for this changeset.
This module inventories the **ops-side** provisional thresholds only:
Operating Review's own recommendation logic, and the forecast/opportunity/
compounding-risk detectors it and other surfaces draw on.

Every record below is ``calibration_state = provisional``, with no
exceptions -- including the three illustrative example rules also shipped
in ``health_rule_registry`` (``completion_stalled``/
``review_latency_sustained``/``data_trust_broken``). Those were originally
recorded ``product_approved`` with only a same-changeset illustrative
record as "evidence"; a Codex adversarial review (2026-08-01) correctly
identified that as unreviewed authority reaching real launch findings, so
they are provisional here like everything else. None of these thresholds
has been through the review this ticket requires (documented sample
sizes, percentile distributions, false-positive/negative review,
small-cohort behavior, owner sign-off). Promoting any entry below to
canonical status is future work requiring an actual owner review, tracked
as a CHAOS-3302 follow-up.

Sources inventoried
--------------------

* ``metrics/forecast.py`` -- ``WIP_CONGESTION_THRESHOLD`` (1.25),
  ``REVIEW_BOTTLENECK_THRESHOLD_HOURS`` (48.0),
  ``INCIDENT_LOAD_THRESHOLD`` (10.0). No named calibration source; these
  read as engineer-chosen round numbers.
* ``metrics/opportunities/flow_detector.py`` -- seven named thresholds
  (``_REVIEW_LATENCY_THRESHOLD_HOURS=24``, ``_CYCLE_TIME_THRESHOLD_HOURS=120``,
  ``_REWORK_RATIO_THRESHOLD=0.20``, ``_WIP_CONGESTION_THRESHOLD=0.40``,
  ``_LOW_THROUGHPUT_THRESHOLD=2.0``, ``_HIGH_CHURN_THRESHOLD=0.30``,
  ``_CHANGE_FAILURE_THRESHOLD=0.15``), each driving an "opportunity" card
  rather than a health/deficiency finding today.
* ``metrics/opportunities/ai_detector.py`` -- ``_FLAKY_RATE_THRESHOLD=0.05``
  plus two inline thresholds (``ai_rework >= 0.25``,
  ``ai_rework - human_rework >= 0.10``).
* ``metrics/compounding_risk.py`` -- ``DEFAULT_THRESHOLDS``, a whole
  dataclass of scoring thresholds, applied org-wide with no documented
  per-org calibration.
* ``metrics/benchmarking/anomalies.py`` -- ``z_threshold=2.0``,
  ``volatility_threshold=0.5``, generic statistical defaults, not derived
  from this product's own data.
* ``metrics/operating_review.py`` -- ``_recommendations_from_sections``
  (:func:`dev_health_ops.metrics.operating_review._recommendations_from_sections`)
  is a **non-compliant existing pattern**, flagged rather than inventoried
  as a threshold: it emits one recommendation sentence for *every* metric
  whose week-over-week delta status is ``"worsened"``, with no minimum
  sample, no sustained-window requirement, and no cohort-size floor. This
  is exactly the "one metric, one bad week" anti-pattern CHAOS-3302's
  qualification contract forbids for a team-needs-attention finding.
  Operating Review's recommendations are a distinct, lower-stakes product
  surface (a weekly digest, not a health/deficiency claim), so this
  changeset does not modify it -- but it must never be treated as, or
  quietly promoted into, a canonical ``HealthRuleDefinition``. Tracked as
  a CHAOS-3302 follow-up decision for the issue owner (bring it under the
  qualification contract, or document why the surface is exempt).
"""

from __future__ import annotations

from datetime import date

from .contracts_v2.health_rules import CalibrationRecord, CalibrationState

_DECIDED_AT = date(2026, 8, 1)
_OWNER = "chris@chrisgeorge.me (CHAOS-3302 calibration inventory pass)"

_NOT_YET_REVIEWED = (
    "Not yet run in shadow against a representative distribution "
    "as part of this changeset; sample size unknown. See CHAOS-3302 "
    "follow-up."
)
_NOT_YET_FP_FN_REVIEWED = (
    "No false-positive/false-negative review has been performed; the "
    "threshold is an engineer-chosen constant carried over from the "
    "surface it currently powers."
)
_NOT_YET_COHORT_REVIEWED = (
    "Small-cohort behavior has not been characterized; the source surface "
    "(an opportunity card or forecast alert) does not currently apply a "
    "minimum-cohort floor."
)

CALIBRATION_RECORDS: tuple[CalibrationRecord, ...] = (
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.wip_congestion.v1",
        rule_id="health_rule.wip_congestion.v1",
        rule_version="health_rule.wip_congestion.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Source: metrics/forecast.py WIP_CONGESTION_THRESHOLD=1.25. "
            "No documented distribution; carried forward as-is."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=_NOT_YET_COHORT_REVIEWED,
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.review_bottleneck_hours.v1",
        rule_id="health_rule.review_bottleneck_hours.v1",
        rule_version="health_rule.review_bottleneck_hours.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Source: metrics/forecast.py REVIEW_BOTTLENECK_THRESHOLD_HOURS=48.0. "
            "A related but distinct constant, flow_detector.py "
            "_REVIEW_LATENCY_THRESHOLD_HOURS=24.0, exists for a different "
            "'opportunity' surface -- the two disagree by 2x with no "
            "documented reconciliation, itself evidence review is overdue."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=_NOT_YET_COHORT_REVIEWED,
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.incident_load.v1",
        rule_id="health_rule.incident_load.v1",
        rule_version="health_rule.incident_load.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Source: metrics/forecast.py INCIDENT_LOAD_THRESHOLD=10.0 "
            "incidents per window. No documented distribution."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=_NOT_YET_COHORT_REVIEWED,
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.change_failure_rate.v1",
        rule_id="health_rule.change_failure_rate.v1",
        rule_version="health_rule.change_failure_rate.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Source: metrics/opportunities/flow_detector.py "
            "_CHANGE_FAILURE_THRESHOLD=0.15 (15%)."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=_NOT_YET_COHORT_REVIEWED,
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.flaky_test_rate.v1",
        rule_id="health_rule.flaky_test_rate.v1",
        rule_version="health_rule.flaky_test_rate.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Source: metrics/opportunities/ai_detector.py "
            "_FLAKY_RATE_THRESHOLD=0.05 (5% weighted flake rate)."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=_NOT_YET_COHORT_REVIEWED,
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.high_churn.v1",
        rule_id="health_rule.high_churn.v1",
        rule_version="health_rule.high_churn.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Source: metrics/opportunities/flow_detector.py "
            "_HIGH_CHURN_THRESHOLD=0.30 (30% rework_churn_ratio_30d)."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=_NOT_YET_COHORT_REVIEWED,
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    # -- Three illustrative example rules, also shipped in
    #    health_rule_registry, demoted to provisional/shadow-only
    #    (2026-08-01 Codex-confirmed correction -- see that module's
    #    _LAUNCH_RULES docstring). They were originally recorded
    #    product_approved with only this same-changeset record as
    #    "evidence" -- a same-changeset illustrative record can never
    #    itself be the review it claims happened, so that combination is
    #    exactly the "claims review that never happened" case
    #    CalibrationRecord/HealthRuleDefinition's own construction-time
    #    validators exist to reject. These records are provisional and
    #    evidence-free, like every other entry in this inventory. The
    #    mechanism they were meant to demonstrate (an approved rule
    #    reaching a launch finding and a team qualification) is now proven
    #    against a registry defined entirely inside
    #    ``test_chaos_3302_health_rule_e2e_controls.py``, never against
    #    this shipped inventory.
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.completion_stalled.v1",
        rule_id="health_rule.completion_stalled.v1",
        rule_version="health_rule.completion_stalled.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Illustrative example rule authored with this changeset "
            "(CHAOS-3302) to demonstrate the registry/evaluation "
            "mechanism. Never observed production traffic and has not "
            "been through review; shipping it as anything other than "
            "provisional would be an unreviewed rule claiming reviewed "
            "authority."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=(
            "minimum_cohort_size=5 suppresses to unknown below that floor "
            "(mechanism verified against a test-scoped registry -- see "
            "test_negative_cohort_below_minimum_suppresses_finding)."
        ),
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes="Illustrative example rule -- see module docstring. Never launch-authorized.",
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.review_latency_sustained.v1",
        rule_id="health_rule.review_latency_sustained.v1",
        rule_version="health_rule.review_latency_sustained.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Illustrative example rule authored with this changeset "
            "(CHAOS-3302); see health_rule_calibration.completion_stalled.v1."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=(
            "minimum_cohort_size=5 suppresses to unknown below that floor."
        ),
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes="Illustrative example rule -- see module docstring. Never launch-authorized.",
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.data_trust_broken.v1",
        rule_id="health_rule.data_trust_broken.v1",
        rule_version="health_rule.data_trust_broken.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "Illustrative example rule authored with this changeset "
            "(CHAOS-3302); a deterministic condition (source health "
            "broken), not a statistical threshold, so no distribution "
            "applies. Never observed production traffic and has not been "
            "through review."
        ),
        false_positive_review="Deterministic condition; no statistical FP rate applies.",
        false_negative_review="Deterministic condition; no statistical FN rate applies.",
        small_cohort_behavior="minimum_cohort_size=1 (portfolio/team/project all eligible).",
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes="Illustrative example rule -- see module docstring. Never launch-authorized.",
    ),
    # -- CHAOS-3304: team workload pressure / investment-balance rules,
    #    newly authored with that changeset. Provisional/evidence-free like
    #    every other entry here -- none of these has run in shadow against
    #    a representative distribution or been through owner sign-off.
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.after_hours_pressure_sustained.v1",
        rule_id="health_rule.after_hours_pressure_sustained.v1",
        rule_version="health_rule.after_hours_pressure_sustained.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "New CHAOS-3304 rule; threshold=0.25 after_hours_commit_ratio "
            "is an engineer-chosen round number, not derived from this "
            "product's own data."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=(
            "minimum_cohort_size=5 suppresses to unknown below that floor "
            "(mechanism verified against a test-scoped registry -- see "
            "test_chaos_3304_workload_health_rules.py)."
        ),
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.review_request_load_pressure.v1",
        rule_id="health_rule.review_request_load_pressure.v1",
        rule_version="health_rule.review_request_load_pressure.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "New CHAOS-3304 rule; threshold=5.0 review requests per active "
            "contributor over 14 days is an engineer-chosen round number."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=(
            "minimum_cohort_size=5 suppresses to unknown below that floor."
        ),
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.pr_interruption_load_pressure.v1",
        rule_id="health_rule.pr_interruption_load_pressure.v1",
        rule_version="health_rule.pr_interruption_load_pressure.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "New CHAOS-3304 rule; threshold=5.0 PR interruptions per "
            "active contributor over 14 days is an engineer-chosen round "
            "number."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=(
            "minimum_cohort_size=5 suppresses to unknown below that floor."
        ),
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
    CalibrationRecord(
        schema_version="health_rule_calibration.v1",
        calibration_id="health_rule_calibration.investment_allocation_shift.v1",
        rule_id="health_rule.investment_allocation_shift.v1",
        rule_version="health_rule.investment_allocation_shift.v1",
        calibration_state=CalibrationState.PROVISIONAL,
        sample_size=0,
        distribution_summary=(
            "New CHAOS-3304 rule; threshold=0.25 (25 percentage points of "
            "new-value share) between the current and prior 14-day window "
            "is an engineer-chosen round number. Deliberately magnitude-"
            "only (a shift toward OR away from new-value work triggers "
            "identically) -- see investment_allocation_shift_observation's "
            "docstring for the 'no feature-work value judgment' rationale."
        ),
        false_positive_review=_NOT_YET_FP_FN_REVIEWED,
        false_negative_review=_NOT_YET_FP_FN_REVIEWED,
        small_cohort_behavior=(
            "minimum_cohort_size=5 suppresses to unknown below that floor."
        ),
        owner=_OWNER,
        decided_at=_DECIDED_AT,
        evidence_ref=None,
        notes=_NOT_YET_REVIEWED,
    ),
)
