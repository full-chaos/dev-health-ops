"""Clause-level tests for CHAOS-3304's workload/investment-balance adapters.

Mirrors ``test_chaos_3303_dimension_observation_adapters.py``'s discipline:
each test names the exact denominator/zero-vs-no-data boundary it proves.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.contracts_v2.health_rules import RuleApplicability
from dev_health_ops.api.dev.dimension_observation_adapters import (
    after_hours_pressure_observation,
    investment_allocation_shift_observation,
    pr_interruption_load_observation,
    review_request_load_observation,
)
from dev_health_ops.api.dev.native_team_workload import (
    TeamCognitiveLoadResult,
    TeamInvestmentMixResult,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_COMMON: dict[str, Any] = dict(
    subject_kind=RuleApplicability.TEAM,
    subject_id="team-1",
    cohort_size=5,
    window_index=0,
    observed_at=_NOW,
)


def _cognitive_load(
    *,
    after_hours_commit_ratio: float | None = 0.1,
    review_request_load: float | None = 10.0,
    pr_interruption_load: float | None = 8.0,
    context_spread_count: float | None = 3.0,
    sample_days: int = 14,
    measured: bool = True,
) -> TeamCognitiveLoadResult:
    return TeamCognitiveLoadResult(
        after_hours_commit_ratio=after_hours_commit_ratio,
        weekend_commit_ratio=0.05,
        pr_interruption_load=pr_interruption_load,
        review_request_load=review_request_load,
        context_spread_count=context_spread_count,
        sample_days=sample_days,
        measured=measured,
    )


def _investment_mix(
    *,
    new_value: float = 40.0,
    ktlo: float = 30.0,
    security: float = 10.0,
    infra: float = 10.0,
    unclassified: float = 10.0,
    measured: bool = True,
) -> TeamInvestmentMixResult:
    return TeamInvestmentMixResult(
        new_value_units=new_value,
        ktlo_units=ktlo,
        security_units=security,
        infra_units=infra,
        unclassified_units=unclassified,
        total_units=new_value + ktlo + security + infra + unclassified,
        measured=measured,
    )


# ---------------------------------------------------------------------------
# after_hours_pressure_observation
# ---------------------------------------------------------------------------


def test_after_hours_pressure_unmeasured_reports_unavailable() -> None:
    result = _cognitive_load(measured=False, after_hours_commit_ratio=None)
    obs = after_hours_pressure_observation(result, comparison_value=None, **_COMMON)
    assert obs.data_semantics == "not_measured"
    assert obs.current_value is None
    assert obs.observed_states == (SourceRequirementState.UNAVAILABLE,)


def test_after_hours_pressure_ratio_needs_no_external_denominator() -> None:
    """A self-normalized ratio is meaningful without an active-contributor
    count -- ``denominator_present`` is True whenever genuinely measured.
    """

    result = _cognitive_load(after_hours_commit_ratio=0.3)
    obs = after_hours_pressure_observation(result, comparison_value=None, **_COMMON)
    assert obs.current_value == 0.3
    assert obs.denominator_present is True
    assert obs.data_semantics == "measured_zero"


def test_after_hours_pressure_carries_own_history_comparison_value() -> None:
    result = _cognitive_load(after_hours_commit_ratio=0.3)
    obs = after_hours_pressure_observation(result, comparison_value=0.1, **_COMMON)
    assert obs.comparison_value == 0.1


def test_after_hours_pressure_discloses_chaos_3331_attribution_gap() -> None:
    """CHAOS-3331 (disclose-and-defer ruling, 2026-08-02):
    ``team_metrics_daily``'s writer (``compute_wellbeing.py``) resolves
    ``team_id`` via a legacy repo-pattern/identity-map resolver, not
    canonical primary attribution -- so ``attribution_present`` is
    ``False`` even for a genuinely measured result, which -- paired with
    this rule's own ``attribution_required=True``
    (``health_rule_registry.py``) -- honestly suppresses every finding to
    ``UNKNOWN``/``missing_attribution`` rather than a fabricated
    ``watch``/``at_risk``.
    """

    result = _cognitive_load(after_hours_commit_ratio=0.9)  # would trigger if not gated
    obs = after_hours_pressure_observation(result, comparison_value=None, **_COMMON)
    assert obs.attribution_present is False


# ---------------------------------------------------------------------------
# review_request_load_observation / pr_interruption_load_observation --
# the denominator-contract "not calculable" boundary.
# ---------------------------------------------------------------------------


def test_review_request_load_with_denominator_reports_per_contributor_rate() -> None:
    result = _cognitive_load(review_request_load=20.0)
    obs = review_request_load_observation(
        result, active_contributor_count=5, comparison_value=None, **_COMMON
    )
    assert obs.current_value == 4.0  # 20 / 5
    assert obs.denominator_present is True


def test_review_request_load_discloses_chaos_3331_attribution_gap_even_with_denominator() -> (
    None
):
    """CHAOS-3331: ``user_metrics_daily``'s writer uses the same legacy
    resolver as ``team_metrics_daily`` -- ``attribution_present`` is
    ``False`` regardless of whether the denominator is present, since the
    two guardrails are independent.
    """

    result = _cognitive_load(review_request_load=20.0)
    obs = review_request_load_observation(
        result, active_contributor_count=5, comparison_value=None, **_COMMON
    )
    assert obs.attribution_present is False


def test_review_request_load_without_denominator_reports_raw_value_not_calculable() -> (
    None
):
    """High raw pressure with no denominator: the raw value is still
    reported (never dropped), but ``denominator_present=False`` so the
    rule's own ``denominator_required=True`` gate reports the burden
    conclusion as not calculable -- see
    ``test_chaos_3304_team_workload_service.py`` for the finding-level
    proof of that gate.
    """

    result = _cognitive_load(review_request_load=99.0)
    obs = review_request_load_observation(
        result, active_contributor_count=None, comparison_value=None, **_COMMON
    )
    assert obs.current_value == 99.0
    assert obs.denominator_present is False
    assert obs.data_semantics == "measured_zero"  # genuinely queried, not no_data


def test_review_request_load_zero_active_contributors_is_not_calculable() -> None:
    result = _cognitive_load(review_request_load=10.0)
    obs = review_request_load_observation(
        result, active_contributor_count=0, comparison_value=None, **_COMMON
    )
    assert obs.denominator_present is False
    assert obs.current_value == 10.0


def test_pr_interruption_load_with_denominator_reports_per_contributor_rate() -> None:
    result = _cognitive_load(pr_interruption_load=16.0)
    obs = pr_interruption_load_observation(
        result, active_contributor_count=4, comparison_value=None, **_COMMON
    )
    assert obs.current_value == 4.0  # 16 / 4
    assert obs.denominator_present is True


def test_pr_interruption_load_unmeasured_reports_unavailable() -> None:
    result = _cognitive_load(measured=False, pr_interruption_load=None)
    obs = pr_interruption_load_observation(
        result, active_contributor_count=5, comparison_value=None, **_COMMON
    )
    assert obs.data_semantics == "not_measured"
    assert obs.current_value is None


def test_pr_interruption_load_discloses_chaos_3331_attribution_gap() -> None:
    result = _cognitive_load(pr_interruption_load=16.0)
    obs = pr_interruption_load_observation(
        result, active_contributor_count=4, comparison_value=None, **_COMMON
    )
    assert obs.attribution_present is False


# ---------------------------------------------------------------------------
# investment_allocation_shift_observation
# ---------------------------------------------------------------------------


def test_investment_shift_missing_comparison_window_is_no_data_not_zero() -> None:
    """A missing baseline is reported honestly as ``no_data`` -- never
    coerced to a "no shift" zero (that would be the exact "missing data as
    zero" anti-pattern this platform forbids).
    """

    current = _investment_mix()
    obs = investment_allocation_shift_observation(current, None, **_COMMON)
    assert obs.data_semantics == "no_data"
    assert obs.current_value is None
    assert obs.denominator_present is False


def test_investment_shift_stable_high_ktlo_mix_reports_neutral_measured_zero() -> None:
    """High KTLO/security/infra share, but a STABLE mix across windows:
    the shift is genuinely zero (measured, not a value judgment about the
    mix's composition) -- proves the "high KTLO/security/infra rendered
    neutrally" acceptance case.
    """

    current = _investment_mix(new_value=10.0, ktlo=60.0, security=20.0, infra=10.0)
    comparison = _investment_mix(new_value=10.0, ktlo=60.0, security=20.0, infra=10.0)
    obs = investment_allocation_shift_observation(current, comparison, **_COMMON)
    assert obs.current_value == 0.0
    assert obs.data_semantics == "measured_zero"


def test_investment_shift_is_also_subject_to_chaos_3331() -> None:
    """Codex-confirmed finding (round 2, 2026-08-02), correcting this
    module's own earlier claim of an asymmetry/exemption here:
    ``investment_metrics_daily``'s writer (``metrics/job_work_items.py``)
    resolves ``team_id`` via the canonical ``resolve_team_attribution`` +
    ``attribution_context`` path in the common case, but that path's own
    ``attribution_context`` load is wrapped in a ``try/except`` that FAILS
    OPEN -- a load failure continues with ``attribution_context=None``,
    and ``resolve_team_attribution`` then falls back through its legacy
    candidate chain and still writes a row. No field on
    ``TeamInvestmentMixResult`` records which path produced a given row,
    so this adapter cannot verify canonical attribution on the read side
    and must fail closed exactly like the three cognitive-load adapters
    above: ``attribution_present=False`` even for a genuinely measured,
    stable-mix result.
    """

    current = _investment_mix(new_value=10.0, ktlo=60.0, security=20.0, infra=10.0)
    comparison = _investment_mix(new_value=10.0, ktlo=60.0, security=20.0, infra=10.0)
    obs = investment_allocation_shift_observation(current, comparison, **_COMMON)
    assert obs.attribution_present is False


def test_investment_shift_large_swing_reports_magnitude_only() -> None:
    current = _investment_mix(
        new_value=80.0, ktlo=10.0, security=5.0, infra=5.0, unclassified=0.0
    )
    comparison = _investment_mix(
        new_value=20.0, ktlo=40.0, security=20.0, infra=20.0, unclassified=0.0
    )
    obs = investment_allocation_shift_observation(current, comparison, **_COMMON)
    # current share = 0.8, comparison share = 0.2 -> |0.8 - 0.2| = 0.6
    assert obs.current_value is not None
    assert obs.current_value == pytest.approx(0.6)
    assert obs.denominator_present is True


def test_investment_shift_no_classified_units_reports_unavailable() -> None:
    current = _investment_mix(
        new_value=0.0,
        ktlo=0.0,
        security=0.0,
        infra=0.0,
        unclassified=0.0,
        measured=False,
    )
    obs = investment_allocation_shift_observation(current, None, **_COMMON)
    assert obs.data_semantics == "not_measured"


def test_investment_mix_unclassified_never_dropped_from_coverage() -> None:
    mix = _investment_mix(
        new_value=10.0, ktlo=10.0, security=0.0, infra=0.0, unclassified=80.0
    )
    assert mix.total_units == 100.0
    assert mix.classification_coverage == 0.2
