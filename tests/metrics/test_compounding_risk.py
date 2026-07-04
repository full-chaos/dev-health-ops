"""Unit tests for the Compounding Risk composite (CHAOS-1641)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dev_health_ops.metrics.compounding_risk import (
    DEFAULT_THRESHOLDS,
    DEFAULT_WEIGHTS,
    REFERENCE_VALUES,
    CompoundingInputs,
    CompoundingThresholds,
    CompoundingWeights,
    compute_compounding_risk,
    severity_for,
)

NOW = datetime(2026, 5, 21, 12, 0, 0, tzinfo=timezone.utc)
DAY = date(2026, 5, 20)


def _inputs(
    *,
    rework_churn: float | None = 0.0,
    complexity_delta: float | None = 0.0,
    review_latency_p90h: float | None = 0.0,
    single_owner_ratio: float | None = 0.0,
    ownership_gini: float | None = 0.0,
    bus_factor: float | None = None,
) -> CompoundingInputs:
    return CompoundingInputs(
        rework_churn=rework_churn,
        complexity_delta=complexity_delta,
        review_latency_p90h=review_latency_p90h,
        single_owner_ratio=single_owner_ratio,
        ownership_gini=ownership_gini,
        bus_factor=bus_factor,
    )


def _compute(inputs: CompoundingInputs):
    return compute_compounding_risk(
        day=DAY,
        scope="repo",
        scope_id="repo-1",
        org_id="org-1",
        inputs=inputs,
        computed_at=NOW,
    )


# ---------------------------------------------------------------------------
# Determinism / structural invariants
# ---------------------------------------------------------------------------


def test_weights_sum_to_one() -> None:
    w = DEFAULT_WEIGHTS
    assert w.churn + w.complexity + w.ownership + w.review == pytest.approx(1.0)


def test_weights_must_sum_to_one() -> None:
    with pytest.raises(ValueError):
        CompoundingWeights(churn=0.1, complexity=0.1, ownership=0.1, review=0.1)


def test_thresholds_must_be_ordered() -> None:
    with pytest.raises(ValueError):
        CompoundingThresholds(elevated=0.8, high=0.5)


def test_scope_must_be_repo_or_team() -> None:
    with pytest.raises(ValueError):
        compute_compounding_risk(
            day=DAY,
            scope="developer",  # forbidden
            scope_id="x",
            org_id="org-1",
            inputs=_inputs(),
            computed_at=NOW,
        )


# ---------------------------------------------------------------------------
# Score / severity boundary cases
# ---------------------------------------------------------------------------


def test_score_is_zero_when_all_inputs_zero() -> None:
    row = _compute(_inputs())
    assert row.compounding_risk == 0.0
    assert row.severity == "low"


def test_score_is_one_when_all_inputs_saturate() -> None:
    row = _compute(
        _inputs(
            rework_churn=REFERENCE_VALUES["churn_ref"] * 2,
            complexity_delta=REFERENCE_VALUES["complexity_ref"] * 2,
            review_latency_p90h=REFERENCE_VALUES["review_ref"] * 2,
            single_owner_ratio=1.0,
            ownership_gini=1.0,
        )
    )
    assert row.compounding_risk == 1.0
    assert row.severity == "high"


def test_score_is_none_when_any_required_input_is_none() -> None:
    row = _compute(_inputs(rework_churn=None))
    assert row.compounding_risk is None
    assert row.severity == "unknown"
    # raw input still surfaced for inspectability
    assert row.complexity_delta == 0.0


def test_severity_thresholds_inclusive_at_lower_edge() -> None:
    # < 0.40 → low
    assert severity_for(0.3999, DEFAULT_THRESHOLDS) == "low"
    # 0.40 → elevated
    assert severity_for(0.40, DEFAULT_THRESHOLDS) == "elevated"
    # 0.64 → elevated
    assert severity_for(0.64, DEFAULT_THRESHOLDS) == "elevated"
    # 0.65 → high
    assert severity_for(0.65, DEFAULT_THRESHOLDS) == "high"
    # None → unknown
    assert severity_for(None, DEFAULT_THRESHOLDS) == "unknown"


# ---------------------------------------------------------------------------
# Normalization semantics
# ---------------------------------------------------------------------------


def test_negative_complexity_delta_is_clamped_to_zero() -> None:
    row = _compute(_inputs(complexity_delta=-0.5))
    assert row.complexity_norm == 0.0
    # only complexity component should be missing weight; others are zero too
    assert row.compounding_risk == 0.0


def test_ownership_norm_takes_max_of_single_owner_and_gini() -> None:
    row = _compute(_inputs(single_owner_ratio=0.4, ownership_gini=0.9))
    assert row.ownership_norm == 0.9


def test_ownership_norm_works_with_only_one_input_present() -> None:
    row = _compute(_inputs(single_owner_ratio=0.6, ownership_gini=None))
    assert row.compounding_risk is not None
    assert row.ownership_norm == 0.6


def test_ownership_blocks_score_when_both_signals_missing() -> None:
    row = _compute(_inputs(single_owner_ratio=None, ownership_gini=None))
    assert row.compounding_risk is None
    assert row.severity == "unknown"


def test_inputs_above_reference_are_clamped_to_one() -> None:
    row = _compute(
        _inputs(
            rework_churn=99.0,
            complexity_delta=99.0,
            review_latency_p90h=99.0,
            single_owner_ratio=2.0,  # nonsense input
            ownership_gini=2.0,
        )
    )
    assert row.churn_norm == 1.0
    assert row.complexity_norm == 1.0
    assert row.review_norm == 1.0
    assert row.ownership_norm == 1.0
    assert row.compounding_risk == 1.0


# ---------------------------------------------------------------------------
# Audit trail
# ---------------------------------------------------------------------------


def test_weights_and_thresholds_are_persisted_in_the_row() -> None:
    row = _compute(_inputs())
    assert row.w_churn == DEFAULT_WEIGHTS.churn
    assert row.w_complexity == DEFAULT_WEIGHTS.complexity
    assert row.w_ownership == DEFAULT_WEIGHTS.ownership
    assert row.w_review == DEFAULT_WEIGHTS.review
    assert row.threshold_elevated == DEFAULT_THRESHOLDS.elevated
    assert row.threshold_high == DEFAULT_THRESHOLDS.high


def test_raw_inputs_are_persisted_for_inspectability() -> None:
    inputs = _inputs(
        rework_churn=0.12,
        complexity_delta=0.05,
        review_latency_p90h=18.0,
        single_owner_ratio=0.7,
        ownership_gini=0.6,
        bus_factor=3.0,
    )
    row = _compute(inputs)
    assert row.rework_churn == 0.12
    assert row.complexity_delta == 0.05
    assert row.review_latency_p90h == 18.0
    assert row.single_owner_ratio == 0.7
    assert row.ownership_gini == 0.6
    assert row.bus_factor == 3.0


def test_org_id_and_scope_are_persisted() -> None:
    row = compute_compounding_risk(
        day=DAY,
        scope="team",
        scope_id="team-7",
        org_id="acme",
        inputs=_inputs(),
        computed_at=NOW,
    )
    assert row.org_id == "acme"
    assert row.scope == "team"
    assert row.scope_id == "team-7"


def test_computed_at_is_passed_through_unchanged() -> None:
    row = _compute(_inputs())
    assert row.computed_at == NOW


# ---------------------------------------------------------------------------
# Custom weights / thresholds
# ---------------------------------------------------------------------------


def test_custom_weights_change_score_predictably() -> None:
    # Put 100% weight on churn; only churn input is nonzero ⇒ score = churn_norm.
    weights = CompoundingWeights(churn=1.0, complexity=0.0, ownership=0.0, review=0.0)
    inputs = _inputs(rework_churn=REFERENCE_VALUES["churn_ref"])  # saturates → 1.0
    row = compute_compounding_risk(
        day=DAY,
        scope="repo",
        scope_id="repo-1",
        org_id="org-1",
        inputs=inputs,
        computed_at=NOW,
        weights=weights,
    )
    assert row.compounding_risk == 1.0
    # Audit trail reflects override
    assert row.w_churn == 1.0
    assert row.w_review == 0.0


def test_custom_thresholds_persist_with_row() -> None:
    thresholds = CompoundingThresholds(elevated=0.50, high=0.80)
    row = compute_compounding_risk(
        day=DAY,
        scope="repo",
        scope_id="repo-1",
        org_id="org-1",
        inputs=_inputs(),
        computed_at=NOW,
        thresholds=thresholds,
    )
    assert row.threshold_elevated == 0.50
    assert row.threshold_high == 0.80


# ---------------------------------------------------------------------------
# Orchestrator (CHAOS-1641 job_daily wiring)
# ---------------------------------------------------------------------------

import uuid  # noqa: E402
from dataclasses import dataclass  # noqa: E402

from dev_health_ops.metrics.compounding_risk import (  # noqa: E402
    build_compounding_risk_rows_for_day,
    load_repo_complexity_delta_30d,
)
from dev_health_ops.metrics.schemas import (  # noqa: E402
    CompoundingRiskDailyRecord,
)


@dataclass
class _FakeRepoMetrics:
    repo_id: uuid.UUID
    rework_churn_ratio_30d: float = 0.0
    single_owner_file_ratio_30d: float = 0.0
    code_ownership_gini: float = 0.0
    bus_factor: int = 0
    pr_first_review_p90_hours: float | None = 0.0


class _FakeSink:
    """Stand-in for the ClickHouse sink. Returns canned ``query_dicts`` results."""

    def __init__(self, complexity_by_repo: dict[str, dict[str, float | None] | None]):
        self._data = complexity_by_repo
        self.calls: list[dict] = []

    def query_dicts(self, query: str, parameters: dict) -> list[dict]:
        self.calls.append(parameters)
        repo = parameters["repo_id"]
        result = self._data.get(repo)
        return [result] if result is not None else []


def test_load_repo_complexity_delta_returns_relative_change() -> None:
    sink = _FakeSink({"r1": {"first_half": 100.0, "second_half": 130.0}})
    delta = load_repo_complexity_delta_30d(sink, repo_id="r1", day=DAY, org_id="acme")
    assert delta is not None
    assert delta == pytest.approx(0.30)


def test_load_repo_complexity_delta_returns_none_when_either_half_missing() -> None:
    sink = _FakeSink({"r1": {"first_half": 100.0, "second_half": None}})
    assert (
        load_repo_complexity_delta_30d(sink, repo_id="r1", day=DAY, org_id="acme")
        is None
    )


def test_load_repo_complexity_delta_returns_none_when_no_rows() -> None:
    sink = _FakeSink({})
    assert (
        load_repo_complexity_delta_30d(sink, repo_id="missing", day=DAY, org_id="acme")
        is None
    )


def test_load_repo_complexity_delta_rejects_too_small_window() -> None:
    sink = _FakeSink({})
    with pytest.raises(ValueError):
        load_repo_complexity_delta_30d(
            sink, repo_id="r1", day=DAY, org_id="acme", window_days=1
        )


def test_orchestrator_produces_one_row_per_repo() -> None:
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    sink = _FakeSink(
        {
            str(repo_a): {"first_half": 100.0, "second_half": 130.0},
            str(repo_b): {"first_half": 100.0, "second_half": 100.0},
        }
    )
    repo_rows = [
        _FakeRepoMetrics(
            repo_id=repo_a,
            rework_churn_ratio_30d=0.15,
            single_owner_file_ratio_30d=0.5,
            code_ownership_gini=0.5,
            pr_first_review_p90_hours=24.0,
        ),
        _FakeRepoMetrics(
            repo_id=repo_b,
            rework_churn_ratio_30d=0.0,
            single_owner_file_ratio_30d=0.0,
            code_ownership_gini=0.0,
            pr_first_review_p90_hours=0.0,
        ),
    ]
    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=repo_rows,
        computed_at=NOW,
    )
    assert len(out) == 2
    assert out[0].scope_id == str(repo_a)
    assert out[1].scope_id == str(repo_b)
    assert out[0].compounding_risk is not None
    assert out[0].compounding_risk > 0
    assert out[1].compounding_risk == 0.0
    for row in out:
        assert row.org_id == "acme"
        assert row.scope == "repo"
        assert (
            row.w_churn + row.w_complexity + row.w_ownership + row.w_review
            == pytest.approx(1.0)
        )


def test_orchestrator_skips_rows_without_repo_id() -> None:
    sink = _FakeSink({})

    @dataclass
    class _NoIdRow:
        rework_churn_ratio_30d: float = 0.0

    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=[_NoIdRow()],
        computed_at=NOW,
    )
    assert out == []


def test_orchestrator_emits_unknown_severity_when_inputs_missing() -> None:
    repo = uuid.uuid4()
    sink = _FakeSink({})
    repo_rows = [
        _FakeRepoMetrics(
            repo_id=repo,
            rework_churn_ratio_30d=0.1,
            single_owner_file_ratio_30d=0.5,
            code_ownership_gini=0.5,
            pr_first_review_p90_hours=24.0,
        ),
    ]
    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=repo_rows,
        computed_at=NOW,
    )
    assert len(out) == 1
    assert out[0].compounding_risk is None
    assert out[0].severity == "unknown"


def test_orchestrator_keeps_missing_review_latency_when_window_has_no_value() -> None:
    repo = uuid.uuid4()
    sink = _FakeSink({str(repo): {"first_half": 100.0, "second_half": 110.0}})
    repo_rows = [
        _FakeRepoMetrics(
            repo_id=repo,
            rework_churn_ratio_30d=0.10,
            single_owner_file_ratio_30d=0.50,
            code_ownership_gini=0.40,
            pr_first_review_p90_hours=None,
        ),
    ]

    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=repo_rows,
        computed_at=NOW,
    )

    assert len(out) == 1
    assert out[0].review_latency_p90h is None
    assert out[0].compounding_risk is None
    assert out[0].severity == "unknown"


# ---------------------------------------------------------------------------
# Team-scope persistence (CHAOS-1641 follow-up)
# ---------------------------------------------------------------------------


def test_orchestrator_emits_team_rows_when_repo_to_team_map_provided() -> None:
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    sink = _FakeSink(
        {
            str(repo_a): {"first_half": 100.0, "second_half": 130.0},  # +30%
            str(repo_b): {"first_half": 100.0, "second_half": 100.0},  #   0%
        }
    )
    repo_rows = [
        _FakeRepoMetrics(
            repo_id=repo_a,
            rework_churn_ratio_30d=0.20,
            single_owner_file_ratio_30d=0.8,
            code_ownership_gini=0.7,
            pr_first_review_p90_hours=48.0,
        ),
        _FakeRepoMetrics(
            repo_id=repo_b,
            rework_churn_ratio_30d=0.0,
            single_owner_file_ratio_30d=0.0,
            code_ownership_gini=0.0,
            pr_first_review_p90_hours=0.0,
        ),
    ]
    repo_to_team = {str(repo_a): "team-X", str(repo_b): "team-X"}

    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=repo_rows,
        computed_at=NOW,
        repo_to_team=repo_to_team,
    )

    # 2 repo rows + 1 team row
    assert len(out) == 3
    by_scope: dict[str, list[CompoundingRiskDailyRecord]] = {r.scope: [] for r in out}
    for r in out:
        by_scope[r.scope].append(r)
    assert len(by_scope["repo"]) == 2
    assert len(by_scope["team"]) == 1
    team_row = by_scope["team"][0]
    assert team_row.scope_id == "team-X"
    # Team score is computed from the *mean of raw inputs* under the same formula.
    # repo_a has saturated complexity_delta (0.30 / 0.20 ref) ⇒ 1.0 norm; repo_b 0.
    # Mean delta = 0.15 / 0.20 = 0.75 (complexity_norm).
    # Mean rework_churn = 0.10 / 0.30 = 0.333 (churn_norm).
    # Mean ownership = max(mean(0.8,0)=0.4, mean(0.7,0)=0.35) = 0.4.
    # Mean review = 24.0 / 48.0 = 0.5.
    # Score = 0.30*0.333 + 0.30*0.75 + 0.20*0.4 + 0.20*0.5 = 0.4849...
    assert team_row.compounding_risk is not None
    assert 0.40 <= team_row.compounding_risk <= 0.55
    assert team_row.severity in ("elevated", "low")  # near threshold


def test_orchestrator_team_row_omitted_when_no_repos_in_team() -> None:
    repo_a = uuid.uuid4()
    sink = _FakeSink(
        {
            str(repo_a): {"first_half": 100.0, "second_half": 110.0},
        }
    )
    repo_rows = [
        _FakeRepoMetrics(
            repo_id=repo_a,
            rework_churn_ratio_30d=0.05,
            single_owner_file_ratio_30d=0.2,
            code_ownership_gini=0.1,
            pr_first_review_p90_hours=12.0,
        ),
    ]
    # No mapping for repo_a, but mapping exists for an unrelated repo.
    repo_to_team = {"some-other-repo": "team-Y"}
    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=repo_rows,
        computed_at=NOW,
        repo_to_team=repo_to_team,
    )
    assert len(out) == 1
    assert out[0].scope == "repo"


def test_orchestrator_team_rows_inherit_weights_and_thresholds() -> None:
    repo_a = uuid.uuid4()
    sink = _FakeSink(
        {
            str(repo_a): {"first_half": 100.0, "second_half": 110.0},
        }
    )
    repo_rows = [
        _FakeRepoMetrics(
            repo_id=repo_a,
            rework_churn_ratio_30d=0.05,
            single_owner_file_ratio_30d=0.2,
            code_ownership_gini=0.1,
            pr_first_review_p90_hours=12.0,
        ),
    ]
    out = build_compounding_risk_rows_for_day(
        sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=repo_rows,
        computed_at=NOW,
        repo_to_team={str(repo_a): "team-Y"},
    )
    team_row = next(r for r in out if r.scope == "team")
    assert team_row.w_churn == DEFAULT_WEIGHTS.churn
    assert team_row.threshold_elevated == DEFAULT_THRESHOLDS.elevated
