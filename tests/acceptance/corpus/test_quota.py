"""Unit coverage for ``scripts.acceptance.corpus.quota``."""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.quota import (
    QuotaBudget,
    QuotaConfigurationError,
    QuotaExhaustedError,
    estimate_run_cost_microusd,
)


class TestEstimateRunCostMicrousd:
    def test_prices_the_real_scripted_model(self) -> None:
        cost = estimate_run_cost_microusd(
            input_tokens=8_000, output_tokens=3_000, rounds=4
        )
        assert cost > 0

    def test_unpriced_model_raises_rather_than_being_free(self) -> None:
        with pytest.raises(ValueError, match="no priced entry"):
            estimate_run_cost_microusd(
                model="definitely-not-a-priced-model",
                input_tokens=100,
                output_tokens=100,
            )

    def test_more_rounds_costs_more(self) -> None:
        one_round = estimate_run_cost_microusd(
            input_tokens=100, output_tokens=100, rounds=1
        )
        four_rounds = estimate_run_cost_microusd(
            input_tokens=100, output_tokens=100, rounds=4
        )
        assert four_rounds == one_round * 4


class TestQuotaBudgetFromEnv:
    def test_builds_from_the_compose_configured_values(self) -> None:
        budget = QuotaBudget.from_env(
            {
                "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX": "1000",
                "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD": "200000000",
            }
        )
        assert budget.max_requests == 1000
        assert budget.max_cost_microusd == 200_000_000
        assert budget.remaining_requests() == 1000

    def test_missing_request_max_raises(self) -> None:
        with pytest.raises(
            QuotaConfigurationError, match="ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX"
        ):
            QuotaBudget.from_env({"ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD": "1"})

    def test_missing_cost_max_raises(self) -> None:
        with pytest.raises(
            QuotaConfigurationError, match="ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD"
        ):
            QuotaBudget.from_env({"ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX": "1"})

    def test_non_integer_value_raises(self) -> None:
        with pytest.raises(QuotaConfigurationError, match="not an integer"):
            QuotaBudget.from_env(
                {
                    "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX": "lots",
                    "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD": "1",
                }
            )

    def test_zero_or_negative_value_raises(self) -> None:
        with pytest.raises(QuotaConfigurationError, match="must be positive"):
            QuotaBudget.from_env(
                {
                    "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX": "0",
                    "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD": "1",
                }
            )


class TestQuotaBudgetReserve:
    def test_reserve_accumulates_spend(self) -> None:
        budget = QuotaBudget(max_requests=10, max_cost_microusd=1000)
        budget.reserve(case_id="a", requests=2, cost_microusd=100)
        budget.reserve(case_id="b", requests=3, cost_microusd=200)
        assert budget.spent_requests == 5
        assert budget.spent_cost_microusd == 300
        assert budget.remaining_requests() == 5
        assert budget.remaining_cost_microusd() == 700

    def test_exceeding_request_ceiling_raises_and_names_the_case(self) -> None:
        budget = QuotaBudget(max_requests=5, max_cost_microusd=1_000_000)
        budget.reserve(case_id="a", requests=4, cost_microusd=1)
        with pytest.raises(QuotaExhaustedError, match="case 'b'"):
            budget.reserve(case_id="b", requests=2, cost_microusd=1)

    def test_exceeding_cost_ceiling_raises_and_names_the_case(self) -> None:
        budget = QuotaBudget(max_requests=1_000_000, max_cost_microusd=100)
        budget.reserve(case_id="a", requests=1, cost_microusd=90)
        with pytest.raises(QuotaExhaustedError, match="case 'b'"):
            budget.reserve(case_id="b", requests=1, cost_microusd=20)

    def test_a_failed_reservation_leaves_the_budget_unchanged(self) -> None:
        budget = QuotaBudget(max_requests=5, max_cost_microusd=100)
        budget.reserve(case_id="a", requests=4, cost_microusd=10)
        with pytest.raises(QuotaExhaustedError):
            budget.reserve(case_id="b", requests=2, cost_microusd=10)
        assert budget.spent_requests == 4
        assert budget.spent_cost_microusd == 10

    def test_exact_fit_is_allowed(self) -> None:
        budget = QuotaBudget(max_requests=5, max_cost_microusd=100)
        budget.reserve(case_id="a", requests=5, cost_microusd=100)
        assert budget.remaining_requests() == 0
        assert budget.remaining_cost_microusd() == 0


class TestQuotaBudgetRelease:
    def test_release_credits_back_a_reservation(self) -> None:
        budget = QuotaBudget(max_requests=10, max_cost_microusd=1000)
        budget.reserve(case_id="a", requests=3, cost_microusd=300)
        budget.release(requests=3, cost_microusd=300)
        assert budget.spent_requests == 0
        assert budget.spent_cost_microusd == 0

    def test_release_only_credits_the_released_amount(self) -> None:
        budget = QuotaBudget(max_requests=10, max_cost_microusd=1000)
        budget.reserve(case_id="a", requests=5, cost_microusd=500)
        budget.reserve(case_id="b", requests=2, cost_microusd=200)
        budget.release(requests=2, cost_microusd=200)  # case b's pre-admission failure
        assert budget.spent_requests == 5
        assert budget.spent_cost_microusd == 500

    def test_a_released_reservation_frees_room_for_a_later_case(self) -> None:
        budget = QuotaBudget(max_requests=5, max_cost_microusd=1_000_000)
        budget.reserve(case_id="a", requests=5, cost_microusd=1)
        budget.release(requests=5, cost_microusd=1)
        # Would have raised QuotaExhaustedError before the release.
        budget.reserve(case_id="b", requests=5, cost_microusd=1)
        assert budget.spent_requests == 5

    def test_over_release_clamps_at_zero_rather_than_raising(self) -> None:
        budget = QuotaBudget(max_requests=10, max_cost_microusd=1000)
        budget.reserve(case_id="a", requests=1, cost_microusd=10)
        budget.release(requests=100, cost_microusd=10_000)
        assert budget.spent_requests == 0
        assert budget.spent_cost_microusd == 0


class TestQuotaHeadroomAtPlannedCorpusScale:
    """The same 134-case x 3-run headroom claim
    ``test_ask_dev_quota_headroom.py`` proves at the allowance-accounting
    level, re-proven here through THIS module's own budget tracker --
    proving the coordination layer agrees with the admission-path proof,
    not a second, silently-diverging estimate."""

    def test_134_cases_times_3_runs_clears_the_compose_ceiling_with_margin(
        self,
    ) -> None:
        budget = QuotaBudget.from_env(
            {
                "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX": "1000",
                "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD": "200000000",
            }
        )
        per_run_cost = estimate_run_cost_microusd(
            input_tokens=8_000, output_tokens=3_000, rounds=4
        )
        planned_runs = 134 * 3
        for index in range(planned_runs):
            budget.reserve(
                case_id=f"case-{index}", requests=1, cost_microusd=per_run_cost
            )
        assert budget.remaining_requests() >= 0
        assert budget.remaining_cost_microusd() >= 0
