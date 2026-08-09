"""CHAOS-3617: flags, budgets, watermark and indexing-failure behaviour.

The issue's operational-trial controls, one class each. The recurring theme
is that every one of these is a *disclosure* requirement rather than a limit
requirement: a bound that silently applies is worse than no bound, because
the result looks complete.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
from dev_health_ops.api.dev.investigation_contract import TruncationReason
from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.budgets import (
    DEFAULT_BUDGETS,
    BudgetOutcome,
    TrialBudgets,
)
from dev_health_ops.context_fabric.graph_arm.flags import (
    TrialStoreConfig,
    graph_projection_enabled,
    graph_read_enabled,
    live_store_required,
    trial_store_config,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_WINDOW_END = datetime(2026, 8, 8, tzinfo=UTC)


class TestFlagsDefaultOff:
    def test_both_flags_are_off_when_unset(self) -> None:
        assert graph_projection_enabled() is False
        assert graph_read_enabled() is False

    @pytest.mark.parametrize("value", ["", "0", "true", "TRUE", "yes", "on", "2"])
    def test_only_an_exact_1_enables_a_flag(self, monkeypatch, value) -> None:
        """ "Anything truthy" would make a stray value enable a shadow arm."""

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", value)
        assert graph_projection_enabled() is False

    def test_the_flags_are_independent(self, monkeypatch) -> None:
        """Read must not imply projection, nor projection imply read.

        Nested flags would make bringing the arm up unsafe: the only safe
        order is to project first with no read path reachable, and switching
        reads off must not stop the projection and reset the watermark.
        """

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        assert graph_projection_enabled() is True
        assert graph_read_enabled() is False

        monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED")
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        assert graph_projection_enabled() is False
        assert graph_read_enabled() is True

    def test_the_live_gate_flag_is_also_off_by_default(self, monkeypatch) -> None:
        monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE", raising=False)
        assert live_store_required() is False


class TestTrialStoreConfiguration:
    def test_an_unconfigured_store_is_none_not_a_default_endpoint(
        self, monkeypatch
    ) -> None:
        """No default host/port.

        A default of localhost:6379 would let a misconfigured environment
        project one organization's graph into whatever Redis happened to be
        listening.
        """

        monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_STORE_URI", raising=False)
        assert trial_store_config() is None

    def test_a_configured_store_parses_host_and_port(self, monkeypatch) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_STORE_URI", "falkor://127.0.0.1:6389")
        config = trial_store_config()
        assert config is not None
        assert (config.host, config.port) == ("127.0.0.1", 6389)

    def test_a_uri_without_a_scheme_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must start with falkor:// or redis://"):
            TrialStoreConfig(uri="127.0.0.1:6389").host

    def test_a_uri_without_a_port_is_refused(self) -> None:
        """The trial store runs on its own isolated port, stated explicitly."""

        with pytest.raises(ValueError, match="names no port"):
            TrialStoreConfig(uri="falkor://127.0.0.1").port


class TestBudgets:
    def test_an_over_budget_outcome_always_carries_a_truncation_reason(self) -> None:
        outcome = DEFAULT_BUDGETS.check_paths(DEFAULT_BUDGETS.max_paths + 1)
        assert outcome.within_budget is False
        assert outcome.truncation_reason is TruncationReason.PATH_BUDGET

    def test_a_within_budget_outcome_carries_no_reason(self) -> None:
        assert DEFAULT_BUDGETS.check_paths(1).truncation_reason is None

    def test_the_limit_itself_is_within_budget(self) -> None:
        """Inclusive bounds. An off-by-one here trains reviewers to ignore
        the truncation disclosure, because it would appear on complete
        results."""

        assert DEFAULT_BUDGETS.check_paths(DEFAULT_BUDGETS.max_paths).within_budget

    def test_a_reason_without_a_flag_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="carries a truncation reason"):
            BudgetOutcome(
                within_budget=True, truncation_reason=TruncationReason.PATH_BUDGET
            )

    def test_a_flag_without_a_reason_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="carries no truncation reason"):
            BudgetOutcome(within_budget=False)

    def test_a_budget_above_the_frozen_contracts_own_bound_is_refused(self) -> None:
        """Otherwise the failure surfaces as a pydantic error at emission
        with nothing pointing at the misconfigured budget."""

        with pytest.raises(ValueError, match="exceeds the frozen packet contract"):
            TrialBudgets(max_paths=101)

    def test_every_kind_of_budget_is_expressible(self) -> None:
        """Rows, nodes, paths, bytes, time and output tokens, per the issue."""

        tiny = TrialBudgets(
            max_ingest_records=1,
            max_nodes_visited=1,
            max_paths=1,
            max_entities=1,
            max_evidence_entries=1,
            max_result_bytes=1,
            max_wall_seconds=0.001,
            max_output_tokens=1,
        )
        assert not tiny.check_ingest_records(2).within_budget
        assert not tiny.check_nodes(2).within_budget
        assert not tiny.check_paths(2).within_budget
        assert not tiny.check_entities(2).within_budget
        assert not tiny.check_evidence(2).within_budget
        assert not tiny.check_bytes(2).within_budget
        assert not tiny.check_elapsed(1.0).within_budget
        assert not tiny.check_output_tokens(2).within_budget

    def test_a_path_budget_that_bites_is_reported_with_its_reason(
        self, alpha_projection
    ) -> None:
        readout = asyncio.run(
            ProjectionGraphReader(alpha_projection).neighbourhood(
                org_id=alpha_projection.org_id,
                seed_canonical_ids=["proj_nightfall_migration"],
                authorized_entity_ids=fixtures.alpha_authorized_ids(),
                max_hops=3,
                budgets=TrialBudgets(max_paths=2),
            )
        )
        assert readout.paths_truncated is True
        assert readout.truncation_reason is TruncationReason.PATH_BUDGET
        assert len(readout.paths) <= 2

    def test_hop_depth_is_bounded_by_the_budget_not_only_the_argument(
        self, alpha_projection
    ) -> None:
        readout = asyncio.run(
            ProjectionGraphReader(alpha_projection).neighbourhood(
                org_id=alpha_projection.org_id,
                seed_canonical_ids=["proj_nightfall_migration"],
                authorized_entity_ids=fixtures.alpha_authorized_ids(),
                max_hops=99,
                budgets=TrialBudgets(max_path_hops=1),
            )
        )
        assert all(len(path.steps) <= 1 for path in readout.paths)

    def test_an_oversized_ingest_batch_truncates_and_says_so(self) -> None:
        from dev_health_ops.context_fabric.graph_arm import build_projection

        projection = build_projection(
            fixtures.alpha_batch(), budgets=TrialBudgets(max_ingest_records=1)
        )
        assert projection.truncated is True
        assert "exceeds the budget" in projection.truncation_detail


class TestWatermark:
    def test_a_never_projected_store_is_unavailable_not_fresh(self) -> None:
        """Checked before staleness, so an empty store can never read as
        "current with nothing in it"."""

        watermark = IndexWatermark(indexed_through=None)
        assert watermark.never_projected is True
        assert (
            watermark.freshness_for(_WINDOW_END) is SourceRequirementState.UNAVAILABLE
        )

    def test_a_watermark_behind_the_window_is_stale(self) -> None:
        watermark = IndexWatermark(
            indexed_through=_WINDOW_END - timedelta(days=2), records_indexed=1
        )
        assert (
            watermark.freshness_for(_WINDOW_END)
            is SourceRequirementState.AVAILABLE_STALE
        )

    def test_a_watermark_inside_the_tolerance_is_current(self) -> None:
        watermark = IndexWatermark(
            indexed_through=_WINDOW_END - timedelta(minutes=5), records_indexed=1
        )
        assert (
            watermark.freshness_for(_WINDOW_END)
            is SourceRequirementState.AVAILABLE_CURRENT
        )

    def test_a_partial_run_is_never_reported_current(self) -> None:
        """ "Indexed up to now but stopped early" still means records before
        the watermark may be missing."""

        watermark = IndexWatermark(
            indexed_through=_WINDOW_END, records_indexed=1, partial=True
        )
        assert (
            watermark.freshness_for(_WINDOW_END)
            is SourceRequirementState.AVAILABLE_STALE
        )

    def test_a_watermark_claiming_records_without_an_instant_is_incoherent(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="one of the two is wrong"):
            IndexWatermark(indexed_through=None, records_indexed=5)

    def test_a_naive_timestamp_is_refused(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            IndexWatermark(indexed_through=datetime(2026, 8, 8))

    def test_the_detail_line_is_content_safe(self) -> None:
        """Logs and traces use this rather than formatting the watermark
        themselves, so it must carry no entity label, title or body."""

        watermark = IndexWatermark(
            indexed_through=_WINDOW_END - timedelta(hours=2), records_indexed=17
        )
        detail = watermark.detail_for(_WINDOW_END)
        assert "17 records" in detail
        assert "Nightfall" not in detail
        assert watermark.indexed_through is not None
        assert watermark.indexed_through.isoformat() in detail

    def test_the_never_projected_detail_says_so_plainly(self) -> None:
        assert "never run" in IndexWatermark(indexed_through=None).detail_for(
            _WINDOW_END
        )
