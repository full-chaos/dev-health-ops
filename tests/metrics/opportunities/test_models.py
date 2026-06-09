"""Unit tests for dev_health_ops.metrics.opportunities.models.

Tests cover:
- ImproveOpportunityKind enum values are stable strings
- ImproveOpportunity is frozen (immutable)
- FlowScopeInput fields default to None
"""

from __future__ import annotations

import uuid

import pytest

from dev_health_ops.metrics.opportunities.models import (
    FlowScopeInput,
    ImproveOpportunity,
    ImproveOpportunityKind,
)


class TestImproveOpportunityKind:
    def test_all_kinds_have_string_values(self) -> None:
        for kind in ImproveOpportunityKind:
            assert isinstance(kind.value, str)
            assert kind.value  # non-empty

    def test_expected_kinds_present(self) -> None:
        values = {k.value for k in ImproveOpportunityKind}
        for expected in (
            "high_review_latency",
            "slow_cycle_time",
            "high_rework",
            "high_wip",
            "low_throughput",
            "high_churn",
            "high_change_failure",
        ):
            assert expected in values

    def test_seven_kinds(self) -> None:
        assert len(ImproveOpportunityKind) == 7


class TestImproveOpportunity:
    def _make(self, **overrides: object) -> ImproveOpportunity:
        defaults: dict[str, object] = {
            "opportunity_id": "abc123",
            "kind": ImproveOpportunityKind.HIGH_REVIEW_LATENCY,
            "entity_type": "repo",
            "entity_id": "repo-1",
            "entity_display_name": None,
            "title": "High review latency",
            "rationale": "p50 first-review was 48 h",
            "score": 0.7,
            "severity": "medium",
            "evidence_refs": ["repo_metrics_daily:pr_first_review_p50_hours:repo-1"],
            "recommended_action": "Reserve a daily review block.",
        }
        defaults.update(overrides)
        return ImproveOpportunity(**defaults)  # type: ignore[arg-type]

    def test_construction(self) -> None:
        opp = self._make()
        assert opp.opportunity_id == "abc123"
        assert opp.kind is ImproveOpportunityKind.HIGH_REVIEW_LATENCY

    def test_frozen(self) -> None:
        opp = self._make()
        with pytest.raises(AttributeError):
            opp.score = 0.9  # type: ignore[misc]

    def test_entity_display_name_defaults_to_none(self) -> None:
        opp = self._make()
        assert opp.entity_display_name is None

    def test_evidence_refs_is_list(self) -> None:
        opp = self._make()
        assert isinstance(opp.evidence_refs, list)


class TestFlowScopeInput:
    def test_both_none_by_default(self) -> None:
        scope = FlowScopeInput()
        assert scope.repo_id is None
        assert scope.team_id is None

    def test_repo_id_accepted(self) -> None:
        uid = uuid.UUID("11111111-1111-1111-1111-111111111111")
        scope = FlowScopeInput(repo_id=uid)
        assert scope.repo_id == uid

    def test_team_id_accepted(self) -> None:
        scope = FlowScopeInput(team_id="team-alpha")
        assert scope.team_id == "team-alpha"

    def test_frozen(self) -> None:
        scope = FlowScopeInput(team_id="x")
        with pytest.raises(AttributeError):
            scope.team_id = "y"  # type: ignore[misc]
