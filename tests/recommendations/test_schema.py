"""Tests for recommendations schema dataclasses.

Covers:
- EvidenceRef construction, field types, immutability
- RuleDef construction, field types, immutability
- Recommendation construction, field types, immutability
- evidence tuple immutability
- Hashability (all frozen dataclasses must be hashable)
- Equality semantics
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dev_health_ops.recommendations.schema import (
    EvidenceRef,
    Recommendation,
    RuleDef,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

START = date(2026, 1, 1)
END = date(2026, 1, 8)
COMPUTED_AT = datetime(2026, 1, 8, 12, 0, 0, tzinfo=timezone.utc)


def make_evidence() -> EvidenceRef:
    return EvidenceRef(
        team_id="team-abc",
        metric_table="work_item_metrics_daily",
        window_start=START,
        window_end=END,
        field="wip_count_end_of_day",
        value=14.0,
    )


def make_rule_def() -> RuleDef:
    return RuleDef(
        id="saturation",
        title="Team Saturation",
        description="Rising WIP with flat throughput.",
        success_criterion="WIP trend turns negative in 2 cycles.",
        severity="warning",
        theme="operational-support",
    )


def make_recommendation() -> Recommendation:
    return Recommendation(
        rule_id="saturation",
        team_id="team-abc",
        org_id="org-xyz",
        computed_at=COMPUTED_AT,
        window_start=START,
        window_end=END,
        severity="warning",
        title="Team Saturation",
        rationale="WIP increased by 0.3 items/day; throughput delta was -1.",
        success_criterion="WIP trend turns negative in 2 cycles.",
        evidence=(make_evidence(),),
    )


# ---------------------------------------------------------------------------
# EvidenceRef
# ---------------------------------------------------------------------------


class TestEvidenceRef:
    def test_construction(self) -> None:
        ev = make_evidence()
        assert ev.team_id == "team-abc"
        assert ev.metric_table == "work_item_metrics_daily"
        assert ev.window_start == START
        assert ev.window_end == END
        assert ev.field == "wip_count_end_of_day"
        assert ev.value == 14.0

    def test_is_frozen(self) -> None:
        ev = make_evidence()
        with pytest.raises((AttributeError, TypeError)):
            ev.team_id = "mutated"  # type: ignore[misc]

    def test_is_hashable(self) -> None:
        ev = make_evidence()
        assert hash(ev) is not None
        assert ev in {ev}

    def test_equality(self) -> None:
        assert make_evidence() == make_evidence()

    def test_inequality_on_value(self) -> None:
        ev1 = make_evidence()
        ev2 = EvidenceRef(
            team_id=ev1.team_id,
            metric_table=ev1.metric_table,
            window_start=ev1.window_start,
            window_end=ev1.window_end,
            field=ev1.field,
            value=99.0,
        )
        assert ev1 != ev2

    def test_field_types(self) -> None:
        ev = make_evidence()
        assert isinstance(ev.team_id, str)
        assert isinstance(ev.metric_table, str)
        assert isinstance(ev.window_start, date)
        assert isinstance(ev.window_end, date)
        assert isinstance(ev.field, str)
        assert isinstance(ev.value, float)


# ---------------------------------------------------------------------------
# RuleDef
# ---------------------------------------------------------------------------


class TestRuleDef:
    def test_construction(self) -> None:
        rd = make_rule_def()
        assert rd.id == "saturation"
        assert rd.title == "Team Saturation"
        assert rd.severity == "warning"
        assert rd.theme == "operational-support"

    def test_is_frozen(self) -> None:
        rd = make_rule_def()
        with pytest.raises((AttributeError, TypeError)):
            rd.id = "mutated"  # type: ignore[misc]

    def test_is_hashable(self) -> None:
        rd = make_rule_def()
        assert hash(rd) is not None
        assert rd in {rd}

    def test_equality(self) -> None:
        assert make_rule_def() == make_rule_def()

    def test_field_types(self) -> None:
        rd = make_rule_def()
        assert isinstance(rd.id, str)
        assert isinstance(rd.title, str)
        assert isinstance(rd.description, str)
        assert isinstance(rd.success_criterion, str)
        assert isinstance(rd.severity, str)
        assert isinstance(rd.theme, str)

    def test_severity_values(self) -> None:
        for sev in ("warning", "critical"):
            rd = RuleDef(
                id="x",
                title="T",
                description="D",
                success_criterion="S",
                severity=sev,
                theme="operational-support",
            )
            assert rd.severity == sev

    def test_theme_values(self) -> None:
        themes = (
            "feature-delivery",
            "operational-support",
            "maintenance-tech-debt",
            "quality-reliability",
            "risk-security",
        )
        for theme in themes:
            rd = RuleDef(
                id="x",
                title="T",
                description="D",
                success_criterion="S",
                severity="warning",
                theme=theme,  # type: ignore[arg-type]
            )
            assert rd.theme == theme


# ---------------------------------------------------------------------------
# Recommendation
# ---------------------------------------------------------------------------


class TestRecommendation:
    def test_construction(self) -> None:
        rec = make_recommendation()
        assert rec.rule_id == "saturation"
        assert rec.team_id == "team-abc"
        assert rec.org_id == "org-xyz"
        assert rec.computed_at == COMPUTED_AT
        assert rec.window_start == START
        assert rec.window_end == END
        assert rec.severity == "warning"
        assert len(rec.evidence) == 1

    def test_is_frozen(self) -> None:
        rec = make_recommendation()
        with pytest.raises((AttributeError, TypeError)):
            rec.rule_id = "mutated"  # type: ignore[misc]

    def test_evidence_is_tuple(self) -> None:
        rec = make_recommendation()
        assert isinstance(rec.evidence, tuple)

    def test_evidence_tuple_is_immutable(self) -> None:
        rec = make_recommendation()
        with pytest.raises(TypeError):
            rec.evidence[0] = make_evidence()  # type: ignore[index]

    def test_evidence_can_be_empty(self) -> None:
        rec = Recommendation(
            rule_id="saturation",
            team_id="team-abc",
            org_id="org-xyz",
            computed_at=COMPUTED_AT,
            window_start=START,
            window_end=END,
            severity="warning",
            title="T",
            rationale="R",
            success_criterion="S",
            evidence=(),
        )
        assert rec.evidence == ()

    def test_is_hashable(self) -> None:
        rec = make_recommendation()
        assert hash(rec) is not None
        assert rec in {rec}

    def test_equality(self) -> None:
        assert make_recommendation() == make_recommendation()

    def test_field_types(self) -> None:
        rec = make_recommendation()
        assert isinstance(rec.rule_id, str)
        assert isinstance(rec.team_id, str)
        assert isinstance(rec.org_id, str)
        assert isinstance(rec.computed_at, datetime)
        assert isinstance(rec.window_start, date)
        assert isinstance(rec.window_end, date)
        assert isinstance(rec.severity, str)
        assert isinstance(rec.title, str)
        assert isinstance(rec.rationale, str)
        assert isinstance(rec.success_criterion, str)
        assert isinstance(rec.evidence, tuple)

    def test_multiple_evidence_refs(self) -> None:
        ev2 = EvidenceRef(
            team_id="team-abc",
            metric_table="team_metrics_daily",
            window_start=START,
            window_end=END,
            field="after_hours_ratio",
            value=0.35,
        )
        rec = Recommendation(
            rule_id="sustainability-risk",
            team_id="team-abc",
            org_id="org-xyz",
            computed_at=COMPUTED_AT,
            window_start=START,
            window_end=END,
            severity="critical",
            title="Sustainability Risk",
            rationale="After-hours 35%, cycle time rising.",
            success_criterion="After-hours drops in 2 cycles.",
            evidence=(make_evidence(), ev2),
        )
        assert len(rec.evidence) == 2
        assert rec.evidence[1].field == "after_hours_ratio"
