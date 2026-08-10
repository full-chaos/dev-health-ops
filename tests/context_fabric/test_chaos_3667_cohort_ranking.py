"""CHAOS-3667: subjectless cohorts require corroboration, not just
comparability.

Before this fix, ``discover_cohort`` included every candidate that shared
ANY basis with another candidate -- a portfolio, an owning team, a
dependency, or merely measuring the same metric NAME as a peer -- with no
check on whether that candidate showed any actual sign of the pressure the
question was asking about. "Which teams are struggling" therefore answered
with "every team comparable to another team", which is a much larger and
much less precise set than "teams showing corroborated pressure".

The corpus's own team fixtures are written with an explicit intended
disposition (see ``investigation_corpus/world.py``'s own comments beside
each team's measurements):

* ``team_atlas`` -- "struggling, and every axis agrees" (multi-signal, must
  be included);
* ``team_dorado`` -- "review/dependency pressure that only shows up
  outward" (two signals, must be included);
* ``team_borealis`` -- "high WIP, nothing else corroborates" (one signal,
  must NOT read as struggling);
* ``team_frost`` -- "healthy except one noisy metric" (one signal, must NOT
  read as struggling);
* ``team_ember`` -- "the numbers exist but the coverage does not support
  them" (no metric this module checks was measured for it; the honest
  answer is "unknown", not "healthy", so it must be preserved for recall).

These are exactly the five test shapes CHAOS-3667 exists to separate, and
this module tests the real corpus rather than hand-built fixtures because
the whole point is a differential result over data nobody hand-tuned for
this test file.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    CohortExclusionReason,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
    FAMILY_PRESSURE_METRICS,
    HIGHER_IS_WORSE_PRESSURE_METRICS,
    OUTLIER_MARGIN_RATIO,
    CohortDiscovery,
    discover_cohort,
)
from dev_health_ops.context_fabric.graph_arm.projection import build_projection


@pytest.fixture(scope="module")
def projection():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


@pytest.fixture(scope="module")
def grant() -> frozenset[str]:
    return frozenset(adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST))


def _discover(projection, grant, family) -> CohortDiscovery:
    return discover_cohort(
        question_family=family,
        nodes=projection.nodes,
        edges=projection.edges,
        authorized_entity_ids=grant,
        as_of=world.WINDOW_END,
    )


def _member_ids(discovery: CohortDiscovery) -> set[str]:
    return {member.canonical_id for member in discovery.proposal.members}


def _exclusion_reasons(discovery: CohortDiscovery) -> dict[str, CohortExclusionReason]:
    return {item.canonical_id: item.reason for item in discovery.proposal.exclusions}


class TestMultiSignalTeamsAreIncluded:
    def test_the_every_axis_agrees_team_is_included(self, projection, grant) -> None:
        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        assert "team_atlas" in _member_ids(discovery)

    def test_the_two_signal_team_is_included(self, projection, grant) -> None:
        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        assert "team_dorado" in _member_ids(discovery)


class TestSingleNoisyMetricTeamsAreExcluded:
    """The measured precision defect, closed: comparable is not corroborated."""

    def test_the_high_wip_nothing_else_team_is_excluded(
        self, projection, grant
    ) -> None:
        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        reasons = _exclusion_reasons(discovery)
        assert "team_borealis" not in _member_ids(discovery)
        assert (
            reasons.get("team_borealis") is CohortExclusionReason.EXCLUDED_BY_QUESTION
        )

    def test_the_one_noisy_metric_team_is_excluded(self, projection, grant) -> None:
        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        reasons = _exclusion_reasons(discovery)
        assert "team_frost" not in _member_ids(discovery)
        assert reasons.get("team_frost") is CohortExclusionReason.EXCLUDED_BY_QUESTION

    def test_the_exclusion_names_what_was_checked(self, projection, grant) -> None:
        """The explainability requirement: a reader can see WHY, not just that."""

        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        borealis = next(
            item
            for item in discovery.proposal.exclusions
            if item.canonical_id == "team_borealis"
        )
        assert "work_in_progress" in borealis.rationale
        assert "single elevated metric" in borealis.rationale


class TestInsufficientDataIsPreservedNotExcluded:
    def test_a_candidate_with_no_relevant_measurement_stays_a_member(
        self, projection, grant
    ) -> None:
        """Coverage gaps must not read as a clean bill of health.

        team_ember's own corpus note is "the numbers exist but the coverage
        does not support them" -- none of the metrics this family checks
        were measured for it, so the honest disposition is "we cannot say",
        which this module treats as keep-for-recall, not exclude.
        """

        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        assert "team_ember" in _member_ids(discovery)


class TestRecallIsPreserved:
    def test_every_previously_included_multi_signal_team_still_appears(
        self, projection, grant
    ) -> None:
        """The recall half of the ticket's own bound: corroborated members
        from before this fix must not have been collaterally dropped."""

        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        members = _member_ids(discovery)
        assert {"team_atlas", "team_dorado"} <= members

    def test_the_comparability_gate_still_runs_first(self, projection, grant) -> None:
        """A team sharing nothing with any peer is still INSUFFICIENT_EVIDENCE,
        not EXCLUDED_BY_QUESTION -- the two gates stay distinguishable."""

        discovery = _discover(projection, grant, QuestionFamilyID.STRUGGLING_TEAMS)
        reasons = _exclusion_reasons(discovery)
        assert reasons.get("team_cinder") is CohortExclusionReason.INSUFFICIENT_EVIDENCE


class TestPortfolioDependencyRiskHasNoMetricGate:
    """A deliberate design boundary, not an oversight -- pinned so a future
    edit that adds one is a visible decision."""

    def test_the_family_has_no_pressure_metrics_declared(self) -> None:
        assert (
            FAMILY_PRESSURE_METRICS[QuestionFamilyID.PORTFOLIO_DEPENDENCY_RISK]
            == frozenset()
        )

    def test_sharing_a_dependency_is_still_sufficient_on_its_own(
        self, projection, grant
    ) -> None:
        discovery = _discover(
            projection, grant, QuestionFamilyID.PORTFOLIO_DEPENDENCY_RISK
        )
        assert not any(
            item.reason is CohortExclusionReason.EXCLUDED_BY_QUESTION
            for item in discovery.proposal.exclusions
        )


class TestTheMarginAndCountConstantsAreConsistent:
    def test_every_family_metric_has_a_declared_direction(self) -> None:
        """A metric this module selects but cannot orient would be compared
        against nothing meaningful; this pins the two tables never drift."""

        declared = {
            metric for metrics in FAMILY_PRESSURE_METRICS.values() for metric in metrics
        }
        assert declared, "no family declares any pressure metric; vacuous"
        assert declared <= HIGHER_IS_WORSE_PRESSURE_METRICS, (
            f"metrics with no declared direction: "
            f"{declared - HIGHER_IS_WORSE_PRESSURE_METRICS}"
        )

    def test_the_margin_is_a_real_fraction(self) -> None:
        assert 0.0 < OUTLIER_MARGIN_RATIO < 1.0
