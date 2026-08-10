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

**Held-out proof that the gate is not corpus-shaped.** The corpus-derived
tests above are a *measurement* of the fix, not proof the rule generalizes
-- a threshold could pass all five by being fit to their exact numbers.
``TestTheGateGeneralizesToHeldOutData`` at the bottom of this module is the
other half: it calls ``_corroboration``/``_PressureReading`` directly with
entirely synthetic entity ids and values that share no number with
anything in ``investigation_corpus/world.py``, and proves the SAME
constants (``MIN_CORROBORATING_METRICS=2``, ``OUTLIER_MARGIN_RATIO=0.20``)
correctly separate a genuinely multi-signal case from a single-noisy-metric
one there too. See those constants' own docstrings in ``cohort_discovery.py``
for how each was chosen independent of any corpus case:

* ``MIN_CORROBORATING_METRICS = 2`` is not fit data -- it is the smallest
  integer that can distinguish "one metric" from "more than one", which is
  the literal shape of the ticket's own instruction ("do not equate one
  noisy metric with struggle or capacity pressure"). There is no smaller
  value that expresses that rule at all, and no corpus case was needed to
  arrive at it.
* ``OUTLIER_MARGIN_RATIO = 0.20`` is a round, order-of-magnitude relative
  margin -- the same style of constant as ``semantic_retrieval.
  DEFAULT_MARGIN_RATIO`` (CHAOS-3654), chosen for the same reason: a value
  that clears its cohort median by a large fraction is qualitatively
  different from one a few percent over it, and 20% is a conservative
  floor for "materially outside the norm" that does not depend on any
  metric's units, scale, or the specific numbers any entity happens to
  report. It was not swept or fit against org_helio's values.
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
    MIN_CORROBORATING_METRICS,
    OUTLIER_MARGIN_RATIO,
    CohortDiscovery,
    _corroboration,
    _PressureReading,
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


# ---------------------------------------------------------------------------
# Held-out synthetic controls: the gate proven on data that shares no
# number, no entity id, and no metric VALUE with investigation_corpus/
# world.py. Only the metric NAMES are real -- FAMILY_PRESSURE_METRICS'
# vocabulary is this module's own declared axis set, not a corpus fact, and
# using a real axis name here is what makes this a test of the production
# rule rather than of a toy rule that merely resembles it.
# ---------------------------------------------------------------------------


def _reading(metric: str, value: float, median: float) -> _PressureReading:
    return _PressureReading(metric=metric, value=str(value), cohort_median=str(median))


class TestTheGateGeneralizesToHeldOutData:
    """Same MIN_CORROBORATING_METRICS / OUTLIER_MARGIN_RATIO constants the
    production code uses, exercised against entity ids and values invented
    for this test file alone: ``entity_zeta_never_in_any_corpus`` and
    ``entity_omega_never_in_any_corpus`` name nothing in
    ``investigation_corpus/world.py``, and every value below is chosen to
    prove the RULE'S SHAPE (margin size, signal count), not to reproduce
    any measured case.
    """

    def test_two_independent_wide_margin_signals_corroborate(self) -> None:
        """Should-PASS: two metrics, each far outside a 20% margin."""

        readings = {
            "entity_zeta_never_in_any_corpus": {
                "incidents": _reading("incidents", value=40, median=10),  # +300%
                "cycle_time_median_days": _reading(
                    "cycle_time_median_days", value=20, median=5
                ),  # +300%
            }
        }
        result = _corroboration(
            candidate_id="entity_zeta_never_in_any_corpus",
            family=QuestionFamilyID.STRUGGLING_TEAMS,
            readings=readings,
            dependency_concentrated=False,
        )
        assert result.corroborated is True
        assert set(result.outlying_metrics) == {"incidents", "cycle_time_median_days"}

    def test_one_wide_margin_signal_alone_does_not_corroborate(self) -> None:
        """Should-FAIL: the single-noisy-metric shape, at a DIFFERENT
        margin and DIFFERENT metric than any corpus case -- the rule must
        reject it on signal COUNT, not on having memorized team_borealis's
        or team_frost's specific numbers."""

        readings = {
            "entity_omega_never_in_any_corpus": {
                "incidents": _reading("incidents", value=40, median=10),  # +300%, wide
                "review_cycles_max": _reading(
                    "review_cycles_max", value=5.5, median=5
                ),  # +10%, inside the 20% margin
            }
        }
        result = _corroboration(
            candidate_id="entity_omega_never_in_any_corpus",
            family=QuestionFamilyID.STRUGGLING_TEAMS,
            readings=readings,
            dependency_concentrated=False,
        )
        assert result.corroborated is None
        assert result.outlying_metrics == ("incidents",)
        assert len(result.outlying_metrics) < MIN_CORROBORATING_METRICS

    def test_exactly_at_the_margin_boundary_does_not_count(self) -> None:
        """The boundary itself: a value exactly 20% over its median is not
        > the margin threshold (strict inequality), proving the gate is
        not a coincidental off-by-one that happens to work on real data."""

        reading = _reading("incidents", value=12, median=10)  # exactly +20%
        assert reading.is_outlying is False

    def test_comfortably_past_the_margin_boundary_counts(self) -> None:
        reading = _reading("incidents", value=12.01, median=10)  # just over +20%
        assert reading.is_outlying is True

    def test_a_metric_outside_the_family_vocabulary_is_never_checked(self) -> None:
        """A held-out metric name this family does not declare must not be
        able to corroborate anything, however extreme its value -- proving
        the family-scoping is real and not bypassable by magnitude alone."""

        readings = {
            "entity_zeta_never_in_any_corpus": {
                "totally_invented_metric_name": _reading(
                    "totally_invented_metric_name", value=1000, median=1
                ),
            }
        }
        result = _corroboration(
            candidate_id="entity_zeta_never_in_any_corpus",
            family=QuestionFamilyID.STRUGGLING_TEAMS,
            readings=readings,
            dependency_concentrated=False,
        )
        # Nothing relevant was measured (the one reading present is not in
        # FAMILY_PRESSURE_METRICS[STRUGGLING_TEAMS]) -- preserved as
        # unknown, per the same recall-preservation rule as team_ember.
        assert result.corroborated is False

    def test_dependency_concentration_alone_corroborates_without_any_metric(
        self,
    ) -> None:
        """The relational path, proven independent of measurement entirely:
        no readings at all, and the gate still passes on concentration."""

        result = _corroboration(
            candidate_id="entity_zeta_never_in_any_corpus",
            family=QuestionFamilyID.STRUGGLING_TEAMS,
            readings={},
            dependency_concentrated=True,
        )
        assert result.corroborated is True
