"""CHAOS-3645: the graph arm's SECOND entry mode -- subjectless cohorts.

:mod:`~.discovery` resolves a reference the question contains. A cohort
question contains none, and not by accident: "which teams are currently
struggling", "where do we have slack", "which declared-complete projects lack
delivery evidence" name no subject *by design*, and every one of the corpus's
fourteen ``discovered_cohort`` cases refused on exactly that -- zero mentions
extracted, therefore zero seeds, therefore no investigation. No improvement to
mention extraction can reach them, because there is nothing in the text to
extract. The arm needs a different way in.

This module is that way in: **question family plus authorized organization
scope, in and a bounded cohort out**, with no reference resolution anywhere in
the path. :func:`discover_cohort` never calls ``extract_mentions``, never
takes a question string, and has no parameter through which one could arrive
-- the same by-construction discipline ``trials.chaos_3619.graph_leg`` applies
to the seeded path, for the same reason.

Four rules shape what it will and will not enumerate.

**The candidate universe comes from a CLOSED set of entity kinds, chosen by
question family.** :data:`FAMILY_CANDIDATE_KINDS` maps a family to the kinds a
question in that family is *about*: teams for the team-pressure families,
projects for the capacity and portfolio families. A family absent from the map
gets no subjectless entry at all, and the absences are deliberate --
``clarification_and_no_match`` is the loudest one. "What is going sideways?"
is a question whose right answer is a clarification; enumerating the whole
organization in response is precisely the unsafe-widening fault the corpus
plants that case to catch, so this module refuses rather than answers.

**Nothing widens the authorization scope.** The universe is intersected with
the principal's authorized entity set before anything else happens, exactly as
``search_candidates`` does, and entities of the right kind that the principal
may not see are counted into ``authorization_filtered_count`` rather than
named. Enumeration is the mode with the most room to leak -- a subject-seeded
walk can only reach what the subject touches, while this one starts from "all
of them" -- so the narrowing happens first, on the way in.

**Every member states a basis, the basis is shared with the other members, and
the basis is one the world can support.** The subject-anchored builder's rule
was "the reason has to be a real traversal"; here there is no subject to
traverse from, so the rule becomes mutual: a candidate joins only if the
projection shows it sharing an anchor -- a portfolio, an owning team, a
dependency, an initiative parent -- or a measured metric with at least one
other candidate. A candidate that shares nothing is EXCLUDED with that stated
as the reason. It is not dropped, and it is not admitted on a basis like "in
the same organization": the frozen ``CohortInclusionBasis`` vocabulary has no
such member, and the corpus oracle checks a stated basis against the world
rather than accepting the string.

**No subject is invented to hang the cohort off.** The obvious shortcut --
promote the strongest candidate to "the subject" and file the rest as its
peers -- would make the packet claim a subject match that never happened:
``SubjectCandidate.match_signals`` is ``min_length=1`` and every member of
``SubjectMatchSignal`` describes matching a reference the question supplied.
A ``scope_enumerated`` proposal therefore commits to no subject, and
``packet_builder`` emits no subject candidates for one. The frozen contract
already permits this -- ``SubjectDiscovery`` says so in its own docstring --
which is what makes the honest shape available at all.

**CHAOS-3667: comparability is necessary and is no longer sufficient.**
Sharing a basis with a peer answers "is this candidate part of the same
comparison" -- it does not answer "is this candidate an instance of the
thing the question is asking about". A candidate that clears the
comparability gate above is now also checked for CORROBORATION
(:func:`_corroboration`): at least two independent measured metrics sitting
materially outside their own cohort median (:data:`OUTLIER_MARGIN_RATIO`,
:data:`MIN_CORROBORATING_METRICS`), or a relational signal
(:func:`_dependency_concentration`) a single metric cannot provide. One
elevated number is a correlate a canonical service happened to report, not
struggle or capacity pressure, and treating it as either is the exact fault
the ticket names. A comparable candidate with no relevant measurement at
all is kept, not excluded: an absence of DATA is not evidence of health,
and preserving recall means a coverage gap must never read as a clean bill
of health.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from dev_health_ops.api.dev.investigation_contract import (
    CohortExclusionReason,
    CohortInclusionBasis,
    ComparisonDimension,
    QuestionFamilyID,
    RelationshipType,
)

from .cohort import (
    CohortCandidate,
    CohortEntryMode,
    CohortExclusionRecord,
    CohortProposal,
)
from .projection import GraphEdge, GraphNode
from .vocabulary import SOURCE_EVIDENCE_ENTITY_ATTRIBUTE, GraphEntityKind

__all__ = [
    "ANCHOR_BASIS_RELATIONSHIPS",
    "FAMILY_CANDIDATE_KINDS",
    "FAMILY_PRESSURE_METRICS",
    "HIGHER_IS_WORSE_PRESSURE_METRICS",
    "MEASUREMENT_COHORT_MEDIAN_ATTRIBUTE",
    "MEASUREMENT_METRIC_ATTRIBUTE",
    "MEASUREMENT_VALUE_ATTRIBUTE",
    "METRIC_COMPARISON_DIMENSIONS",
    "MIN_CONCENTRATED_DEPENDENTS",
    "CohortDiscovery",
    "UnsupportedCohortFamilyError",
    "discover_cohort",
]


class UnsupportedCohortFamilyError(NotImplementedError):
    """This family has no subjectless entry, and that is a decision.

    Raised rather than returning an empty proposal so the caller records a
    named capability boundary instead of an empty comparison. An empty cohort
    and a refused family are the same zero in a records file, and only one of
    them is a statement the arm meant to make.
    """


#: Which entity kinds a subjectless question in each family is ABOUT.
#:
#: Closed on purpose, and the absences carry as much meaning as the entries.
#: A family reaches this map only when "the answer is a set of X" is what the
#: family means, so the enumeration is answering the question rather than
#: sweeping the tenant and hoping. Absent, and staying absent:
#:
#: * ``clarification_and_no_match`` -- the right answer is a clarification.
#:   Enumerating an organization in response to "what is going sideways?" is
#:   the unsafe-widening fault, not a cohort;
#: * ``ambiguous_identity`` and ``colloquial_follow_up`` -- both turn on a
#:   reference the question DOES contain, so the seeded path owns them and a
#:   scope enumeration would answer a question nobody asked;
#: * ``project_status_drivers`` and ``staffing_language`` -- both are about a
#:   named subject's state; the cohort-shaped variants of those questions
#:   classify into the capacity families instead.
FAMILY_CANDIDATE_KINDS: Mapping[QuestionFamilyID, frozenset[GraphEntityKind]] = {
    QuestionFamilyID.STRUGGLING_TEAMS: frozenset({GraphEntityKind.TEAM}),
    QuestionFamilyID.PRESSURE_SIGNALS: frozenset({GraphEntityKind.TEAM}),
    QuestionFamilyID.PROJECT_CAPACITY: frozenset({GraphEntityKind.PROJECT}),
    QuestionFamilyID.PORTFOLIO_DEPENDENCY_RISK: frozenset({GraphEntityKind.PROJECT}),
    QuestionFamilyID.DECLARED_VERSUS_ACTUAL: frozenset({GraphEntityKind.PROJECT}),
}

#: Relationships that put two candidates on a shared ANCHOR, and the basis
#: each one supports.
#:
#: Directed, and the direction is the whole correctness argument. The oracle's
#: ``world.shares_basis`` reads these edges with the member as the SOURCE --
#: a project holds ``belongs_to_portfolio``, a portfolio does not hold it back
#: -- so a basis derived from the reverse direction would be stated by the arm
#: and denied by the world. That is exactly the "well-explained but factually
#: irrelevant member" fault, and reading the edge the way the world reads it is
#: what keeps this side of it honest.
#:
#: ``owned_by_team`` is here for PROJECT candidates and is unreachable for TEAM
#: candidates, because a team is the TARGET of that edge and never its source.
#: Two teams therefore never share team ownership, which is correct and is why
#: the team families lean on the measurement basis below.
ANCHOR_BASIS_RELATIONSHIPS: Mapping[RelationshipType, CohortInclusionBasis] = {
    RelationshipType.BELONGS_TO_PORTFOLIO: CohortInclusionBasis.SAME_PORTFOLIO,
    RelationshipType.OWNED_BY_TEAM: CohortInclusionBasis.SHARED_TEAM_OWNERSHIP,
    RelationshipType.DEPENDS_ON: CohortInclusionBasis.SHARED_DEPENDENCY,
}

#: The projection attribute carrying a canonical measurement's metric name.
#: Same literal ``drivers.py`` reads; named here so the two uses are greppable
#: together.
MEASUREMENT_METRIC_ATTRIBUTE = "measurement_metric"

#: Which comparison axis each canonical metric can actually support.
#:
#: **This is the ARM's own reading of the canonical metric vocabulary, written
#: from the metric names, and it is deliberately not imported from the
#: corpus.** The corpus keeps an equivalent table for its oracle
#: (``world.COMPARISON_DIMENSION_METRICS``). Importing that one would make the
#: arm agree with the oracle by construction and the agreement would measure
#: nothing; keeping them independent is what lets a disagreement show up as a
#: failed dimension rather than as a table nobody can be wrong about.
#:
#: Metrics absent from this map are absent on purpose. ``contributors_ever``,
#: ``interruption_load_percentile`` and ``days_open`` are real numbers the
#: projection carries and no ``ComparisonDimension`` states, so they can make
#: two entities *comparable* -- they are measured the same way -- without
#: licensing a claim that the packet supports a named axis of comparison.
METRIC_COMPARISON_DIMENSIONS: Mapping[str, ComparisonDimension] = {
    "completed_items": ComparisonDimension.DELIVERY_THROUGHPUT,
    "cycle_time_median_days": ComparisonDimension.CYCLE_TIME,
    "cycle_time_p90_days": ComparisonDimension.CYCLE_TIME,
    "median_review_wait_days": ComparisonDimension.REVIEW_LOAD,
    "outbound_review_share": ComparisonDimension.REVIEW_LOAD,
    "review_cycles_max": ComparisonDimension.REVIEW_LOAD,
    "work_in_progress": ComparisonDimension.WORK_IN_PROGRESS,
    "incidents": ComparisonDimension.INCIDENT_LOAD,
    "production_deployments": ComparisonDimension.DEPLOYMENT_FREQUENCY,
    "new_value_share": ComparisonDimension.INVESTMENT_MIX,
    "ktlo_share": ComparisonDimension.INVESTMENT_MIX,
    "open_deficiencies": ComparisonDimension.OPEN_DEFICIENCY_COUNT,
    "missing_controls": ComparisonDimension.OPEN_DEFICIENCY_COUNT,
    "open_child_units": ComparisonDimension.STATUS_DECLARATION_GAP,
    "target_date_changes": ComparisonDimension.STATUS_DECLARATION_GAP,
    "arrived_items": ComparisonDimension.CAPACITY_LOAD_RATIO,
    "assigned_fte": ComparisonDimension.CAPACITY_LOAD_RATIO,
    "feed_lag_days": ComparisonDimension.DATA_COVERAGE,
}

#: CHAOS-3667: the projection attributes carrying a canonical measurement's
#: cited value and its cohort median. Same literals ``drivers.py`` reads
#: (``measurement_value``, ``measurement_cohort_median``); named here so a
#: reader can see the two "cite, never compute" callers side by side.
MEASUREMENT_VALUE_ATTRIBUTE = "measurement_value"
MEASUREMENT_COHORT_MEDIAN_ATTRIBUTE = "measurement_cohort_median"

#: CHAOS-3667: which metrics corroborate PRESSURE for a subjectless cohort
#: question in each family -- the arm's own reading, independent of
#: ``drivers.MEASUREMENT_CATEGORY`` for the same reason
#: :data:`METRIC_COMPARISON_DIMENSIONS` is independent of the corpus's table:
#: two tables that could disagree are worth more than one nobody can be wrong
#: about. Every metric here is drawn from :data:`METRIC_COMPARISON_DIMENSIONS`
#: -- a metric this module does not already recognise structurally is not
#: added here either.
#:
#: An empty set (``PORTFOLIO_DEPENDENCY_RISK``) means the family's own
#: comparability basis already IS the pressure signal: a project sharing a
#: dependency with another authorized project is what "at risk from a shared
#: dependency" means, and no canonical measurement narrows that further here
#: -- concentration is read from the relationship itself in
#: :func:`_dependency_concentration`.
FAMILY_PRESSURE_METRICS: Mapping[QuestionFamilyID, frozenset[str]] = {
    # STRUGGLING_TEAMS and PRESSURE_SIGNALS share one metric set: both are
    # TEAM-kind questions about the same underlying pressure, and a reader
    # who asked either would expect the same evidence to count.
    QuestionFamilyID.STRUGGLING_TEAMS: frozenset(
        {
            "incidents",
            "cycle_time_median_days",
            "cycle_time_p90_days",
            "median_review_wait_days",
            "review_cycles_max",
            "work_in_progress",
            "outbound_review_share",
        }
    ),
    QuestionFamilyID.PRESSURE_SIGNALS: frozenset(
        {
            "incidents",
            "cycle_time_median_days",
            "cycle_time_p90_days",
            "median_review_wait_days",
            "review_cycles_max",
            "work_in_progress",
            "outbound_review_share",
        }
    ),
    QuestionFamilyID.PROJECT_CAPACITY: frozenset(
        {
            "arrived_items",
            "work_in_progress",
            "cycle_time_median_days",
        }
    ),
    QuestionFamilyID.PORTFOLIO_DEPENDENCY_RISK: frozenset(),
    QuestionFamilyID.DECLARED_VERSUS_ACTUAL: frozenset(
        {
            "open_child_units",
            "target_date_changes",
            "open_deficiencies",
            "missing_controls",
        }
    ),
}

#: CHAOS-3667: every metric in :data:`FAMILY_PRESSURE_METRICS` moves in the
#: same direction -- higher is worse -- which is why there is one comparison
#: rule below rather than a per-metric direction table. Stated as its own
#: constant (rather than left implicit) so a metric moving the other way,
#: added later, is a deliberate edit here and not a silent wrong-direction
#: comparison; :func:`_all_pressure_metrics_are_higher_is_worse` in the test
#: suite pins that this set and the family map never drift apart.
HIGHER_IS_WORSE_PRESSURE_METRICS: frozenset[str] = frozenset(
    {
        "incidents",
        "cycle_time_median_days",
        "cycle_time_p90_days",
        "median_review_wait_days",
        "review_cycles_max",
        "work_in_progress",
        "outbound_review_share",
        "arrived_items",
        "open_child_units",
        "target_date_changes",
        "open_deficiencies",
        "missing_controls",
    }
)

#: How many other comparable candidates must depend on the same target for
#: that target's own SHARED_DEPENDENCY membership to read as *concentration*
#: rather than an incidental pair. Two is the floor at which "shared" starts
#: meaning "several projects lean on this", which is what a dependency-risk
#: question is actually asking about.
MIN_CONCENTRATED_DEPENDENTS = 2

#: CHAOS-3667: how far a cited value must clear its cohort median, as a
#: fraction of the median, to count as outlying at all.
#:
#: A bare ``value > median`` -- the comparison ``drivers._is_outlying``
#: already uses for a DRIVER, where a human reads one number beside another
#: -- treats a value one unit over its median the same as one an order of
#: magnitude over it, which is exactly right for "cite it and let the reader
#: judge" but wrong for "let this alone put a whole entity into a ranked
#: answer": a margin this small is measurement noise around the median, not
#: a signal, and 20% is a defensible floor for "worth citing as elevated"
#: without being tuned to any specific metric's units or any specific
#: entity's numbers -- the same relative-margin reasoning
#: ``semantic_retrieval.DEFAULT_MARGIN_RATIO`` uses for a different
#: comparison entirely.
OUTLIER_MARGIN_RATIO = 0.20

#: CHAOS-3667: how many independent outlying metrics a candidate needs
#: before a measurement corroborates its inclusion.
#:
#: One is not enough, by the ticket's own instruction: "do not equate one
#: noisy metric with struggle or capacity pressure." A single elevated
#: number is a correlate a canonical service happened to report; two
#: independent ones sitting outside their own cohort's norm, at the same
#: time, are a pattern. Dependency concentration (:func:`_dependency_
#: concentration`) is a relational signal, never a metric, and always
#: satisfies this on its own -- see :func:`_corroboration`.
MIN_CORROBORATING_METRICS = 2

#: How many enumerated members the cohort may carry.
#:
#: One below the frozen contract's fifty, leaving room for the committed
#: subject the SUBJECT_ANCHORED mode adds -- the same headroom
#: :func:`~.cohort.build_cohort` reserves, kept identical so a reader is not
#: asked to hold two bounds.
MAX_COHORT_MEMBERS = 49

#: How many exclusions may be named. The rest are disclosed as a count.
MAX_COHORT_EXCLUSIONS = 50


@dataclass(frozen=True, slots=True)
class CohortDiscovery:
    """A subjectless cohort and what the enumeration had to leave out.

    ``proposal`` is the same :class:`~.cohort.CohortProposal` the seeded path
    produces, tagged ``SCOPE_ENUMERATED``, so one packet builder serves both
    entry modes and neither gets a private assembly path that could drift.

    The counts beside it are the enumeration's own disclosures. They are
    separate fields rather than prose because each answers a different
    question a reader will actually ask: how wide was the universe before
    authorization, how many of its members the graph could not relate to
    anything, and how many the size bound dropped.
    """

    proposal: CohortProposal
    candidate_kinds: tuple[GraphEntityKind, ...]
    universe_size: int
    authorization_filtered_count: int

    @property
    def is_comparable(self) -> bool:
        return self.proposal.is_comparable


def _valid_at(
    valid_from: datetime | None, valid_to: datetime | None, when: datetime
) -> bool:
    """Whether a projected fact is live at ``when``.

    Open intervals on both ends: an unbounded fact is live, which matches how
    the projection records something with no recorded end. Applied to every
    edge and measurement this module reads, because a cohort assembled from
    lapsed relationships is a comparison of last quarter's organization
    presented as this one's.
    """

    if valid_from is not None and valid_from > when:
        return False
    return not (valid_to is not None and valid_to <= when)


def _anchors_by_basis(
    candidate_ids: frozenset[str], edges: Sequence[GraphEdge], as_of: datetime
) -> dict[str, dict[CohortInclusionBasis, set[str]]]:
    """For each candidate, the anchors it holds, grouped by the basis they mean.

    The candidate must be the edge's SOURCE -- see
    :data:`ANCHOR_BASIS_RELATIONSHIPS` for why the direction is not incidental.
    """

    held: dict[str, dict[CohortInclusionBasis, set[str]]] = {}
    for edge in edges:
        basis = ANCHOR_BASIS_RELATIONSHIPS.get(edge.relationship)
        if basis is None:
            continue
        if edge.source_canonical_id not in candidate_ids:
            continue
        if not _valid_at(edge.valid_from, edge.valid_to, as_of):
            continue
        held.setdefault(edge.source_canonical_id, {}).setdefault(basis, set()).add(
            edge.target_canonical_id
        )
    return held


def _initiative_parents(
    candidate_ids: frozenset[str], edges: Sequence[GraphEdge], as_of: datetime
) -> dict[str, set[str]]:
    """Each candidate's ``parent_of`` parents, read in the world's direction.

    ``world.shares_basis`` resolves ``SAME_INITIATIVE`` by looking for a
    ``parent_of`` edge whose TARGET is the member, which is the one basis read
    against the arrow rather than along it. Kept in its own function so that
    asymmetry is visible rather than buried as an exception inside the loop
    above.
    """

    parents: dict[str, set[str]] = {}
    for edge in edges:
        if edge.relationship is not RelationshipType.PARENT_OF:
            continue
        if edge.target_canonical_id not in candidate_ids:
            continue
        if not _valid_at(edge.valid_from, edge.valid_to, as_of):
            continue
        parents.setdefault(edge.target_canonical_id, set()).add(
            edge.source_canonical_id
        )
    return parents


def _measured_metrics(
    candidate_ids: frozenset[str], nodes: Iterable[GraphNode], as_of: datetime
) -> dict[str, set[str]]:
    """Each candidate's measured metric names, from the projection's own nodes.

    The metric NAME only -- never the value. This function decides who is
    comparable with whom; it does not compare, rank or threshold.

    CHAOS-3667 adds a SEPARATE function, :func:`_pressure_readings`, that
    does read values -- but only to CITE a canonical measurement against its
    own cohort median, exactly as ``drivers._is_outlying`` already does for
    the seeded path. Comparing two numbers a canonical service minted is not
    computing a new one; the boundary this docstring describes has not
    moved, and the two functions are kept apart so a reader can see which
    one is which.
    """

    metrics: dict[str, set[str]] = {}
    for node in nodes:
        metric = node.attributes.get(MEASUREMENT_METRIC_ATTRIBUTE)
        entity = node.attributes.get(SOURCE_EVIDENCE_ENTITY_ATTRIBUTE)
        if not metric or entity not in candidate_ids:
            continue
        if not _valid_at(node.valid_from, node.valid_to, as_of):
            continue
        metrics.setdefault(str(entity), set()).add(str(metric))
    return metrics


@dataclass(frozen=True, slots=True)
class _PressureReading:
    """One candidate's cited value and cohort median for one metric."""

    metric: str
    value: str
    cohort_median: str

    @property
    def is_outlying(self) -> bool:
        """Whether this citation clears its cohort median by a real margin.

        Every metric :data:`FAMILY_PRESSURE_METRICS` can select is in
        :data:`HIGHER_IS_WORSE_PRESSURE_METRICS`, so there is one comparison
        rule rather than a per-metric direction lookup -- see that
        constant's own docstring for why a metric moving the other way is a
        deliberate edit, not a silent one.

        Gated by :data:`OUTLIER_MARGIN_RATIO`, not a bare ``value >
        median``: see that constant's own docstring for why a value one
        unit over its median must not read the same as one an order of
        magnitude over it here.
        """

        try:
            value, median = float(self.value), float(self.cohort_median)
        except (TypeError, ValueError):
            # Non-numeric or absent is not comparable, and guessing is worse
            # than declining: the caller treats this as "not corroborated",
            # never as "corroborated by default".
            return False
        if median > 0:
            return value > median * (1 + OUTLIER_MARGIN_RATIO)
        # A zero-or-negative median has no meaningful percentage margin --
        # any positive excess over it is already qualitatively different
        # from the cohort's norm, not a rounding difference.
        return value > median


def _pressure_readings(
    candidate_ids: frozenset[str], nodes: Iterable[GraphNode], as_of: datetime
) -> dict[str, dict[str, _PressureReading]]:
    """Each candidate's cited value/median pairs, keyed by metric name.

    CHAOS-3667. Reads exactly what ``drivers._measurement_candidates``
    already reads off the same attribute keys -- ``measurement_value`` and
    ``measurement_cohort_median`` -- and does the same thing with them: cite
    both, verbatim, never derive a third number. A measurement with no
    cohort median (drivers.py's own ``INSUFFICIENT_MEASUREMENT`` case) is
    skipped here for the same reason it cannot say "elevated" there --
    :meth:`_PressureReading.is_outlying` would refuse it anyway, but
    skipping keeps a metric with nothing to compare it against from
    occupying the entry at all.
    """

    readings: dict[str, dict[str, _PressureReading]] = {}
    for node in nodes:
        metric = node.attributes.get(MEASUREMENT_METRIC_ATTRIBUTE)
        entity = node.attributes.get(SOURCE_EVIDENCE_ENTITY_ATTRIBUTE)
        if not metric or entity not in candidate_ids:
            continue
        if not _valid_at(node.valid_from, node.valid_to, as_of):
            continue
        value = node.attributes.get(MEASUREMENT_VALUE_ATTRIBUTE)
        median = node.attributes.get(MEASUREMENT_COHORT_MEDIAN_ATTRIBUTE)
        if value is None or median is None:
            continue
        readings.setdefault(str(entity), {})[str(metric)] = _PressureReading(
            metric=str(metric), value=str(value), cohort_median=str(median)
        )
    return readings


@dataclass(frozen=True, slots=True)
class _Corroboration:
    """Whether a candidate's inclusion is corroborated, and by what.

    Three-valued rather than a bare bool, because "checked and found
    unremarkable" and "nothing to check" are different findings and a
    reader excluded on the strength of the wrong one would be told a claim
    the arm never actually verified.
    """

    #: ``True`` -- keep, corroborated. ``False`` -- keep, no relevant signal
    #: was available to check (preserve recall on unknown data). ``None`` --
    #: exclude: relevant signals existed and fewer than
    #: :data:`MIN_CORROBORATING_METRICS` were outlying.
    corroborated: bool | None
    #: The metric names that WERE outlying, whether or not that was enough
    #: to corroborate. Populated on both ``True`` (>= the threshold) and
    #: ``None`` (below it, including the single-noisy-metric shape the
    #: exclusion rationale names by number) -- never on ``False``, where
    #: nothing was checked at all.
    outlying_metrics: tuple[str, ...] = ()
    #: Every metric name that was checked (outlying or not), for the
    #: exclusion rationale to name everything that was actually looked at,
    #: not only the unremarkable ones. Populated only when
    #: ``corroborated is None``.
    checked_metrics: tuple[str, ...] = ()


def _corroboration(
    *,
    candidate_id: str,
    family: QuestionFamilyID,
    readings: Mapping[str, Mapping[str, _PressureReading]],
    dependency_concentrated: bool,
) -> _Corroboration:
    """Whether canonical evidence backs including this candidate.

    Relational concentration (CHAOS-3667: "dependency criticality") always
    corroborates on its own -- it is not a measurement, so it never competes
    with or is overridden by the metric check below.

    A family with no entry in :data:`FAMILY_PRESSURE_METRICS`
    (``PORTFOLIO_DEPENDENCY_RISK``) has no metric gate at all: its own
    comparability basis (a shared dependency) already IS the signal.
    """

    if dependency_concentrated:
        return _Corroboration(corroborated=True)

    relevant = FAMILY_PRESSURE_METRICS.get(family, frozenset())
    if not relevant:
        return _Corroboration(corroborated=True)

    mine = readings.get(candidate_id, {})
    checked = sorted(name for name in mine if name in relevant)
    if not checked:
        # No relevant metric was measured for this candidate at all -- an
        # absence of DATA, not an absence of PRESSURE. Preserving recall
        # means not letting a coverage gap read as a clean bill of health.
        return _Corroboration(corroborated=False)

    outlying = tuple(name for name in checked if mine[name].is_outlying)
    if len(outlying) >= MIN_CORROBORATING_METRICS:
        return _Corroboration(corroborated=True, outlying_metrics=outlying)
    # Fewer than MIN_CORROBORATING_METRICS outlying signals -- including
    # zero -- is "checked and not corroborated", not "unknown": the metric
    # WAS measured and compared, and either sat inside its cohort's norm or
    # was the one noisy signal the ticket names by name ("do not equate one
    # noisy metric with struggle or capacity pressure"). Both read the same
    # way to a caller deciding whether this candidate belongs in the
    # answer, so both share the exclusion rather than one silently
    # widening to look corroborated.
    return _Corroboration(
        corroborated=None,
        outlying_metrics=outlying,
        checked_metrics=tuple(checked),
    )


def _dependency_concentration(
    candidate_ids: frozenset[str],
    anchors: Mapping[str, Mapping[CohortInclusionBasis, set[str]]],
) -> frozenset[str]:
    """Candidates that depend on a target at least
    :data:`MIN_CONCENTRATED_DEPENDENTS` other candidates also depend on.

    Reads the SAME anchor data :func:`discover_cohort` already computed for
    the comparability basis -- no new traversal, only a different question
    asked of it: not "do two candidates share a dependency" but "does one
    dependency concentrate several of them".
    """

    depended_by: dict[str, set[str]] = {}
    for candidate in candidate_ids:
        for target in anchors.get(candidate, {}).get(
            CohortInclusionBasis.SHARED_DEPENDENCY, set()
        ):
            depended_by.setdefault(target, set()).add(candidate)
    concentrated: set[str] = set()
    for dependents in depended_by.values():
        if len(dependents) >= MIN_CONCENTRATED_DEPENDENTS:
            concentrated.update(dependents)
    return frozenset(concentrated)


def discover_cohort(
    *,
    question_family: QuestionFamilyID,
    nodes: Sequence[GraphNode],
    edges: Sequence[GraphEdge],
    authorized_entity_ids: Sequence[str] | frozenset[str],
    as_of: datetime,
    max_members: int = MAX_COHORT_MEMBERS,
    max_exclusions: int = MAX_COHORT_EXCLUSIONS,
) -> CohortDiscovery:
    """Enumerate a bounded, mutually-comparable cohort for a subjectless family.

    Note what is absent from the signature, because absence is the guarantee:
    no question, no mention, no seed, no case id. The only inputs are the
    analytical job's family, the projection, and what the principal may see --
    so this cannot be accidentally handed a subject, and a question whose text
    yields nothing to extract is no worse off here than one that names three
    projects.

    Raises :class:`UnsupportedCohortFamilyError` for a family with no entry in
    :data:`FAMILY_CANDIDATE_KINDS`. That refusal is the point of the closed
    map and must reach the caller as a refusal.
    """

    kinds = FAMILY_CANDIDATE_KINDS.get(question_family)
    if kinds is None:
        raise UnsupportedCohortFamilyError(
            f"family {question_family.value} has no subjectless candidate "
            "universe: the arm will not answer it by enumerating the "
            "organization, because a question this family covers is not a "
            "question about a set of entities. Refused rather than answered "
            "with a cohort nobody asked for"
        )

    authorized = frozenset(authorized_entity_ids)
    ordered_kinds = tuple(sorted(kinds, key=lambda kind: kind.value))

    # Authorization is applied on the way IN, before anything is enumerated,
    # ranked or counted. An entity the principal may not see must never
    # occupy a member slot, an exclusion slot or a truncation count -- all
    # three are ways it becomes visible without being "returned".
    labels: dict[str, tuple[GraphEntityKind, str]] = {}
    withheld: set[str] = set()
    for node in nodes:
        kind = node.entity_kind
        if not node.is_entity or kind is None or kind not in kinds:
            continue
        if node.canonical_id not in authorized:
            withheld.add(node.canonical_id)
            continue
        labels[node.canonical_id] = (kind, node.display_label)

    candidate_ids = frozenset(labels)
    anchors = _anchors_by_basis(candidate_ids, edges, as_of)
    parents = _initiative_parents(candidate_ids, edges, as_of)
    metrics = _measured_metrics(candidate_ids, nodes, as_of)
    # CHAOS-3667. Read once, up front, alongside the comparability inputs
    # above -- both are consulted per candidate below, after the
    # comparability gate, to decide whether a comparable candidate also
    # corroborates the question's own pressure claim.
    readings = _pressure_readings(candidate_ids, nodes, as_of)
    concentrated = _dependency_concentration(candidate_ids, anchors)

    # peer -> basis -> the anchors it SHARES with at least one other
    # candidate. Shared, not merely held: a portfolio only this candidate
    # belongs to explains nothing about a comparison.
    shared: dict[str, dict[CohortInclusionBasis, set[str]]] = {}

    def _record(peer: str, basis: CohortInclusionBasis, values: Iterable[str]) -> None:
        entry = shared.setdefault(peer, {}).setdefault(basis, set())
        entry.update(values)

    for peer in sorted(candidate_ids):
        others = [other for other in candidate_ids if other != peer]
        for basis in sorted(
            {
                *ANCHOR_BASIS_RELATIONSHIPS.values(),
            },
            key=lambda item: item.value,
        ):
            mine = anchors.get(peer, {}).get(basis, set())
            if not mine:
                continue
            common = {
                anchor
                for other in others
                for anchor in mine & anchors.get(other, {}).get(basis, set())
            }
            if common:
                _record(peer, basis, common)
        mine_parents = parents.get(peer, set())
        if mine_parents:
            common_parents = {
                parent
                for other in others
                for parent in mine_parents & parents.get(other, set())
            }
            if common_parents:
                _record(peer, CohortInclusionBasis.SAME_INITIATIVE, common_parents)
        mine_metrics = metrics.get(peer, set())
        if mine_metrics:
            common_metrics = {
                metric
                for other in others
                for metric in mine_metrics & metrics.get(other, set())
            }
            if common_metrics:
                _record(
                    peer,
                    CohortInclusionBasis.COMPARABLE_DELIVERY_PROFILE,
                    common_metrics,
                )

    members: list[CohortCandidate] = []
    exclusions: list[CohortExclusionRecord] = []
    for peer in sorted(candidate_ids):
        kind, label = labels[peer]
        bases = shared.get(peer)
        if not bases:
            # Considered and rejected, with the reason recorded. "Why is this
            # team not in the comparison" is a question the packet exists to
            # answer, and an absence answers nothing.
            exclusions.append(
                CohortExclusionRecord(
                    canonical_id=peer,
                    kind=kind,
                    reason=CohortExclusionReason.INSUFFICIENT_EVIDENCE,
                    rationale=(
                        "the projection shows nothing this subject shares with "
                        "any other candidate of its kind -- no portfolio, "
                        "owning team, dependency, initiative parent or measured "
                        "metric in common -- so there is no stated basis on "
                        "which to compare it"
                    ),
                )
            )
            continue
        # CHAOS-3667: comparability alone is no longer inclusion. A
        # candidate that shares a basis with a peer but shows no evidence of
        # the pressure the QUESTION is actually asking about is not the
        # answer to "which teams are struggling" merely for being similar
        # to one that is.
        corroboration = _corroboration(
            candidate_id=peer,
            family=question_family,
            readings=readings,
            dependency_concentrated=peer in concentrated,
        )
        if corroboration.corroborated is None:
            outlying = set(corroboration.outlying_metrics)
            checked_detail = ", ".join(
                f"{name} (elevated)" if name in outlying else name
                for name in corroboration.checked_metrics
            )
            single_signal_note = (
                f"; {', '.join(sorted(outlying))} is elevated on its own, but a "
                f"single elevated metric is not treated as corroborated pressure"
                if outlying
                else ""
            )
            exclusions.append(
                CohortExclusionRecord(
                    canonical_id=peer,
                    kind=kind,
                    reason=CohortExclusionReason.EXCLUDED_BY_QUESTION,
                    rationale=(
                        f"this {kind.value} is comparable to other candidates "
                        f"({', '.join(basis.value for basis in bases)}), and its "
                        f"metrics relevant to this question were checked -- "
                        f"{checked_detail} -- but fewer than "
                        f"{MIN_CORROBORATING_METRICS} sit clearly outside the "
                        f"cohort's own norm{single_signal_note}, so nothing "
                        "corroborates including it in this specific answer"
                    ),
                )
            )
            continue
        members.append(
            CohortCandidate(
                canonical_id=peer,
                kind=kind,
                display_label=label,
                basis_anchors=tuple(
                    (basis, tuple(sorted(bases[basis])))
                    for basis in sorted(bases, key=lambda item: item.value)
                ),
            )
        )

    truncated_count = max(0, len(members) - max_members) + max(
        0, len(exclusions) - max_exclusions
    )
    included = members[:max_members]
    exclusions = exclusions[:max_exclusions]

    dimensions = _supported_dimensions(
        [member.canonical_id for member in included], metrics, anchors
    )

    return CohortDiscovery(
        proposal=CohortProposal(
            # No subject was resolved and none is invented. The empty string
            # is inert here: ``entry_mode`` is what the packet builder reads,
            # and it refuses a committed subject in this mode rather than
            # looking for one.
            subject_id="",
            members=tuple(included),
            exclusions=tuple(exclusions),
            dimensions=dimensions,
            truncated=bool(truncated_count),
            truncated_count=truncated_count,
            authorization_filtered_count=len(withheld),
            entry_mode=CohortEntryMode.SCOPE_ENUMERATED,
        ),
        candidate_kinds=ordered_kinds,
        universe_size=len(candidate_ids),
        authorization_filtered_count=len(withheld),
    )


def _supported_dimensions(
    member_ids: Sequence[str],
    metrics: Mapping[str, set[str]],
    anchors: Mapping[str, Mapping[CohortInclusionBasis, set[str]]],
) -> tuple[ComparisonDimension, ...]:
    """The axes these members can actually be compared on.

    Derived from what the members HAVE, never from a fixed list attached to
    the basis. Two entities are comparable on delivery throughput when both
    carry a ``completed_items`` measurement -- one number is not a comparison,
    and a dimension nothing supports is a comparison the reader is left to
    perform, which is the dashboard-redirect fault in cohort clothing.

    ``DEPENDENCY_EXPOSURE`` is the one axis backed by relationships rather
    than measurements, so it is satisfied by two or more members depending on
    the same thing.
    """

    supported: set[ComparisonDimension] = set()
    counts: dict[ComparisonDimension, int] = {}
    for member in member_ids:
        seen: set[ComparisonDimension] = set()
        for metric in metrics.get(member, set()):
            dimension = METRIC_COMPARISON_DIMENSIONS.get(metric)
            if dimension is not None:
                seen.add(dimension)
        for dimension in seen:
            counts[dimension] = counts.get(dimension, 0) + 1
    supported.update(
        dimension
        for dimension, count in counts.items()
        if count >= 2  # noqa: PLR2004
    )

    depended: dict[str, set[str]] = {}
    for member in member_ids:
        for target in anchors.get(member, {}).get(
            CohortInclusionBasis.SHARED_DEPENDENCY, set()
        ):
            depended.setdefault(target, set()).add(member)
    if any(len(sources) >= 2 for sources in depended.values()):  # noqa: PLR2004
        supported.add(ComparisonDimension.DEPENDENCY_EXPOSURE)

    return tuple(sorted(supported, key=lambda item: item.value))
