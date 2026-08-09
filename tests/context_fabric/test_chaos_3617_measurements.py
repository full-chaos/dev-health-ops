"""CHAOS-3617 PR2: the arm cites canonical numbers and never produces one.

The correction exists because CHAOS-3499 measured something adjacent to the
product question. Struggling-teams and capacity are the families the real
questions live in, so an arm that scored thin there for a *scope* reason
would hand the ADR a scope artifact dressed as a capability result — a softer
rerun of the same failure. Measurements are therefore in scope, in exactly
one shape: **cited, never computed.**

The load-bearing test in this module is not that the numbers appear. It is
:class:`TestTheArmPerformsNoArithmetic`, which scans the arm's own source for
arithmetic on measurement values — because "the arm does not compute" is a
property of the code, and asserting it about a handful of outputs would leave
every un-exercised path free to derive whatever it liked.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    AssertionBasis,
    DriverCategory,
    DriverExclusionReason,
    DriverStanding,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.drivers import (
    MEASUREMENT_CATEGORY,
    PERSON_COUNTING_METRICS,
    StandingMechanism,
    discover_drivers,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphObservationKind

_ARM_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "dev_health_ops"
    / "context_fabric"
    / "graph_arm"
)


@pytest.fixture(scope="module")
def helio():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


def _findings(projection, subject: str):
    grant = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
    readout = asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[subject],
            authorized_entity_ids=sorted(grant),
            max_hops=2,
        )
    )
    return {
        item.driver_id: item
        for item in discover_drivers(readout, subject, as_of=world.TRIAL_NOW)[0]
    }


# --------------------------------------------------------------------------
# The rule the whole batch rests on
# --------------------------------------------------------------------------


class TestTheArmPerformsNoArithmetic:
    """A property of the code, checked against the code.

    Asserting it about outputs would leave every path this suite does not
    exercise free to derive whatever it liked, and a derived number is
    indistinguishable from a cited one once it is in the packet.

    Scoped to the modules a canonical number passes through rather than
    matched by variable name. A name-based scan was tried first and fired on
    the hash embedder's vector normalisation in ``backend.py`` — arithmetic
    that has nothing to do with measurements — while still missing anything
    assigned to a local first. Scope plus operator is both narrower and
    stronger.

    **Two kinds of number, and the distinction is the guard.** Adversarial
    review found the scan covering ``corpus_adapter.py`` and ``drivers.py``
    while ``packet_builder.py`` — which assembles the packet — computed
    ``filtered_total = readout.authorization_filtered_count +
    cohort_authorization_filtered`` straight into a limitation string. Simply
    widening the scan would have banned that too, and it should not be
    banned:

    * a **canonical analytical measurement** is a claim about the
      organization — a cycle time, a review wait, a work-in-progress count.
      The arm may cite one verbatim and may never derive one. That is the
      whole correction: a number the arm computed carries a canonical
      service's authority without its evidence.
    * **operational metadata about the run itself** — how many results the
      authorization filter removed, how many candidates a bound dropped, what
      rank something came back at — is the arm describing its own behaviour.
      Deriving it is not merely allowed, it is required: the alternative is a
      packet that cannot say what it left out.

    So arithmetic is banned everywhere in these modules *except at named
    sites*, and :data:`OPERATIONAL_ARITHMETIC` is that list. A named site with
    a stated reason can be argued with in review; a quiet module-level
    exception cannot, because nobody sees it.
    """

    #: Every module a canonical measurement, or a packet carrying one, passes
    #: through. ``packet_builder.py`` is here because a number reaching the
    #: packet is a number reaching a consumer, whichever module minted it.
    MEASUREMENT_MODULES = ("corpus_adapter.py", "drivers.py", "packet_builder.py")

    #: Operators that can produce a NEW value from existing ones. ``Add`` is
    #: included: list concatenation uses it too, which is why the allowlist
    #: below names sites rather than operators.
    DERIVING_OPS = (
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.FloorDiv,
        ast.Mod,
        ast.Pow,
    )

    #: Names that identify a canonical measurement specifically. Bare
    #: ``value`` is deliberately absent — it appears all over the arm for
    #: unrelated reasons, and a guard that cries wolf gets relaxed.
    MEASUREMENT_NAMES = frozenset(
        {"measurement_value", "measurement_cohort_median", "cohort_median"}
    )

    #: ``(module, exact expression, why it is operational)``. The expression
    #: is matched verbatim against ``ast.unparse``, so a site that changes
    #: shape stops being permitted and has to be argued for again.
    #:
    #: Every entry here is either set algebra over identifiers — which yields
    #: a set, not a number — or a count of what the run itself did. None of
    #: them touches a measurement value, and a test below asserts that rather
    #: than trusting these sentences.
    OPERATIONAL_ARITHMETIC: tuple[tuple[str, str, str], ...] = (
        (
            "drivers.py",
            "_blocking_candidates(context, readout) + "
            "_open_child_candidates(context, readout) + "
            "_symptom_candidates(context, readout) + "
            "_measurement_candidates(context, readout)",
            "list concatenation of the four rule outputs; no operand is a number",
        ),
        (
            "packet_builder.py",
            "readout.authorization_filtered_count + cohort_authorization_filtered",
            "counts how many results the caller's own grant removed, from the "
            "traversal and the cohort; bookkeeping about this run, and the "
            "packet cannot disclose what it withheld without it",
        ),
        (
            "packet_builder.py",
            "set(ids) - set(handle_by_observation)",
            "set difference over evidence identifiers; yields the ids with no "
            "handle, not a number",
        ),
        (
            "packet_builder.py",
            "({member.canonical_id for member in cohort.members} | "
            "{exclusion.canonical_id for exclusion in cohort.exclusions}) - "
            "set(readout.authorized_entity_ids)",
            "set difference over canonical ids; yields the cohort entries "
            "outside the authorized set",
        ),
        (
            "packet_builder.py",
            "set(family.required_source_classes) - set(observed_classes)",
            "set difference over source classes; yields the ones this run "
            "never observed",
        ),
    )

    def _binops(self):
        """Every arithmetic site, counted once.

        Only *outermost* BinOps are yielded: ``a + b + c`` is one site, and
        walking every nested node would make the allowlist enumerate an
        expression's internal shape instead of naming the site.
        """

        for name in self.MEASUREMENT_MODULES:
            tree = ast.parse((_ARM_ROOT / name).read_text())
            nested = {
                id(operand)
                for node in ast.walk(tree)
                if isinstance(node, ast.BinOp)
                for operand in (node.left, node.right)
            }
            for node in ast.walk(tree):
                if isinstance(node, ast.BinOp) and id(node) not in nested:
                    yield name, node

    def _unnamed_arithmetic(self):
        permitted = {
            (module, expression)
            for module, expression, _ in self.OPERATIONAL_ARITHMETIC
        }
        return [
            f"{name}: {ast.unparse(node)[:110]}"
            for name, node in self._binops()
            if isinstance(node.op, self.DERIVING_OPS)
            and (name, ast.unparse(node)) not in permitted
        ]

    def test_every_arithmetic_site_is_named_on_the_operational_allowlist(self) -> None:
        """Citing is reading. Deriving is measuring, and measuring is theirs.

        ``31 against a cohort median of 14`` cites two canonical numbers.
        ``2.2x the median`` invents a third, and that third number is the arm
        measuring — the fault the whole correction exists to prevent, in its
        smallest possible form. An unnamed operator anywhere in these modules
        fails here, so it cannot be reintroduced through a local variable, a
        helper, or a module the scan used not to read.
        """

        offenders = self._unnamed_arithmetic()
        assert not offenders, (
            "a module that handles canonical measurements derives a number at "
            "a site the operational allowlist does not name. Either it is "
            "bookkeeping about the run, in which case add it to "
            "OPERATIONAL_ARITHMETIC with the reason, or it is an analytical "
            f"derivation and must not exist: {offenders}"
        )

    def test_no_allowlisted_site_touches_a_measurement(self) -> None:
        """The rule that stops the allowlist becoming the way around the ban.

        An entry may describe itself as operational in prose and still read a
        canonical number. This checks the expression rather than the sentence.
        """

        assert self.OPERATIONAL_ARITHMETIC, "an empty allowlist makes this vacuous"
        for module, expression, reason in self.OPERATIONAL_ARITHMETIC:
            assert reason.strip(), (module, expression)
            offending = sorted(
                name for name in self.MEASUREMENT_NAMES if name in expression
            )
            assert not offending, (module, expression, offending)

    def test_no_allowlist_entry_is_stale(self) -> None:
        """A permission for code that no longer exists is drift, not safety.

        Left in place it would silently pre-authorise whatever later happened
        to take the same shape at that site.
        """

        found = {(name, ast.unparse(node)) for name, node in self._binops()}
        stale = [
            (module, expression)
            for module, expression, _ in self.OPERATIONAL_ARITHMETIC
            if (module, expression) not in found
        ]
        assert not stale, stale

    def test_no_measurement_value_is_summed(self) -> None:
        """The measurement-name rule, which no allowlist entry can waive."""

        offenders = [
            f"{name}: {ast.unparse(node)[:70]}"
            for name, node in self._binops()
            if isinstance(node.op, self.DERIVING_OPS)
            and any(item in ast.unparse(node) for item in self.MEASUREMENT_NAMES)
        ]
        assert not offenders, offenders

    def test_the_scan_would_notice_a_planted_derivation(self) -> None:
        """Anti-vacuity, both halves. A scan matching nothing proves nothing."""

        planted = ast.parse(
            "ratio = measurement_value / cohort_median\n"
            "total = measurement_value + measurement_cohort_median\n"
        )
        derived = [
            node
            for node in ast.walk(planted)
            if isinstance(node, ast.BinOp) and isinstance(node.op, self.DERIVING_OPS)
        ]
        summed = [
            node
            for node in ast.walk(planted)
            if isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Add)
            and any(item in ast.unparse(node) for item in self.MEASUREMENT_NAMES)
        ]
        assert derived and summed

    def test_the_scan_actually_reads_those_modules(self) -> None:
        """A path typo would make every assertion above pass over nothing."""

        for name in self.MEASUREMENT_MODULES:
            assert (_ARM_ROOT / name).is_file(), name
        seen = {name for name, _ in self._binops()}
        assert seen == set(self.MEASUREMENT_MODULES), seen

    def test_comparison_is_not_arithmetic_and_is_still_allowed(self) -> None:
        """The boundary, stated so the guard is not read as banning too much.

        A comparison produces a boolean, not a number, and deciding whether
        a cited value sits on the worse side of its cited median is how the
        arm knows there is anything to report at all. Banning it would leave
        the arm unable to use a measurement for anything.
        """

        from dev_health_ops.context_fabric.graph_arm.drivers import _is_outlying

        assert _is_outlying("31", "14", "work_in_progress") is True
        assert _is_outlying("9", "18", "completed_items") is True
        assert _is_outlying("9", "18", "work_in_progress") is False


class TestNumbersReachThePacketVerbatim:
    def test_the_stored_value_is_byte_identical_to_the_corpus(self, helio) -> None:
        """No rounding, no scaling, no unit conversion in the adapter.

        A value this arm reshaped would be a number a canonical service never
        produced, presented under that service's authority.
        """

        stored = {
            node.canonical_id: node.attributes
            for node in helio.nodes
            if node.observation_kind is GraphObservationKind.MEASUREMENT
        }
        assert stored, "no measurement was ingested; this test is vacuous"
        checked = 0
        for measurement in world.WORLD_MEASUREMENTS:
            if measurement.tenant_id != world.ORG_HELIO:
                continue
            attributes = stored.get(measurement.measurement_key)
            if attributes is None:
                continue
            checked += 1
            assert attributes["measurement_value"] == measurement.value
            assert attributes["measurement_unit"] == measurement.unit
            assert attributes["measurement_evidence_slug"] == measurement.evidence_slug
            if measurement.cohort_median is not None:
                assert (
                    attributes["measurement_cohort_median"] == measurement.cohort_median
                )
        assert checked > 5, checked


# --------------------------------------------------------------------------
# The families the real questions live in
# --------------------------------------------------------------------------


class TestTheStrugglingTeamCase:
    def test_the_elevated_metrics_are_cited_with_a_measured_basis(self, helio) -> None:
        """``team_atlas``: WIP 31 against a cohort median of 14.

        The corpus's central struggling-team case. Every finding must carry
        a ``MEASURED`` basis, because a canonical service produced the number
        and minted evidence for it — and the contract only lets a measured
        basis exist when a handle backs it.
        """

        found = _findings(helio, "team_atlas")
        wip = found["drv_metric_atlas_wip"]
        assert wip.assertion_basis is AssertionBasis.MEASURED
        assert wip.mechanism == StandingMechanism.CITED_MEASUREMENT
        assert wip.category is DriverCategory.DELIVERY_PRESSURE
        assert wip.evidence_ids == ("atlas_wip",)

    def test_a_metric_below_its_median_in_the_wrong_direction_is_also_cited(
        self, helio
    ) -> None:
        """``completed_items`` 9 against a median of 18.

        Direction is per metric, never inferred. If one rule governed both,
        this and ``work_in_progress`` would be reported exactly backwards —
        and backwards is worse than silent, because a reader acts on it.
        """

        found = _findings(helio, "team_atlas")
        assert "drv_metric_atlas_completed" in found

    def test_a_metric_on_the_healthy_side_produces_no_finding(self, helio) -> None:
        """The control. Otherwise "every metric is cited" would pass.

        ``team_meridian``-style healthy values must not generate a finding
        at all — a cohort comparison that fires either way is not a
        comparison.
        """

        healthy = [
            measurement
            for measurement in world.WORLD_MEASUREMENTS
            if measurement.tenant_id == world.ORG_HELIO
            and measurement.cohort_median is not None
            and measurement.metric in MEASUREMENT_CATEGORY
            and measurement.entity_id != "team_atlas"
        ]
        assert healthy, "no comparable metric outside atlas; vacuous"
        for measurement in healthy:
            found = _findings(helio, measurement.entity_id)
            key = f"drv_metric_{measurement.measurement_key}"
            if key not in found:
                return
        pytest.fail("every comparable metric produced a finding; nothing is healthy")

    def test_a_cited_measurement_never_becomes_the_judgment(self, helio) -> None:
        """A number being high is a correlate, not a cause.

        This is the sharpest form of "the graph determines what is relevant;
        canonical services determine what is measurable": measurements enrich
        the packet and are capped at ``CANDIDATE_ONLY``, so the judgment
        still has to come from structure.
        """

        found = _findings(helio, "team_atlas")
        cited = [
            item
            for item in found.values()
            if item.mechanism == StandingMechanism.CITED_MEASUREMENT
        ]
        assert cited
        assert all(not item.is_asserted for item in cited)


class TestTheQualifiedCapacityCase:
    def test_a_number_with_no_cohort_comparison_says_so(self, helio) -> None:
        """``proj_solstice``: demand exists, a comparison does not.

        The corpus is explicit that the mismatch is still measurable while no
        allocation denominator exists. "You have the number and still cannot
        say whether it is unusual" is the honest state, and it is reported
        rather than dropped.
        """

        found = _findings(helio, "proj_solstice")
        completions = found["drv_metric_solstice_completions"]
        assert completions.standing is DriverStanding.EXCLUDED
        assert (
            completions.exclusion_reason
            is DriverExclusionReason.INSUFFICIENT_MEASUREMENT
        )

    def test_insufficient_measurement_is_now_genuinely_reachable(self, helio) -> None:
        """It was an unreachable branch before this batch, and was named as one.

        Retiring that admission is part of the batch: the reason is produced
        by a real corpus shape now, not merely present in an enum.
        """

        found = _findings(helio, "proj_solstice")
        assert any(
            item.exclusion_reason is DriverExclusionReason.INSUFFICIENT_MEASUREMENT
            for item in found.values()
        )


class TestTheNoEvidenceEitherDirectionCase:
    """``proj_tidal`` is a positive control, not a failure to avoid."""

    def test_a_subject_with_no_measurement_asserts_nothing(self, helio) -> None:
        """Neither allocation nor sufficient delivery signal.

        A staffing conclusion here has no evidence in *either* direction, so
        the honest output is no measurement finding at all — and the
        confidence machinery is only trustworthy if it produces this.
        """

        found = _findings(helio, "proj_tidal")
        cited = [
            item
            for item in found.values()
            if item.mechanism == StandingMechanism.CITED_MEASUREMENT
        ]
        assert cited == []
        assert not any(item.is_asserted for item in found.values())

    def test_the_absence_is_disclosed_rather_than_silent(self, helio) -> None:
        """Something must still tell the reader the sources were thin.

        The corpus attaches a source-health record for exactly this, and it
        surfaces as a reportable candidate. Silence and "we looked and the
        feed is thin" are different answers.
        """

        found = _findings(helio, "proj_tidal")
        assert found, "the subject produced nothing at all, disclosure included"


# --------------------------------------------------------------------------
# The person-level prohibition
# --------------------------------------------------------------------------


class TestNoPersonLevelClaimIsEverBuilt:
    def test_contributor_counts_never_become_a_driver(self, helio) -> None:
        """``proj_lattice``: eleven contributors ever, two in window.

        The corpus says outright that the raw roster is the misleading
        number. An aggregate count names nobody, so *citing* it is
        legitimate — but a driver whose subject IS a count of people is one
        inference away from naming them, and the contract bans person-level
        ranking outright.
        """

        assert PERSON_COUNTING_METRICS
        found = _findings(helio, "proj_lattice")
        for metric in PERSON_COUNTING_METRICS:
            assert not any(
                metric in item.summary_subject or metric in item.summary_detail
                for item in found.values()
            ), metric

    def test_the_person_filter_is_what_refuses_them(self, helio, monkeypatch) -> None:
        """Isolated from the category map, which also happens to reject them.

        Person-counting metrics carry no entry in ``MEASUREMENT_CATEGORY``,
        so a test on the corpus alone passes whether or not the person
        filter exists — the mutation that removed it SURVIVED, and was right
        to. This gives one of them a category, so the ONLY thing left
        refusing it is the filter under test.
        """

        from dev_health_ops.context_fabric.graph_arm import drivers

        metric = "contributors_ever"
        assert metric in PERSON_COUNTING_METRICS
        assert metric not in MEASUREMENT_CATEGORY, (
            "the metric is already categorised, so this no longer isolates "
            "the person filter"
        )
        monkeypatch.setitem(
            drivers.MEASUREMENT_CATEGORY,
            metric,
            DriverCategory.CAPACITY_OR_STAFFING,
        )
        found = _findings(helio, "proj_lattice")
        # Asserted over the FAMILY, not by naming an id. The corpus's
        # measurement keys are ``lattice_touching_contributors`` and
        # ``lattice_active_contributors``, and a first version of this test
        # guessed ``lattice_contributors`` — passing while proving nothing,
        # which is the same wrong-identifier mistake the harness caught in
        # the adjacency guard.
        person_findings = sorted(name for name in found if "contributor" in name)
        assert not person_findings, (
            f"a count of people became a driver once it had a category: "
            f"{person_findings}"
        )

        # Anti-vacuity, both halves: the patch really took, and the metric
        # really is present in this subject's readout, so the absence above
        # is the filter refusing it rather than the number being missing.
        assert drivers.MEASUREMENT_CATEGORY[metric] is (
            DriverCategory.CAPACITY_OR_STAFFING
        )
        stored = {
            node.attributes.get("measurement_metric")
            for node in helio.nodes
            if node.observation_kind is GraphObservationKind.MEASUREMENT
        }
        assert metric in stored

    def test_the_counts_are_still_ingested_and_readable(self, helio) -> None:
        """Refusing to *reason* about them is not refusing to carry them.

        Dropping the numbers at ingestion would make the arm look correct
        for the wrong reason, and would hide from a reader that the roster
        and the active set differ by nine.
        """

        stored = {
            node.attributes.get("measurement_metric")
            for node in helio.nodes
            if node.observation_kind is GraphObservationKind.MEASUREMENT
        }
        assert PERSON_COUNTING_METRICS <= stored

    def test_no_measurement_category_can_express_a_person(self) -> None:
        """Structural clearance for the verifier.

        Every category a citation can reach is a unit-of-work or system
        axis. There is no person-shaped category to reach for, so the
        prohibition does not rest on the metric list alone.
        """

        person_shaped = {
            category
            for category in MEASUREMENT_CATEGORY.values()
            if "person" in category.value or "individual" in category.value
        }
        assert not person_shaped
        assert MEASUREMENT_CATEGORY, "the map is empty; this is vacuous"
