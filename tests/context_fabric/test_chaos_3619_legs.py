"""CHAOS-3619: the two legs, and the two-field channel between them.

Binding conditions on the two-leg ruling, each asserted rather than promised:

1. the Leg B channel is EXACTLY the declared question family and comparison
   shape -- no subjects, no expected entities, no oracle or dimension
   metadata;
2. every Leg B native figure carries the "stronger than deployed" label;
3. Leg B is framed as the counterfactual it is -- the production fallback
   classifier operating perfectly -- and therefore as an UPPER BOUND;
4. the native A-to-B delta and the Leg B graph-versus-native comparison are
   both per family, never summed, never cross-leg aggregated;
5. records carry the leg id.

Condition 1 is the one that would be easiest to erode by accident: a builder
that took a ``CorpusCase`` could read anything on it, and a reviewer would
have to prove a negative about a whole object. The guard below reads the
module's AST instead, so an added attribute access fails loudly.
"""

from __future__ import annotations

import ast
import dataclasses
from pathlib import Path

import pytest

from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus.cases import CorpusCase
from trials.chaos_3619 import legs
from trials.chaos_3619.legs import (
    LEG_B_CHANNEL_FIELDS,
    LEG_B_NATIVE_LABEL,
    LegBChannel,
    LegId,
    leg_b_channel,
    reading_rule,
)
from trials.chaos_3619.records import ArmResult, CaseRecord, InterpretationDisposition

_LEGS_SOURCE = Path(legs.__file__)


class TestTheLegBChannelIsExactlyTwoFields:
    def test_the_channel_type_carries_exactly_the_permitted_fields(self) -> None:
        fields = tuple(f.name for f in dataclasses.fields(LegBChannel))
        assert fields == LEG_B_CHANNEL_FIELDS, (
            f"the Leg B channel now carries {fields}; the ruling permits only "
            f"{LEG_B_CHANNEL_FIELDS}. Anything else hands an arm part of the "
            "answer"
        )

    def test_the_builder_takes_values_not_a_case(self) -> None:
        """Over-reading is made impossible at the boundary, not detectable
        after it.

        A builder accepting a ``CorpusCase`` could read every field on it and
        nothing in the signature would say so.
        """

        tree = ast.parse(_LEGS_SOURCE.read_text())
        builder = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "leg_b_channel"
        )
        params = [
            a.arg
            for a in (
                *builder.args.args,
                *builder.args.kwonlyargs,
                *builder.args.posonlyargs,
            )
        ]
        assert tuple(params) == LEG_B_CHANNEL_FIELDS, params

    def test_the_module_reads_no_other_corpus_case_attribute(self) -> None:
        """The negative, read from the AST.

        Every attribute this module accesses is collected and checked against
        what a ``CorpusCase`` offers. If the module ever starts reading
        ``expected_answer``, ``scoring_dimension_ids``, ``principal_id`` or
        anything else the case carries, this fails -- which is the whole
        point of condition 1.
        """

        case_attributes = {f.name for f in dataclasses.fields(CorpusCase)}
        permitted = set(LEG_B_CHANNEL_FIELDS)
        tree = ast.parse(_LEGS_SOURCE.read_text())
        touched = {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        leaked = (touched & case_attributes) - permitted
        assert not leaked, (
            f"the Leg B module reads corpus-case attribute(s) {sorted(leaked)} "
            f"beyond the permitted {sorted(permitted)}; that hands an arm part "
            "of what it is scored against"
        )

    def test_the_channel_is_identical_for_both_arms(self) -> None:
        """One object, handed to both. Two builds could diverge; one cannot."""

        channel = leg_b_channel(
            question_family=QuestionFamilyID.PROJECT_STATUS_DRIVERS,
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
        )
        assert channel.question_family is QuestionFamilyID.PROJECT_STATUS_DRIVERS
        assert channel.comparison_shape is ComparisonShape.SINGULAR_SUBJECT


class TestTheLegBNativeLabel:
    def test_the_label_says_stronger_than_deployed(self) -> None:
        """The words matter: a reader must not take Leg B native as shipped
        behaviour."""

        assert "stronger than deployed" in LEG_B_NATIVE_LABEL

    def test_a_leg_b_native_row_can_carry_the_label(self) -> None:
        result = ArmResult(
            arm_id="native",
            disposition="scored",
            detail="scored",
            latency_ms=5,
            packet_emitted=True,
            figure_label=LEG_B_NATIVE_LABEL,
        )
        assert result.figure_label == LEG_B_NATIVE_LABEL


class TestLegBIsFramedAsAnUpperBound:
    """Condition 3. The framing is load-bearing for CHAOS-3621.

    Providing the declared family is the production fallback classifier
    operating PERFECTLY. Stating that converts unmeasured native headroom
    into a measured upper bound -- but only if it is stated AS an upper
    bound, because a real classifier would not be perfect.
    """

    def test_the_reading_rule_names_the_upper_bound(self) -> None:
        rule = reading_rule()
        assert "UPPER BOUND" in rule, rule

    def test_the_reading_rule_names_the_unwired_classifier_counterfactual(
        self,
    ) -> None:
        rule = reading_rule()
        assert "deliberately unwired" in rule
        assert "perfectly" in rule

    def test_the_reading_rule_forbids_aggregation_across_legs(self) -> None:
        rule = reading_rule()
        assert "never aggregated" in rule or "never summed" in rule

    def test_the_reading_rule_states_both_legs_questions(self) -> None:
        rule = reading_rule()
        assert "what does the product do today" in rule
        assert "graph assistance add" in rule


class TestRecordsCarryTheLeg:
    def test_a_case_record_carries_its_leg(self) -> None:
        record = CaseRecord(
            case_id="T01",
            question="q",
            question_family="struggling_teams",
            corpus_family="team_intelligence",
            comparison_shape="discovered_cohort",
            variant_kind="exact",
            expected_answer="direct",
            principal_id="principal_helio_analyst",
            organization_id="org_helio",
            declared_dimension_ids=("subject_top_1",),
            leg=LegId.AS_DEPLOYED.value,
        )
        assert record.leg == "leg_a_as_deployed"

    def test_the_two_legs_are_distinct(self) -> None:
        assert LegId.AS_DEPLOYED.value != LegId.JOB_HELD_CONSTANT.value


class TestTheInterpretationDispositionMakesTheConfoundAttributable:
    """The confound ruling's addition 1.

    Without a per-case interpretation outcome the confound conditions every
    figure and explains none: a reader cannot tell "native never recognised
    the question" from "native recognised it and still lacked the lineage".
    """

    def test_it_records_the_floor_rather_than_making_a_reader_recompute(
        self,
    ) -> None:
        """The floor is a module constant that can move.

        A record that stored only the confidence would silently change
        meaning the day the floor changed.
        """

        disposition = InterpretationDisposition(
            intent_id="bounded_investigation",
            confidence=0.4,
            below_fallback_floor=True,
            fallback_floor=0.6,
            derived_question_family=None,
        )
        assert disposition.fallback_floor == 0.6
        assert disposition.below_fallback_floor is True

    def test_an_unmapped_intent_records_no_family_rather_than_a_guess(
        self,
    ) -> None:
        """``None`` is the unprojectable case and must stay distinguishable
        from a family that happened to be derived."""

        disposition = InterpretationDisposition(
            intent_id="bounded_investigation",
            confidence=0.4,
            below_fallback_floor=True,
            fallback_floor=0.6,
            derived_question_family=None,
        )
        assert disposition.derived_question_family is None

    def test_the_zero_model_claim_is_visible_per_row(self) -> None:
        """Recorded per row rather than resting on a paragraph elsewhere."""

        disposition = InterpretationDisposition(
            intent_id="entity_status",
            confidence=0.9,
            below_fallback_floor=False,
            fallback_floor=0.6,
            derived_question_family="project_status_drivers",
        )
        assert disposition.classifier_consulted is False

    def test_the_real_constants_match_what_the_records_will_carry(self) -> None:
        """Anti-drift: the pinned floor must be the interpreter's own.

        A record set stating a floor the interpreter does not use would make
        every below/above judgment in the artifact wrong while looking
        internally consistent.
        """

        from dev_health_ops.api.dev.question_interpreter import (
            FALLBACK_CONFIDENCE_FLOOR,
        )

        assert FALLBACK_CONFIDENCE_FLOOR == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# The report renders both legs, and never lets an absence read as a pass
# ---------------------------------------------------------------------------


def _two_leg_payload() -> dict:
    """A minimal two-leg record set, as the report consumes it (plain data)."""

    def case(leg: str, case_id: str, corpus_family: str, native: dict) -> dict:
        return {
            "case_id": case_id,
            "question": "q",
            "question_family": "ambiguous_identity",
            "corpus_family": corpus_family,
            "comparison_shape": "singular_subject",
            "variant_kind": "natural",
            "expected_answer": "direct",
            "principal_id": "principal_helio_analyst",
            "organization_id": "org_helio",
            "declared_dimension_ids": ["subject_top_1"],
            "leg": leg,
            "arms": [
                native,
                {
                    "arm_id": "graph_assisted_shadow_arm",
                    "disposition": "scored",
                    "detail": "",
                    "latency_ms": 3,
                    "packet_emitted": True,
                    "dimension_outcomes": [
                        {
                            "dimension_id": "subject_top_1",
                            "verdict": "pass",
                            "detail": "",
                        }
                    ],
                    "interpretation": None,
                    "figure_label": "",
                },
            ],
        }

    unprojectable = {
        "arm_id": "native",
        "disposition": "arm_declared_gap",
        "detail": "no native family",
        "latency_ms": 1,
        "packet_emitted": False,
        "dimension_outcomes": [],
        "figure_label": "",
        "interpretation": {
            "intent_id": "bounded_investigation",
            "confidence": 0.4,
            "below_fallback_floor": True,
            "fallback_floor": 0.6,
            "derived_question_family": None,
            "classifier_consulted": False,
        },
    }
    handed = {
        "arm_id": "native",
        "disposition": "scored",
        "detail": "",
        "latency_ms": 2,
        "packet_emitted": True,
        "dimension_outcomes": [
            {"dimension_id": "subject_top_1", "verdict": "fail", "detail": ""}
        ],
        "figure_label": LEG_B_NATIVE_LABEL,
        "interpretation": None,
    }
    return {
        "schema_version": "chaos_3619_trial_results.v1",
        "binding": {
            "run_class": "measured",
            "tree_clean": True,
            "dependency_versions": {},
        },
        "cases": [
            case(LegId.AS_DEPLOYED.value, "A01", "adversarial_safety", unprojectable),
            case(LegId.JOB_HELD_CONSTANT.value, "A01", "adversarial_safety", handed),
        ],
        "non_authored": [],
    }


class TestTheReportRendersBothLegsHonestly:
    def test_each_leg_gets_its_own_section(self) -> None:
        from trials.chaos_3619.report import render_report

        rendered = render_report(_two_leg_payload())
        assert f"### Leg `{LegId.AS_DEPLOYED.value}`" in rendered
        assert f"### Leg `{LegId.JOB_HELD_CONSTANT.value}`" in rendered

    def test_every_leg_b_native_table_carries_the_label(self) -> None:
        """Condition 2, checked on the rendered document rather than trusted.

        A label defined and never emitted is the shape in which a caveat
        stops reaching the reader.
        """

        from trials.chaos_3619.report import render_report

        rendered = render_report(_two_leg_payload())
        leg_b = rendered.split(f"### Leg `{LegId.JOB_HELD_CONSTANT.value}`", 1)[1]
        assert LEG_B_NATIVE_LABEL in leg_b
        assert f"#### Arm `native` — {LEG_B_NATIVE_LABEL}" in leg_b

    def test_the_confound_section_separates_result_from_confound(self) -> None:
        """The ADR-bound distinction, per family, in the document itself."""

        from trials.chaos_3619.report import render_report

        rendered = render_report(_two_leg_payload())
        assert "a RESULT (reference resolution is the capability under trial)" in (
            rendered
        )
        assert "UNMEASURED NATIVE HEADROOM" in rendered
        assert "deliberately unwired" in rendered
        assert "not a starved baseline" in rendered
        assert "is a hard limit" in rendered

    def test_the_safety_column_callout_names_the_unmeasured_cases(self) -> None:
        """All nine adversarial-safety cases go unmeasured for native in Leg
        A. An unmeasured safety column and a clean one look identical."""

        from trials.chaos_3619.report import render_report

        rendered = render_report(_two_leg_payload())
        assert "Safety column: unmeasured is not clean" in rendered
        assert "Do not read this leg's native safety column as clean" in rendered
        assert "`A01`" in rendered

    def test_the_report_contains_no_cross_leg_total(self) -> None:
        """Condition 4/5: never summed, never cross-leg aggregated."""

        from trials.chaos_3619.report import render_report

        rendered = render_report(_two_leg_payload()).lower()
        for banned in ("overall score", "combined total", "aggregate score"):
            assert banned not in rendered, banned


# ---------------------------------------------------------------------------
# Unsound dimensions: computable is not publishable
# ---------------------------------------------------------------------------


class TestUnsoundDimensionsRenderNotMeasured:
    """A dimension whose INPUTS are defective must not publish a verdict.

    PR #1617's verifier measured 40% of graph-arm packets carrying an
    evidence entry whose entity_id contradicts the world record its handle
    names. `entity_sightings()` reads that field as a sighting, sightings
    feed the authorization audit, and the audit feeds
    ZERO_UNAUTHORIZED_RESULTS -- a MUST_BE_ZERO safety dimension. The verdict
    is computable and meaningless, and it can MASK a real leak by attributing
    a leaked entity's evidence to a permitted one.
    """

    def test_every_registry_entry_names_an_owner(self) -> None:
        """An unsound dimension with no owner is an untested one."""

        from trials.chaos_3619.unsound import UNSOUND_DIMENSIONS

        for entry in UNSOUND_DIMENSIONS:
            assert entry.owner.startswith("CHAOS-"), entry
            assert entry.arm_ids, f"{entry.dimension_id} suppresses no arm"
            assert len(entry.reason) > 80, "reason too thin to act on"

    def test_the_suppression_is_scoped_to_the_affected_arm(self) -> None:
        """A defect in one arm's emitter must not blank the other arm's
        column -- that would hide a real result behind someone else's bug."""

        from trials.chaos_3619.unsound import is_unsound

        assert is_unsound("zero_unauthorized_results", "graph_assisted_shadow_arm")
        assert not is_unsound("zero_unauthorized_results", "native")

    def test_an_unaffected_dimension_is_untouched(self) -> None:
        from trials.chaos_3619.unsound import is_unsound

        assert not is_unsound("subject_top_1", "graph_assisted_shadow_arm")

    def test_a_suppressed_cell_renders_not_measured_not_a_verdict(self) -> None:
        """The behaviour, on the rendered document.

        A PASS here would be the worst possible output: a MUST_BE_ZERO safety
        dimension reporting clean on inputs that cannot support the claim.
        """

        from trials.chaos_3619.report import render_report

        payload = {
            "schema_version": "chaos_3619_trial_results.v1",
            "binding": {
                "run_class": "measured",
                "tree_clean": True,
                "dependency_versions": {},
            },
            "cases": [
                {
                    "case_id": "A09",
                    "question": "q",
                    "question_family": "clarification_and_no_match",
                    "corpus_family": "adversarial_safety",
                    "comparison_shape": "singular_subject",
                    "variant_kind": "natural",
                    "expected_answer": "clarified",
                    "principal_id": "principal_helio_analyst",
                    "organization_id": "org_helio",
                    "declared_dimension_ids": ["zero_unauthorized_results"],
                    "leg": LegId.AS_DEPLOYED.value,
                    "arms": [
                        {
                            "arm_id": "graph_assisted_shadow_arm",
                            "disposition": "scored",
                            "detail": "",
                            "latency_ms": 1,
                            "packet_emitted": True,
                            "dimension_outcomes": [
                                {
                                    "dimension_id": "zero_unauthorized_results",
                                    "verdict": "pass",
                                    "detail": "",
                                }
                            ],
                            "interpretation": None,
                            "figure_label": "",
                        }
                    ],
                }
            ],
            "non_authored": [],
        }
        rendered = render_report(payload)
        assert "| x1 |" in rendered, (
            "a suppressed safety cell rendered a verdict; a MUST_BE_ZERO "
            "dimension reporting clean on defective inputs is the worst "
            "output this report could produce"
        )
        assert "| P1 |" not in rendered

    def test_the_registry_is_rendered_with_its_owner(self) -> None:
        """Suppression without attribution is indistinguishable from an
        oracle that simply never ran."""

        from trials.chaos_3619.report import render_report

        rendered = render_report(
            {
                "schema_version": "chaos_3619_trial_results.v1",
                "binding": {
                    "run_class": "measured",
                    "tree_clean": True,
                    "dependency_versions": {},
                },
                "cases": [],
                "non_authored": [],
            }
        )
        assert "NOT MEASURED because their inputs are defective" in rendered
        assert "CHAOS-3627" in rendered

    def test_an_empty_registry_still_renders_the_section(self) -> None:
        """Deleting the last entry must not delete the section.

        A vanishing section leaves a reader unable to tell "nothing is
        suppressed" from "this report predates the idea".
        """

        import trials.chaos_3619.report as report_module
        from trials.chaos_3619.report import _unsound_section

        original = report_module.UNSOUND_DIMENSIONS
        try:
            report_module.UNSOUND_DIMENSIONS = ()
            lines = "\n".join(_unsound_section())
        finally:
            report_module.UNSOUND_DIMENSIONS = original
        assert "None." in lines
        assert "the verdict the oracles actually returned" in lines
