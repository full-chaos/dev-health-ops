"""CHAOS-3619: the report may not state a number its own records contradict.

The ``trials/chaos_3499`` precedent, and it exists for the reason that trial
learned the hard way: two rounds of adversarial review found stale figures in
a document a manual sweep had already "fixed" once. Manual sweeps keep
failing because the document is long and the numbers move every run, so the
sweep is replaced by a mechanical check -- every load-bearing claim is
re-derived from the committed records and compared.

The difference from that precedent, and it matters: CHAOS-3499's version ran
against a committed artifact and could only check the artifact that existed.
This one checks the RENDERER, over synthetic record sets built here, so the
guarantee holds for artifacts that do not exist yet -- including the measured
sweep, which cannot be written until CHAOS-3627's family lands. A renderer
proven to derive its claims from its input cannot produce a document that
disagrees with the input it was given.

What is checked is the *coupling*, not the wording. A test asserting exact
prose would be rewritten by the first person who improved a sentence, and
would then assert nothing. So each guard changes a number in the records and
requires the rendered document to change with it -- which is the only
property that actually protects a reader.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from trials.chaos_3619.legs import LEG_B_NATIVE_LABEL, LegId
from trials.chaos_3619.report import render_report


def _arm(
    arm_id: str,
    disposition: str,
    *,
    verdicts: tuple[tuple[str, str], ...] = (),
    interpretation: dict[str, Any] | None = None,
    label: str = "",
) -> dict[str, Any]:
    return {
        "arm_id": arm_id,
        "disposition": disposition,
        "detail": "",
        "latency_ms": 1,
        "packet_emitted": disposition == "scored",
        "dimension_outcomes": [
            {"dimension_id": d, "verdict": v, "detail": ""} for d, v in verdicts
        ],
        "interpretation": interpretation,
        "figure_label": label,
    }


def _case(
    case_id: str,
    leg: str,
    family: str,
    arms: list[dict[str, Any]],
    *,
    corpus_family: str = "team_intelligence",
    dimensions: tuple[str, ...] = ("subject_top_1",),
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "question": "q",
        "question_family": family,
        "corpus_family": corpus_family,
        "comparison_shape": "singular_subject",
        "variant_kind": "natural",
        "expected_answer": "direct",
        "principal_id": "principal_helio_analyst",
        "organization_id": "org_helio",
        "declared_dimension_ids": list(dimensions),
        "leg": leg,
        "arms": arms,
    }


def _payload(cases: list[dict[str, Any]], **binding: Any) -> dict[str, Any]:
    return {
        "schema_version": "chaos_3619_trial_results.v1",
        "binding": {
            "run_class": "measured",
            "tree_clean": True,
            "commit": "abc123",
            "feature_tip_commit": "def456",
            "dependency_versions": {"graphiti-core": "0.29.3"},
            **binding,
        },
        "cases": cases,
        "non_authored": [],
    }


_UNRECOGNIZED = {
    "intent_id": "bounded_investigation",
    "confidence": 0.4,
    "below_fallback_floor": True,
    "fallback_floor": 0.6,
    "derived_question_family": None,
    "classifier_consulted": False,
}
_RECOGNIZED = {
    "intent_id": "entity_status",
    "confidence": 0.9,
    "below_fallback_floor": False,
    "fallback_floor": 0.6,
    "derived_question_family": "project_status_drivers",
    "classifier_consulted": False,
}


def _confound_row(rendered: str, family: str) -> list[str]:
    """The confound table's row for one family, as cells."""

    for line in rendered.splitlines():
        if line.startswith(f"| `{family}` |") and "->" not in line:
            return [cell.strip() for cell in line.strip("|").split("|")]
    raise AssertionError(f"no confound row for {family!r} in:\n{rendered}")


class TestTheConfoundTableIsDerivedNotWritten:
    def test_the_below_floor_count_tracks_the_records(self) -> None:
        """Change the records, the document must change.

        This is the whole coupling. A hand-written "34 of 39" would satisfy
        any assertion about the words and none about the truth.
        """

        one_below = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [_arm("native", "arm_declared_gap", interpretation=_UNRECOGNIZED)],
                )
            ]
        )
        two_below = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [_arm("native", "arm_declared_gap", interpretation=_UNRECOGNIZED)],
                ),
                _case(
                    "T02",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [_arm("native", "arm_declared_gap", interpretation=_UNRECOGNIZED)],
                ),
            ]
        )
        first = _confound_row(render_report(one_below), "struggling_teams")
        second = _confound_row(render_report(two_below), "struggling_teams")
        assert first[1] == "1" and first[2] == "1"
        assert second[1] == "2" and second[2] == "2"

    def test_a_recognized_question_is_not_counted_below_the_floor(self) -> None:
        """The other direction. Without it the count could be 'every case'
        and both assertions above would still pass."""

        payload = _payload(
            [
                _case(
                    "S01",
                    LegId.AS_DEPLOYED.value,
                    "project_status_drivers",
                    [_arm("native", "scored", interpretation=_RECOGNIZED)],
                )
            ]
        )
        row = _confound_row(render_report(payload), "project_status_drivers")
        assert row[1] == "1", row
        assert row[2] == "0", "a recognized question was counted below the floor"
        assert row[3] == "0", "a mapped family was counted unprojectable"

    def test_leg_b_cases_do_not_contaminate_the_leg_a_confound_table(self) -> None:
        """The confound is a Leg A property.

        Counting Leg B rows would report interpretation failures for a leg
        that was handed the classification, which is the opposite of true.
        """

        payload = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [_arm("native", "arm_declared_gap", interpretation=_UNRECOGNIZED)],
                ),
                _case(
                    "T01",
                    LegId.JOB_HELD_CONSTANT.value,
                    "struggling_teams",
                    [
                        _arm(
                            "native",
                            "scored",
                            verdicts=(("subject_top_1", "fail"),),
                            label=LEG_B_NATIVE_LABEL,
                        )
                    ],
                ),
            ]
        )
        row = _confound_row(render_report(payload), "struggling_teams")
        assert row[1] == "1", (
            "the Leg A confound table counted a Leg B case; interpretation "
            "failures would be reported for a leg handed its classification"
        )


class TestTheFamilyDimensionTablesAreDerived:
    def test_a_verdict_change_moves_the_rendered_cell(self) -> None:
        passing = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [
                        _arm(
                            "graph_assisted_shadow_arm",
                            "scored",
                            verdicts=(("subject_top_1", "pass"),),
                        )
                    ],
                )
            ]
        )
        failing = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [
                        _arm(
                            "graph_assisted_shadow_arm",
                            "scored",
                            verdicts=(("subject_top_1", "fail"),),
                        )
                    ],
                )
            ]
        )
        assert "| P1 |" in render_report(passing)
        assert "| F1 |" in render_report(failing)

    def test_an_unscored_case_renders_as_not_measured_never_as_a_zero(
        self,
    ) -> None:
        """The single most consequential rendering rule.

        A NOT RUN cell and a failing cell must not look alike: one says the
        arm was wrong, the other says nobody looked.
        """

        payload = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [_arm("native", "not_run_timeout")],
                )
            ]
        )
        rendered = render_report(payload)
        assert "| x1 |" in rendered, rendered
        assert "| F1 |" not in rendered, (
            "an unmeasured case rendered as a failure; the report claims a "
            "measurement that never happened"
        )

    def test_the_legend_is_emitted_with_the_tables(self) -> None:
        """A token set whose meaning lives in someone's head is not a legend."""

        rendered = render_report(
            _payload(
                [
                    _case(
                        "T01",
                        LegId.AS_DEPLOYED.value,
                        "struggling_teams",
                        [_arm("native", "not_run_timeout")],
                    )
                ]
            )
        )
        assert "NOT a zero" in rendered
        assert "this is not a pass" in rendered


class TestTheBindingClaimsAreDerived:
    def test_the_rendered_binding_reports_the_records_commit(self) -> None:
        rendered = render_report(_payload([], commit="deadbeef"))
        assert "deadbeef" in rendered

    def test_a_dirty_tree_is_disclosed(self) -> None:
        """A dirty-tree run is legitimate; citing it as reproducible is not."""

        rendered = render_report(_payload([], tree_clean=False))
        assert "not clean" in rendered

    def test_a_void_run_is_banner_marked_and_cannot_read_as_a_result(
        self,
    ) -> None:
        rendered = render_report(_payload([], run_class="smoke_void"))
        assert "THIS IS NOT A TRIAL RESULT" in rendered

    def test_a_measured_run_carries_no_void_banner(self) -> None:
        """The control: the banner must be conditional, not decorative."""

        rendered = render_report(_payload([]))
        assert "THIS IS NOT A TRIAL RESULT" not in rendered


class TestTheReportStatesNoAggregate:
    """The anti-aggregate rule, applied to the PROSE as well as the fields.

    A sentence saying "the graph arm won 31 of 39" is the same violation as a
    ``total`` field, in a form that is easier to quote.
    """

    @pytest.mark.parametrize(
        "banned",
        ["overall score", "aggregate score", "combined total", "final score"],
    )
    def test_no_headline_phrasing_appears(self, banned: str) -> None:
        payload = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [
                        _arm(
                            "graph_assisted_shadow_arm",
                            "scored",
                            verdicts=(("subject_top_1", "pass"),),
                        )
                    ],
                )
            ]
        )
        assert banned not in render_report(payload).lower()

    def test_no_percentage_is_rendered_anywhere(self) -> None:
        """A ratio is one division from being read as a score.

        Cells are counts. A percent sign in the document means somebody
        started summarising.
        """

        payload = _payload(
            [
                _case(
                    "T01",
                    LegId.AS_DEPLOYED.value,
                    "struggling_teams",
                    [
                        _arm(
                            "graph_assisted_shadow_arm",
                            "scored",
                            verdicts=(("subject_top_1", "pass"),),
                        )
                    ],
                )
            ]
        )
        rendered = render_report(payload)
        assert not re.search(r"\d\s*%", rendered), rendered
