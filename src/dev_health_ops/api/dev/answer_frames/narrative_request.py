"""The narrative provider's bounded brief — plan §a P5.

"Narrative sees an allowlist, never the frame." A certified narrative
provider's request payload is built by this explicit projection over a
declared field allowlist, never by ``frame.model_dump()``. The provider gets
no tool registry and no field that is not classified here.

Mirrors the mechanism ``contracts_v2.no_answer_policy`` already uses for the
no-answer field-projection guardrail: every field of ``DevAnswerFrame`` is
classified in ``NARRATIVE_BRIEF_FIELD_POLICY``, and that classification is
checked for totality against the live model at import time
(``assert_narrative_brief_policy_is_total``, called at the bottom of this
module). A field added to ``DevAnswerFrame`` without a disposition here
breaks this module's import — a second, independent enforcement point from
``no_answer_policy``'s own totality check, deliberately not unified with it:
the two policies answer different questions (what a no-answer outcome may
disclose to the *user*, versus what any outcome may disclose to the
*narrative provider*) and a field can legitimately need different answers
to each (``facts`` is ``ABSENT`` under the no-answer policy but
``INCLUDED`` here, because the no-answer policy governs the five outcomes
that carry no facts at all, and the narrative brief is only ever built for
a frame that already has content).

CHAOS-3297 plan (e), "Frame shape and the 3298 freeze point": stack #3
landed embedded ``health_findings``/``deficiency_findings`` blocks
(``HealthRuleFinding``/``DeficiencyFinding``) alongside the pre-existing
opaque ``health_profile_refs``/``finding_refs``/``deficiency_refs``
pointers. All three ref fields stay ``EXCLUDED`` (opaque identifiers, no
narratable content). The two new embedded blocks are **also** ``EXCLUDED``,
deliberately more conservative than ``facts``/``metrics``/``conflicts``:
unlike those, no ``validators.validate_narrative_*`` function grounds a
narrative claim against finding content today (``grep health_findings
deficiency_findings contracts_v2/validators.py`` returns nothing) -- P5's
"the provider cannot author independent scope, outcome, numbers, evidence,
completion, readiness, or source coverage" guardrail is exactly the
category ``blast_radius``/``remediation``/``remediation_template`` fall
into, and admitting them to the brief without a corresponding grounding
check would let an unvalidated narrative describe a finding no check
verifies it got right. Revisit to ``INCLUDED`` only alongside a finding-
grounding validator, not before.

Sub-field note (orchestrator ruling, 2026-08-02): stack #3 is adding
``DevMetricRefV2.evidence_classification`` (closed vocabulary,
exclusive-or with ``evidence_ref_ids``; distinguishes a legacy-v1-sourced
metric's explicit unminted marker from a plan-minted metric that never sets
it). ``metrics`` stays ``INCLUDED`` as a *frame* field -- a narrative still
needs metric values/labels/comparisons to write grounded prose -- but
``evidence_classification`` itself is provenance metadata, not narrative
content, and is stripped per-metric by ``_project_metric_for_brief`` before
it can reach the provider. This is a targeted sub-field exclusion, not a
second totality-checked policy: ``metrics`` is a bounded, closed contract
(``DevMetricRefV2``) with a small field count, and the two provenance
fields it can carry (``evidence_ref_ids``, now also
``evidence_classification``) are named explicitly rather than projected
through an allowlist of their own.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame

__all__ = [
    "NARRATIVE_BRIEF_FIELD_POLICY",
    "NarrativeFieldDisposition",
    "assert_narrative_brief_policy_is_total",
    "build_narrative_brief",
]


class NarrativeFieldDisposition(StrEnum):
    """Whether one ``DevAnswerFrame`` field reaches the narrative provider."""

    #: Included verbatim (via ``model_dump(mode="json")``) in the brief.
    INCLUDED = "included"
    #: Never sent to the provider — internal correlation/provenance, or an
    #: opaque pointer with no narratable content.
    EXCLUDED = "excluded"


_INCLUDED = NarrativeFieldDisposition.INCLUDED
_EXCLUDED = NarrativeFieldDisposition.EXCLUDED

#: Every field of ``DevAnswerFrame``, classified. Checked for totality
#: against the model at import time by ``assert_narrative_brief_policy_is_total``.
NARRATIVE_BRIEF_FIELD_POLICY: dict[str, NarrativeFieldDisposition] = {
    # Correlation/provenance handles — no narrative value, and frame_id/
    # run_id are exactly the internal tokens guardrail (f) forbids in public
    # copy; there is no reason a provider ever needs them to write prose.
    "schema_version": _EXCLUDED,
    "frame_id": _EXCLUDED,
    "run_id": _EXCLUDED,
    "generated_at": _EXCLUDED,
    # Closed vocabulary, safe to disclose, and useful context: it tells the
    # provider which of the eight renderer sections it is writing prose for
    # (validate_narrative_readiness_claim and friends already assume the
    # provider knows this much).
    "public_outcome": _INCLUDED,
    # validate_narrative_subject_claim requires the narrative to name the
    # frame's committed subject by its canonical identity (display_label /
    # entity_id) -- the provider cannot satisfy that guard without seeing
    # subject_ref.
    "subject_ref": _INCLUDED,
    # An opaque cohort handle with no display content (see the module
    # docstring); nothing validates a narrative claim against it.
    "subject_set_ref": _EXCLUDED,
    # CHAOS-3325 disambiguation candidates: real entity display labels the
    # provider may need to name in a needs_clarification narrative.
    "clarification_candidates": _INCLUDED,
    # The direct deterministic verdict the narrative must never contradict.
    "direct_answer": _INCLUDED,
    # validate_narrative_numeric_containment binds every completion
    # percentage/ratio the narrative cites to this block.
    "completion": _INCLUDED,
    # validate_narrative_readiness_claim binds every ready/not-ready claim
    # to this block's state.
    "readiness": _INCLUDED,
    # The section structure narrative prose maps onto
    # (referenced_section_ids).
    "sections": _INCLUDED,
    # The fact structure narrative prose maps onto (referenced_fact_ids);
    # also the source of every numeral validate_narrative_numeric_containment
    # admits and the recommendation grounding
    # validate_narrative_recommendation_claim requires.
    "facts": _INCLUDED,
    # Bounded metric refs; comparison values a narrative may cite.
    "metrics": _INCLUDED,
    "comparisons": _INCLUDED,
    # Internal relationship-graph structure. A narrative is never validated
    # against a relationship path, and the raw path shape carries no prose
    # value -- excluded to keep the brief bounded (P5: "not the full tool
    # registry").
    "relationship_paths": _EXCLUDED,
    # Opaque pointer IDs with no narratable content.
    "health_profile_refs": _EXCLUDED,
    "finding_refs": _EXCLUDED,
    "deficiency_refs": _EXCLUDED,
    # CHAOS-3297 stack #3's embedded findings blocks -- EXCLUDED because no
    # narrative validator grounds a claim against them yet; see the module
    # docstring's "Frame shape and the 3298 freeze point" note.
    "health_findings": _EXCLUDED,
    "health_findings_truncated": _EXCLUDED,
    "deficiency_findings": _EXCLUDED,
    "deficiency_findings_truncated": _EXCLUDED,
    # Producer-authored conflict summaries the narrative may need to
    # reflect faithfully.
    "conflicts": _INCLUDED,
    # Disclosed limitations/qualifications the narrative may paraphrase but
    # never invent or omit.
    "limitations": _INCLUDED,
    # Internal per-step source-observation records -- not user-facing
    # content; the qualification signal they feed (limitations, coverage)
    # is already included in its own right.
    "source_observations": _EXCLUDED,
    # DevCoverageV2's source-availability detail is operational, not
    # narratable; the frame's readiness/completion blocks already carry the
    # user-facing consequence of any coverage gap.
    "coverage": _EXCLUDED,
    # Raw evidence handles -- the narrative cites facts, not evidence refs
    # directly; no validator checks a narrative claim against this field.
    "evidence": _EXCLUDED,
    # Server-composed follow-up copy the renderer emits on its own; the
    # narrative body does not need to be consistent with it.
    "safe_follow_up_questions": _EXCLUDED,
    # Internal platform provenance -- guardrail (f)'s exact reason a
    # no-answer frame drops this block entirely applies here too.
    "versions": _EXCLUDED,
}


def assert_narrative_brief_policy_is_total() -> None:
    """Raise unless every ``DevAnswerFrame`` field has a brief disposition.

    Called at import time (bottom of this module), mirroring
    ``no_answer_policy.assert_no_answer_policy_is_total`` — a field added to
    ``DevAnswerFrame`` without a disposition here breaks the package import
    rather than silently reaching (or silently never reaching) the
    narrative provider.
    """

    declared = set(NARRATIVE_BRIEF_FIELD_POLICY)
    actual = set(DevAnswerFrame.model_fields)
    unclassified = sorted(actual - declared)
    if unclassified:
        raise RuntimeError(
            f"DevAnswerFrame field(s) {unclassified} have no narrative-brief "
            "disposition; classify them in narrative_request."
            "NARRATIVE_BRIEF_FIELD_POLICY (INCLUDED / EXCLUDED)"
        )
    stale = sorted(declared - actual)
    if stale:
        raise RuntimeError(
            f"narrative-brief policy names removed DevAnswerFrame field(s) {stale}"
        )


#: Fields of ``DevMetricRefV2`` that never reach the narrative provider even
#: though the enclosing ``metrics`` frame field is ``INCLUDED`` -- provenance
#: about *how* a metric's evidence was sourced, not narratable content. See
#: the module docstring's "Sub-field note".
_METRIC_BRIEF_EXCLUDED_SUBFIELDS: frozenset[str] = frozenset(
    {"evidence_ref_ids", "evidence_classification"}
)


def _project_metric_for_brief(metric: dict[str, Any]) -> dict[str, Any]:
    """Strip provenance sub-fields from one dumped ``DevMetricRefV2``.

    ``dict.pop(key, None)`` on a key that does not exist yet (e.g.
    ``evidence_classification`` before stack #3 lands it) is a no-op, so
    this is safe to ship ahead of the field's landing and takes effect
    automatically once it exists -- no second edit required.
    """

    return {
        key: value
        for key, value in metric.items()
        if key not in _METRIC_BRIEF_EXCLUDED_SUBFIELDS
    }


def build_narrative_brief(frame: DevAnswerFrame) -> dict[str, Any]:
    """Project ``frame`` through the allowlist a narrative provider may see.

    Never ``frame.model_dump()`` restricted after the fact — the allowlist
    is the totality-checked table above, so a field cannot reach the
    provider by accident of dict iteration order or a missed exclusion.
    """

    dumped = frame.model_dump(mode="json")
    included = {
        name
        for name, disposition in NARRATIVE_BRIEF_FIELD_POLICY.items()
        if disposition is NarrativeFieldDisposition.INCLUDED
    }
    brief = {key: value for key, value in dumped.items() if key in included}
    if "metrics" in brief:
        brief["metrics"] = [_project_metric_for_brief(m) for m in brief["metrics"]]
    return brief


assert_narrative_brief_policy_is_total()
