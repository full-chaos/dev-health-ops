"""CHAOS-3301 codex adversarial-review findings, converted to permanent tests.

Three findings were confirmed by execution against the merged CHAOS-3301
branch (report: `.claude/jobs/.../codex-review-chaos-3301.md`). Each was
reproduced fail-before against the pre-fix code (observed to match the
report's own recorded output byte-for-byte), fixed in ``src/``, and observed
pass-after. This file pins the pass-after state as a permanent regression
fence, following ``test_chaos_3301_controls.py``'s style: assert the real
seam (executed calls, persisted subject set), never a diagnostic string
alone.

* **HIGH** -- duplicate aliases of one entity falsely satisfied D2's
  ``>=2``-exact-match threshold (raw mention count, not distinct entities),
  letting an unresolved third mention slip past termination and the entity
  commit as a false-complete singular. Fixed in
  ``subject_preflight.py``'s D2 gate (dedup by ``(kind, canonical_id)``
  before comparing against 2).
* **HIGH** -- the uncapped mention-count check only summed typed grammar
  candidates, so 25 typed + 1 resolvable bare name silently truncated to a
  "complete" 25-subject set instead of a 26-subject oversized rejection.
  Fixed in ``question_interpreter.py``'s ``_add_untyped_mentions`` /
  ``interpret`` (uncapped total now includes untyped candidates).
* **MEDIUM** -- every unresolved outcome, including ``AMBIGUOUS_CANDIDATES``,
  was filed under ``unresolved_mention_ids`` with a generic warning. Fixed in
  ``subject_preflight.py``'s cohort-branch omission accounting (partitioned
  by outcome into ``unresolved_mention_ids`` vs ``ambiguous_mention_ids``).
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.subject_preflight import SUBJECT_BEARING_TOOLS
from tests._chaos_3301_subjects import (
    HALO_PROJECT,
    NOVA_PROJECT,
    RunOutput,
    SubjectSetRecorder,
    case_r1_duplicate_alias_with_unresolved_mention,
    case_r2_twentyfive_typed_plus_resolvable_bare_name,
    case_r3_ambiguous_mention_in_cohort,
)

DETERMINISM_ITERATIONS = 20


def _subject_bearing_calls(output: RunOutput) -> list[str]:
    return [
        request.tool_id.value
        for request in output.calls
        if request.tool_id in SUBJECT_BEARING_TOOLS
    ]


# ---------------------------------------------------------------------------
# R1 -- duplicate aliases must not falsely activate D2 (HIGH)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_r1_duplicate_alias_does_not_falsely_activate_d2() -> None:
    output = await case_r1_duplicate_alias_with_unresolved_mention()

    # The pre-D2 lowest-ordinal termination applies: with only one distinct
    # committed entity, the unresolved "Vesta" mention must terminate the
    # run rather than being silently omitted from a committed singular.
    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.error is not None
    assert output.result.error.code == "scope_not_found"
    assert _subject_bearing_calls(output) == []
    assert output.calls == []
    assert output.preflight_outcomes() == ("unresolved_no_authorized_match",)

    # No committed-subject state -- singular or cohort -- may be persisted
    # for a run that terminated instead of proceeding.
    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert output.recorder.subject_sets == []


@pytest.mark.asyncio
async def test_r1_is_deterministic_across_twenty_runs() -> None:
    results = [
        (await case_r1_duplicate_alias_with_unresolved_mention()).outcome_tuple()
        for _ in range(DETERMINISM_ITERATIONS)
    ]
    assert len(results) == DETERMINISM_ITERATIONS
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# R2 -- 25 typed + 1 resolvable bare name is 26, and must be rejected (HIGH)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_r2_25_typed_plus_bare_name_is_rejected_as_oversized() -> None:
    output = await case_r2_twentyfive_typed_plus_resolvable_bare_name()

    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled"
    assert _subject_bearing_calls(output) == []
    assert output.preflight_outcomes() == ("oversized_mention_set_in_v1",)

    # No 25-member set may be silently persisted as a "complete" cohort --
    # the whole point of the finding is that the 26th subject must never be
    # allowed to vanish into an apparently-complete smaller set.
    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert output.recorder.subject_sets == []


@pytest.mark.asyncio
async def test_r2_is_deterministic_across_twenty_runs() -> None:
    results = [
        (await case_r2_twentyfive_typed_plus_resolvable_bare_name()).outcome_tuple()
        for _ in range(DETERMINISM_ITERATIONS)
    ]
    assert len(results) == DETERMINISM_ITERATIONS
    assert len(set(results)) == 1


# ---------------------------------------------------------------------------
# R3 -- ambiguity is filed separately from no-match/unresolved (MEDIUM)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_r3_ambiguous_mention_is_filed_as_ambiguous_not_unresolved() -> None:
    output = await case_r3_ambiguous_mention_in_cohort()

    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled"
    assert _subject_bearing_calls(output) == []
    assert output.preflight_outcomes() == ("committed_cohort_v1_only",)

    assert output.recorder is not None
    assert isinstance(output.recorder, SubjectSetRecorder)
    assert len(output.recorder.subject_sets) == 1
    subject_set = output.recorder.subject_sets[0]

    committed_ids = {ref.entity_id for ref in subject_set.committed_entity_refs}
    assert committed_ids == {HALO_PROJECT.canonical_id, NOVA_PROJECT.canonical_id}
    assert subject_set.cohort_complete is False

    # The ambiguous "Atlas" mention must land in ambiguous_mention_ids, never
    # unresolved_mention_ids -- and the two fields must stay disjoint.
    assert subject_set.unresolved_mention_ids == ()
    assert len(subject_set.ambiguous_mention_ids) == 1
    assert not (
        set(subject_set.unresolved_mention_ids) & set(subject_set.ambiguous_mention_ids)
    )

    # The warning set must distinguish ambiguity from no-match, not reuse the
    # generic "could not be resolved" wording for both.
    assert any("ambiguous" in warning for warning in subject_set.warnings)


@pytest.mark.asyncio
async def test_r3_is_deterministic_across_twenty_runs() -> None:
    results = [
        (await case_r3_ambiguous_mention_in_cohort()).outcome_tuple()
        for _ in range(DETERMINISM_ITERATIONS)
    ]
    assert len(results) == DETERMINISM_ITERATIONS
    assert len(set(results)) == 1
