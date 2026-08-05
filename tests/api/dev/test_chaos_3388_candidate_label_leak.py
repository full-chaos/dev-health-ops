"""CHAOS-3388 codex re-review (HIGH, confirmed): interpolated candidate
labels must never trip the internal-token denylist.

``preflight_outcomes.project_preflight_error`` interpolates real, catalog-
confirmed candidate display labels into the v1 ``DevError.safe_message`` for
a ``needs_clarification`` preflight termination (CHAOS-3388,
``_name_candidates``). Those labels are ordinary user/organization-authored
catalog text -- nothing stops one from containing a substring that happens to
collide with the closed internal-state vocabulary
(``no_match_terminal.INTERNAL_TOKEN_DENYLIST``), e.g. a project or issue
titled "...not_ready...".

Before this fix, the preflight TERMINATE branch persisted the richer
``needs_clarification`` frame (with its typed ``clarification_candidates``)
*before* calling ``orchestrator.finish()`` -- and ``finish()``'s fail-closed
leak scan built its ``attested`` allowlist from ``DevAnswer`` fields only
(``no_match_terminal.attested_strings``), which is always ``None`` on this
error-only path. So a candidate label containing a denylisted token was
scanned with no exemption, ``internal_token_leak_field`` flagged it, and
``finish()`` discarded the clarification and rewrote the terminal to a bare
``internal_error`` -- while the richer needs-clarification frame it had
already recorded a moment earlier was never touched, leaving the persisted
frame permanently inconsistent with the terminal state/error actually
written.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    run_preflight_orchestrator,
)

#: A real catalog label that happens to contain a denylisted internal token
#: ("not_ready" is a ``DevActualCompletion.state`` member, derived into
#: ``INTERNAL_TOKEN_DENYLIST``) -- exactly the shape of collision the
#: attestation escape hatch exists for elsewhere in this module (see
#: ``no_match_terminal.internal_token_leak``'s own docstring example).
_LEAKY_LABEL = "Provider not_ready Migration"

_LEAKY_ISSUE = AuthorizedEntity(
    kind=EntityKind.ISSUE,
    canonical_id="issue-provider-migration",
    label=_LEAKY_LABEL,
    repository_id=None,
)

#: Names no project in the fixture, so the named-project resolution
#: terminates ``NO_AUTHORIZED_MATCH`` and the CHAOS-3366 closest-matches
#: fallback runs a wide search for "Migration" -- a substring of
#: ``_LEAKY_LABEL`` -- and offers ``_LEAKY_ISSUE`` back as the sole
#: candidate.
_QUESTION = 'What is the status of the "Migration" project?'


@pytest.mark.asyncio
async def test_a_candidate_label_containing_a_denylisted_token_still_clarifies() -> (
    None
):
    """The terminal must stay a clarification, never internal_error."""

    output = await run_preflight_orchestrator(
        question=_QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, _LEAKY_ISSUE)],
        script_id="candidate-label-leak",
    )

    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE, (
        "a candidate label token collision must never fail-closed rewrite "
        f"the terminal to {output.result.state}"
    )
    assert output.result.error is not None
    assert output.result.error.code == "scope_ambiguous", (
        "the fail-closed guard must not have fired: got "
        f"{output.result.error.code!r} / {output.result.error.safe_message!r}"
    )
    assert output.result.error.code != "internal_error"
    # The candidate name itself must still reach the user -- proof the
    # attestation didn't just silently drop the candidate to dodge the scan.
    assert _LEAKY_LABEL in output.result.error.safe_message


@pytest.mark.asyncio
async def test_a_candidate_label_leak_never_flags_the_token_leak_counter() -> None:
    """No internal-token-leak signal fires for an authorized catalog label."""

    output = await run_preflight_orchestrator(
        question=_QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, _LEAKY_ISSUE)],
        script_id="candidate-label-leak-counter",
    )

    assert output.result.error is not None
    assert output.result.error.code != "internal_error"
    assert output.result.error.safe_message != "The request could not be completed."


@pytest.mark.asyncio
async def test_the_persisted_frame_stays_consistent_with_the_terminal() -> None:
    """The already-recorded needs-clarification frame must match the terminal.

    Before this fix, a leak false-positive on the candidate label rewrote the
    terminal to ``internal_error`` *after* the richer ``needs_clarification``
    frame had already been persisted -- leaving a run whose frame and
    terminal state permanently disagreed about what happened.
    """

    output = await run_preflight_orchestrator(
        question=_QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, _LEAKY_ISSUE)],
        script_id="candidate-label-leak-consistency",
    )

    assert output.recorder is not None
    assert len(output.recorder.frames) == 1
    frame = output.recorder.frames[0]
    assert frame.public_outcome == "needs_clarification"
    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.error is not None
    assert output.result.error.code == "scope_ambiguous"
