"""CHAOS-3537: the never-widen bound, re-expressed so it survives the wire.

CHAOS-3536 established that ``minimum``/``maximum`` never reached a provider:
``_structural_schema`` keeps only ``_STRUCTURAL_SCHEMA_KEYS``, and the
bounding keywords are not in it. The schema half of CHAOS-3389's never-widen
guarantee did not exist, and ``_verify`` was the sole guard.

This closes it WITHOUT touching the shared allowlist. ``enum`` is already on
it, so the bound is re-expressed as an explicit enumeration of the authorized
indices, which survives projection untouched and is strictly STRONGER than a
range: ``[0, n-1]`` admits every integer between the ends, while the enum
admits exactly the indices this call authorized.

Measured live against gpt-5-nano before implementing (CHAOS-3537 probe, 15
calls):

* strict mode ACCEPTS an integer enum, at 3 and at 50 candidates, and accepts
  the EMPTY enum used for the zero-candidate case;
* given a prompt that explicitly instructs it to return out-of-range index 7,
  the shipped (enum-less) schema returned ``selected_candidate_index: 7`` and
  ``candidate_indices: [7]`` at confidence 0.92, **3 runs out of 3**;
* the same prompt against the enum-bound schema could not express 7 in any of
  3 runs -- it returned null or an in-range index.

That is the whole case for this change, and note what it is NOT: ``_verify``
rejects index 7 either way, so this was never an authorization bypass. What
it was is a documented structural guarantee that did not exist, and a real
model demonstrably reaching for an unauthorized index when asked to.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from dev_health_ops.api.dev.qua_shadow import (
    QUAShadowConfig,
    QuestionUnderstandingShadow,
)
from dev_health_ops.llm.agent.openai_compatible import build_completion_request


def _shadow() -> QuestionUnderstandingShadow:
    return QuestionUnderstandingShadow(
        provider=None, scope_service=cast(Any, None), config=QUAShadowConfig()
    )


def _wire_mention_properties(
    *, candidate_count: int, mention_count: int = 1
) -> dict[str, Any]:
    """The mention schema as PRODUCTION sends it, after the projection."""

    generated = _shadow()._response_schema(
        mention_count=mention_count, candidate_count=candidate_count
    )
    request = build_completion_request(
        model="gpt-5-nano",
        messages=(),
        tools=(),
        response_schema=generated,
        max_output_tokens=6000,
    )
    schema = request["response_format"]["json_schema"]["schema"]
    qua = schema["properties"]["value"]["anyOf"][0]
    return cast(dict[str, Any], qua["properties"]["mentions"]["items"]["properties"])


@pytest.mark.parametrize("candidate_count", (1, 3, 25, 50))
def test_only_authorized_indices_are_expressible_on_the_wire(
    candidate_count: int,
) -> None:
    """The guarantee CHAOS-3389 documented, now actually true of the wire.

    Asserted on the PROJECTED schema, because the generator's output is not
    what production sends -- that distinction is the whole of CHAOS-3536.
    """

    properties = _wire_mention_properties(candidate_count=candidate_count)
    authorized = list(range(candidate_count))

    selected = properties["selected_candidate_index"]
    assert selected["enum"] == [*authorized, None], (
        "exactly the authorized indices, plus null for no_match"
    )
    assert properties["candidate_indices"]["items"]["enum"] == authorized


@pytest.mark.parametrize("candidate_count", (1, 3, 25, 50))
def test_the_index_after_the_last_authorized_one_is_not_expressible(
    candidate_count: int,
) -> None:
    """Stated as the attack rather than as the shape.

    The live probe's out-of-range index was reachable in the shipped schema
    3 runs out of 3. Whatever the enum's shape, the property that matters is
    that the next index up cannot be named.
    """

    properties = _wire_mention_properties(candidate_count=candidate_count)

    assert candidate_count not in properties["selected_candidate_index"]["enum"]
    assert candidate_count not in properties["candidate_indices"]["items"]["enum"]


def test_no_index_at_all_is_expressible_when_nothing_was_authorized() -> None:
    """The zero-candidate case, now closed on BOTH fields.

    CHAOS-3536 could only close ``selected_candidate_index`` (via
    ``{"type": "null"}``) and had to leave ``candidate_indices`` unbounded,
    because it is a non-optional tuple that a null would fail to parse and
    ``maxItems: 0`` is stripped by the projection.

    An EMPTY enum solves it: the array stays an array (so it parses as the
    empty tuple), and no element value is admissible. Verified live -- strict
    mode accepts the empty enum and the model returned ``[]`` on both runs.
    """

    properties = _wire_mention_properties(candidate_count=0)

    assert properties["selected_candidate_index"] == {"type": "null"}
    assert properties["candidate_indices"]["items"]["enum"] == [], (
        "with nothing authorized, no index may be admissible in the array "
        "either -- this is the residual CHAOS-3536 had to leave open"
    )


def test_no_match_stays_expressible() -> None:
    """The control: bounding must not remove the ability to decline.

    An enum of exactly the authorized indices, with no null, would force the
    model to select SOMETHING -- converting a never-widen fix into a
    must-always-guess bug, which is worse than what it replaced.
    """

    properties = _wire_mention_properties(candidate_count=3)

    assert None in properties["selected_candidate_index"]["enum"]
    assert "null" in properties["selected_candidate_index"]["type"]


def test_the_bound_is_per_call_and_tracks_the_shortlist() -> None:
    """Two different calls must not share a bound.

    The enum is rebuilt per call from that call's combined shortlist, so a
    smaller shortlist really is a tighter schema rather than a cached one.
    """

    small = _wire_mention_properties(candidate_count=2)
    large = _wire_mention_properties(candidate_count=9)

    assert small["selected_candidate_index"]["enum"] == [0, 1, None]
    assert large["selected_candidate_index"]["enum"] == [*range(9), None]


def test_the_bound_is_call_wide_not_per_mention() -> None:
    """Unchanged from CHAOS-3536, restated because enum does not fix it.

    ``_response_schema`` is still built once per call from the COMBINED
    shortlist, so a mention whose own slice is empty still sees every
    authorized index of the call. The enum narrows the call's index space to
    what the CALL authorized; only ``_verify`` narrows it to what the MENTION
    authorized. Both guards are needed and neither subsumes the other.
    """

    properties = _wire_mention_properties(candidate_count=50, mention_count=3)

    assert properties["selected_candidate_index"]["enum"] == [*range(50), None], (
        "a multi-mention call is bounded by the combined shortlist, not by "
        "any single mention's slice"
    )
