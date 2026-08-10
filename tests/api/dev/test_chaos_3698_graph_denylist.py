"""CHAOS-3698: the graph-assisted vocabulary is reachable in user-visible
prose the moment the graph-routing assembler (CHAOS-3502/3650) starts
building real ``DevAnswer``s from graph packets, and the fail-closed
internal-token backstop (``no_match_terminal.INTERNAL_TOKEN_DENYLIST``) must
cover it before that happens -- not after.

These are string-level negative controls, mirroring
``test_no_match_terminal.py``'s own convention (see its module docstring):
assert the LITERAL tokens a producer could echo are caught, rather than that
some mapping function exists. A test that only checks the union contains an
enum reference passes with the enum member renamed or the union silently
narrowed.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID
from dev_health_ops.api.dev.graph_investigation_query import (
    CohortDiscoveryFamily,
    GraphQueryOutcome,
)
from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    PacketLimitationKind,
)
from dev_health_ops.api.dev.no_match_terminal import (
    INTERNAL_TOKEN_DENYLIST,
    internal_token_leak,
    internal_token_leak_field,
)

#: Named by CHAOS-3698's own report: exactly the tokens that were 0 hits in
#: this module before the fix, written out here as literals -- never derived
#: from the module under test, per the same "a control that imports its own
#: expected string cannot fail when the code is wrong" reasoning
#: ``test_no_match_terminal.py`` states for the PRD tokens.
_CHAOS_3698_REPORTED_TOKENS = (
    "deadline_exceeded",
    "provider_failure",
    "team_pressure",
    "project_capacity",
    "discovered_cohort",
)


@pytest.mark.parametrize("token", _CHAOS_3698_REPORTED_TOKENS)
def test_the_reported_missing_tokens_are_now_in_the_denylist(token: str) -> None:
    assert token in INTERNAL_TOKEN_DENYLIST


@pytest.mark.parametrize(
    "member",
    [
        GraphQueryOutcome.DEADLINE_EXCEEDED,
        GraphQueryOutcome.PROVIDER_FAILURE,
        CohortDiscoveryFamily.TEAM_PRESSURE,
        CohortDiscoveryFamily.PROJECT_CAPACITY,
        QuestionIntentID.DISCOVERED_COHORT,
        ComparisonShape.DISCOVERED_COHORT,
        *PacketLimitationKind,
    ],
)
def test_every_underscore_bearing_graph_vocabulary_member_is_denylisted(
    member,
) -> None:
    """Every underscore-bearing member of the four graph-assisted enums this
    fix unions in, checked by iterating the LIVE enum classes rather than a
    hand-copied list -- if a member is renamed or a new one is added, this
    test walks it automatically instead of silently missing it.
    """

    if "_" not in member.value:
        pytest.skip(f"{member!r} has no underscore -- not in scope for the denylist")
    assert member.value in INTERNAL_TOKEN_DENYLIST


@pytest.mark.parametrize("token", _CHAOS_3698_REPORTED_TOKENS)
def test_internal_token_leak_catches_a_graph_token_in_model_authored_prose(
    token: str,
) -> None:
    """The actual backstop function, not just set membership: a sentence a
    model could plausibly author, naming the internal token inline, must be
    caught by :func:`internal_token_leak` -- the live call
    ``orchestrator.finish()`` makes over every terminal.
    """

    sentence = f"The result depends on {token} signals for this request."
    assert internal_token_leak([sentence]) == token


def test_internal_token_leak_field_names_the_field_for_a_graph_token() -> None:
    leaked = internal_token_leak_field(
        [("direct_summary", "This cohort was found via team_pressure discovery.")]
    )
    assert leaked == ("direct_summary", "team_pressure")


def test_packet_limitation_kind_disclosure_text_is_caught_if_echoed_raw() -> None:
    """A ``PacketLimitationKind`` member is graph-arm-internal vocabulary
    (``authorization_filtered``, ``truncated_traversal``, ...) that a
    producer could plausibly echo raw into a limitation sentence instead of
    translating it -- exactly the class of leak ``completion_truncation_
    detail`` was fixed for under CHAOS-3377. Any member reaching prose
    verbatim must be caught.
    """

    sentence = (
        "This answer is limited: truncated_traversal was reported by the graph arm."
    )
    assert internal_token_leak([sentence]) == "truncated_traversal"


def test_ordinary_prose_naming_no_graph_token_is_not_flagged() -> None:
    """Negative control, mirroring ``test_internal_token_leak_ignores_
    ordinary_prose`` in the sibling module: healthy prose about teams and
    projects must not trip the widened union."""

    sentence = (
        "The Platform team is under pressure this sprint due to project capacity."
    )
    assert internal_token_leak([sentence]) is None
