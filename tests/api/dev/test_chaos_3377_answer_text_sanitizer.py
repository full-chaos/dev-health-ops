"""CHAOS-3377 defect 3: structural JSON-tail sanitization.

Fixed structurally (a trailing-artifact regex applied to any model-authored
text), not by replacing the one literal reported string -- these tests prove
that: the exact reported tail is covered, but so are shapes that are not a
literal match for it.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.answer_text_sanitizer import sanitize_model_text


def test_the_reported_live_tail_is_stripped() -> None:
    text = "The project is 39 of 69 required items complete.}}}{"
    assert (
        sanitize_model_text(text) == "The project is 39 of 69 required items complete."
    )


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Done for now.} }", "Done for now."),
        ("Verdict reached.]}", "Verdict reached."),
        ("All set.{}{}", "All set."),
        ("No artifact at all.", "No artifact at all."),
        (
            "Ends in a real closing paren (see above)",
            "Ends in a real closing paren (see above)",
        ),
        ("Trailing brackets][", "Trailing brackets"),
    ],
)
def test_generic_trailing_artifact_shapes_are_stripped_not_just_the_literal_one(
    raw: str, expected: str
) -> None:
    """The rule is structural (a trailing run of >=2 JSON-structural
    characters), so it generalizes past the one literal '}}}{' the ticket
    reported -- proving this is not a one-off string replace.
    """

    assert sanitize_model_text(raw) == expected


def test_does_not_touch_interior_braces_that_are_part_of_real_prose() -> None:
    text = "The payload is `{}` by default in every environment."
    assert sanitize_model_text(text) == text


def test_single_trailing_brace_is_left_alone() -> None:
    """A single trailing structural character is not, by itself, a strong
    enough signal of a parser-boundary leak (the run threshold is >=2) --
    this is the deliberate false-positive boundary, not a gap.
    """

    text = "The response is done}"
    assert sanitize_model_text(text) == text


def test_empty_and_none_like_inputs_are_returned_unchanged() -> None:
    assert sanitize_model_text("") == ""


def test_repeated_application_is_idempotent() -> None:
    text = "Verdict reached.}}}{"
    once = sanitize_model_text(text)
    twice = sanitize_model_text(once)
    assert once == twice
