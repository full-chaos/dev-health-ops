"""CHAOS-3377 defect 3: structural JSON-tail sanitization.

Fixed structurally (a real bracket-validity check over the trailing run of
JSON-structural characters), not by replacing the one literal reported
string, and NOT by a blanket "any trailing JSON-shaped punctuation" regex
either -- codex adversarial review (round 2) reproduced three cases where
that blanket rule corrupted legitimate content. These tests prove both
halves: the exact reported tail (and other genuinely INVALID shapes) are
still stripped, and the reviewer's three repro strings (and other genuinely
VALID bracket literals) are left untouched.
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
        ("No artifact at all.", "No artifact at all."),
        (
            "Ends in a real closing paren (see above)",
            "Ends in a real closing paren (see above)",
        ),
        ("Trailing brackets][", "Trailing brackets"),
        # A single dangling closing brace with nothing to match is exactly
        # as structurally invalid as a longer run -- the bracket-validity
        # check does not need a length threshold to catch it.
        ("The response is done}", "The response is done"),
    ],
)
def test_invalid_trailing_bracket_shapes_are_stripped(raw: str, expected: str) -> None:
    """The rule is structural (bracket-matching validity), so it generalizes
    past the one literal '}}}{' the ticket reported -- proving this is not a
    one-off string replace.
    """

    assert sanitize_model_text(raw) == expected


# --- codex adversarial review round 2: must-NOT-strip negative controls ---
# (the exact three repro strings that defeated the first revision's blanket
# "any trailing run of >=2 JSON-structural characters" rule)


@pytest.mark.parametrize(
    "text",
    [
        "Expected payload: {}",
        "Valid alternatives are []",
        "Use an empty object: { }",
    ],
)
def test_valid_balanced_bracket_examples_are_never_stripped(text: str) -> None:
    """Reviewer repros: each of these ends in a syntactically VALID,
    balanced, well-nested bracket literal that reads as a plausible inline
    example, not leaked debris. A blanket "trailing JSON-shaped punctuation"
    rule corrupted all three; the bracket-validity check must not.
    """

    assert sanitize_model_text(text) == text


def test_all_set_with_two_empty_object_literals_is_not_stripped() -> None:
    """Two back-to-back empty object literals are still validly bracketed
    (each opens and closes in turn) -- not the unbalanced, opens-nothing
    shape a leaked tail has.
    """

    text = "All set.{}{}"
    assert sanitize_model_text(text) == text


def test_does_not_touch_interior_braces_that_are_part_of_real_prose() -> None:
    text = "The payload is `{}` by default in every environment."
    assert sanitize_model_text(text) == text


def test_empty_and_none_like_inputs_are_returned_unchanged() -> None:
    assert sanitize_model_text("") == ""


def test_repeated_application_is_idempotent() -> None:
    text = "Verdict reached.}}}{"
    once = sanitize_model_text(text)
    twice = sanitize_model_text(once)
    assert once == twice


def test_no_trailing_structural_run_is_a_no_op() -> None:
    text = "A perfectly ordinary sentence with no trailing punctuation issue"
    assert sanitize_model_text(text) == text
