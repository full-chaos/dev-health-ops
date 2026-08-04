"""Unit coverage for the CHAOS-3388 acronym/parenthetical-alias scorer."""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.alias_matching import (
    acronym_candidates,
    alias_forms,
    strip_parentheticals,
)

# The real production catalog row this ticket's live repro named (CHAOS-3388,
# org 70d529e0-3c06-4597-8480-794fd02328b6): the fixture below is the actual
# ClickHouse `projects.name` value, read from the live dev environment, not a
# hand-authored stand-in.
_ACR_PROJECT_NAME = "Dev Health Agent Context Runtime (Context Fabric)"


def test_strip_parentheticals_splits_primary_from_alias() -> None:
    primary, aliases = strip_parentheticals(_ACR_PROJECT_NAME)
    assert primary == "Dev Health Agent Context Runtime"
    assert aliases == ("Context Fabric",)


def test_strip_parentheticals_is_a_no_op_without_parentheses() -> None:
    primary, aliases = strip_parentheticals("Nightfall")
    assert primary == "Nightfall"
    assert aliases == ()


def test_strip_parentheticals_handles_multiple_groups() -> None:
    primary, aliases = strip_parentheticals("Falcon (FLC) legacy (deprecated)")
    assert primary == "Falcon legacy"
    assert aliases == ("FLC", "deprecated")


def test_strip_parentheticals_drops_an_empty_group() -> None:
    primary, aliases = strip_parentheticals("Nightfall ()")
    assert primary == "Nightfall"
    assert aliases == ()


@pytest.mark.parametrize(
    ("text", "expected_member"),
    [
        ("Agent Context Runtime", "ACR"),
        ("Dev Health Agent Context Runtime", "DHACR"),
        # The window contained within the longer name, not only the
        # whole-name acronym -- this is the load-bearing property CHAOS-3388
        # needs: a user's shorthand for a sub-phrase of the full name.
        ("Dev Health Agent Context Runtime", "ACR"),
        ("Context Fabric", "CF"),
    ],
)
def test_acronym_candidates_contains_expected_window(
    text: str, expected_member: str
) -> None:
    assert expected_member in acronym_candidates(text)


def test_acronym_candidates_excludes_a_single_word() -> None:
    """A one-word name's own initial is not a meaningful acronym."""

    assert acronym_candidates("Nightfall") == frozenset()
    assert acronym_candidates("") == frozenset()


def test_acronym_candidates_is_bounded_by_word_count() -> None:
    huge = " ".join(f"Word{i}" for i in range(64))
    # Must not hang or explode combinatorially; a real display name is never
    # this long, so the cap is allowed to simply stop generating windows.
    candidates = acronym_candidates(huge)
    assert candidates  # some windows still generated within the cap
    assert all(len(candidate) <= 16 for candidate in candidates)


def test_alias_forms_names_the_real_repro_project() -> None:
    forms = alias_forms(_ACR_PROJECT_NAME)
    assert forms.literal_aliases == {"context fabric"}
    assert "acr" in forms.acronyms
    assert "cf" in forms.acronyms
    # The acronym set is never eligible as a literal alias and vice versa --
    # callers key auto-commit eligibility only off `literal_aliases`.
    assert "acr" not in forms.literal_aliases
    assert "context fabric" not in forms.acronyms


def test_alias_forms_of_a_plain_name_has_no_literal_aliases() -> None:
    forms = alias_forms("Nightfall")
    assert forms.literal_aliases == frozenset()
    assert forms.acronyms == frozenset()


def test_a_nonsense_query_matches_nothing_in_the_real_catalog_name() -> None:
    """CHAOS-3388 acceptance: "Zebra Project" must never fabricate a match."""

    forms = alias_forms(_ACR_PROJECT_NAME)
    assert "zebra" not in forms.acronyms
    assert "zebra project" not in forms.literal_aliases
    assert "zebra" not in forms.literal_aliases
