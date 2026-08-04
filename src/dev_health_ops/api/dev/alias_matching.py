"""CHAOS-3388: acronym and parenthetical-alias forms for a catalog display name.

The CHAOS-3366 closest-matches fallback (``subject_preflight._close_matches``)
searches the catalog through a plain ``LIKE '%query%'`` substring match
(``scope_catalog.ClickHouseAuthorizedEntityCatalog``). That finds nothing for
a mention like "ACR" against a catalog project named "Dev Health Agent
Context Runtime (Context Fabric)": "acr" is not a contiguous substring of
that label, even though it is transparently the acronym of a phrase inside
it. This module is the pure, catalog-independent scoring layer that closes
that gap -- it never touches a database and never decides commit-vs-candidate
policy; callers own both.

Two distinct kinds of alias, scored differently by every caller in this
codebase (see ``scope_service.py``'s broadened literal-match check and
``subject_preflight.py``'s acronym-augmented closest-matches search):

* **Literal aliases** -- a parenthetical segment of the name is itself a real
  alternate name ("(Context Fabric)" -> "Context Fabric"), not a derived
  abbreviation. Typing it is exactly as deliberate an act as typing the
  primary label, so callers may treat an exact (casefolded) match the same
  way they already treat a literal label/id match.
* **Acronyms** -- the initials of a contiguous run of two or more words,
  taken over the primary name and over each literal alias in turn. This is
  always a *derived* signal, never a name the catalog itself asserts, so it
  is never eligible for the same auto-commit treatment as a literal alias:
  every caller must offer it as a candidate, never a pick (CHAOS-3289 history
  -- never guess into a wrong subject).

Every acronym window (not only the whole-name acronym) is generated because
a real display name routinely carries organization/product boilerplate a
user's shorthand omits: "Dev Health Agent Context Runtime" collapses to
"DHACR" as a whole, but a user who says "ACR" means the "Agent Context
Runtime" phrase inside it. Rather than guess which words are "boilerplate"
with a hand-maintained stopword list (itself a fabrication risk -- guessing
which words don't count), every contiguous window of length >= 2 is
generated; a single word's own initial is not, since that would let every
one-letter query collide with every name that happens to start with it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = [
    "NameAliasForms",
    "acronym_candidates",
    "alias_forms",
    "strip_parentheticals",
]

_PARENTHETICAL = re.compile(r"\(([^()]*)\)")
_WHITESPACE = re.compile(r"\s+")
#: A "word" for acronym purposes: a run of letters/digits, optionally
#: hyphenated/apostrophized internally ("dev-health" is one word). Bare
#: punctuation tokens contribute no initial and are dropped.
_WORD = re.compile(r"[A-Za-z0-9]+(?:['’-][A-Za-z0-9]+)*")

#: A name with more words than this contributes no acronym windows at all --
#: not a realistic catalog display name, and without a cap the O(n^2) window
#: enumeration below has no bound.
_MAX_WORDS_FOR_ACRONYMS = 16


def strip_parentheticals(name: str) -> tuple[str, tuple[str, ...]]:
    """Split ``name`` into its primary text and its parenthetical aliases.

    ``"Dev Health Agent Context Runtime (Context Fabric)"`` ->
    ``("Dev Health Agent Context Runtime", ("Context Fabric",))``. A name with
    no parentheses returns itself unchanged and an empty alias tuple; nested
    or multiple parenthetical groups are each returned as their own alias.
    """

    aliases = tuple(
        stripped
        for match in _PARENTHETICAL.finditer(name)
        if (stripped := match.group(1).strip())
    )
    primary = _WHITESPACE.sub(" ", _PARENTHETICAL.sub(" ", name)).strip()
    return primary, aliases


def _words(text: str) -> tuple[str, ...]:
    return tuple(_WORD.findall(text))


def acronym_candidates(text: str) -> frozenset[str]:
    """Every contiguous-window acronym of ``text``, uppercased.

    A window is >= 2 words, so a single-word name contributes nothing (its
    own initial is not a meaningful acronym). Every start/end pair is taken,
    not only the whole-text acronym, so "Agent Context Runtime" is generated
    as one candidate acronym ("ACR") of the longer name "Dev Health Agent
    Context Runtime" alongside the whole-name acronym ("DHACR") -- see the
    module docstring for why no stopword list is used to pick the "right"
    sub-phrase instead.
    """

    words = _words(text)[:_MAX_WORDS_FOR_ACRONYMS]
    count = len(words)
    if count < 2:
        return frozenset()
    return frozenset(
        "".join(word[0] for word in words[start:end]).upper()
        for start in range(count)
        for end in range(start + 2, count + 1)
    )


@dataclass(frozen=True, slots=True)
class NameAliasForms:
    """The alias vocabulary derived from one catalog display name.

    Both sets are casefolded so callers can compare directly against a
    casefolded, stripped user query without re-normalizing.
    """

    #: Parenthetical alias text(s), verbatim minus surrounding whitespace --
    #: eligible for the same literal-equality treatment as the primary label.
    literal_aliases: frozenset[str]
    #: Derived acronyms of the primary name and of each literal alias --
    #: candidate-only, never auto-commit eligible.
    acronyms: frozenset[str]


def alias_forms(name: str) -> NameAliasForms:
    """The literal-alias and acronym vocabularies for one catalog name."""

    primary, parenthetical_aliases = strip_parentheticals(name)
    acronyms = set(acronym_candidates(primary))
    for alias in parenthetical_aliases:
        acronyms.update(acronym_candidates(alias))
    return NameAliasForms(
        literal_aliases=frozenset(alias.casefold() for alias in parenthetical_aliases),
        acronyms=frozenset(acronym.casefold() for acronym in acronyms),
    )
