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

Two distinct kinds of alias, both scored here but held to the SAME
commit policy by every caller in this codebase (see ``scope_service.py``'s
exact-match check and ``subject_preflight.py``'s acronym-augmented
closest-matches search):

* **Literal aliases** -- a parenthetical segment of the name, split out
  mechanically from wherever a "(" / ")" pair appears in the label
  ("(Context Fabric)" -> "Context Fabric"). Some read as a genuine
  alternate name; others read as a qualifier on the primary label
  ("Payments (Legacy)" -> "Legacy", "Reports (Archived)" -> "Archived").
  Nothing about the catalog schema tells the two apart -- there is no
  explicit alias field, only a parenthesis this module splits on -- so a
  literal alias is exactly as *derived* a signal as an acronym is, despite
  reading as more "literal" to a person. CHAOS-3388 codex re-review (HIGH,
  confirmed): an earlier revision let a unique literal-alias match commit
  outright, which auto-committed "the Legacy project" onto whichever
  catalog entity happened to carry "(Legacy)" -- answering about an entity
  the user never actually named.
* **Acronyms** -- the initials of a contiguous run of two or more words,
  taken over the primary name and over each literal alias in turn. Also a
  *derived* signal, never a name the catalog itself asserts.

Neither kind is ever eligible for auto-commit: every caller must offer a
match on either one as a candidate, never a pick (CHAOS-3289 history --
never guess into a wrong subject). Should the catalog ever grow a real,
explicit alias field distinct from this label-splitting, that field's
values -- not this module's derived ``literal_aliases`` -- would be the
input eligible for a different policy.

**Amendment (CHAOS-3525, team-lead ruling 2026-08-07) -- an evidence-class
distinction, NOT a repeal.** The rule above is unchanged and permanent: a
DETERMINISTIC alias or acronym match is forbidden from auto-commit, forever.
Nothing in this module auto-commits, and no future caller of it may.

What the amendment adds is a different KIND of evidence, not a relaxation of
this one. The Question Understanding Agent's verified proposal
(``qua_promotion.py``) may commit over a candidate this module derived --
reached only after the deterministic layer has already declined, so the two
never compete for the same decision.

Recorded here, beside the rule, because a reader who finds only one of the two
would draw the wrong conclusion either way. The CHAOS-3289 incident was a
*derived string match* standing in for intent -- "the Legacy project"
committing onto whichever entity carried ``(Legacy)`` -- and a string overlap
is not evidence about what a person meant. That reasoning still holds exactly
as written, which is why the deterministic rule stands. A model that
has read the question and the candidate labels is a different class of
evidence, and it is gated as such: the deterministic layer must decline
first, the span must be structurally unambiguous -- it must have matched
exactly one authorized entity, and must name that entity rather than its
family (``qua_promotion._structurally_admissible``, CHAOS-3553) -- and the
entity is re-authorized against the catalog at commit time.

This paragraph used to name a confidence floor as the middle gate. CHAOS-3539
measured that floor over 336 rows and found it separates a correct commit
from a wrong one barely at all, so the claim is removed rather than softened:
a reader who believes a threshold is holding the line will reason about this
module's amendment on a guarantee that was never there. Crucially the commit is
never silent -- ``no_match_terminal.disclose_subject_match`` names the span
and the label in the user-facing answer, so a reader can catch exactly the
mistake 3289 produced. A derived alias may now be *selected*; it can never be
selected *quietly*.

How much of the 3289 shape that actually removes is stated precisely at
``qua_promotion._STRUCTURALLY_DISTINGUISHING_TOKENS``, and it is not all of
it: a single-word qualifier ("Legacy") and a qualifier's acronym ("LO" for
"Legacy Operations") are refused, but a MULTI-WORD qualifier named in full
("Legacy Operations") is admitted, because it is token-for-token identical to
a multi-word alternate name ("Context Fabric") that CHAOS-3525 requires
committing. That is this module's own "nothing about the catalog schema tells
the two apart", reaching its limit -- the explicit alias field named two
paragraphs above is what would close it, and disclosure is what bounds it
meanwhile.

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
from enum import StrEnum

__all__ = [
    "NameAliasForms",
    "SpanMatch",
    "SpanMatchClass",
    "acronym_candidates",
    "alias_forms",
    "classify_span_match",
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
    #: a *derived* signal like ``acronyms`` below (see the module docstring
    #: for why), never eligible for auto-commit, only for the candidate
    #: list.
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


class SpanMatchClass(StrEnum):
    """How a mention's span relates to one catalog label (CHAOS-3553).

    Deliberately a property of the PAIR ``(span, label)``, not a record of
    which search pass returned the row. The two are close but not the same,
    and the difference is what makes this useful: the alias pass
    (``scope_catalog._alias_matches``) returns parenthetical-alias and acronym
    hits in one undifferentiated bucket, and the substring pass returns an
    exact whole-label match and an incidental fragment in another. A consumer
    that needs to know whether the user NAMED the entity or merely brushed
    against its label cannot recover that from the bucket.

    Ordered from most to least specific; ``classify_span_match`` applies them
    in that order, so a span that qualifies two ways gets the stronger class.
    """

    #: The span is the whole label, modulo case and surrounding whitespace.
    EXACT_LABEL = "exact_label"
    #: The span equals a parenthetical segment of the label ("(Context
    #: Fabric)"). A *derived* signal -- see this module's docstring on why a
    #: parenthetical is no more authoritative than an acronym.
    ALIAS = "alias"
    #: The span equals an acronym of the label or of one of its parentheticals
    #: ("ACR" for "... Agent Context Runtime ..."). Also derived.
    ACRONYM = "acronym"
    #: None of the above: the label merely contains the span, which is every
    #: hit the catalog's ``LIKE '%query%'`` returns and nothing more.
    SUBSTRING_PARTIAL = "substring_partial"


@dataclass(frozen=True, slots=True)
class SpanMatch:
    """The typed provenance of one ``(span, label)`` pair.

    CHAOS-3422's lesson, quoted verbatim in
    ``scope_service._dedupe_preserving_rank``: "an ``AuthorizedEntity``
    carries no match provenance for a later layer to recover". That absence is
    why a downstream dedupe silently destroyed alias precedence -- the
    information had to be re-derived and the re-derivation was wrong. This
    type is that provenance, carried rather than re-derived.

    All three fields are computed together, from the same span, so a consumer
    can never pair a class from one query with a coverage count from another.

    ``match_class`` describes the match; the two fields beneath it are the
    independent CORROBORATIONS a consumer can weigh. They are kept separate
    from the class on purpose: ``match_class`` collapses a span that qualifies
    two ways down to the most specific one, and a policy that needs to know
    whether the OTHER way also held cannot recover it from the collapsed
    value. ``qua_promotion`` needs exactly that -- see
    ``is_acronym_of_primary_name``.
    """

    match_class: SpanMatchClass
    #: How many DISTINCT tokens of the label the span accounts for. Zero for a
    #: match that shares no word with the label at all -- which is the normal
    #: case for ``ACRONYM``, where the span is initials rather than words.
    label_tokens_covered: int
    #: Whether the span is derivable as an acronym of the label's PRIMARY
    #: NAME -- the label with its parentheticals and articles removed.
    #:
    #: Computed independently of ``match_class``, so it stays true for a span
    #: that ``match_class`` reports as ``ALIAS``. That overlap is the whole
    #: reason this field exists: a parenthetical is a literal fragment of the
    #: label so it wins the class, but "(MWA)" on "Meridian Web Application
    #: (MWA)" is ALSO the acronym of the primary name, while "(Legacy)" on
    #: "Payments (Legacy)" is the acronym of nothing. The label corroborates
    #: the first and says nothing about the second, and this module's
    #: docstring records that the difference between an alternate NAME and a
    #: mere QUALIFIER is real and that no catalog field marks it.
    #:
    #: **"Primary name" is load-bearing, not incidental phrasing.** An earlier
    #: revision tested against ``alias_forms(label).acronyms``, which unions
    #: the acronyms of the primary name with those of every parenthetical --
    #: so a MULTI-WORD qualifier corroborated itself. "LO" against "Payments
    #: (Legacy Operations)" was admitted on the strength of an acronym of
    #: "Legacy Operations", re-opening through two derivations the exact
    #: qualifier bug the field was added to close (adversarial review round 2,
    #: HIGH; reproduced before fixing). Corroboration has to come from the name
    #: the CATALOG asserts, never from another derived form -- an acronym of a
    #: parenthetical is doubly derived and corroborates nothing.
    #:
    #: Articles are dropped before deriving, so an article-only label ("The
    #: An") yields no acronyms and can corroborate nothing, rather than
    #: "corroborating" the initials of its own articles.
    is_acronym_of_primary_name: bool = False


#: Articles, excluded from ``label_tokens_covered`` on BOTH sides.
#:
#: This is not a stopword list and not a judgment about which words are
#: "boilerplate" -- this module refuses that guess for acronym windows and the
#: refusal stands. It repairs an ASYMMETRY that already exists in the
#: pipeline: ``question_interpreter`` strips a leading article from a mention
#: span, so the span side arrives article-free, while the catalog label side
#: never does. Counting a label's article as content the user "accounted for"
#: therefore measures a difference between two normalizations rather than
#: anything the user said.
#:
#: The asymmetry is not even consistent on the span side, which is how it was
#: found: "the Platform Team" yields the span "Platform", but "The Platform
#: Team" -- capitalised, so the extractor keeps it -- yields "The Platform".
#: The second covers ``{the, platform}`` of "The Platform Team" and would
#: clear a two-token bound having named ONE word of the entity, with the
#: entity's own distinguishing token ("Team") dropped by the extractor.
#: Reported by adversarial review (codex, HIGH) and reproduced before fixing.
#:
#: Deliberately articles ONLY, a closed grammatical class -- not prepositions,
#: not conjunctions, not "platform"/"service"/"team". Anything beyond this is
#: the salience guess the module docstring rules out.
_ARTICLES = frozenset({"the", "a", "an"})


def _distinct_tokens(text: str) -> frozenset[str]:
    return frozenset(word.casefold() for word in _words(text))


def _distinct_content_tokens(text: str) -> frozenset[str]:
    return _distinct_tokens(text) - _ARTICLES


def classify_span_match(*, span: str, label: str) -> SpanMatch:
    """Classify how ``span`` identifies ``label``, and how much of it it covers.

    Pure and catalog-independent, like everything else in this module: it
    decides nothing about commit policy, it only reports the shape of the
    match. ``qua_promotion`` is the layer that attaches consequences.

    The classification is computed from the pair rather than read off the
    search pass that produced the row, so it is stable whether or not
    ``include_alias_matches`` was set on the search -- a row found only by
    substring containment against a label whose parenthetical happens to equal
    the span really IS an alias match, and reporting it as incidental
    containment would understate it.

    One boundary worth stating because it is invisible from the outside: the
    catalog's per-kind SQL also matches on identifiers (``id``,
    ``project_key``, a repository's ``toString(id)``), not only on the label.
    A row found that way and whose label is unrelated to the span classifies
    as ``SUBSTRING_PARTIAL`` with low coverage, which is a conservative answer
    rather than a correct one. It costs nothing today: CHAOS-3525's recorded
    scope note bounds every consumer to the spans the deterministic
    interpreter extracts (title-case forms and acronyms, never slug or id
    forms), so an id-shaped span does not reach this function on the paths
    that read the result. Should that population ever widen, this is the line
    that needs an identifier class of its own.

    **Two tokenizer limits, both fail-closed, both stated rather than
    discovered later.** ``_WORD`` is ASCII-only and treats an internal hyphen
    as part of one word. So a non-Latin label ("Платформа Мобильная Служба")
    yields no tokens at all and a hyphenated span ("Dev-Health" against "Dev
    Health Platform") yields no shared tokens -- both report
    ``label_tokens_covered == 0``. Neither is a wrong-subject risk: zero
    coverage REFUSES, so the affected question falls through to ranked
    clarification instead of committing. What it costs is capability, not
    safety -- a correct non-Latin or hyphenated partial name asks for
    confirmation where an ASCII one would not. These are pre-existing
    properties of ``_WORD`` (``acronym_candidates`` has always had them), not
    introduced here, and widening them is its own ticket with its own
    evidence. Named by adversarial review (codex, MEDIUM) and reproduced.
    """

    normalized_span = span.strip().casefold()
    normalized_label = label.strip().casefold()
    span_tokens = _distinct_content_tokens(span)
    covered = len(
        {token for token in _distinct_content_tokens(label) if token in span_tokens}
    )
    forms = alias_forms(label)
    # Computed unconditionally, NOT inside the class ladder below: a
    # parenthetical that is also an acronym takes the ALIAS branch, and a
    # consumer that needs the acronym fact must still be able to see it.
    #
    # Derived from the PRIMARY name only, with articles dropped -- deliberately
    # NARROWER than ``forms.acronyms``, which also unions in the acronyms of
    # every parenthetical and would let a multi-word qualifier corroborate
    # itself ("LO" for "Payments (Legacy Operations)"). See the field's own
    # comment on ``SpanMatch``.
    primary, _ = strip_parentheticals(label)
    primary_content = " ".join(
        word for word in _words(primary) if word.casefold() not in _ARTICLES
    )
    is_acronym_of_primary = normalized_span in {
        acronym.casefold() for acronym in acronym_candidates(primary_content)
    }

    if normalized_span and normalized_span == normalized_label:
        match_class = SpanMatchClass.EXACT_LABEL
    elif normalized_span in forms.literal_aliases:
        match_class = SpanMatchClass.ALIAS
    elif normalized_span in forms.acronyms:
        # Still classified ACRONYM on the WIDER vocabulary: the class
        # describes the match, and an acronym of a parenthetical genuinely is
        # one. Only the CORROBORATION above is narrowed.
        match_class = SpanMatchClass.ACRONYM
    else:
        match_class = SpanMatchClass.SUBSTRING_PARTIAL
    return SpanMatch(
        match_class=match_class,
        label_tokens_covered=covered,
        is_acronym_of_primary_name=is_acronym_of_primary,
    )
