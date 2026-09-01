"""The Python-side GraphQL document digest -- CHAOS-4697's hardest trap.

``cmd/query-api/internal/digest.Document`` (Go) computes:

    sha256(strings.TrimSpace(text))

The edge dispatcher must reproduce that EXACT digest for an incoming
request's query text, to look up ``go_api_routing_state`` before it knows
whether the operation is Go-owned at all. Reimplementing a digest
algorithm in a second language is precisely the defect class CHAOS-4696
documents (two corrections on that ticket, both from someone measuring the
wrong bytes) -- so this module does the narrowest possible thing, and pins
it against Go's own producer rather than trusting the reasoning below on
its own.

**The trap, measured, not assumed (2026-09-01):** Python's ``str.strip()``
uses ``str.isspace()``, which is NOT the same character class as Go's
``unicode.IsSpace``. Enumerating both classes over U+0000..U+3000 finds
exactly four divergent code points: **U+001C, U+001D, U+001E, U+001F**
(Information Separator Four/Three/Two/One). Python's ``isspace()`` treats
them as whitespace (they carry the Unicode bidirectional class "B",
which Python's definition includes); Go's ``unicode.IsSpace`` does not
(they are outside the ``White_Space`` property, which is what Go's
Latin-1 fast path and the ``unicode.White_Space`` range table both use).
Every other code point up to U+3000 (the highest value ``unicode.IsSpace``
recognises) agrees between the two languages.

So ``str.strip()`` is NOT safe here: a query text with a leading or
trailing U+001C-U+001F would trim differently in Python than in Go and
silently digest-miss. :func:`_go_trim_space` below enumerates Go's actual
whitespace set instead of leaning on Python's broader default -- see
``tests/api/graphql/test_go_api_document_digest.py`` for the enumeration
proof and a cross-language conformance test that pins this module's
output against ``cmd/query-api/internal/digest.Document`` over the real
12-document corpus (via ``registrydump``), the same shape
``tests/api/graphql/_go_schema_digest.py`` already uses for the schema
digest.

This is the ONE place in the Python edge that computes a document digest.
Do not reimplement this a second time.
"""

from __future__ import annotations

import hashlib

__all__ = ["document_digest"]

#: Go's `unicode.IsSpace` (see unicode/graphic.go): the ASCII/Latin-1 fast
#: path (tab, newline, vertical tab, form feed, carriage return, space,
#: NEL U+0085, NBSP U+00A0) plus every code point in the Unicode
#: `White_Space` property above Latin-1. Deliberately NOT Python's
#: `str.isspace()` set -- see the module docstring for the measured
#: divergence (U+001C-U+001F).
_GO_WHITESPACE_CODEPOINTS: frozenset[int] = frozenset(
    {
        0x09,
        0x0A,
        0x0B,
        0x0C,
        0x0D,
        0x20,
        0x85,
        0xA0,
        0x1680,
        0x2000,
        0x2001,
        0x2002,
        0x2003,
        0x2004,
        0x2005,
        0x2006,
        0x2007,
        0x2008,
        0x2009,
        0x200A,
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
    }
)


def _go_trim_space(text: str) -> str:
    """Reproduce Go's ``strings.TrimSpace`` byte-for-byte (well, rune-for-
    rune): strip leading/trailing runes in :data:`_GO_WHITESPACE_CODEPOINTS`
    only. Deliberately not ``text.strip()`` -- see the module docstring.
    """
    start = 0
    end = len(text)
    while start < end and ord(text[start]) in _GO_WHITESPACE_CODEPOINTS:
        start += 1
    while end > start and ord(text[end - 1]) in _GO_WHITESPACE_CODEPOINTS:
        end -= 1
    return text[start:end]


def document_digest(text: str) -> str:
    """The hex-encoded sha256 digest of a GraphQL document's text, using
    the SAME algorithm ``cmd/query-api/internal/digest.Document`` uses:
    ``sha256(TrimSpace(text))``, UTF-8 encoded.

    ``text`` must be the query text exactly as extracted from the
    request (JSON-decoded body field or URL-decoded query param) -- never
    re-printed/reformatted GraphQL. See this module's docstring and
    ``go_api_dispatcher.py``'s "forward verbatim" contract.
    """
    return hashlib.sha256(_go_trim_space(text).encode("utf-8")).hexdigest()
