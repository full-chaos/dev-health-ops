"""Tests for the Python-side document digest (CHAOS-4697).

Two things must both be true for the edge dispatcher to look up the right
``go_api_routing_state`` row: (1) Python's whitespace-trim character class
must be pinned to Go's, not assumed equal, and (2) the resulting digest
must match Go's own producer over REAL registered document text, not a
hand-typed sample -- see ``go_api_document_digest.py``'s module docstring
for why (CHAOS-4696 is two corrections about exactly this class of
mistake).
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from dev_health_ops.api.graphql.go_api_document_digest import (
    _GO_WHITESPACE_CODEPOINTS,
    _go_trim_space,
    document_digest,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_ROUTE_GO = REPO_ROOT / "cmd" / "query-api" / "query_route.go"
REGISTRYDUMP_DIR = REPO_ROOT / "cmd" / "query-api" / "tools" / "registrydump"


# ---------------------------------------------------------------------------
# The measured Python/Go whitespace divergence (module docstring's claim,
# proven here rather than only asserted in prose).
# ---------------------------------------------------------------------------


def test_python_isspace_and_go_whitespace_diverge_exactly_on_information_separators():
    """Enumerates every code point Python's str.isspace() considers
    whitespace, up to U+3000 (the highest code point Go's unicode.IsSpace
    recognises), and asserts the ONLY divergence from
    _GO_WHITESPACE_CODEPOINTS is U+001C-U+001F (Information Separator
    Four/Three/Two/One) -- Python treats them as whitespace (Unicode
    bidirectional class "B"), Go does not (outside the White_Space
    property). If this test ever fails, the divergent set has changed and
    _GO_WHITESPACE_CODEPOINTS must be re-derived, not patched around.
    """
    divergent = {
        cp
        for cp in range(0x3001)
        if chr(cp).isspace() != (cp in _GO_WHITESPACE_CODEPOINTS)
    }
    assert divergent == {0x1C, 0x1D, 0x1E, 0x1F}


@pytest.mark.parametrize("codepoint", [0x1C, 0x1D, 0x1E, 0x1F])
def test_go_trim_space_does_not_strip_information_separators(codepoint: int):
    """The exact divergence: Python's str.strip() WOULD strip these; Go's
    TrimSpace does not, so neither may _go_trim_space."""
    ch = chr(codepoint)
    text = f"{ch}query Foo {{ bar }}{ch}"
    assert ch.isspace()  # sanity: Python really does call this whitespace
    trimmed = _go_trim_space(text)
    assert trimmed == text  # NOT stripped -- matches Go, not str.strip()
    assert trimmed != text.strip()  # str.strip() would have changed it


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  query Foo { bar }  ", "query Foo { bar }"),
        ("\t\nquery Foo { bar }\r\n", "query Foo { bar }"),
        (" query Foo { bar } ", "query Foo { bar }"),  # NBSP
        (" query Foo { bar } ", "query Foo { bar }"),  # line/para sep
        ("query Foo { bar }", "query Foo { bar }"),  # no-op
        ("   ", ""),  # all whitespace
    ],
)
def test_go_trim_space_ordinary_cases(raw: str, expected: str):
    assert _go_trim_space(raw) == expected


def test_document_digest_hashes_trimmed_utf8():
    assert document_digest("  hello  ") == hashlib.sha256(b"hello").hexdigest()


def test_document_digest_does_not_touch_internal_whitespace():
    # TrimSpace only trims the ENDS; internal whitespace (including
    # newlines inside a GraphQL document) must survive untouched, since
    # Go's algorithm only trims the ends too.
    text = "query Foo {\n  bar\n}"
    assert document_digest(f"  {text}  ") == hashlib.sha256(text.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Cross-language conformance: pin this module's output against Go's own
# producer over the REAL 12-document corpus (same shape as
# tests/api/graphql/_go_schema_digest.py's schema-digest producer).
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _registrydump_documents() -> list[dict[str, str]]:
    go = shutil.which("go")
    if go is None:
        pytest.skip("go toolchain not on PATH -- required for the conformance test")
    result = subprocess.run(
        [go, "run", str(REGISTRYDUMP_DIR), "-file", str(QUERY_ROUTE_GO)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOWORK": "off"},
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"registrydump failed:\nstdout={result.stdout}\nstderr={result.stderr}"
        )
    docs = json.loads(result.stdout)
    if not docs:
        raise RuntimeError("registrydump enumerated ZERO registered documents")
    return docs


def test_python_digest_matches_go_digest_over_the_real_corpus():
    """The conformance test the CHAOS-4697 brief demands: this module's
    document_digest() must equal cmd/query-api/internal/digest.Document's
    output (via registrydump, which calls that exact function -- see its
    own doc comment) for every one of the REAL registered documents, not
    a hand-typed sample.
    """
    docs = _registrydump_documents()
    assert len(docs) >= 12, f"expected at least 12 registered documents, got {docs}"
    mismatches = [
        (d["operation"], document_digest(d["document"]), d["digest"])
        for d in docs
        if document_digest(d["document"]) != d["digest"]
    ]
    assert mismatches == [], (
        f"Python digest disagreed with Go's producer for: {mismatches}"
    )


def test_document_digest_over_corpus_with_added_information_separator_padding():
    """Belt-and-braces: pad each real document with the exact divergent
    whitespace class (U+001C-U+001F) and confirm the padded digest is
    UNCHANGED by that padding only when Go would also leave it unchanged
    -- i.e. confirms _go_trim_space, not str.strip(), governs the padded
    case too, using real document text rather than a synthetic string.
    """
    docs = _registrydump_documents()
    for d in docs[:3]:  # a few real documents is enough; not a fuzz test
        text = d["document"]
        padded = f"\x1c{text}\x1d"
        # Padding with a non-Go-whitespace character must NOT match the
        # unpadded digest (Go would not trim it either) -- if Python
        # silently trimmed it (str.strip() semantics), the digests would
        # incorrectly collide.
        assert document_digest(padded) != document_digest(text)
