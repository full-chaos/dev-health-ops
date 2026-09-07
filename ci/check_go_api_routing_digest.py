#!/usr/bin/env python3
"""Fail when the Go-API schema digest moves without being written down.

Every ``go_api_routing_state`` row is keyed by the canonical schema digest
of ``contracts/graphql/v1/schema.graphql``. Change that file by one byte
and every existing routing row stops matching: the Python dispatcher's
lookup misses, ``PostgresSwitch.Enabled`` returns false, and every request
falls back to Python. Correctly, safely -- and completely silently.

That is not a hypothetical. On 2026-09-01, PR #2065 (``33b3f3f21d``) moved
the digest from ``sha256:67b87d38…`` to ``sha256:29d509cd…`` hours after
twelve ``canary``/``100`` rows had been seeded at the old value. All twelve
died on that commit. No test, no log line, no metric and no review comment
noticed. It was found six days later.

This checker makes the SDL move impossible to land unnoticed. It enforces
two things, and the second is the one that matters:

1. ``contracts/graphql/v1/schema-digest.json`` matches the digest computed
   from the SDL right now. A bare SDL edit fails here.
2. That pinned digest appears in the "Schema-digest history" table of
   ``docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md``.
   So mechanically bumping the pin to make check 1 pass ALSO fails, until
   someone writes down what changed -- which is the point at which they
   read the recovery procedure sitting directly above that table.

Deliberately import-light: stdlib only, no ``dev_health_ops`` import. It
must run in a bare CI step, and it reproduces the digest algorithm in the
one place duplication is acceptable -- an independent second implementation
is what gives the check its value. (``go_api_schema_digest.py`` is the
runtime producer; this is the auditor. If they ever disagree,
``tests/api/graphql/test_go_api_schema_digest.py`` says so.)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

SDL_RELATIVE = Path("contracts/graphql/v1/schema.graphql")
PIN_RELATIVE = Path("contracts/graphql/v1/schema-digest.json")
HISTORY_DOC_RELATIVE = Path(
    "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md"
)
HISTORY_HEADING = "### Schema-digest history"

REGENERATE_HINT = (
    '  1. Update {pin} -> "schema_digest": "{actual}"\n'
    "  2. Add a dated row for {actual} to the '{heading}' table in {doc}\n"
    "     (the recovery procedure an operator needs is the section above it)\n"
    "  3. Re-enable routing AFTER the query-api image is rebuilt from this\n"
    "     SDL:  dev-hops go-api routing enable --operations all-registered \\\n"
    "             --candidate-build <ops-sha> --mode canary\n"
    "     Then confirm with:  dev-hops go-api routing status\n"
    "  4. Re-run this checker:  python ci/check_go_api_routing_digest.py --root ."
)


def compute_schema_digest(sdl_path: Path) -> str:
    """``"sha256:" + hex(sha256(raw bytes))`` -- the same algorithm
    ``cmd/query-api/internal/digest.Schema`` and
    ``api/graphql/go_api_schema_digest.current_schema_digest`` use. No
    parsing and no reprinting on any side, so a Python read and a Go
    ``go:embed`` read of identical bytes cannot disagree.
    """
    return "sha256:" + hashlib.sha256(sdl_path.read_bytes()).hexdigest()


def _leading_fence_run(stripped_line: str) -> tuple[str | None, int]:
    """``("`", 4)`` for a line opening/closing with ```` ```` ````, else
    ``(None, 0)``. Recognises both ``` and ~~~ fences, and reports the run
    LENGTH so nesting can be tracked."""
    for char in ("`", "~"):
        if stripped_line.startswith(char * 3):
            run = len(stripped_line) - len(stripped_line.lstrip(char))
            return char, run
    return None, 0


def _digest_appears_in_a_history_row(history_section: str, digest: str) -> bool:
    """True iff ``digest`` appears in a row of THE history table.

    "A line starting with ``|``" is not enough (codex r2, P2): a fenced
    code block containing a pipe-prefixed line satisfied that, and so did a
    row of an unrelated table added further down the page. Both would let
    someone tick the gate without recording anything an operator can read
    as history, which is the one thing this check exists to force.

    So the search is bounded three ways:

    * it stops at the next markdown heading, so only THIS section counts;
    * fenced code blocks are skipped, so an example is not a record;
    * the row needs at least the four cells the table declares (digest, in
      force from, moved by, notes), all non-empty -- a ``| digest | | | |``
      stub records nothing either.
    """
    # Fence tracking by CHARACTER AND LENGTH, not a boolean (codex r3, P1).
    # A toggle cannot represent nesting: ```` opening, ``` opening, ```
    # closing, ```` closing flipped the flag back to "outside" halfway
    # through, so an example row inside the nested block satisfied the gate.
    # CommonMark's rule is that a fence closes only on a run of the SAME
    # character at least as long as the opener, which is exactly what is
    # needed here.
    fence_char: str | None = None
    fence_len = 0
    for line in history_section.splitlines():
        stripped = line.strip()
        run_char, run_len = _leading_fence_run(stripped)
        if run_char is not None:
            if fence_char is None:
                fence_char, fence_len = run_char, run_len
                continue
            if run_char == fence_char and run_len >= fence_len:
                fence_char, fence_len = None, 0
            continue
        if fence_char is not None:
            continue
        # The section ends where the next heading begins; a table below an
        # unrelated later heading is not this table.
        if stripped.startswith("#"):
            return False
        if not stripped.startswith("|") or digest not in stripped:
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) >= 4 and all(cells[:4]):
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root to check (default: the current directory).",
    )
    ns = parser.parse_args(argv)
    root = Path(ns.root).resolve()

    sdl_path = root / SDL_RELATIVE
    pin_path = root / PIN_RELATIVE
    doc_path = root / HISTORY_DOC_RELATIVE

    for path, label in (
        (sdl_path, "canonical SDL"),
        (pin_path, "schema-digest pin"),
        (doc_path, "schema-digest history doc"),
    ):
        if not path.is_file():
            print(f"FAIL: {label} not found at {path}", file=sys.stderr)
            return 1

    actual = compute_schema_digest(sdl_path)

    try:
        pin = json.loads(pin_path.read_text())
    except json.JSONDecodeError as exc:
        print(f"FAIL: {PIN_RELATIVE} is not valid JSON: {exc}", file=sys.stderr)
        return 1
    pinned = pin.get("schema_digest")
    if not isinstance(pinned, str) or not pinned:
        print(
            f"FAIL: {PIN_RELATIVE} has no usable 'schema_digest' string",
            file=sys.stderr,
        )
        return 1

    hint = REGENERATE_HINT.format(
        pin=PIN_RELATIVE,
        actual=actual,
        doc=HISTORY_DOC_RELATIVE,
        heading="Schema-digest history",
    )

    if pinned != actual:
        print(
            "FAIL: the Go-API schema digest MOVED.\n"
            f"  computed from {SDL_RELATIVE}: {actual}\n"
            f"  pinned in {PIN_RELATIVE}:     {pinned}\n"
            "\n"
            "  Every go_api_routing_state row keyed to the pinned digest is now\n"
            "  DEAD: both planes will fall back to Python on every request, and\n"
            "  neither will error. This is the 2026-09-01 failure -- it went\n"
            "  undetected for six days.\n"
            "\n"
            f"{hint}",
            file=sys.stderr,
        )
        return 1

    doc_text = doc_path.read_text()
    if HISTORY_HEADING not in doc_text:
        print(
            f"FAIL: {HISTORY_DOC_RELATIVE} has no '{HISTORY_HEADING}' section -- "
            "this checker cannot verify the digest was written down.",
            file=sys.stderr,
        )
        return 1
    history = doc_text.split(HISTORY_HEADING, 1)[1]
    # A TABLE ROW, not a prose mention (codex r1, P2). A substring search
    # over the section passed when the digest merely appeared in a sentence
    # after the heading, which records nothing an operator can read as
    # history -- and "make the checker green" is exactly the pressure this
    # gate exists to resist. Require a markdown row that carries the digest.
    if not _digest_appears_in_a_history_row(history, actual):
        print(
            f"FAIL: schema digest {actual} is pinned and correct, but is NOT "
            f"recorded as a ROW in the '{HISTORY_HEADING}' table of "
            f"{HISTORY_DOC_RELATIVE}.\n"
            "\n"
            "  Updating the pin alone hides the consequence. A digest move\n"
            "  invalidates every routing row; the history table is where an\n"
            "  operator finds out that happened and when.\n"
            "\n"
            f"{hint}",
            file=sys.stderr,
        )
        return 1

    print(
        f"OK: Go-API schema digest {actual} matches {PIN_RELATIVE} and is "
        f"recorded in {HISTORY_DOC_RELATIVE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
