#!/usr/bin/env python3
"""Normalise a mutation-battery table to JSONL, and validate it.

THE ONE AUTHORITY for what a mutant table means. The bigboy harness
(``_shared/mutation-battery.sh``) and the hosted matrix workflow
(``.github/workflows/mutation-battery.yml``) both call this file, so a table
that is accepted in one place is accepted identically in the other. A second
copy of these rules is how the two venues silently disagree about what a
battery measured.

Accepted input forms, auto-detected from the first non-blank line:

  TSV    id<TAB>file<TAB>needle<TAB>replacement[<TAB>kind]
         Tab-separated with NO backslash escapes -- escapes are not
         interpreted, so a needle here is single-line and tab-free by
         construction. A backslash is refused rather than silently taken
         literally, because a table author who wrote ``\\t`` meant a tab and
         would otherwise get the two characters.

  JSONL  one object per line: id, file, needle, optional replacement (default
         "") and optional kind (default "delete"). The form to use when a
         needle spans lines or contains tabs.

KIND is per row:

  delete   (default) the replacement must be a strict deletion -- shorter than
           the needle and different from it. Enforced in preflight.py against a
           real tree, not here.
  replace  lifts the strictly-shorter rule FOR THAT ROW ONLY (rule :412): a
           wrong-VALUE property where deleting the line would only re-test
           wiring. Still needs replacement != needle, is still build+vet gated,
           and still needs a named "--- FAIL: Test" line to count as a kill.

An unrecognised kind is REFUSED rather than defaulted. Defaulting an unknown
kind to "replace" would silently lift the deletion-only rule for a row that
never asked for it, and defaulting it to "delete" would silently refuse a row
whose author meant "replace" -- neither failure is visible in a summary.
"""

import argparse
import json
import sys

VALID_KINDS = ("delete", "replace")


def _fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(2)


def parse(raw):
    """Return the mutant list. Raises SystemExit(2) with a named reason."""
    first = ""
    for line in raw.splitlines():
        if line.strip():
            first = line.strip()
            break

    out = []
    if first.startswith("{"):
        form = "JSONL"
        for i, line in enumerate(raw.splitlines(), 1):
            if not line.strip():
                continue
            try:
                m = json.loads(line)
            except Exception as exc:  # noqa: BLE001 - the message is the point
                _fail(f"PARSE-ERROR line {i}: {exc}")
            for key in ("id", "file", "needle"):
                if key not in m:
                    _fail(f"PARSE-ERROR line {i}: missing key {key}")
            out.append(
                {
                    "id": m["id"],
                    "file": m["file"],
                    "needle": m["needle"],
                    "replacement": m.get("replacement", ""),
                    "kind": m.get("kind", "delete"),
                }
            )
    else:
        form = "TSV"
        for i, line in enumerate(raw.split("\n"), 1):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) == 3:
                parts = parts + [""]
            if len(parts) == 4:
                parts = parts + ["delete"]
            if len(parts) != 5:
                _fail(
                    f"PARSE-ERROR line {i}: {len(parts)} tab-separated fields, want 3, 4 or 5 "
                    "(id, file, needle, replacement, kind)"
                )
            mid, path, needle, repl, kind = parts
            if "\\" in needle or "\\" in repl:
                _fail(
                    f"PARSE-ERROR line {i}: TSV needle/replacement carry a backslash -- escapes "
                    "are not interpreted; use the JSONL form for multi-line needles"
                )
            out.append(
                {
                    "id": mid,
                    "file": path,
                    "needle": needle,
                    "replacement": repl,
                    "kind": (kind.strip() or "delete"),
                }
            )

    if not out:
        _fail("PARSE-ERROR: the table names no mutants")

    seen = set()
    for m in out:
        if not m["id"]:
            _fail("PARSE-ERROR: a row has an empty id")
        if m["id"] in seen:
            _fail("PARSE-ERROR: duplicate mutant id {}".format(m["id"]))
        seen.add(m["id"])
        # An id becomes an artifact name and a matrix key, so it is held to a
        # conservative charset rather than sanitised later where the mapping
        # back to the table would be guesswork.
        for ch in m["id"]:
            if not (ch.isalnum() or ch in "._-"):
                _fail(
                    "PARSE-ERROR {}: ids may use only letters, digits, dot, underscore and "
                    "hyphen -- an id is an artifact name and a matrix key".format(
                        m["id"]
                    )
                )
        if m["id"].startswith("_"):
            _fail(
                "PARSE-ERROR {}: ids starting with '_' are reserved for the harness's own "
                "arms (_BASELINE, _SENTINEL)".format(m["id"])
            )
        if m["kind"] not in VALID_KINDS:
            _fail(
                "PARSE-ERROR {}: kind must be one of {}, got {!r} -- an unrecognised kind is never "
                "defaulted, because defaulting it either way silently changes which rule the row "
                "is held to".format(m["id"], "/".join(VALID_KINDS), m["kind"])
            )
    return form, out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("table", help="path to the table, or - for stdin")
    ap.add_argument("--out", default="-", help="write JSONL here (default stdout)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.table == "-":
        raw = sys.stdin.read()
    else:
        with open(args.table, encoding="utf-8") as f:
            raw = f.read()

    form, mutants = parse(raw)

    lines = "".join(json.dumps(m, sort_keys=True) + "\n" for m in mutants)
    if args.out == "-":
        sys.stdout.write(lines)
    else:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(lines)
    if not args.quiet:
        print(
            f"normalised {len(mutants)} mutants ({form} form), "
            f"{sum(1 for m in mutants if m['kind'] == 'replace')} replacement row(s)",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
