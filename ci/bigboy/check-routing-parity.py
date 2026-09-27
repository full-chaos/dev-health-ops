#!/usr/bin/env python3
"""CHAOS-6987 (gap 5, D2731): bigboy's GraphQL routing ledger must match prod's.

ci/bigboy/routing-ops.txt is the tracked list of operations enabled on prod (from the
scribe's prod readback). bigboy-cut.sh enables every listed operation that bigboy's
ledger is missing (`dho goapi routing enable`), then runs this check against a fresh
`dho goapi routing status -json`.

routing-ops.txt format, one operation per line; `#` starts a comment:
    featureFlags
    testopsRisk KNOWN-MISSING CHAOS-6993 <reason>

A KNOWN-MISSING operation is enabled on prod but cannot be enabled on bigboy yet. It
is never silently passed: the report names it, and the exit code is 3 (not 0) while
it stays missing. If it becomes reachable, the check fails (1) so the marker is removed.

Checks (each finding is printed on stderr as `DRIFT: ...`):
  * status JSON must be complete (catalog loaded, no DB/classification error);
  * every listed, non-known-missing operation is reachable on bigboy;
  * no operation outside the list is reachable on bigboy;
  * every reachable operation is mode=canary rollout=100 owner=go (prod's shape);
  * every listed operation exists in bigboy's catalog.

Usage:
  check-routing-parity.py <routing-ops.txt> <status.json>            report + verdict
  check-routing-parity.py <routing-ops.txt> <status.json> --to-enable  print the
      comma-joined operations bigboy must enable (listed, not known-missing, not
      reachable); prints nothing when there are none.
Exit: 0 parity; 1 drift; 2 usage/input error; 3 parity except named KNOWN-MISSING ops.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

OP_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXPECTED_SHAPE = ("canary", 100, "go")


@dataclass
class OpsList:
    required: list[str] = field(default_factory=list)
    known_missing: dict[str, str] = field(default_factory=dict)


def parse_ops(text: str) -> OpsList:
    out = OpsList()
    seen: set[str] = set()
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        op = parts[0]
        if not OP_RE.match(op):
            raise ValueError(f"line {n}: not an operation name: {op!r}")
        if op in seen:
            raise ValueError(f"line {n}: duplicate operation {op}")
        seen.add(op)
        if len(parts) == 1:
            out.required.append(op)
        elif (
            parts[1] == "KNOWN-MISSING"
            and len(parts) >= 3
            and parts[2].startswith("CHAOS-")
        ):
            out.known_missing[op] = " ".join(parts[2:])
        else:
            raise ValueError(
                f"line {n}: expected `<op>` or `<op> KNOWN-MISSING CHAOS-<n> <reason>`"
            )
    if not out.required:
        raise ValueError(
            "routing-ops list has ZERO required operations -- refusing to pass on an empty list"
        )
    return out


def load_status(text: str) -> dict:
    doc = json.loads(text)
    if not isinstance(doc, dict) or not isinstance(doc.get("operations"), list):
        raise ValueError("status JSON has no operations array")
    return doc


def status_errors(doc: dict) -> list[str]:
    errs = []
    if doc.get("catalog_loaded") is not True:
        errs.append(f"status catalog not loaded ({doc.get('catalog_error')})")
    for key in ("registry_db_error", "classification_error", "go_plane_error"):
        if doc.get(key):
            errs.append(f"status {key}: {doc[key]}")
    if not doc["operations"]:
        errs.append(
            "status reports ZERO operations -- a ledger read that returned nothing is not a pass"
        )
    return errs


def reachable(doc: dict) -> dict[str, dict]:
    return {o["operation"]: o for o in doc["operations"] if o.get("reachable") is True}


def to_enable(ops: OpsList, doc: dict) -> list[str]:
    live = reachable(doc)
    return [op for op in ops.required if op not in live]


def check(ops: OpsList, doc: dict) -> tuple[list[str], list[str]]:
    """Return (drift findings, known-missing notes)."""
    findings = status_errors(doc)
    if findings:
        return findings, []
    catalog = {o["operation"] for o in doc["operations"]}
    live = reachable(doc)
    listed = set(ops.required) | set(ops.known_missing)
    for op in sorted(listed - catalog):
        findings.append(f"listed operation {op} is not in bigboy's catalog")
    for op in ops.required:
        if op in catalog and op not in live:
            findings.append(
                f"operation {op} is enabled on prod but NOT reachable on bigboy"
            )
    for op in sorted(set(live) - listed):
        findings.append(
            f"operation {op} is reachable on bigboy but NOT in the prod list"
        )
    for op, row in sorted(live.items()):
        shape = (row.get("mode"), row.get("rollout_percentage"), row.get("owner"))
        if shape != EXPECTED_SHAPE:
            findings.append(
                f"operation {op} shape {shape} differs from prod {EXPECTED_SHAPE}"
            )
    notes: list[str] = []
    for op, why in sorted(ops.known_missing.items()):
        if op in live:
            findings.append(
                f"KNOWN-MISSING operation {op} is now reachable -- remove its marker ({why})"
            )
        else:
            notes.append(f"KNOWN-MISSING: {op} ({why})")
    return findings, notes


def main(argv: list[str]) -> int:
    mode_enable = "--to-enable" in argv
    args = [a for a in argv if a != "--to-enable"]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        ops = parse_ops(Path(args[0]).read_text(encoding="utf-8"))
        doc = load_status(Path(args[1]).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2
    if mode_enable:
        errs = status_errors(doc)
        if errs:
            for e in errs:
                print(f"DRIFT: {e}", file=sys.stderr)
            return 1
        print(",".join(to_enable(ops, doc)))
        return 0
    findings, notes = check(ops, doc)
    for f in findings:
        print(f"DRIFT: {f}", file=sys.stderr)
    for n in notes:
        print(n)
    live = reachable(doc)
    verdict = "DRIFT" if findings else ("KNOWN-GAP" if notes else "PARITY")
    print(
        f"ROUTING_PARITY verdict={verdict} listed={len(ops.required) + len(ops.known_missing)} "
        f"required={len(ops.required)} reachable={len(live)} known_missing={len(notes)} drift={len(findings)}"
    )
    if findings:
        return 1
    return 3 if notes else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
