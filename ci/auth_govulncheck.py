#!/usr/bin/env python3
"""Gate for `govulncheck -format json` over the auth packages (CHAOS-4887).

Reads the JSON message stream on stdin and the baseline file named by argv[1]
(one OSV id per line, `#` comments). Exit 1 when a REACHABLE finding (one whose
trace names a function, i.e. the code calls into the vulnerable symbol) is not in
the baseline: a new vulnerability blocks merge. A baseline entry that is no
longer reachable is reported as stale so the list shrinks; it does not fail.
An empty stream or one with no `config` message fails too: a scan that did not
happen must not read green.
"""

from __future__ import annotations

import json
import sys


def messages(text: str):
    decoder = json.JSONDecoder()
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            return
        message, index = decoder.raw_decode(text, index)
        yield message


def baseline(path: str) -> set[str]:
    ids: set[str] = set()
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            entry = line.split("#", 1)[0].strip()
            if entry:
                ids.add(entry)
    return ids


def main() -> int:
    known = baseline(sys.argv[1])
    saw_config = False
    reachable: dict[str, str] = {}
    for message in messages(sys.stdin.read()):
        if "config" in message:
            saw_config = True
        finding = message.get("finding")
        if not finding:
            continue
        trace = finding.get("trace") or []
        if trace and trace[0].get("function"):
            module = trace[0].get("module", "?")
            reachable[finding["osv"]] = f"{module}@{trace[0].get('version', '?')} fixed in {finding.get('fixed_version', '?')}"
    if not saw_config:
        print("auth govulncheck: no scan configuration in the output; the scan did not run", file=sys.stderr)
        return 1
    new = sorted(set(reachable) - known)
    stale = sorted(known - set(reachable))
    for osv in sorted(set(reachable) & known):
        print(f"auth govulncheck: known (baseline) {osv} {reachable[osv]}")
    for osv in stale:
        print(f"auth govulncheck: baseline entry {osv} is no longer reachable; delete it from the baseline")
    for osv in new:
        print(f"auth govulncheck: NEW reachable vulnerability {osv} {reachable[osv]}", file=sys.stderr)
    print(f"auth govulncheck: {len(reachable)} reachable, {len(new)} new, {len(stale)} stale baseline entries")
    return 1 if new else 0


if __name__ == "__main__":
    sys.exit(main())
