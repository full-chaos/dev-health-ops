#!/usr/bin/env python3
"""Gate for `govulncheck -format json` over the auth packages (CHAOS-4887).

Reads the JSON message stream on stdin. Exit 1 on ANY REACHABLE finding (one
whose trace names a function, i.e. the code calls into the vulnerable symbol).
There is no ignore list: a pull request cannot whitelist a vulnerability. An
empty stream or one with no `config` message fails too: a scan that did not
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


def main() -> int:
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
            reachable[finding["osv"]] = (
                f"{module}@{trace[0].get('version', '?')} fixed in {finding.get('fixed_version', '?')}"
            )
    if not saw_config:
        print(
            "auth govulncheck: no scan configuration in the output; the scan did not run",
            file=sys.stderr,
        )
        return 1
    for osv in sorted(reachable):
        print(f"auth govulncheck: reachable {osv} {reachable[osv]}", file=sys.stderr)
    print(f"auth govulncheck: {len(reachable)} reachable")
    return 1 if reachable else 0


if __name__ == "__main__":
    sys.exit(main())
