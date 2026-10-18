#!/usr/bin/env python3
"""Auth gate scope candidates from `go list -tags=integration -json ./...` (CHAOS-10103).

Reads the JSON object stream on stdin and the module path in argv[1]. Prints,
sorted, the import path of every package that LOOKS like auth (a path segment,
relative to the module, starting with `auth` or `audit`) or whose production,
test or external-test imports include the auth schema adapter. Parsing the
structured output (not text munging) keeps a package named `x_test`, or one that
exists only under a build tag, from being misread or missed.
"""

from __future__ import annotations

import json
import re
import sys

SEGMENT = re.compile(r"^(auth|audit)")


def objects(text: str):
    decoder = json.JSONDecoder()
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            return
        value, index = decoder.raw_decode(text, index)
        yield value


def main() -> int:
    module = sys.argv[1]
    adapter = f"{module}/internal/storage/postgres/authschema"
    found: set[str] = set()
    seen = 0
    for package in objects(sys.stdin.read()):
        path = package["ImportPath"]
        if not path.startswith(module):
            continue
        seen += 1
        relative = path[len(module) :].lstrip("/")
        imports = (
            package.get("Imports", [])
            + package.get("TestImports", [])
            + package.get("XTestImports", [])
        )
        if (
            any(SEGMENT.match(segment) for segment in relative.split("/") if segment)
            or adapter in imports
        ):
            found.add(path)
    if seen == 0:
        print("auth scope: go list reported no package of the module", file=sys.stderr)
        return 1
    for path in sorted(found):
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
