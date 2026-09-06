#!/usr/bin/env python3
"""Apply ONE mutant to a tree, and prove the file moved.

Prints exactly one word on stdout:

  APPLIED    the needle matched once and was replaced
  NOMATCH    the needle is not in the file  -> the caller reports HARNESS_ERROR
  MULTI:<n>  the needle matches n>1 times   -> the caller reports HARNESS_ERROR

and, on APPLIED, the before/after sha256 on stderr so the caller can record the
digest-moved proof without hashing the file a second time.

An unapplied mutant is UNPROVEN, never a pass. This script never decides that
for the caller -- it reports what happened and lets the classifier own the
three-state verdict, which is the same split the bigboy harness uses.
"""

import argparse
import hashlib
import json
import os
import sys


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", required=True)
    ap.add_argument("--spec", required=True, help="a single-mutant JSON file")
    args = ap.parse_args()

    m = json.loads(open(args.spec, encoding="utf-8").read())
    path = os.path.join(args.root, m["file"])
    body = open(path, encoding="utf-8").read()

    n = body.count(m["needle"])
    if n == 0:
        print("NOMATCH")
        return 0
    if n > 1:
        print(f"MULTI:{n}")
        return 0

    before = sha256_of(path)
    open(path, "w", encoding="utf-8").write(
        body.replace(m["needle"], m.get("replacement", ""), 1)
    )
    after = sha256_of(path)
    if before == after:
        # Cannot happen for a non-empty change, which is exactly why it is
        # checked: a silent no-op here would read downstream as a real arm.
        print("NOMATCH")
        print(
            "digest did NOT move on {} ({})".format(m["file"], before), file=sys.stderr
        )
        return 0
    print("APPLIED")
    print(
        "digest-moved {} -> {} on {}".format(before[:12], after[:12], m["file"]),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
