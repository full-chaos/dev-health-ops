#!/usr/bin/env python3
"""Pre-flight a normalised mutant table against a real tree.

Two rules, both of which must hold for EVERY row before a single test suite is
spent -- a stale table is cheap to detect and expensive to discover halfway
through a battery:

  EXACTLY-ONCE   the needle occurs exactly once in its file. Zero means the
                 table has gone stale against the tree and the arm would prove
                 nothing; more than one means the arm would mutate an
                 arbitrary occurrence and the verdict would not be reproducible.

  DELETION-ONLY  for a kind=delete row, the replacement is shorter than the
                 needle and different from it. A kind=replace row is held only
                 to "different from it" (rule :412).

Refusing the WHOLE RUN on any bad row is deliberate. A battery that quietly
skips two of its arms reports a smaller number that looks like the same number.
"""

import argparse
import json
import os
import sys


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--root", required=True, help="tree the needles are matched against"
    )
    ap.add_argument("--table", required=True, help="normalised JSONL table")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    bad = 0
    cache = {}
    for line in open(args.table, encoding="utf-8"):
        if not line.strip():
            continue
        m = json.loads(line)
        path = os.path.join(args.root, m["file"])
        if path not in cache:
            if not os.path.exists(path):
                print("PREFLIGHT {}: file {} does not exist".format(m["id"], m["file"]))
                bad += 1
                continue
            cache[path] = open(path, encoding="utf-8").read()
        body = cache[path]

        n = body.count(m["needle"])
        if n != 1:
            print(
                f"PREFLIGHT {m['id']}: needle occurs {n} times in {m['file']}, "
                "want EXACTLY 1"
            )
            bad += 1
            continue

        repl = m.get("replacement", "")
        kind = m.get("kind", "delete")
        if repl == m["needle"]:
            print(
                "PREFLIGHT {}: replacement is IDENTICAL to the needle -- the arm would prove nothing".format(
                    m["id"]
                )
            )
            bad += 1
            continue
        if kind == "delete" and len(repl) >= len(m["needle"]):
            print(
                f"PREFLIGHT {m['id']}: replacement ({len(repl)} chars) is not shorter "
                f"than the needle ({len(m['needle'])}) and the "
                "row is kind=delete -- this battery is DELETION-ONLY unless the ROW opts in with "
                "kind=replace (rule :412)"
            )
            bad += 1
            continue

        if not args.quiet:
            print(
                f"ok preflight {m['id']} [{kind}]: needle x1 in {m['file']}, "
                f"{len(m['needle'])} -> {len(repl)} chars"
            )

    if bad:
        print(f"PREFLIGHT REFUSED: {bad} bad row(s)", file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
