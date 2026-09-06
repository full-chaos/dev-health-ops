#!/usr/bin/env python3
"""Emit the GitHub Actions matrix for a battery, from a normalised table.

One matrix entry per mutant, plus the two harness arms (_BASELINE, _SENTINEL)
which run as ordinary matrix entries so they parallelise with everything else.

The layout is printed BEFORE anything is provisioned, so a dispatch can be read
and argued with before it spends fifty runners. A plan that the run is free to
contradict is worse than no plan, so the matrix this prints IS the set of jobs
that run -- there is no dynamic re-assignment afterwards.
"""

import argparse
import json
import sys

# GitHub caps a matrix at 256 jobs. Refusing at the cap is better than having
# the platform silently truncate the run into a smaller battery that reports a
# number looking exactly like the full one.
MATRIX_CAP = 256


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", required=True)
    ap.add_argument(
        "--sentinel-file", default="", help="file the _SENTINEL arm appends to"
    )
    ap.add_argument(
        "--only-arms",
        default="",
        help="comma-separated mutant ids to measure instead of the whole table. "
        "_BASELINE and _SENTINEL always run: they are the validity controls, and a "
        "subset measured without them is uninterpretable.",
    )
    args = ap.parse_args()

    mutants = [
        json.loads(line) for line in open(args.table, encoding="utf-8") if line.strip()
    ]
    if not mutants:
        print("REFUSING: the table names no mutants", file=sys.stderr)
        return 2

    only = [x.strip() for x in args.only_arms.split(",") if x.strip()]
    if only:
        known = {m["id"] for m in mutants}
        unknown = [o for o in only if o not in known]
        if unknown:
            print(
                "REFUSING: only_arms names {}, which {} not in the table -- a typo would "
                "silently measure fewer arms than asked for".format(
                    ", ".join(unknown), "is" if len(unknown) == 1 else "are"
                ),
                file=sys.stderr,
            )
            return 2
        chosen = [m for m in mutants if m["id"] in only]
    else:
        chosen = mutants

    # _BASELINE and _SENTINEL run on EVERY invocation, subset or not. They are
    # what makes any verdict interpretable: without a green baseline every arm
    # reads as KILLED, and without a green sentinel the harness may be
    # false-killing. A subset that skipped them would be cheaper and worthless.
    entries = [{"id": "_BASELINE"}, {"id": "_SENTINEL"}]
    entries += [{"id": m["id"]} for m in chosen]

    if len(entries) > MATRIX_CAP:
        print(
            f"REFUSING: {len(entries)} matrix entries exceeds GitHub's cap of "
            f"{MATRIX_CAP} -- split the table",
            file=sys.stderr,
        )
        return 2

    sentinel_file = args.sentinel_file or mutants[0]["file"]
    print(json.dumps({"include": entries}))
    print(f"arm_count={len(chosen)}", file=sys.stderr)
    print(f"table_arm_count={len(mutants)}", file=sys.stderr)
    print(f"run_scope={'partial' if only else 'full'}", file=sys.stderr)
    print(f"matrix_size={len(entries)}", file=sys.stderr)
    print(f"sentinel_file={sentinel_file}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
