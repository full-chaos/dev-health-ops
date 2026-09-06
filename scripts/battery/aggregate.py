#!/usr/bin/env python3
"""Merge per-arm verdicts into the ONE verdict for a battery.

THE MERGE STEP IS THE SINGLE VERDICT. No individual arm's exit code is ever the
battery's exit code: one arm's `go test` rc is not evidence about the battery,
exactly as one shard's rc is not evidence about a corpus. This script reads the
per-arm JSON files that run_arm.sh wrote and computes the run's rc from the
totals alone.

The rc is 0 only when ALL of:
  * the _BASELINE arm passed          (an already-red package reports every
                                       mutant KILLED, so a red baseline makes
                                       every other number meaningless)
  * the _SENTINEL arm passed          (a red sentinel means the harness
                                       false-kills)
  * every mutant in the table has a verdict file  (a missing arm is a
                                       HARNESS_ERROR, never a quiet skip -- a
                                       battery that drops two arms reports a
                                       smaller number that looks like the same
                                       number)
  * harness_error == 0
  * unexpected_survivors == 0

EXPECTED_SURVIVORS is a list of mutant IDS. Empty (and unset) means NONE -- not
"all" and not "unchecked": with it empty, any survivor at all fails the run. It
is never a count and never a boolean. An id that is not in the table REFUSES,
because a typo'd expected survivor silently admits a real one. An expected
survivor is still COUNTED in `survived`; it is excluded only from
`unexpected_survivors`, which is what the rc reads.
"""

import argparse
import glob
import json
import os
import sys


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", required=True, help="normalised JSONL table")
    ap.add_argument("--arms-dir", required=True, help="dir of per-arm JSON verdicts")
    ap.add_argument(
        "--expected-survivors", default="", help="space-separated ids; empty = none"
    )
    ap.add_argument("--execution-shape", default="hosted")
    ap.add_argument("--packages", default="")
    ap.add_argument("--tip", default="", help="the TREE UNDER TEST (the specimen)")
    ap.add_argument(
        "--harness-sha",
        default="",
        help="the harness sha of scripts/battery/* that classified (the instrument)",
    )
    ap.add_argument(
        "--only-arms",
        default="",
        help="comma-separated ids this run measured. Marks the summary PARTIAL so a "
        "subset can never be read as a full verdict.",
    )
    ap.add_argument("--floor", default="0")
    ap.add_argument("--summary-out", default="-")
    args = ap.parse_args()

    table = []
    with open(args.table, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                table.append(json.loads(line))
    all_ids = [m["id"] for m in table]
    kinds = {m["id"]: m.get("kind", "delete") for m in table}

    # A PARTIAL RUN IS LABELLED, LOUDLY, AND ITS UNMEASURED ARMS ARE LISTED.
    # A subset re-measurement is a legitimate answer to an infrastructure flake,
    # but it is NOT a battery verdict: the arms it did not run are unknown, not
    # passing. Counting only the measured ones without saying so would let
    # "killed=1 survived=0 harness_error=0" from a one-arm re-measure look
    # exactly like a clean full run.
    only = [x.strip() for x in args.only_arms.split(",") if x.strip()]
    unknown_only = [o for o in only if o not in all_ids]
    if unknown_only:
        print(
            "REFUSING: only_arms names {}, which {} not in the table".format(
                ", ".join(unknown_only), "is" if len(unknown_only) == 1 else "are"
            ),
            file=sys.stderr,
        )
        return 2
    ids = [i for i in all_ids if i in only] if only else all_ids
    not_measured = [i for i in all_ids if i not in ids]

    expected = [x for x in args.expected_survivors.split() if x]
    unknown = [e for e in expected if e not in ids]
    if unknown:
        print(
            "REFUSING: EXPECTED_SURVIVORS names {}, which {} not in the table -- a typo'd "
            "expected survivor silently admits a real one".format(
                ", ".join(unknown), "is" if len(unknown) == 1 else "are"
            ),
            file=sys.stderr,
        )
        return 2

    verdicts = {}
    for path in sorted(
        glob.glob(os.path.join(args.arms_dir, "**", "*.json"), recursive=True)
    ):
        try:
            with open(path, encoding="utf-8") as f:
                v = json.loads(f.read())
        except Exception as exc:  # noqa: BLE001
            print(f"skipping unreadable verdict {path}: {exc}", file=sys.stderr)
            continue
        if isinstance(v, dict) and "id" in v:
            verdicts[v["id"]] = v

    def state_of(arm):
        v = verdicts.get(arm)
        if v is None:
            return None
        return v.get("state")

    baseline = state_of("_BASELINE")
    sentinel = state_of("_SENTINEL")

    killed = survived = harness = killed_replacement = 0
    unexpected = []
    rows = []
    for mid in ids:
        v = verdicts.get(mid)
        if v is None:
            state, detail = (
                "HARNESS_ERROR",
                "NO VERDICT ARTIFACT -- the arm never reported",
            )
            ran = named = 0
        else:
            state = v.get("state", "HARNESS_ERROR")
            detail = v.get("detail", "")
            ran = v.get("ran", 0)
            named = v.get("named_failures", 0)
        if state == "KILLED":
            killed += 1
            if kinds.get(mid) == "replace":
                killed_replacement += 1
        elif state == "SURVIVED":
            survived += 1
            if mid in expected:
                detail += " [EXPECTED]"
            else:
                unexpected.append(mid)
        else:
            state = "HARNESS_ERROR"
            harness += 1
        rows.append((mid, kinds.get(mid, "delete"), state, ran, named, detail))

    lines = []
    lines.append("mutation battery summary")
    lines.append(f"execution_shape={args.execution_shape}")
    # BOTH SHAS, ALWAYS. A battery runs the CURRENT rules against a historical
    # tip: the rules are the instrument and the tip is the specimen. That is the
    # right way round -- but it means the same tip can classify differently
    # after a rules change, so a summary naming only the tip is not reproducible
    # evidence. Naming both makes the pair readable months later.
    lines.append(f"tip={args.tip}  (tree under test / specimen)")
    lines.append(
        "harness_sha=%s  (scripts/battery that classified / instrument)"
        % (args.harness_sha or "UNRECORDED")
    )
    if only:
        lines.append(
            f"run_scope=PARTIAL -- {len(ids)} of {len(all_ids)} arms measured; "
            "THIS IS NOT A FULL VERDICT"
        )
        lines.append("measured_arms={}".format(" ".join(ids)))
        lines.append("not_measured={}".format(" ".join(not_measured)))
    else:
        lines.append("run_scope=full -- every arm in the table was measured")
    lines.append(f"packages={args.packages}")
    lines.append(f"floor={args.floor} (SUM over the package list)")
    lines.append(
        f"arms_in_table={len(ids)} arms_reported={sum(1 for i in ids if i in verdicts)}"
    )
    lines.append("baseline=%s" % (baseline or "MISSING"))
    lines.append("sentinel=%s" % (sentinel or "MISSING"))
    lines.append("expected_survivors=%s" % (" ".join(expected) if expected else "none"))
    lines.append(
        "unexpected_survivors=%s" % (" ".join(unexpected) if unexpected else "none")
    )
    lines.append("")
    lines.append(
        f"killed={killed} (of which replacement={killed_replacement}) "
        f"survived={survived} harness_error={harness}"
    )
    lines.append("")
    lines.append(f"{'ID':<34} {'KIND':<8} {'STATE':<14} {'RAN':<7} {'NAMED':<7} DETAIL")
    for mid, kind, state, ran, named, detail in rows:
        lines.append(
            f"{mid:<34} {kind:<8} {state:<14} {ran:<7} {named:<7} {detail[:160]}"
        )

    problems = []
    if baseline != "PASS":
        problems.append(
            "BASELINE is %s -- every mutant verdict in this run is void"
            % (baseline or "MISSING")
        )
    if sentinel != "PASS":
        problems.append(
            "SENTINEL is %s -- the harness may be false-killing"
            % (sentinel or "MISSING")
        )
    if harness:
        problems.append(f"{harness} harness error(s)")
    if unexpected:
        problems.append(
            f"{len(unexpected)} unexpected survivor(s): {' '.join(unexpected)}"
        )
    if problems:
        lines.append("")
        for p in problems:
            lines.append(f"FAIL: {p}")

    text = "\n".join(lines) + "\n"
    if args.summary_out == "-":
        sys.stdout.write(text)
    else:
        with open(args.summary_out, "w", encoding="utf-8") as f:
            f.write(text)
        sys.stdout.write(text)

    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
