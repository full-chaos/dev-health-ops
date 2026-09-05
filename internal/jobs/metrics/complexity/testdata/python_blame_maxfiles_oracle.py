"""Oracle for the blame x max_files budget arithmetic in run_complexity_db_job.

Ports the exact expressions at job_complexity_db.py, verbatim, rather than
reimplementing them, so a transcription slip in the Go port (fileselect.go)
shows up as a real mismatch instead of two independently-written copies of
the same intended formula silently agreeing with each other:

  missing              = max(total_files - non_empty, 0)         (line 317)
  remaining (updated)  = max(remaining - consumed, 0) if remaining is not None
                         else None                                (lines 314-315)
  attempt_blame        = missing > 0 and (remaining is None or remaining > 0)
                                                                    (line 318)

Reads a JSON array of cases on stdin, each:
  {"max_files": int|null, "total_files": int, "non_empty": int,
   "git_files_returned": int}

Emits, per case, the NonEmptyBranch plan's three derived values: missing,
remaining (after the git_files phase), and attempt_blame -- using `null` for
Python's None so a JSON-level nil/None mismatch is visible on either side.

This script is the ORACLE and never the implementation.
"""

from __future__ import annotations

import json
import sys


def plan_non_empty_branch(
    max_files: int | None, total_files: int, non_empty: int, git_files_returned: int
) -> dict:
    remaining = max_files
    if remaining is not None:
        remaining = max(remaining - git_files_returned, 0)

    missing = max(total_files - non_empty, 0)
    attempt_blame = missing > 0 and (remaining is None or remaining > 0)

    return {
        "missing": missing,
        "remaining": remaining,
        "attempt_blame": attempt_blame,
    }


def main() -> int:
    cases = json.load(sys.stdin)
    results = []
    for case in cases:
        results.append(
            plan_non_empty_branch(
                case["max_files"],
                case["total_files"],
                case["non_empty"],
                case["git_files_returned"],
            )
        )
    json.dump(results, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
