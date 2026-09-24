"""The `venue-oracles` required check's verdict over its sharded run.

venue-oracles.yml runs one venue-oracle package per matrix job
(`venue-oracles-package`), planned by `venue-oracles-plan` from
`ci/check_go.sh venue-oracle-packages`. The job named `venue-oracles`, the
context branch protection requires, runs after both with `if: always()` and
calls this script, so the check can only pass when:

* the plan succeeded and decided the change set is not Go-relevant, and no
  shard ran (the honest skip the single-job workflow already reported); or
* the plan succeeded, decided it is relevant, the shard matrix as a whole
  succeeded, AND every planned package left its completion marker -- so a
  matrix that ran fewer packages than planned cannot pass on GitHub's
  aggregate result alone.

Anything else -- a failed, cancelled or skipped plan or shard, a marker
missing or unplanned -- fails the check.

Inputs come from the environment: PLAN_RESULT, SHARDS_RESULT (the `needs`
results), RELEVANT and PACKAGES (the plan's outputs), and MARKERS_DIR, the
directory the shards' completion markers were downloaded into. Each marker
is a file whose content is the package it completed (`./internal/...`).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def verdict(
    plan_result: str,
    shards_result: str,
    relevant: str,
    packages_json: str,
    completed: list[str],
) -> tuple[bool, str]:
    """Decide the check: (passed, reason)."""
    if plan_result != "success":
        return False, f"the plan job did not succeed ({plan_result!r})"
    if relevant == "false":
        if shards_result != "skipped":
            return False, (
                f"the plan decided the change set is not Go-relevant, but the "
                f"shards reported {shards_result!r} instead of 'skipped'"
            )
        return True, "skipped: no Go-relevant paths in this change set"
    if relevant != "true":
        return (
            False,
            f"the plan's relevance output is {relevant!r}, not 'true' or 'false'",
        )
    try:
        planned = json.loads(packages_json)
    except json.JSONDecodeError as error:
        return False, f"the plan's package list is not JSON: {error}"
    if (
        not isinstance(planned, list)
        or not planned
        or not all(isinstance(p, str) for p in planned)
    ):
        return (
            False,
            f"the plan's package list is empty or malformed: {packages_json!r}",
        )
    if shards_result != "success":
        return False, f"the shard matrix did not succeed ({shards_result!r})"
    missing = sorted(set(planned) - set(completed))
    unplanned = sorted(set(completed) - set(planned))
    if missing or unplanned:
        return False, (
            f"completion markers do not match the plan: missing {missing}, unplanned {unplanned}"
        )
    if len(completed) != len(set(completed)):
        return False, f"a package reported completion twice: {sorted(completed)}"
    return True, f"all {len(planned)} venue-oracle packages passed"


def read_markers(directory: str) -> list[str]:
    root = Path(directory)
    if not root.is_dir():
        return []
    return [
        path.read_text(encoding="utf-8").strip()
        for path in sorted(root.iterdir())
        if path.is_file()
    ]


def main() -> int:
    passed, reason = verdict(
        os.environ.get("PLAN_RESULT", ""),
        os.environ.get("SHARDS_RESULT", ""),
        os.environ.get("RELEVANT", ""),
        os.environ.get("PACKAGES", ""),
        read_markers(os.environ.get("MARKERS_DIR", "")),
    )
    print(("PASS: " if passed else "FAIL: ") + reason)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
