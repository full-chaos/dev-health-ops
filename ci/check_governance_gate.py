#!/usr/bin/env python3
"""Lightweight PR governance gate for test evidence and risk notes."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys

SRC_PREFIX = "src/"
TEST_PREFIX = "tests/"
TEST_FILE_PATTERN = re.compile(r"(^|/)(test_.*|.*_test)\.py$", re.IGNORECASE)
PLACEHOLDER_VALUES = {
    "",
    "n/a",
    "na",
    "none",
    "tbd",
    "todo",
    "pending",
    "<commands run and key results>",
    "<blast radius, rollback approach, monitoring, follow-up issues>",
}


def _git_changed_files(base_sha: str, head_sha: str) -> list[str]:
    cmd = [
        "git",
        "diff",
        "--name-only",
        "--diff-filter=ACMRTUXB",
        base_sha,
        head_sha,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        print("ERROR: Unable to compute changed files from git diff.")
        if stderr:
            print(stderr)
        print(
            "Hint: ensure both base/head commits are fetched (for Actions use fetch-depth: 0)."
        )
        sys.exit(2)

    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _extract_marker_value(body: str, marker: str) -> str:
    """Extract marker value from body, allowing inline or following lines."""
    lines = body.splitlines()
    marker_prefix = f"{marker}:"

    for index, line in enumerate(lines):
        if not line.strip().upper().startswith(marker_prefix):
            continue

        remainder = line.split(":", 1)[1].strip()
        values: list[str] = []
        if remainder:
            values.append(remainder)

        for next_line in lines[index + 1 :]:
            stripped = next_line.strip()
            if not stripped:
                continue
            if re.match(r"^[A-Z][A-Z-]+:\s*", stripped):
                break
            if stripped.startswith("##"):
                break
            if stripped.startswith("<!--"):
                continue

            cleaned = stripped.lstrip("-* ").strip()
            if cleaned:
                values.append(cleaned)

        return " ".join(values).strip()

    return ""


def _is_meaningful(value: str) -> bool:
    normalized = re.sub(r"\s+", " ", value.strip()).lower()
    if normalized in PLACEHOLDER_VALUES:
        return False
    if normalized.startswith("<") and normalized.endswith(">"):
        return False
    return bool(normalized)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Enforce lightweight governance policy for PRs that modify src/: "
            "either tests must change or PR body must include TEST-EVIDENCE and RISK-NOTES."
        )
    )
    parser.add_argument("--base-sha", help="Base commit SHA for diff.")
    parser.add_argument("--head-sha", help="Head commit SHA for diff.")
    parser.add_argument(
        "--changed-file",
        action="append",
        default=[],
        help="Changed file path (repeatable). Bypasses git diff for local testing.",
    )
    parser.add_argument(
        "--pr-body",
        default=os.environ.get("PR_BODY", ""),
        help="PR body content (defaults to PR_BODY env var).",
    )
    args = parser.parse_args()

    changed_files = [f for f in args.changed_file if f]
    if not changed_files:
        if not args.base_sha or not args.head_sha:
            print(
                "ERROR: supply --base-sha and --head-sha, or provide --changed-file entries."
            )
            return 2
        changed_files = _git_changed_files(args.base_sha, args.head_sha)

    src_changes = sorted(path for path in changed_files if path.startswith(SRC_PREFIX))
    if not src_changes:
        print("Governance gate: no src/ changes detected, skipping strict evidence checks.")
        return 0

    test_changes = sorted(
        path
        for path in changed_files
        if path.startswith(TEST_PREFIX) or TEST_FILE_PATTERN.search(path)
    )
    if test_changes:
        print(
            "Governance gate: src/ and test changes detected; policy satisfied without PR markers."
        )
        return 0

    test_evidence = _extract_marker_value(args.pr_body or "", "TEST-EVIDENCE")
    risk_notes = _extract_marker_value(args.pr_body or "", "RISK-NOTES")

    missing: list[str] = []
    if not _is_meaningful(test_evidence):
        missing.append("TEST-EVIDENCE")
    if not _is_meaningful(risk_notes):
        missing.append("RISK-NOTES")

    if missing:
        print("Governance gate failed.")
        print(f"Detected src/ changes without test file updates ({len(src_changes)} files).")
        for path in src_changes[:10]:
            print(f"  - {path}")
        if len(src_changes) > 10:
            print(f"  ... and {len(src_changes) - 10} more")
        print(
            "Add meaningful PR body markers or modify tests:\n"
            "  TEST-EVIDENCE: <commands + results>\n"
            "  RISK-NOTES: <blast radius + rollback + follow-up>"
        )
        print(f"Missing/invalid markers: {', '.join(missing)}")
        return 1

    print("Governance gate: src/ changes detected, PR markers provided; policy satisfied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
