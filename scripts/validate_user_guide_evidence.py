#!/usr/bin/env python3
"""Validate the local 48-artifact user-guide visual-evidence packet."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Final

from scripts.user_guide_evidence_contract import SourceRevision
from scripts.user_guide_evidence_validation import validate_evidence_root

ROOT: Final = Path(__file__).resolve().parents[1]


class EvidenceValidationError(Exception): ...


def _source_revision() -> SourceRevision:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "log", "-1", "--format=%H%n%cI"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise EvidenceValidationError(
            result.stderr.strip() or "cannot resolve source HEAD"
        )
    head_sha, committed_at = result.stdout.strip().splitlines()
    return SourceRevision(
        head_sha=head_sha,
        committed_at=datetime.fromisoformat(committed_at.replace("Z", "+00:00")),
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
        source = _source_revision()
    except EvidenceValidationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    errors = validate_evidence_root(arguments.evidence_root, source)
    if errors:
        for issue in errors:
            print(f"ERROR: {issue}")
        return 1
    print("Validated 5 canonical manifests and 48 user-guide PNG artifacts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
