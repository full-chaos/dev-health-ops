"""`**/*.go` must match a root-level file in ci/go_relevance.py (its own test; goes with the script)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "changed,expected_relevant",
    [
        # `**/` matches ZERO OR MORE leading directories. Translating it as
        # `.*/` demanded at least one, so every ROOT-LEVEL Go file was judged
        # irrelevant and the gate skipped -- a false green produced by the
        # mechanism that replaced the vacuous no-op, on the change class most
        # likely to break the build.
        ("root.go", True),
        ("go.mod", True),
        ("go.sum", True),
        # ...and the nested forms it already handled must keep working.
        ("internal/x/y.go", True),
        ("cmd/dev-health-worker/main.go", True),
        ("testdata/a.json", True),
        ("internal/x/testdata/a.json", True),
        # The negative control: without it this only shows a decider that
        # returns true.
        ("docs/README.md", False),
    ],
)
def test_root_level_paths_match_the_double_star_prefix(
    changed: str, expected_relevant: bool
) -> None:
    """`**/*.go` must match `root.go`, not only `dir/root.go`."""

    result = subprocess.run(
        ["python3", str(ROOT / "ci" / "go_relevance.py")],
        input=changed + "\n",
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, f"go_relevance.py failed: {result.stderr[:300]}"
    relevant = "relevant=true" in result.stdout
    assert relevant is expected_relevant, (
        f"{changed!r}: relevant={relevant}, expected {expected_relevant}. "
        "A root-level Go change judged irrelevant skips the gate entirely."
    )
