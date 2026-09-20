"""Reader for ci/go_integration_shards.tsv that parses it exactly as the planner does.

ci/check_go.sh (load_integration_shard_manifest) reads the manifest with
`IFS=$'\t ' read -r key value extra`: fields split on runs of tabs AND spaces,
leading and trailing whitespace ignored, a row whose first field is empty or
starts with `#` skipped. Any other reading of the file here would let the tests
disagree with the planner about what the manifest contains, so this is the one
place the Python side reads it.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "ci" / "go_integration_shards.tsv"


def manifest_rows(path: Path = MANIFEST) -> list[list[str]]:
    """Data rows in file order, split like the planner's `read -r key value extra`."""
    rows: list[list[str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if not fields or fields[0].startswith("#"):
            continue
        rows.append(fields)
    return rows


def manifest_packages(path: Path = MANIFEST) -> set[str]:
    """Package keys (every data row except the `shards` row)."""
    rows = manifest_rows(path)
    assert rows and rows[0][0] == "shards", rows[:1]
    return {row[0] for row in rows[1:]}


if __name__ == "__main__":
    # `python -m tests.tooling.go_integration_manifest` lists the package keys the
    # tests will expect, for comparing against `ci/check_go.sh integration-shard-plan`.
    print("\n".join(sorted(manifest_packages())))
