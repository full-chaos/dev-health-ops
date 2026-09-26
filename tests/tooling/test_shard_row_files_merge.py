"""CHAOS-6926: the shard manifest and the venue weights merge without conflicts.

Every PR that added a Go package, re-timed one or added a venue registry row used to
edit neighbouring lines of one sorted flat file (`ci/go_integration_shards.tsv`,
`ci/venue_oracle_weights.tsv`), so any two open PRs conflicted and each cost a rebase,
a re-vet and a CI rerun. Both are directories of per-package row files now.

This test EXECUTES the claim: real git branches that (a) add a package, (b) add another
package that sorts next to it, (c) re-time the package that sorts right after them,
(d) re-time a distant package, and on the venue side (e) add a row to one package's
weights file, (f) add a row to another's, (g) add a whole new package file, all merge
into one result with no conflict, and the manifest planner accepts the merged result.
A control runs the SAME edits against the old flat layout and shows git conflicts
there, so this test cannot pass on a layout that has the old problem.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SHARDS = ROOT / "ci" / "go_integration_shards.d"
WEIGHTS = ROOT / "ci" / "venue_oracle_weights.d"

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None, reason="this test drives real git merges"
)


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.invalid",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
    }
    proc = subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        env=env,
    )
    if check:
        assert proc.returncode == 0, f"git {' '.join(args)}: {proc.stderr}{proc.stdout}"
    return proc


def _row_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.tsv"), key=lambda p: p.name.encode())


def _package_files() -> list[Path]:
    return [p for p in _row_files(SHARDS) if p.name != "_shards.tsv"]


def _commit_all(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


def _reweigh(path: Path, delta: int) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if line and not line.startswith("#") and "\t" in line:
            key, weight = line.split("\t")
            lines[index] = f"{key}\t{int(weight) + delta}"
            break
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _weights_rows(directory: Path) -> list[list[str]]:
    rows = []
    for file in _row_files(directory):
        for line in file.read_text(encoding="utf-8").splitlines():
            assert not line.startswith(("<<<<<<<", "=======", ">>>>>>>")), file
            if line.strip() and not line.startswith("#"):
                rows.append(line.split("\t"))
    return rows


def test_branches_that_add_and_retime_rows_merge_without_conflict(
    tmp_path: Path,
) -> None:
    packages = _package_files()
    assert len(packages) > 10, (
        "the manifest directory is empty: the test measured nothing"
    )
    added_a, added_b, neighbour = packages[20], packages[21], packages[22]
    distant = packages[100]
    venue = _row_files(WEIGHTS)
    assert len(venue) >= 4
    venue_x, venue_y = venue[1], venue[3]

    repo = tmp_path / "repo"
    (repo / "ci").mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    shutil.copytree(SHARDS, repo / "ci" / SHARDS.name)
    shutil.copytree(WEIGHTS, repo / "ci" / WEIGHTS.name)
    # The base lacks the two packages the branches add back.
    (repo / "ci" / SHARDS.name / added_a.name).unlink()
    (repo / "ci" / SHARDS.name / added_b.name).unlink()
    _commit_all(repo, "base")

    def branch(name: str, edit) -> None:
        _git(repo, "checkout", "-q", "-b", name, "main")
        edit()
        _commit_all(repo, name)
        _git(repo, "checkout", "-q", "main")

    shards = repo / "ci" / SHARDS.name
    weights = repo / "ci" / WEIGHTS.name
    branch("a-adds-a-package", lambda: shutil.copy(added_a, shards / added_a.name))
    branch(
        "b-adds-the-next-package", lambda: shutil.copy(added_b, shards / added_b.name)
    )
    branch("c-retimes-the-neighbour", lambda: _reweigh(shards / neighbour.name, 7))
    branch("d-retimes-a-distant-package", lambda: _reweigh(shards / distant.name, 3))

    def add_venue_row(target: Path, test: str) -> None:
        package = target.read_text(encoding="utf-8").split("\t")[0]
        with (weights / target.name).open("a", encoding="utf-8") as handle:
            handle.write(f"{package}\t{test}\t11\n")

    def new_venue_package() -> None:
        (weights / "internal__zzz__newvenue.tsv").write_text(
            "internal/zzz/newvenue\tTestNewVenueOracle\t600\tunmeasured\n",
            encoding="utf-8",
        )

    branch("e-adds-a-venue-row", lambda: add_venue_row(venue_x, "TestZZAddedByE"))
    branch(
        "f-adds-a-venue-row-elsewhere", lambda: add_venue_row(venue_y, "TestZZAddedByF")
    )
    branch("g-adds-a-venue-package", new_venue_package)

    for name in (
        "a-adds-a-package",
        "b-adds-the-next-package",
        "c-retimes-the-neighbour",
        "d-retimes-a-distant-package",
        "e-adds-a-venue-row",
        "f-adds-a-venue-row-elsewhere",
        "g-adds-a-venue-package",
    ):
        merged = _git(repo, "merge", "--no-edit", "-q", name, check=False)
        assert merged.returncode == 0, (
            f"merging {name} conflicted: {merged.stdout}{merged.stderr}"
        )

    # The merged manifest is the real one with exactly the two re-timings applied,
    # and the planner accepts it (same package set as discovery).
    merged_shards = repo / "ci" / SHARDS.name
    assert _row_files(merged_shards)[0].name == "_shards.tsv"
    real = {p.name: p.read_text(encoding="utf-8") for p in _row_files(SHARDS)}
    got = {p.name: p.read_text(encoding="utf-8") for p in _row_files(merged_shards)}
    assert sorted(got) == sorted(real)
    changed = sorted(name for name in real if real[name] != got[name])
    assert changed == sorted([neighbour.name, distant.name]), changed
    plan = subprocess.run(
        ["bash", str(ROOT / "ci" / "check_go.sh"), "integration-shard-plan"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env={
            **os.environ,
            "DEV_HEALTH_GO_INTEGRATION_SHARD_MANIFEST": str(merged_shards),
        },
        timeout=600,
    )
    assert plan.returncode == 0, plan.stderr[-1500:] + plan.stdout[-500:]

    # The merged venue weights carry every branch's row and no conflict marker.
    rows = {(r[0], r[1]) for r in _weights_rows(repo / "ci" / WEIGHTS.name)}
    assert any(test == "TestZZAddedByE" for _, test in rows)
    assert any(test == "TestZZAddedByF" for _, test in rows)
    assert ("internal/zzz/newvenue", "TestNewVenueOracle") in rows


def test_the_old_flat_layout_conflicts_on_the_same_edits(tmp_path: Path) -> None:
    """The control: the SAME kind of edits against ONE sorted flat file conflict.

    Two branches that each insert a row at the same place, and one that re-times the
    line next to an insertion, cannot merge cleanly in a single sorted file. If git ever
    merged these cleanly, the directory layout would no longer be the fix and this test
    would say so.
    """
    packages = _package_files()
    lines = [
        "\t".join(p.read_text(encoding="utf-8").strip().splitlines()[-1].split("\t"))
        for p in packages
    ]
    lines.sort()
    repo = tmp_path / "flat"
    (repo / "ci").mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    flat = repo / "ci" / "go_integration_shards.tsv"
    base = list(lines)
    del base[20:22]
    flat.write_text("shards\t6\n" + "\n".join(base) + "\n", encoding="utf-8")
    _commit_all(repo, "base")

    def edit_branch(name: str, edit) -> None:
        _git(repo, "checkout", "-q", "-b", name, "main")
        rows = flat.read_text(encoding="utf-8").splitlines()
        edit(rows)
        flat.write_text("\n".join(rows) + "\n", encoding="utf-8")
        _commit_all(repo, name)
        _git(repo, "checkout", "-q", "main")

    edit_branch("a", lambda rows: rows.insert(21, lines[20]))
    edit_branch("b", lambda rows: rows.insert(21, lines[21]))

    def retime(rows: list[str]) -> None:
        key, weight = rows[22].split("\t")
        rows[22] = f"{key}\t{int(weight) + 7}"

    edit_branch("c", retime)
    _git(repo, "merge", "--no-edit", "-q", "a")
    two_adds = _git(repo, "merge", "--no-edit", "-q", "b", check=False)
    _git(repo, "merge", "--abort", check=False)
    retimed = _git(repo, "merge", "--no-edit", "-q", "c", check=False)
    assert two_adds.returncode != 0 or retimed.returncode != 0, (
        "git merged the flat file's adjacent edits cleanly: the per-package layout "
        "would no longer be needed"
    )
