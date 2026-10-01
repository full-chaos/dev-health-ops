"""ci/live_python_oracles.d: one file per live-Python oracle command and proof.

CHAOS-7656. Every freeze PR used to delete its blocks from the same lines of
``ci/check_go.sh``, so each merge made the next freeze PR conflict. The entries
are now one small file each, read in sorted order; a freeze PR deletes its files
and never touches a shared line. These tests pin the reader: the resolved list
(``check_go.sh live-python-oracles --list``) changes by exactly the entries
removed, an empty or missing directory fails loudly, and a malformed entry fails
instead of being skipped.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRIES = ROOT / "ci" / "live_python_oracles.d"


def _list(directory: Path | None = None) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    if directory is not None:
        env["LIVE_PYTHON_ORACLES_DIR"] = str(directory)
    return subprocess.run(
        ["bash", str(ROOT / "ci" / "check_go.sh"), "live-python-oracles", "--list"],
        capture_output=True,
        text=True,
        env=env,
        cwd=ROOT,
        timeout=120,
    )


def _resolved(result: subprocess.CompletedProcess[str]) -> list[str]:
    return [
        line for line in result.stdout.splitlines() if line.startswith(("RUN ", "PROOF "))
    ]


def _copy(tmp_path: Path) -> Path:
    target = tmp_path / "entries"
    shutil.copytree(ENTRIES, target)
    return target


def test_the_resolved_list_has_every_entry_in_sorted_order() -> None:
    result = _list()
    assert result.returncode == 0, result.stderr
    resolved = _resolved(result)
    runs = sorted(ENTRIES.glob("*.run"))
    proofs = sorted(ENTRIES.glob("*.proof"))
    assert len(runs) >= 1 and len(proofs) >= 1
    assert len(resolved) == len(runs) + len(proofs)
    assert [line.split(" ", 1)[0] for line in resolved] == ["RUN"] * len(runs) + [
        "PROOF"
    ] * len(proofs)
    # the order is the file names' order, not the directory's
    packages = [
        dict(line.split("=", 1) for line in entry.read_text().splitlines() if line)[
            "package"
        ]
        for entry in runs
    ]
    assert [line.split(" package=")[1] for line in resolved[: len(runs)]] == packages
    names = [
        dict(line.split("=", 1) for line in entry.read_text().splitlines() if line)[
            "name"
        ]
        for entry in proofs
    ]
    assert [line.split(" ")[1].removeprefix("name=") for line in resolved[len(runs) :]] == names


def test_every_run_entry_names_a_package_that_exists() -> None:
    for entry in sorted(ENTRIES.glob("*.run")):
        fields = dict(
            line.split("=", 1) for line in entry.read_text().splitlines() if line
        )
        package = fields["package"].removeprefix("./").removesuffix("/...")
        assert (ROOT / package).is_dir(), (
            f"{entry.name} runs {fields['package']}, which does not exist: a stale "
            "entry runs nothing and its proof can never be written"
        )


def test_every_proof_name_is_unique() -> None:
    names = [
        line.split("=", 1)[1]
        for entry in sorted(ENTRIES.glob("*.proof"))
        for line in entry.read_text().splitlines()
        if line.startswith("name=")
    ]
    assert len(names) == len(set(names))


def test_removing_one_entry_changes_the_list_by_exactly_that_entry(tmp_path: Path) -> None:
    before = _resolved(_list())
    copy = _copy(tmp_path)
    victim = sorted(copy.glob("*.run"))[3]
    victim_text = victim.read_text()
    victim.unlink()
    result = _list(copy)
    assert result.returncode == 0, result.stderr
    after = _resolved(result)
    removed = [line for line in before if line not in after]
    assert len(removed) == 1 and len(after) == len(before) - 1, removed
    package = dict(
        line.split("=", 1) for line in victim_text.splitlines() if line
    )["package"]
    assert f"package={package}" in removed[0]
    # and the remaining order is the old order
    assert after == [line for line in before if line in after]


def test_removing_one_proof_changes_the_list_by_exactly_that_proof(tmp_path: Path) -> None:
    before = _resolved(_list())
    copy = _copy(tmp_path)
    victim = sorted(copy.glob("*.proof"))[5]
    name = dict(line.split("=", 1) for line in victim.read_text().splitlines() if line)["name"]
    victim.unlink()
    after = _resolved(_list(copy))
    removed = [line for line in before if line not in after]
    assert len(removed) == 1 and f"name={name} " in removed[0], removed


def test_an_empty_directory_fails_loudly(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    result = _list(empty)
    assert result.returncode != 0
    assert "holds no .run entry" in result.stderr


def test_a_directory_with_runs_and_no_proof_fails_loudly(tmp_path: Path) -> None:
    copy = _copy(tmp_path)
    for proof in copy.glob("*.proof"):
        proof.unlink()
    result = _list(copy)
    assert result.returncode != 0
    assert "holds no .proof entry" in result.stderr


def test_a_missing_directory_fails_loudly(tmp_path: Path) -> None:
    result = _list(tmp_path / "does-not-exist")
    assert result.returncode != 0
    assert "missing or unreadable" in result.stderr


def test_a_malformed_entry_fails_instead_of_being_skipped(tmp_path: Path) -> None:
    copy = _copy(tmp_path)
    sorted(copy.glob("*.run"))[0].write_text("pakage=./internal/x\n")
    result = _list(copy)
    assert result.returncode != 0
    assert "unknown key" in result.stderr
    copy = _copy(tmp_path / "second")
    sorted(copy.glob("*.run"))[0].write_text("label=nothing to run\n")
    result = _list(copy)
    assert result.returncode != 0
    assert "needs a package" in result.stderr
    copy = _copy(tmp_path / "third")
    sorted(copy.glob("*.proof"))[0].write_text("name=x\n")
    result = _list(copy)
    assert result.returncode != 0
    assert "needs a name and a message" in result.stderr
