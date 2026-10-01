"""ci/live_python_oracles.d: one file per live-Python oracle: its command AND its proofs.

CHAOS-7656. Every freeze PR used to delete its blocks from the same lines of
``ci/check_go.sh``, so each merge made the next freeze PR conflict. The entries
are now one small file each, read in sorted order; a freeze PR deletes its file
and never touches a shared line. A file holds the command and the proof markers
that prove it ran, so a proof cannot be dropped without its command (a measurement
that need not happen). These tests pin the reader: the resolved list
(``check_go.sh live-python-oracles --list``) changes by exactly the entry removed,
an empty or missing directory fails loudly, and a malformed entry fails instead of
being skipped.
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
        line
        for line in result.stdout.splitlines()
        if line.startswith(("RUN ", "PROOF "))
    ]


def _copy(tmp_path: Path) -> Path:
    target = tmp_path / "entries"
    shutil.copytree(ENTRIES, target)
    return target


def _fields(entry: Path) -> list[tuple[str, str]]:
    return [
        tuple(line.split("=", 1))  # type: ignore[misc]
        for line in entry.read_text().splitlines()
        if line
    ]


def _proofs(entry: Path) -> list[str]:
    return [value.split("|", 1)[0] for key, value in _fields(entry) if key == "proof"]


def test_the_resolved_list_has_every_entry_in_file_name_order() -> None:
    result = _list()
    assert result.returncode == 0, result.stderr
    resolved = _resolved(result)
    entries = sorted(ENTRIES.glob("*.run"))
    assert len(entries) >= 1
    expected_packages = [dict(_fields(entry))["package"] for entry in entries]
    assert [
        line.split(" package=")[1] for line in resolved if line.startswith("RUN ")
    ] == expected_packages
    expected_proofs = [name for entry in entries for name in _proofs(entry)]
    assert [
        line.split(" ")[1].removeprefix("name=")
        for line in resolved
        if line.startswith("PROOF ")
    ] == expected_proofs


def test_every_run_entry_names_a_package_that_exists_and_a_proof() -> None:
    for entry in sorted(ENTRIES.glob("*.run")):
        package = (
            dict(_fields(entry))["package"].removeprefix("./").removesuffix("/...")
        )
        assert (ROOT / package).is_dir(), (
            f"{entry.name} runs {package}, which does not exist: a stale entry "
            "runs nothing and its proof can never be written"
        )
        assert _proofs(entry), f"{entry.name} declares no proof"


def test_every_proof_name_is_unique_across_the_entries() -> None:
    names = [name for entry in sorted(ENTRIES.glob("*.run")) for name in _proofs(entry)]
    assert len(names) == len(set(names))


def test_removing_one_entry_changes_the_list_by_exactly_that_entry(
    tmp_path: Path,
) -> None:
    before = _resolved(_list())
    copy = _copy(tmp_path)
    victim = sorted(copy.glob("*.run"))[3]
    fields = dict(_fields(victim))
    proofs = _proofs(victim)
    victim.unlink()
    result = _list(copy)
    assert result.returncode == 0, result.stderr
    after = _resolved(result)
    removed = [line for line in before if line not in after]
    assert len(removed) == 1 + len(proofs), removed
    assert f"package={fields['package']}" in removed[0]
    assert [line.split(" ")[1] for line in removed[1:]] == [
        f"name={name}" for name in proofs
    ]
    assert after == [line for line in before if line in after]


def test_a_run_without_a_proof_fails_loudly(tmp_path: Path) -> None:
    copy = _copy(tmp_path)
    victim = sorted(copy.glob("*.run"))[5]
    victim.write_text(
        "".join(
            line + "\n"
            for line in victim.read_text().splitlines()
            if not line.startswith("proof")
        )
    )
    result = _list(copy)
    assert result.returncode != 0
    assert "declares no proof" in result.stderr


def test_an_empty_directory_fails_loudly(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    result = _list(empty)
    assert result.returncode != 0
    assert "holds no .run entry" in result.stderr


def test_a_missing_directory_fails_loudly(tmp_path: Path) -> None:
    result = _list(tmp_path / "does-not-exist")
    assert result.returncode != 0
    assert "missing or unreadable" in result.stderr


def test_a_malformed_entry_fails_instead_of_being_skipped(tmp_path: Path) -> None:
    cases = {
        "pakage=./internal/x\nproof=a|b\n": "unknown key",
        "label=nothing to run\nproof=a|b\n": "needs a package",
        "package=./internal/x\nproof=a|b\nproof=a|c\n": "listed twice",
        "package=./internal/x\nproof_match=zzz|^x$\n": "no earlier proof line",
    }
    for number, (text, message) in enumerate(cases.items()):
        copy = _copy(tmp_path / f"case{number}")
        sorted(copy.glob("*.run"))[0].write_text(text)
        result = _list(copy)
        assert result.returncode != 0, text
        assert message in result.stderr, (text, result.stderr)
