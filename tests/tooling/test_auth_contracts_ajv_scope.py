"""The scope step of the Auth Contracts ajv workflow must fail closed.

A failed ``git diff`` read as "nothing changed" would skip the whole ajv check
and leave a green job, so every command failure has to fail the step.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "auth-contracts-ajv" / "scope.sh"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        env={
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.test",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.test",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "PATH": "/usr/bin:/bin:/usr/local/bin",
        },
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q")
    (tmp_path / "a.txt").write_text("a")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "base")
    return tmp_path


def _commit(repo: Path, relative: str) -> tuple[str, str]:
    base = _git(repo, "rev-parse", "HEAD")
    target = repo / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "change")
    return base, _git(repo, "rev-parse", "HEAD")


def _run(
    repo: Path, tmp: Path, **env: str
) -> tuple[subprocess.CompletedProcess[str], str]:
    out = tmp / "github_output"
    out.write_text("")
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=repo,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", "GITHUB_OUTPUT": str(out), **env},
    )
    return result, out.read_text()


def test_a_contract_change_requires_the_check(repo: Path, tmp_path: Path) -> None:
    base, head = _commit(repo, "contracts/auth/v1/jsonschema/x.json")
    result, output = _run(
        repo, tmp_path, EVENT="pull_request", BASE_SHA=base, HEAD_SHA=head
    )
    assert result.returncode == 0, result.stderr
    assert output.strip() == "run=true"


def test_an_unrelated_change_skips_the_check(repo: Path, tmp_path: Path) -> None:
    base, head = _commit(repo, "docs/other.md")
    result, output = _run(
        repo, tmp_path, EVENT="pull_request", BASE_SHA=base, HEAD_SHA=head
    )
    assert result.returncode == 0, result.stderr
    assert output.strip() == "run=false"


def test_a_push_always_runs_the_check(repo: Path, tmp_path: Path) -> None:
    result, output = _run(repo, tmp_path, EVENT="push")
    assert result.returncode == 0, result.stderr
    assert output.strip() == "run=true"


@pytest.mark.parametrize(
    "env",
    [
        {"EVENT": "pull_request", "BASE_SHA": "0" * 40, "HEAD_SHA": "1" * 40},
        {"EVENT": "pull_request", "BASE_SHA": "", "HEAD_SHA": ""},
        {"EVENT": "pull_request"},
    ],
    ids=["unknown-shas", "empty-shas", "missing-shas"],
)
def test_a_failed_diff_fails_the_step_instead_of_skipping_the_check(
    repo: Path, tmp_path: Path, env: dict[str, str]
) -> None:
    result, output = _run(repo, tmp_path, **env)
    assert result.returncode != 0, (
        "the scope step swallowed the failure and reported: " + output
    )
    assert "run=false" not in output
