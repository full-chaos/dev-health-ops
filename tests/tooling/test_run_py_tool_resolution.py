"""`scripts/run_py_tool.sh` must prefer the venv of the worktree it runs in.

WHY THIS TEST EXISTS
--------------------
Resolution order 2 was `git rev-parse --git-common-dir`, which from inside a
LINKED worktree points at the MAIN checkout. So a lane that had built its own
`.venv` -- as the lane brief instructs -- still got the main checkout's tools.

That is not merely a different interpreter, it is a different DEPENDENCY SET.
lefthook's `mypy` then type-checked the lane's source against libraries the lane
never installed: a `types-jsonschema` missing from the main checkout surfaced as
errors in files the author had not touched. That is exactly the CHAOS-3913
failure `run_py_tool.sh` was written to end, reappearing one level in, and the
script's own header asserted the thing that made it invisible -- that a linked
worktree "has no .venv of its own".

The bug was reachable only from a linked worktree, which is why no unit test
caught it and why this one builds a real one. It is the same lesson as the rest
of this repo's guards: a property that only holds in a configuration nobody
tests is a property nobody has.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "run_py_tool.sh"


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def _fake_tool(venv: Path, marker: str) -> None:
    """A `mypy` that identifies which venv it came from."""
    binary = venv / "bin" / "mypy"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text(f"#!/bin/sh\necho {marker}\n", encoding="utf-8")
    binary.chmod(0o755)


@pytest.fixture
def linked_worktree(tmp_path: Path) -> tuple[Path, Path]:
    """A real main checkout plus a real linked worktree, both with the script."""
    main = tmp_path / "main"
    main.mkdir()
    _git("init", "-q", cwd=main)
    _git("config", "user.email", "t@t", cwd=main)
    _git("config", "user.name", "t", cwd=main)
    (main / "scripts").mkdir()
    (main / "scripts" / "run_py_tool.sh").write_text(
        SCRIPT.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (main / "scripts" / "run_py_tool.sh").chmod(0o755)
    (main / "seed").write_text("x", encoding="utf-8")
    _git("add", "-A", cwd=main)
    _git("commit", "-qm", "init", cwd=main)

    linked = tmp_path / "linked"
    _git("worktree", "add", "--detach", str(linked), "HEAD", cwd=main)
    return main, linked


def _run(worktree: Path, environment: dict[str, str] | None = None) -> str:
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    env.update(environment or {})
    result = subprocess.run(
        [str(worktree / "scripts" / "run_py_tool.sh"), "mypy"],
        cwd=worktree,
        capture_output=True,
        text=True,
        env=env,
    )
    return result.stdout.strip()


def test_a_linked_worktree_uses_its_own_venv(
    linked_worktree: tuple[Path, Path],
) -> None:
    """The regression. Both venvs exist; the LOCAL one must win.

    Before the fix this returned MAIN, because --git-common-dir resolves to the
    main checkout from inside a linked worktree.
    """
    main, linked = linked_worktree
    _fake_tool(main / ".venv", "MAIN")
    _fake_tool(linked / ".venv", "LINKED")

    assert _run(linked) == "LINKED", (
        "a linked worktree with its own .venv must use it; resolving to the main "
        "checkout's .venv silently substitutes a different dependency set for the "
        "one this lane installed"
    )


def test_a_linked_worktree_without_a_venv_still_finds_the_shared_one(
    linked_worktree: tuple[Path, Path],
) -> None:
    """The fallback the original order existed for, which must survive."""
    main, linked = linked_worktree
    _fake_tool(main / ".venv", "MAIN")

    assert _run(linked) == "MAIN"


def test_an_activated_virtualenv_still_wins(
    linked_worktree: tuple[Path, Path],
) -> None:
    """Order 1 outranks both: an explicit activation is always intentional."""
    main, linked = linked_worktree
    _fake_tool(main / ".venv", "MAIN")
    _fake_tool(linked / ".venv", "LINKED")
    activated = linked.parent / "activated"
    _fake_tool(activated, "ACTIVATED")

    assert _run(linked, {"VIRTUAL_ENV": str(activated)}) == "ACTIVATED"


def test_the_main_checkout_is_unaffected(
    linked_worktree: tuple[Path, Path],
) -> None:
    """In the main checkout both anchors agree, so behaviour is unchanged."""
    main, _ = linked_worktree
    _fake_tool(main / ".venv", "MAIN")

    assert _run(main) == "MAIN"
