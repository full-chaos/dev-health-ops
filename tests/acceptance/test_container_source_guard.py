"""Regression coverage for CHAOS-3572: the ORDINARY-boot wrong-worktree guard.

CHAOS-3544 (#1582) gave the one-off mint script a host-checkout <->
container-served-source signature check. CHAOS-3572 is the same failure at a
wider blast radius: `compose.yml` is launched with `--project-directory
<ops_root>` and bind-mounts that directory at /app, so ANY boot -- not just a
mint -- can silently serve a different worktree's code. `docker ps` looks
healthy, the API answers, tests pass; the only symptom is that results
describe code the operator is not looking at.

This drives the REAL `container_source_guard_check` function in
`scripts/acceptance/container_source_guard.sh` through its `bash
container_source_guard.sh <ops_root> <compose_arg>...` test-only direct-
invocation entry point (same precedent as `--ch-probe-only` in
`ci/local_validate.sh`, see `tests/tooling/test_local_validate_stage_manifest.py`).
`docker` is always a PATH-shadowing stub -- no test here touches a real
container or daemon.

RED proof (recorded here, not re-asserted): before `container_source_guard.sh`
existed, `test_mismatched_signature_is_refused_with_exit_70_naming_both_paths`
below fails outright (`FileNotFoundError`/nonzero from a script that is not
there). Planting the wrong-worktree condition against the OLD boot path
(`run_ask_dev_compose.sh` before this change) never refuses at all -- the
launcher has no signature check between `up -d --build --wait` and the first
`fixtures world-restore`, so a container serving another checkout's code boots
straight through. Mutation check: delete the mismatch branch's `return 70` in
`container_source_guard_check` (or hardcode it to `return 0`) and this file's
mismatch test fails while the match test keeps passing -- the two tests
cannot both go green through a guard that stopped comparing anything.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUARD_SCRIPT = ROOT / "scripts" / "acceptance" / "container_source_guard.sh"
_BASE_PATH = "/usr/bin:/bin"
_TIMEOUT = 30

# Mirrors CONTAINER_SOURCE_GUARD_FILES in container_source_guard.sh. Kept as
# an independent literal (not imported/parsed out of the shell file) so this
# test cannot be satisfied by a guard that silently changed what it hashes --
# it fixes the fixture universe itself.
_GUARD_FILES = (
    "dev_health_ops/__init__.py",
    "dev_health_ops/fixtures/generators/interactions.py",
    "dev_health_ops/fixtures/ttl_horizon.py",
    "dev_health_ops/fixtures/world_snapshot.py",
    "dev_health_ops/fixtures/world.py",
)

_NON_DOCKER_TOOLS = ("bash", "shasum", "awk", "tr", "cat")


def _fake_ops_root(tmp_path: Path) -> Path:
    """A minimal host checkout: only the files the guard actually hashes."""
    ops_root = tmp_path / "ops-checkout"
    for rel in _GUARD_FILES:
        path = ops_root / "src" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        # Content includes the relative path so every file's bytes are
        # distinct -- a guard that hashed the same file five times over would
        # still "work" against identical fixtures.
        path.write_text(f"# fixture content for {rel}\n", encoding="utf-8")
    return ops_root


def _expected_signature(ops_root: Path) -> str:
    """Independently reproduces the host-side combination the guard's bash
    pipeline performs (`shasum -a 256 <file> | awk '{print $1}'` per file,
    then `shasum -a 256` over the concatenated per-file hash lines) -- NOT by
    calling into the guard, or this would just be tautological."""
    per_file_hashes = [
        hashlib.sha256((ops_root / "src" / rel).read_bytes()).hexdigest()
        for rel in _GUARD_FILES
    ]
    combined = ("\n".join(per_file_hashes) + "\n").encode("utf-8")
    return hashlib.sha256(combined).hexdigest()


def _fake_bin_dir(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for tool in _NON_DOCKER_TOOLS:
        found = shutil.which(tool)
        if found:
            (bin_dir / tool).symlink_to(found)
    return bin_dir


def _write_fake_docker(
    bin_dir: Path, *, container_signature: str, served_from: str
) -> None:
    """A PATH-shadowing `docker` stub standing in for BOTH `docker compose
    ... exec -T api python -c ...` (the container-side signature probe) and
    `docker inspect <id> --format ...` (the mismatch-path Mounts lookup) --
    the same narrow-fake-the-one-call-that-matters shape
    `test_local_validate_stage_manifest.py` uses for `docker ps`.
    """
    script = bin_dir / "docker"
    script.write_text(
        "#!/bin/bash\n"
        'args="$*"\n'
        'if [ "$1" = "compose" ]; then\n'
        '  case "$args" in\n'
        "    *' exec '*)\n"
        f"      printf '%s\\n' {_sh_quote(container_signature)}\n"
        "      exit 0\n"
        "      ;;\n"
        "    *' ps '*)\n"
        "      printf '%s\\n' fake-container-id\n"
        "      exit 0\n"
        "      ;;\n"
        "  esac\n"
        "  exit 0\n"
        "fi\n"
        'if [ "$1" = "inspect" ]; then\n'
        f"  printf '%s\\n' {_sh_quote(served_from)}\n"
        "  exit 0\n"
        "fi\n"
        "exit 0\n"
    )
    script.chmod(0o755)


def _sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def _run_guard(
    tmp_path: Path,
    ops_root: Path,
    *,
    container_signature: str,
    served_from: str = "/unused",
) -> subprocess.CompletedProcess:
    bin_dir = _fake_bin_dir(tmp_path)
    _write_fake_docker(
        bin_dir, container_signature=container_signature, served_from=served_from
    )
    compose_args = [
        "docker",
        "compose",
        "--project-name",
        "dev-health-ask-dev-acceptance",
        "--project-directory",
        str(ops_root),
        "-f",
        "compose.yml",
        "--profile",
        "ask-dev-acceptance",
    ]
    return subprocess.run(
        ["bash", str(GUARD_SCRIPT), str(ops_root), *compose_args],
        cwd=ROOT,
        env={"PATH": f"{bin_dir}:{_BASE_PATH}"},
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )


def test_matching_signature_is_accepted(tmp_path: Path) -> None:
    """Sanity check: a container genuinely serving this checkout must pass,
    or the mismatch test below would prove nothing about discrimination."""
    ops_root = _fake_ops_root(tmp_path)
    result = _run_guard(
        tmp_path, ops_root, container_signature=_expected_signature(ops_root)
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "matches this checkout" in combined, combined
    assert "REFUSING" not in combined, combined


def test_mismatched_signature_is_refused_with_exit_70_naming_both_paths(
    tmp_path: Path,
) -> None:
    """The CHAOS-3572 mechanism itself: plant a container whose source
    signature does NOT match the host checkout (the wrong-worktree
    condition) and observe a hard, loudly-worded refusal -- distinct exit
    code, both paths named, and the guard clause ordering print (verify
    ran) present.
    """
    ops_root = _fake_ops_root(tmp_path)
    wrong_worktree = tmp_path / "ops-worktrees" / "some-other-lane"
    result = _run_guard(
        tmp_path,
        ops_root,
        container_signature="0" * 64,  # cannot collide with a real sha256
        served_from=str(wrong_worktree),
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 70, combined
    assert "REFUSING" in combined, combined
    assert "verifying the api container is serving this checkout" in combined, (
        f"the check must actually have RUN, not merely concluded.\n{combined}"
    )
    assert str(ops_root) in combined, (
        f"the refusal must name the host checkout's path.\n{combined}"
    )
    assert str(wrong_worktree) in combined, (
        f"the refusal must name the worktree the container is actually "
        f"serving (via docker inspect's Mounts source) -- a signature "
        f"mismatch alone tells an operator THAT it is wrong, not WHICH "
        f"checkout it is instead.\n{combined}"
    )
    assert "matches this checkout" not in combined, combined


def test_guard_runs_before_reporting_a_verdict(tmp_path: Path) -> None:
    """The verifying line must precede either verdict line -- a guard that
    printed its conclusion before actually comparing anything would still
    satisfy naive substring checks."""
    ops_root = _fake_ops_root(tmp_path)
    result = _run_guard(
        tmp_path, ops_root, container_signature=_expected_signature(ops_root)
    )
    combined = result.stdout + result.stderr
    assert combined.index("verifying the api container is serving") < combined.index(
        "matches this checkout"
    ), combined
