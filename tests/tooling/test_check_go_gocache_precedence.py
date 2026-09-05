"""`ci/check_go.sh` must not overwrite an already-inherited GOCACHE.

WHY THIS TEST EXISTS (CHAOS-5224)
----------------------------------
`ci/check_go.sh` used to set ``GOCACHE`` unconditionally from
``${TMPDIR:-/tmp}/dev-health-go-build-cache``, ignoring any ``GOCACHE`` the
caller had already exported. On bigboy this grew a THIRD Go build cache on
the root disk (7.5G observed), separate from the two legitimate bind-mounted
caches (``~/.cache/go-build``, ``~/go/pkg/mod``). Exporting ``GOCACHE``
before calling the script was silently ineffective.

The fixed precedence: an explicit ``DEV_HEALTH_GO_CACHE`` wins first;
otherwise an already-inherited ``GOCACHE`` wins; only when NEITHER is set
does the script fall back to ``${TMPDIR:-/tmp}/dev-health-go-build-cache``.

WHAT THIS ASSERTS
-----------------
This extracts the actual precedence lines verbatim from ``ci/check_go.sh``
(not a reimplementation) and runs them under real ``bash``, with each of the
four (DEV_HEALTH_GO_CACHE, GOCACHE) presence combinations, asserting the
resulting ``GOCACHE`` and that the directory actually gets created.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK_GO = REPO_ROOT / "ci" / "check_go.sh"

START_MARKER = "DEV_HEALTH_GO_CACHE="
END_MARKER = 'export GOCACHE="${DEV_HEALTH_GO_CACHE}"'


def _gocache_precedence_snippet() -> str:
    """Return the script's own GOCACHE-precedence lines, verbatim."""
    lines = CHECK_GO.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(START_MARKER))
    end = next(i for i, line in enumerate(lines) if line.strip() == END_MARKER)
    assert start < end, (
        f"{CHECK_GO}: found START_MARKER at line {start + 1} after END_MARKER "
        f"at line {end + 1} -- the script's shape changed, re-derive the markers"
    )
    return "\n".join(lines[start : end + 1])


def _run(env: dict[str, str]) -> str:
    """Run the extracted snippet under real bash with exactly `env`, return
    the resulting GOCACHE value it exports."""
    script = _gocache_precedence_snippet() + '\nprintf "%s" "$GOCACHE"\n'
    proc = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )
    assert proc.returncode == 0, (
        f"snippet exited {proc.returncode}\nstdout={proc.stdout!r}\n"
        f"stderr={proc.stderr!r}"
    )
    return proc.stdout


def test_snippet_still_matches_the_expected_precedence_shape() -> None:
    """Guard the extraction itself: if check_go.sh's precedence expression
    ever regresses to skip straight to the tmp fallback, fail loudly here
    rather than silently testing stale text."""
    snippet = _gocache_precedence_snippet()
    assert "${DEV_HEALTH_GO_CACHE:-${GOCACHE:-" in snippet, (
        "ci/check_go.sh's DEV_HEALTH_GO_CACHE precedence no longer falls "
        "back through an inherited GOCACHE before the tmp default -- "
        f"CHAOS-5224 has regressed. Extracted snippet:\n{snippet}"
    )


def test_neither_set_falls_back_to_tmpdir_default(tmp_path: Path) -> None:
    env = {"PATH": "/usr/bin:/bin", "TMPDIR": str(tmp_path)}
    result = _run(env)
    expected = str(tmp_path / "dev-health-go-build-cache")
    assert result == expected, f"expected tmp fallback {expected!r}, got {result!r}"
    assert Path(result).is_dir(), f"expected {result!r} to have been mkdir -p'd"


def test_inherited_gocache_is_respected_not_overwritten(tmp_path: Path) -> None:
    """The CHAOS-5224 regression case: a caller-exported GOCACHE must win
    over the tmp fallback when DEV_HEALTH_GO_CACHE is not set."""
    inherited = tmp_path / "caller-chosen-cache"
    env = {
        "PATH": "/usr/bin:/bin",
        "TMPDIR": str(tmp_path / "unused-tmpdir"),
        "GOCACHE": str(inherited),
    }
    result = _run(env)
    assert result == str(inherited), (
        f"an inherited GOCACHE must be respected, not overwritten by the tmp "
        f"fallback: expected {inherited!r}, got {result!r}"
    )
    assert Path(result).is_dir(), f"expected {result!r} to have been mkdir -p'd"


def test_explicit_dev_health_go_cache_wins_over_inherited_gocache(
    tmp_path: Path,
) -> None:
    chosen = tmp_path / "explicitly-chosen-cache"
    inherited = tmp_path / "caller-chosen-cache"
    env = {
        "PATH": "/usr/bin:/bin",
        "TMPDIR": str(tmp_path / "unused-tmpdir"),
        "GOCACHE": str(inherited),
        "DEV_HEALTH_GO_CACHE": str(chosen),
    }
    result = _run(env)
    assert result == str(chosen), (
        f"an explicit DEV_HEALTH_GO_CACHE must win over an inherited GOCACHE: "
        f"expected {chosen!r}, got {result!r}"
    )
    assert Path(result).is_dir(), f"expected {result!r} to have been mkdir -p'd"


def test_explicit_dev_health_go_cache_wins_with_no_gocache_set(
    tmp_path: Path,
) -> None:
    chosen = tmp_path / "explicitly-chosen-cache"
    env = {
        "PATH": "/usr/bin:/bin",
        "TMPDIR": str(tmp_path / "unused-tmpdir"),
        "DEV_HEALTH_GO_CACHE": str(chosen),
    }
    result = _run(env)
    assert result == str(chosen), (
        f"expected the explicit DEV_HEALTH_GO_CACHE {chosen!r}, got {result!r}"
    )
    assert Path(result).is_dir(), f"expected {result!r} to have been mkdir -p'd"
