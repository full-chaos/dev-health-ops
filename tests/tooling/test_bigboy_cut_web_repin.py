"""bigboy-cut.sh: `web` is repinned to the CI image for web's own main branch HEAD every cut,
never rebuilt on the host (CHAOS-7019).

`web` is a LOCAL BUILD service in the root compose.yml (build: context: ./web), unlike
api/query-api/go-api which the bigboy overlay pins to CI-built ghcr.io digests. bigboy-cut.sh's
STEP sequence never touched `web` at all -- a web-only merge (or a web+ops merge landing
together, as CHAOS-6262 did) left a stale host build silently serving pages the backend
underneath had already changed shape for, and the first CHAOS-6262 web-path proof attempt hit
exactly this (a degraded "temporarily unavailable" panel instead of a real 404).
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"
REPIN_WEB = ROOT / "ci" / "bigboy" / "bigboy-repin-web.sh"


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines()


def _first(lines: list[str], needle: str, *, start: int = 0) -> int:
    for i in range(start, len(lines)):
        stripped = lines[i].lstrip()
        if needle in lines[i] and not stripped.startswith("#"):
            return i
    raise AssertionError(f"{needle!r} not found after line {start + 1}")


def test_web_repin_runs_after_repin_and_before_migrate_up() -> None:
    lines = _lines(CUT)
    repin_at = _first(lines, "bigboy-repin.sh")
    web_repin_at = _first(lines, "bigboy-repin-web.sh", start=repin_at)
    migrate_at = _first(lines, "--no-deps migrate", start=web_repin_at)
    assert repin_at < web_repin_at < migrate_at, (
        f"web repin (line {web_repin_at + 1}) must run after the ops image repin "
        f"(line {repin_at + 1}) and before migrate (line {migrate_at + 1})"
    )


def test_up_recreates_web_alongside_the_other_pinned_images() -> None:
    lines = _lines(CUT)
    up_at = _first(lines, "--no-deps --no-build api query-api go-api")
    up_line = lines[up_at]
    assert "--no-build" in up_line, "web must never be host-built"
    tokens = up_line.split()
    assert "web" in tokens[tokens.index("go-api") : tokens.index("go-api") + 2], (
        f"`web` must be recreated in the same up invocation as api/query-api/go-api: {up_line!r}"
    )


def test_repin_web_script_never_builds_on_the_host() -> None:
    text = REPIN_WEB.read_text()
    assert "compose build" not in text and " --build" not in text, (
        "bigboy-repin-web.sh must never invoke a host build -- digest repin only"
    )
    assert "imagetools inspect" in text, (
        "the digest must be resolved via imagetools, never hand-typed"
    )
    assert "sha256:" not in text.split("imagetools inspect", 1)[0], (
        "no hand-typed digest before the first imagetools resolve"
    )


def test_repin_web_resolves_from_web_repo_main_not_the_ops_sha() -> None:
    text = REPIN_WEB.read_text()
    assert "dev-health-web/commits/main" in text, (
        "the web image must be resolved from dev-health-web's OWN main branch HEAD, "
        "independent of the ops NEW/OLD8 cut arguments"
    )


# ---- CHAOS-7901: the caller picks the web build (WEB_REPIN=skip | head | <40 hex>), run for real with stubs ----

OLD_DIGEST = "sha256:" + "a" * 64
NEW_DIGEST = "sha256:" + "b" * 64
HEAD_SHA = "1" * 40
OTHER_SHA = "2" * 40


def _stub(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _run_repin(tmp_path: Path, *, repin: str | None, image_for: str | None = HEAD_SHA, extra=None):
    """Run bigboy-repin-web.sh against a fake bigboy root with a fake gh and docker on PATH.

    gh answers the web main HEAD sha and logs its call; docker answers an image digest only for the
    tag of `image_for` and logs its call. Returns (completed process, overlay text, calls log text)."""
    root = tmp_path / "root"
    (root / "compose").mkdir(parents=True)
    overlay = root / "compose" / "compose.bigboy.images.yml"
    overlay.write_text(f"services:\n  web:\n    image: ghcr.io/full-chaos/dev-health-web@{OLD_DIGEST}\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "calls.log"
    _stub(bindir / "gh", f'echo "gh $*" >> "{log}"\necho {HEAD_SHA}\n')
    tag = f"sha-{image_for[:7]}" if image_for else "sha-none"
    _stub(
        bindir / "docker",
        f'echo "docker $*" >> "{log}"\n'
        f'case "$*" in *"dev-health-web:{tag} "*) echo \'{{"digest":"{NEW_DIGEST}"}}\' ;; esac\n',
    )
    env = {
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "BIGBOY_ROOT": str(root),
        "WEB_REPIN_WAIT_TRIES": "1",
        "WEB_REPIN_WAIT_SECS": "0",
        **(extra or {}),
    }
    if repin is not None:
        env["WEB_REPIN"] = repin
    done = subprocess.run(
        ["bash", str(REPIN_WEB)], env=env, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60
    )
    return done, overlay.read_text(), log.read_text() if log.exists() else ""


def test_repin_web_skip_changes_nothing_and_calls_nothing(tmp_path: Path) -> None:
    done, overlay, calls = _run_repin(tmp_path, repin="skip")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "SKIPPED" in done.stdout and OLD_DIGEST in done.stdout
    assert OLD_DIGEST in overlay and NEW_DIGEST not in overlay
    assert calls == "", f"skip must not call gh or docker: {calls!r}"


def test_repin_web_explicit_sha_uses_that_commit_and_never_asks_for_main_head(tmp_path: Path) -> None:
    done, overlay, calls = _run_repin(tmp_path, repin=OTHER_SHA, image_for=OTHER_SHA)
    assert done.returncode == 0, done.stdout + done.stderr
    assert NEW_DIGEST in overlay and OLD_DIGEST not in overlay
    assert "gh " not in calls, f"an explicit sha must not resolve web main HEAD: {calls!r}"
    assert f"sha-{OTHER_SHA[:7]}" in calls


def test_repin_web_default_is_still_web_main_head(tmp_path: Path) -> None:
    for repin in (None, "head"):
        sub = tmp_path / f"case-{repin}"
        sub.mkdir()
        done, overlay, calls = _run_repin(sub, repin=repin, image_for=HEAD_SHA)
        assert done.returncode == 0, done.stdout + done.stderr
        assert NEW_DIGEST in overlay
        assert calls.startswith("gh api repos/full-chaos/dev-health-web/commits/main"), calls


def test_repin_web_refuses_other_values_before_any_call_or_write(tmp_path: Path) -> None:
    for bad in ("SKIP", "main", "abc123", OTHER_SHA[:39], OTHER_SHA.upper().replace("2", "A")):
        sub = tmp_path / f"bad-{abs(hash(bad))}"
        sub.mkdir()
        done, overlay, calls = _run_repin(sub, repin=bad)
        assert done.returncode == 2, (bad, done.returncode, done.stdout)
        assert OLD_DIGEST in overlay and NEW_DIGEST not in overlay
        assert calls == "", (bad, calls)


def test_repin_web_explicit_sha_without_an_image_fails_rc3_and_changes_nothing(tmp_path: Path) -> None:
    done, overlay, _ = _run_repin(tmp_path, repin=OTHER_SHA, image_for=HEAD_SHA)
    assert done.returncode == 3, done.stdout + done.stderr
    assert "no CI image" in done.stdout and "WEB_REPIN=" in done.stdout
    assert OLD_DIGEST in overlay and NEW_DIGEST not in overlay


def test_repin_web_wait_bounds_must_be_numbers(tmp_path: Path) -> None:
    done, overlay, calls = _run_repin(tmp_path, repin="head", extra={"WEB_REPIN_WAIT_TRIES": "forty"})
    assert done.returncode == 2 and calls == ""
    assert OLD_DIGEST in overlay


def test_cut_documents_the_web_repin_choice_next_to_the_call() -> None:
    lines = _lines(CUT)
    call_at = _first(lines, "bigboy-repin-web.sh")
    assert any("WEB_REPIN" in line for line in lines[max(0, call_at - 4) : call_at]), (
        "bigboy-cut.sh must document WEB_REPIN beside the repin-web call"
    )
