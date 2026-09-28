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
    assert "imagetools inspect" in text, "the digest must be resolved via imagetools, never hand-typed"
    assert "sha256:" not in text.split("imagetools inspect", 1)[0], (
        "no hand-typed digest before the first imagetools resolve"
    )


def test_repin_web_resolves_from_web_repo_main_not_the_ops_sha() -> None:
    text = REPIN_WEB.read_text()
    assert "dev-health-web/commits/main" in text, (
        "the web image must be resolved from dev-health-web's OWN main branch HEAD, "
        "independent of the ops NEW/OLD8 cut arguments"
    )
