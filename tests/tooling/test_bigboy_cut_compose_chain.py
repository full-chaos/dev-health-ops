"""bigboy-cut.sh: the compose chain carries the router overlay (CHAOS-7131).

compose.bigboy.router.yml sets web's BACKEND_URL (the plane-split router) and AUTH_URL. The cut's
COMPOSE_FILE omitted it, so the `up` that recreates web built it from the base file alone: web lost
both names, REST calls returned 500 and the auth callback/logout used the wrong origin. These tests
resolve the chain the script actually exports, so removing the overlay fails them.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"
ROUTER = ROOT / "ci" / "bigboy" / "compose.bigboy.router.yml"
REQUIRED = ROOT / "ci" / "bigboy" / "check-web-env-required.sh"


def _chain() -> list[str]:
    match = re.search(r"^export COMPOSE_FILE=(\S+)$", CUT.read_text(), re.MULTILINE)
    assert match, "bigboy-cut.sh no longer exports COMPOSE_FILE"
    return match.group(1).split(":")


def test_chain_includes_the_router_overlay_last() -> None:
    chain = _chain()
    assert "$HERE/compose.bigboy.router.yml" in chain, chain
    assert chain[-1] == "$HERE/compose.bigboy.router.yml", (
        "the router overlay must come last so its web.environment wins"
    )


def test_resolved_web_service_carries_backend_url_and_auth_url(tmp_path: Path) -> None:
    """Materialize the chain the script exports (stub base files, the REAL router overlay) and
    let `docker compose config` merge it: web must come out with BACKEND_URL and AUTH_URL."""
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "compose.bigboy.router.yml").write_text(ROUTER.read_text())
    chain = [entry.replace("$HERE", str(tools)) for entry in _chain()]
    for entry in chain:
        if entry.startswith(str(tools)):
            continue
        target = tmp_path / entry
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry == "compose.yml":
            target.write_text(
                "services:\n"
                "  web:\n    image: web:test\n    environment:\n      BACKEND_URL: http://api:8000\n"
                "  traefik:\n    image: traefik:test\n"
            )
        else:
            target.write_text("services: {}\n")
    (tmp_path / ".traefik-dynamic").mkdir()
    proc = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "COMPOSE_FILE": ":".join(chain),
        },
    )
    assert proc.returncode == 0, proc.stderr
    web_env = json.loads(proc.stdout)["services"]["web"]["environment"]
    assert web_env["BACKEND_URL"] == "http://traefik:3000", web_env
    assert "AUTH_URL" in web_env, sorted(web_env)


def _run_required(tmp_path: Path, names: str | None, *required: str):
    file = tmp_path / "names"
    if names is not None:
        file.write_text(names)
    return subprocess.run(
        ["bash", str(REQUIRED), str(file), *required],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def test_required_names_present_passes(tmp_path: Path) -> None:
    proc = _run_required(
        tmp_path, "AUTH_URL\nBACKEND_URL\nNODE_ENV\n", "BACKEND_URL", "AUTH_URL"
    )
    assert proc.returncode == 0, proc.stderr


def test_missing_name_fails_and_is_named(tmp_path: Path) -> None:
    proc = _run_required(
        tmp_path, "NODE_ENV\nBACKEND_URL_OLD\n", "BACKEND_URL", "AUTH_URL"
    )
    assert proc.returncode == 1
    assert "BACKEND_URL" in proc.stderr and "AUTH_URL" in proc.stderr, proc.stderr


def test_unread_names_file_fails_loudly(tmp_path: Path) -> None:
    proc = _run_required(tmp_path, None, "BACKEND_URL")
    assert proc.returncode == 2, proc.stderr
    empty = _run_required(tmp_path, "", "BACKEND_URL")
    assert empty.returncode == 2, empty.stderr


def test_cut_checks_web_env_after_the_web_recreate() -> None:
    lines = CUT.read_text().splitlines()
    up = next(
        i for i, line in enumerate(lines) if "st up $?" in line and "go-api web" in line
    )
    check_at = next(
        i for i, line in enumerate(lines) if "check-web-env-required.sh" in line
    )
    assert check_at > up, "the web env check must run after the recreate it verifies"
    assert "BACKEND_URL AUTH_URL" in lines[check_at]
