"""CHAOS-6993: shape guards for the bigboy GraphQL prove harness (no live stack here)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "bigboy" / "bigboy-graphql-prove.sh"
OVERLAY = ROOT / "ci" / "bigboy" / "compose.bigboy.prove.yml"


def test_script_parses() -> None:
    assert (
        subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0
    )


def test_script_never_changes_query_api_posture() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    for forbidden in (
        "DEV_HEALTH_ENV",
        "GO_API_PROOF_ROUTE_ENABLED",
        "-proof-url",
        "-mode shadow",
        "up -d",
    ):
        assert forbidden not in text, forbidden


def test_script_refuses_prover_build_skew_and_runs_no_routing_verb() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "go-api-tools:sha-$S7" in text and "REFUSED: venue-prove image" in text
    assert "dho goapi routing" not in text
    assert "-candidate-build $NEW" in text


def test_credentials_never_on_argv_or_stdout() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "GO_API_PROVE_PROOF_BEARER" not in text
    assert 'echo "$PROVE_ORG' not in text and "echo $PROVE_ORG" not in text
    overlay = OVERLAY.read_text(encoding="utf-8")
    assert "JWT_SECRET_KEY: ${JWT_SECRET_KEY:?" in overlay
    assert "PROVE_ORG: ${PROVE_ORG:?" in overlay


SHA = "0123456789abcdef0123456789abcdef01234567"
DIGEST = "sha256:" + "ab" * 32

# A `docker` that records every call (one line, tab-separated argv) and answers the three
# read-only questions the harness asks before it proves: the tools image digest, the local
# org, and whatever a compose run prints.
DOCKER_STUB = f"""#!/usr/bin/env bash
{{ printf 'CALL'; printf '\\t%s' "$@"; printf '\\n'; }} >> "$STUB_LOG"
case "$1" in
  buildx) echo '{{"digest":"{DIGEST}"}}' ;;
  exec) cat > /dev/null; echo 11111111-1111-4111-8111-111111111111 ;;
  compose) echo 'stub compose run' ;;
esac
"""


def _run_harness(tmp_path: Path, *args: str) -> tuple[int, list[list[str]]]:
    """Run the harness in an isolated root with a recording docker and gh; return its exit
    status and every docker call it made."""
    root = tmp_path / "root"
    (root / "compose").mkdir(parents=True)
    (root / "compose" / "compose.bigboy.images.yml").write_text(
        f"services:\n  venue-prove:\n    image: ghcr.io/example/go-api-tools@{DIGEST}\n",
        encoding="utf-8",
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(DOCKER_STUB, encoding="utf-8")
    (bin_dir / "gh").write_text("#!/usr/bin/env bash\necho '{}'\n", encoding="utf-8")
    for stub in ("docker", "gh"):
        (bin_dir / stub).chmod(0o755)
    log = tmp_path / "docker-calls.log"
    log.touch()
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "BIGBOY_ROOT": str(root),
        "STUB_LOG": str(log),
        "ENABLE_OPS": " ",
    }
    done = subprocess.run(
        ["bash", str(SCRIPT), *args], capture_output=True, text=True, env=env
    )
    calls = [
        line.split("\t")[1:] for line in log.read_text(encoding="utf-8").splitlines()
    ]
    return done.returncode, calls


def _prove_command(calls: list[list[str]]) -> str:
    """The command string the harness hands to the venue-prove container."""
    proves = [call for call in calls if "venue-prove" in call]
    assert len(proves) == 1, f"want exactly one venue-prove run, got {len(proves)}"
    command = proves[0][-1]
    assert "dho goapi prove " in command and f"-candidate-build {SHA}" in command
    return command


def test_the_default_edge_is_the_routed_graphql_in_go_edge_mode(
    tmp_path: Path,
) -> None:
    """Run for real against a recording docker: with no second argument the prover is handed
    -go-edge and the routed /graphql (the stack has no Python edge)."""
    rc, calls = _run_harness(tmp_path, SHA)
    assert rc == 0, calls
    command = _prove_command(calls)
    assert " -go-edge -edge-url http://traefik:3000/graphql " in command
    assert "localhost:8000" not in command
    assert "through the routed /graphql in Go-edge mode," in command


def test_go_edge_hands_the_prover_the_routed_graphql_in_go_edge_mode(
    tmp_path: Path,
) -> None:
    """With --go-edge the prover is handed -go-edge and the routed /graphql, and the review
    evidence of every receipt says which edge it was."""
    rc, calls = _run_harness(tmp_path, SHA, "--go-edge")
    assert rc == 0, calls
    command = _prove_command(calls)
    assert " -go-edge -edge-url http://traefik:3000/graphql " in command
    assert "localhost:8000" not in command
    assert "through the routed /graphql in Go-edge mode," in command


@pytest.mark.parametrize("second", ["--goedge", "-go-edge", "go-edge", "--go-edge=1"])
def test_an_unknown_edge_argument_is_refused_before_anything_runs(
    tmp_path: Path, second: str
) -> None:
    """The edge is never guessed from a misspelt flag: usage, exit 2, and no docker call."""
    rc, calls = _run_harness(tmp_path, SHA, second)
    assert rc == 2 and calls == []
