"""CHAOS-6993: shape guards for the bigboy GraphQL prove harness (no live stack here)."""

from __future__ import annotations

import subprocess
from pathlib import Path

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


def test_script_refuses_prover_build_skew_and_repoints_before_prove() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "go-api-tools:sha-$S7" in text and "REFUSED: venue-prove image" in text
    assert text.index("routing repoint") < text.index("dho goapi prove")
    assert "-candidate-build $NEW" in text and "-expect-build $NEW" in text


def test_credentials_never_on_argv_or_stdout() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "GO_API_PROVE_PROOF_BEARER" not in text
    assert 'echo "$PROVE_ORG' not in text and "echo $PROVE_ORG" not in text
    overlay = OVERLAY.read_text(encoding="utf-8")
    assert "JWT_SECRET_KEY: ${JWT_SECRET_KEY:?" in overlay
    assert "PROVE_ORG: ${PROVE_ORG:?" in overlay
