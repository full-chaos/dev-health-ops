"""Secret handling in the kind proof (CHAOS-10079): no credential in argv, manifests or describe output."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "check_auth_k8s.sh"
POSTGRES = ROOT / "ci" / "auth_k8s" / "postgres.yaml"
SECRET = re.compile(r"[0-9a-f]{24}")

HARNESS = """set -euo pipefail
SCRIPT_DIR="$1"; LOG="$2"; LOCAL_PG=img; LOCAL_SVC=svc
K() { printf 'ARGV %s\\n' "$*" >>"$LOG"; if [ ! -t 0 ]; then printf 'STDIN-BEGIN\\n' >>"$LOG"; cat >>"$LOG" || true; fi; }
K_RENDER() { envsubst '${LOCAL_PG} ${PG_PASSWORD}' <"$1"; }
"""


def _step2() -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    block = re.search(r"^PG_PASSWORD=.*?(?=^# --- the release)", text, re.DOTALL | re.MULTILINE)
    assert block, "step 2 of ci/check_auth_k8s.sh moved"
    return block.group(0).replace("openssl genpkey", "true genpkey")


@pytest.mark.skipif(shutil.which("envsubst") is None or shutil.which("openssl") is None, reason="envsubst/openssl")
def test_no_credential_reaches_an_argv_or_a_plain_manifest(tmp_path: Path) -> None:
    log = tmp_path / "log"
    script = tmp_path / "step2.sh"
    script.write_text(HARNESS + "WORK=" + str(tmp_path) + "\n" + _step2(), encoding="utf-8")
    result = subprocess.run(
        ["bash", str(script), str(ROOT / "ci"), str(log)],
        capture_output=True, text=True, check=False, stdin=subprocess.DEVNULL, env={**os.environ, "PATH": os.environ["PATH"]},
    )
    assert result.returncode == 0, result.stderr
    argv_lines = [ln for ln in log.read_text(encoding="utf-8").splitlines() if ln.startswith("ARGV ")]
    leaked = [ln for ln in argv_lines if SECRET.search(ln)]
    assert not leaked, f"credential in argv (visible to ps and audit tooling): {leaked}"
    for ln in argv_lines:
        assert "--from-literal" not in ln, ln


def test_postgres_password_is_a_secret_reference_not_a_literal() -> None:
    docs = list(yaml.safe_load_all(POSTGRES.read_text(encoding="utf-8")))
    env = docs[0]["spec"]["template"]["spec"]["containers"][0]["env"]
    password = next(e for e in env if e["name"] == "POSTGRES_PASSWORD")
    assert "value" not in password, "kubectl describe prints a literal env value into CI logs"
    assert "secretKeyRef" in password.get("valueFrom", {})
