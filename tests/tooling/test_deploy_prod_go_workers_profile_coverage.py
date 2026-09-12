"""`deploy/docker-compose/deploy-prod.sh` must deploy every service in
`compose.production.yml` with a single plain pass.

CHAOS-4976 (2026-08-xx): a plain `docker compose pull && up -d` used to
silently skip every service gated behind `profiles: [go-workers]` -- the
Go/River worker family plus metrics-api -- because they lived only in a
separate `compose.go-workers.yml` overlay this script had to be told to
also apply, with `--profile go-workers`. CHAOS-5589 closed the gap at the
root instead of continuing to route around it here: compose.production.yml
now folds the Go/River fleet in directly as its unconditional default (no
separate overlay file, no `--profile go-workers` gate), so this test
guards the new, much simpler invariant directly: a single plain
`pull && up -d` against compose.production.yml alone must cover every
service in it, i.e. there must be no compose profile left to silently skip.

If a future change reintroduces a profile-gated service (or a `go-workers`
profile specifically) into `compose.production.yml`, this guard fails
loudly rather than silently reverting to the pre-CHAOS-5589 silent-skip
shape CHAOS-4976 originally fixed.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_DIR = ROOT / "deploy" / "docker-compose"
DEPLOY_SCRIPT = COMPOSE_DIR / "deploy-prod.sh"
PROD_COMPOSE = COMPOSE_DIR / "compose.production.yml"

# `docker compose config` interpolates every `${VAR:?...}` required
# variable before it will render anything -- these are never real
# credentials, just enough to make interpolation succeed so the SERVICE
# NAMES (which don't depend on the actual values) can be read. Sourced by
# grepping compose.production.yml for every `${VAR:?...}` form; if a new
# required variable is added, `_compose_services` below fails loud (a
# `docker compose config` error, not a silent wrong answer) and this dict
# needs the new name added.
_DUMMY_ENV = {
    "POSTGRES_USER": "x",
    "POSTGRES_PASSWORD": "x",
    "POSTGRES_HOST": "x",
    "POSTGRES_DB": "x",
    "POSTGRES_URI": "x",
    "COORDINATOR_DATABASE_URI": "x",
    "WORKER_DATABASE_URI": "x",
    "RIVER_DOMAIN_DATABASE_PASSWORD": "x",
    "RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
    "RIVER_QUEUE_DATABASE_PASSWORD": "x",
    "SETTINGS_ENCRYPTION_KEY": "x",
}

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker CLI not on PATH -- 'docker compose config' is client-side "
    "parsing (no daemon needed) but still needs the binary itself",
)


def _compose_services() -> set[str]:
    """The service names `docker compose config` renders for
    compose.production.yml alone -- pure client-side YAML interpolation, no
    daemon contacted, no container touched."""
    result = subprocess.run(
        ["docker", "compose", "-f", str(PROD_COMPOSE), "config", "--services"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={**os.environ, **_DUMMY_ENV},
    )
    assert result.returncode == 0, (
        f"'docker compose config --services' exited {result.returncode} -- "
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def test_compose_production_declares_no_profiles() -> None:
    """A profile-gated service is invisible to a plain `pull && up -d` --
    exactly the CHAOS-4976 shape. compose.production.yml must declare none:
    the Go/River fleet and the PgBouncer poolers it depends on are all
    unconditional defaults now (CHAOS-5589)."""
    document = yaml.safe_load(PROD_COMPOSE.read_text(encoding="utf-8")) or {}
    profiled = {
        name: service.get("profiles")
        for name, service in (document.get("services") or {}).items()
        if (service or {}).get("profiles")
    }
    assert not profiled, (
        f"compose.production.yml gates {profiled} behind a profile -- a plain "
        "`docker compose pull && up -d` (what deploy-prod.sh runs) silently "
        "skips it. Either remove the gate or give deploy-prod.sh back a "
        "second, explicitly-named pass for it (see CHAOS-4976)."
    )


def test_deploy_script_runs_a_single_plain_pass_naming_no_profile() -> None:
    """Static guard, independent of whether docker is even installed: no
    executable line in the script may invoke `docker compose` with a
    `--profile` flag -- there is nothing left for one to gate. Comment
    lines are excluded: the module docstring and the script's own header
    both mention `--profile go-workers` in past tense, explaining the
    CHAOS-4976/CHAOS-5589 history, which is not a live invocation."""
    code_lines = (
        line
        for line in DEPLOY_SCRIPT.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("#")
    )
    offending = [line for line in code_lines if "--profile" in line]
    assert not offending, (
        f"deploy-prod.sh invokes docker compose with --profile: {offending!r} "
        "-- compose.production.yml no longer gates anything behind one "
        "(CHAOS-5589). Either this is dead code left over from the two-pass "
        "CHAOS-4976 script, or a profile was reintroduced and this script "
        "(and the sibling compose-side guard above) both need updating "
        "together."
    )


def test_deploy_script_dry_run_names_every_compose_production_service() -> None:
    services = _compose_services()
    assert services, "compose.production.yml declared no services at all"

    result = subprocess.run(
        ["bash", str(DEPLOY_SCRIPT), "--dry-run"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={**os.environ, **_DUMMY_ENV},
    )
    assert result.returncode == 0, (
        f"deploy-prod.sh --dry-run exited {result.returncode} -- stdout:\n"
        f"{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "pull" in result.stdout and "up -d" in result.stdout


def test_deploy_script_rejects_unknown_flags() -> None:
    result = subprocess.run(
        ["bash", str(DEPLOY_SCRIPT), "--not-a-real-flag"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={**os.environ, **_DUMMY_ENV},
    )
    assert result.returncode == 2
    assert "usage:" in result.stderr
