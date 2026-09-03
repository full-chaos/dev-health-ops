"""`deploy/docker-compose/deploy-prod.sh` must never silently skip a
profile-gated service.

CHAOS-4976: prod's actual deploy was a plain `docker compose pull && up
-d` -- no `--profile` flag. Every service `compose.go-workers.yml` (and
`compose.production.yml`'s own `metrics-api`) gates behind `profiles:
[go-workers]` was therefore never pulled or restarted, even though those
containers were already running: the deploy reported success having done
nothing for the whole Go/River worker family. The measured symptom was a
Go-worker post-step's writes silently stopping after a deploy that never
actually reached the worker images at all.

WHAT THIS ASSERTS
------------------
1. The set of services gated behind `--profile go-workers` (derived from
   the REAL, live-interpolated compose config, not a hand-maintained
   list) is non-empty today, and `deploy-prod.sh --dry-run` names every
   one of them explicitly in its printed plan.
2. `deploy-prod.sh` never invokes a BARE `--profile go-workers up -d` (or
   `pull`) with no service names following -- the exact shape that would
   silently drop back to covering the whole profile implicitly instead of
   the explicit, derived-at-run-time list this guard exists to keep
   honest.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_DIR = ROOT / "deploy" / "docker-compose"
DEPLOY_SCRIPT = COMPOSE_DIR / "deploy-prod.sh"
PROD_COMPOSE = COMPOSE_DIR / "compose.production.yml"
GO_WORKERS_COMPOSE = COMPOSE_DIR / "compose.go-workers.yml"
PROFILE = "go-workers"

# `docker compose config` interpolates every `${VAR:?...}` required
# variable in both compose files before it will render anything -- these
# are never real credentials, just enough to make interpolation succeed
# so the SERVICE NAMES (which don't depend on the actual values) can be
# read. Sourced by grepping both compose files for every `${VAR:?...}`
# form; if a new required variable is added, `_compose_services` below
# fails loud (a `docker compose config` error, not a silent wrong
# answer) and this dict needs the new name added.
_DUMMY_ENV = {
    "POSTGRES_USER": "x",
    "POSTGRES_PASSWORD": "x",
    "POSTGRES_HOST": "x",
    "POSTGRES_DB": "x",
    "POSTGRES_URI": "x",
    "COORDINATOR_DATABASE_URI": "x",
    "WORKER_DATABASE_URI": "x",
    "RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
    "RIVER_QUEUE_DATABASE_PASSWORD": "x",
    "SETTINGS_ENCRYPTION_KEY": "x",
}

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker CLI not on PATH -- 'docker compose config' is client-side "
    "parsing (no daemon needed) but still needs the binary itself",
)


def _compose_services(profile: str | None) -> set[str]:
    """The service names `docker compose config` renders for
    compose.production.yml + compose.go-workers.yml, with or without
    `--profile go-workers` -- pure client-side YAML interpolation, no
    daemon contacted, no container touched."""
    import os

    args = [
        "docker",
        "compose",
        "-f",
        str(PROD_COMPOSE),
        "-f",
        str(GO_WORKERS_COMPOSE),
    ]
    if profile is not None:
        args += ["--profile", profile]
    args += ["config", "--services"]
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={**os.environ, **_DUMMY_ENV},
    )
    assert result.returncode == 0, (
        f"'docker compose config --services' (profile={profile!r}) exited "
        f"{result.returncode} -- stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _go_workers_only_services() -> set[str]:
    """The services that ONLY appear once `--profile go-workers` is
    added -- independently recomputed here, never trusted from the
    script's own claim, so a regression in the script's OWN derivation
    logic cannot silently agree with itself."""
    base = _compose_services(None)
    profiled = _compose_services(PROFILE)
    return profiled - base


def test_go_workers_profile_is_nonempty_and_the_deploy_script_names_every_service() -> (
    None
):
    go_workers_only = _go_workers_only_services()
    assert go_workers_only, (
        "no service resolved under --profile go-workers -- either "
        "compose.go-workers.yml is missing/unreadable, or every "
        "profiled service was removed from it. If this is intentional, "
        "this guard (and CHAOS-4976's whole premise) needs revisiting, "
        "not silently deleting"
    )

    import os

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
    missing = sorted(svc for svc in go_workers_only if svc not in result.stdout)
    assert not missing, (
        f"deploy-prod.sh --dry-run's printed plan does not name {missing} "
        "-- every service that only exists under --profile go-workers "
        "must appear explicitly in the plan, or a deploy running this "
        "script silently drops it the same way the pre-CHAOS-4976 plain "
        f"`pull && up -d` did. Full dry-run stdout:\n{result.stdout}"
    )


# A bare `--profile go-workers up -d` (or `pull`) with nothing after it is
# the EXACT regression this guard exists to prevent: covering the whole
# profile implicitly instead of naming each derived service. Matches a
# profile flag followed by `up -d`/`pull` and then end-of-line (allowing
# trailing whitespace), so a line that DOES name services afterward is not
# flagged.
_BARE_PROFILE_INVOCATION = re.compile(
    r'--profile\s+"?\$\{?PROFILE\}?"?\s+(up\s+-d(\s+--no-deps)?|pull)\s*$',
    re.MULTILINE,
)


def test_deploy_script_never_invokes_a_bare_profile_up_or_pull() -> None:
    """Static guard, independent of whether docker is even installed:
    the script's own source must never contain a `--profile go-workers
    up -d` (or `pull`) with no service names following on that same
    line -- the shape that silently reverts to covering the whole
    profile implicitly. Verified this row goes RED by removing the
    trailing `"${go_workers_only[@]}"` from a copy of the real up/pull
    lines before trusting it."""
    source = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    matches = _BARE_PROFILE_INVOCATION.findall(source)
    assert not matches, (
        "deploy-prod.sh contains a bare '--profile ... up -d'/'pull' with "
        f"no service names following: {matches!r} -- every profile-scoped "
        "compose invocation must name its services explicitly, derived at "
        "run time (see the module docstring)"
    )
