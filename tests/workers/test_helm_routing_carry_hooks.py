"""CHAOS-7023: the chart's pre/post-upgrade go-api routing carry/repoint hooks.

Sibling of CHAOS-7022 (bigboy-cut.sh's own STEPs, ci/bigboy/bigboy-cut.sh). Both close the
same gap: go_api_routing_state rows are keyed by schema_digest, so a roll that changes the
GraphQL SDL un-routes every currently-enabled operation the instant the first new pod starts
unless carry has already copied those rows forward against the still-live pre-roll query-api
-- and a roll with NO schema change can still leave rows naming a build from a PRIOR roll if
nothing ever repoints them (the real rev196 prod roll's own gap, D2811).
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "routing-carry"

_CARRY = f"{_RELEASE}-dev-health-routing-carry"
_REPOINT = f"{_RELEASE}-dev-health-routing-repoint"

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")

_PINNED_TOOLS_IMAGE = (
    "ghcr.io/full-chaos/dev-health-go-api-tools"
    "@sha256:df5bb659aa5d38624de3c5f33cd66a929d9196f8baee07bb164c3d64b5772c38"
)
_MINT_ORG = "00000000-0000-0000-0000-000000000000"

_ENABLED = (
    "queryApi.enabled=true",
    f"migrations.hook.goApiRoutingTools.image={_PINNED_TOOLS_IMAGE}",
    f"migrations.hook.goApiRoutingTools.mintOrg={_MINT_ORG}",
)


def _render(*sets: str) -> list[dict]:
    argv = ["helm", "template", _RELEASE, str(_CHART)]
    for item in sets:
        argv += ["--set", item]
    completed = subprocess.run(argv, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def _render_stderr(*sets: str) -> tuple[int, str]:
    argv = ["helm", "template", _RELEASE, str(_CHART)]
    for item in sets:
        argv += ["--set", item]
    completed = subprocess.run(argv, capture_output=True, text=True)
    return completed.returncode, completed.stderr


def _jobs(*sets: str) -> dict[str, dict]:
    return {doc["metadata"]["name"]: doc for doc in _render(*sets) if doc.get("kind") == "Job"}


def _container(job: dict) -> dict:
    return job["spec"]["template"]["spec"]["containers"][0]


def _script(job: dict) -> str:
    return _container(job)["args"][0]


# --- gating: no knob to disable, but no live query-api means no premise -----


def test_absent_when_query_api_disabled() -> None:
    """queryApi.enabled=false by default -- carry's whole premise (a live process to copy
    FROM) does not exist, so neither hook may render even though there is no dedicated
    enabled/disabled flag for this gate."""
    jobs = _jobs()
    assert _CARRY not in jobs
    assert _REPOINT not in jobs


def test_absent_when_migrations_hook_disabled() -> None:
    jobs = _jobs("migrations.hook.enabled=false", *_ENABLED)
    assert _CARRY not in jobs
    assert _REPOINT not in jobs


def test_present_with_no_dedicated_enable_flag() -> None:
    """This is a gate, not an optional feature (card-7016.md WORK ORDER B): there is no
    migrations.hook.goApiRoutingTools.enabled to turn off, unlike routeActivate."""
    jobs = _jobs(*_ENABLED)
    assert _CARRY in jobs
    assert _REPOINT in jobs


# --- hook lifecycle: pre-upgrade for carry, post-upgrade for repoint, never pre-install -----


def test_carry_is_pre_upgrade_only() -> None:
    job = _jobs(*_ENABLED)[_CARRY]
    events = job["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert events == ["pre-upgrade"], (
        "carry must never run on pre-install -- there is no live query-api to carry FROM "
        f"on a fresh install, got {events}"
    )
    assert job["metadata"]["annotations"]["helm.sh/hook-delete-policy"] == "before-hook-creation"


def test_repoint_is_post_upgrade_only() -> None:
    job = _jobs(*_ENABLED)[_REPOINT]
    events = job["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert events == ["post-upgrade"]
    assert job["metadata"]["annotations"]["helm.sh/hook-delete-policy"] == "before-hook-creation"


# --- image pin, same bar as every other hook image ---------------------------------------


def test_floating_tag_is_refused() -> None:
    rc, stderr = _render_stderr(
        "queryApi.enabled=true",
        "migrations.hook.goApiRoutingTools.image=ghcr.io/full-chaos/dev-health-go-api-tools:latest",
        f"migrations.hook.goApiRoutingTools.mintOrg={_MINT_ORG}",
    )
    assert rc != 0
    assert "is not a pinned image" in stderr


def test_missing_mint_org_is_refused() -> None:
    rc, stderr = _render_stderr(
        "queryApi.enabled=true",
        f"migrations.hook.goApiRoutingTools.image={_PINNED_TOOLS_IMAGE}",
    )
    assert rc != 0
    assert "mintOrg is required" in stderr


# --- carry's own control flow: refuse-not-skip, with the one documented self-heal --------


def test_carry_script_treats_digest_agreement_as_a_pass() -> None:
    script = _script(_jobs(*_ENABLED)[_CARRY])
    assert "this binary's SDL is the one the deployed process already computes" in script
    assert "not a failure" in script or "nothing to carry" in script


def test_carry_script_has_the_repoint_then_retry_fallback() -> None:
    """The documented, real exception (rev196, both bigboy's re-cut and the actual prod
    roll): a refusal naming 'run repoint first' triggers ONE repoint-then-retry before
    anything is treated as fatal."""
    script = _script(_jobs(*_ENABLED)[_CARRY])
    assert "run repoint first" in script
    assert script.count("dho goapi routing repoint") == 1
    assert script.count("dho goapi routing carry") == 2, (
        "exactly two carry attempts: the first, and the one retry after repoint"
    )


def test_carry_script_aborts_on_any_other_refusal() -> None:
    script = _script(_jobs(*_ENABLED)[_CARRY])
    assert "ABORTING upgrade" in script
    assert "exit 1" in script


def test_repoint_script_is_unconditional() -> None:
    """No digest check, no if/elif branching -- provenance-only, safe every roll."""
    script = _script(_jobs(*_ENABLED)[_REPOINT])
    assert "if " not in script and "elif" not in script
    assert script.count("dho goapi routing repoint") == 1


# --- credential wiring: envelope key file, not an env var carrying the raw PEM -----------


def test_envelope_key_is_mounted_as_a_file_not_an_env_var() -> None:
    jobs = _jobs(*_ENABLED)
    for job in (jobs[_CARRY], jobs[_REPOINT]):
        container = _container(job)
        env_names = {e["name"] for e in container.get("env", [])}
        assert "GO_API_ENVELOPE_PRIVATE_KEY" not in env_names, (
            "the private key must reach `dho mint envelope -key-file` as a mounted file, "
            "not duplicated into a plaintext env var"
        )
        mount_names = {m["name"] for m in container.get("volumeMounts", [])}
        assert "envelope-key" in mount_names
        volumes = {v["name"]: v for v in job["spec"]["template"]["spec"]["volumes"]}
        assert volumes["envelope-key"]["secret"]["items"][0]["key"] == "GO_API_ENVELOPE_PRIVATE_KEY"


def test_postgres_uri_sourced_from_the_pgbouncer_secret() -> None:
    jobs = _jobs(*_ENABLED)
    for job in (jobs[_CARRY], jobs[_REPOINT]):
        container = _container(job)
        pg = next(e for e in container["env"] if e["name"] == "POSTGRES_URI")
        assert pg["valueFrom"]["secretKeyRef"]["key"] == "POSTGRES_URI"
        assert "go-pgbouncer" in pg["valueFrom"]["secretKeyRef"]["name"]
