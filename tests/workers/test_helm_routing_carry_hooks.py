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

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)

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


def _render(*sets: str, is_upgrade: bool = True) -> list[dict]:
    # D2830: these two hook Jobs are pre-upgrade/post-upgrade ONLY -- Helm never creates or
    # runs them on an install, so the chart now gates the whole block (Jobs + the image-pin/
    # mintOrg/lockstep checks) on `.Release.IsUpgrade`, not `queryApi.enabled` alone.
    # `--is-upgrade` simulates a real `helm upgrade` so this suite keeps exercising that
    # gated block; default True since every existing test here means to.
    argv = ["helm", "template", _RELEASE, str(_CHART)]
    if is_upgrade:
        argv.append("--is-upgrade")
    for item in sets:
        argv += ["--set", item]
    completed = subprocess.run(argv, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def _render_stderr(*sets: str, is_upgrade: bool = True) -> tuple[int, str]:
    argv = ["helm", "template", _RELEASE, str(_CHART)]
    if is_upgrade:
        argv.append("--is-upgrade")
    for item in sets:
        argv += ["--set", item]
    completed = subprocess.run(argv, capture_output=True, text=True)
    return completed.returncode, completed.stderr


def _jobs(*sets: str) -> dict[str, dict]:
    return {
        doc["metadata"]["name"]: doc
        for doc in _render(*sets)
        if doc.get("kind") == "Job"
    }


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


def test_present_even_when_migrations_hook_disabled() -> None:
    """D2829 P2-a (#3369 r1 P2): these two Jobs gate on `queryApi.enabled` ONLY. They are
    not migrations (values.yaml's migrations.hook.goApiRoutingTools comment: no
    enable/disable knob, this is a gate) -- an operator disabling migrations.hook for an
    unrelated reason (e.g. a release with no schema change to apply) must not silently
    disable the routing safety gate as a side effect. Before this fix, BOTH hooks were
    absent whenever migrations.hook.enabled=false, regardless of queryApi.enabled."""
    jobs = _jobs("migrations.hook.enabled=false", *_ENABLED)
    assert _CARRY in jobs, (
        "the carry hook must still render with migrations.hook.enabled=false as long as "
        "queryApi.enabled=true -- it gates on queryApi.enabled only"
    )
    assert _REPOINT in jobs, (
        "the repoint hook must still render with migrations.hook.enabled=false as long as "
        "queryApi.enabled=true -- it gates on queryApi.enabled only"
    )


def test_present_with_no_dedicated_enable_flag() -> None:
    """This is a gate, not an optional feature (card-7016.md WORK ORDER B): there is no
    migrations.hook.goApiRoutingTools.enabled to turn off, unlike routeActivate."""
    jobs = _jobs(*_ENABLED)
    assert _CARRY in jobs
    assert _REPOINT in jobs


def test_absent_and_no_pin_failure_on_an_install_shape_render() -> None:
    """D2830 (#3369 collateral regression): these two Jobs are pre-upgrade/post-upgrade
    ONLY -- Helm never creates or runs them on an install, so an install-shape render
    (no `--is-upgrade`, matching real `helm install` and any OTHER chart test that renders
    with queryApi.enabled=true for an unrelated reason, e.g. deploy/helm/dev-health's own
    query_api_subcommand_test.go) must not even evaluate the image-pin/mintOrg/lockstep
    `fail` checks -- not just omit the Jobs. Before this fix, `queryApi.enabled=true` alone
    (with no `migrations.hook.goApiRoutingTools.image` set) failed the WHOLE render with
    'is not a pinned image', breaking every other chart test that happened to also set
    queryApi.enabled=true."""
    completed = subprocess.run(
        ["helm", "template", _RELEASE, str(_CHART), "--set", "queryApi.enabled=true"],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    docs = [doc for doc in yaml.safe_load_all(completed.stdout) if doc]
    job_names = {doc["metadata"]["name"] for doc in docs if doc.get("kind") == "Job"}
    assert _CARRY not in job_names
    assert _REPOINT not in job_names


# --- hook lifecycle: pre-upgrade for carry, post-upgrade for repoint, never pre-install -----


def test_carry_is_pre_upgrade_only() -> None:
    job = _jobs(*_ENABLED)[_CARRY]
    events = job["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert events == ["pre-upgrade"], (
        "carry must never run on pre-install -- there is no live query-api to carry FROM "
        f"on a fresh install, got {events}"
    )
    assert (
        job["metadata"]["annotations"]["helm.sh/hook-delete-policy"]
        == "before-hook-creation"
    )


def test_repoint_is_post_upgrade_only() -> None:
    job = _jobs(*_ENABLED)[_REPOINT]
    events = job["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert events == ["post-upgrade"]
    assert (
        job["metadata"]["annotations"]["helm.sh/hook-delete-policy"]
        == "before-hook-creation"
    )


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


def test_tools_image_pinned_to_a_different_commit_than_query_api_is_refused() -> None:
    """D2829 P2-b (#3369 r1 P2): the tools image's commit must be cross-checked against
    `queryApi.image` (the image actually being rolled), not `.Values.image` (the Python
    api image -- what `dev-health.lockstepImageCheck` hardcodes). Before this fix, a tools
    image and a query-api image pinned to DIFFERENT commits rendered clean as long as they
    each happened to agree with the unrelated Python api image (or didn't have one to
    disagree with at all) -- `carry` would then compute a target digest for a commit the
    actual roll never runs."""
    rc, stderr = _render_stderr(
        "queryApi.enabled=true",
        "queryApi.image.tag=sha-aaaaaaaaaaaa",
        "migrations.hook.goApiRoutingTools.image=ghcr.io/full-chaos/dev-health-go-api-tools:sha-bbbbbbbbbbbb",
        f"migrations.hook.goApiRoutingTools.mintOrg={_MINT_ORG}",
    )
    assert rc != 0, "a tools/query-api commit mismatch must fail the render"
    assert "are pinned to different commits" in stderr
    assert "queryApi.image" in stderr


def test_tools_image_pinned_to_the_same_commit_as_query_api_renders_clean() -> None:
    """Positive control for the test above: proves the lockstep check compares against
    queryApi.image (and passes when they DO agree), not that it always fails."""
    rc, stderr = _render_stderr(
        "queryApi.enabled=true",
        "queryApi.image.tag=sha-cccccccccccc",
        "migrations.hook.goApiRoutingTools.image=ghcr.io/full-chaos/dev-health-go-api-tools:sha-cccccccccccc",
        f"migrations.hook.goApiRoutingTools.mintOrg={_MINT_ORG}",
    )
    assert rc == 0, (
        f"a matching tools/query-api commit must render clean, got: {stderr}"
    )


# --- carry's own control flow: refuse-not-skip, with the one documented self-heal --------


def test_carry_script_treats_digest_agreement_as_a_pass() -> None:
    """D2828/D2829: the digest-agreement branch keys off `dho ... -json`'s `reason` field
    ("digest_unchanged"), extracted with grep+sed (no `jq` in this image -- see the chart's
    own file-level comment), never off the raw error TEXT. That text was wrong twice during
    this ticket's own review before landing on this structural fix -- see
    tests/tooling/test_bigboy_cut_carry_refusal_text_behavior.py (CHAOS-7022) for the
    end-to-end proof against the REAL dho binary and a real fake registry server."""
    script = _script(_jobs(*_ENABLED)[_CARRY])
    assert '"$REASON" = "digest_unchanged"' in script, (
        "the digest-unchanged branch must key off -json's reason field, not off any "
        "particular error text"
    )
    assert "not a failure" in script or "nothing to carry" in script


def test_carry_script_has_the_repoint_then_retry_fallback() -> None:
    """The documented, real exception (rev196, both bigboy's re-cut and the actual prod
    roll): a refusal naming the stale-build reason triggers ONE repoint-then-retry before
    anything is treated as fatal.

    D2828/D2829: keyed off -json's `reason="stale_build"`, not off carry's raw error text.
    """
    script = _script(_jobs(*_ENABLED)[_CARRY])
    assert '"$REASON" = "stale_build"' in script, (
        "the stale-build repoint-then-retry branch must key off -json's reason field, not "
        "off any particular error text"
    )
    assert script.count("dho goapi routing repoint") == 1
    assert script.count("dho goapi routing carry") == 2, (
        "exactly two carry attempts: the first, and the one retry after repoint"
    )


def test_carry_script_aborts_on_any_other_refusal() -> None:
    script = _script(_jobs(*_ENABLED)[_CARRY])
    assert "ABORTING upgrade" in script
    assert "exit 1" in script


def test_repoint_script_is_unconditional() -> None:
    """No digest check, no if/elif branching -- provenance-only, safe every roll. The write
    itself is unconditional once the rollout wait (a dry-run probe loop, see
    test_helm_routing_repoint_waits_for_rollout.py) has passed: exactly one real repoint."""
    script = _script(_jobs(*_ENABLED)[_REPOINT])
    assert "if " not in script and "elif" not in script
    calls = [
        line
        for line in script.replace("\\\n", " ").splitlines()
        if "dho goapi routing repoint" in line
    ]
    assert len(calls) == 2, calls
    assert sum("-dry-run" in call for call in calls) == 1, calls


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
        assert (
            volumes["envelope-key"]["secret"]["items"][0]["key"]
            == "GO_API_ENVELOPE_PRIVATE_KEY"
        )


def test_postgres_uri_sourced_from_the_registry_role_secret() -> None:
    """D2831 (#3369 r2 P1, reproduced live: 'permission denied for table
    go_api_routing_state'): the Go pgbouncer Secret's POSTGRES_URI is the
    RIVER_DOMAIN_DATABASE_ROLE DSN -- values.yaml's own comment on
    GO_API_REGISTRY_POSTGRES_URI says granting these tables to that role is
    exactly what must NOT happen ("the shape of a prior multi-day outage").
    These hooks must source POSTGRES_URI from the SAME Secret+key
    query-api-deployment.yaml already uses for the role that owns the
    registry tables, never the pgbouncer Secret."""
    jobs = _jobs(*_ENABLED)
    for job in (jobs[_CARRY], jobs[_REPOINT]):
        container = _container(job)
        pg = next(e for e in container["env"] if e["name"] == "POSTGRES_URI")
        assert pg["valueFrom"]["secretKeyRef"]["key"] == "GO_API_REGISTRY_POSTGRES_URI"
        assert "go-pgbouncer" not in pg["valueFrom"]["secretKeyRef"]["name"]
        # same Secret the envelope-key volume mounts from -- one shared Secret,
        # not a second credential source.
        volumes = {v["name"]: v for v in job["spec"]["template"]["spec"]["volumes"]}
        assert (
            pg["valueFrom"]["secretKeyRef"]["name"]
            == volumes["envelope-key"]["secret"]["secretName"]
        )


@pytest.mark.parametrize("job_name", [_CARRY, _REPOINT])
def test_hook_pod_pins_a_numeric_uid_so_run_as_non_root_is_verifiable(
    job_name: str,
) -> None:
    """The tools image's `USER toolsuser` is a name. With runAsNonRoot and no numeric
    runAsUser the kubelet cannot verify the user and refuses the pod
    (CreateContainerConfigError: "image has non-numeric user (toolsuser)"), which stalled a
    pre-upgrade hook until Helm's deadline. Both hook Jobs must pin a numeric uid and gid."""
    pod = _jobs(*_ENABLED)[job_name]["spec"]["template"]["spec"]
    context = pod["securityContext"]
    assert context["runAsNonRoot"] is True
    assert isinstance(context.get("runAsUser"), int) and context["runAsUser"] > 0, (
        context
    )
    assert isinstance(context.get("runAsGroup"), int) and context["runAsGroup"] > 0, (
        context
    )


def test_tools_dockerfile_user_is_the_numeric_uid_the_chart_pins() -> None:
    dockerfile = (
        Path(__file__).resolve().parents[2] / "docker" / "go-api-tools.Dockerfile"
    ).read_text()
    users = [
        line.split(None, 1)[1].strip()
        for line in dockerfile.splitlines()
        if line.startswith("USER ")
    ]
    assert users and users[-1] == "10001:10001", users
    pod = _jobs(*_ENABLED)[_CARRY]["spec"]["template"]["spec"]["securityContext"]
    assert users[-1] == f"{pod['runAsUser']}:{pod['runAsGroup']}"
