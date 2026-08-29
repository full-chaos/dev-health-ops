"""The migration chain must render in the order the running system uses.

CHAOS-4428. The chart used to render ONE migrate Job; role provisioning and the
River schema were run by hand, and their order is load-bearing:

    weight  0   …-migrate            Alembic + ClickHouse
    weight  5   …-provision-roles    the three runtime logins
    weight 10   …-river-migrate      River schema + the full grant posture

River's preflight needs the three logins to exist, so it must follow
provisioning. Get the order wrong and every Go worker fails readiness with
`failed_checks:"domain_postgres"`, which names the symptom and not the cause.

Both new hooks run on pre-install AND pre-upgrade. An earlier design ran
provisioning on pre-install only, believing it would revoke what River grants.
That is false on current code and the evidence is executed, not argued
(production deploy 5.5, 2026-08-28: 116 grants -> 119 -> 119 across three
passes, no revoke; provision_river_roles.sql:87-105 is bootstrap-only since
CHAOS-4261; applyRuntimeGrants is unconditional at river/migrate.go:196-211).
The `both events` assertions below are what stop that belief being
reintroduced.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "dev-health"
_RELEASE = "hook-chain"

_MIGRATE = f"{_RELEASE}-dev-health-migrate"
_PROVISION = f"{_RELEASE}-dev-health-provision-roles"
_RIVER = f"{_RELEASE}-dev-health-river-migrate"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _render(*sets: str) -> list[dict]:
    argv = ["helm", "template", _RELEASE, str(_CHART)]
    for item in sets:
        argv += ["--set", item]
    completed = subprocess.run(argv, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def _jobs(*sets: str) -> dict[str, dict]:
    return {
        doc["metadata"]["name"]: doc
        for doc in _render(*sets)
        if doc.get("kind") == "Job"
    }


_BOTH_ON = (
    "migrations.hook.provisionRoles.enabled=true",
    "migrations.hook.riverMigrate.enabled=true",
)


def test_new_hooks_are_off_by_default() -> None:
    """An existing deployment must not acquire two new Jobs by upgrading."""
    names = set(_jobs())
    assert _PROVISION not in names
    assert _RIVER not in names
    assert _MIGRATE in names, "the pre-existing migrate hook must still render"


def test_chain_renders_in_weight_order() -> None:
    jobs = _jobs(*_BOTH_ON)
    for name in (_MIGRATE, _PROVISION, _RIVER):
        assert name in jobs, f"{name} did not render; got {sorted(jobs)}"

    weights = {
        name: int(jobs[name]["metadata"]["annotations"]["helm.sh/hook-weight"])
        for name in (_MIGRATE, _PROVISION, _RIVER)
    }
    assert weights[_MIGRATE] < weights[_PROVISION] < weights[_RIVER], (
        "River's preflight needs the logins provisioning creates, and "
        f"provisioning needs the tables Alembic creates: {weights}"
    )


def test_migrate_job_weight_is_unchanged() -> None:
    """The existing Job keeps weight 0 -- the new hooks bracket it.

    Pinned because the design that produced this chain first read `-5` off the
    migrate ConfigMap and assumed it was the Job's. Anyone repeating that
    mistake would renumber this and silently reorder the chain.
    """
    jobs = _jobs(*_BOTH_ON)
    assert jobs[_MIGRATE]["metadata"]["annotations"]["helm.sh/hook-weight"] == "0"


@pytest.mark.parametrize("name", [_PROVISION, _RIVER])
def test_new_hooks_run_on_install_and_upgrade(name: str) -> None:
    jobs = _jobs(*_BOTH_ON)
    events = jobs[name]["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert "pre-install" in events, f"{name} must run on a fresh install"
    assert "pre-upgrade" in events, (
        f"{name} must also run on upgrade: provisioning is bootstrap-only "
        "since CHAOS-4261 and riverMigrate re-applies the full grant posture "
        "on every run, so skipping upgrades leaves posture drift unrepaired"
    )


@pytest.mark.parametrize("name", [_PROVISION, _RIVER])
def test_failed_migration_pods_are_retained_for_their_logs(name: str) -> None:
    """No `hook-succeeded`: a failed migration's pod must survive.

    That retention is how the CHAOS-4463 grant gap was diagnosed at all.
    """
    jobs = _jobs(*_BOTH_ON)
    policy = jobs[name]["metadata"]["annotations"]["helm.sh/hook-delete-policy"]
    assert "before-hook-creation" in policy
    assert "hook-succeeded" not in policy, (
        f"{name} would delete its own evidence on the run that needs it least "
        "and keep none on the run that needs it most"
    )


def test_provisioning_uses_the_ops_image_that_carries_the_sql() -> None:
    """A bare postgres image has psql but not provision_river_roles.sql."""
    jobs = _jobs(*_BOTH_ON)
    container = jobs[_PROVISION]["spec"]["template"]["spec"]["containers"][0]
    migrate_image = jobs[_MIGRATE]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert container["image"] == migrate_image, (
        "provisioning runs the ops runtime image, the same one the migrate "
        f"Job uses: {container['image']} != {migrate_image}"
    )
    command = " ".join(container["command"])
    assert "provision_river_roles.sql" in command


def test_river_migrate_defaults_to_the_ops_image_that_carries_the_binary() -> None:
    """`dev-health-worker-migrate` lives in the ops runtime image.

    docker/Dockerfile:87 installs it into /usr/local/bin in the `runner`
    target, and Compose's go-river-migrate builds and runs that same image.
    An earlier version of this chart named
    `ghcr.io/full-chaos/dev-health-go-worker-migrate:latest`, which CI has
    never published -- caught by
    tests/tooling/test_go_image_publishing.py::test_every_referenced_go_image_is_published
    with "deployment renderers name Go images that CI never publishes". A
    chart that renders an unpullable image fails at ImagePullBackOff, long
    after the operator has committed to the upgrade.
    """
    jobs = _jobs(*_BOTH_ON)
    river_image = jobs[_RIVER]["spec"]["template"]["spec"]["containers"][0]["image"]
    migrate_image = jobs[_MIGRATE]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert river_image == migrate_image, (
        "river-migrate must default to the ops runtime image that actually "
        f"carries the binary: {river_image} != {migrate_image}"
    )


def test_river_migrate_asserts_posture_after_applying_it() -> None:
    """`--check` is the posture assertion, and it must run AFTER the apply."""
    jobs = _jobs(*_BOTH_ON)
    container = jobs[_RIVER]["spec"]["template"]["spec"]["containers"][0]
    command = " ".join(container["command"])
    apply_at = command.find("dev-health-worker-migrate &&")
    check_at = command.find("dev-health-worker-migrate --check")
    assert apply_at != -1, f"the migrator is not invoked: {command}"
    assert check_at > apply_at, (
        "--check must follow the apply; a check that runs first proves "
        f"nothing about what the apply did: {command}"
    )


def test_role_passwords_never_appear_in_the_rendered_manifest() -> None:
    """Passwords reach psql through env from the pooler Secret, not argv.

    A --set value inlined into the command would put the password in the Job
    spec, readable by anything with get on Jobs, and in every `helm get
    manifest` for the life of the release.
    """
    jobs = _jobs(
        *_BOTH_ON,
        "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=s3cret-domain",
    )
    rendered = yaml.safe_dump(jobs[_PROVISION])
    assert "s3cret-domain" not in rendered, (
        "the provisioning Job embeds a role password in its own manifest"
    )
    env = {
        item["name"]: item
        for item in jobs[_PROVISION]["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert "secretKeyRef" in env["RIVER_DOMAIN_DATABASE_PASSWORD"]["valueFrom"]


def test_provisioning_without_a_password_secret_fails_the_render() -> None:
    """Roles created with passwords the poolers do not share cannot connect."""
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            "--set",
            "migrations.hook.provisionRoles.enabled=true",
            "--set",
            "goWorkers.pgbouncer.secret.create=false",
            "--set",
            "goWorkers.pgbouncer.secret.externalSecretName=someone-elses",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "RIVER_" in completed.stderr, (
        f"the error must name what the Secret has to carry: {completed.stderr}"
    )
