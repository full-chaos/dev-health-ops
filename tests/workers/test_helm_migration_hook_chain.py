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

import os
import shutil
import subprocess
import textwrap
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


# --- the password Secret has to exist while a pre-install hook runs ---------
#
# Helm creates a chart's regular resources AFTER its pre-install hooks, so on a
# fresh install the PgBouncer Secret does not exist yet when the weight-5
# provisioning Job starts: the pod stays in CreateContainerConfigError until the
# release times out. This is the same problem migrate-job.yaml solves for its own
# ConfigMap/Secret, and it is solved the same way -- the hook ships its own copy
# when the chart owns the credentials, and references the external Secret
# directly when it does not, because that one exists independently of the
# release. Guarding on `create=true` instead rejected the only configuration
# that actually worked.


def test_provisioning_password_secret_is_available_to_the_hook(
    tmp_path: Path,
) -> None:
    docs = _docs_from_values(_values(river_migrate=True), tmp_path)
    jobs = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Job"}
    env = {
        item["name"]: item
        for item in jobs[_PROVISION]["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    referenced = env["RIVER_DOMAIN_DATABASE_PASSWORD"]["valueFrom"]["secretKeyRef"][
        "name"
    ]

    secrets = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Secret"}
    assert referenced in secrets, (
        f"the provisioning Job references Secret {referenced!r}, which the "
        f"chart does not render at all: {sorted(secrets)}"
    )
    secret = secrets[referenced]
    for key in (
        "RIVER_DOMAIN_DATABASE_PASSWORD",
        "RIVER_QUEUE_DATABASE_PASSWORD",
        "RIVER_COORDINATOR_DATABASE_PASSWORD",
    ):
        assert key in secret["stringData"], f"{referenced} omits {key}"

    annotations = secret["metadata"].get("annotations", {})
    events = annotations.get("helm.sh/hook", "").split(",")
    assert "pre-install" in events, (
        f"{referenced} is a regular resource, which Helm creates only AFTER "
        "the pre-install hooks: the weight-5 provisioning Job would start "
        f"against a Secret that does not exist yet ({annotations})"
    )
    weight = int(annotations.get("helm.sh/hook-weight", "0"))
    assert weight < 5, (
        f"{referenced} must be created before the Job that mounts it: {weight}"
    )


def test_provisioning_accepts_a_pre_created_external_secret(tmp_path: Path) -> None:
    """`create=false` + an external Secret is a supported fresh-install path."""
    values = _values(river_migrate=True) + textwrap.dedent("""
        goWorkers:
          pgbouncer:
            secret:
              create: false
              externalSecretName: river-role-credentials
        """)
    docs = _docs_from_values(values, tmp_path)
    jobs = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Job"}
    env = {
        item["name"]: item
        for item in jobs[_PROVISION]["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    for key in (
        "RIVER_DOMAIN_DATABASE_PASSWORD",
        "RIVER_QUEUE_DATABASE_PASSWORD",
        "RIVER_COORDINATOR_DATABASE_PASSWORD",
    ):
        assert env[key]["valueFrom"]["secretKeyRef"]["name"] == (
            "river-role-credentials"
        ), f"{key} does not read the operator's own Secret"


def test_provisioning_without_any_password_secret_fails_the_render(
    tmp_path: Path,
) -> None:
    """Fail closed only when NEITHER Secret exists, not merely when the chart
    does not own it. Roles created with passwords the poolers do not share
    cannot connect, so the render must still refuse to guess."""
    values = _values(river_migrate=True) + textwrap.dedent("""
        goWorkers:
          enabled: false
          pgbouncer:
            secret:
              create: false
              externalSecretName: ""
        """)
    completed = _render_values(values, tmp_path)
    assert completed.returncode != 0
    assert "RIVER_" in completed.stderr, (
        f"the error must name what the Secret has to carry: {completed.stderr}"
    )


# --- the weight-0 migrate Job must not run the River step itself ------------
#
# `dev-hops migrate postgres` invokes `dev-health-worker-migrate` itself
# whenever MIGRATION_DATABASE_URI (or _FILE) is set --
# src/dev_health_ops/migrate.py::_run_river_upgrade returns 0 early only when
# BOTH are absent. That implicit step runs inside the weight-0 Job, i.e. BEFORE
# the weight-5 provisioning hook has created the runtime logins, and River's
# preflight requires them to exist with rolcanlogin
# (internal/storage/river/migrate.go:142-186). On a fresh database it therefore
# fails and Helm aborts the release before provisioning ever runs.
#
# Compose keeps the two apart by never letting the two DSNs share a variable:
# its `migrate` service reads MIGRATION_DATABASE_URI (compose.yml:196) and
# unsets it when empty (compose.yml:187-188), while the River DSN reaches
# go-river-migrate under a DIFFERENT name, GO_WORKER_MIGRATION_DATABASE_URI
# (compose.go-workers.yml:156). The chart cannot split the variable that way --
# both Jobs envFrom one migration Secret, and when secrets.create=false its
# contents belong to the operator -- so it applies the same separation where
# Compose already applies it, in the migrate entrypoint: hand the DSN to Alembic
# as `--db` and take it out of the environment.

_MIGRATION_DSN = "postgresql://migrator:pw@postgres:5432/devhealth"


def _values(river_migrate: bool) -> str:
    return textwrap.dedent(f"""
        migrations:
          hook:
            provisionRoles: {{enabled: true}}
            riverMigrate: {{enabled: {str(river_migrate).lower()}}}
            secretData:
              MIGRATION_DATABASE_URI: "{_MIGRATION_DSN}"
              POSTGRES_URI: ""
              CLICKHOUSE_URI: ""
        """)


def _render_values(values: str, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Render from a values FILE, never `--set` on a list index.

    `--set goWorkers.groups[0].x=y` replaces the whole list element and drops
    the sibling keys the templates dereference; the render then dies on a nil
    pointer, which reads exactly like a guard firing.
    """
    values_file = tmp_path / "values.yaml"
    values_file.write_text(values, encoding="utf-8")
    return subprocess.run(
        ["helm", "template", _RELEASE, str(_CHART), "-f", str(values_file)],
        capture_output=True,
        text=True,
    )


def _docs_from_values(values: str, tmp_path: Path) -> list[dict]:
    completed = _render_values(values, tmp_path)
    assert completed.returncode == 0, completed.stderr
    return [doc for doc in yaml.safe_load_all(completed.stdout) if doc]


def _jobs_from_values(values: str, tmp_path: Path) -> dict[str, dict]:
    return {
        doc["metadata"]["name"]: doc
        for doc in _docs_from_values(values, tmp_path)
        if doc.get("kind") == "Job"
    }


def _run_migrate_command(job: dict, tmp_path: Path, **env: str) -> tuple[int, str]:
    """Execute the migrate Job's shell with a recording `dev-hops` stub.

    The defect is in shell logic, so a substring assertion on the rendered
    command would pass against a script that still exports the variable. This
    runs the real script and records, per invocation, both the argv and whether
    MIGRATION_DATABASE_URI was still in the environment.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    log = tmp_path / "dev-hops.log"
    stub = bin_dir / "dev-hops"
    stub.write_text(
        "#!/bin/sh\n"
        'set -- "$@"\n'
        'argv="$*"\n'
        'if [ "${MIGRATION_DATABASE_URI+x}" = x ]; then seen=set; else seen=unset; fi\n'
        'if [ "${MIGRATION_DATABASE_URI_FILE+x}" = x ]; then'
        " seenfile=set; else seenfile=unset; fi\n"
        'printf "%s|%s|%s\\n" "$argv" "$seen" "$seenfile" >> "$DEV_HOPS_LOG"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    container = job["spec"]["template"]["spec"]["containers"][0]
    command = container["command"]
    assert command[:2] == ["sh", "-c"], command
    completed = subprocess.run(
        ["sh", "-c", command[2]],
        capture_output=True,
        text=True,
        env={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "DEV_HOPS_LOG": str(log),
            **env,
        },
    )
    return completed.returncode, log.read_text(encoding="utf-8") if log.exists() else ""


def _postgres_invocation(calls: str) -> tuple[str, str, str]:
    for line in calls.splitlines():
        argv, seen, seen_file = line.rsplit("|", 2)
        if argv.endswith("migrate postgres"):
            return argv, seen, seen_file
    raise AssertionError(f"`migrate postgres` was never invoked: {calls!r}")


def test_migrate_job_does_not_run_the_implicit_river_step(tmp_path: Path) -> None:
    """With the River hook enabled, weight 0 must leave River to weight 10."""
    jobs = _jobs_from_values(_values(river_migrate=True), tmp_path)
    code, calls = _run_migrate_command(
        jobs[_MIGRATE],
        tmp_path,
        MIGRATION_DATABASE_URI=_MIGRATION_DSN,
        POSTGRES_URI="",
        DATABASE_URI="",
    )
    assert code == 0, calls
    argv, seen, seen_file = _postgres_invocation(calls)
    assert (seen, seen_file) == ("unset", "unset"), (
        "`dev-hops migrate postgres` still sees MIGRATION_DATABASE_URI, so it "
        "runs dev-health-worker-migrate at weight 0 -- before the weight-5 hook "
        f"has created the roles its preflight requires: {calls!r}"
    )
    assert f"--db {_MIGRATION_DSN}" in argv, (
        f"Alembic must still receive the elevated DSN, now explicitly: {argv!r}"
    )


def test_migrate_job_still_runs_river_when_the_hook_is_off(tmp_path: Path) -> None:
    """The suppression is scoped to the hook taking the step over.

    Without this, tightening the entrypoint to always unset the variable would
    pass the test above while silently removing the River migration from every
    deployment that has not opted into the new hook.
    """
    jobs = _jobs_from_values(_values(river_migrate=False), tmp_path)
    code, calls = _run_migrate_command(
        jobs[_MIGRATE],
        tmp_path,
        MIGRATION_DATABASE_URI=_MIGRATION_DSN,
        POSTGRES_URI="",
        DATABASE_URI="",
    )
    assert code == 0, calls
    argv, seen, _seen_file = _postgres_invocation(calls)
    assert seen == "set", (
        "with riverMigrate off nothing else applies the River schema, so the "
        f"env-var opt-in must survive: {calls!r}"
    )
    assert "--db" not in argv, (
        "--db combined with MIGRATION_DATABASE_URI is rejected by "
        f"migrate.py::_run_upgrade: {argv!r}"
    )
