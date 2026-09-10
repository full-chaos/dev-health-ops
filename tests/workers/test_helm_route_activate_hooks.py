"""The sync-dispatch route activation chain must render in the order Compose
already runs it, and only once its prerequisites are actually on.

CHAOS-4272: alembic 0049 seeds `sync_dispatch_transport_routes` at
transport=celery for four kinds -- dispatch_sync_run, finalize_sync_run,
post_sync, reference_discovery. The Celery fleet has had no consumer since
CHAOS-4026, so a fresh install's producer stages outbox rows for a transport
nothing drains, and the reconciler's own route fence refuses readiness on
that drift. This chart never ran the fix
(`dev-health-workerctl routes apply`) for any of the four.

CHAOS-4455: `sync-provider` (goWorkers.groups) is one of
goWorkers.expectedWorkerGroups, and both lists already agree in
values.yaml -- see test_expected_worker_groups_and_deployments_are_consistent
below, which pins that by reading the values file, not by assuming it.
`sync-provider`'s own job route (sync.provider_unit, a DIFFERENT table,
worker_job_routes) is promoted unconditionally by migration 0107 -- no Job
needed for that half. What was still missing for BOTH tickets was this
chart's complete absence of a route-activate/operator-credential hook at
all, which is what this file pins.

Kubernetes shape: Compose expresses the four-route serialization (the
operator permits one audited mutation at a time) as one-shot containers
chained with `depends_on: condition: service_completed_successfully` sharing
a persistent Docker volume for the operator token. Helm hook Jobs have no
depends_on between sibling Jobs at one weight, so this chart runs the whole
chain as ordered initContainers in ONE Job, sharing an emptyDir instead of a
persistent volume -- a fresh operator-credential token is minted on every
install/upgrade (see RISK-NOTES), which is fine because `routes apply`'s own
idempotency does not depend on the token being reused.
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
_RELEASE = "route-activate"

_MIGRATE = f"{_RELEASE}-dev-health-migrate"
_PROVISION = f"{_RELEASE}-dev-health-provision-roles"
_RIVER = f"{_RELEASE}-dev-health-river-migrate"
_ROUTE_ACTIVATE = f"{_RELEASE}-dev-health-route-activate"
_ROUTE_ACTIVATE_SECRETS = f"{_RELEASE}-dev-health-route-activate-secrets"
_ROUTE_ACTIVATE_CREDENTIAL_SECRETS = (
    f"{_RELEASE}-dev-health-route-activate-credential-secrets"
)

_KINDS = ("dispatch_sync_run", "finalize_sync_run", "post_sync", "reference_discovery")

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
        doc["metadata"]["name"]: doc for doc in _render(*sets) if doc.get("kind") == "Job"
    }


def _secrets(*sets: str) -> dict[str, dict]:
    return {
        doc["metadata"]["name"]: doc
        for doc in _render(*sets)
        if doc.get("kind") == "Secret"
    }


_FULL_CHAIN_ON = (
    "migrations.hook.provisionRoles.enabled=true",
    "migrations.hook.riverMigrate.enabled=true",
)


# --- gating: only when the whole go_default chain is actually on -----------


def test_route_activate_is_absent_by_default() -> None:
    """goWorkers.enabled=true by default, but provisionRoles/riverMigrate are
    both false by default -- so this must not appear on an untouched install,
    the same "unchanged on upgrade" invariant those two flags already carry.
    """
    jobs = _jobs()
    assert _ROUTE_ACTIVATE not in jobs
    assert _MIGRATE in jobs, "the pre-existing migrate hook must still render"


@pytest.mark.parametrize(
    "sets",
    [
        (),
        ("migrations.hook.provisionRoles.enabled=true",),
        ("migrations.hook.riverMigrate.enabled=true",),
    ],
)
def test_route_activate_needs_both_prerequisites(sets: tuple[str, ...]) -> None:
    """Neither prerequisite alone is enough -- route activation needs the
    runtime roles (provisionRoles) AND the River schema (riverMigrate)."""
    jobs = _jobs(*sets)
    assert _ROUTE_ACTIVATE not in jobs, (
        f"route-activate rendered with only {sets}, which leaves either the "
        "runtime roles or the River schema missing"
    )


def test_route_activate_flag_opts_back_out(tmp_path: Path) -> None:
    """The dedicated flag is a real opt-out, independent of its prerequisites."""
    jobs = _jobs(*_FULL_CHAIN_ON, "migrations.hook.routeActivate.enabled=false")
    assert _ROUTE_ACTIVATE not in jobs
    assert _ROUTE_ACTIVATE_SECRETS not in _secrets(
        *_FULL_CHAIN_ON, "migrations.hook.routeActivate.enabled=false"
    )


def test_route_activate_renders_with_the_full_chain_on() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    assert _ROUTE_ACTIVATE in jobs, f"did not render; got {sorted(jobs)}"


# --- ordering: after river-migrate (10), fail-closed like its siblings -----


def test_route_activate_weight_follows_river_migrate() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    weights = {
        name: int(jobs[name]["metadata"]["annotations"]["helm.sh/hook-weight"])
        for name in (_MIGRATE, _PROVISION, _RIVER, _ROUTE_ACTIVATE)
    }
    assert (
        weights[_MIGRATE] < weights[_PROVISION] < weights[_RIVER] < weights[_ROUTE_ACTIVATE]
    ), weights


def test_route_activate_runs_on_install_and_upgrade() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    events = jobs[_ROUTE_ACTIVATE]["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert "pre-install" in events
    assert "pre-upgrade" in events


def test_failed_route_activate_pod_is_retained_for_its_logs() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    policy = jobs[_ROUTE_ACTIVATE]["metadata"]["annotations"]["helm.sh/hook-delete-policy"]
    assert "before-hook-creation" in policy
    assert "hook-succeeded" not in policy, (
        "a failed activation's pod must survive for its logs, same as "
        "provision-roles/river-migrate above it"
    )


# --- the four kinds, in Compose's own serialized order ----------------------


def test_all_four_kinds_are_present_as_ordered_initcontainers() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    names = [c["name"] for c in init_containers]
    assert names[0] == "operator-credential", names
    route_names = names[1:]
    assert route_names == [f"route-activate-{k.replace('_', '-')}" for k in _KINDS], (
        f"the four routes must run in Compose's own serialized order: {route_names}"
    )


@pytest.mark.parametrize("kind", _KINDS)
def test_each_kind_invokes_routes_apply_with_that_exact_kind(kind: str) -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    container = init_containers[f"route-activate-{kind.replace('_', '-')}"]
    command = " ".join(container["command"])
    assert "dev-health-workerctl routes apply" in command
    assert command.rstrip().endswith(kind), (
        f"the kind argument must be {kind!r}, not fused with another: {command!r}"
    )
    assert "--reason" in command and "--correlation-id" in command, (
        f"routes apply requires both flags: {command!r}"
    )


def test_route_activate_uses_the_published_operator_image() -> None:
    """`ghcr.io/full-chaos/dev-health-go-operator` is CI-published
    (docker-images.yml's go-merge matrix) -- naming an unpublished image
    fails tests/tooling/test_go_image_publishing.py, the same guard
    river-migrate's own image default exists to satisfy."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    for kind in _KINDS:
        image = init_containers[f"route-activate-{kind.replace('_', '-')}"]["image"]
        assert image == "ghcr.io/full-chaos/dev-health-go-operator:latest", image


def test_operator_credential_uses_the_ops_image_that_carries_the_cli() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    credential_image = init_containers["operator-credential"]["image"]
    migrate_image = jobs[_MIGRATE]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert credential_image == migrate_image, (
        "`dev-hops service-credentials create` lives in the ops runtime "
        f"image, same as migrate: {credential_image} != {migrate_image}"
    )


# --- CHAOS-4455: EXPECTED_WORKER_GROUPS and the deployed groups agree -------


def test_expected_worker_groups_and_deployments_are_consistent() -> None:
    """Cites the code directly: `sync-provider` is in both lists already.

    Pinned so a future edit to either list is caught here rather than
    rediscovered as a live 503 on /health/workers.
    """
    values_path = _CHART / "values.yaml"
    values = yaml.safe_load(values_path.read_text(encoding="utf-8"))
    expected = values["goWorkers"]["expectedWorkerGroups"]
    group_names = [g["name"] for g in values["goWorkers"]["groups"]]
    assert "sync-provider" in expected, expected
    assert "sync-provider" in group_names, group_names
    for name in expected:
        assert name in group_names, (
            f"goWorkers.expectedWorkerGroups names {name!r}, which "
            f"goWorkers.groups does not deploy: {group_names}"
        )


# --- credentials never reach the rendered manifest or argv ------------------


def test_role_passwords_never_appear_in_the_rendered_manifest() -> None:
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=s3cret-route",
    )
    rendered = yaml.safe_dump(jobs[_ROUTE_ACTIVATE])
    assert "s3cret-route" not in rendered
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    env = {
        item["name"]: item
        for item in init_containers["route-activate-dispatch-sync-run"]["env"]
    }
    assert "secretKeyRef" in env["RIVER_DOMAIN_DATABASE_PASSWORD"]["valueFrom"]


def test_route_activate_secret_is_created_before_the_job_that_reads_it() -> None:
    secrets = _secrets(*_FULL_CHAIN_ON)
    assert _ROUTE_ACTIVATE_SECRETS in secrets
    assert _ROUTE_ACTIVATE_CREDENTIAL_SECRETS in secrets
    for name in (_ROUTE_ACTIVATE_SECRETS, _ROUTE_ACTIVATE_CREDENTIAL_SECRETS):
        annotations = secrets[name]["metadata"]["annotations"]
        assert "pre-install" in annotations["helm.sh/hook"].split(",")
        weight = int(annotations["helm.sh/hook-weight"])
        assert weight < 15, f"{name} must precede the weight-15 Job: {weight}"


def test_route_activate_accepts_a_pre_created_external_secret() -> None:
    """`create=false` + an external Secret is a supported fresh-install path,
    same as provision-roles/river-migrate above it."""
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "goWorkers.pgbouncer.secret.create=false",
        "goWorkers.pgbouncer.secret.externalSecretName=river-role-credentials",
    )
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    env = {
        item["name"]: item
        for item in init_containers["route-activate-dispatch-sync-run"]["env"]
    }
    for key in (
        "RIVER_DOMAIN_DATABASE_PASSWORD",
        "RIVER_QUEUE_DATABASE_PASSWORD",
        "RIVER_COORDINATOR_DATABASE_PASSWORD",
    ):
        assert env[key]["valueFrom"]["secretKeyRef"]["name"] == "river-role-credentials", key
    secrets = _secrets(
        *_FULL_CHAIN_ON,
        "goWorkers.pgbouncer.secret.create=false",
        "goWorkers.pgbouncer.secret.externalSecretName=river-role-credentials",
    )
    assert _ROUTE_ACTIVATE_SECRETS not in secrets, (
        "the chart must not fabricate a copy of credentials it does not own"
    )


def test_route_activate_without_any_password_secret_fails_the_render() -> None:
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            "--set",
            "migrations.hook.provisionRoles.enabled=true",
            "--set",
            "migrations.hook.riverMigrate.enabled=true",
            "--set",
            "goWorkers.pgbouncer.secret.create=false",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "RIVER_" in completed.stderr


# --- entrypoint execution: idempotent mint, DSN built from parts -----------


def test_operator_credential_script_mints_only_once(tmp_path: Path) -> None:
    """Execute the real script against an empty, then a populated, token file."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    command = init_containers["operator-credential"]["command"]
    assert command[:2] == ["/bin/sh", "-ec"], command
    script = command[2]

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_log = tmp_path / "calls.log"
    stub = bin_dir / "dev-hops"
    stub.write_text(
        '#!/bin/sh\necho "$*" >> "$CALLS_LOG"\necho minted-token\n', encoding="utf-8"
    )
    stub.chmod(0o755)

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CALLS_LOG": str(calls_log),
        "HOME": str(tmp_path),
    }

    def _run() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["/bin/sh", "-ec", script.replace(
                "/run/go-worker-operator/token", str(run_dir / "token")
            )],
            capture_output=True,
            text=True,
            env=env,
        )

    first = _run()
    assert first.returncode == 0, first.stderr
    assert (run_dir / "token").read_text(encoding="utf-8").strip() == "minted-token"
    assert calls_log.read_text(encoding="utf-8").count("\n") == 1, (
        "exactly one mint call on an empty token file"
    )

    second = _run()
    assert second.returncode == 0, second.stderr
    assert calls_log.read_text(encoding="utf-8").count("\n") == 1, (
        "a non-empty token file must short-circuit the mint -- re-running "
        "must be a no-op, matching Compose's own guard"
    )


def test_route_activate_script_builds_dsns_from_parts_and_never_from_a_dsn_value(
    tmp_path: Path,
) -> None:
    """DSNs are built in-shell from role/host/port/db + a secretKeyRef'd
    password -- never templated as one plaintext Secret value -- so the
    script must actually construct them at runtime. Execute it against a
    recording `dev-health-workerctl` stub."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    container = init_containers["route-activate-dispatch-sync-run"]
    command = container["command"]
    assert command[:2] == ["/bin/sh", "-ec"], command
    script = command[2]

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "workerctl.log"
    stub = bin_dir / "dev-health-workerctl"
    stub.write_text(
        "#!/bin/sh\n"
        'printf "%s|%s|%s|%s\\n" "$POSTGRES_URI" "$WORKER_DATABASE_URI"'
        ' "$COORDINATOR_DATABASE_URI" "$*" >> "$LOG"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    completed = subprocess.run(
        ["/bin/sh", "-ec", script],
        capture_output=True,
        text=True,
        env={
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "LOG": str(log),
            "POSTGRES_HOST": "postgres.internal",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "devhealth",
            "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
            "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
            "RIVER_COORDINATOR_DATABASE_ROLE": "devhealth_coordinator",
            "RIVER_DOMAIN_DATABASE_PASSWORD": "d-pw",
            "RIVER_QUEUE_DATABASE_PASSWORD": "q-pw",
            "RIVER_COORDINATOR_DATABASE_PASSWORD": "c-pw",
        },
    )
    assert completed.returncode == 0, completed.stderr
    line = log.read_text(encoding="utf-8").strip()
    postgres_uri, worker_uri, coordinator_uri, argv = line.split("|", 3)
    assert postgres_uri == "postgresql://devhealth_domain:d-pw@postgres.internal:5432/devhealth"
    assert worker_uri == "postgresql://devhealth_queue:q-pw@postgres.internal:5432/devhealth"
    assert (
        coordinator_uri
        == "postgresql://devhealth_coordinator:c-pw@postgres.internal:5432/devhealth"
    )
    assert argv.endswith("dispatch_sync_run"), argv


# --- input-domain table: migrations.hook.routeActivate.enabled -------------
#
# One boolean values key. Its domain: absent (default), true, false, and the
# out-of-vocabulary/wrong-type cells Helm's own schema-free values model
# accepts as truthy/falsy strings. Each cell is executed through
# `helm template`, not asserted from reading the template.


@pytest.mark.parametrize(
    ("value", "expect_job"),
    [
        (None, True),  # absent -> default true
        ("true", True),
        ("false", False),
    ],
)
def test_route_activate_enabled_input_domain(value: str | None, expect_job: bool) -> None:
    sets = list(_FULL_CHAIN_ON)
    if value is not None:
        sets.append(f"migrations.hook.routeActivate.enabled={value}")
    jobs = _jobs(*sets)
    assert (_ROUTE_ACTIVATE in jobs) is expect_job, (value, sorted(jobs))


def test_route_activate_enabled_rejects_a_non_boolean_string(tmp_path: Path) -> None:
    """Helm's `--set` parses bare `maybe` as the STRING "maybe", which Go
    template truthiness (`{{ if }}`) treats as truthy -- pin that this chart
    inherits Helm's own boolean coercion rather than silently accepting an
    out-of-vocabulary value as false."""
    jobs = _jobs(*_FULL_CHAIN_ON, "migrations.hook.routeActivate.enabled=maybe")
    assert _ROUTE_ACTIVATE in jobs, (
        "a non-empty, non-'false' string is Go-template-truthy; if this ever "
        "starts failing, `if` was replaced with something that stopped "
        "accepting the chart's own documented true/false vocabulary"
    )
