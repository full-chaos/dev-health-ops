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


def test_route_activate_respects_the_outer_hook_enabled_gate() -> None:
    """codex review (r3 REPORT mutation `outer_hook_gate` -- SURVIVED, now
    killed): this file's whole content (both Secrets and the Job) is wrapped
    in `{{- if .Values.migrations.hook.enabled }}`, the SAME outer gate
    migrate-job.yaml and river-hooks.yaml already share -- but nothing
    anywhere set `migrations.hook.enabled=false` with the rest of the chain
    on, so mutating this line survived the entire suite. `hook.enabled`
    defaults to true, so every other test above renders through it
    unnoticed; this is the one test that actually turns it off."""
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "migrations.hook.enabled=false",
    )
    assert _ROUTE_ACTIVATE not in jobs, sorted(jobs)
    assert _MIGRATE not in jobs, (
        "hook.enabled=false must disable every hook Job, not just this "
        f"one: {sorted(jobs)}"
    )


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
    assert names[1] == "route-dsn", (
        "the DSN-building step (ops runtime image, has a shell) must run "
        f"before any route-activate-* step (distroless, no shell): {names}"
    )
    route_names = names[2:]
    assert route_names == [f"route-activate-{k.replace('_', '-')}" for k in _KINDS], (
        f"the four routes must run in Compose's own serialized order: {route_names}"
    )


def test_the_no_op_done_container_actually_exits_zero() -> None:
    """codex review (r3, P3 -- executed, fixed): NOTHING pinned this
    container's command -- mutating it from `exit 0` to `exit 1` survived
    the entire runnable suite. Kubernetes Jobs require a `containers` entry
    even though every real step here is an initContainer (see the
    template's own comment); a non-zero exit on this no-op would fail an
    otherwise fully-succeeded activation Job for no reason."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    containers = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1, containers
    assert containers[0]["name"] == "done", containers
    assert containers[0]["command"] == ["/bin/sh", "-ec", "exit 0"], containers[0]


@pytest.mark.parametrize("kind", _KINDS)
def test_each_kind_invokes_routes_apply_with_that_exact_kind(kind: str) -> None:
    """No `command:` override on these containers -- the operator image has
    no shell (see the distroless test below), so the args are passed
    straight to its own ENTRYPOINT (dev-health-workerctl)."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    container = init_containers[f"route-activate-{kind.replace('_', '-')}"]
    assert "command" not in container, (
        f"this container has no shell to run a command script in: {container.get('command')}"
    )
    args = container["args"]
    assert args[:2] == ["routes", "apply"], args
    assert args[-1] == kind, (
        f"the kind argument must be {kind!r}, not fused with another: {args!r}"
    )
    assert "--reason" in args and "--correlation-id" in args, (
        f"routes apply requires both flags: {args!r}"
    )
    # codex review (r3, P3 -- executed, fixed): NOTHING previously asserted
    # these four env values -- a mutation redirecting any one of them to a
    # nonexistent path survived the entire runnable suite (75 passed, 1
    # skipped) and was only caught by the reviewer's separate real-operator
    # execution (`configuration_error`/`authentication_failed`). Pin the
    # literal paths at the template level too, so this class of mutation is
    # killed cheaply, without needing a live container.
    env = {item["name"]: item.get("value") for item in container["env"]}
    assert env["POSTGRES_URI_FILE"] == "/run/route-dsn/POSTGRES_URI", env
    assert env["WORKER_DATABASE_URI_FILE"] == "/run/route-dsn/WORKER_DATABASE_URI", env
    assert env["COORDINATOR_DATABASE_URI_FILE"] == "/run/route-dsn/COORDINATOR_DATABASE_URI", env
    assert env["WORKER_OPERATOR_TOKEN_FILE"] == "/run/go-worker-operator/token", env
    assert env["RIVER_DATABASE_SCHEMA"] == "river", env


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


def test_route_activate_operator_image_override_is_honoured() -> None:
    """codex review (r3 REPORT mutation `image_override` -- SURVIVED, now
    killed): `migrations.hook.routeActivate.image`'s consumer
    (`$operatorImage`) was never exercised with a real override set -- only
    its DEFAULT was pinned above. Pairwise knob x consumer, per the prompt's
    amendment: set the knob, execute its reader."""
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "migrations.hook.routeActivate.image=ghcr.io/example/custom-operator:v9",
    )
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    for kind in _KINDS:
        image = init_containers[f"route-activate-{kind.replace('_', '-')}"]["image"]
        assert image == "ghcr.io/example/custom-operator:v9", image


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


def test_route_dsn_uses_the_ops_image_that_has_a_shell_and_python(tmp_path: Path) -> None:
    """codex review (r1, P1 -- executed): the operator image is built FROM
    gcr.io/distroless/static-debian12:nonroot and has no /bin/sh at all --
    confirmed executing `docker run --entrypoint /bin/sh <operator image>`,
    `exec: "/bin/sh": stat /bin/sh: no such file or directory`. DSN
    construction (which needs a shell plus python3 for percent-encoding)
    must therefore run in the ops runtime image, never the operator image."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    dsn_image = init_containers["route-dsn"]["image"]
    migrate_image = jobs[_MIGRATE]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert dsn_image == migrate_image, (
        f"route-dsn must use the ops runtime image, not the operator image: "
        f"{dsn_image} != {migrate_image}"
    )


@pytest.mark.parametrize("kind", _KINDS)
def test_route_activate_containers_never_invoke_a_shell(kind: str) -> None:
    """Regression pin for the P1 above: no route-activate-* container may
    have a `command:` at all (a shell invocation), since the operator image
    has none to run it with."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    container = init_containers[f"route-activate-{kind.replace('_', '-')}"]
    assert "command" not in container, container.get("command")
    assert not any(
        arg in ("/bin/sh", "/bin/bash", "sh", "bash") for arg in container.get("args", [])
    ), container["args"]


def test_pod_sets_fsgroup_so_non_root_containers_share_emptydirs() -> None:
    """codex review (r1, P1 -- executed): a fresh emptyDir is root:root by
    default; without fsGroup, the non-root writer (operator-credential /
    route-dsn, ops image UID 10001) cannot create files in it either --
    confirmed executing the real image against a fresh emptyDir-equivalent
    volume: `cannot create /run/go-worker-operator/token: Permission
    denied`. fsGroup makes every container in the pod (regardless of its
    own primary UID) a supplementary member of that GID, and Kubernetes
    chowns/chmods each mounted volume to it."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    pod_security_context = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["securityContext"]
    assert pod_security_context.get("fsGroup") is not None, pod_security_context
    assert pod_security_context["runAsNonRoot"] is True


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


def test_expected_worker_groups_actually_render_as_deployments() -> None:
    """codex review (r3 REPORT mutation `remove_worker_deployments` --
    SURVIVED, now killed): the sibling test above reads `values.yaml` as
    TEXT, so it can never catch a Deployment template that silently stops
    rendering for an expected group -- that doesn't change either list's
    own membership. Renders go-workers.yaml for real
    (`goWorkers.enabled=true`) and checks every EXPECTED group has a real
    Deployment."""
    values = yaml.safe_load((_CHART / "values.yaml").read_text(encoding="utf-8"))
    expected = values["goWorkers"]["expectedWorkerGroups"]

    docs = _render("goWorkers.enabled=true")
    deployments = {
        d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"
    }
    fullname_prefix = f"{_RELEASE}-dev-health-go-"

    for name in expected:
        deployment_name = f"{fullname_prefix}{name}"
        assert deployment_name in deployments, (
            f"{name!r} is expected but has no rendered Deployment: "
            f"{sorted(deployments)}"
        )
        containers = deployments[deployment_name]["spec"]["template"]["spec"]["containers"]
        assert len(containers) == 1, containers


def test_worker_group_deployment_uses_that_groups_own_declared_image(
    tmp_path: Path,
) -> None:
    """codex review (r3 REPORT mutation `group_pin_wrong_image` -- SURVIVED,
    now killed): a FIRST version of this check compared the rendered image
    against a value read from the SAME values.yaml being exercised --
    mutating the file moved both sides together, so the comparison was
    vacuous by construction (the family of self-comparison traps elsewhere
    in this repo's own standing rules). This overrides ONE group's image via
    a values FILE (never a `--set` on a list index -- that drops the
    group's sibling keys and crashes the render on a nil pointer) to an
    independent, test-owned literal, and asserts the Deployment actually
    used it -- the render path genuinely reads `$group.image`, not a
    hardcoded string."""
    values_file = tmp_path / "values.yaml"
    values_file.write_text(
        textwrap.dedent(
            """
            goWorkers:
              enabled: true
              groups:
                - name: sync-provider
                  image: ghcr.io/example/sync-provider-test-pin:v42
                  queues: [sync_provider]
                  queueConcurrency: {sync_provider: 2}
                  replicas: 1
                  terminationGracePeriodSeconds: 960
                  autoscaling: {enabled: false}
            """
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        ["helm", "template", _RELEASE, str(_CHART), "-f", str(values_file)],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    docs = [doc for doc in yaml.safe_load_all(completed.stdout) if doc]
    deployment = next(
        d
        for d in docs
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-go-sync-provider")
    )
    image = deployment["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image == "ghcr.io/example/sync-provider-test-pin:v42", image


# --- credentials never reach the JOB spec or the route-activate containers -
#
# codex review (r1, P3): the ORIGINAL version of this test's name/docstring
# overclaimed "never appear in the rendered manifest" -- a Secret's own
# `stringData` legitimately DOES carry the plaintext value at render time
# (that is what a Kubernetes Secret object IS; `helm template --set
# goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=...`
# renders it at deploy/helm/dev-health/templates/go-pgbouncer.yaml and this
# file's own route-activate-secrets Secret, both already-established chart
# patterns unrelated to this PR). What this test actually verifies, made
# explicit below: the password reaches only ONE Secret object (never a
# second copy anywhere else) and only via secretKeyRef in the JOB spec --
# never inlined into a container's own `env[].value`, `args`, or `command`.
# The post-fix design is now STRICTER than before: route-activate-* containers
# carry no password env at all (moved to the route-dsn step), so the
# Job-spec assertion covers the whole Job, not just one container.


def test_role_passwords_reach_only_the_route_activate_secret_via_secretkeyref() -> None:
    docs = _render(
        *_FULL_CHAIN_ON,
        "goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=s3cret-route",
    )
    jobs = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Job"}
    secrets = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Secret"}

    job_rendered = yaml.safe_dump(jobs[_ROUTE_ACTIVATE])
    assert "s3cret-route" not in job_rendered, (
        "the plaintext password must never be inlined into the Job spec itself"
    )

    holders = [name for name, s in secrets.items() if "s3cret-route" in yaml.safe_dump(s)]
    assert holders == [_ROUTE_ACTIVATE_SECRETS] or set(holders) == {
        _ROUTE_ACTIVATE_SECRETS,
        f"{_RELEASE}-dev-health-provision-roles-secrets",
        f"{_RELEASE}-dev-health-go-pgbouncer",
    }, (
        "the password must reach only the chart's own established Secret "
        f"objects for this value, never an extra copy: {holders}"
    )

    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    dsn_env = {item["name"]: item for item in init_containers["route-dsn"]["env"]}
    assert "secretKeyRef" in dsn_env["RIVER_DOMAIN_DATABASE_PASSWORD"]["valueFrom"]
    for kind in _KINDS:
        route_env_names = {
            item["name"]
            for item in init_containers[f"route-activate-{kind.replace('_', '-')}"]["env"]
        }
        assert not route_env_names & {
            "RIVER_DOMAIN_DATABASE_PASSWORD",
            "RIVER_QUEUE_DATABASE_PASSWORD",
            "RIVER_COORDINATOR_DATABASE_PASSWORD",
        }, (
            f"route-activate-{kind} must not carry password env at all -- "
            f"DSN construction moved to route-dsn: {route_env_names}"
        )


def test_route_activate_secret_is_created_before_the_job_that_reads_it() -> None:
    # postgresql.enabled=true: the credential Secret only renders once a DSN
    # actually resolves (r2 P1 fix) -- _FULL_CHAIN_ON alone has no bundled
    # postgres and no secretData DSN, so riverMigrationDSN is empty there,
    # same reason river-migrate's own dedicated Secret needs it too.
    secrets = _secrets(*_FULL_CHAIN_ON, "postgresql.enabled=true")
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
    env = {item["name"]: item for item in init_containers["route-dsn"]["env"]}
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
    mint_call = calls_log.read_text(encoding="utf-8")
    assert mint_call.count("\n") == 1, (
        "exactly one mint call on an empty token file"
    )
    # codex review (r3 REPORT mutations `scope_operate`/`remove_scope_read` --
    # SURVIVED, now killed): the stub's received argv was logged but never
    # asserted against -- a mutation dropping either `--scope` flag from the
    # mint command survived entirely. `service-credentials create` without
    # `workers:read` would mint a token the operator itself would later
    # refuse on (`authentication_failed`), same class of invisible-at-Info
    # regression as the unencoded-DB-name P1 above.
    assert "--service worker-operator" in mint_call, mint_call
    assert "--scope workers:read" in mint_call, mint_call
    assert "--scope workers:operate" in mint_call, mint_call

    second = _run()
    assert second.returncode == 0, second.stderr
    assert calls_log.read_text(encoding="utf-8").count("\n") == 1, (
        "a non-empty token file must short-circuit the mint -- re-running "
        "must be a no-op, matching Compose's own guard"
    )


def _run_route_dsn_script(tmp_path: Path, **env: str) -> Path:
    """Execute the real route-dsn script (ops image: has /bin/sh AND
    python3) and return the directory it wrote POSTGRES_URI/
    WORKER_DATABASE_URI/COORDINATOR_DATABASE_URI into."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    container = init_containers["route-dsn"]
    command = container["command"]
    assert command[:2] == ["/bin/sh", "-ec"], command
    script = command[2]

    out_dir = tmp_path / "run" / "route-dsn"
    out_dir.mkdir(parents=True)
    script = script.replace("/run/route-dsn", str(out_dir))

    base_env = {
        "PATH": os.environ["PATH"],
        "POSTGRES_HOST": "postgres.internal",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "devhealth",
        "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
        "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
        "RIVER_COORDINATOR_DATABASE_ROLE": "devhealth_coordinator",
        "RIVER_DOMAIN_DATABASE_PASSWORD": "d-pw",
        "RIVER_QUEUE_DATABASE_PASSWORD": "q-pw",
        "RIVER_COORDINATOR_DATABASE_PASSWORD": "c-pw",
    }
    base_env.update(env)
    completed = subprocess.run(
        ["/bin/sh", "-ec", script], capture_output=True, text=True, env=base_env
    )
    assert completed.returncode == 0, completed.stderr
    return out_dir


def test_route_dsn_script_builds_dsns_from_parts_and_never_from_a_dsn_value(
    tmp_path: Path,
) -> None:
    """DSNs are built in-shell from role/host/port/db + a secretKeyRef'd
    password -- never templated as one plaintext Secret value -- so the
    script must actually construct them at runtime, into files the
    distroless route-activate-* containers read via the `_FILE` convention
    (they have no shell to build a DSN in themselves)."""
    out_dir = _run_route_dsn_script(tmp_path)
    postgres_uri = (out_dir / "POSTGRES_URI").read_text(encoding="utf-8")
    worker_uri = (out_dir / "WORKER_DATABASE_URI").read_text(encoding="utf-8")
    coordinator_uri = (out_dir / "COORDINATOR_DATABASE_URI").read_text(encoding="utf-8")
    assert postgres_uri == "postgresql://devhealth_domain:d-pw@postgres.internal:5432/devhealth"
    assert worker_uri == "postgresql://devhealth_queue:q-pw@postgres.internal:5432/devhealth"
    assert (
        coordinator_uri
        == "postgresql://devhealth_coordinator:c-pw@postgres.internal:5432/devhealth"
    )


# --- input-domain: passwords containing URI-special characters -------------
#
# codex review (r1, P1 -- executed): an unencoded `#` in a password truncates
# the URI at the fragment delimiter, dropping everything after it (including
# the host/port/db) -- confirmed against the real `dev-health-workerctl`
# binary: `{"error":{"code":"database_unavailable"}}` with the raw password,
# success once percent-encoded. Every RFC 3986 reserved/sub-delim character
# that can appear in a generated password is exercised here, in ONE pass.


_RFC3986_GEN_DELIMS = list(":/?#[]@")
_RFC3986_SUB_DELIMS = list("!$&'()*+,;=")

_URI_SPECIAL_PASSWORD_CASES = [
    ("simple", "plainpassword123"),
    ("percent", "pw%with%percent"),
    ("space", "pw with space"),
    ("unicode", "pw☃snowman"),
    ("empty", ""),
    ("all-gen-delims-combined", "".join(_RFC3986_GEN_DELIMS)),
    ("all-sub-delims-combined", "".join(_RFC3986_SUB_DELIMS)),
] + [
    (f"gen-delim-{ord(c)}-{c!r}", f"pw{c}with{c}char")
    for c in _RFC3986_GEN_DELIMS
] + [
    (f"sub-delim-{ord(c)}-{c!r}", f"pw{c}with{c}char")
    for c in _RFC3986_SUB_DELIMS
]


@pytest.mark.parametrize(("case_id", "password"), _URI_SPECIAL_PASSWORD_CASES)
def test_route_dsn_script_percent_encodes_every_uri_special_password(
    tmp_path: Path, case_id: str, password: str
) -> None:
    import urllib.parse

    out_dir = _run_route_dsn_script(tmp_path, RIVER_DOMAIN_DATABASE_PASSWORD=password)
    postgres_uri = (out_dir / "POSTGRES_URI").read_text(encoding="utf-8")
    expected_pw = urllib.parse.quote(password, safe="")
    expected = f"postgresql://devhealth_domain:{expected_pw}@postgres.internal:5432/devhealth"
    assert postgres_uri == expected, (case_id, postgres_uri, expected)

    # The written URI must actually PARSE to the original password -- the
    # real proof the encoding round-trips, not just that some encoding ran.
    parsed = urllib.parse.urlsplit(postgres_uri)
    assert urllib.parse.unquote(parsed.username or "") == "devhealth_domain"
    assert urllib.parse.unquote(parsed.password or "") == password, (case_id, postgres_uri)


def test_route_dsn_script_percent_encodes_the_queue_and_coordinator_passwords_too(
    tmp_path: Path,
) -> None:
    """codex review (r3, P3 -- executed, fixed): the sweep above only ever
    varied RIVER_DOMAIN_DATABASE_PASSWORD -- encode() is called separately
    for all three roles, so a defect isolated to the queue or coordinator
    call site survived the whole domain sweep. One reserved-character case
    per remaining role, same round-trip proof as the domain sweep."""
    import urllib.parse

    out_dir = _run_route_dsn_script(
        tmp_path, RIVER_QUEUE_DATABASE_PASSWORD="q#pw", RIVER_COORDINATOR_DATABASE_PASSWORD="c#pw"
    )
    worker_uri = (out_dir / "WORKER_DATABASE_URI").read_text(encoding="utf-8")
    coordinator_uri = (out_dir / "COORDINATOR_DATABASE_URI").read_text(encoding="utf-8")
    assert worker_uri == "postgresql://devhealth_queue:q%23pw@postgres.internal:5432/devhealth", worker_uri
    assert (
        coordinator_uri == "postgresql://devhealth_coordinator:c%23pw@postgres.internal:5432/devhealth"
    ), coordinator_uri
    assert urllib.parse.unquote(urllib.parse.urlsplit(worker_uri).password) == "q#pw"
    assert urllib.parse.unquote(urllib.parse.urlsplit(coordinator_uri).password) == "c#pw"


def test_route_dsn_script_percent_encodes_the_database_name(tmp_path: Path) -> None:
    """codex review (r3, P1 -- executed, fixed, regression pin): a database
    name containing a URI-reserved character truncated the DSN at that
    character (a `#` starts a fragment) -- the operator then activated the
    route against the TRUNCATED database while the intended one, silently,
    stayed on its prior transport. This is the r1 password class recurring
    on the database-name component, which the r1 fix never touched."""
    import urllib.parse

    out_dir = _run_route_dsn_script(tmp_path, POSTGRES_DB="review#db")
    postgres_uri = (out_dir / "POSTGRES_URI").read_text(encoding="utf-8")
    assert postgres_uri == "postgresql://devhealth_domain:d-pw@postgres.internal:5432/review%23db", postgres_uri
    parsed = urllib.parse.urlsplit(postgres_uri)
    assert parsed.fragment == "", (
        "an unencoded `#` in the database name starts a URI fragment and "
        f"truncates the path silently: {postgres_uri}"
    )
    assert urllib.parse.unquote(parsed.path.lstrip("/")) == "review#db", postgres_uri


# --- CLASS SWEEP (r3 amendment): every URI-embedded component, not just ----
# the password or the database name in isolation. The r3 P1 fix encoded ONLY
# the database name; team-lead/chris's amendment to the prompt of record
# names this exact shape (a fix for one field of a class, never swept to its
# siblings) as its own method failure. The three roles and the host are the
# remaining operator-settable URI components (port is always numeric). Every
# cell of the SAME RFC 3986 set already proven against the password is
# re-executed here against role and host too -- one parametrized sweep,
# reusing _URI_SPECIAL_PASSWORD_CASES so no sibling of the class is sampled.

_URI_COMPONENT_ENV_VAR = {
    "role": "RIVER_DOMAIN_DATABASE_ROLE",
    "host": "POSTGRES_HOST",
}
_URI_COMPONENT_BASE = {
    "role": "devhealth_domain",
    "host": "postgres.internal",
}


@pytest.mark.parametrize("component", sorted(_URI_COMPONENT_ENV_VAR))
@pytest.mark.parametrize(("case_id", "value"), _URI_SPECIAL_PASSWORD_CASES)
def test_route_dsn_script_percent_encodes_every_uri_component(
    tmp_path: Path, component: str, case_id: str, value: str
) -> None:
    import urllib.parse

    env_var = _URI_COMPONENT_ENV_VAR[component]
    out_dir = _run_route_dsn_script(tmp_path, **{env_var: value})
    postgres_uri = (out_dir / "POSTGRES_URI").read_text(encoding="utf-8")

    role = urllib.parse.quote(value, safe="") if component == "role" else _URI_COMPONENT_BASE["role"]
    host = urllib.parse.quote(value, safe="") if component == "host" else _URI_COMPONENT_BASE["host"]
    expected = f"postgresql://{role}:d-pw@{host}:5432/devhealth"
    assert postgres_uri == expected, (component, case_id, postgres_uri, expected)


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


def test_route_activate_enabled_rejects_a_non_boolean_string() -> None:
    """codex review (r2, P3 -- executed, fixed): before values.schema.json
    typed this key, `--set-string …enabled=maybe` rendered CLEAN -- Go
    template truthiness (`{{ if }}`) treats any non-empty, non-'false'
    string as truthy, so out-of-vocabulary values were silently accepted as
    true. values.schema.json now types this key `boolean`, and Helm
    validates values against it on every `template`/`install`/`upgrade`."""
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set-string",
            "migrations.hook.routeActivate.enabled=maybe",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0, (
        "an out-of-vocabulary string must now fail the render, not silently "
        "activate route-activate"
    )
    assert "/migrations/hook/routeActivate/enabled" in completed.stderr, completed.stderr
    assert "boolean" in completed.stderr, completed.stderr


def test_route_activate_enabled_rejects_an_explicit_null() -> None:
    """codex review (r3, P1 -- executed, fixed): `--set-json …enabled=null`
    DELETES the key during Helm's values coalescing rather than ever
    reaching the type:boolean check above -- the merged values object then
    has no `enabled` property at all, which the chart's `if` treats as
    false, silently disabling activation with exit 0. Reproduced before
    this fix: exit 0, migrate/provision-roles/river-migrate Jobs render,
    route-activate does not. `required: [enabled]` on the routeActivate
    object closes this without rejecting a render that never overrode the
    key at all (values.yaml's own default stays present in that case)."""
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set-json",
            "migrations.hook.routeActivate.enabled=null",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0, (
        "an explicit null override must fail the render now -- silently "
        "deactivating route-activate with no error is the exact r3 P1"
    )
    assert "/migrations/hook/routeActivate" in completed.stderr, completed.stderr
    assert "enabled" in completed.stderr, completed.stderr


# codex review (r3, chris amendment -- executed): the mandated domain cell
# table for this key is {absent, null, "", "true"(string), 1(number)} -- the
# r3 body LISTED these cells without executing all of them. Each is now an
# executed test, not a table row: absent (already covered by
# test_route_activate_enabled_input_domain), null (above), and these two.
def test_route_activate_enabled_rejects_an_empty_string() -> None:
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set-string",
            "migrations.hook.routeActivate.enabled=",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0, completed.stderr
    assert "got string, want boolean" in completed.stderr, completed.stderr


def test_route_activate_enabled_rejects_the_string_true() -> None:
    """`--set-string enabled=true` is the STRING "true", not the boolean --
    Helm's own type coercion never happens for `--set-string`, so this must
    fail the same way `enabled=maybe` does, not be silently accepted just
    because the text spells a valid boolean word."""
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set-string",
            "migrations.hook.routeActivate.enabled=true",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0, completed.stderr
    assert "got string, want boolean" in completed.stderr, completed.stderr


def test_route_activate_enabled_rejects_the_number_one() -> None:
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set-json",
            "migrations.hook.routeActivate.enabled=1",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0, completed.stderr
    assert "got number, want boolean" in completed.stderr, completed.stderr


@pytest.mark.parametrize("value", ["true", "false"])
def test_route_activate_enabled_still_accepts_real_booleans(value: str) -> None:
    """The new schema type must not reject the chart's own documented
    true/false vocabulary -- only reject what was never a boolean."""
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set",
            f"migrations.hook.routeActivate.enabled={value}",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


# --- P1 (r2, executed): the external migration-Secret path -----------------
#
# `secrets.create=false` + `migrations.hook.externalSecretName` is a
# supported fresh-install path for provision-roles/river-migrate above (see
# test_provisioning_accepts_a_pre_created_external_secret and
# test_weight_10_secret_carries_the_elevated_dsn in
# test_helm_migration_hook_chain.py) -- route-activate ignored it entirely:
# it always minted its OWN credential Secret from riverMigrationDSN, which
# resolves from Helm VALUES and is empty when the DSN lives only in the
# operator's externally-managed Secret. The mint then ran against an empty
# environment and failed "missing required input(s)" (reproduced against the
# real `dev-hops service-credentials create` CLI, see the docstring below).


def test_route_activate_credential_secret_is_not_minted_when_secrets_are_external() -> None:
    """Mirrors test_route_activate_accepts_a_pre_created_external_secret
    (pgbouncer Secret) but for the migration Secret this hook's
    operator-credential step actually reads.

    codex review (r3 REPORT mutation `secret_owner_gate` -- SURVIVED against
    this exact test, now killed): the original combo had NO dsn configured
    at all, so $riverDSN was ALSO empty -- removing ONLY the
    `secrets.create` check didn't change the outcome, because the separate
    `$riverDSN` guard alone already suppressed the Secret. Adding
    `postgresql.enabled=true` gives this combo a NON-EMPTY $riverDSN (the
    bundled-postgres default connection string), which isolates the
    `secrets.create` check: without it, the Secret would now render."""
    secrets = _secrets(
        *_FULL_CHAIN_ON,
        "postgresql.enabled=true",
        "secrets.create=false",
        "secrets.externalSecretName=app-secrets",
        "migrations.hook.externalSecretName=migration-secrets",
        "goWorkers.pgbouncer.secret.create=false",
        "goWorkers.pgbouncer.secret.externalSecretName=river-role-credentials",
    )
    assert _ROUTE_ACTIVATE_CREDENTIAL_SECRETS not in secrets, (
        "the chart must not fabricate a copy of a DSN it does not own -- "
        f"got {sorted(secrets)}"
    )
    # codex review mutation-table gap found while re-verifying this test
    # (r3, P1-shaped -- executed, fixed): checking only the INTERNAL name
    # above missed a worse shape entirely -- removing just the
    # `secrets.create` gate (keeping `$riverDSN`) makes the chart emit a REAL
    # `kind: Secret` object named EXACTLY "migration-secrets", the operator's
    # OWN external Secret name, clobbering it on `helm upgrade`. The chart
    # must never emit a Secret object under a name this lane does not own.
    assert "migration-secrets" not in secrets, (
        "the chart must never render a Secret object under the EXTERNAL "
        f"secret's own name -- that overwrites what the operator owns: {sorted(secrets)}"
    )


@pytest.mark.parametrize(
    ("sets", "expected_secret"),
    [
        (
            ("migrations.hook.externalSecretName=migration-secrets",),
            "migration-secrets",
        ),
        (
            (),  # migrations.hook.externalSecretName absent -> falls back
            "app-secrets",  # to secrets.externalSecretName, same as migrate-job.yaml
        ),
    ],
)
def test_route_activate_credential_mint_reads_the_operators_own_secret(
    sets: tuple[str, ...], expected_secret: str
) -> None:
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "secrets.create=false",
        "secrets.externalSecretName=app-secrets",
        "goWorkers.pgbouncer.secret.create=false",
        "goWorkers.pgbouncer.secret.externalSecretName=river-role-credentials",
        *sets,
    )
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    sources = [
        item["secretRef"]["name"]
        for item in init_containers["operator-credential"]["envFrom"]
        if "secretRef" in item
    ]
    assert sources == [expected_secret], sources


def test_route_activate_credential_secret_still_renders_when_secrets_are_bundled() -> None:
    """The gate must not accidentally suppress the create=true path."""
    secrets = _secrets(*_FULL_CHAIN_ON, "postgresql.enabled=true")
    assert _ROUTE_ACTIVATE_CREDENTIAL_SECRETS in secrets
    assert secrets[_ROUTE_ACTIVATE_CREDENTIAL_SECRETS]["stringData"]["DATABASE_URI"]


def test_route_activate_credential_secret_prefers_migration_database_uri(
) -> None:
    """codex review (r3 REPORT mutation `fallback_migration_clause` --
    SURVIVED, now killed): `dev-health.riverMigrationDSN`'s own precedence
    (MIGRATION_DATABASE_URI first, then the Secret's other compatibility
    aliases, then a bundled postgres) was never exercised FROM route-activate
    specifically -- only river-migrate's sibling Secret test covers it.

    A first version of this test set ONLY MIGRATION_DATABASE_URI (no
    candidate it could lose precedence TO), so an OR-order mutation on the
    helper survived it too -- both MIGRATION_DATABASE_URI and the bundled
    postgres (via secretData.POSTGRES_URI, the compatibility alias the
    helper also checks) are now set AT ONCE, to distinct values, so only
    the correct precedence produces the expected result."""
    secrets = _secrets(
        *_FULL_CHAIN_ON,
        "postgresql.enabled=true",
        "migrations.hook.secretData.MIGRATION_DATABASE_URI=postgresql://migrator:pw@postgres:5432/devhealth",
        "migrations.hook.secretData.POSTGRES_URI=postgresql://should-lose:pw@postgres:5432/devhealth",
    )
    assert (
        secrets[_ROUTE_ACTIVATE_CREDENTIAL_SECRETS]["stringData"]["DATABASE_URI"]
        == "postgresql://migrator:pw@postgres:5432/devhealth"
    ), secrets[_ROUTE_ACTIVATE_CREDENTIAL_SECRETS]["stringData"]


def test_route_activate_credential_secret_is_absent_when_no_dsn_resolves_and_secrets_are_bundled() -> None:
    """codex review (r2, P1 -- executed, fixed): secrets.create=true alone is
    not enough -- if NEITHER a bundled postgres NOR any secretData DSN is
    configured, riverMigrationDSN is empty and minting a Secret with an
    empty DATABASE_URI is exactly the r2 P1 failure, just with the chart
    owning the Secret instead of an operator. Same $riverDSN guard
    river-migrate's own dedicated Secret already applies."""
    secrets = _secrets(*_FULL_CHAIN_ON)
    assert _ROUTE_ACTIVATE_CREDENTIAL_SECRETS not in secrets, sorted(secrets)


def test_operator_credential_cli_fails_closed_without_any_dsn_env() -> None:
    """Executed against the REAL `dev-hops service-credentials create` CLI
    (not a stub) with an empty environment -- the exact reproduction of the
    r2 P1 finding, kept as a red-first regression pin: this must always fail
    with the CLI's own "missing required input(s)" message, never silently
    proceed or hang trying to reach a default host.

    codex review (r3, P1 -- executed, fixed): the mandated review-worktree
    setup is `uv sync --no-install-project` -- dependencies only, the
    `dev_health_ops` package itself is never installed into the venv (no
    dist-info, no editable .pth). This subprocess used to rely on the venv
    ALREADY having the project installed (true only when a lane's own
    iteration venv did a full `uv sync --extra dev`); under the mandated
    setup it fails `ModuleNotFoundError: No module named 'dev_health_ops'`
    (reproduced with `python -S`, which skips the same site-packages .pth
    processing) BEFORE the CLI's own argument parsing ever runs -- exit 1,
    not the guard's exit 2. Setting PYTHONPATH to `src` explicitly makes the
    subprocess importable regardless of install mode, matching how `pytest`
    itself resolves the package via `pyproject.toml`'s `pythonpath` setting
    without needing the project installed either."""
    venv_python = Path(__file__).resolve().parents[2] / ".venv" / "bin" / "python"
    if not venv_python.exists():
        pytest.skip("no local .venv to exec the real CLI against")
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    completed = subprocess.run(
        [
            str(venv_python),
            "-m",
            "dev_health_ops.cli",
            "service-credentials",
            "create",
            "--service",
            "worker-operator",
            "--scope",
            "workers:read",
            "--scope",
            "workers:operate",
        ],
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"], "PYTHONPATH": src_dir},
    )
    assert completed.returncode == 2, completed.stderr
    assert "missing required input(s)" in completed.stderr
    assert "POSTGRES_URI/DATABASE_URI" in completed.stderr


def _run_operator_credential_script(tmp_path: Path, **env: str) -> subprocess.CompletedProcess[str]:
    """Execute the REAL rendered operator-credential script (post-fix) with a
    stub `dev-hops` that records the env it actually received, so the
    fallback logic is proven at the shell level, not argued from reading the
    template."""
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
    seen_env_file = tmp_path / "seen-env"
    stub = bin_dir / "dev-hops"
    stub.write_text(
        "#!/bin/sh\n"
        'printf \'POSTGRES_URI=%s\\n\' "${POSTGRES_URI:-<unset>}" > "$SEEN_ENV_FILE"\n'
        'printf \'DATABASE_URI=%s\\n\' "${DATABASE_URI:-<unset>}" >> "$SEEN_ENV_FILE"\n'
        "echo minted-token\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    script = script.replace("/run/go-worker-operator/token", str(run_dir / "token"))
    base_env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "SEEN_ENV_FILE": str(seen_env_file),
        "HOME": str(tmp_path),
    }
    base_env.update(env)
    completed = subprocess.run(
        ["/bin/sh", "-ec", script], capture_output=True, text=True, env=base_env
    )
    if completed.returncode == 0:
        completed.seen_env = dict(  # type: ignore[attr-defined]
            line.split("=", 1) for line in seen_env_file.read_text(encoding="utf-8").splitlines()
        )
    return completed


_MIGRATION_DSN = "postgresql://migrator:pw@postgres:5432/devhealth"
_EXPLICIT_POSTGRES_URI = "postgresql://explicit:pw@postgres:5432/devhealth"
_EXPLICIT_DATABASE_URI = "postgresql://explicit-db:pw@postgres:5432/devhealth"


@pytest.mark.parametrize(
    ("case_id", "env", "expected_postgres_uri"),
    [
        # absent: dev-hops sees neither -- it fails closed on its own
        # (pinned against the real CLI above); the stub just proves the
        # script itself does not fabricate a value out of nothing.
        ("all-absent", {}, "<unset>"),
        # MIGRATION_DATABASE_URI only (the external Secret's own key) ->
        # mapped across, the r2 P1 fix.
        (
            "migration-database-uri-only",
            {"MIGRATION_DATABASE_URI": _MIGRATION_DSN},
            _MIGRATION_DSN,
        ),
        # POSTGRES_URI already present -> must win, never clobbered by the
        # fallback (mirrors river-migrate's own precedence direction).
        (
            "postgres-uri-wins-over-migration",
            {
                "POSTGRES_URI": _EXPLICIT_POSTGRES_URI,
                "MIGRATION_DATABASE_URI": _MIGRATION_DSN,
            },
            _EXPLICIT_POSTGRES_URI,
        ),
        # empty-string MIGRATION_DATABASE_URI is treated as absent, same as
        # the chart's other `-z` guards (river-hooks.yaml, migrate-job.yaml).
        (
            "empty-migration-database-uri",
            {"MIGRATION_DATABASE_URI": ""},
            "<unset>",
        ),
    ],
)
def test_operator_credential_script_dsn_input_domain(
    tmp_path: Path, case_id: str, env: dict[str, str], expected_postgres_uri: str
) -> None:
    completed = _run_operator_credential_script(tmp_path, **env)
    assert completed.returncode == 0, (case_id, completed.stderr)
    assert completed.seen_env["POSTGRES_URI"] == expected_postgres_uri, (  # type: ignore[attr-defined]
        case_id,
        completed.seen_env,  # type: ignore[attr-defined]
    )


def test_operator_credential_script_does_not_clobber_an_explicit_database_uri(
    tmp_path: Path,
) -> None:
    """DATABASE_URI is `dev-hops`'s other recognised name -- the fallback
    must defer to it too, not only to POSTGRES_URI."""
    completed = _run_operator_credential_script(
        tmp_path,
        DATABASE_URI=_EXPLICIT_DATABASE_URI,
        MIGRATION_DATABASE_URI=_MIGRATION_DSN,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.seen_env["DATABASE_URI"] == _EXPLICIT_DATABASE_URI  # type: ignore[attr-defined]
    assert completed.seen_env["POSTGRES_URI"] == "<unset>"  # type: ignore[attr-defined]


# --- P2 (r2, executed): the operator image's own pull policy ---------------
#
# codex review (r2, P2 -- executed): defaulting to .Values.image.pullPolicy
# pulled in a local kind/kiac profile's Never (documented for the
# side-loaded APPLICATION image only) and produced ErrImageNeverPull on the
# operator image, a REGISTRY image that is never side-loaded.


def test_route_activate_operator_pull_policy_is_independent_of_image_pull_policy() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON, "image.pullPolicy=Never")
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    for kind in _KINDS:
        name = f"route-activate-{kind.replace('_', '-')}"
        assert init_containers[name]["imagePullPolicy"] == "IfNotPresent", (
            f"{name} inherited image.pullPolicy=Never, the exact "
            "ErrImageNeverPull class this fix exists to prevent"
        )
    # The ops-runtime containers (the side-loaded APPLICATION image) must
    # still inherit image.pullPolicy -- this fix is scoped to the operator
    # image only, not a blanket override.
    for name in ("operator-credential", "route-dsn"):
        assert init_containers[name]["imagePullPolicy"] == "Never", name


def test_route_activate_operator_pull_policy_override_still_wins() -> None:
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "image.pullPolicy=Never",
        "migrations.hook.routeActivate.pullPolicy=Always",
    )
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    assert init_containers["route-activate-dispatch-sync-run"]["imagePullPolicy"] == "Always"


@pytest.mark.parametrize("policy", ["IfNotPresent", "Always", "Never"])
def test_route_activate_operator_pull_policy_default_input_domain(policy: str) -> None:
    """Every valid Kubernetes pullPolicy value must render through
    unmodified when explicitly set, regardless of image.pullPolicy."""
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "image.pullPolicy=Never",
        f"migrations.hook.routeActivate.pullPolicy={policy}",
    )
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    assert init_containers["route-activate-dispatch-sync-run"]["imagePullPolicy"] == policy
