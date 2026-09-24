"""The sync-dispatch route activation chain must render in the order Compose
already runs it, and only once its prerequisites are actually on.

CHAOS-4272: alembic 0049 seeds `sync_dispatch_transport_routes` at
transport=celery for four kinds -- dispatch_sync_run, finalize_sync_run,
post_sync, reference_discovery. The Celery fleet has had no consumer since
CHAOS-4026, so a fresh install's producer stages outbox rows for a transport
nothing drains, and the reconciler's own route fence refuses readiness on
that drift. This chart never ran the fix
(`dho workers routes apply`) for any of the four.

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
_ROUTE_ACTIVATE_CREDENTIAL_SECRETS = f"{_RELEASE}-dev-health-route-activate-credential-secrets"  # no longer rendered: the operator-token mint step is gone

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
        doc["metadata"]["name"]: doc
        for doc in _render(*sets)
        if doc.get("kind") == "Job"
    }


def _secrets(*sets: str) -> dict[str, dict]:
    return {
        doc["metadata"]["name"]: doc
        for doc in _render(*sets)
        if doc.get("kind") == "Secret"
    }


_PINNED_OPERATOR_IMAGE = (
    "ghcr.io/full-chaos/dev-health-go-operator"
    "@sha256:a6acfd0b8cc78d4d2fd4160c68ab3f8b69eb282b1c2433117af01c954f576534"
)

_FULL_CHAIN_ON = (
    "migrations.hook.provisionRoles.enabled=true",
    "migrations.hook.riverMigrate.enabled=true",
    f"migrations.hook.routeActivate.image={_PINNED_OPERATOR_IMAGE}",
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
        # The River hook needs a pinned dho image of its own to render at all.
        (
            "migrations.hook.riverMigrate.enabled=true",
            f"migrations.hook.riverMigrate.image={_PINNED_OPERATOR_IMAGE}",
        ),
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
        weights[_MIGRATE]
        < weights[_PROVISION]
        < weights[_RIVER]
        < weights[_ROUTE_ACTIVATE]
    ), weights


def test_route_activate_runs_on_install_and_upgrade() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    events = jobs[_ROUTE_ACTIVATE]["metadata"]["annotations"]["helm.sh/hook"].split(",")
    assert "pre-install" in events
    assert "pre-upgrade" in events


def test_failed_route_activate_pod_is_retained_for_its_logs() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    policy = jobs[_ROUTE_ACTIVATE]["metadata"]["annotations"][
        "helm.sh/hook-delete-policy"
    ]
    assert "before-hook-creation" in policy
    assert "hook-succeeded" not in policy, (
        "a failed activation's pod must survive for its logs, same as "
        "provision-roles/river-migrate above it"
    )


# --- the four kinds, in Compose's own serialized order ----------------------


def test_all_four_kinds_are_present_as_ordered_initcontainers() -> None:
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"][
        "initContainers"
    ]
    names = [c["name"] for c in init_containers]
    # No operator-credential step: `dho workers` takes no token.
    assert names[0] == "route-dsn", (
        "the DSN-building step (ops runtime image, has a shell) must run "
        f"before any route-activate-* step (distroless, no shell): {names}"
    )
    route_names = names[1:]
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
    straight to its own ENTRYPOINT (dho workers)."""
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
    assert args[:3] == ["workers", "routes", "apply"], args
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
    assert (
        env["COORDINATOR_DATABASE_URI_FILE"]
        == "/run/route-dsn/COORDINATOR_DATABASE_URI"
    ), env
    # No operator token: `dho workers` has none to read.
    assert "WORKER_OPERATOR_TOKEN_FILE" not in env, env
    assert env["RIVER_DATABASE_SCHEMA"] == "river", env


def test_route_activate_uses_the_published_operator_image() -> None:
    """`ghcr.io/full-chaos/dev-health-go-operator` is CI-published
    (docker-images.yml's go-merge matrix) -- naming an unpublished image
    fails tests/tooling/test_go_image_publishing.py, the same guard
    river-migrate's own image default exists to satisfy. There is no
    floating default any more (the render refuses one -- see the image
    input-domain tests below), so this pins a real digest of the published
    repo name through to all four kind containers instead."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    for kind in _KINDS:
        image = init_containers[f"route-activate-{kind.replace('_', '-')}"]["image"]
        assert image == _PINNED_OPERATOR_IMAGE, image


def test_route_activate_operator_image_override_is_honoured() -> None:
    """codex review (r3 REPORT mutation `image_override` -- SURVIVED, now
    killed): `migrations.hook.routeActivate.image`'s consumer
    (`$operatorImage`) was never exercised with a real override set -- only
    its DEFAULT was pinned above. Pairwise knob x consumer, per the prompt's
    amendment: set the knob, execute its reader. The override itself must be
    pinned (this repo's `sha-<12 hex>` immutable-tag convention here, to
    prove that form is honoured too, not just a digest) -- a floating tag
    would now fail the image guard before ever reaching this assertion."""
    jobs = _jobs(
        *_FULL_CHAIN_ON,
        "migrations.hook.routeActivate.image=ghcr.io/example/custom-operator:sha-abc123456789",
    )
    init_containers = {
        c["name"]: c
        for c in jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["initContainers"]
    }
    for kind in _KINDS:
        image = init_containers[f"route-activate-{kind.replace('_', '-')}"]["image"]
        assert image == "ghcr.io/example/custom-operator:sha-abc123456789", image


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
        arg in ("/bin/sh", "/bin/bash", "sh", "bash")
        for arg in container.get("args", [])
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
    pod_security_context = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"][
        "securityContext"
    ]
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
        containers = deployments[deployment_name]["spec"]["template"]["spec"][
            "containers"
        ]
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
                  image: ghcr.io/example/dev-health-go-dho:sync-provider-test-pin-v42
                  subcommand: worker
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
        if d.get("kind") == "Deployment"
        and d["metadata"]["name"].endswith("-go-sync-provider")
    )
    image = deployment["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image == "ghcr.io/example/dev-health-go-dho:sync-provider-test-pin-v42", (
        image
    )


def test_sync_provider_checked_in_default_image_is_the_published_go_worker_image() -> (
    None
):
    """codex review (r4, P3 -- executed): the sibling test above proves the
    template CONSUMES `$group.image` (via an independent override) but
    cannot prove the CHECKED-IN default in values.yaml itself is correct --
    overriding it removes the very value under test. This asserts the
    UNMODIFIED default render's `sync-provider` image against a hardcoded,
    independently-typed literal (the same pattern
    `test_route_activate_uses_the_published_operator_image` already uses
    for the operator image, not read back from the same file being
    checked)."""
    docs = _render("goWorkers.enabled=true")
    deployment = next(
        d
        for d in docs
        if d.get("kind") == "Deployment"
        and d["metadata"]["name"].endswith("-go-sync-provider")
    )
    image = deployment["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image == "ghcr.io/full-chaos/dev-health-go-dho:latest", image


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

    holders = [
        name for name, s in secrets.items() if "s3cret-route" in yaml.safe_dump(s)
    ]
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
            for item in init_containers[f"route-activate-{kind.replace('_', '-')}"][
                "env"
            ]
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
    secrets = _secrets(*_FULL_CHAIN_ON, "postgresql.enabled=true")
    assert _ROUTE_ACTIVATE_SECRETS in secrets
    # The operator-token mint step and its credential Secret are gone.
    assert _ROUTE_ACTIVATE_CREDENTIAL_SECRETS not in secrets
    for name in (_ROUTE_ACTIVATE_SECRETS,):
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
        assert (
            env[key]["valueFrom"]["secretKeyRef"]["name"] == "river-role-credentials"
        ), key
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
            f"migrations.hook.routeActivate.image={_PINNED_OPERATOR_IMAGE}",
            "--set",
            "goWorkers.pgbouncer.secret.create=false",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    assert "RIVER_" in completed.stderr


# --- entrypoint execution: idempotent mint, DSN built from parts -----------


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
    assert (
        postgres_uri
        == "postgresql://devhealth_domain:d-pw@postgres.internal:5432/devhealth"
    )
    assert (
        worker_uri
        == "postgresql://devhealth_queue:q-pw@postgres.internal:5432/devhealth"
    )
    assert (
        coordinator_uri
        == "postgresql://devhealth_coordinator:c-pw@postgres.internal:5432/devhealth"
    )


# --- input-domain: passwords containing URI-special characters -------------
#
# codex review (r1, P1 -- executed): an unencoded `#` in a password truncates
# the URI at the fragment delimiter, dropping everything after it (including
# the host/port/db) -- confirmed against the real `dho workers`
# binary: `{"error":{"code":"database_unavailable"}}` with the raw password,
# success once percent-encoded. Every RFC 3986 reserved/sub-delim character
# that can appear in a generated password is exercised here, in ONE pass.


_RFC3986_GEN_DELIMS = list(":/?#[]@")
_RFC3986_SUB_DELIMS = list("!$&'()*+,;=")

_URI_SPECIAL_PASSWORD_CASES = (
    [
        ("simple", "plainpassword123"),
        ("percent", "pw%with%percent"),
        ("space", "pw with space"),
        ("unicode", "pw☃snowman"),
        ("empty", ""),
        ("all-gen-delims-combined", "".join(_RFC3986_GEN_DELIMS)),
        ("all-sub-delims-combined", "".join(_RFC3986_SUB_DELIMS)),
    ]
    + [(f"gen-delim-{ord(c)}-{c!r}", f"pw{c}with{c}char") for c in _RFC3986_GEN_DELIMS]
    + [(f"sub-delim-{ord(c)}-{c!r}", f"pw{c}with{c}char") for c in _RFC3986_SUB_DELIMS]
)


@pytest.mark.parametrize(("case_id", "password"), _URI_SPECIAL_PASSWORD_CASES)
def test_route_dsn_script_percent_encodes_every_uri_special_password(
    tmp_path: Path, case_id: str, password: str
) -> None:
    import urllib.parse

    out_dir = _run_route_dsn_script(tmp_path, RIVER_DOMAIN_DATABASE_PASSWORD=password)
    postgres_uri = (out_dir / "POSTGRES_URI").read_text(encoding="utf-8")
    expected_pw = urllib.parse.quote(password, safe="")
    expected = (
        f"postgresql://devhealth_domain:{expected_pw}@postgres.internal:5432/devhealth"
    )
    assert postgres_uri == expected, (case_id, postgres_uri, expected)

    # The written URI must actually PARSE to the original password -- the
    # real proof the encoding round-trips, not just that some encoding ran.
    parsed = urllib.parse.urlsplit(postgres_uri)
    assert urllib.parse.unquote(parsed.username or "") == "devhealth_domain"
    assert urllib.parse.unquote(parsed.password or "") == password, (
        case_id,
        postgres_uri,
    )


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
        tmp_path,
        RIVER_QUEUE_DATABASE_PASSWORD="q#pw",
        RIVER_COORDINATOR_DATABASE_PASSWORD="c#pw",
    )
    worker_uri = (out_dir / "WORKER_DATABASE_URI").read_text(encoding="utf-8")
    coordinator_uri = (out_dir / "COORDINATOR_DATABASE_URI").read_text(encoding="utf-8")
    assert (
        worker_uri
        == "postgresql://devhealth_queue:q%23pw@postgres.internal:5432/devhealth"
    ), worker_uri
    assert (
        coordinator_uri
        == "postgresql://devhealth_coordinator:c%23pw@postgres.internal:5432/devhealth"
    ), coordinator_uri
    assert (
        urllib.parse.unquote(urllib.parse.urlsplit(worker_uri).password or "") == "q#pw"
    )
    assert (
        urllib.parse.unquote(urllib.parse.urlsplit(coordinator_uri).password or "")
        == "c#pw"
    )


def test_route_dsn_script_percent_encodes_the_queue_and_coordinator_roles_too(
    tmp_path: Path,
) -> None:
    """codex review (r4, P3 -- executed, fixed): the r3 class-sweep test
    (`test_route_dsn_script_percent_encodes_every_uri_component`) varies
    ONLY `RIVER_DOMAIN_DATABASE_ROLE` and reads ONLY `POSTGRES_URI` -- the
    domain role's own encode() call site is a DIFFERENT line from the
    queue/coordinator role call sites, same shape as the password-sweep gap
    the sibling test above already closes. Independently confirmed
    (mutation): replacing `${queue_role}`/`${coordinator_role}` with the raw
    env vars in their own printf lines survived the entire file -- this is
    the regression pin that closes it."""
    out_dir = _run_route_dsn_script(
        tmp_path,
        RIVER_QUEUE_DATABASE_ROLE="queue#role",
        RIVER_COORDINATOR_DATABASE_ROLE="coordinator#role",
    )
    worker_uri = (out_dir / "WORKER_DATABASE_URI").read_text(encoding="utf-8")
    coordinator_uri = (out_dir / "COORDINATOR_DATABASE_URI").read_text(encoding="utf-8")
    assert (
        worker_uri == "postgresql://queue%23role:q-pw@postgres.internal:5432/devhealth"
    ), worker_uri
    assert (
        coordinator_uri
        == "postgresql://coordinator%23role:c-pw@postgres.internal:5432/devhealth"
    ), coordinator_uri


def test_route_dsn_script_brackets_an_ipv6_host_instead_of_percent_encoding_it(
    tmp_path: Path,
) -> None:
    """codex review (r4, P1 -- executed, fixed): a URI host is authority
    syntax, not a percent-encodable component like the userinfo/path parts
    -- an IPv6 literal MUST be wrapped in brackets, `[<addr>]`, verbatim.
    Percent-encoding it (the r3 fix's treatment of the host, same as every
    other component) escapes the colons that make it parseable as an
    address at all -- reproduced against the real operator: an unbracketed
    IPv4-mapped IPv6 host returns `database_unavailable`; only the bracketed
    form connects. DNS names and IPv4 dotted-quads are unaffected -- they
    still go through the ordinary percent-encoder (a no-op for their
    always-safe characters)."""
    import urllib.parse

    for host, expected_authority in (
        ("postgres.internal", "postgres.internal"),
        ("127.0.0.1", "127.0.0.1"),
        ("::ffff:127.0.0.1", "[::ffff:127.0.0.1]"),
        ("::1", "[::1]"),
        ("2001:db8::1", "[2001:db8::1]"),
    ):
        out_dir = _run_route_dsn_script(
            tmp_path / host.replace(":", "_"), POSTGRES_HOST=host
        )
        postgres_uri = (out_dir / "POSTGRES_URI").read_text(encoding="utf-8")
        expected = (
            f"postgresql://devhealth_domain:d-pw@{expected_authority}:5432/devhealth"
        )
        assert postgres_uri == expected, (host, postgres_uri, expected)
        # The written URI must actually PARSE back to the original host --
        # the real proof a Postgres client can extract it, not just that
        # brackets appear somewhere in the string.
        parsed = urllib.parse.urlsplit(postgres_uri)
        assert parsed.hostname == host.lower(), (host, parsed.hostname)
        assert parsed.port == 5432, (host, parsed.port)


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
    assert (
        postgres_uri
        == "postgresql://devhealth_domain:d-pw@postgres.internal:5432/review%23db"
    ), postgres_uri
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

    role = (
        urllib.parse.quote(value, safe="")
        if component == "role"
        else _URI_COMPONENT_BASE["role"]
    )
    host = (
        urllib.parse.quote(value, safe="")
        if component == "host"
        else _URI_COMPONENT_BASE["host"]
    )
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
def test_route_activate_enabled_input_domain(
    value: str | None, expect_job: bool
) -> None:
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
    assert "/migrations/hook/routeActivate/enabled" in completed.stderr, (
        completed.stderr
    )
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


# --- P2 (r2, executed): the operator image's own pull policy ---------------
#
# codex review (r2, P2 -- executed): defaulting to .Values.image.pullPolicy
# pulled in a local kind/kiac profile's Never (documented for the
# side-loaded APPLICATION image only) and produced ErrImageNeverPull on the
# operator image, a REGISTRY image that is never side-loaded.


def test_route_activate_operator_pull_policy_is_independent_of_image_pull_policy() -> (
    None
):
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
    assert init_containers["route-dsn"]["imagePullPolicy"] == "Never"


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
    assert (
        init_containers["route-activate-dispatch-sync-run"]["imagePullPolicy"]
        == "Always"
    )


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
    assert (
        init_containers["route-activate-dispatch-sync-run"]["imagePullPolicy"] == policy
    )
