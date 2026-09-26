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
        # The provision-roles hook runs `dho migrate roles` (CHAOS-6951), so it
        # needs a pinned dho image of its own to render at all, like the River hook.
        (
            "migrations.hook.provisionRoles.enabled=true",
            f"migrations.hook.provisionRoles.image={_PINNED_OPERATOR_IMAGE}",
        ),
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
    # No operator-credential step (`dho workers` takes no token) and, since
    # CHAOS-6902, no DSN-building step either: `dho workers` assembles the DSNs
    # from the component env each step is given.
    assert names == [f"route-activate-{k.replace('_', '-')}" for k in _KINDS], (
        f"the four routes must run in Compose's own serialized order: {names}"
    )
    assert "route-dsn" not in names


def test_the_no_op_done_container_actually_exits_zero() -> None:
    """Kubernetes Jobs require a `containers` entry even though every real
    step here is an initContainer (see the template's own comment); a non-zero
    exit on this no-op would fail an otherwise fully-succeeded activation Job
    for no reason. Since CHAOS-6902 it is the operator image (distroless: no
    shell, no Python) running `dho version`, which prints the build metadata
    and exits 0."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    containers = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1, containers
    assert containers[0]["name"] == "done", containers
    assert containers[0]["image"] == _PINNED_OPERATOR_IMAGE, containers[0]
    assert "command" not in containers[0], containers[0]
    assert containers[0]["args"] == ["version"], containers[0]


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
    # CHAOS-6902: the three DSNs are assembled by `dho workers` from the
    # component form; nothing in the pod builds or reads a DSN file. Pin every
    # component key at the template level: host/port/database/user as plain
    # values, the password only by secretKeyRef, and no pre-built URI key (the
    # component form and the URI form are mutually exclusive in ResolveDSN).
    env = {item["name"]: item for item in container["env"]}
    assert env["DEV_HEALTH_PG_DB"]["value"] == "devhealth", env
    for role, secret_key in (
        ("DOMAIN", "RIVER_DOMAIN_DATABASE_PASSWORD"),
        ("QUEUE", "RIVER_QUEUE_DATABASE_PASSWORD"),
        ("COORDINATOR", "RIVER_COORDINATOR_DATABASE_PASSWORD"),
    ):
        prefix = f"DEV_HEALTH_PG_{role}_"
        assert env[prefix + "HOST"]["value"] == "postgres.example.com", env
        assert env[prefix + "PORT"]["value"] == "5432", env
        assert env[prefix + "USER"]["value"] == f"devhealth_{role.lower()}", env
        password = env[prefix + "PASSWORD"]
        assert "value" not in password, password
        assert password["valueFrom"]["secretKeyRef"]["key"] == secret_key, password
    for forbidden in (
        "POSTGRES_URI",
        "WORKER_DATABASE_URI",
        "COORDINATOR_DATABASE_URI",
        "POSTGRES_URI_FILE",
        "WORKER_DATABASE_URI_FILE",
        "COORDINATOR_DATABASE_URI_FILE",
        "WORKER_OPERATOR_TOKEN_FILE",
    ):
        assert forbidden not in env, (forbidden, env)
    assert env["RIVER_DATABASE_SCHEMA"]["value"] == "river", env
    assert "volumeMounts" not in container, container.get("volumeMounts")


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


def test_pod_shares_no_volumes_and_needs_no_fsgroup() -> None:
    """CHAOS-6902: the DSN files (and the emptyDir and fsGroup that let a
    root:root emptyDir be written by a non-root init container and read by a
    distroless one) are gone: every step assembles its own DSNs in memory."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    pod = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]
    assert pod["securityContext"] == {"runAsNonRoot": True}, pod["securityContext"]
    assert "volumes" not in pod, pod.get("volumes")
    for container in pod["initContainers"] + pod["containers"]:
        assert "volumeMounts" not in container, container["name"]


def test_route_activate_hook_runs_no_python_and_no_shell() -> None:
    """The point of CHAOS-6902: the route-activate hook Job references no Python
    image and no shell. Every container is the operator image; nothing in the
    rendered Job names the ops runtime image, /bin/sh or python3."""
    jobs = _jobs(*_FULL_CHAIN_ON)
    job = jobs[_ROUTE_ACTIVATE]
    pod = job["spec"]["template"]["spec"]
    images = {c["image"] for c in pod["initContainers"] + pod["containers"]}
    assert images == {_PINNED_OPERATOR_IMAGE}, images
    rendered = yaml.safe_dump(job)
    for needle in ("/bin/sh", "python3", "dev-hops-api", "urllib"):
        assert needle not in rendered, (needle, rendered[:200])


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
    for kind in _KINDS:
        route_env = {
            item["name"]: item
            for item in init_containers[f"route-activate-{kind.replace('_', '-')}"][
                "env"
            ]
        }
        # CHAOS-6902: each step holds the passwords for `dho workers` to assemble
        # its DSNs, only ever by secretKeyRef, never as a value, and never under
        # the old RIVER_* password names.
        for role in ("DOMAIN", "QUEUE", "COORDINATOR"):
            password = route_env[f"DEV_HEALTH_PG_{role}_PASSWORD"]
            assert "secretKeyRef" in password["valueFrom"], (kind, password)
            assert "value" not in password, (kind, password)
        assert not set(route_env) & {
            "RIVER_DOMAIN_DATABASE_PASSWORD",
            "RIVER_QUEUE_DATABASE_PASSWORD",
            "RIVER_COORDINATOR_DATABASE_PASSWORD",
        }, (kind, sorted(route_env))


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
    for kind in _KINDS:
        env = {
            item["name"]: item
            for item in init_containers[f"route-activate-{kind.replace('_', '-')}"][
                "env"
            ]
        }
        for role in ("DOMAIN", "QUEUE", "COORDINATOR"):
            reference = env[f"DEV_HEALTH_PG_{role}_PASSWORD"]["valueFrom"][
                "secretKeyRef"
            ]
            assert reference["name"] == "river-role-credentials", (kind, role)
            assert reference["key"] == f"RIVER_{role}_DATABASE_PASSWORD", (kind, role)
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


# --- the DSN encoding, formerly a shell + python3 init container ------------
#
# CHAOS-6902 deleted the route-dsn init container. What it pinned (a `#` in a
# password or database name, every RFC 3986 reserved character in a role,
# password, database or host, an IPv6 host in brackets, the queue and
# coordinator call sites separately) is now pinned where the encoding lives:
# internal/platform/config TestRouteDSNVenueOracleMatchesThePythonProducer runs
# the deleted script's exact text (testdata/route_dsn_init.sh, sha256-pinned)
# and the component form `dho workers` reads over the same grid and compares
# the identity pgx reads from each DSN; the frozen golden replays it without a
# shell.


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
    # Since CHAOS-6902 no container of this Job is the side-loaded APPLICATION
    # image: the no-op `done` container is the operator image too, so it takes
    # the same pull policy (a local profile's `Never` on it would fail the Job).
    containers = jobs[_ROUTE_ACTIVATE]["spec"]["template"]["spec"]["containers"]
    assert containers[0]["imagePullPolicy"] == "IfNotPresent", containers[0]


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


def test_route_activate_refuses_an_empty_bundled_database_name() -> None:
    """CHAOS-6902 r1 P1: an empty bundled `postgresql.credentials.database`
    rendered `DEV_HEALTH_PG_DB ''`, which `config.ResolveDSN` turns into the
    database `postgres` while the deleted init container's URI named no
    database at all (the server then uses the role name) -- a different
    connection identity the chart used to accept. The bundled branch of the
    database helper must refuse an empty name, loudly, at render time."""
    completed = subprocess.run(
        [
            "helm",
            "template",
            _RELEASE,
            str(_CHART),
            *[x for s in _FULL_CHAIN_ON for x in ("--set", s)],
            "--set",
            "postgresql.enabled=true",
            "--set-string",
            "postgresql.credentials.database=",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0, completed.stdout[:400]
    assert "postgresql.credentials.database" in completed.stderr, completed.stderr
