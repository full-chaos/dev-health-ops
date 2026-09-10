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
    assert names[1] == "route-dsn", (
        "the DSN-building step (ops runtime image, has a shell) must run "
        f"before any route-activate-* step (distroless, no shell): {names}"
    )
    route_names = names[2:]
    assert route_names == [f"route-activate-{k.replace('_', '-')}" for k in _KINDS], (
        f"the four routes must run in Compose's own serialized order: {route_names}"
    )


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
    assert calls_log.read_text(encoding="utf-8").count("\n") == 1, (
        "exactly one mint call on an empty token file"
    )

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
