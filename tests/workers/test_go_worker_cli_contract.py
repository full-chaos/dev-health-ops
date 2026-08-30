"""CHAOS-4020 acceptance: the Go workers are configured by flags, not by ~85
unfindable environment variables.

The inventory that opened the ticket found 85 operator-facing variables across
``cmd/dev-health-worker``, ``-reconciler``, ``-scheduler``, and the stream
runner: 79 in the platform config package (40 of them provider route switches),
5 read straight from the process environment by the tracing package, and
``DEV_HEALTH_PROFILE``.

Three properties are pinned here, one per acceptance criterion:

* every setting is reachable from ``--help`` (discovery),
* a misspelled option cannot reach a deploy manifest unnoticed (loud failure),
* a standard deployment configures a handful of credentials (env count).

These assertions read the checked-in Go registries and the deploy manifests, so
they fail on the surface an operator actually touches rather than on a mirrored
list maintained by hand.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PACKAGE = _REPO_ROOT / "internal" / "platform" / "config"
_OPTIONS_SOURCE = _CONFIG_PACKAGE / "options.go"
_ROUTES_SOURCE = _CONFIG_PACKAGE / "routes.go"

_GO_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose" / "compose.go-workers.yml"
_GO_SWARM = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.go-workers.yml"
_GO_KUBERNETES = _REPO_ROOT / "deploy" / "kubernetes" / "go-workers.yaml"
_HELM_WORKERS = (
    _REPO_ROOT / "deploy" / "helm" / "dev-health" / "templates" / "go-workers.yaml"
)

# Credentials stay in the environment on purpose. A DSN or token passed as a
# process argument is readable through `ps`, `docker inspect`, and the very
# `docker compose config` output this ticket asks operators to read, so the
# registry declares them Secret and offers no flag for them.
_CREDENTIALS = frozenset(
    {
        "POSTGRES_URI",
        "WORKER_DATABASE_URI",
        "COORDINATOR_DATABASE_URI",
        "CLICKHOUSE_URI",
        "VALKEY_URI",
        "SETTINGS_ENCRYPTION_KEY",
        "SETTINGS_ENCRYPTION_SALT",
        "PAGER_DUTY_CLIENT_ID",
        "PAGER_DUTY_SECRET",
        "WORKER_OPERATIONAL_BRIDGE_TOKEN",
    }
)

# AUTO_RUN_MIGRATIONS is read by the Python runtime image's entrypoint, not by
# any Go binary, so it is not part of the worker configuration surface.
_NON_WORKER_ENV = frozenset({"AUTO_RUN_MIGRATIONS"})

# CHAOS-4587: OPERATIONAL_ORDERING_CONTRACT IS read directly by Go binaries
# (internal/jobs/metrics/remaining/dora_native_clickhouse.go,
# internal/providersync/pagerduty_services_effects_clickhouse.go via
# os.LookupEnv), but deliberately outside internal/platform/config's flag
# registry -- it gates admission against the *stored ClickHouse table
# contract*, a deploy-time/data-migration coordination concern (see
# .github/docs-legacy/architecture/canonical-operational-model.md
# "Ordering-contract rollout and recovery"), not a per-process runtime
# behavior a flag would suit. Registering it as a flag would still leave the
# same value needing to travel from the deploy layer's `${...:-2}` default
# into a flag's own default, without changing the CHAOS-4020 typo-safety
# property this test enforces for the ~85-variable surface that motivated
# it. Carved out like _NON_WORKER_ENV, for a different reason: this key is
# CHAOS-4020-surface-exempt, not Go-binary-exempt.
_ORDERING_CONTRACT_ENV = frozenset({"OPERATIONAL_ORDERING_CONTRACT"})


def _go_flag_names() -> frozenset[str]:
    """Every long flag the option registry offers, including aliases."""
    source = _OPTIONS_SOURCE.read_text(encoding="utf-8")
    names = set(re.findall(r'\bFlag:\s*"([a-z0-9-]+)"', source))
    for block in re.findall(r"Aliases:\s*\[\]string\{([^}]*)\}", source):
        names.update(re.findall(r'"([a-z0-9-]+)"', block))
    assert names, "no flags parsed from the option registry"
    return frozenset(names)


def _go_secret_env() -> frozenset[str]:
    source = _OPTIONS_SOURCE.read_text(encoding="utf-8")
    return frozenset(re.findall(r'\{Env:\s*"([A-Z0-9_]+)",\s*Secret:\s*true', source))


def _flags_of(items: object) -> set[str]:
    """Long flag names in one container's command/args list."""
    if not isinstance(items, list):
        return set()
    return {
        match.group(1)
        for match in (re.match(r"--([a-z][a-z0-9-]*)=", str(item)) for item in items)
        if match
    }


def _compose_manifest_flags(path: Path) -> set[str]:
    """Flags the Go worker services of a Compose/Swarm file actually pass.

    Scanning the file as raw text would also sweep up the psql provisioning
    entrypoint's --host/--dbname, which belong to a different program.
    """
    flags: set[str] = set()
    for service in _compose_worker_services(path).values():
        flags |= _flags_of(service.get("command"))
    return flags


def _kubernetes_manifest_flags(path: Path) -> set[str]:
    flags: set[str] = set()
    for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if not document or document.get("kind") != "Deployment":
            continue
        for container in document["spec"]["template"]["spec"]["containers"]:
            flags |= _flags_of(container.get("args"))
    return flags


def _helm_manifest_flags(path: Path) -> set[str]:
    """Flags a Helm template emits, read from the template source.

    The template is not YAML until it is rendered, so this reads the literal
    flag names out of the quoted list items and printf formats that produce
    them.
    """
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r'-\s*(?:\{\{[^"]*)?"--([a-z][a-z0-9-]*)=', text))


def _compose_worker_services(path: Path) -> dict[str, dict]:
    services = yaml.safe_load(path.read_text(encoding="utf-8"))["services"]
    return {
        name: service
        for name, service in services.items()
        if isinstance(service.get("labels"), dict)
        and "dev-health.io/worker-group" in service["labels"]
    }


def test_registry_declares_the_credentials_that_stay_in_the_environment() -> None:
    """The documented handful is exactly the credential set.

    If a credential loses its Secret marker it gains a flag, and the value lands
    in `ps` output; if a non-credential gains one it becomes unreachable from
    --help. Either direction fails here.
    """
    assert _go_secret_env() == _CREDENTIALS


@pytest.mark.parametrize(
    ("path", "extract"),
    [
        (_GO_COMPOSE, _compose_manifest_flags),
        (_GO_SWARM, _compose_manifest_flags),
        (_GO_KUBERNETES, _kubernetes_manifest_flags),
        (_HELM_WORKERS, _helm_manifest_flags),
    ],
    ids=["compose", "swarm", "kubernetes", "helm"],
)
def test_deploy_manifests_only_pass_flags_the_binaries_accept(
    path: Path, extract
) -> None:
    """A misspelled option in a manifest fails here, and again at startup.

    This is the half of the contract the environment surface could never offer.
    A misspelled variable name is indistinguishable from an unset one and stays
    silently inert -- exactly how a typo'd ``OTEL_SERVICE_NAMEi`` survived in
    production. A misspelled flag is rejected by ``flag.ContinueOnError`` at
    startup; this test moves that failure earlier still, to review time.
    """
    passed = extract(path)
    assert passed, f"{path.name} passes no flags at all"
    unknown = passed - _go_flag_names()
    assert not unknown, (
        f"{path.name} passes flags no Go worker binary declares: {sorted(unknown)}. "
        "Add them to internal/platform/config/options.go or fix the spelling."
    )


@pytest.mark.parametrize("path", [_GO_COMPOSE, _GO_SWARM], ids=["compose", "swarm"])
def test_compose_surfaces_keep_only_credentials_in_the_environment(
    path: Path,
) -> None:
    """`docker compose config` shows the deployed configuration.

    Before CHAOS-4020 a worker's configuration was reconstructed from a shared
    env anchor plus a host .env file. Every non-credential setting now renders
    in ``command:``, so what a container runs is readable in one place.
    """
    services = _compose_worker_services(path)
    assert services, f"{path.name} declares no Go worker services"
    for name, service in services.items():
        environment = set(service.get("environment") or {})
        leaked = environment - _CREDENTIALS - _NON_WORKER_ENV - _ORDERING_CONTRACT_ENV
        assert not leaked, (
            f"{path.name}:{name} still configures {sorted(leaked)} through the "
            "environment; pass them as flags in command: instead"
        )
        command = service.get("command") or []
        assert command, f"{path.name}:{name} passes no flags at all"
        assert all(str(item).startswith("--") for item in command), (
            f"{path.name}:{name} mixes positional arguments into command:"
        )


def test_kubernetes_workers_keep_only_credentials_in_inline_env() -> None:
    documents = [
        document
        for document in yaml.safe_load_all(_GO_KUBERNETES.read_text(encoding="utf-8"))
        if document and document.get("kind") == "Deployment"
    ]
    assert documents, "no Go worker Deployments found"
    for document in documents:
        container = document["spec"]["template"]["spec"]["containers"][0]
        inline = {item["name"] for item in container.get("env") or []}
        leaked = inline - _CREDENTIALS - _NON_WORKER_ENV - _ORDERING_CONTRACT_ENV
        assert not leaked, (
            f"{document['metadata']['name']} still configures {sorted(leaked)} "
            "through inline env; pass them as args instead"
        )
        assert container.get("args"), (
            f"{document['metadata']['name']} passes no flags at all"
        )


def test_every_worker_deployment_states_its_own_drain_budget() -> None:
    """The drain budget is the setting most likely to be silently inherited.

    It must cover the longest selected job timeout, so a group that does not
    state it is relying on a 30s package default that no real queue selection
    can satisfy.
    """
    for path in (_GO_COMPOSE, _GO_SWARM):
        for name, service in _compose_worker_services(path).items():
            command = " ".join(str(item) for item in service.get("command") or [])
            assert "--shutdown-timeout=" in command, (
                f"{path.name}:{name} does not state a drain budget"
            )


def _celery_queue_names() -> frozenset[str]:
    """The Celery app's declared queue set, read from the producer itself.

    Generated from ``workers.config.task_queues`` rather than hand-authored, so
    a queue added on the Python side cannot drift away from this comparison
    silently.
    """
    from dev_health_ops.workers.config import task_queues

    return frozenset(task_queues)


def _go_queue_names() -> frozenset[str]:
    """The queues the Go River fleet actually serves, per the deployment manifest."""
    import json

    manifest = json.loads(
        (_REPO_ROOT / "deploy" / "go-workers" / "deployment.json").read_text(
            encoding="utf-8"
        )
    )
    return frozenset(
        queue
        for process in manifest["processes"]
        for queue in process.get("queues", [])
    )


def test_go_queue_names_that_exist_in_celery_agree_byte_for_byte() -> None:
    """Where the two runtimes name the same queue, they must spell it the same.

    CHAOS-4020 mirrors the Celery worker CLI: `-Q/--queues` takes queue names,
    with Celery's meaning. The two fleets do not serve identical queue sets --
    the Go River topology has queues Celery never had, and Celery has queues no
    Go handler implements -- so this pins the part that must not drift: a queue
    both runtimes know is spelled identically, and the divergence is explicit
    rather than accidental.

    The divergence lists below are the migration's open question, tracked on
    CHAOS-4020. A name moving between the two sets should fail here and be a
    deliberate edit, not a surprise.
    """
    celery = _celery_queue_names()
    go = _go_queue_names()

    shared = celery & go
    assert shared == {"metrics", "reports", "sync", "webhooks"}, (
        f"the shared queue vocabulary changed: {sorted(shared)}"
    )

    # Queues the Go fleet serves that the Celery app never declared.
    assert go - celery == {
        "coverage",
        "heartbeat",
        "investment",
        "retention",
        "sync_provider",
        "workgraph",
    }, f"Go-only queues changed: {sorted(go - celery)}"

    # Celery queues no Go worker implements a handler for. A Go worker must not
    # accept these: startup validation requires the selected queue set to equal
    # the CONSTRUCTED handler set, so selecting one would fail readiness.
    assert celery - go == {
        "backfill",
        "default",
        "external-ingest",
        "ingest",
        "monitoring",
        "scheduler",
        "sync.github",
        "sync.github.heavy",
        "sync.github.light",
        "sync.github.medium",
        "sync.gitlab",
        "sync.gitlab.heavy",
        "sync.gitlab.light",
        "sync.gitlab.medium",
        "sync.jira",
        "sync.jira.medium",
        "sync.launchdarkly",
        "sync.linear",
        "sync.linear.medium",
    }, f"Celery-only queues changed: {sorted(celery - go)}"


def test_every_queue_a_manifest_selects_is_one_the_go_fleet_serves() -> None:
    """`-Q` names real queues; a manifest cannot select one nothing serves."""
    go = _go_queue_names()
    for path, extract in (
        (_GO_COMPOSE, _compose_worker_services),
        (_GO_SWARM, _compose_worker_services),
    ):
        for name, service in extract(path).items():
            for item in service.get("command") or []:
                if not str(item).startswith("--queues="):
                    continue
                selected = set(str(item).split("=", 1)[1].split(","))
                unknown = selected - go
                assert not unknown, (
                    f"{path.name}:{name} selects queues the Go fleet does not "
                    f"serve: {sorted(unknown)}"
                )


# Flags whose value would otherwise come from the shared ConfigMap. Because
# resolution is flag > env, hard-coding one of these into a Deployment's args
# silently disables the ConfigMap as an operator tuning surface.
_CONFIGMAP_BACKED_FLAGS = {
    "--river-schema": "RIVER_DATABASE_SCHEMA",
    "--domain-database-role": "RIVER_DOMAIN_DATABASE_ROLE",
    "--queue-database-role": "RIVER_QUEUE_DATABASE_ROLE",
    "--operational-bridge-url": "WORKER_OPERATIONAL_BRIDGE_URL",
    "--operational-bridge-allow-insecure": "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE",
    "--pagerduty-webhook-transport": "PAGERDUTY_WEBHOOK_TRANSPORT",
}


def test_kubernetes_args_never_shadow_the_configmap() -> None:
    """A Deployment must not hard-code a setting the ConfigMap owns.

    CHAOS-4020 made flags win over the environment. A Deployment that both
    imports ``dev-health-config`` via envFrom AND passes the same setting as an
    argument therefore renders the ConfigMap inert for that key: editing it
    changes nothing.

    ``PAGERDUTY_WEBHOOK_TRANSPORT`` is the sharp case. Both runtimes read it,
    and the contract is that exactly one owns the webhook stream. If a
    ConfigMap edit flipped the Python API to ``river`` while a hard-coded Go
    argument kept ``celery``, the stream would have two owners reconciling and
    deleting each other's entries.
    """
    configmap = yaml.safe_load(
        (_REPO_ROOT / "deploy" / "kubernetes" / "configmap.yaml").read_text(
            encoding="utf-8"
        )
    )["data"]

    for document in yaml.safe_load_all(_GO_KUBERNETES.read_text(encoding="utf-8")):
        if not document or document.get("kind") != "Deployment":
            continue
        container = document["spec"]["template"]["spec"]["containers"][0]
        imports_configmap = any(
            source.get("configMapRef") for source in container.get("envFrom") or []
        )
        if not imports_configmap:
            continue
        passed = _flags_of(container.get("args"))
        for flag, variable in _CONFIGMAP_BACKED_FLAGS.items():
            if variable not in configmap:
                continue
            assert flag.removeprefix("--") not in passed, (
                f"{document['metadata']['name']} hard-codes {flag}, which makes "
                f"ConfigMap key {variable} inert for this Deployment"
            )


def test_provider_routes_have_no_enablement_surface_at_all() -> None:
    """Route enablement must not exist, on the CLI or anywhere else.

    CHAOS-4054 deleted the plane outright. A worker executes the queues it
    subscribes to (``-Q``); the user's sync config decides what should run. The
    previous guard settled for "the forty switches have no flag", which was the
    right guard while the switches existed. The successor guard checks the
    thing that actually matters now: the registry file is gone, the Config type
    carries no route enablement field, and no route enablement name survives as
    a recognised setting.
    """
    assert not _ROUTES_SOURCE.exists(), (
        f"{_ROUTES_SOURCE.name} is back; the provider route switch registry was "
        "deleted with the enablement plane and must not return"
    )

    config_source = (_CONFIG_PACKAGE / "config.go").read_text(encoding="utf-8")
    options_source = (_CONFIG_PACKAGE / "options.go").read_text(encoding="utf-8")

    # Assembled from parts so this guard does not report its own source.
    switch_pattern = "WORKER_" + "[A-Z0-9_]+" + "_ENABLED"
    for name, source in (
        ("config.go", config_source),
        ("options.go", options_source),
    ):
        offending = sorted(set(re.findall(switch_pattern, source)))
        assert not offending, (
            f"{name} reintroduced route enablement variable(s) {offending}; "
            "capability is always on in the binary"
        )

    offending_fields = sorted(set(re.findall(r"\bWorker\w+Enabled\b", config_source)))
    assert not offending_fields, (
        f"config.go reintroduced route enablement field(s) {offending_fields}"
    )

    assert "--routes" not in options_source, (
        "options.go must not introduce a --routes flag; queue topology is the "
        "only serving lever"
    )
    assert "GO_PROVIDER_ROUTES" not in config_source, (
        "the all-routes preset was deleted with the switches it defaulted on"
    )
