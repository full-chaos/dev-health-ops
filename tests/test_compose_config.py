from __future__ import annotations

import argparse
import re
from pathlib import Path

import pytest
import yaml

from dev_health_ops.workers.config import task_queues


def _parse_queues(command_str: str) -> set[str]:
    """Extract the -Q/--queues list from a celery worker command string."""
    queues: set[str] = set()
    tokens = command_str.split()
    for i, token in enumerate(tokens):
        if token in ("-Q", "--queues") and i + 1 < len(tokens):
            queues.update(q for q in tokens[i + 1].split(",") if q)
        elif token.startswith("--queues="):
            queues.update(q for q in token.split("=", 1)[1].split(",") if q)
        elif token.startswith("-Q") and len(token) > 2:
            queues.update(q for q in token[2:].split(",") if q)
    return queues


def _stringify_command(value: object) -> str:
    return (
        " ".join(str(part) for part in value) if isinstance(value, list) else str(value)
    )


def _container_command_string(service: dict) -> str:
    parts = []
    entrypoint = service.get("entrypoint")
    command = service.get("command")
    if entrypoint:
        parts.append(_stringify_command(entrypoint))
    if command:
        parts.append(_stringify_command(command))
    return " ".join(parts)


def _go_worker_arguments(service: dict) -> dict[str, str]:
    command = service.get("command") or []
    assert isinstance(command, list), "Go worker command must use list form"
    arguments: dict[str, str] = {}
    for item in command:
        name, separator, value = str(item).partition("=")
        assert separator and name.startswith("--"), (
            f"invalid Go worker argument: {item}"
        )
        arguments[name] = value
    return arguments


def test_compose_workers_cover_every_celery_queue() -> None:
    """CHAOS-2278: the union of -Q lists across all compose celery worker
    services must cover every queue declared in workers.config.task_queues.

    Guards against adding a queue (or a worker topology change) that leaves
    a queue with no consumer — tasks routed there would silently never run.
    The previous topology shipped exactly that bug: `ingest` and `reports`
    existed in task_queues but no compose worker consumed them.
    """
    compose_path = Path(__file__).resolve().parents[1] / "compose.yml"
    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    consumed_queues: set[str] = set()
    worker_services: list[str] = []
    for name, service in compose_data["services"].items():
        command = service.get("command")
        if command is None:
            continue
        command_str = _container_command_string(service)
        tokens = command_str.split()
        if "celery" not in tokens or "worker" not in tokens:
            continue
        worker_services.append(name)
        consumed_queues.update(_parse_queues(command_str))

    assert worker_services, "no celery worker services found in compose.yml"

    missing = set(task_queues) - consumed_queues
    assert not missing, (
        f"queues declared in workers.config.task_queues but consumed by no "
        f"compose worker service: {sorted(missing)} "
        f"(workers: {sorted(worker_services)}, consumed: {sorted(consumed_queues)})"
    )


def test_celery_config_has_backfill_queue() -> None:
    assert "backfill" in task_queues


def test_celery_config_has_per_provider_sync_queues() -> None:
    """CHAOS-2299: each known sync provider has a dedicated queue so queue
    depth answers "is <provider> stuck?", and the shared `sync` queue stays
    declared as the fallback for unknown providers and messages already in
    flight at deploy time."""
    for provider in ("github", "gitlab", "linear", "jira", "launchdarkly"):
        assert f"sync.{provider}" in task_queues
    assert "sync" in task_queues


def test_queue_monitor_beat_entry() -> None:
    """CHAOS-2299: queue depth/age telemetry runs every minute on a dedicated
    `monitoring` queue — not `default`, which can flood (telemetry would die
    exactly when it is needed)."""
    from dev_health_ops.workers.config import beat_schedule

    entry = beat_schedule["monitor-queue-depths"]
    assert entry["task"] == "dev_health_ops.workers.tasks.monitor_queue_depths"
    assert entry["schedule"] == 60.0
    assert entry["options"] == {"queue": "monitoring"}


def test_scheduled_sync_dispatcher_uses_scheduler_queue() -> None:
    from dev_health_ops.workers.config import beat_schedule

    entry = beat_schedule["dispatch-scheduled-syncs"]
    assert entry["task"] == "dev_health_ops.workers.tasks.dispatch_scheduled_syncs"
    assert entry["schedule"] == 300.0
    assert entry["options"] == {"queue": "scheduler"}


def test_scheduler_queue_declared_and_consumed_redundantly() -> None:
    assert "scheduler" in task_queues

    compose_path = Path(__file__).resolve().parents[1] / "compose.yml"
    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    consumers: list[str] = []
    for name, service in compose_data["services"].items():
        command = service.get("command")
        if command is None:
            continue
        command_str = _container_command_string(service)
        tokens = command_str.split()
        if "celery" not in tokens or "worker" not in tokens:
            continue
        if "scheduler" in _parse_queues(command_str):
            consumers.append(name)

    assert "worker-heavy" in consumers
    assert len(consumers) >= 2, (
        f"`scheduler` must be consumed by >=2 worker services for redundancy, "
        f"found: {sorted(consumers)}"
    )


def test_monitoring_queue_declared_and_consumed_redundantly() -> None:
    """The `monitoring` queue must exist in task_queues and be consumed by at
    least two compose worker services so queue telemetry survives one pool
    being saturated or down."""
    assert "monitoring" in task_queues

    compose_path = Path(__file__).resolve().parents[1] / "compose.yml"
    compose_data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    consumers: list[str] = []
    for name, service in compose_data["services"].items():
        command = service.get("command")
        if command is None:
            continue
        command_str = _container_command_string(service)
        tokens = command_str.split()
        if "celery" not in tokens or "worker" not in tokens:
            continue
        if "monitoring" in _parse_queues(command_str):
            consumers.append(name)

    assert len(consumers) >= 2, (
        f"`monitoring` must be consumed by >=2 worker services for redundancy, "
        f"found: {sorted(consumers)}"
    )


# ---------------------------------------------------------------------------
# CHAOS-2304: production deploy stacks must run migrations as an explicit
# one-shot step — app services never ambient-migrate (AUTO_RUN_MIGRATIONS=false).
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PROD_COMPOSE = _REPO_ROOT / "deploy" / "docker-compose" / "compose.production.yml"
_LEGACY_COMPOSE = _REPO_ROOT / "compose.yml"
_SWARM_STACK = _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml"
_GO_WORKER_OVERLAY = _REPO_ROOT / "deploy" / "go-workers" / "compose-go-workers.yml"
_GO_CONFIG_PACKAGE = _REPO_ROOT / "internal" / "platform" / "config"
_K8S_DIR = _REPO_ROOT / "deploy" / "kubernetes"
_HELM_DIR = _REPO_ROOT / "deploy" / "helm" / "dev-health"


#: CHAOS-4054 deleted the whole ``WORKER_*_ENABLED`` route-switch plane: no
#: env var gates whether a shipped route may execute any more (readiness and
#: plannability live entirely in the checked-in provider matrix). The
#: per-pair census this module used to derive from the matrix therefore has
#: no successor value -- see ``test_compose_declares_no_provider_route_switches``
#: and ``test_provider_route_env_example_declares_no_route_switches`` below,
#: which assert the literal opposite: zero such keys anywhere in the stack.
_WORKER_ENABLED_SWITCH_PATTERN = re.compile(r"\bWORKER_[A-Z0-9_]+_ENABLED\b")

_PROVIDER_ROUTE_CONFIG_NAMES = (
    "WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH",
    "WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH",
)


def _platform_compose_path() -> Path | None:
    for parent in _REPO_ROOT.parents:
        candidate = parent / "compose.yml"
        if not candidate.exists():
            continue
        services = _load_yaml(candidate).get("services") or {}
        api_volumes = services.get("api", {}).get("volumes") or []
        if "api" in services and "worker" in services and "./ops:/app" in api_volumes:
            return candidate
    return None


def _platform_go_compose_path() -> Path | None:
    for parent in _REPO_ROOT.parents:
        candidate = parent / "compose.yml"
        if not candidate.exists():
            continue
        services = _load_yaml(candidate).get("services") or {}
        if "api" in services and "go-worker" in services:
            return candidate
    return None


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# Measured cost of one Go worker replica on the queue-session endpoint:
# WORKER_DATABASE_MAX_CONNS pgxpool connections (2) plus the long-lived River
# notifier session, which lives outside that pool budget. Sizing from the
# environment variable alone under-counts by a third.
_QUEUE_CONNECTIONS_PER_REPLICA = 3
_QUEUE_WORKER_GROUPS = ("go-worker", "go-worker-heavy", "go-worker-ops")
# The platform stack must support this many replicas of every worker group.
_PLATFORM_REPLICAS_PER_GROUP = 3
# Reconciler, scheduler, and riverui hold queue-endpoint sessions too.
_QUEUE_SINGLETON_CONNECTIONS = 5
# Migrations bypass the poolers and connect to PostgreSQL directly.
_MIGRATION_CONNECTIONS = 5
# Superuser slots PostgreSQL keeps back from ordinary roles.
_POSTGRES_RESERVED = 3

_COMPOSE_DEFAULT = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*:-(?P<default>[^}]*)\}$")


def _compose_int(value: object) -> int:
    """Resolve a compose scalar that may be ``${VAR:-default}`` to its int.

    Interpolated defaults are what an unconfigured checkout actually runs, so
    a budget assertion has to read through the substitution rather than
    compare the literal string.
    """
    text = str(value)
    match = _COMPOSE_DEFAULT.match(text)
    if match is not None:
        text = match.group("default")
    return int(text)


def _postgres_max_connections(service: dict) -> int:
    """Return the max_connections the postgres service is started with."""
    command = service.get("command") or []
    if isinstance(command, str):
        command = command.split()
    for entry in command:
        text = str(entry)
        if text.startswith("max_connections="):
            return _compose_int(text.split("=", 1)[1])
    # The postgres image default when the service passes no override.
    return 100


def _command_string(service: dict) -> str:
    return _container_command_string(service)


def _is_beat_service(name: str, service: dict) -> bool:
    command = _command_string(service).split()
    return name == "beat" or ("celery" in command and "beat" in command)


def _assert_compose_beat_singleton(path: Path) -> None:
    services = _load_yaml(path).get("services") or {}
    beat_services = [
        (name, service)
        for name, service in services.items()
        if _is_beat_service(name, service)
    ]
    assert len(beat_services) == 1, f"{path.name} must define exactly one beat service"
    for name, service in beat_services:
        replicas = (service.get("deploy") or {}).get("replicas")
        assert replicas in (None, 1), f"{path.name}:{name} must not exceed 1 replica"


def test_platform_go_worker_drain_contract_matches_groups() -> None:
    compose_path = _platform_go_compose_path()
    if compose_path is None:
        pytest.skip("platform compose checkout is unavailable")

    services = _load_yaml(compose_path)["services"]
    manifest = {
        process["name"]: process
        for process in _load_yaml(_REPO_ROOT / "deploy/go-workers/deployment.json")[
            "processes"
        ]
    }
    service_profiles = {
        "go-worker": "sync",
        "go-worker-heavy": "heavy",
        "go-worker-ops": "ops",
    }
    for service_name, profile in service_profiles.items():
        grace = manifest[profile]["shutdown_grace_seconds"]
        service = services[service_name]
        assert _go_worker_arguments(service)["--shutdown-timeout"] == f"{grace}s"
        assert service["stop_grace_period"] == f"{grace}s"


def test_production_compose_has_one_shot_migrate_service() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    migrate = services.get("migrate")
    assert migrate is not None, "compose.production.yml must define a migrate service"
    assert migrate.get("restart") == "no"
    entrypoint = " ".join(str(p) for p in migrate["entrypoint"])
    assert "dev-hops migrate clickhouse" in entrypoint
    assert "dev-hops migrate postgres" in entrypoint


def test_production_compose_app_services_gate_on_migrate() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    for name in ("api", "billing-edge", "worker", "beat"):
        deps = services[name].get("depends_on") or {}
        assert (
            deps.get("migrate", {}).get("condition") == "service_completed_successfully"
        ), f"{name} must gate on migrate completing successfully"


def test_production_compose_disables_ambient_migrations() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    for name in ("api", "worker", "beat"):
        env = services[name].get("environment") or {}
        assert env.get("AUTO_RUN_MIGRATIONS") == "false", (
            f"{name} must set AUTO_RUN_MIGRATIONS=false — schema is applied by "
            f"the one-shot migrate service"
        )


def test_production_api_healthcheck_uses_ready_probe() -> None:
    services = _load_yaml(_PROD_COMPOSE)["services"]
    command = " ".join(str(part) for part in services["api"]["healthcheck"]["test"])

    assert "/ready" in command
    assert "/health" not in command


def test_legacy_compose_has_one_shot_migrate_service() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    migrate = services.get("migrate")
    assert migrate is not None, "compose.yml must define a migrate service"
    assert migrate.get("restart") == "no"
    entrypoint = " ".join(str(p) for p in migrate["entrypoint"])
    assert "dev-hops migrate clickhouse" in entrypoint
    assert "dev-hops migrate postgres" in entrypoint


def test_legacy_compose_migrate_waits_for_postgres_health() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    migrate = services["migrate"]

    depends_on = migrate.get("depends_on") or {}
    assert depends_on.get("postgres", {}).get("condition") == "service_healthy"
    assert depends_on.get("clickhouse", {}).get("condition") == "service_healthy"


def test_legacy_compose_migrate_uses_local_build_matching_api() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    migrate = services["migrate"]
    api = services["api"]

    assert migrate.get("image") is None
    assert isinstance(migrate.get("build"), dict)
    assert migrate["build"] == api["build"]


def test_local_postgres_bootstraps_distinct_go_runtime_roles() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    environment = services["postgres"]["environment"]
    assert environment["RIVER_DOMAIN_DATABASE_ROLE"] == (
        "${RIVER_DOMAIN_DATABASE_ROLE:-devhealth_domain}"
    )
    assert environment["RIVER_QUEUE_DATABASE_ROLE"] == (
        "${RIVER_QUEUE_DATABASE_ROLE:-devhealth_queue}"
    )

    init_script = (_REPO_ROOT / "docker" / "init-extra-dbs.sh").read_text(
        encoding="utf-8"
    )
    assert "CREATE ROLE %I LOGIN NOSUPERUSER" in init_script
    for attribute in ("NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS"):
        assert attribute in init_script
    assert '-v app_database="$POSTGRES_DB"' in init_script
    assert ':"app_database"' in init_script
    assert "GRANT CONNECT ON DATABASE" in init_script
    assert (
        'REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role",'
        ' :"queue_role", :"coordinator_role";' in init_script
    )
    # CHAOS-3033: the coordinator role of the Option B split is provisioned
    # here alongside the other two so local dev and CI are self-provisioning.
    # It deliberately receives no per-table grants -- those are owned by the
    # pinned River migration, derived from postgres.CoordinatorPosture() -- so
    # this only asserts the login and its ability to connect.
    for coordinator_fragment in (
        "${RIVER_COORDINATOR_DATABASE_ROLE:-devhealth_coordinator}",
        '-v coordinator_role="$river_coordinator_role"',
        ":'coordinator_role', :'coordinator_password'",
        'GRANT CONNECT ON DATABASE :"app_database" TO :"coordinator_role";',
        'GRANT USAGE ON SCHEMA public TO :"coordinator_role";',
        'REVOKE CREATE ON SCHEMA public FROM :"coordinator_role";',
    ):
        assert coordinator_fragment in init_script
    # worker_job_routes is coordinator-exclusive under the split, so the domain
    # role must no longer be granted it here -- that grant contradicted
    # domainPosture() and would fail domain readiness if provisioning ran
    # after a migration.
    assert "worker_job_routes" not in init_script
    for grant in (
        "GRANT USAGE ON SCHEMA public",
        "REVOKE CREATE ON SCHEMA public FROM",
        "public.alembic_version",
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public",
        "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public",
        "GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox",
        "GRANT SELECT, INSERT ON TABLE public.worker_job_delivery_abandonments",
        "GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences",
    ):
        assert grant in init_script
    assert "REVOKE CREATE ON SCHEMA public FROM PUBLIC" not in init_script
    assert (
        'REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, :"domain_role", :"queue_role";'
        in init_script
    )
    assert (
        "GRANT INSERT ON TABLE public.worker_job_completion_fences" not in init_script
    )
    _assert_least_privilege_domain_grants(
        init_script.split('GRANT USAGE ON SCHEMA public TO :"queue_role";', maxsplit=1)[
            0
        ]
    )


def test_provision_river_roles_sql_is_not_a_grant_authority() -> None:
    """CHAOS-4261: this script must be role-creation-and-connectivity
    bootstrap only. Every prior version carried its own hand-maintained
    per-table GRANT whitelist behind a REVOKE ALL, which drifted behind
    postgres.domainPosture()/coordinatorPosture() as tables were added over
    time -- and because it ran on every `docker compose run go-workerctl` (or
    any other service depending on go-river-provision without
    go-river-migrate), each drift silently wiped a production role back down
    to the stale subset. The single authority for every per-table/sequence
    privilege on all three runtime roles is now
    internal/storage/river/migrate.go's runtimeGrantStatements /
    coordinatorGrantStatements, applied only by go-river-migrate.

    This is a completeness test, not a narrow-set enshrinement: whatever this
    script DOES grant must be a subset of nothing wider than schema-level
    bootstrap, because the moment it grants a single table-level privilege
    again, it is back to being a second copy of the manifest that can drift
    from the Go source of truth. Assert the absence of the whole class of
    statement, not a specific table list.
    """
    upgrade_script = (
        _REPO_ROOT / "scripts" / "worker" / "provision_river_roles.sql"
    ).read_text(encoding="utf-8")
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION" in upgrade_script
    assert "WHERE NOT EXISTS" in upgrade_script
    # All three Option B runtime roles must be pairwise distinct, so the guard
    # now covers the coordinator role too.
    assert (
        "domain_role, queue_role, and coordinator_role must be distinct"
        in upgrade_script
    )
    assert (
        'REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role",'
        ' :"queue_role", :"coordinator_role";' in upgrade_script
    )
    # Bootstrap connectivity for all three roles -- login + CONNECT + schema
    # USAGE, and nothing that touches a table or a sequence.
    for role in ("domain_role", "queue_role", "coordinator_role"):
        assert (
            f'GRANT CONNECT ON DATABASE :"app_database" TO :"{role}"' in upgrade_script
        )
        assert f'GRANT USAGE ON SCHEMA public TO :"{role}"' in upgrade_script
        assert f'REVOKE CREATE ON SCHEMA public FROM :"{role}"' in upgrade_script
    # The whole class of statement this ticket removed must never come back,
    # not just the specific tables it used to name. Strip `--` line comments
    # first: this docstring and the script's own explanatory comments both
    # say "REVOKE ALL" in prose (describing what the OLD script did), and a
    # raw substring check would flag its own documentation.
    code_only = "\n".join(
        line.split("--", 1)[0] for line in upgrade_script.splitlines()
    )
    assert "REVOKE ALL" not in code_only
    assert (
        re.search(r"GRANT\s+(SELECT|INSERT|UPDATE|DELETE)[^;]*ON TABLE", code_only)
        is None
    ), (
        "provision_river_roles.sql grants a table-level privilege again -- "
        "this makes it a grant authority, which is exactly the CHAOS-4261 "
        "defect. Per-table/sequence grants belong solely in "
        "internal/storage/river/migrate.go, applied by go-river-migrate."
    )
    # Any privilege, not just SELECT/INSERT/UPDATE/DELETE: coordinatorGrantStatements'
    # OWN sequence grant in migrate.go is `GRANT USAGE ON SEQUENCE ...`, which a
    # verb-restricted regex here would silently miss if it ever leaked into this
    # bootstrap-only script.
    assert re.search(r"GRANT\s+\S[^;]*ON SEQUENCE", code_only) is None


def test_init_extra_dbs_sh_is_only_reachable_through_postgres_container_init() -> None:
    """CHAOS-4261: `docker/init-extra-dbs.sh` has the same REVOKE-ALL-then-
    whitelist shape `provision_river_roles.sql` used to and was deliberately
    left untouched by this ticket -- it is not implicated in the prod
    incident (Postgres's own `docker-entrypoint-initdb.d` mechanism only
    ever runs it once, against a brand-new, empty data volume, and the local
    `go-river-migrate` step that follows always restores the full posture
    the same run) and is out of this ticket's stated scope.

    That safety argument depends entirely on nothing else ever being able to
    invoke it. Assert the precondition directly rather than trusting the
    argument to stay true: the script must be mounted by exactly one
    service (`postgres` in the legacy root `compose.yml`, into
    `/docker-entrypoint-initdb.d/`) and never referenced by any other
    compose file in this repository -- if a future change wires it into
    `compose.production.yml`, the go-workers overlay, or the swarm stack,
    this fails loudly instead of quietly recreating CHAOS-4261 for a second
    script.
    """
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    mounting_services = [
        name
        for name, service in services.items()
        if any(
            "init-extra-dbs.sh" in str(volume)
            for volume in service.get("volumes") or []
        )
    ]
    assert mounting_services == ["postgres"], (
        f"init-extra-dbs.sh must be mounted by exactly the postgres service, "
        f"found: {mounting_services}"
    )
    postgres_volumes = [
        str(volume) for volume in services["postgres"].get("volumes") or []
    ]
    assert any(
        "init-extra-dbs.sh:/docker-entrypoint-initdb.d/" in volume
        for volume in postgres_volumes
    ), (
        "init-extra-dbs.sh must be mounted into /docker-entrypoint-initdb.d/ (initdb-only, never re-run against an existing volume)"
    )

    # A comment cross-referencing the filename (deploy/go-workers/
    # compose-go-workers.yml does this, explaining why go-river-provision
    # exists at all) is documentation, not a reachability path -- check each
    # service's actual volumes/entrypoint/command fields, the only places a
    # compose file can make Postgres execute a script, not the raw file text.
    for other_compose in (_PROD_COMPOSE, _SWARM_STACK, _GO_WORKER_OVERLAY):
        other_services = _load_yaml(other_compose).get("services") or {}
        for name, service in other_services.items():
            for field in ("volumes", "entrypoint", "command"):
                assert "init-extra-dbs.sh" not in str(service.get(field) or ""), (
                    f"{other_compose.name} service {name!r} references "
                    f"init-extra-dbs.sh in its {field!r} -- it is a local-"
                    "Postgres-container-init-only script and must never be "
                    "reachable outside the postgres service's own initdb mount"
                )


def _assert_least_privilege_domain_grants(domain_script: str) -> None:
    # worker_job_routes is absent by design: CHAOS-3033 attributes it
    # exclusively to the coordinator role, and domainPosture() declares no
    # privilege on it at all. Re-adding it here would put the provisioning
    # scripts back in contradiction with domain readiness.
    expected_read_only_tables = {
        "integrations",
        "integration_credentials",
        "sync_dispatch_transport_routes",
    }
    configured_read_only_tables = _tables_for_formatted_grant(
        domain_script, "GRANT SELECT ON TABLE public.%I TO %I"
    )
    assert configured_read_only_tables == expected_read_only_tables, (
        "read-only grant block covers the wrong tables"
    )
    # These tables are mutable domain state, so they deliberately live in the
    # separate read/write grant rather than the read-only VALUES list above.
    expected_read_write_tables = {
        "integration_sources",
        "integration_datasets",
        "sync_runs",
        "sync_run_units",
    }
    configured_read_write_tables = _tables_for_formatted_grant(
        domain_script, "GRANT SELECT, INSERT, UPDATE ON TABLE public.%I TO %I"
    )
    assert configured_read_write_tables == expected_read_write_tables, (
        "read-write grant block covers the wrong tables"
    )
    for grant in (
        "GRANT SELECT ON TABLE public.%I TO %I",
        "GRANT SELECT, INSERT, UPDATE ON TABLE public.%I TO %I",
        "GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks",
        "GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox",
        "GRANT SELECT, INSERT ON TABLE public.worker_job_outbox",
        "GRANT SELECT, INSERT, DELETE ON TABLE public.sync_run_unit_effect_snapshots",
    ):
        assert grant in domain_script
    for forbidden in (
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES",
        "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES",
        "DELETE ON ALL TABLES",
    ):
        assert forbidden not in domain_script
    # Scoped claim: within the provisioning scripts' DOMAIN SECTION, only the
    # snapshot and reviewed worker-lifecycle tables receive per-table DELETE.
    # The domain ROLE holds DELETE on more tables through domainPosture(); they
    # are simply not granted by these scripts. Stating this unscoped would be
    # false.
    #
    # Assert the exact set rather than banning one spelling: admitting the
    # snapshot grant meant relaxing a blanket "DELETE ON TABLE" ban, and a
    # relaxed ban admits every future DELETE grant too. A templated
    # `public.%I` DELETE block surfaces here as the literal "%I" and so
    # cannot slip through this equality either.
    assert _tables_for_delete_grants(domain_script) == {
        "sync_run_unit_effect_snapshots",
        "worker_concurrency_leases",
        "worker_instances",
    }


def _tables_for_delete_grants(domain_script: str) -> set[str]:
    """Tables granted DELETE, whatever order the privilege list is written in.

    Matching "DELETE ON TABLE" only sees DELETE written last:
    `GRANT SELECT, DELETE, UPDATE ON TABLE ...` slips straight past it. That
    used to be covered by a blanket ban on the substring "DELETE ON TABLE",
    which this file had to relax to admit the snapshot grant -- so the narrow
    regex and the removed ban left the same hole open together. Parse the
    privilege list instead.
    """
    granted: set[str] = set()
    for privileges, table in re.findall(
        r"GRANT\s+([A-Z,\s()a-z_]+?)\s+ON TABLE public\.([A-Za-z0-9_%]+)",
        domain_script,
    ):
        names = {
            privilege.split("(", maxsplit=1)[0].strip().upper()
            for privilege in privileges.split(",")
        }
        if "DELETE" in names:
            granted.add(table)
    return granted


# init-extra-dbs.sh (local dev bootstrap) is the only provisioning script
# left with a per-table domain-section grant block to slice out this way.
# CHAOS-4261 removed that block from provision_river_roles.sql entirely (see
# test_provision_river_roles_sql_is_not_a_grant_authority) -- it is no longer
# a grant authority at all, so there is no domain section left to test here.
_DOMAIN_SECTION_MARKERS = {
    "init-extra-dbs.sh": 'GRANT USAGE ON SCHEMA public TO :"queue_role";',
}


def _domain_section(script_path: Path) -> str:
    marker = _DOMAIN_SECTION_MARKERS[script_path.name]
    script = script_path.read_text(encoding="utf-8")
    assert marker in script, (
        f"{script_path.name} does not contain its domain-section marker "
        f"{marker!r}; slicing on a missing marker yields the whole file and "
        f"every domain assertion silently widens to the queue role"
    )
    return script.split(marker, maxsplit=1)[0]


def _tables_for_formatted_grant(domain_script: str, grant: str) -> set[str]:
    pattern = re.compile(
        rf"SELECT\s+format\(\s*'{re.escape(grant)}'.*?"
        r"FROM\s+\((.*?)\)\s+AS\s+required\(table_name\)",
        re.DOTALL,
    )
    matches = pattern.findall(domain_script)
    assert len(matches) == 1, f"expected exactly one VALUES block for {grant!r}"
    return set(re.findall(r"\('([^']+)'\)", matches[0]))


@pytest.mark.parametrize(
    "script_path",
    (_REPO_ROOT / "docker" / "init-extra-dbs.sh",),
)
def test_least_privilege_domain_grants_reject_swapped_privilege_blocks(
    script_path: Path,
) -> None:
    domain_script = _domain_section(script_path)
    read_only_grant = "GRANT SELECT ON TABLE public.%I TO %I"
    read_write_grant = "GRANT SELECT, INSERT, UPDATE ON TABLE public.%I TO %I"
    inverted = (
        domain_script.replace(read_only_grant, "__READ_ONLY_GRANT__")
        .replace(read_write_grant, read_only_grant)
        .replace("__READ_ONLY_GRANT__", read_write_grant)
    )
    assert inverted != domain_script, "the swap fixture changed nothing"

    # The control: unmodified input must PASS. Without it a bare
    # pytest.raises proves only that SOMETHING raised -- deleting the swap
    # assertions entirely still satisfied it.
    _assert_least_privilege_domain_grants(domain_script)
    with pytest.raises(AssertionError) as failure:
        _assert_least_privilege_domain_grants(inverted)
    # And the reason is asserted, not assumed: the swap must be caught by the
    # read-only/read-write table sets, not by some unrelated later assertion.
    assert "grant block covers the wrong tables" in str(failure.value), str(
        failure.value
    )


def test_legacy_compose_disables_ambient_migrations() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    for name in ("api", "billing-edge", "worker", "worker-ingest", "worker-heavy"):
        env = services[name].get("environment") or {}
        assert env.get("AUTO_RUN_MIGRATIONS") == "false", (
            f"{name} must set AUTO_RUN_MIGRATIONS=false — schema is applied by "
            f"the one-shot migrate service"
        )


def test_legacy_compose_app_services_gate_on_migrate() -> None:
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    for name in ("api", "billing-edge", "worker", "worker-ingest", "worker-heavy"):
        deps = services[name].get("depends_on") or {}
        assert (
            deps.get("migrate", {}).get("condition") == "service_completed_successfully"
        ), f"{name} must gate on migrate completing successfully"


def test_swarm_stack_has_migrate_service_and_disables_ambient_migrations() -> None:
    services = _load_yaml(_SWARM_STACK)["services"]
    migrate = services.get("migrate")
    assert migrate is not None, "stack.yml must define a migrate service"
    restart = migrate["deploy"]["restart_policy"]["condition"]
    assert restart == "none", "swarm migrate must be one-shot (restart: none)"
    entrypoint = " ".join(str(p) for p in migrate["entrypoint"])
    assert "dev-hops migrate clickhouse" in entrypoint
    for name in ("api", "worker"):
        env = services[name].get("environment") or {}
        assert env.get("AUTO_RUN_MIGRATIONS") == "false"


@pytest.mark.parametrize(
    "manifest",
    [_LEGACY_COMPOSE, _PROD_COMPOSE, _SWARM_STACK],
)
def test_compose_migration_jobs_remove_empty_dedicated_uri(manifest: Path) -> None:
    """Compose interpolation creates an empty-but-present value by default.

    The migration CLI deliberately treats an explicitly empty elevated DSN as
    a configuration error, so each compatibility entrypoint must remove that
    placeholder before running the legacy POSTGRES_URI-only Alembic path.
    """
    migrate = _load_yaml(manifest)["services"]["migrate"]
    entrypoint = " ".join(str(part) for part in migrate["entrypoint"])
    assert "unset MIGRATION_DATABASE_URI" in entrypoint
    assert "MIGRATION_DATABASE_URI_FILE+x" in entrypoint


def test_kubernetes_manifests_run_migrations_as_job() -> None:
    job_docs = [
        d
        for d in yaml.safe_load_all(
            (_K8S_DIR / "migrate-job.yaml").read_text(encoding="utf-8")
        )
        if d
    ]
    jobs = [d for d in job_docs if d.get("kind") == "Job"]
    assert len(jobs) == 1
    pod_spec = jobs[0]["spec"]["template"]["spec"]
    assert pod_spec["restartPolicy"] == "Never"
    command = " ".join(pod_spec["containers"][0]["command"])
    assert "dev-hops migrate clickhouse" in command
    assert "unset MIGRATION_DATABASE_URI" in command
    assert "MIGRATION_DATABASE_URI_FILE+x" in command

    config = _load_yaml(_K8S_DIR / "configmap.yaml")
    assert config["data"]["AUTO_RUN_MIGRATIONS"] == "false"

    kustomization = _load_yaml(_K8S_DIR / "kustomization.yaml")
    assert "migrate-job.yaml" in kustomization["resources"]

    migration_secret = next(
        document
        for document in _k8s_docs("secrets.yaml")
        if document.get("kind") == "Secret"
        and document["metadata"]["name"] == "dev-health-migration-secrets"
    )
    migration_data = migration_secret.get("stringData") or {}
    assert "MIGRATION_DATABASE_URI" not in migration_data, (
        "the checked-in migration Secret must omit MIGRATION_DATABASE_URI; "
        "an empty key is a fail-closed CLI configuration error"
    )
    assert set(migration_data) == {"POSTGRES_URI", "CLICKHOUSE_URI"}


def _k8s_docs(filename: str) -> list[dict]:
    return [
        d
        for d in yaml.safe_load_all((_K8S_DIR / filename).read_text(encoding="utf-8"))
        if d
    ]


def test_kubernetes_secret_exposes_clickhouse_uri_for_migrate(monkeypatch) -> None:
    """`dev-hops migrate clickhouse` (the Job) and `status --check` (the
    wait-for-migrations initContainers) resolve CLICKHOUSE_URI via
    resolve_sink_uri — they do NOT read DATABASE_URI. Without CLICKHOUSE_URI
    in the secret the migrate Job fails on first boot."""
    from dev_health_ops.db import resolve_sink_uri

    shared_secret = next(
        d
        for d in _k8s_docs("secrets.yaml")
        if d.get("kind") == "Secret" and d["metadata"]["name"] == "dev-health-secrets"
    )
    migration_secret = next(
        d
        for d in _k8s_docs("secrets.yaml")
        if d.get("kind") == "Secret"
        and d["metadata"]["name"] == "dev-health-migration-secrets"
    )
    uri = migration_secret["stringData"].get("CLICKHOUSE_URI")
    assert uri, "dev-health-migration-secrets must define CLICKHOUSE_URI"
    assert uri.startswith("clickhouse://")

    # The value must be resolvable exactly the way the migrate CLI resolves it.
    monkeypatch.setenv("CLICKHOUSE_URI", uri)
    assert resolve_sink_uri(argparse.Namespace(analytics_db=None)) == uri

    # ...and the Job must actually see the secret (envFrom).
    job = next(d for d in _k8s_docs("migrate-job.yaml") if d.get("kind") == "Job")
    container = job["spec"]["template"]["spec"]["containers"][0]
    secret_refs = {
        ref["secretRef"]["name"]
        for ref in container.get("envFrom", [])
        if "secretRef" in ref
    }
    assert secret_refs == {"dev-health-migration-secrets"}
    assert "GITHUB_TOKEN" in shared_secret["stringData"]
    assert "GITHUB_TOKEN" not in migration_secret["stringData"]


def test_kubernetes_app_deployments_wait_for_migrations() -> None:
    """CHAOS-2304 safety net: a naive `kubectl apply -k` rolls Deployments
    without waiting for the migrate Job. api must carry a read-only
    wait-for-migrations initContainer that blocks until the schema is
    current (`dev-hops migrate clickhouse status --check`) and never runs
    DDL itself. The Go worker groups in go-workers.yaml (CHAOS-4195, which
    replaced the Celery worker.yaml this test used to also parametrize over)
    deploy at replicas: 0 and carry no such initContainer of their own."""
    deployment = next(d for d in _k8s_docs("api.yaml") if d.get("kind") == "Deployment")
    pod_spec = deployment["spec"]["template"]["spec"]
    waiter = next(
        (
            c
            for c in pod_spec.get("initContainers") or []
            if c["name"] == "wait-for-migrations"
        ),
        None,
    )
    assert waiter is not None, (
        "api.yaml must define a wait-for-migrations initContainer"
    )

    command = " ".join(waiter["command"])
    assert "dev-hops migrate clickhouse status --check" in command
    # Read-only contract: every dev-hops invocation in the waiter is the
    # status --check probe — it must never run the upgrade (DDL) path.
    assert command.count("dev-hops") == command.count(
        "dev-hops migrate clickhouse status --check"
    )

    secret_refs = {
        ref["secretRef"]["name"]
        for ref in waiter.get("envFrom", [])
        if "secretRef" in ref
    }
    assert "dev-health-secrets" in secret_refs, (
        "waiter needs the secret env (CLICKHOUSE_URI) to resolve the DSN"
    )


def test_helm_chart_runs_migrations_as_pre_upgrade_hook() -> None:
    # Helm templates are Go-templated, so assert on text rather than YAML.
    template = (_HELM_DIR / "templates" / "migrate-job.yaml").read_text(
        encoding="utf-8"
    )
    assert 'helm.sh/hook: {{ join "," $hookEvents }}' in template
    assert "helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded" in template
    assert "dev-hops migrate clickhouse" in template
    assert "dev-hops admin features seed" in template
    assert "unset MIGRATION_DATABASE_URI" in template
    assert "MIGRATION_DATABASE_URI_FILE+x" in template

    helpers = (_HELM_DIR / "templates" / "_helpers.tpl").read_text(encoding="utf-8")
    assert "AUTO_RUN_MIGRATIONS" in helpers
    assert '.Values.secrets.data "DATABASE_URI"' in helpers

    values = _load_yaml(_HELM_DIR / "values.yaml")
    assert values["migrations"]["hook"]["enabled"] is True
    assert values["migrations"]["hook"]["events"] == [
        "pre-install",
        "pre-upgrade",
    ]
    assert values["migrations"]["hook"]["localBundledPostgres"] is False


def test_deploy_stacks_keep_celery_beat_singleton() -> None:
    """Compose-only: the archived Celery Beat service must stay a singleton
    on every deploy target that still checks one in. CHAOS-4195 deleted the
    Kubernetes (beat.yaml) and Helm (beat-deployment.yaml) equivalents this
    test used to also assert on -- their Go successor is the `scheduler`
    goWorkers group (`kubernetes/go-workers.yaml`'s dev-health-go-scheduler
    Deployment), which is a singleton by operational convention ("Run exactly
    one active production scheduler", docs/operate/configure/
    workers-and-schedules.md) rather than a machine-checked replica pin --
    like every group it deploys at replicas: 0 until an operator scales it,
    so there is no static "always exactly 1" invariant left to assert here."""
    for stack in (_REPO_ROOT / "compose.yml", _PROD_COMPOSE, _SWARM_STACK):
        _assert_compose_beat_singleton(stack)


def test_celery_worker_prefetch_multiplier_is_one() -> None:
    """CHAOS-2277: long-running tasks (sync, stream consumers) + default
    prefetch (4) let reserved slow-queue messages fill the QoS window and
    block fetching from other queues entirely — Sync Now appeared stuck
    until a worker restart released the unacked reservations. One-at-a-time
    fetching keeps cross-queue round-robin fair."""
    from dev_health_ops.workers.config import worker_prefetch_multiplier

    assert worker_prefetch_multiplier == 1


def test_celery_worker_prefetch_is_disabled_for_redis() -> None:
    from dev_health_ops.workers.config import worker_disable_prefetch

    assert worker_disable_prefetch is True


# CHAOS-4026 (2026-08-21): test_stream_consumer_beat_ticks_do_not_outlive_cadence
# tested the process-ingest-streams/process-product-telemetry-streams beat
# entries and run_ingest_consumer/run_product_telemetry_consumer, deleted
# with this cleanup (Go's stream-ingest process now natively consumes both
# streams; there is no Python beat-tick cadence left to bound). See
# tests/workers/test_celery_dead_code_contract.py.


def test_worker_commands_disable_prefetch_for_redis() -> None:
    """Compose-only: `--disable-prefetch` is a Celery/Redis QoS flag
    (CHAOS-2277). CHAOS-4195 deleted the Kubernetes (worker.yaml) and Helm
    (worker-deployment.yaml/worker-pools.yaml) equivalents this test used to
    also assert on -- the Go worker's River client has no prefetch concept to
    disable, so there is nothing there for this flag to apply to."""
    for path in (_REPO_ROOT / "compose.yml", _PROD_COMPOSE, _SWARM_STACK):
        services = _load_yaml(path).get("services") or {}
        worker_commands = [
            _command_string(service).split()
            for service in services.values()
            if "worker" in _command_string(service).split()
        ]

        assert worker_commands
        for command in worker_commands:
            assert "--disable-prefetch" in command


def test_local_compose_workers_import_mounted_source() -> None:
    services = _load_yaml(_REPO_ROOT / "compose.yml").get("services") or {}

    for service_name in ("worker", "worker-ingest", "worker-heavy"):
        service = services[service_name]
        command = _command_string(service).split()
        assert "worker" in command
        assert service["environment"]["PYTHONPATH"] == "/app/src"
        assert "./:/app" in service["volumes"]


def test_platform_compose_workers_and_beat_import_mounted_source() -> None:
    compose_path = _platform_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    service_names = ["worker", "worker-ingest", "worker-heavy", "beat"]
    if "worker-wi" in services:
        service_names.append("worker-wi")

    for service_name in service_names:
        service = services[service_name]
        assert service["environment"]["PYTHONPATH"] == "/app/src"
        assert "./ops:/app" in service["volumes"]


def test_platform_compose_provider_worker_consumes_sync_dispatch_queue() -> None:
    compose_path = _platform_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    provider_worker = services.get("worker-wi")
    if provider_worker is None:
        pytest.skip("platform compose.yml has no split provider worker")

    queues = _parse_queues(_container_command_string(provider_worker))
    assert "sync" in queues


def test_platform_compose_operator_credential_is_wired() -> None:
    """The optional monorepo Compose surface must expose the route operator.

    CHAOS-4054 deleted the ``DEV_HEALTH_ENV``/``GO_PROVIDER_ROUTES``
    local-all preset outright (the ``.env.go-all`` convenience file and the
    per-pair ``WORKER_*_ENABLED`` switch census it used to wire into every
    service are both gone -- a shipped route is always executable, so there
    is nothing left for a preset to turn on). What survives from this test is
    the operator-credential wiring, which has nothing to do with switches.
    """

    compose_path = _platform_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    operator = services.get("go-workerctl")
    assert operator is not None, "platform Compose must expose the route operator"
    operator_environment = operator.get("environment") or {}
    assert operator_environment["COORDINATOR_DATABASE_URI"].startswith(
        "postgresql://${RIVER_COORDINATOR_DATABASE_ROLE"
    )
    assert operator_environment["WORKER_OPERATOR_TOKEN"] == (
        "${WORKER_OPERATOR_TOKEN:-}"
    )


def test_platform_compose_applies_sync_routes_before_readiness() -> None:
    """Local no-Celery startup must converge routes without a readiness cycle."""

    compose_path = _platform_go_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    activators = {
        "go-sync-dispatch-route-activate": "dispatch_sync_run",
        "go-sync-finalize-route-activate": "finalize_sync_run",
        "go-sync-post-route-activate": "post_sync",
        "go-sync-reference-route-activate": "reference_discovery",
    }
    serial_order = tuple(activators)
    ready_dependencies = services["go-worker-ready"].get("depends_on") or {}

    for index, (service_name, kind) in enumerate(activators.items()):
        service = services[service_name]
        command = service.get("command") or []
        assert command[:2] == ["routes", "apply"]
        assert command[-1] == kind
        dependencies = service.get("depends_on") or {}
        assert "go-worker-ready" not in dependencies
        assert dependencies["go-worker-operator-credential"]["condition"] == (
            "service_completed_successfully"
        )
        assert dependencies["go-worker"]["condition"] == "service_started"
        assert dependencies["go-reconciler"]["condition"] == "service_started"
        if index:
            assert dependencies[serial_order[index - 1]]["condition"] == (
                "service_completed_successfully"
            )
        assert ready_dependencies[service_name]["condition"] == (
            "service_completed_successfully"
        )


def test_platform_compose_runs_the_go_scheduler_without_a_profile() -> None:
    """Local no-Celery startup must include the native periodic-work owner."""

    compose_path = _platform_go_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    migrate = services["migrate"]
    assert (migrate.get("build") or {})["context"] == ("${DEV_HEALTH_OPS_ROOT:-./ops}")
    assert "${DEV_HEALTH_OPS_ROOT:-./ops}:/app" in (migrate.get("volumes") or [])

    scheduler = services.get("go-scheduler")
    assert scheduler is not None, "platform Compose must run the Go scheduler"
    assert not scheduler.get("profiles"), "local scheduler must not require a profile"
    assert (scheduler.get("build") or {})["target"] == "scheduler"

    environment = scheduler.get("environment") or {}
    assert "DEV_HEALTH_PROFILE" not in environment
    assert environment["DEV_HEALTH_HTTP_ADDR"] == ":8080"
    assert environment["POSTGRES_URI"].startswith(
        "postgresql://${RIVER_DOMAIN_DATABASE_ROLE"
    )
    assert environment["WORKER_DATABASE_URI"].startswith(
        "postgresql://${RIVER_QUEUE_DATABASE_ROLE"
    )
    assert environment["COORDINATOR_DATABASE_URI"].startswith(
        "postgresql://${RIVER_COORDINATOR_DATABASE_ROLE"
    )
    assert environment["PGBOUNCER_TRANSACTION_MODE"] == "true"

    dependencies = scheduler.get("depends_on") or {}
    assert dependencies["go-worker-migrate"]["condition"] == (
        "service_completed_successfully"
    )
    # The scheduler opens pooled connections immediately, so a started-but-not
    # yet-accepting pooler is not good enough. Every pool it dials defines a
    # healthcheck, so gate on all three rather than only the domain pool: the
    # queue and coordinator pools were added later and were not asserted at
    # all, which is how this pin went stale unnoticed.
    for pool in ("pgbouncer", "pgbouncer-river-queue", "pgbouncer-river-coordinator"):
        assert dependencies[pool]["condition"] == "service_healthy", pool

    ready = services["go-worker-ready"]
    ready_dependencies = ready.get("depends_on") or {}
    assert ready_dependencies["go-scheduler"]["condition"] == "service_started"
    assert "http://go-scheduler:8080/readyz" in _command_string(ready)
    assert "beat" not in services


def test_compose_workers_override_runner_entrypoint() -> None:
    for path in (_LEGACY_COMPOSE, _PROD_COMPOSE, _SWARM_STACK):
        services = _load_yaml(path).get("services") or {}
        for service_name in ("worker", "worker-ingest", "worker-heavy", "beat"):
            service = services[service_name]
            assert service["entrypoint"] == ["celery"]
            command = _stringify_command(service["command"])
            assert command.split()[0] != "celery"
            assert "dev_health_ops.workers.celery_app" in command


def test_production_workers_use_semantic_postgres_uri() -> None:
    for path in (_PROD_COMPOSE, _SWARM_STACK):
        services = _load_yaml(path).get("services") or {}
        for service_name in ("api", "worker", "worker-ingest", "worker-heavy"):
            environment = services[service_name]["environment"]
            assert environment["POSTGRES_URI"].startswith("postgresql+asyncpg://")
            assert environment["DATABASE_URI"].startswith("postgresql+asyncpg://")
            assert environment["CLICKHOUSE_URI"].startswith("clickhouse://")


def test_production_stacks_consume_monitoring_queue() -> None:
    """The monitor-queue-depths beat entry enqueues to `monitoring`
    unconditionally — every production Celery stack's worker must consume it
    or telemetry tasks accumulate unconsumed forever (1,440/day). Celery-only:
    CHAOS-4195 deleted the Kubernetes (worker.yaml) and Helm (values.yaml
    worker pools) equivalents this test used to also cover -- the Go
    successors don't use Celery's `-Q`/`queues:` queue-list shape or a
    literal `monitoring` queue name at all, so there's nothing there for this
    regex-based check to match against."""
    import re

    stacks = [
        _PROD_COMPOSE,
        _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml",
    ]
    for stack in stacks:
        text = stack.read_text(encoding="utf-8")
        queue_lists = re.findall(r"(?:- |queues: \")(default,[a-z.,]+)", text)
        assert any("monitoring" in q for q in queue_lists), (
            f"{stack.name}: no worker queue list includes 'monitoring'"
        )


def _compose_worker_queues(path: Path) -> set[str]:
    """Union of -Q lists across every celery worker service in a compose file."""
    data = _load_yaml(path)
    consumed: set[str] = set()
    for _name, service in (data.get("services") or {}).items():
        cmd = _command_string(service)
        toks = cmd.split()
        if "celery" not in toks or "worker" not in toks:
            continue
        consumed |= _parse_queues(cmd)
    return consumed


def test_production_stacks_cover_every_celery_queue() -> None:
    """CHAOS-2308: every production Celery deploy stack must consume every
    queue in workers.config.task_queues across the union of its worker pools.
    A queue declared in task_queues but consumed by no prod worker silently
    accumulates forever (backfill jobs, webhook events, ingest, reports,
    cost-class sync). Mirrors test_compose_workers_cover_every_celery_queue
    for the prod stacks. Celery-only: CHAOS-4195 deleted the Kubernetes
    (worker.yaml) and Helm (values.yaml worker pools) coverage this test used
    to also assert, along with the now-unused `_k8s_worker_queues`/
    `_helm_worker_queues` helpers -- the Go successors don't route through
    Celery queue names."""
    all_queues = set(task_queues)
    coverage = {
        "compose.production.yml": _compose_worker_queues(_PROD_COMPOSE),
        "docker-swarm/stack.yml": _compose_worker_queues(_SWARM_STACK),
    }
    for name, consumed in coverage.items():
        missing = all_queues - consumed
        assert not missing, (
            f"{name}: production worker pools miss queues {sorted(missing)} "
            f"declared in workers.config.task_queues (consumed: {sorted(consumed)})"
        )


# ---------------------------------------------------------------------------
# CHAOS-3142: local-dev postgres hardening, plus the incident that motivated
# it.
#
# This file previously shared its Compose project name (`dev-health`) with
# the repo-root `compose.yml`, a separate file with an incompatible postgres
# definition. Because Compose tracks service ownership by
# (project name, service name), an `up` from either file could recreate the
# other file's already-running postgres container in place -- which is
# exactly what happened: the additive Go worker services for this ticket
# were designed to live here, adding RIVER_COORDINATOR_DATABASE_ROLE to this
# postgres service's environment, and `docker compose up -d postgres` from
# this file recreated the repo-root project's live postgres container with
# this file's incompatible role/database/PGDATA layout. The Go worker
# services moved to the repo-root compose.yml (the file that actually owns
# that project), and this file's project name changed so the collision is
# now structurally impossible. The digest pin and PGDATA fix below are
# independent, also-real defects that predated and outlasted the same
# incident; they still matter for this file's own isolated volume.
# ---------------------------------------------------------------------------


def test_ops_compose_project_name_is_not_dev_health() -> None:
    """The regression barrier for the CHAOS-3142 incident: this file's
    Compose project name must never again collide with the repo-root
    compose.yml's `dev-health` project. A shared name lets either file's
    `up` recreate the other's already-running containers with an
    incompatible postgres definition -- silently, since Compose reports it
    as an ordinary "Recreate", not a conflict.

    Mutation coverage (manually verified): setting `name: dev-health` back
    makes this test fail.
    """
    compose = _load_yaml(_LEGACY_COMPOSE)
    assert compose.get("name") != "dev-health", (
        "CHAOS-3142: this file's compose project name must not be "
        "'dev-health' -- that collides with the repo-root compose.yml's "
        "project and lets either file's `up` recreate the other's "
        "containers with an incompatible postgres definition"
    )
    assert compose.get("name"), "compose.yml must declare an explicit project name"


def test_local_postgres_image_pinned_and_pgdata_is_subdirectory() -> None:
    """CHAOS-3142 incident: `postgres:latest` is a moving tag. It silently
    changed base image family (Debian glibc -> Alpine musl) between when the
    local dev cluster was created and a later `docker compose up`/pull, and
    combined with a `PG_DATA` typo (the image only reads `PGDATA`) the new
    image version picked its own default data directory -- a DIFFERENT
    subdirectory of the same mounted volume -- and silently initialized a
    brand-new empty cluster beside the real one instead of using it.

    Pin the image by digest (matching the Postgres build already pinned in
    internal/testsupport/containers/harness.go) and set PGDATA explicitly to
    a subdirectory of the /var/lib/postgresql volume mount, never the mount
    root itself.

    Mutation coverage (manually verified): reverting the image back to
    `postgres:latest` fails the digest assertion; reverting PGDATA back to
    `PG_DATA: /var/lib/postgresql/` fails both the "PG_DATA not read" and
    "PGDATA must be set" assertions; setting `PGDATA: /var/lib/postgresql/`
    (the mount root) fails the subdirectory assertion.
    """
    services = _load_yaml(_LEGACY_COMPOSE)["services"]
    postgres = services["postgres"]

    assert "@sha256:" in postgres["image"], (
        f"postgres image must be pinned by digest, not a moving tag: {postgres['image']!r}"
    )
    tag = postgres["image"].split("@sha256:", 1)[0]
    assert not tag.endswith(":latest"), (
        f"postgres image tag must not be 'latest', even when digest-pinned: {postgres['image']!r}"
    )

    env = postgres["environment"]
    assert "PG_DATA" not in env, (
        "PG_DATA is not an env var the postgres image reads (typo for PGDATA); "
        "its presence means the fix was reverted or duplicated"
    )
    assert "PGDATA" in env, (
        "PGDATA must be set explicitly so the cluster path is pinned"
    )

    mount_root = next(
        volume.split(":", 1)[1]
        for volume in postgres["volumes"]
        if volume.startswith("postgres_data:")
    )
    pgdata = env["PGDATA"]
    assert pgdata.rstrip("/") != mount_root.rstrip("/"), (
        "PGDATA must be a subdirectory of the volume mount, not the mount root itself"
    )
    assert pgdata.rstrip("/").startswith(mount_root.rstrip("/") + "/"), (
        f"PGDATA {pgdata!r} must be nested under the volume mount {mount_root!r}"
    )


def test_go_config_package_declares_no_route_switches() -> None:
    """CHAOS-4054 step 3: the typed Go config surface must name zero
    ``WORKER_*_ENABLED`` route switches. Capability is always on in the
    binary now; there is nothing left for config.go (or any file in the
    package) to gate."""
    configured = frozenset(
        name
        for source in sorted(_GO_CONFIG_PACKAGE.glob("*.go"))
        if not source.name.endswith("_test.go")
        for name in _WORKER_ENABLED_SWITCH_PATTERN.findall(source.read_text())
    )
    assert not configured, (
        f"Go typed config must declare no route switches, found: {sorted(configured)}"
    )


def test_compose_declares_no_provider_route_switches() -> None:
    """Ticket step-5 acceptance: the rendered compose config contains ZERO
    ``WORKER_*_ENABLED`` keys anywhere -- not on the shared Celery env
    anchor, not on worker/beat, not on the additive Go profile. The route
    switch plane is deleted, not defaulted off.

    The GitHub work-item route's two file-path configs are NOT switches
    (``WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH`` /
    ``..._INVESTMENT_CONFIG_PATH``) and must still be present, unset by
    default.
    """
    services = _load_yaml(_LEGACY_COMPOSE)["services"]

    shared_env = services["api"]["environment"]  # &env anchor: api, worker, beat
    for name in ("api", "worker", "worker-heavy", "beat"):
        env = services[name]["environment"]
        matched = [key for key in env if _WORKER_ENABLED_SWITCH_PATTERN.fullmatch(key)]
        assert not matched, f"{name} still declares route switches: {sorted(matched)}"

    for name in _PROVIDER_ROUTE_CONFIG_NAMES:
        assert shared_env[name] == f"${{{name}:-}}"

    go_worker_env = _load_yaml(_GO_WORKER_OVERLAY)["services"]["go-worker"][
        "environment"
    ]
    matched = [
        key for key in go_worker_env if _WORKER_ENABLED_SWITCH_PATTERN.fullmatch(key)
    ]
    assert not matched, f"go-worker still declares route switches: {sorted(matched)}"
    for name in _PROVIDER_ROUTE_CONFIG_NAMES:
        assert go_worker_env[name] == f"${{{name}:-}}"


def test_provider_route_env_example_declares_no_route_switches() -> None:
    """The example must not inventory a deleted switch plane, but must still
    inventory the two file-path configs that remain real deployment inputs."""
    lines = _REPO_ROOT.joinpath(".env.example").read_text().splitlines()
    declared = set(lines)

    matched = [line for line in lines if _WORKER_ENABLED_SWITCH_PATTERN.search(line)]
    assert not matched, f".env.example still mentions route switches: {matched}"

    for name in _PROVIDER_ROUTE_CONFIG_NAMES:
        assert f'# {name}=""' in declared
        assert not any(line.startswith(f"{name}=") for line in lines)

    assert not any(line.startswith("COMPOSE_PROFILES=") for line in lines), (
        ".env.example must not activate the opt-in Go profile"
    )


def test_go_profile_overlay_never_depends_on_python_migrate() -> None:
    """CHAOS-3142/CHAOS-3143: no `go-*` service may `depends_on` the Python
    `migrate` service.

    `depends_on` pulls a service in regardless of profile. The application
    migrator defers 0066 by default, but an environment carrying the explicit
    cutover authorization would still flip routes before the downstream Go
    runtimes can start, violating 0066's ordering contract.

    Standing up the Go observation path must never be able to move real traffic
    as a side effect, so the Python schema stays an explicitly authorized
    prerequisite (`migrate postgres --revision 0065`) rather than a compose
    dependency edge.

    Mutation coverage (manually verified): re-adding
    `migrate: {condition: service_completed_successfully}` to
    go-worker-migrate's depends_on fails this test.
    """
    services = _load_yaml(_GO_WORKER_OVERLAY)["services"]
    go_services = {
        name: spec for name, spec in services.items() if name.startswith("go-")
    }
    assert go_services, "the overlay must define the go-* services it exists to carry"

    for name, spec in go_services.items():
        depends_on = spec.get("depends_on") or {}
        # Normalise both the mapping (long) and list (short) syntaxes.
        dependencies = (
            set(depends_on) if isinstance(depends_on, dict) else set(depends_on)
        )
        assert "migrate" not in dependencies, (
            f"{name} must not depend on the Python `migrate` service: that edge runs "
            "the Python migration service on a plain `--profile go up`; an "
            "authorized 0066 cutover would run before any Go consumer can start"
        )


def test_go_profile_overlay_services_are_all_profile_gated() -> None:
    """CHAOS-3142: every service the overlay adds must be gated behind the `go`
    profile, so a default `docker compose up` brings up the unchanged Celery
    stack and nothing else.

    This is what makes the overlay safe to leave in place permanently -- the
    developer opts in per-invocation (`--profile go`) or via COMPOSE_PROFILES in
    the root `.env`, and forgetting to opt in costs nothing.

    Mutation coverage (manually verified): deleting `profiles: ["go"]` from any
    go-* service fails this test.
    """
    services = _load_yaml(_GO_WORKER_OVERLAY)["services"]
    for name, spec in services.items():
        assert spec.get("profiles") == ["go"], (
            f'{name} must declare profiles: ["go"] so a default `up` never starts it'
        )


def test_go_profile_overlay_worker_selects_manifest_queues_without_runtime_profile() -> (
    None
):
    """CHAOS-3851: the local River worker uses the registered sync queues.

    Compose's `go` activation profile is a deployment opt-in and is unrelated
    to the worker's removed runtime profile contract.
    """
    services = _load_yaml(_GO_WORKER_OVERLAY)["services"]
    worker = services["go-worker"]
    environment = worker["environment"]
    assert "DEV_HEALTH_PROFILE" not in environment
    assert "DEV_HEALTH_QUEUES" not in environment
    assert "DEV_HEALTH_QUEUE_CONCURRENCY" not in environment
    assert "DEV_HEALTH_WORKER_GROUP" not in environment
    arguments = _go_worker_arguments(worker)
    sync_queues = next(
        process["queues"]
        for process in _load_yaml(_REPO_ROOT / "deploy/go-workers/deployment.json")[
            "processes"
        ]
        if process["name"] == "sync"
    )
    assert arguments["--queues"] == ",".join(sync_queues)
    sync_process = next(
        process
        for process in _load_yaml(_REPO_ROOT / "deploy/go-workers/deployment.json")[
            "processes"
        ]
        if process["name"] == "sync"
    )
    assert arguments["--queue-concurrency"] == ",".join(
        f"{entry['queue']}={entry['max_workers']}"
        for entry in sync_process["queue_workers"]
    )
    assert arguments["--worker-group"] == "sync"
    assert worker["profiles"] == ["go"]


def test_platform_go_runtime_uses_bounded_session_poolers() -> None:
    compose_path = _platform_go_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    transaction_pool = services["pgbouncer"]
    queue_pool = services["pgbouncer-river-queue"]
    coordinator_pool = services["pgbouncer-river-coordinator"]
    assert queue_pool["environment"].get("POOL_MODE") == "session"
    assert coordinator_pool["environment"].get("POOL_MODE") == "session"
    # PgBouncer budgets are per (database, user) pool. Each endpoint has one
    # fixed River role and one database, so these are true backend ceilings.
    assert (
        queue_pool["environment"].get("DB_USER")
        == "${RIVER_QUEUE_DATABASE_ROLE:-devhealth_queue}"
    )
    assert (
        coordinator_pool["environment"].get("DB_USER")
        == "${RIVER_COORDINATOR_DATABASE_ROLE:-devhealth_coordinator}"
    )
    # Assert the budget rule, not a literal. Session mode pins one server
    # connection per client session for the life of that session, so
    # DEFAULT_POOL_SIZE is a hard ceiling on worker replicas rather than a
    # throughput knob: the first replica past it never reaches ready, it
    # crash-loops on preclaim-readiness with the queue-database checks
    # failing. A literal pinned here passes for whatever number happens to be
    # deployed, including one that is already too small.
    queue_pool_size = _compose_int(queue_pool["environment"].get("DEFAULT_POOL_SIZE"))
    required_queue_backends = (
        _QUEUE_CONNECTIONS_PER_REPLICA
        * _PLATFORM_REPLICAS_PER_GROUP
        * len(_QUEUE_WORKER_GROUPS)
    ) + _QUEUE_SINGLETON_CONNECTIONS
    assert queue_pool_size >= required_queue_backends, (
        f"queue session pool holds {queue_pool_size} backends; "
        f"{_PLATFORM_REPLICAS_PER_GROUP} replicas of "
        f"{len(_QUEUE_WORKER_GROUPS)} worker groups plus singletons need "
        f"{required_queue_backends}"
    )
    coordinator_pool_size = _compose_int(
        coordinator_pool["environment"].get("DEFAULT_POOL_SIZE")
    )
    transaction_pool_size = _compose_int(
        transaction_pool["environment"].get("DEFAULT_POOL_SIZE")
    )
    assert coordinator_pool_size >= 10
    assert transaction_pool_size >= 20
    assert "RESERVE_POOL_SIZE" not in transaction_pool["environment"]

    # The three pools plus direct migration connections must fit the server.
    # PgBouncer opens backends lazily, so an over-subscribed budget surfaces
    # only under the load that needs every pool at once.
    usable = _postgres_max_connections(services["postgres"]) - _POSTGRES_RESERVED
    declared = (
        queue_pool_size
        + coordinator_pool_size
        + transaction_pool_size
        + _MIGRATION_CONNECTIONS
    )
    assert declared <= usable, (
        f"pooler backends plus migrations ({declared}) exceed usable "
        f"PostgreSQL connections ({usable})"
    )

    for service_name in ("go-worker", "go-worker-heavy", "go-worker-ops"):
        environment = services[service_name]["environment"]
        assert "@pgbouncer-river-queue:6433/" in environment["WORKER_DATABASE_URI"]
        assert environment["WORKER_DATABASE_MODE"] == "session"
        assert "COORDINATOR_DATABASE_URI" not in environment
    for service_name in ("go-reconciler", "go-scheduler", "go-worker-route-activate"):
        environment = services[service_name]["environment"]
        assert "@pgbouncer-river-queue:6433/" in environment["WORKER_DATABASE_URI"]
        assert (
            "@pgbouncer-river-coordinator:6434/"
            in environment["COORDINATOR_DATABASE_URI"]
        )
        assert environment["COORDINATOR_DATABASE_MODE"] == "session"


def test_go_reconciler_declares_a_readyz_healthcheck() -> None:
    """CHAOS-4239: "any stage kills the process" (the crash-loop this ticket
    fixes) and "a wedged process is never restarted" are the same defect from
    opposite directions. The Go per-stage budgets and step-overrun watchdog
    make /readyz report unhealthy when a stage is stuck; without a Compose
    healthcheck nothing outside the container ever asks, so a wedged
    reconciler would sit invisible in `docker ps` alongside a state that says
    "unless-stopped" forever.

    The runtime image is distroless (docker/go-worker.Dockerfile,
    gcr.io/distroless/static-debian12) with no shell and no curl/wget, so the
    test string must be the reconciler binary's own `healthcheck`
    subcommand (cmd/dev-health-reconciler/main.go), never a CMD-SHELL
    one-liner that could not run in this image at all.

    No other service in this overlay declares a healthcheck to match
    interval/timeout/retries against -- this asserts the reconciler's own
    values are present and sane, not copied from a sibling.

    Mutation coverage (manually verified): deleting the `healthcheck` key,
    or changing `test` to a CMD-SHELL form, or to a command other than the
    binary's own `healthcheck` subcommand, each fail this test.
    """
    services = _load_yaml(_GO_WORKER_OVERLAY)["services"]
    healthcheck = services["go-reconciler"].get("healthcheck")
    assert healthcheck is not None, "go-reconciler must declare a healthcheck"

    test = healthcheck.get("test")
    assert isinstance(test, list) and test[:1] == ["CMD"], (
        "the runtime image is distroless -- has no shell, no curl, no wget -- "
        "so the healthcheck must use exec ('CMD', ...) form, never CMD-SHELL"
    )
    assert test == [
        "CMD",
        "/usr/local/bin/dev-health-reconciler",
        "healthcheck",
    ], (
        "the healthcheck must invoke the reconciler binary's own `healthcheck` "
        "subcommand: nothing else executable exists in the distroless image"
    )

    for key in ("interval", "timeout", "retries", "start_period"):
        assert key in healthcheck, f"go-reconciler healthcheck is missing {key!r}"
    assert set(healthcheck) == {
        "test",
        "interval",
        "timeout",
        "retries",
        "start_period",
    }

    for other_name, other_spec in services.items():
        if other_name == "go-reconciler":
            continue
        assert "healthcheck" not in other_spec, (
            f"{other_name} declares a healthcheck too, but this test's "
            "docstring claims go-reconciler's is the only one in this "
            "overlay -- update the docstring if that changed on purpose"
        )
