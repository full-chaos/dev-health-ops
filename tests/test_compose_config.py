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
_GO_CONFIG = _REPO_ROOT / "internal" / "platform" / "config" / "config.go"
_K8S_DIR = _REPO_ROOT / "deploy" / "kubernetes"
_HELM_DIR = _REPO_ROOT / "deploy" / "helm" / "dev-health"

_PROVIDER_ROUTE_SWITCH_NAMES = frozenset(
    {
        "WORKER_GITHUB_BLAME_ENABLED",
        "WORKER_GITHUB_CICD_ENABLED",
        "WORKER_GITHUB_COMMIT_STATS_ENABLED",
        "WORKER_GITHUB_COMMITS_ENABLED",
        "WORKER_GITHUB_DEPLOYMENTS_ENABLED",
        "WORKER_GITHUB_FILES_ENABLED",
        "WORKER_GITHUB_PR_COMMENTS_ENABLED",
        "WORKER_GITHUB_PR_REVIEWS_ENABLED",
        "WORKER_GITHUB_PRS_ENABLED",
        "WORKER_GITHUB_REPO_METADATA_ENABLED",
        "WORKER_GITHUB_SECURITY_ENABLED",
        "WORKER_GITHUB_TESTS_ENABLED",
        "WORKER_GITHUB_WORK_ITEMS_ENABLED",
        "WORKER_GITLAB_BLAME_ENABLED",
        "WORKER_GITLAB_CICD_ENABLED",
        "WORKER_GITLAB_COMMIT_STATS_ENABLED",
        "WORKER_GITLAB_COMMITS_ENABLED",
        "WORKER_GITLAB_DEPLOYMENTS_ENABLED",
        "WORKER_GITLAB_FEATURE_FLAGS_ENABLED",
        "WORKER_GITLAB_FILES_ENABLED",
        "WORKER_GITLAB_INCIDENTS_ENABLED",
        "WORKER_GITLAB_PR_COMMENTS_ENABLED",
        "WORKER_GITLAB_PR_REVIEWS_ENABLED",
        "WORKER_GITLAB_PRS_ENABLED",
        "WORKER_GITLAB_REPO_METADATA_ENABLED",
        "WORKER_GITLAB_SECURITY_ENABLED",
        "WORKER_GITLAB_TESTS_ENABLED",
        "WORKER_GITLAB_WORK_ITEMS_ENABLED",
        "WORKER_JIRA_INCIDENTS_ENABLED",
        "WORKER_JIRA_WORK_ITEMS_ENABLED",
        "WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED",
        "WORKER_LINEAR_WORK_ITEMS_ENABLED",
        "WORKER_PAGERDUTY_BUSINESS_SERVICES_ENABLED",
        "WORKER_PAGERDUTY_ESCALATION_POLICIES_ENABLED",
        "WORKER_PAGERDUTY_INCIDENTS_ENABLED",
        "WORKER_PAGERDUTY_ON_CALLS_ENABLED",
        "WORKER_PAGERDUTY_SCHEDULES_ENABLED",
        "WORKER_PAGERDUTY_SERVICES_ENABLED",
        "WORKER_PAGERDUTY_TEAMS_ENABLED",
        "WORKER_PAGERDUTY_USERS_ENABLED",
    }
)

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
    # The coordinator login is provisioned here for deployed environments the
    # same way init-extra-dbs.sh does it for local dev, and likewise receives no
    # per-table grants -- the pinned River migration owns those.
    for coordinator_fragment in (
        ":'coordinator_role',",
        'GRANT CONNECT ON DATABASE :"app_database" TO :"coordinator_role";',
        'GRANT USAGE ON SCHEMA public TO :"coordinator_role";',
        'REVOKE CREATE ON SCHEMA public FROM :"coordinator_role";',
    ):
        assert coordinator_fragment in upgrade_script
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
        assert grant in upgrade_script
    assert "REVOKE CREATE ON SCHEMA public FROM PUBLIC" not in upgrade_script
    assert (
        'REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, :"domain_role", :"queue_role";'
        in upgrade_script
    )
    assert (
        "GRANT INSERT ON TABLE public.worker_job_completion_fences"
        not in upgrade_script
    )
    queue_section = upgrade_script.split("-- The queue role", maxsplit=1)[1]
    assert "GRANT SELECT ON TABLE public.sync_runs TO %I" in queue_section
    assert "GRANT SELECT ON TABLE public.sync_run_units TO %I" in queue_section
    assert "GRANT SELECT, UPDATE ON TABLE public.sync_runs" not in queue_section
    assert "GRANT SELECT, INSERT ON TABLE public.sync_runs" not in queue_section
    assert "GRANT SELECT, UPDATE ON TABLE public.sync_run_units" not in queue_section
    assert "GRANT SELECT, INSERT ON TABLE public.sync_run_units" not in queue_section
    _assert_least_privilege_domain_grants(
        upgrade_script.split("-- The queue role", maxsplit=1)[0]
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


# The two provisioning scripts mark the end of the domain section differently.
# Slicing both on "-- The queue role" silently returned the WHOLE of
# init-extra-dbs.sh, which has no such marker -- so assertions meant for the
# domain role were reading the queue role's grants too, and the negative test
# below passed by raising for the wrong reason.
_DOMAIN_SECTION_MARKERS = {
    "init-extra-dbs.sh": 'GRANT USAGE ON SCHEMA public TO :"queue_role";',
    "provision_river_roles.sql": "-- The queue role",
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
    (
        _REPO_ROOT / "docker" / "init-extra-dbs.sh",
        _REPO_ROOT / "scripts" / "worker" / "provision_river_roles.sql",
    ),
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


@pytest.mark.parametrize("manifest", ["api.yaml", "worker.yaml"])
def test_kubernetes_app_deployments_wait_for_migrations(manifest: str) -> None:
    """CHAOS-2304 safety net: a naive `kubectl apply -k` rolls Deployments
    without waiting for the migrate Job. api/worker must carry a read-only
    wait-for-migrations initContainer that blocks until the schema is
    current (`dev-hops migrate clickhouse status --check`) and never runs
    DDL itself."""
    deployment = next(d for d in _k8s_docs(manifest) if d.get("kind") == "Deployment")
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
        f"{manifest} must define a wait-for-migrations initContainer"
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
    for stack in (_REPO_ROOT / "compose.yml", _PROD_COMPOSE, _SWARM_STACK):
        _assert_compose_beat_singleton(stack)

    k8s_beats = []
    for doc in yaml.safe_load_all((_K8S_DIR / "beat.yaml").read_text()):
        if not doc or doc.get("kind") != "Deployment":
            continue
        containers = doc["spec"]["template"]["spec"].get("containers") or []
        for container in containers:
            command = container.get("command") or []
            if "celery" in command and "beat" in command:
                k8s_beats.append(doc)

    assert len(k8s_beats) == 1
    assert k8s_beats[0]["spec"].get("replicas") == 1

    kustomization = _load_yaml(_K8S_DIR / "kustomization.yaml")
    assert "beat.yaml" in kustomization["resources"]

    beat_template = (_HELM_DIR / "templates" / "beat-deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert "SINGLETON: exactly 1 beat replica" in beat_template
    assert "replicas: 1" in beat_template


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


def test_stream_consumer_beat_ticks_do_not_outlive_cadence() -> None:
    from dev_health_ops.api.ingest.consumer import BLOCK_MS as INGEST_BLOCK_MS
    from dev_health_ops.api.product_telemetry.consumer import (
        BLOCK_MS as PRODUCT_TELEMETRY_BLOCK_MS,
    )
    from dev_health_ops.workers.config import beat_schedule

    cases = {
        "process-ingest-streams": INGEST_BLOCK_MS,
        "process-product-telemetry-streams": PRODUCT_TELEMETRY_BLOCK_MS,
    }

    for entry_name, block_ms in cases.items():
        entry = beat_schedule[entry_name]
        schedule_seconds = float(entry["schedule"])
        max_iterations = int(entry["kwargs"]["max_iterations"])

        assert (max_iterations * block_ms) / 1000 < schedule_seconds
        assert entry["options"] == {"queue": "ingest", "expires": 30}


def test_worker_commands_disable_prefetch_for_redis() -> None:
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

    k8s_commands: list[list[str]] = []
    for doc in yaml.safe_load_all((_K8S_DIR / "worker.yaml").read_text()):
        if not doc or doc.get("kind") != "Deployment":
            continue
        for container in doc["spec"]["template"]["spec"].get("containers") or []:
            command = container.get("command") or []
            if "worker" in command:
                k8s_commands.append(command)

    assert k8s_commands
    for command in k8s_commands:
        assert "--disable-prefetch" in command

    helm_templates = [
        _HELM_DIR / "templates" / "worker-deployment.yaml",
        _HELM_DIR / "templates" / "worker-pools.yaml",
    ]
    for template in helm_templates:
        text = template.read_text(encoding="utf-8")
        assert "--disable-prefetch" in text


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


def test_platform_compose_wires_local_all_provider_routes_end_to_end() -> None:
    """The optional monorepo Compose surface must wire one preset to both runtimes."""

    compose_path = _platform_compose_path()
    if compose_path is None:
        pytest.skip("platform compose.yml is only present in the monorepo checkout")

    services = _load_yaml(compose_path).get("services") or {}
    for service_name in ("api", "worker", "beat", "go-worker"):
        environment = services[service_name].get("environment") or {}
        assert environment["DEV_HEALTH_ENV"] == "${DEV_HEALTH_ENV:-}"
        assert environment["GO_PROVIDER_ROUTES"] == "${GO_PROVIDER_ROUTES:-}"
        assert _PROVIDER_ROUTE_SWITCH_NAMES <= environment.keys(), (
            f"{service_name} is missing provider route switches: "
            f"{sorted(_PROVIDER_ROUTE_SWITCH_NAMES - environment.keys())}"
        )

    operator = services.get("go-workerctl")
    assert operator is not None, "platform Compose must expose the route operator"
    operator_environment = operator.get("environment") or {}
    assert operator_environment["COORDINATOR_DATABASE_URI"].startswith(
        "postgresql://${RIVER_COORDINATOR_DATABASE_ROLE"
    )
    assert operator_environment["WORKER_OPERATOR_TOKEN"] == (
        "${WORKER_OPERATOR_TOKEN:-}"
    )

    preset_path = compose_path.with_name(".env.go-all")
    assert preset_path.is_file()
    preset = {
        key: value
        for line in preset_path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
        for key, value in (line.split("=", maxsplit=1),)
    }
    assert preset == {
        "COMPOSE_PROFILES": "go",
        "DEV_HEALTH_ENV": "local",
        "GO_PROVIDER_ROUTES": "all",
    }


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
    unconditionally — every production stack's worker must consume it or
    telemetry tasks accumulate unconsumed forever (1,440/day)."""
    import re

    stacks = [
        _PROD_COMPOSE,
        _REPO_ROOT / "deploy" / "docker-swarm" / "stack.yml",
        _REPO_ROOT / "deploy" / "kubernetes" / "worker.yaml",
        _REPO_ROOT / "deploy" / "helm" / "dev-health" / "values.yaml",
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


def _k8s_worker_queues(path: Path) -> set[str]:
    """Union of -Q lists across every worker Deployment in a k8s manifest."""
    consumed: set[str] = set()
    for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if not doc or doc.get("kind") != "Deployment":
            continue
        pod = doc["spec"]["template"]["spec"]
        for container in pod.get("containers") or []:
            cmd = container.get("command") or []
            for i, tok in enumerate(cmd):
                if tok == "-Q" and i + 1 < len(cmd):
                    consumed |= {q for q in str(cmd[i + 1]).split(",") if q}
    return consumed


def _helm_worker_queues(values_path: Path) -> set[str]:
    """Union of queue lists across every enabled worker pool in helm values."""
    values = _load_yaml(values_path)
    consumed: set[str] = set()
    for pool in ("worker", "workerIngest", "workerExternalIngest", "workerHeavy"):
        cfg = values.get(pool) or {}
        if cfg.get("enabled") is False:
            continue
        queues = cfg.get("queues")
        if queues:
            consumed |= {q for q in str(queues).split(",") if q}
    return consumed


def test_production_stacks_cover_every_celery_queue() -> None:
    """CHAOS-2308: every production deploy stack must consume every queue in
    workers.config.task_queues across the union of its worker pools. A queue
    declared in task_queues but consumed by no prod worker silently accumulates
    forever (backfill jobs, webhook events, ingest, reports, cost-class sync).
    Mirrors test_compose_workers_cover_every_celery_queue for the prod stacks."""
    all_queues = set(task_queues)
    coverage = {
        "compose.production.yml": _compose_worker_queues(_PROD_COMPOSE),
        "docker-swarm/stack.yml": _compose_worker_queues(_SWARM_STACK),
        "kubernetes/worker.yaml": _k8s_worker_queues(_K8S_DIR / "worker.yaml"),
        "helm values.yaml": _helm_worker_queues(_HELM_DIR / "values.yaml"),
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


def test_provider_route_switch_inventory_matches_go_config() -> None:
    """The packaging census must move with the typed Go configuration surface."""
    configured = frozenset(
        re.findall(r'"(WORKER_[A-Z0-9_]+_ENABLED)"', _GO_CONFIG.read_text())
    )
    assert configured == _PROVIDER_ROUTE_SWITCH_NAMES


def test_route_switches_default_off_for_producer_gate() -> None:
    """Every typed provider switch passes through both runtimes, default-off.

    The shared ``&env`` anchor feeds the Python producer processes and the Go
    profile feeds the executor. A name missing on either side can route a unit
    to a process with no matching handler; a true default would activate a
    landed provider route merely by deploying this packaging change.
    """
    services = _load_yaml(_LEGACY_COMPOSE)["services"]

    shared_env = services["api"]["environment"]  # &env anchor: api, worker, beat
    for switch in _PROVIDER_ROUTE_SWITCH_NAMES:
        assert shared_env[switch] == f"${{{switch}:-false}}", (
            f"{switch} must default to false on the shared Celery env anchor"
        )

    # worker/worker-heavy/beat inherit the anchor via `<<: *env`/`<<: *worker-base`;
    # confirm the merge actually carried the keys rather than being shadowed.
    for name in ("worker", "worker-heavy", "beat"):
        env = services[name]["environment"]
        for switch in _PROVIDER_ROUTE_SWITCH_NAMES:
            assert env[switch] == f"${{{switch}:-false}}", (
                f"{switch} must reach {name} through the shared env anchor"
            )

    # The GitHub work-item route rejects guessed filesystem defaults. These are
    # deliberately explicit deployment inputs and stay empty until a reviewed
    # activation supplies mounted/runtime paths.
    for name in _PROVIDER_ROUTE_CONFIG_NAMES:
        assert shared_env[name] == f"${{{name}:-}}"

    # The additive Go profile carries the exact same inactive switches and
    # worker-only paths. This is configuration, not profile activation.
    go_worker_env = _load_yaml(_GO_WORKER_OVERLAY)["services"]["go-worker"][
        "environment"
    ]
    for switch in _PROVIDER_ROUTE_SWITCH_NAMES:
        assert go_worker_env[switch] == f"${{{switch}:-false}}"
    for name in _PROVIDER_ROUTE_CONFIG_NAMES:
        assert go_worker_env[name] == f"${{{name}:-}}"


def test_provider_route_env_example_is_unset_and_default_off() -> None:
    """The example inventories opt-ins without enabling a route or profile."""
    lines = _REPO_ROOT.joinpath(".env.example").read_text().splitlines()
    declared = set(lines)

    for switch in _PROVIDER_ROUTE_SWITCH_NAMES:
        assert f'# {switch}="false"' in declared
        assert not any(line.startswith(f"{switch}=") for line in lines)
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
    assert queue_pool["environment"].get("DEFAULT_POOL_SIZE") == "22"
    assert coordinator_pool["environment"].get("DEFAULT_POOL_SIZE") == "10"
    assert transaction_pool["environment"].get("DEFAULT_POOL_SIZE") == "20"
    assert "RESERVE_POOL_SIZE" not in transaction_pool["environment"]

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
